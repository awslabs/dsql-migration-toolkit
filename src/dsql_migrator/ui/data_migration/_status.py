# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC status / controller / deploy-formatting logic (NiceGUI-free).

Extracted from the Data Migration screen package ``__init__`` so the heavy
status, network-probe, classification, and deploy-stage *formatting* logic lives
apart from the NiceGUI render code. Everything here is pure or read-only network
I/O; nothing builds NiceGUI widgets. The package ``__init__`` re-imports these
names so the public import surface of ``dsql_migrator.ui.data_migration`` is
unchanged and the remaining render code resolves them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_STACK_NAME,
    ConnectorState,
    ConnectorStatus,
    build_cdc_status_view,
    cdc_expected_connector_names,
)
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.target_introspector import count_target_rows
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationJob,
    StepStatus,
)
from dsql_migrator.ui.connect import make_source_engine_factory
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

# Min seconds between render-time CDC discovery reads (describe_stacks +
# list_connectors). Re-renders within this window reuse the cached CDC state.
_CDC_DISCOVERY_THROTTLE_SECONDS = 5.0

# Pure formatters/view-models from the sibling submodule (NiceGUI-free).
from dsql_migrator.ui.data_migration._models import format_duration


def _current_job(
    job_manager: JobManager, job_id: Optional[str]
) -> Optional[MigrationJob]:
    """Return the current job snapshot for ``job_id``, or ``None`` if absent."""
    if job_id is None:
        return None
    try:
        return job_manager.get_status(job_id)
    except JobNotFoundError:
        return None


def _read_cdc_template_body() -> Optional[str]:
    """Read the canonical cdc-stack CloudFormation template, or None on failure."""
    from pathlib import Path

    # repo layout: <root>/deploy/cdc-stack/cdc-stack.yaml; this file is at
    # <root>/src/dsql_migrator/ui/data_migration/_status.py → parents[4] is the
    # repo root (data_migration is now a package, one level deeper than before).
    try:
        root = Path(__file__).resolve().parents[4]
        template = root / "deploy" / "cdc-stack" / "cdc-stack.yaml"
        return template.read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001
        return None


# Per-operation terminal notice headers (kind -> (done_header, fail_header)) and
# bodies. The header carries the verdict ("… complete"/"… failed"); the body gives
# the next step. Rendered as AWS-style success/error notices.
_CDC_ACTION_NOUN = {
    "infra": ("Infrastructure deploy", "Infrastructure deploy"),
    "start": ("CDC start", "CDC start"),
    "stop": ("Stop CDC", "Stop CDC"),
    "delete": ("Delete infrastructure", "Delete infrastructure"),
}
_CDC_ACTION_TERMINAL = {
    "infra": (
        "Infrastructure is ready — run Start CDC to create the connectors.",
        "See the log above. If the stack rolled back, use Delete CDC "
        "infrastructure before retrying.",
    ),
    "start": (
        "Connectors should appear in live status below.",
        "See the log above for the cause. The cdc-stack rolls back automatically; "
        "fix the cause and retry Start CDC.",
    ),
    "stop": (
        "Connectors removed. MSK and infrastructure are preserved.",
        "See the log above and retry.",
    ),
    "delete": (
        "The cdc-stack and its MSK/VPC resources were torn down.",
        "See the log above. You may need to delete the stack from the "
        "CloudFormation console.",
    ),
}


# Display labels per stage id, grouped by operation kind. Keys cover every
# chunk_id the four orchestration functions emit (see core/cdc_deployer.py).
_CDC_STAGE_LABELS = {
    "infra": {
        "check_existing": "Checking for existing stack",
        "validate_params": "Validating infrastructure parameters",
        "create_stack": "Submitting stack creation",
        "stack_create": "Creating infrastructure",
        "infra_ready": "Infrastructure ready",
    },
    "start": {
        "discover_stack": "Discovering cdc-stack",
        "validate_params": "Validating configuration",
        "fetch_bootstrap": "Fetching MSK bootstrap brokers",
        "submit_connectors": "Starting connectors (topics + source + sink)",
        "stack_connectors": "Connectors deploying",
        "connectors_running": "Waiting for connectors (source + sink)",
        "pipeline_running": "Pipeline running",
    },
    "stop": {
        "discover_stack": "Discovering cdc-stack",
        "submit_stop": "Submitting connector removal",
        "stack_stop": "Removing connectors",
        "connectors_gone": "CDC stopped",
    },
    "delete": {
        "discover_stack": "Discovering cdc-stack",
        "submit_delete": "Submitting stack deletion",
        "stack_delete": "Deleting infrastructure",
        "deleted": "Infrastructure deleted",
    },
}
_CDC_ACTION_TITLE = {
    "infra": "Deploy progress",
    "start": "Start CDC progress",
    "stop": "Stop CDC progress",
    "delete": "Delete progress",
}

# Rough per-stage time estimates (seconds), shown beside each stage so the user
# knows what to expect. Grounded in the orchestration timeouts/poll intervals in
# core/cdc_deployer.py (connector creation polls up to 600s; stack ops ~15-20 min)
# and the live spike timings. These are ballpark hints, not guarantees -- actual
# time varies with AWS provisioning and table count. Stages absent from a kind's
# map (or estimated at 0) show no hint.
_CDC_STAGE_ETA_SECONDS = {
    "infra": {
        "check_existing": 5,
        "validate_params": 2,
        "create_stack": 10,
        "stack_create": 18 * 60,  # MSK Serverless provisioning dominates
        "infra_ready": 2,
    },
    "start": {
        # MSK Connect connector creation is slow: Fargate provisioning + plugin
        # download (our plugin zips are ~70-90 MiB) + Kafka Connect worker boot +
        # Glue Schema Registry connect, ~10-15 min EACH. Source and sink now deploy
        # IN PARALLEL (one pass, topics pre-created), so the connector-wait stages
        # are estimated at ONE connector's time (~max), not the sum of two.
        "discover_stack": 5,
        "validate_params": 2,
        "fetch_bootstrap": 5,
        "submit_connectors": 10,
        "stack_connectors": 13 * 60,     # CFN creates topics + both connectors (parallel)
        "connectors_running": 13 * 60,   # both reach RUNNING concurrently (~max)
        "pipeline_running": 5,
    },
    "stop": {
        "discover_stack": 5,
        "submit_stop": 10,
        "stack_stop": 2 * 60,
        "connectors_gone": 5,
    },
    "delete": {
        "discover_stack": 5,
        "submit_delete": 10,
        "stack_delete": 5 * 60,
        "cleanup_secret": 5,
        "deleted": 2,
    },
}


def _format_eta_hint(seconds: int) -> str:
    """Return a short '~N min'/'~N sec' ETA hint for a stage, or '' for trivial."""
    if seconds <= 0 or seconds < 10:
        return ""  # sub-10s stages aren't worth a hint
    if seconds < 60:
        return f"~{seconds}s"
    minutes = round(seconds / 60)
    return f"~{minutes} min"
_CDC_DEPLOY_STAGE_STYLE = {
    "PENDING": ("radio_button_unchecked", "grey"),
    "IN_PROGRESS": ("hourglass_top", "primary"),
    "DONE": ("check_circle", "positive"),
    "FAILED": ("error", "negative"),
}


def _deploy_total_duration(job) -> str:
    """Return the whole operation's wall-clock duration (e.g. '4m 12s'), or ''.

    Derived from the chunk timestamps: earliest ``started_at`` to latest
    ``finished_at`` across all stages. Returns '' when timings are unavailable
    (e.g. an interrupted-then-restored job with no reliable start/finish).
    """
    starts = [c.started_at for c in job.chunks if getattr(c, "started_at", None)]
    ends = [c.finished_at for c in job.chunks if getattr(c, "finished_at", None)]
    if not starts or not ends:
        return ""
    seconds = (max(ends) - min(starts)).total_seconds()
    return format_duration(seconds) if seconds >= 0 else ""


# Non-ASCII punctuation that some monospace fonts lack a glyph for (rendered as a
# tofu box in the deploy log). Mapped to safe ASCII equivalents at render time.
_LOG_GLYPH_FALLBACKS = {
    "…": "...",  # … horizontal ellipsis
    "—": "-",   # — em dash
    "–": "-",   # – en dash
    "‘": "'",   # ‘ left single quote
    "’": "'",   # ’ right single quote
    "“": '"',   # " left double quote
    "”": '"',   # " right double quote
    " ": " ",   # non-breaking space
}


def _ascii_log(message: str) -> str:
    """Replace non-ASCII punctuation in a deploy-log message with ASCII fallbacks.

    The deploy log renders in a monospace ``ui.code`` block; on some systems that
    font has no glyph for characters like the em-dash or ellipsis, so they show as
    a missing-glyph box. Substituting plain ASCII keeps the log readable everywhere
    without changing the meaning.
    """
    for fancy, plain in _LOG_GLYPH_FALLBACKS.items():
        if fancy in message:
            message = message.replace(fancy, plain)
    return message


def _migration_status_tables(migration_state, job_manager) -> list[str]:
    """The table names to show in the per-table migration-status view.

    Prefers the Full Load job's chunk ids (the tables actually migrated) -- the
    authoritative set across Full Load + CDC. But CDC commonly runs WITHOUT a Full
    Load job in this session (reconnected to an already-running pipeline, or a
    CDC-only run), so fall back to the tables reconciled from the live stack's config.
    Without this fallback the per-table view -- and its scan-free CDC metrics (net
    rows, stream lag, and the live lag chart, all scoped to this set) -- would be
    empty even while the pipeline is actively streaming. ``[]`` only when neither the
    job nor a reconciled set is known.
    """
    job = _current_job(job_manager, migration_state.job_id)
    if job is not None:
        return [c.chunk_id for c in job.chunks]
    return list(getattr(migration_state, "cdc_reconciled_table_names", []) or [])


def _single_int_pk_by_table(inventory, table_names) -> dict[str, str]:
    """Map each table to its single integer-PK column, for the watermark compare.

    Only single-column PKs qualify (a MAX over a composite key is meaningless for
    a high-water comparison); tables without one map to "" (skipped by the max-PK
    readers). Read from the source inventory's TableDef.primary_key. Pure.
    """
    out: dict[str, str] = {name: "" for name in table_names}
    if inventory is None:
        return out
    by_name = {t.name: t for t in getattr(inventory, "tables", [])}
    for name in table_names:
        tbl = by_name.get(name)
        pk = list(getattr(tbl, "primary_key", []) or []) if tbl is not None else []
        if len(pk) == 1:
            out[name] = pk[0]
    return out


def _fetch_migration_row_counts(migration_state, session, table_names, inventory=None):
    """Read source/target ``COUNT(*)`` and ``MAX(pk)`` per table (BLOCKING, read-only).

    Runs on a worker thread (the caller uses ``run.io_bound``). Source uses the
    same read-only MySQL engine the loader uses; target uses the DSQL IAM
    connector. The ``MAX(pk)`` per side is the stream high-water mark: comparing
    them tells whether CDC's leading edge has caught up (no lag) independently of
    the row COUNT, so a mid-stream gap is distinguishable from a lagging stream.
    Either side degrades to all-``None`` on failure so the view still renders.
    Returns ``(source_counts, target_counts, source_max_pk, target_max_pk, now,
    source_available)`` -- ``source_available`` is False when the source could not
    be read (no connection/password after a restore), so the UI can explain why
    the source columns are blank rather than showing a misleading dash.
    """
    from datetime import datetime, timezone

    source_counts: dict[str, Optional[int]] = {name: None for name in table_names}
    target_counts: dict[str, Optional[int]] = {name: None for name in table_names}
    source_max_pk: dict[str, Optional[int]] = {name: None for name in table_names}
    target_max_pk: dict[str, Optional[int]] = {name: None for name in table_names}
    pk_by_table = _single_int_pk_by_table(inventory, table_names)
    # Whether the source side could be read at all. Credentials are never persisted
    # (Property 7), so after a session restore the source connection/password may be
    # gone even though the target (IAM, no password) still works -- in that case the
    # source columns stay blank and the UI must say WHY (re-enter the source
    # connection), instead of silently showing a dash that looks like a bug.
    source_available = False
    # Source side (MySQL): a scan-free row ESTIMATE (information_schema) + an
    # index-only MAX(pk). Both are negligible-load even on large-scale tables, so the
    # consistency view never runs a COUNT(*) full scan against the live production
    # source. The estimate is a baseline (can drift under heavy writes); Validation
    # (Step 4) does the exact COUNT(*)/checksum reconciliation when needed.
    try:
        from sqlalchemy import text  # noqa: F401 - ensure SQLAlchemy present
        from dsql_migrator.core.watermark import estimate_source_rows, max_pk_source

        source_config = getattr(session, "source_config", None)
        has_password = getattr(session, "source_password", None) is not None
        if source_config is not None and session.has_source() and has_password:
            engine_factory = make_source_engine_factory(session.source_password)
            engine = engine_factory(source_config)
            with engine.connect() as connection:
                source_counts = estimate_source_rows(connection, list(table_names))
                source_max_pk = max_pk_source(connection, pk_by_table)
            source_available = True
    except Exception:  # noqa: BLE001 - read-only best effort; keep None on failure
        pass
    # Target side (DSQL): IAM connector, exact COUNT(*) + MAX(pk) per table.
    try:
        from dsql_migrator.core.target_introspector import max_pk_target

        target_config = getattr(session, "target_config", None)
        if target_config is not None and session.has_target():
            connector = DsqlConnector(
                target_config, aws_profile=getattr(session, "aws_profile", None)
            )
            target_counts = count_target_rows(
                list(table_names), connection_factory=connector.connect
            )
            target_max_pk = max_pk_target(
                pk_by_table, connection_factory=connector.connect
            )
    except Exception:  # noqa: BLE001 - read-only best effort
        pass
    return (
        source_counts,
        target_counts,
        source_max_pk,
        target_max_pk,
        datetime.now(timezone.utc),
        source_available,
    )


def _cdc_status_view(migration_state, job_manager) -> Optional[LoadStatusView]:
    """Return the current CDC :class:`LoadStatusView`, or ``None`` if unavailable.

    The view is rebuilt by the CDC poller from the controller's read-only signals
    and stored on the session-scoped ``migration_state`` (not on ``MigrationJob``,
    which is a durable ``extra="forbid"`` model). Until a controller is wired and
    a poll has run there is no signal, so the live panels are skipped. Defensive:
    any access error degrades to ``None`` so the screen still renders.
    """
    view = getattr(migration_state, "cdc_status_view", None)
    return view if isinstance(view, LoadStatusView) else None


def _filter_mine(raw_connectors, stack_name: str) -> list[str]:
    """Return only the connector names this tool's cdc-stack would create.

    Scopes region-wide ``list_connectors()`` output down to the two names a
    cdc-stack with ``stack_name`` deploys (``cdc_expected_connector_names``), so
    an unrelated connector in the same account/region is never mistaken for "my
    CDC". Returns names in data-flow order (source, then sink) for the ones found
    -- so a partial deploy (only the source up yet) is reported honestly. Pure.
    """
    expected = cdc_expected_connector_names(stack_name)
    present = {
        c.get("connectorName")
        for c in raw_connectors
        if c.get("connectorName")
    }
    return [name for name in expected if name in present]


def _running_mine(raw_connectors, stack_name: str) -> list[str]:
    """Return my detected connectors whose MSK ``connectorState`` is ``RUNNING``.

    MSK Connect reports a connector as ``CREATING``/``UPDATING`` for ~10-20 min
    while it provisions, then ``RUNNING`` once it is actually streaming. The
    lifecycle card uses this (vs :func:`_filter_mine`, which only checks
    existence) so a still-provisioning sink is shown as "Provisioning" rather than
    a misleading "Streaming". In data-flow order; pure (no AWS I/O of its own).
    """
    expected = cdc_expected_connector_names(stack_name)
    running = {
        c.get("connectorName")
        for c in raw_connectors
        if c.get("connectorName")
        and str(c.get("connectorState", "")).upper() == "RUNNING"
    }
    return [name for name in expected if name in running]


def _is_inflight_stack_status(status: Optional[str]) -> bool:
    """True when a CloudFormation StackStatus is a live, still-running operation.

    CloudFormation in-progress statuses all end in ``_IN_PROGRESS`` (e.g.
    ``CREATE_IN_PROGRESS``, ``UPDATE_ROLLBACK_IN_PROGRESS``). Everything else that
    is not a stable state is terminal-stuck (``ROLLBACK_FAILED``,
    ``ROLLBACK_COMPLETE``, ``UPDATE_ROLLBACK_FAILED``, ``DELETE_FAILED`` …): waiting
    will never clear it, so the user must delete the stack and retry.
    """
    return bool(status) and status.upper().endswith("_IN_PROGRESS")


def cdc_teardown_in_flight(
    job_manager: JobManager,
    *,
    teardown_job_id: Optional[str],
    deploy_job_id: Optional[str],
    action_kind: Optional[str],
    stack_status: Optional[str],
) -> bool:
    """Pure predicate: is a CDC teardown (stop/delete) currently running?

    Start over must not race an in-flight teardown -- resetting would fire a second
    background teardown and then wipe the session, hiding the running delete (and,
    for a custom stack name, leaving it unre-discoverable). Three signals, first
    match wins:

    (0) the **durable teardown marker** (``teardown_job_id``) is a PENDING/RUNNING
        job. This is set the instant the teardown is submitted and SURVIVES the
        Start-over session reset, so it closes the race window that (a)+(b) alone
        left open: right after Start over → delete, the local ``deploy_job_id`` is
        wiped by the reset and the CloudFormation stack has not yet flipped to
        ``DELETE_IN_PROGRESS``, so a second Start over used to slip through.
    (a) the local lifecycle job (``deploy_job_id``) is a PENDING/RUNNING stop/delete.
    (b) the freshly-probed ``stack_status`` is ``DELETE_IN_PROGRESS`` -- the only
        stack-level status that is an UNAMBIGUOUS teardown. It is deliberately NOT
        any ``*_IN_PROGRESS``: a Deploy (``CREATE_IN_PROGRESS``) or Start CDC
        (``UPDATE_IN_PROGRESS``) drives the SAME stack through a live status for
        ~minutes, and those must NOT hard-block Start over -- they only WARN via
        ``cdc_op_in_flight`` (blocking them would trap a user escaping a stuck run).
        An in-session stop is a benign ``UPDATE`` already covered by (0)/(a); its
        rare cross-session variant is short and low-harm, so not blocking it here is
        preferred over trapping a deploy/start. A settled-but-stuck stack
        (``ROLLBACK_COMPLETE`` / ``DELETE_FAILED``) is likewise NOT blocked -- the
        user should still be able to Start over and delete it.
    """
    tjob = _current_job(job_manager, teardown_job_id)
    if tjob is not None and getattr(tjob, "status", None) in ("PENDING", "RUNNING"):
        return True
    job = _current_job(job_manager, deploy_job_id)
    if (
        job is not None
        and getattr(job, "status", None) in ("PENDING", "RUNNING")
        and action_kind in ("stop", "delete")
    ):
        return True
    return bool(stack_status) and stack_status.upper() == "DELETE_IN_PROGRESS"


def should_replace_teardown_marker(
    job_manager: JobManager,
    current_job_id: Optional[str],
    new_job_id: Optional[str],
) -> bool:
    """Whether a newly submitted teardown may claim the single durable marker.

    The marker is one slot, so a second teardown must NOT clobber a DIFFERENT one
    still running: otherwise the banner switches to the wrong (e.g. shorter stop)
    job and, when THAT settles, ``clear_cdc_teardown`` wipes tracking of the still-
    running delete -- reopening the exact Start-over race the marker was added to
    close. Allow the write when there is no current marker, it is the SAME job, or
    the currently-tracked job has already settled / is unknown to the manager; refuse
    it only while a different tracked teardown is still PENDING/RUNNING (keep the
    first, longer-lived one). This needs two concurrent teardowns on one session
    (e.g. two browser tabs) -- rare; the un-tracked second targets the same stack and
    is redundant, so dropping its tracking is safe.
    """
    if not current_job_id or current_job_id == new_job_id:
        return True
    cur = _current_job(job_manager, current_job_id)
    return not (
        cur is not None and getattr(cur, "status", None) in ("PENDING", "RUNNING")
    )


def cdc_teardown_banner_state(
    job_manager: JobManager, teardown_job_id: Optional[str]
) -> Optional[str]:
    """State of the tracked teardown job, for the persistent banner:

    * ``"running"`` -- PENDING/RUNNING → show the in-progress banner.
    * ``"failed"``  -- FAILED/CANCELLED → show the actionable "teardown failed —
      retry cleanup" banner. The caller does NOT clear the marker (it persists so
      the user can retry or dismiss); MSK/NAT may still be billing.
    * ``None``      -- no marker, or the job is DONE / unknown to the manager (lost
      across a restart) → the caller clears the marker and hides the banner.

    Pure/read-only; the state-clearing side effect stays with the caller.
    """
    if not teardown_job_id:
        return None
    job = _current_job(job_manager, teardown_job_id)
    status = getattr(job, "status", None) if job is not None else None
    if status in ("PENDING", "RUNNING"):
        return "running"
    if status in ("FAILED", "CANCELLED"):
        return "failed"
    return None


def _classify_cdc_stack_phase(discovery) -> tuple[str, Optional[str]]:
    """Map a stack discovery (or None) to a lifecycle phase + raw status.

    Returns one of ``"absent"`` / ``"infra"`` / ``"running"`` / ``"unstable"``.
    ``"infra"`` means the stack is stable but ``MskBootstrapServers`` is blank (no
    connectors yet); ``"running"`` means it carries a bootstrap value (connectors
    deployed). Connector-level detection still overrides this to ``"running"`` in
    :func:`_ensure_cdc_controller`. Pure: no AWS.
    """
    if discovery is None:
        return "absent", None
    status = discovery.stack_status
    if not discovery.is_stable:
        return "unstable", status
    bootstrap = (discovery.current_parameters or {}).get("MskBootstrapServers", "")
    if bootstrap:
        return "running", status
    return "infra", status


def _probe_cdc_stack_phase(migration_state, session) -> None:
    """Probe the cdc-stack's lifecycle phase once and cache it on the state.

    Read-only ``cloudformation:describe_stacks`` via the deployer's non-raising
    ``describe_stack_or_none``, so an absent stack is a normal "Deploy" state, not
    an error. Best-effort: any failure leaves the phase unprobed (the card then
    defaults to the Deploy form). Only probes once per session unless re-triggered
    by a finished lifecycle job (which clears the cache via ``set_cdc_stack_phase``
    on refresh).
    """
    target = getattr(session, "target_config", None)
    region = getattr(target, "region", None) if target else None
    if not region:
        return
    try:
        from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer

        deployer = build_cdc_stack_deployer(
            region,
            aws_profile=getattr(session, "aws_profile", None),
            assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
        )
        mine = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
        discovery = deployer.describe_stack_or_none(mine)
        # Account-scoped discovery: OTHER mysql-dsql-cdc-* stacks this session does
        # not target. Lets the card offer to ADOPT an existing pipeline instead of
        # deploying a duplicate (a reset single-task session forgets which stack it
        # deployed). Best-effort (list_cdc_stacks returns [] on any read error).
        others = [(n, s) for (n, s) in deployer.list_cdc_stacks() if n != mine]
    except Exception:  # noqa: BLE001 - leave unprobed; card defaults to Deploy
        return
    phase, status = _classify_cdc_stack_phase(discovery)
    migration_state.set_cdc_stack_phase(phase, status=status)
    migration_state.set_cdc_other_stacks(others)
    # Reconcile the replicated table set from the live stack's TableIncludeList
    # (source connector's table.include.list = each table's ``.name``), so an
    # ADOPTED / out-of-band pipeline resolves its tables even when this session
    # holds no watermark or in-session selection. Empty when the stack is absent
    # or carries no such param (fresh infra) -- the normal config path then applies.
    includes_raw = (
        (getattr(discovery, "current_parameters", None) or {}).get("TableIncludeList", "")
        if discovery is not None
        else ""
    )
    migration_state.set_cdc_reconciled_table_names(
        [n for n in includes_raw.split(",") if n.strip()]
    )


def _ensure_cdc_controller(migration_state, session) -> None:
    """Wire the read-only MSK Connect controller, probe stack phase, detect connectors.

    Builds the controller from the session's target region + AWS profile and
    lists connectors, but only counts the ones THIS tool's cdc-stack would create
    (see :func:`_filter_mine`) -- so unrelated connectors in the same region do
    not make the CDC step look "already running". When none of mine are present
    (the normal pre-deploy state) ``cdc_connector_names`` stays empty, so the
    lifecycle card uses the probed stack phase (absent → Deploy, infra → Start).
    Best-effort and read-only: it never deploys anything.
    """
    # Throttle: this runs on every CDC-step render and makes blocking AWS reads
    # (describe_stacks + list_connectors) while mutating CDC state. Re-running it
    # on rapid re-renders (typing, refreshes, the deploy poller) is wasteful and
    # couples render latency to AWS. Skip if a discovery ran very recently; live
    # deploy/stop progress has its own poller, so 5s staleness here is harmless.
    import time as _time

    now = _time.monotonic()
    last = getattr(migration_state, "_cdc_discovery_monotonic", None)
    if last is not None and (now - last) < _CDC_DISCOVERY_THROTTLE_SECONDS:
        return
    migration_state._cdc_discovery_monotonic = now
    # Probe the stack phase (cheap describe_stacks) so the lifecycle card reflects
    # the latest state after a deploy/stop/delete completes.
    _probe_cdc_stack_phase(migration_state, session)

    if getattr(migration_state, "cdc_controller", None) is not None:
        # Controller already wired; just refresh connector detection so a Stop that
        # removed connectors flips the phase back off "running".
        controller = migration_state.cdc_controller
        stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
        try:
            raw = controller.list_connectors()
            names = _filter_mine(raw, stack)
            running = _running_mine(raw, stack)
        except Exception:  # noqa: BLE001
            names = list(getattr(migration_state, "cdc_connector_names", []) or [])
            running = list(
                getattr(migration_state, "cdc_connector_running_names", []) or []
            )
        migration_state.set_cdc_connector_names(names)
        migration_state.set_cdc_connector_running_names(running)
        return
    target = getattr(session, "target_config", None)
    if target is None:
        return
    try:
        from dsql_migrator.core.msk_connect_controller import (
            build_msk_connect_controller,
        )

        controller = build_msk_connect_controller(
            target.region, aws_profile=getattr(session, "aws_profile", None)
        )
        stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
        raw = controller.list_connectors()
        names = _filter_mine(raw, stack)
        running = _running_mine(raw, stack)
    except Exception:  # noqa: BLE001 - no controller is a valid (un-provisioned) state
        return
    # Store the controller regardless (the poller needs it after a later deploy);
    # `running` is driven by whether any of MY connectors are present, not by the
    # controller existing -- so an empty match keeps the Start action visible.
    migration_state.set_cdc_controller(controller)
    if not names:
        return
    migration_state.set_cdc_connector_names(names)
    migration_state.set_cdc_connector_running_names(running)
    # My connectors are deployed and detected -> the CDC step is underway. Advance
    # the workflow stepper to IN_PROGRESS once. CDC is continuous so it has no
    # terminal DONE here -- "stop" (delete) is a later explicit action.
    try:
        if get_status(session.workflow, WorkflowStep.CDC) is StepStatus.NOT_STARTED:
            session.set_workflow(
                with_status(
                    session.workflow, WorkflowStep.CDC, StepStatus.IN_PROGRESS
                )
            )
    except Exception:  # noqa: BLE001 - workflow advance is best-effort
        pass


# A change rate at or below this (events/sec) counts as "no changes flowing".
# Not 0: the source (Debezium) connector never fully goes silent even when the
# captured tables are idle -- heartbeat.interval.ms=300000 emits a heartbeat record
# every 5 min (~0.0033/s), which the CloudWatch moving average blips up to ~0.03/s,
# so SourceRecordPollRate has an irreducible floor. 0.1/s sits well above that
# heartbeat floor yet far below any real migration change traffic (typically >=1/s),
# so a drained pipeline reads as idle instead of lingering as "streaming". Idle still
# requires BOTH rates below this (see cdc_activity_summary), so a stalled sink (source
# still producing, sink not sending) is NOT mislabelled idle.
_CDC_IDLE_RATE_THRESHOLD = 0.1


@dataclass(frozen=True)
class CdcActivitySummary:
    """CDC throughput snapshot for the cutover 'no changes flowing' signal.

    ``source_poll_rate``/``sink_send_rate`` are the latest CloudWatch averages
    (events/sec) across the connectors; ``None`` when unknown (no datapoint /
    CloudWatch unreachable). ``idle`` is True ONLY when both rates are known AND
    ~0 -- an unknown rate yields ``idle=None`` (never asserted idle), so the UI
    never tells the operator "no changes flowing" when it actually cannot tell.
    """

    source_poll_rate: Optional[float] = None
    sink_send_rate: Optional[float] = None
    idle: Optional[bool] = None


def cdc_activity_summary(health: dict) -> CdcActivitySummary:
    """Summarize per-connector health into an aggregate activity signal (pure).

    Takes the max known rate across connectors for each metric (the worst-case
    "still flowing" signal). ``idle`` is True only when BOTH the source poll rate
    and the sink send rate are known and at/below the idle threshold; if either
    is unknown, ``idle`` is None (cannot confirm caught up). Pure: no AWS/NiceGUI.
    """
    polls = [h.poll_rate for h in health.values() if getattr(h, "poll_rate", None) is not None]
    sends = [h.send_rate for h in health.values() if getattr(h, "send_rate", None) is not None]
    source_poll = max(polls) if polls else None
    sink_send = max(sends) if sends else None
    if source_poll is None or sink_send is None:
        idle: Optional[bool] = None
    else:
        idle = source_poll <= _CDC_IDLE_RATE_THRESHOLD and sink_send <= _CDC_IDLE_RATE_THRESHOLD
    return CdcActivitySummary(
        source_poll_rate=source_poll, sink_send_rate=sink_send, idle=idle
    )


def _fetch_cdc_status(migration_state, tables=None):
    """Read connector state + CloudWatch task health (BLOCKING network I/O).

    Returns ``(statuses, health, dlq_errors, applied_ops, lag_ms, lag_series)`` from
    the duck-typed ``MskConnectController``, or ``None`` when there is nothing to read
    or the read fails. This is the only part of the CDC poll that touches the network,
    so the live poller runs it on a worker thread (``run.io_bound``) -- keeping the
    blocking calls off the NiceGUI event loop so the browser's WebSocket keep-alive
    is never starved. Pure read: it does not mutate ``migration_state`` (the apply
    step does that on the loop). ``tables`` (the migrated table set) scopes the
    scan-free per-table applied-ops metrics read; omitted (non-UI callers) -> skipped.
    """
    controller = getattr(migration_state, "cdc_controller", None)
    names = list(getattr(migration_state, "cdc_connector_names", []) or [])
    if controller is None or not names:
        return None
    try:
        statuses = controller.connector_statuses(names)
        health = controller.connector_health(names)
    except Exception:  # noqa: BLE001 - keep the last good view on any error
        return None
    # Best-effort: read the sink's per-table applied-ops (Inserts/Updates/Deletes) so
    # the per-table monitor shows CDC progress WITHOUT a source/target COUNT(*).
    # Only when the controller exposes the reader and we know the table set; never
    # fatal (the COUNT-based fallback still renders if this is empty).
    applied_ops: dict = {}
    stack = getattr(migration_state, "cdc_stack_name", None)
    ops_reader = getattr(controller, "applied_ops_by_table", None)
    if callable(ops_reader) and stack and tables:
        try:
            applied_ops = dict(ops_reader(stack, list(tables)) or {})
        except Exception:  # noqa: BLE001 - advisory, keep status even if it fails
            applied_ops = {}
    # Best-effort: read per-table end-to-end replication lag (ms) from the sink's
    # ReplicationLagMs metric -- a time-based, PK-agnostic lag for the "Stream lag"
    # column (accurate, unlike the MAX(pk) leading-edge fallback). Never fatal.
    lag_ms: dict = {}
    lag_reader = getattr(controller, "replication_lag_by_table", None)
    if callable(lag_reader) and stack and tables:
        try:
            lag_ms = dict(lag_reader(stack, list(tables)) or {})
        except Exception:  # noqa: BLE001 - advisory, keep status even if it fails
            lag_ms = {}
    # Best-effort: pipeline-wide replication-lag TIME SERIES (max across tables per
    # 1-minute bucket over the trailing window) for the "Stream lag over time" trend
    # chart. Reuses the same CloudWatch metric the per-table read fetches; never fatal.
    lag_series: list = []
    series_reader = getattr(controller, "replication_lag_series", None)
    if callable(series_reader) and stack and tables:
        try:
            lag_series = list(series_reader(stack, list(tables)) or [])
        except Exception:  # noqa: BLE001 - advisory, keep status even if it fails
            lag_series = []
    # Best-effort: pull NEW sink dead-letter events from the connector's
    # CloudWatch log group so the DLQ surface reflects the real pipeline (not just
    # in-tool errors). Only when the controller exposes the reader; never fatal.
    dlq_errors: list = []
    stack_name = getattr(migration_state, "cdc_stack_name", None)
    reader = getattr(controller, "dlq_errors", None)
    if callable(reader) and stack_name:
        try:
            dlq_errors = list(reader(f"/msk-connect/{stack_name}-cdc") or [])
        except Exception:  # noqa: BLE001 - advisory, keep status even if logs fail
            dlq_errors = []
    return statuses, health, dlq_errors, applied_ops, lag_ms, lag_series


def cdc_error_log_key(migration_state) -> str:
    """Return the error-log key the CDC stream records DLQ events under.

    The single error log is keyed by job id, but CDC commonly runs WITHOUT a Full
    Load job in this session (CDC-only, or a resumed stream), so
    ``migration_state.job_id`` is ``None`` -- and keying off ``"" `` would silently
    drop every quarantined record (the DLQ panel would always read empty even
    though CloudWatch has events). Fall back to a STABLE per-stack key
    (``"cdc:<stack>"``) so DLQ fold, depth, the record list, the activity-log
    lines, and the download all agree on one key whether or not a Full Load ran.
    """
    job_id = getattr(migration_state, "job_id", None)
    if job_id:
        return job_id
    stack = getattr(migration_state, "cdc_stack_name", None) or CDC_DEFAULT_STACK_NAME
    return f"cdc:{stack}"


def _apply_cdc_status(migration_state, fetched) -> None:
    """Build the CDC status view from a fetched ``(statuses, health)`` and store it.

    Pure/in-memory (no network): folds CloudWatch errored-task counts into the
    connector state (a connector MSK reports as RUNNING but with errored tasks is
    degraded -> FAILED) and writes the view + throughput onto ``migration_state``.
    Safe to call on the event loop after :func:`_fetch_cdc_status` returns.
    """
    if fetched is None:
        return
    statuses, health, *rest = fetched
    dlq_errors = rest[0] if rest else []
    applied_ops = rest[1] if len(rest) > 1 else {}
    lag_ms = rest[2] if len(rest) > 2 else {}
    lag_series = rest[3] if len(rest) > 3 else []
    # Store the scan-free per-table applied-ops (I/U/D) the sink reported so the
    # per-table monitor can show CDC progress without a COUNT(*). MERGE into the
    # last-known map rather than replace: the counts are cumulative (monotonic), and
    # a flaky/empty poll (CloudWatch throttle/timeout, or tables momentarily empty)
    # would otherwise blank the columns -> the "appears then disappears" flicker. So
    # only apply a NON-empty read, and keep prior per-table values for any table not
    # in this read. reset_in_place still clears the map on a genuine reset.
    setter = getattr(migration_state, "set_cdc_applied_ops_by_table", None)
    if callable(setter) and applied_ops:
        merged = dict(getattr(migration_state, "cdc_applied_ops_by_table", {}) or {})
        merged.update(applied_ops)
        setter(merged)
    # Store per-table replication lag (ms) for the time-based "Stream lag" column.
    lag_setter = getattr(migration_state, "set_cdc_replication_lag_by_table", None)
    if callable(lag_setter):
        lag_setter(lag_ms or {})
    # Append one live sample to the rolling lag series behind the live chart (hybrid:
    # seeded from the CloudWatch 1-min history, then extended each poll). current_ms =
    # current worst-across-tables lag; 0 when caught up (metric emits but no recent
    # datapoint); None when the metric is unavailable entirely (don't plot a fake 0).
    recorder = getattr(migration_state, "record_cdc_lag_sample", None)
    if callable(recorder):
        if lag_ms:
            current_ms = max(int(v) for v in lag_ms.values())
        elif lag_series or getattr(migration_state, "cdc_replication_lag_series", None):
            current_ms = 0
        else:
            current_ms = None
        recorder(
            current_ms=current_ms,
            now_epoch=int(datetime.now(timezone.utc).timestamp()),
            seed_series=lag_series or [],
        )
    adjusted = []
    for status in statuses:
        h = health.get(status.name)
        state = status.state
        tasks_failed = 0
        if h is not None and h.errored_tasks:
            tasks_failed = h.errored_tasks
            if state is ConnectorState.RUNNING and h.errored_tasks > 0:
                state = ConnectorState.FAILED
        adjusted.append(
            ConnectorStatus(
                name=status.name,
                state=state,
                tasks_total=(h.running_tasks or 0) + tasks_failed if h else 0,
                tasks_failed=tasks_failed,
            )
        )
    # CDC records under a stable key even when no Full Load job exists this session
    # (see cdc_error_log_key) -- otherwise an empty job_id would drop every DLQ event.
    job_id = cdc_error_log_key(migration_state)
    # Fold any newly-read DLQ events into the single error log so DLQ depth and the
    # per-table "Quarantined" count reflect the real pipeline. Reuse the CDC
    # orchestrator's CdcConnectorError -> DataErrorRecord conversion (credential-free).
    if dlq_errors and job_id:
        from dsql_migrator.core.cdc import CdcPipelineOrchestrator

        CdcPipelineOrchestrator(error_source=lambda: dlq_errors).surface_errors(
            job_id, migration_state.error_log
        )
        # Also record each NEW DLQ event in the durable activity-log file, so a
        # quarantine is auditable outside the live UI (the file persists; the UI's
        # in-memory error log does not). The controller's dlq_errors() already
        # cursor-dedups, so what reaches here is the new-since-last-poll set -- one
        # FAILURE line per quarantined record, credential-free (table + SQLSTATE +
        # reason; no row values or SQL).
        from dsql_migrator.core.activity_log import (
            ActivityCategory,
            ActivityStatus,
            log_activity,
        )

        for err in dlq_errors:
            log_activity(
                ActivityCategory.CDC,
                "quarantine record to DLQ",
                status=ActivityStatus.FAILURE,
                target=getattr(err, "table", None),
                error_code=getattr(err, "error_code", None),
                detail=getattr(err, "message", None),
            )
    error_summary = migration_state.error_log.summary(job_id)
    # DLQ depth: MSK Connect publishes no DLQ-topic depth metric, so we use the
    # quarantined-record count from the single error log (the rows the sink set
    # aside as poison) as the depth signal. That is exactly the "what did not reach
    # the target" the DLQ panel reports, and it is data we already have -- so the
    # panel populates without a separate Kafka consumer-lag read. It is 0 (not None)
    # on a clean stream, so the panel always renders once streaming ("0 quarantined")
    # rather than silently disappearing.
    dlq_depth = error_summary.total_errors if error_summary is not None else 0
    view = build_cdc_status_view(adjusted, error_summary, dlq_depth=dlq_depth)
    migration_state.set_cdc_status_view(view)
    # Aggregate throughput so the UI can show "no changes flowing" (cutover
    # signal) without re-deriving it on every render.
    migration_state.set_cdc_activity(cdc_activity_summary(health))


def _refresh_cdc_status(migration_state) -> None:
    """Synchronous fetch + apply of the CDC status view (read-only, best-effort).

    A thin wrapper over :func:`_fetch_cdc_status` + :func:`_apply_cdc_status`. The
    live UI poller calls those two separately (fetch on a worker thread, apply on
    the loop); this combined form is kept for non-UI/test callers.
    """
    _apply_cdc_status(migration_state, _fetch_cdc_status(migration_state))


# Tailwind tone -> (border, background, icon-color, icon) for the health cards.
_CDC_TONE_STYLE = {
    "ok": ("border-green-200", "bg-green-50", "positive", "check_circle"),
    "warn": ("border-amber-300", "bg-amber-50", "warning", "warning"),
    "bad": ("border-red-300", "bg-red-50", "negative", "error"),
    "alarm": ("border-red-300", "bg-red-50", "negative", "error"),
}
