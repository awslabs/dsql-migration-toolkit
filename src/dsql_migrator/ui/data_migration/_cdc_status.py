# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC status / controller / deploy-formatting logic + shared job & error-log views (NiceGUI-free).

The heavy CDC status, network-probe, classification, teardown-planning, and
deploy-stage *formatting* logic lives here, apart from the NiceGUI render code.
Everything here is pure or read-only network I/O; nothing builds NiceGUI widgets.

Two things here are NOT CDC-specific but live here by COHESION, not by feature:

- ``_current_job`` -- the shared :class:`~dsql_migrator.core.job_manager.JobManager`
  snapshot accessor. Its primary user is this module (the teardown/status logic); the
  rest of the package imports it from here.
- The error-log *partitioning* views. Full Load and CDC share one error log, so
  ``full_load_error_records`` (the Full Load's own records) and ``cdc_dlq_records``
  (CDC's own) are two sides of the SAME split and both depend on
  ``is_cdc_error_record``. Keeping them together is what makes the Full Load error
  panel and the CDC DLQ panel add up to the whole log; splitting the FL readers into
  the FL engine would fragment that and drag a CDC predicate into the FL module.

The package ``__init__`` re-imports these names so the public import surface of
``dsql_migrator.ui.data_migration`` is unchanged and the render code resolves them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Optional, Sequence

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
# core/cdc_deployer.py (connector creation polls up to 600s; infra create ~10-15 min)
# and the live spike timings. These are ballpark hints, not guarantees -- actual
# time varies with AWS provisioning and table count. Stages absent from a kind's
# map (or estimated at 0) show no hint.
_CDC_STAGE_ETA_SECONDS = {
    "infra": {
        # The first two stages were previously unestimated, so the total ETA
        # under-reported the wait. They upload the three bundled artifacts
        # (Debezium ~31 MiB + DSQL sink ~11 MiB + seeder Lambda ~1 MiB); already
        # up-to-date objects are skipped, so this is the cold-start estimate.
        "ensure_bucket": 10,
        "upload_plugins": 60,
        "check_existing": 5,
        "validate_params": 2,
        "create_stack": 10,
        # MSK Serverless provisioning dominates. Lowered from 18 min after repeated
        # live runs finished the whole infra create in ~10 min (the per-stage hints
        # and the total ETA now match the ~10-15 min the deploy dialog states); it is
        # a ballpark, so a slower AWS run simply overruns the hint rather than misleads.
        "stack_create": 9 * 60,
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


def _refresh_pg_slot_health(migration_state, source_config, connection) -> None:
    """Read the PostgreSQL replication slot's WAL health onto ``migration_state``.

    PostgreSQL-only and best-effort: reuses the already-open read-only source
    ``connection`` to read ``pg_replication_slots`` for the deterministic slot name of
    this session's CDC stack, and stores the :class:`SlotHealth` (or None) via
    :meth:`set_cdc_slot_health`. A MySQL source (dialect returns None) or any failure
    leaves it None. Never raises -- it must not disturb the row-count read.
    """
    try:
        from dsql_migrator.core.models import SourceType

        if getattr(source_config, "source_type", None) is not SourceType.POSTGRES:
            migration_state.set_cdc_slot_health(None)
            return
        from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME
        from dsql_migrator.core.cdc_pg_slot import pg_slot_name
        from dsql_migrator.core.source_dialect import dialect_for

        stack_name = getattr(migration_state, "cdc_stack_name", None) or CDC_DEFAULT_STACK_NAME
        dialect = dialect_for(SourceType.POSTGRES)
        health = dialect.read_replication_slot_health(
            connection, pg_slot_name(stack_name)
        )
        migration_state.set_cdc_slot_health(health)
    except Exception:  # noqa: BLE001 - best-effort; never disturb the counts read
        pass


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
            # Thread the SOURCE-engine dialect so the scan-free estimate + max-PK reads use
            # PostgreSQL SQL (pg_class.reltuples, double-quoted idents) for a PG source, not
            # the MySQL default (information_schema.table_rows, backticks). Without it these
            # reads raise on PG and the broad `except` below silently returns None -- the
            # CDC-convergence monitor goes blind and could green-light a premature cutover.
            from dsql_migrator.core.source_dialect import dialect_for

            dialect = dialect_for(source_config.source_type)
            engine_factory = make_source_engine_factory(session.source_password)
            engine = engine_factory(source_config)
            with engine.connect() as connection:
                source_counts = estimate_source_rows(connection, list(table_names), dialect)
                source_max_pk = max_pk_source(connection, pk_by_table, dialect)
                # PostgreSQL CDC only: piggyback the source read-only connection to read
                # the replication slot's WAL-retention health (a cheap pg_replication_slots
                # SELECT), so the monitor can warn about WAL pressure before the source
                # disk fills. Best-effort; MySQL's dialect returns None (no slot).
                _refresh_pg_slot_health(migration_state, source_config, connection)
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


# CloudFormation statuses a discovered cdc-stack must NOT be offered for "attach".
# Attaching points the session at a stack and re-reads its live state, so it only makes
# sense for a stack that is (or can become) a working pipeline. A half-deleted or
# rolled-back stack is the opposite: its resources are partly gone, so streaming from it
# is impossible -- and offering "Attach" hides the real problem, which is that MSK/NAT
# may still be BILLING with no session tracking it. These need cleanup, not adoption.
_UNATTACHABLE_STACK_STATUSES = frozenset(
    {
        "DELETE_FAILED",
        "DELETE_IN_PROGRESS",
        "ROLLBACK_COMPLETE",
        "ROLLBACK_FAILED",
        "ROLLBACK_IN_PROGRESS",
        "CREATE_FAILED",
        "UPDATE_ROLLBACK_FAILED",
        "UPDATE_ROLLBACK_COMPLETE",
        "UPDATE_ROLLBACK_IN_PROGRESS",
    }
)


def stack_status_needs_cleanup(status: Optional[str]) -> bool:
    """True when a cdc-stack is in a state that leaves resources needing cleanup.

    A teardown JOB finishing is not the same as the STACK being gone: a delete can end
    in ``DELETE_FAILED`` (typically leftover Lambda ENIs pinning the subnets), and a
    job record can vanish with an app restart. In both cases the cross-view banner used
    to go silent while the leftover Amazon MSK / NAT kept BILLING with nothing in the UI
    saying so. This lets the banner stay on the (cached) stack status instead of only
    the job's. Pure.
    """
    return bool(status) and str(status).upper() in _UNATTACHABLE_STACK_STATUSES


def teardown_stack_confirmed_gone(deployer, stack_name: Optional[str]) -> bool:
    """True ONLY when CloudFormation DEFINITIVELY reports the stack does not exist.

    Self-heal for the stale "CDC teardown failed" banner: after a DELETE_FAILED the
    marker is kept (job record / cached status both frozen at failed), so if the
    operator finishes the cleanup out of band (e.g. terminates the ENI-pinning
    bastion and re-runs delete-stack from the CLI) nothing in the UI ever re-reads
    the live stack, and the banner lingers forever.

    Reuses the existing read-only ``describe_stack_or_none`` probe (the same
    ``cloudformation:DescribeStacks`` read behind the restored-session re-verify
    notice), which returns ``None`` only for a true does-not-exist and RAISES on any
    unexpected/permission/throttle error. This helper is deliberately conservative:
    it returns ``True`` (safe to clear the failed banner) ONLY on that definitive
    ``None``. A stack still present in ANY state (including ``DELETE_FAILED``), a
    missing deployer/stack name, or ANY error -> ``False`` (keep the banner), so a
    real, still-billing failure is never wrongly cleared. Pure aside from the one
    injected read.
    """
    if deployer is None or not stack_name:
        return False
    try:
        return deployer.describe_stack_or_none(stack_name) is None
    except Exception:  # noqa: BLE001 - any ambiguous/errored read keeps the banner
        return False


def cdc_attach_scope_mismatch(
    streamed_tables: "Sequence[str]", loaded_tables: "Sequence[str]"
) -> "list[str]":
    """Return the loaded tables a candidate stack does NOT stream, sorted; ``[]`` if fine.

    Attaching points the session at a live pipeline and, because the pipeline is streaming,
    promotes Data Migration to ``DONE`` and unlocks Validation. That is only sound when the
    pipeline actually covers what this session loaded. A stack streaming a DIFFERENT table
    set (e.g. ``ecommerce_demo.*`` while this session loaded ``ecommerce.*``) would leave
    every loaded table with no CDC at all -- silently losing every source change after the
    watermark -- while the UI reported the migration as complete and let the operator
    proceed to cut over.

    Asymmetric on purpose. Tables the stack streams but this session did NOT load are NOT
    a mismatch: the pipeline is simply broader (it may serve another table set in parallel),
    and nothing this session owns is left uncovered. Only the reverse -- a loaded table the
    pipeline ignores -- is a real gap.

    Returns ``[]`` when either side is unknown (an un-probed stack has no
    ``TableIncludeList``; a session with no confirmed selection has nothing to check), so
    this only blocks a mismatch it can actually prove. Comparison is case-insensitive on
    the qualified ``schema.table`` name, matching how the connector's
    ``table.include.list`` is written.
    """
    streamed = {str(name).strip().lower() for name in streamed_tables if str(name).strip()}
    loaded = [str(name).strip() for name in loaded_tables if str(name).strip()]
    if not streamed or not loaded:
        return []
    return sorted(name for name in loaded if name.lower() not in streamed)


def cdc_teardown_stack_names(
    *,
    own_stack_name: "Optional[str]",
    stack_phase: "Optional[str]",
    connector_names: "Sequence[str]" = (),
    other_stacks: "Sequence[tuple[str, str]]" = (),
) -> "list[str]":
    """The cdc-stack name(s) a Start-over teardown would act on, in order.

    One list, used by BOTH the offer and the teardown, so the two can never disagree
    about which stacks exist. Start over previously resolved a SINGLE name and adopted a
    discovered stack only when there was exactly one: with two or more it fell back to
    this session's own name, which in that branch does not exist -- so the dialog offered
    "Delete all CDC infrastructure", the delete found nothing, and the operator was left
    with MSK / NAT billing and a success toast. Returning every name it would really
    touch removes that whole class of mismatch and lets the tiles NAME them, which is
    the only way an operator can tell a pipeline they must not delete from one they can.

    ``own_stack_name`` is included when this session actually targets a live stack --
    i.e. the probe found a non-``absent`` phase, or connectors exist under that name.
    Discovered stacks under other names (``other_stacks``) follow, de-duplicated and
    order-stable so the UI listing and the teardown iterate identically. Pure.
    """
    names: list[str] = []
    own = (own_stack_name or "").strip()
    own_is_live = (stack_phase is not None and stack_phase != "absent") or bool(
        [n for n in connector_names if n]
    )
    if own and own_is_live:
        names.append(own)
    for name, _status in other_stacks:
        candidate = str(name).strip()
        if candidate and candidate not in names:
            names.append(candidate)
    return names


def next_unfinished_teardown(
    job_manager: JobManager, queue: "Sequence[tuple[str, str]]"
) -> "Optional[tuple[str, str, int, int]]":
    """The teardown the banner should follow, as ``(job_id, stack, index, total)``.

    ``queue`` is every ``(job_id, stack)`` one Start-over teardown launched, in order.
    Returns the FIRST entry still PENDING/RUNNING, with its 1-based position and the
    queue length so the banner can say "2 of 3" -- or ``None`` when every entry has
    settled (the caller then clears the marker).

    Needed because the durable marker is a single slot: it was claimed by the first stack
    and never re-pointed, so the banner vanished as soon as that stack finished even
    though the others were still deleting -- MSK / NAT still billing, nothing on screen.
    A job unknown to the manager (record pruned / lost across a restart) counts as
    settled, matching how the single-stack path already treats it.

    Pure apart from the JobManager status reads.
    """
    entries = [(str(j), str(s)) for j, s in (queue or []) if j]
    total = len(entries)
    for index, (job_id, stack) in enumerate(entries, start=1):
        job = _current_job(job_manager, job_id)
        if job is not None and getattr(job, "status", None) in ("PENDING", "RUNNING"):
            return job_id, stack, index, total
    return None


def teardown_queue_progress(
    queue: "Sequence[tuple[str, str]]", job_id: "Optional[str]"
) -> "Optional[tuple[int, int]]":
    """``(position, total)`` of ``job_id`` in a multi-stack teardown queue, else ``None``.

    Returns ``None`` for a single-stack teardown (nothing to disambiguate) or a job that is
    not in the queue. The banner names ONE stack, so without this a multi-stack teardown
    read as if that stack were the only one -- and appeared to finish early while the rest
    were still deleting and still billing. Pure.
    """
    entries = [(str(j), str(s)) for j, s in (queue or []) if j]
    if len(entries) <= 1 or not job_id:
        return None
    for index, (queued_job, _stack) in enumerate(entries, start=1):
        if queued_job == str(job_id):
            return index, len(entries)
    return None


def finished_teardown_stacks(
    queue: "Sequence[tuple[str, str]]", tracked_stack: "Optional[str]"
) -> "list[str]":
    """Every stack a just-finished teardown covered, for the completion notice.

    Prefers the full queue (a multi-stack teardown surfaced only the tracked stack, so the
    notice would have understated what was torn down) and falls back to the single tracked
    stack. Pure.
    """
    from_queue = [str(s) for _job, s in (queue or []) if s]
    if from_queue:
        return from_queue
    return [str(tracked_stack)] if tracked_stack else []


def cdc_teardown_plan(
    stack_names: "Sequence[str]", *, cleanup_secret: bool
) -> "list[tuple[str, bool]]":
    """Per-stack teardown plan: ``[(stack_name, cleanup_secret), ...]``.

    One entry per stack the offer covered -- tearing down only the first would contradict
    the dialog, which now LISTS the stacks by name for the operator to confirm.

    ``cleanup_secret`` is applied to the FIRST stack only. The tool-managed source
    credentials secret is shared across stacks (it is created out-of-band, so
    CloudFormation cannot own it); scheduling its deletion once per stack would retry a
    delete on an already-scheduled secret for every extra stack. Pure.
    """
    plan: list[tuple[str, bool]] = []
    for name in stack_names:
        cleaned = str(name).strip()
        if not cleaned:
            continue
        # Index into the KEPT entries, not the raw input: a leading blank/whitespace name
        # would otherwise consume position 0 and the shared secret cleanup would be
        # dropped entirely (nothing else ever schedules it, so the source credentials
        # would linger in Secrets Manager).
        plan.append((cleaned, cleanup_secret and not plan))
    return plan


def split_attachable_stacks(
    stacks: "Sequence[tuple[str, str]]",
) -> "tuple[list[tuple[str, str]], list[tuple[str, str]]]":
    """Split discovered cdc-stacks into ``(attachable, needs_cleanup)``.

    ``needs_cleanup`` holds stacks in a failed/rolled-back/deleting state
    (:data:`_UNATTACHABLE_STACK_STATUSES`). Offering "Attach to <stack>" for those was
    actively harmful: a ``DELETE_FAILED`` stack cannot stream (its resources are partly
    gone), so attaching produces a dead session -- while the real, urgent fact is that
    its MSK / NAT may still be billing after a teardown that did not finish. Pure.
    """
    attachable: list[tuple[str, str]] = []
    needs_cleanup: list[tuple[str, str]] = []
    for name, status in stacks:
        target = (
            needs_cleanup
            if str(status).upper() in _UNATTACHABLE_STACK_STATUSES
            else attachable
        )
        target.append((name, status))
    return attachable, needs_cleanup


def _is_inflight_stack_status(status: Optional[str]) -> bool:
    """True when a CloudFormation StackStatus is a live, still-running operation.

    CloudFormation in-progress statuses all end in ``_IN_PROGRESS`` (e.g.
    ``CREATE_IN_PROGRESS``, ``UPDATE_ROLLBACK_IN_PROGRESS``). Everything else that
    is not a stable state is terminal-stuck (``ROLLBACK_FAILED``,
    ``ROLLBACK_COMPLETE``, ``UPDATE_ROLLBACK_FAILED``, ``DELETE_FAILED`` …): waiting
    will never clear it, so the user must delete the stack and retry.
    """
    return bool(status) and status.upper().endswith("_IN_PROGRESS")


def is_infra_create_stack_status(status: Optional[str]) -> bool:
    """True when the stack status is the CDC *infrastructure* create in flight.

    Only the first deploy uses ``create_stack``
    (:func:`~dsql_migrator.core.cdc_deployer.run_cdc_infra_deploy`); every later
    connector operation (Start / Stop CDC) goes through ``submit_update``, so it
    reports ``UPDATE_IN_PROGRESS``. That makes ``CREATE_IN_PROGRESS`` an unambiguous
    "MSK/networking is being provisioned, no connector exists yet" signal.

    Used to keep the ~15-20 min infra create from reading as a live *migration*
    operation: during it nothing streams and no Full Load is running, so the
    prerequisite checks are exactly what the user should be doing (the generic
    ``_is_inflight_stack_status`` would disable them for the whole create). Pure.
    """
    return bool(status) and status.upper() == "CREATE_IN_PROGRESS"


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


def cdc_has_committed_offset(parameters: "Optional[Mapping[str, str]]") -> bool:
    """True when this cdc-stack has ALREADY streamed, so its resume offset exists.

    Start CDC does not need a watermark in that case: the source connector's compacted
    offsets topic (pinned to a FIXED name, ``<stack>-debezium-source-offsets``, so it is
    not a per-instance UUID topic) survives a Stop -- a Stop only blanks
    ``MskBootstrapServers``, which deletes the two connectors and leaves MSK, the topics
    and the seeder Lambda in place. On the next Start the seeder reads that offset and
    SKIPS the seed when it is at/past the watermark (``seeder.py`` no-clobber guard), so
    streaming resumes exactly where it stopped. A watermark is only ever needed for the
    FIRST start, when the offsets topic is empty and the connector would otherwise begin
    at the source's current binlog and silently lose everything since the Full Load.

    The signal is ``DeploySink == "true"`` while the bootstrap is blank, and it is
    unambiguous because of how the two writes differ: the infra create pins
    ``DeploySink="false"``, only Start CDC sets it to ``"true"``, and Stop overrides ONLY
    ``MskBootstrapServers`` (everything else is carried forward as
    ``UsePreviousValue``). So that combination is reachable only by "started, then
    stopped" -- never by a fresh infra-only deploy.

    Pure (reads already-fetched CloudFormation parameters); ``None``/missing -> False, so
    an unreadable probe falls back to requiring a start point rather than claiming a
    resume point that may not exist.
    """
    params = parameters or {}
    bootstrap = str(params.get("MskBootstrapServers", "") or "").strip()
    deploy_sink = str(params.get("DeploySink", "") or "").strip().lower()
    return not bootstrap and deploy_sink == "true"


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
    # Has this stack streamed before? If so its resume offset is already committed to
    # the (fixed-name, Stop-surviving) offsets topic, so Start CDC needs no watermark --
    # see cdc_has_committed_offset. Read from the SAME describe as the phase so the two
    # can never disagree about the stack they describe.
    migration_state.set_cdc_has_committed_offset(
        cdc_has_committed_offset(getattr(discovery, "current_parameters", None))
    )
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
    # Also read each ATTACH CANDIDATE's replicated table set, so the attach offer can be
    # withheld when the pipeline does not cover what this session loaded. ``list_stacks``
    # returns no parameters, so this needs one describe per candidate -- bounded by the
    # number of cdc-* stacks in the account (normally 0-2) and only on the throttled
    # discovery, not per render. Best-effort per stack: a candidate whose parameters
    # cannot be read maps to an empty set, which the scope check treats as "unknown" and
    # therefore does NOT block.
    candidate_tables: "dict[str, list[str]]" = {}
    for name, _status in others:
        try:
            candidate = deployer.describe_stack_or_none(name)
        except Exception:  # noqa: BLE001 - one unreadable candidate must not break others
            candidate = None
        raw = (
            (getattr(candidate, "current_parameters", None) or {}).get(
                "TableIncludeList", ""
            )
            if candidate is not None
            else ""
        )
        candidate_tables[name] = [n.strip() for n in raw.split(",") if n.strip()]
    migration_state.set_cdc_other_stack_tables(candidate_tables)


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
        # This is the path a Stop/Delete lands on (the controller is already wired), so
        # it is where the step status has to be able to go back DOWN.
        _sync_cdc_step_status(session, streaming=bool(names))
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
        # No connectors of mine: this is also how a Stop/Delete reads once it lands,
        # so the step status must follow (see _sync_cdc_step_status).
        _sync_cdc_step_status(session, streaming=False)
        return
    migration_state.set_cdc_connector_names(names)
    migration_state.set_cdc_connector_running_names(running)
    _sync_cdc_step_status(session, streaming=True)


def _sync_cdc_step_status(session, *, streaming: bool) -> None:
    """Track the CDC workflow step to whether MY connectors currently exist.

    Promotion used to be one-way: detected connectors moved the step
    NOT_STARTED -> IN_PROGRESS and nothing ever moved it back, so after a Stop CDC (or
    a full infrastructure Delete) the Data Migration badge kept reading "CDC:
    IN_PROGRESS" for a pipeline with no connectors at all -- and because the workflow
    is persisted, that stale value came back on every restore. The badge's own contract
    is that it moves BETWEEN NOT_STARTED and IN_PROGRESS, which needs both directions.

    CDC has no terminal DONE (it is continuous replication that ends only by an
    explicit Stop/Delete), so the honest resting state after a teardown is NOT_STARTED:
    nothing is streaming, and Start CDC is the action on offer again.

    Only ever moves between those two values -- a FAILED or DONE recorded elsewhere is
    left alone rather than being overwritten by a routine discovery pass. Best-effort:
    a workflow write must never break a render.
    """
    want = StepStatus.IN_PROGRESS if streaming else StepStatus.NOT_STARTED
    try:
        current = get_status(session.workflow, WorkflowStep.CDC)
        if current is want:
            return
        # Downgrade only from the status this function itself sets; never clobber a
        # FAILED/DONE that some other path recorded deliberately.
        if not streaming and current is not StepStatus.IN_PROGRESS:
            return
        if streaming and current is not StepStatus.NOT_STARTED:
            return
        session.set_workflow(with_status(session.workflow, WorkflowStep.CDC, want))
    except Exception:  # noqa: BLE001 - workflow sync is best-effort
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

# Consecutive polls the source-flowing-but-sink-silent divergence must hold before it
# is reported as a stall. The CDC poll runs every ~5s (_CDC_POLL_INTERVAL_SECONDS), so
# 3 polls is ~10-15s of sustained divergence. Why persistence is required: both rates
# are CloudWatch averages over a trailing window, so the moment a burst of writes ends
# the source still shows its residual while the sink has correctly gone quiet -- a
# one-poll divergence that is indistinguishable from a real stall. A real stall is
# permanent (an ejected consumer never rejoins by itself), so waiting a few polls loses
# no detection while removing the false alarm.
_SINK_STALL_CONFIRM_POLLS = 3


@dataclass(frozen=True)
class CdcActivitySummary:
    """CDC throughput snapshot for the cutover 'no changes flowing' signal.

    ``source_poll_rate``/``sink_send_rate`` are the latest CloudWatch averages
    (events/sec) across the connectors; ``None`` when unknown (no datapoint /
    CloudWatch unreachable). ``idle`` is True ONLY when both rates are known AND
    ~0 -- an unknown rate yields ``idle=None`` (never asserted idle), so the UI
    never tells the operator "no changes flowing" when it actually cannot tell.

    ``sink_stalled`` is the DIVERGENCE signal for THIS poll: the source is producing
    real change traffic while the sink applies nothing. It is deliberately a separate
    field from ``idle`` rather than a change to it -- ``idle`` gates the cut-over
    "drained" judgement and must keep reading False here (a stalled pipeline is not
    drained), which is exactly why the stall was invisible: not-idle renders as the
    reassuring "Streaming -- changes are flowing".

    ``sink_stall_confirmed`` is what the UI acts on: the divergence seen on
    :data:`_SINK_STALL_CONFIRM_POLLS` CONSECUTIVE polls. A single-poll divergence is
    NOT a stall -- both rates are CloudWatch averages over a trailing 5-minute window,
    and just after a burst of writes finishes the source still carries its residual
    (plus a Debezium heartbeat every 5 min, which is why the idle threshold is 0.1 and
    not 0) while the sink has legitimately gone quiet with nothing left to apply. That
    transient looks identical to a real stall for a poll or two, and it fired a false
    "Sink stalled" alarm on a healthy pipeline during demo testing. A REAL stall is
    permanent (an ejected consumer does not rejoin on its own), so requiring
    persistence costs nothing in detection and removes the false positive.
    """

    source_poll_rate: Optional[float] = None
    sink_send_rate: Optional[float] = None
    idle: Optional[bool] = None
    sink_stalled: Optional[bool] = None
    sink_stall_confirmed: bool = False
    # Consecutive polls the divergence has held, carried forward by _apply_cdc_status.
    sink_stall_polls: int = 0


def cdc_activity_summary(
    health: dict, *, previous: Optional[CdcActivitySummary] = None
) -> CdcActivitySummary:
    """Summarize per-connector health into an aggregate activity signal (pure).

    Takes the max known rate across connectors for each metric (the worst-case
    "still flowing" signal). ``idle`` is True only when BOTH the source poll rate
    and the sink send rate are known and at/below the idle threshold; if either
    is unknown, ``idle`` is None (cannot confirm caught up). Pure: no AWS/NiceGUI.

    ``sink_stalled`` inverts that pairing: True when the source is above the
    threshold (real changes are being produced) and the sink is at/below it (nothing
    is being applied). That combination is the signature of a sink that has stopped
    writing to DSQL -- e.g. a consumer ejected from its group, which keeps the
    connector at RUNNING with no errored task, so no other signal here catches it.
    Both rates must be known; otherwise it is None (never asserted).

    ``previous`` (the prior poll's summary) carries the consecutive-divergence count
    forward, so ``sink_stall_confirmed`` only becomes True once the divergence has held
    for :data:`_SINK_STALL_CONFIRM_POLLS` polls. Pass ``None`` for the first poll. The
    count resets the moment the sink sends anything -- and also when a rate becomes
    unknown, because an unreadable metric is not evidence of a stall.
    """
    polls = [h.poll_rate for h in health.values() if getattr(h, "poll_rate", None) is not None]
    sends = [h.send_rate for h in health.values() if getattr(h, "send_rate", None) is not None]
    source_poll = max(polls) if polls else None
    sink_send = max(sends) if sends else None
    if source_poll is None or sink_send is None:
        idle: Optional[bool] = None
        sink_stalled: Optional[bool] = None
    else:
        idle = source_poll <= _CDC_IDLE_RATE_THRESHOLD and sink_send <= _CDC_IDLE_RATE_THRESHOLD
        sink_stalled = (
            source_poll > _CDC_IDLE_RATE_THRESHOLD
            and sink_send <= _CDC_IDLE_RATE_THRESHOLD
        )
    # Only a TRUE divergence extends the streak; False or None (unknown rate) clears it.
    prior_polls = getattr(previous, "sink_stall_polls", 0) or 0
    streak = prior_polls + 1 if sink_stalled else 0
    return CdcActivitySummary(
        source_poll_rate=source_poll,
        sink_send_rate=sink_send,
        idle=idle,
        sink_stalled=sink_stalled,
        sink_stall_polls=streak,
        sink_stall_confirmed=streak >= _SINK_STALL_CONFIRM_POLLS,
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
        # Scope the applied-ops window to events applied SINCE the Full Load watermark
        # (this migration's gapless resume point), so ops from PRIOR CDC runs still
        # inside the sink metric's long trailing window are excluded -- a clean
        # Full-Load->CDC run with no post-watermark writes then reads 0. Falls back to
        # the reader's default window when the start is unknown.
        _ops_kwargs: dict = {}
        _since = getattr(migration_state, "cdc_ops_window_start", None)
        if _since is not None:
            try:
                _elapsed = (datetime.now(timezone.utc) - _since).total_seconds()
                _ops_kwargs["window_seconds"] = int(
                    max(60, min(_elapsed, 14 * 24 * 3600))
                )
            except Exception:  # noqa: BLE001 - fall back to the default window
                _ops_kwargs = {}
        try:
            applied_ops = dict(ops_reader(stack, list(tables), **_ops_kwargs) or {})
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


def is_cdc_error_record(record) -> bool:
    """True when this error record came from the CDC stream, not the Full Load.

    ``cdc_error_log_key`` returns the Full Load ``job_id`` whenever one exists, so both
    sources share ONE error-log key -- which put Full Load quarantines inside the
    "Dead-letter queue (poison records)" card. That is wrong three ways: Full Load has
    no DLQ (its batch loader sets a row aside; no message ever reaches a broker), the
    card's copy then claims those rows were "isolated to the DLQ (the pipeline keeps
    running)" when no pipeline existed at the time, and -- worst -- a user who has just
    excluded an oversized column sees a non-zero count and concludes the exclusion
    failed, when a zero CDC count is exactly the proof that it worked.

    The discriminator already exists in the data: the Full Load writers set
    ``chunk_id`` to the table name (``_engine.py``), while CDC's ``surface_errors``
    (``core/cdc.py``) never sets it. So ``chunk_id is None`` means "not from the Full
    Load". Keyed on that rather than on a timestamp cut-off: a "since CDC started"
    filter depends on a start time that a restored session loses, and Full Load
    quarantines minutes before a CDC start would be indistinguishable anyway.

    Pure; a record whose ``chunk_id`` is absent entirely counts as CDC.
    """
    return getattr(record, "chunk_id", None) is None


def cdc_dlq_records(migration_state, log_key: str) -> list:
    """Read the error log for ``log_key`` and keep only the CDC-sourced records.

    The one place the CDC/Full-Load split is applied. Every DLQ surface -- the depth
    badge, the per-table chips, the record list and the download -- must go through
    this, because they all read the same key: filtering one of them alone would make
    the count disagree with the rows beneath it.

    Memoized on the log's append-only record COUNT: the CDC poll and the screen
    re-render call this 4-5 times per ~5 s tick, and each call used to copy the whole
    (uncapped, growing) error log and re-run the per-record CDC/Full-Load predicate --
    O(records) work repeated 4-5x every tick, growing unbounded exactly during a drift
    storm when the log is largest. Because the log is append-only, an unchanged count
    means an unchanged view, so the expensive copy+filter runs only when a NEW record
    arrived; a genuine reset drops the count to 0 and invalidates the cache. The
    returned list is a shared READ-ONLY view (all callers iterate it or ``sorted()`` a
    copy -- none mutate it in place).

    Best-effort: an unreadable log yields ``[]`` rather than breaking the panel.
    """
    if not log_key:
        return []
    error_log = getattr(migration_state, "error_log", None)
    if error_log is None:
        return []
    try:
        count = error_log.count(log_key)
    except Exception:  # noqa: BLE001 - advisory list; never break the panel
        return []
    cache = getattr(migration_state, "_cdc_dlq_records_cache", None)
    if cache is not None and cache[0] == log_key and cache[1] == count:
        return cache[2]
    try:
        records = error_log.records(log_key)
    except Exception:  # noqa: BLE001 - advisory list; never break the panel
        return []
    filtered = [r for r in records or () if is_cdc_error_record(r)]
    try:
        migration_state._cdc_dlq_records_cache = (log_key, count, filtered)
    except Exception:  # noqa: BLE001 - caching is best-effort; correctness unaffected
        pass
    return filtered


def full_load_error_records(error_log, job_id: str) -> list:
    """Read ``job_id``'s error records and keep only the Full Load's own.

    The mirror of :func:`cdc_dlq_records`, and needed for the same reason: CDC records
    under the Full Load's ``job_id`` whenever one ran (see :func:`cdc_error_log_key`),
    so an unfiltered read here counts DEAD-LETTERED rows as Full Load failures. That
    inflates "Download Full Load error log (N errors)" and puts CDC rows in the file --
    the same defect as the DLQ card, pointing the other way, and it reads at cut-over as
    "the Full Load lost N rows" when it did not.

    Filtering both directions is what makes the two screens add up to the whole log.

    Best-effort: an unreadable log yields ``[]`` rather than breaking the panel.
    """
    if not job_id:
        return []
    try:
        records = error_log.records(job_id)
    except Exception:  # noqa: BLE001 - advisory; never break the panel
        return []
    return [r for r in records or () if not is_cdc_error_record(r)]


def full_load_error_summary(error_log, job_id: str):
    """Summarize ONLY the Full Load's own error records under ``job_id``.

    Same shape as ``ErrorLogStore.summary`` so it drops into the existing Full Load
    consumers (count badge, per-table rows, download label). See
    :func:`full_load_error_records`.
    """
    from dsql_migrator.core.models import ErrorLogSummary

    records = full_load_error_records(error_log, job_id)
    by_table: dict[str, int] = {}
    for record in records:
        by_table[record.table] = by_table.get(record.table, 0) + 1
    return ErrorLogSummary(
        total_errors=len(records),
        errors_by_table=by_table,
        log_available=bool(records),
    )


def full_load_latest_messages(error_log, job_id: str) -> dict:
    """Latest message per table, from the Full Load's own records only.

    Drives the per-table failure reason in the Full Load table; without the filter a
    dead-lettered CDC row could supply the "why" for a table the Full Load loaded fine.
    """
    messages: dict[str, str] = {}
    for record in full_load_error_records(error_log, job_id):
        messages[record.table] = record.message
    return messages


def cdc_dlq_summary(migration_state, log_key: str):
    """Summarize ONLY the CDC-sourced records under ``log_key``.

    Same shape as ``ErrorLogStore.summary`` (total + per-table counts +
    ``log_available``) so it drops into the DLQ panel's existing consumers, but built
    from :func:`cdc_dlq_records` so Full Load quarantines never inflate the DLQ depth
    or add a table chip. See :func:`is_cdc_error_record` for why they were mixed.
    """
    from dsql_migrator.core.models import ErrorLogSummary

    records = cdc_dlq_records(migration_state, log_key)
    by_table: dict[str, int] = {}
    for record in records:
        by_table[record.table] = by_table.get(record.table, 0) + 1
    return ErrorLogSummary(
        total_errors=len(records),
        errors_by_table=by_table,
        log_available=bool(records),
    )


def cdc_schema_drift_summary(migration_state, log_key: str) -> list:
    """Group the CDC DLQ records that reveal a source schema change.

    Reads the SAME CDC-filtered records as :func:`cdc_dlq_summary` (so the drift
    banner and the DLQ depth agree), classifies each record's SQLSTATE via
    :func:`~dsql_migrator.core.cdc.classify_schema_drift`, and returns one
    :class:`SchemaDriftSummary` per (table, kind) that had at least one drift
    record. Ordinary poison rows (no drift kind) are skipped, so an empty list
    means "no source DDL detected". Built from the whole error log (not the
    per-poll delta) so the count is cumulative and survives a restored session.
    Best-effort and pure; never raises into the poller.
    """
    from dsql_migrator.core.cdc import classify_schema_drift
    from dsql_migrator.core.models import SchemaDriftSummary

    counts: dict[tuple[str, str], int] = {}
    for record in cdc_dlq_records(migration_state, log_key):
        kind = classify_schema_drift(getattr(record, "error_code", None))
        if kind is None:
            continue
        key = (record.table, kind.value)
        counts[key] = counts.get(key, 0) + 1
    return [
        SchemaDriftSummary(table=table, kind=kind, count=count)
        for (table, kind), count in sorted(counts.items())
    ]


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
    # CDC-sourced records ONLY. The key is shared with the Full Load (it IS the Full
    # Load job id whenever one ran), so an unfiltered summary put batch-loader
    # quarantines into the DLQ card -- see is_cdc_error_record.
    error_summary = cdc_dlq_summary(migration_state, job_id)
    # DLQ depth: MSK Connect publishes no DLQ-topic depth metric, so we use the
    # quarantined-record count from the single error log (the rows the sink set
    # aside as poison) as the depth signal. That is exactly the "what did not reach
    # the target" the DLQ panel reports, and it is data we already have -- so the
    # panel populates without a separate Kafka consumer-lag read. It is 0 (not None)
    # on a clean stream, so the panel always renders once streaming ("0 quarantined")
    # rather than silently disappearing.
    dlq_depth = error_summary.total_errors if error_summary is not None else 0
    # Classify the CDC DLQ into source-schema-drift groups (empty when none), so the
    # monitor can flag "the source ran DDL the target hasn't caught up to" instead of
    # an opaque quarantine count. Same records as the depth badge (cdc_dlq_records).
    schema_drift = cdc_schema_drift_summary(migration_state, job_id)
    view = build_cdc_status_view(
        adjusted, error_summary, dlq_depth=dlq_depth, schema_drift=schema_drift
    )
    migration_state.set_cdc_status_view(view)
    # Aggregate throughput so the UI can show "no changes flowing" (cutover
    # signal) without re-deriving it on every render. The prior summary is passed in
    # so the consecutive-divergence streak behind sink_stall_confirmed carries over.
    previous = getattr(migration_state, "cdc_activity", None)
    activity = cdc_activity_summary(health, previous=previous)
    migration_state.set_cdc_activity(activity)
    _log_sink_stall_transition(previous, activity)


def _log_sink_stall_transition(previous, current) -> None:
    """Record a sink stall (and its recovery) in the durable activity log.

    Only on a TRANSITION: the CDC poll runs every few seconds, so logging the state
    itself would write hundreds of identical lines. The event matters because the
    stall is otherwise invisible outside the live screen -- the connector stays
    RUNNING with no errored task, so nothing else in the log would show that
    replication stopped, and the activity-log file survives the task that produced it.

    Keys off ``sink_stall_confirmed``, not the raw per-poll ``sink_stalled``: a
    single-poll divergence is a normal post-burst artefact of the trailing-window
    rates, and logging it would put a FAILURE line in the durable log for a healthy
    pipeline (which is exactly what happened before the confirm threshold existed).

    Best-effort: a logging failure must never break the status poll.
    """
    was = bool(getattr(previous, "sink_stall_confirmed", False))
    now = bool(getattr(current, "sink_stall_confirmed", False))
    if was == now:
        return
    from dsql_migrator.core.activity_log import (
        ActivityCategory,
        ActivityStatus,
        log_activity,
    )

    source_rate = getattr(current, "source_poll_rate", None)
    sink_rate = getattr(current, "sink_send_rate", None)
    rates = (
        f"source {source_rate:.2f} rec/s, sink {sink_rate:.2f} rec/s"
        if source_rate is not None and sink_rate is not None
        else "rates unavailable"
    )
    try:
        if now:
            log_activity(
                ActivityCategory.CDC,
                "sink stalled",
                status=ActivityStatus.FAILURE,
                detail=(
                    "The source is producing changes but the sink has applied none "
                    f"({rates}), so changes are not reaching DSQL. The connector can "
                    "still report RUNNING with no errored task while this happens. Do "
                    "not cut over until the sink send rate recovers."
                ),
            )
        else:
            log_activity(
                ActivityCategory.CDC,
                "sink recovered",
                status=ActivityStatus.SUCCESS,
                detail=f"The sink is applying changes again ({rates}).",
            )
    except Exception:  # pragma: no cover - monitoring must never break the poll
        pass


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


def cdc_discovery_fingerprint(migration_state) -> tuple:
    """Snapshot the state CDC discovery can change, for change detection.

    Discovery runs ~0.05s after the Data Migration screen renders, reads AWS on a
    worker thread, and used to call the screen's full ``refresh()`` unconditionally
    when it finished. That rebuilds every widget -- including Start Full Load -- so a
    click landing in the window between render and refresh went to an element that no
    longer existed and was silently dropped. The operator saw a button that "only
    works on the second press", on every revisit, even though discovery had found
    nothing new (the common case: the same stack, the same connectors).

    Comparing this fingerprint before and after lets the caller skip the rebuild when
    nothing actually changed, while still refreshing when it did -- so the duplicate-MSK
    guard (adopt an existing pipeline instead of deploying a second, billable cluster)
    still appears as soon as discovery reports it.

    Covers every field ``_ensure_cdc_controller`` / ``_probe_cdc_stack_phase`` write:
    controller presence, connector names, running-connector names, stack phase, the
    other-stacks list, and whether the phase probe has reported. Presence (not identity)
    for the controller: it is rebuilt on each probe, so comparing the object would
    always look changed.
    """
    return (
        getattr(migration_state, "cdc_controller", None) is not None,
        tuple(getattr(migration_state, "cdc_connector_names", ()) or ()),
        tuple(getattr(migration_state, "cdc_connector_running_names", ()) or ()),
        getattr(migration_state, "cdc_stack_phase", None),
        tuple(
            tuple(entry) if isinstance(entry, (list, tuple)) else entry
            for entry in (getattr(migration_state, "cdc_other_stacks", ()) or ())
        ),
        bool(getattr(migration_state, "cdc_stack_phase_checked", False)),
    )
