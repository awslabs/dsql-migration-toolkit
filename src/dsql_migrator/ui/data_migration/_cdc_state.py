# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure CDC state / phase predicates (extracted from ``_cdc_ui.py``).

Side-effect-free duck-typed predicates over the migration state + job manager that
gate the CDC UI: whether the pipeline is live / streaming-started, whether an infra
deploy or a teardown is in flight, whether the monitoring surfaces should render, and
the shared poll-interval constant. Pure (only ``getattr`` + ``_current_job``), so they
are unit-tested directly and importable by both ``_cdc_ui`` and ``_cdc_monitoring``
without a cycle. One-directional: imports only stdlib + ``_status`` (never ``_cdc_ui``).
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.ui.data_migration._status import _current_job

# How often the CDC step polls MSK Connect + the DSQL target for live status.
# Slower than the Full Load poll: these are network round-trips to AWS/DSQL, and
# connector state / replication lag change on the order of seconds, not 0.5s.
_CDC_POLL_INTERVAL_SECONDS = 5.0


def _cdc_is_streaming(migration_state) -> bool:
    """True when a CDC controller + connector names are wired (pipeline is live).

    The single gate for arming the ~5 s CDC poll timers: both the live-status
    region and the per-table net-rows table re-render on this cadence only while
    streaming, and stay static (manual refresh only) otherwise. Duck-typed so the
    UI double and a bare state object both work.
    """
    return getattr(migration_state, "cdc_controller", None) is not None and bool(
        getattr(migration_state, "cdc_connector_names", []) or []
    )


def cdc_pipeline_live(migration_state) -> bool:
    """True only when CDC connectors actually exist and are streaming.

    The narrow, no-false-positive signal: connectors have been detected (a live
    controller with connector names), or the cdc-stack phase is ``running``. Unlike
    :func:`cdc_streaming_started`, an *in-flight start job* does NOT count here --
    the connectors are still coming up (~10-20 min on MSK Connect) and no row has
    reached the target yet.

    Use this for anything that asserts CDC is DONE / data has flowed (e.g. promoting
    the Data Migration step, which drives the "Success" badge and unlocks
    Validation). Use :func:`cdc_streaming_started` instead for "the start point is
    committed, stop editing inputs", which must latch the moment Start is pressed.
    """
    controller = getattr(migration_state, "cdc_controller", None)
    names = getattr(migration_state, "cdc_connector_names", []) or []
    if controller is not None and names:
        return True  # connectors detected -> streaming
    return getattr(migration_state, "cdc_stack_phase", None) == "running"


def cdc_infra_deploy_in_flight(migration_state, job_manager) -> bool:
    """True while this session's cdc-stack CREATE job is PENDING/RUNNING.

    Deliberately DISTINCT from :func:`cdc_streaming_started`, which excludes an ``infra``
    job: that predicate answers "are CDC's inputs committed / is anything streaming?"
    (it gates promoting the step to DONE, which an infra create must never do). This one
    answers "is a long, billable provisioning run under way?" -- the question the
    migration-type lock and the oversized-LOB exclusion lock both need, since
    ``ColumnExcludeList`` is baked into the stack at create time and the create's only
    progress/teardown controls live on the CDC sub-step.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if getattr(migration_state, "cdc_action_kind", None) != "infra":
        return False
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    return job is not None and job.status in ("PENDING", "RUNNING")


def cdc_streaming_started(migration_state, job_manager) -> bool:
    """True once CDC has been started, so its inputs must no longer change.

    "Started" means the pipeline is live (:func:`cdc_pipeline_live` -- detected
    connectors or cdc-stack phase ``running``), or a **connector-level** CDC
    lifecycle job (start/stop/delete) is in flight. After this point the start
    position is already seeded into the MSK connect-offsets topic and the table set
    is fixed by the running source connector, so editing the CDC start point or the
    table selection would have no effect on the live pipeline and only mislead the
    operator. Mirrors the "running" detection in :func:`_render_cdc_start_action`.
    Read-only/best-effort.

    This latches earlier than :func:`cdc_pipeline_live` on purpose: an in-flight
    connector start job counts, because the inputs are already committed even though
    the connectors have not reached RUNNING yet. It is therefore the wrong signal
    for "data has arrived" -- use :func:`cdc_pipeline_live` for that (promoting the
    Data Migration step / the "Success" badge).

    An in-flight ``kind="infra"`` job is deliberately EXCLUDED: the infrastructure
    create (``create_stack``: MSK Serverless, networking, plugins, IAM) makes no
    connectors -- the template gates both on ``HasBootstrapServers``, which the
    infra pass leaves blank -- so nothing is streaming and no offset is seeded for
    the ~15-20 min it runs. Treating it as "streaming" made an infra deploy
    masquerade as a live pipeline: it promoted the Data Migration step to DONE
    (unlocking Validation with zero rows loaded, and never downgrading), disabled
    Start Full Load, froze the table picker, and silently turned a "Drop & reload"
    re-run into an append. Excluding it is also what lets the ~15-20 min MSK create
    overlap the Full Load instead of serializing after it.
    """
    if cdc_pipeline_live(migration_state):
        return True
    # Only a connector-level lifecycle job counts (start/stop/delete); an "infra"
    # create touches no connector, so it must not read as streaming.
    if getattr(migration_state, "cdc_action_kind", None) == "infra":
        return False
    deploy_job = _current_job(
        job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
    )
    return deploy_job is not None and deploy_job.status in ("PENDING", "RUNNING")


def cdc_teardown_badge(migration_state, job_manager) -> Optional[tuple[str, str]]:
    """``(badge_text, color)`` while a teardown is in flight, else ``None``.

    The CDC pipeline card derives its badge from the live connector phase, and a
    teardown does not remove the connectors instantly: CloudFormation is still
    deleting them, so discovery keeps reporting both as RUNNING and
    :func:`classify_cdc_card_phase` keeps returning ``"running"``. Because the badge
    chain tested ``phase == "running"`` FIRST, pressing "Delete CDC infrastructure"
    left a green **"Streaming"** pinned next to a card whose body already said
    "Deleting infrastructure" -- two contradictory verdicts, and the reassuring one
    was the wrong one (nothing will be streaming shortly).

    A teardown is therefore checked BEFORE the phase: the operator's own committed
    action outranks a connector state that is only true for another minute or two.
    Names which teardown it is, since Stop CDC (connectors only, MSK kept) and Delete
    infrastructure (everything) leave very different systems behind.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    kind = getattr(migration_state, "cdc_action_kind", None)
    if kind not in ("delete", "stop"):
        return None
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    if job is None or job.status not in ("PENDING", "RUNNING"):
        return None
    # "primary" (not positive/warning): a teardown the operator asked for is a normal
    # in-progress operation, not a fault.
    return ("Deleting…", "primary") if kind == "delete" else ("Stopping…", "primary")


def cdc_monitoring_visible(migration_state, job_manager) -> bool:
    """Whether the CDC monitoring surfaces should render at all.

    One predicate for the three views that only make sense against a live pipeline --
    **Live status** (connector health + stream-lag chart), the **dead-letter queue**
    panel nested inside it, and the **per-table migration status** table. They read the
    same signals, so a divergent gate would leave the screen self-contradictory.

    Two conditions, both required:

    * CDC has STARTED (:func:`cdc_streaming_started`) -- true from the moment Start CDC
      is pressed, so the views are present through the connectors' ~10-20 min ramp,
      which is exactly when the operator wants to watch them come up.
    * No teardown is in flight (:func:`cdc_teardown_badge`). This is the gap: a Delete
      (or Stop) does not remove the connectors instantly, so discovery keeps reporting
      them and ``cdc_streaming_started`` stays true for the whole ~20 min teardown --
      leaving a live stream-lag chart, connector health, and per-table replication
      figures on screen for a pipeline being dismantled. Those numbers are about to
      become meaningless, and the delete progress is what the operator needs instead.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if not cdc_streaming_started(migration_state, job_manager):
        return False
    return cdc_teardown_badge(migration_state, job_manager) is None
