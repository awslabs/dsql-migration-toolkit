# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-session Data Migration state (NiceGUI-free).

The two per-session STATE classes for the Data Migration step live here so the
package ``__init__`` keeps only the NiceGUI screen + render helpers. These classes
hold the session's table selection, prerequisite reports, CDC lifecycle/monitoring
state, the downloadable error log, the current job id, and the last failure
message; the live migration progress itself lives in the
:class:`~dsql_migrator.core.job_manager.JobManager` keyed by ``job_id``.

These classes are intentionally NiceGUI-free (no ``ui`` param, no widget
building) so they can be unit tested directly. ``DataMigrationState`` is
re-exported from the package ``__init__`` so the public import surface is
unchanged.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Optional, Sequence

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_STACK_NAME,
    CdcResumePoint,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationMode,
    PrerequisiteReport,
    TableSelection,
)

from dsql_migrator.ui.data_migration._models import MigrationType
from dsql_migrator.ui.data_migration._status import CdcActivitySummary


class DataMigrationState:
    """Per-session Data Migration sub-flow state (selection, prereqs, job, errors).

    The live migration state (chunks, watermark, progress) lives in the
    :class:`~dsql_migrator.core.job_manager.JobManager` keyed by ``job_id``; this
    holds the session's table selection, the per-mode prerequisite reports, the
    downloadable error log, the current ``job_id``, and the last failure message.
    Mutated by the UI poller/handlers and read on render, so it is guarded by a
    lock (mirroring the sibling step screens).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Optional bound SessionConnectionState. When set (via bind_session), the
        # migration type and the CDC infra inputs are written THROUGH to the
        # session, which is the authoritative store now that the mode is chosen
        # on the Data Migration step. The local fields below remain as a
        # fallback for tests / call sites that construct a state without a session.
        self._session: object = None
        self.job_id: Optional[str] = None
        self._error: Optional[str] = None
        # The migration type selected when _error was recorded, so the screen can tell
        # a live failure from one carried over across a type switch. None = unknown.
        self._error_migration_type: "Optional[MigrationType]" = None
        # Multi-table selection; empty selected_tables => all (inferred default).
        self.selection: TableSelection = TableSelection()
        # Whether the user has explicitly changed the table picker. Until then
        # the picker shows the pre-selected default (all generated-DDL tables);
        # once touched, an explicit empty selection means "no tables selected".
        self.selection_touched: bool = False
        # Whether the user has accepted permanently-quarantined rows (e.g. values
        # over DSQL's ~1 MiB per-value limit) so a quarantine-only incomplete Full
        # Load no longer blocks CDC. Set explicitly via "Accept quarantined rows";
        # carried into re-runs so the engine completes a quarantine-only run.
        self.accept_quarantined_rows: bool = False
        # Active sub-step of the Prerequisites -> Full Load -> CDC stepper. Held
        # here so it survives the content re-render driven by the progress poller
        # (None => derive a sensible default from the current job/prereq state).
        self.active_substep: Optional[str] = None
        # Last sub-step the screen actually rendered as "active". Used only to
        # detect a real sub-step transition (e.g. Full Load finishing -> CDC
        # opens) so the screen can bring the stepper back into view exactly once
        # on that change -- without yanking the page on the routine progress-poll
        # re-renders that keep the same active sub-step. UI-only; not persisted.
        self.last_rendered_substep: Optional[str] = None
        # Migration type the user selected (Full load only / CDC only / both).
        # Determines which prerequisite mode is checked and which stepper
        # sub-steps are shown; defaults to Full load only. Exposed via the
        # ``migration_type`` property which reads through to a bound session when
        # present (the session is the authoritative store).
        self._migration_type: MigrationType = MigrationType.FULL_LOAD_ONLY
        # Single per-session downloadable error log for Full Load + CDC errors
        # (single error path -- Req 13.2 / Property 15).
        self.error_log: ErrorLogStore = ErrorLogStore()
        # Last prerequisite report per mode (set when the user runs checks).
        self._prereq_reports: dict[MigrationMode, PrerequisiteReport] = {}
        # The LOB exclusion each mode's report was checked against, so the run guard
        # can block a load whose exclusion changed after the checks (the exclusion
        # analogue of a late table add caught by prereq_scope_gap). Keyed by mode.
        self._prereq_report_lob_exclusions: dict[
            MigrationMode, dict[str, frozenset[str]]
        ] = {}
        # The prerequisite MODE that actually gated the most recent started Full
        # Load. The reports themselves are deliberately not persisted (a restored
        # connection can't be trusted), so ``full_load_run_guard_reason`` excuses an
        # absent report once a run exists -- that is what lets a reconnected user
        # re-run a finished load. But the excuse must be scoped to the mode that
        # cleared the gate: a Full-load-only run passed only the FULL_LOAD checks,
        # so switching the type to add CDC afterwards must NOT inherit that pass
        # (the CDC-only checks -- binlog ROW/FULL, replication grants -- were never
        # run). Persisted so the distinction survives a restart.
        self._prereq_gated_mode: Optional[MigrationMode] = None
        # Modes whose prerequisite checks are currently running (transient, in
        # memory only -- drives the immediate "checking..." feedback so the
        # button does not appear unresponsive during the read-only checks).
        self._prereq_running: set[MigrationMode] = set()
        # Selected target tables that already contain rows (computed by a
        # read-only probe when the user clicks Start). Drives the Start-dialog
        # choice below; NOT itself the drop set.
        self._tables_with_data: frozenset[str] = frozenset()
        # Target PK per selected table, cached from the SAME pre-dialog probe that
        # fills _tables_with_data (one target round trip, not one per render). A value
        # of None means "probed, could not read" -- distinct from an absent key, which
        # means "not probed". Both are treated conservatively downstream.
        self._target_primary_keys: dict[str, Optional[list[str]]] = {}
        # The user's run-wide choice for those pre-existing tables: "append"
        # (keep existing rows, load only the missing ones -- idempotent
        # SKIP_EXISTING, the non-destructive default) or "drop" (DROP+recreate
        # each first, for a clean fresh load). Stored so a retry / per-table
        # Reload follows the SAME choice instead of silently reverting to append.
        self._reload_mode: str = "append"
        # One-shot flag: open the Full Load confirm dialog on the next render
        # (set after the async non-empty-target check completes).
        self.pending_full_load_confirm: bool = False
        # Oversized-LOB columns the user opted to EXCLUDE from the migration (H13),
        # keyed by table name -> set of column names. Empty => exclude nothing.
        # A SINGLE, migration-wide selection: it drops the column from BOTH the Full
        # Load INSERT list (the exporter/importer derive columns from the effective
        # TableDef) AND CDC capture (the connector template's ColumnExcludeList /
        # Debezium column.exclude.list). One source of truth keeps Full Load and CDC
        # in lockstep -- a column excluded from one but not the other would leave
        # silent partial data across the gapless handoff. An explicit, opt-in choice
        # (no silent data loss); nothing is excluded unless the user ticks it.
        self._excluded_lob_columns: dict[str, set[str]] = {}
        # Composite-PK record-key override for CDC, keyed by db.table -> target key
        # columns [leading, pk...]. Set from the applied Schema Conversion when a
        # table's DSQL target has a composite key, so Debezium re-keys the change
        # record (message.key.columns) to match the target's ON CONFLICT/DELETE key
        # -- no sink change needed. Empty => every table keys on its source PK.
        self._cdc_message_key_columns: dict[str, list[str]] = {}
        # How the operator chose the CDC start point: "auto" (gapless from the
        # Full Load watermark) or "manual" (an explicit GTID / binlog position).
        # An explicit mode (vs. inferring it from whether an override is set)
        # lets the UI show a clear radio choice and keep "Manual + empty inputs"
        # distinct from "Automatic".
        self._cdc_start_mode: str = "auto"
        # Manual CDC start-position override the operator entered (a GTID set or
        # a binlog file:position). Only used when ``_cdc_start_mode == "manual"``.
        self._cdc_start_gtid: Optional[str] = None
        self._cdc_start_binlog_file: Optional[str] = None
        self._cdc_start_binlog_pos: Optional[int] = None
        # Live CDC monitoring (Phase 2). The status view is rebuilt by the CDC
        # poller from the controller's read-only signals; the controller talks to
        # the already-deployed MSK Connect connectors (decision-change 8 -- the UI
        # never deploys them). All transient/session-only -- not on MigrationJob.
        self.cdc_status_view: Optional[LoadStatusView] = None
        self.cdc_controller: Optional[object] = None  # MskConnectController
        self.cdc_connector_names: list[str] = []
        # Subset of cdc_connector_names whose MSK connectorState is RUNNING (vs
        # still CREATING/UPDATING). Lets the lifecycle card distinguish "deployed
        # and streaming" from "still provisioning" so a CREATING sink is not
        # mislabeled "Streaming". Transient/session-only (re-derived on discovery).
        self.cdc_connector_running_names: list[str] = []
        # Table set reconciled from the live cdc-stack's ``TableIncludeList`` param
        # (the source connector's table.include.list = each table's ``.name``). This
        # lets an ADOPTED / out-of-band pipeline -- one this session did not itself
        # Full-Load + Start (e.g. after a session reset + "Attach to <stack>") --
        # still resolve WHICH tables are being replicated, so the CDC config preview,
        # the "select a table" guard, and the per-table status reflect the running
        # pipeline instead of showing "no tables selected". Populated by the phase
        # probe; empty when no stack / no params. Transient/session-only.
        self.cdc_reconciled_table_names: list[str] = []
        self.cdc_activity: Optional["CdcActivitySummary"] = None
        # Per-table source/target row counts for the migration-status view, fetched
        # on demand (a direct COUNT(*) on each side -- read-only but adds source
        # load, so it is an explicit "Refresh counts" action, not an auto-poll).
        # name -> count (or None when the table is absent/erroring on that side).
        self.row_count_source: dict[str, Optional[int]] = {}
        self.row_count_target: dict[str, Optional[int]] = {}
        # Per-table MAX(single-int PK) on each side: the stream high-water mark for
        # the CDC "is it behind?" check (independent of row count).
        self.row_max_pk_source: dict[str, Optional[int]] = {}
        self.row_max_pk_target: dict[str, Optional[int]] = {}
        self.row_counts_fetched_at: Optional[datetime] = None
        # Per-table applied-ops the CDC sink reports since it started streaming, from
        # its ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` CloudWatch
        # metrics. Refreshed every CDC poll -- scan-free (no source/target COUNT), so
        # it drives the "Changes since Full Load" (I/U/D) column cheaply. name ->
        # {"inserts","updates","deletes"}; empty when the metrics are unavailable
        # (older plugin / sink not emitting).
        self.cdc_applied_ops_by_table: dict[str, dict[str, int]] = {}
        # Per-table end-to-end replication lag (milliseconds) from the sink's
        # ``ReplicationLagMs`` CloudWatch metric (apply time minus source commit time).
        # Refreshed every CDC poll; drives the time-based "Stream lag" column (a far
        # more accurate signal than the MAX(pk) leading-edge fallback). name -> ms;
        # empty/absent when the metric is unavailable (older plugin) or the table is
        # idle/caught up.
        self.cdc_replication_lag_by_table: dict[str, int] = {}
        # Rolling replication-lag series backing the live "Stream lag" chart:
        # [(epoch_seconds, max_lag_ms), ...], bounded to ~15 min. Hybrid — seeded once
        # from CloudWatch's 1-minute history (survives a reload) then extended by each
        # ~5s poll's current worst-across-tables lag, so the line updates continuously.
        # Maintained by record_cdc_lag_sample; empty when the metric is unavailable.
        self.cdc_replication_lag_series: list[tuple[int, int]] = []
        # CDC lifecycle actions (Deploy infra / Start / Stop / Delete) -- each runs
        # as a JobManager CloudFormation job; only one runs at a time, so a single
        # ``cdc_deploy_job_id`` + ``cdc_action_kind`` (which operation it is, for the
        # right stage labels) + an append-only step log live here (transient). The
        # cdc-stack name every operation targets.
        self.cdc_deploy_job_id: Optional[str] = None
        self.cdc_action_kind: Optional[str] = None  # "infra"|"start"|"stop"|"delete"
        self.cdc_stack_name: str = CDC_DEFAULT_STACK_NAME
        self._cdc_deploy_log: list[tuple[datetime, str]] = []
        # Set when the operator explicitly asks to (re)deploy CDC infrastructure after
        # a teardown, so the ~20-line BYO-VPC form is not thrust at them the instant a
        # delete finishes -- at that moment "it's gone" is the answer they want, and a
        # deploy form reads as though the tool were about to rebuild the MSK cluster
        # they just paid to remove. Only gates the FORM; a first-ever deploy (no
        # teardown in this session) still shows it directly. See
        # ``cdc_redeploy_needs_confirmation``.
        self.cdc_redeploy_confirmed: bool = False
        # Durable marker for an in-flight CDC *teardown* (stop/delete). Distinct from
        # ``cdc_deploy_job_id`` above (which drives the in-CDC-step stage-progress card
        # and IS wiped by a Start-over reset): this triple feeds the cross-view
        # "teardown in progress" banner and the Start-over race-guard, and is
        # deliberately PRESERVED across ``reset_in_place`` -- a Start-over that chose
        # stop/delete submits the teardown just before the reset, and wiping the
        # marker would hide the running teardown (the very gap the banner closes).
        # ``kind`` is "stop"|"delete"; cleared once the job settles (see the banner
        # getter in ``app.py``). Transient/session-only (not persisted across restart;
        # the CDC step's DELETE_IN_PROGRESS stack-probe notice covers that case).
        self.cdc_teardown_job_id: Optional[str] = None
        self.cdc_teardown_kind: Optional[str] = None  # "stop"|"delete"
        self.cdc_teardown_stack: Optional[str] = None
        # EVERY (job_id, stack) the current Start-over teardown launched, in teardown
        # order. Start over can target several cdc-stacks at once (see
        # ``cdc_teardown_plan``), but the marker above is a single slot -- so the banner
        # followed only the FIRST stack and vanished the moment it settled, while the
        # others were still deleting and still billing for MSK / NAT with nothing on
        # screen. This list lets the banner advance to the next unfinished stack and
        # report "2 of 3". Preserved across a Start-over reset, like the marker.
        self.cdc_teardown_queue: list[tuple[str, str]] = []
        # A FINISHED teardown the operator has not dismissed yet: {"kind", "stacks"}.
        # Completion used to be signalled only by a ui.notify toast, which hangs off a
        # ui.timer and so is gone after a refresh -- leaving no way to tell "finished"
        # from "never ran", for an operation that takes 15-45 min and is expected to be
        # left unattended. Held durably so the banner can report the result until it is
        # explicitly closed. Preserved across a Start-over reset, like the marker.
        self.cdc_teardown_done: dict = {}
        # Everything a one-click "Retry cleanup" needs to re-launch this teardown
        # AFTER a Start-over session reset has wiped the session config (region /
        # deploy-role / profile / whether to also delete the tool-managed secret).
        # {"region","role_arn","profile","cleanup_secret"}. Preserved with the marker
        # across reset_in_place; empty when no teardown is tracked.
        self.cdc_teardown_ctx: dict = {}
        # UI-only: remembered open/closed state of the "Deploy log" expansion.
        # Anchored on the (session-scoped) state -- NOT a local of the render fn --
        # so the whole CDC panel's ~5s live-poll rebuild does not snap a log the
        # user opened shut. ``_render_deploy_log`` reads/writes ``["open"]``.
        self.cdc_deploy_log_ui_state: dict = {"open": False}
        # BYO-VPC infrastructure inputs the customer enters in the Deploy-infra
        # form (VpcId, subnets, plugin S3 keys, source host/secret, DsqlClusterArn,
        # …). Filled values only; pre-seeded from the target/source config where
        # known. Transient/session-only.
        self._cdc_infra_inputs: dict[str, str] = {}
        # Cached, best-effort cdc-stack lifecycle phase so the UI can pick the right
        # action without an AWS call on every render: "absent" (no stack -> Deploy),
        # "infra" (stack up, no connectors -> Start), "running" (connectors ->
        # Stop), "unstable" (in-progress/rolled-back), or None (not yet probed).
        # ``cdc_stack_phase_status`` carries the raw StackStatus for messages.
        self.cdc_stack_phase: Optional[str] = None
        self.cdc_stack_phase_status: Optional[str] = None
        self.cdc_stack_phase_checked: bool = False
        # True when the probed cdc-stack has ALREADY streamed, so its resume offset is
        # committed to the (Stop-surviving, fixed-name) offsets topic and Start CDC needs
        # NO watermark -- streaming resumes where it stopped. See
        # ``_status.cdc_has_committed_offset``. False until probed, so an unread stack
        # falls back to requiring a start point rather than claiming a resume point.
        self.cdc_has_committed_offset: bool = False
        # Other ``mysql-dsql-cdc-*`` stacks discovered in the account that the
        # current session does NOT target (name != ``cdc_stack_name``). Populated
        # best-effort by the render-time probe so the CDC screen can offer to ADOPT
        # an existing pipeline instead of showing a fresh deploy -- and never
        # silently create a second, costly MSK stack. List of (name, StackStatus).
        self.cdc_other_stacks: list[tuple[str, str]] = []
        # Each attach-candidate stack's replicated table set (stack name ->
        # TableIncludeList). Used to withhold the attach offer when a candidate pipeline
        # does not cover the tables this session loaded -- attaching promotes Data
        # Migration to DONE, so a mismatched pipeline would report the migration complete
        # while the loaded tables had no CDC. Missing/empty == unknown (does not block).
        self.cdc_other_stack_tables: dict[str, list[str]] = {}
        # Monotonic timestamp of the last render-time CDC discovery (describe +
        # list connectors); throttles those AWS reads across rapid re-renders.
        # Reset to None here so a Start-over (reset_in_place re-runs __init__)
        # forces a fresh discovery.
        self._cdc_discovery_monotonic: Optional[float] = None
        # Optional dedicated CDC deploy-role ARN (process config, threaded from
        # AppConfig at screen-build time). When set, the cdc-stack deployer assumes
        # this role for the privileged CloudFormation/MSK/IAM operations instead of
        # using the app's own (task-role) identity. None = local dev / admin creds.
        self.cdc_deploy_role_arn: Optional[str] = None
        # Optional customer-managed KMS key id for the tool-created source secret
        # (process config, threaded at screen-build time). None -> default key.
        self.cdc_secret_kms_key_id: Optional[str] = None

    def set_lob_exclusion(self, table: str, column: str, exclude: bool) -> None:
        """Toggle whether one oversized-LOB column is excluded from the migration.

        The selection is migration-wide (H13): it drops the column from both the
        Full Load INSERT list and CDC capture, so the two paths never disagree.
        """
        with self._lock:
            current = self._excluded_lob_columns.setdefault(table, set())
            if exclude:
                current.add(column)
            else:
                current.discard(column)
                if not current:
                    self._excluded_lob_columns.pop(table, None)

    def lob_exclusions(self) -> dict[str, set[str]]:
        """Return a copy of the per-table excluded-LOB-column selection (H13)."""
        with self._lock:
            return {
                table: set(columns)
                for table, columns in self._excluded_lob_columns.items()
            }

    def set_cdc_message_key_columns(
        self, message_key_columns: "dict[str, list[str]]"
    ) -> None:
        """Record the composite record-key override for CDC (db.table -> key cols).

        Replaces the whole map (recomputed from the applied Schema Conversion on
        each render). Empty means every table keys on its source PK.
        """
        with self._lock:
            self._cdc_message_key_columns = {
                table: list(cols) for table, cols in message_key_columns.items()
            }

    def cdc_message_key_columns(self) -> "dict[str, list[str]]":
        """Return a copy of the composite record-key override map for CDC."""
        with self._lock:
            return {
                table: list(cols)
                for table, cols in self._cdc_message_key_columns.items()
            }

    def set_cdc_start_mode(self, mode: str) -> None:
        """Record the chosen CDC start mode: ``"auto"`` or ``"manual"``.

        ``"auto"`` uses the Full Load watermark (gapless); ``"manual"`` uses the
        operator-entered GTID / binlog position. Any value other than ``"manual"``
        normalizes to ``"auto"`` so the default is always the safe gapless path.
        """
        with self._lock:
            self._cdc_start_mode = "manual" if mode == "manual" else "auto"

    def cdc_start_mode(self) -> str:
        """Return the chosen CDC start mode (``"auto"`` or ``"manual"``)."""
        with self._lock:
            return self._cdc_start_mode

    def set_cdc_start_position(
        self,
        *,
        gtid: Optional[str] = None,
        binlog_file: Optional[str] = None,
        binlog_pos: Optional[int] = None,
    ) -> None:
        """Record the manual CDC start-position override (GTID and/or binlog).

        Blank/``None`` values clear that field. Stored verbatim; the UI validates
        the strings before calling this (advisory) and :meth:`cdc_start_override`
        assembles a :class:`CdcResumePoint` from whatever is set.
        """
        with self._lock:
            self._cdc_start_gtid = (gtid or "").strip() or None
            self._cdc_start_binlog_file = (binlog_file or "").strip() or None
            self._cdc_start_binlog_pos = binlog_pos

    def cdc_start_override(self) -> Optional[CdcResumePoint]:
        """Return the manual start position as a CdcResumePoint, or None.

        ``None`` means no usable manual override -- the caller uses the Full Load
        watermark. Returns ``None`` whenever the mode is ``"auto"`` (regardless of
        any stale entered values) so switching back to Automatic cleanly drops the
        override. In ``"manual"`` mode a point is returned when a GTID or a
        complete binlog file:position is present; :meth:`CdcResumePoint.has_coordinates`
        then confirms it is usable.
        """
        with self._lock:
            if self._cdc_start_mode != "manual":
                return None
            gtid = self._cdc_start_gtid
            binlog_file = self._cdc_start_binlog_file
            binlog_pos = self._cdc_start_binlog_pos
        has_binlog = binlog_file is not None and binlog_pos is not None
        if not gtid and not has_binlog:
            return None
        return CdcResumePoint(
            gtid_executed=gtid,
            binlog_file=binlog_file if has_binlog else None,
            binlog_position=binlog_pos if has_binlog else None,
        )

    def set_cdc_status_view(self, view: Optional[LoadStatusView]) -> None:
        """Record the latest CDC monitoring view (rebuilt by the poller)."""
        with self._lock:
            self.cdc_status_view = view

    def set_cdc_activity(self, activity: Optional["CdcActivitySummary"]) -> None:
        """Record the latest CDC throughput summary (rebuilt by the poller)."""
        with self._lock:
            self.cdc_activity = activity

    def set_cdc_deploy_job_id(
        self, job_id: Optional[str], *, kind: Optional[str] = None
    ) -> None:
        """Record (or clear) the running CDC lifecycle job's id + which operation.

        ``kind`` is one of ``"infra"``/``"start"``/``"stop"``/``"delete"`` so the
        progress UI can label the right stage set; cleared with the job id.
        """
        with self._lock:
            self.cdc_deploy_job_id = job_id
            self.cdc_action_kind = kind if job_id is not None else None

    def set_cdc_redeploy_confirmed(self, confirmed: bool) -> None:
        """Record that the operator asked to (re)deploy CDC infrastructure.

        Latches the answer to the post-teardown "redeploy?" prompt so the form stays
        open across the card's refreshes; reset it to hide the form again.
        """
        with self._lock:
            self.cdc_redeploy_confirmed = bool(confirmed)

    def set_cdc_teardown(
        self,
        job_id: Optional[str],
        *,
        kind: Optional[str] = None,
        stack: Optional[str] = None,
        ctx: Optional[dict] = None,
    ) -> None:
        """Record an in-flight CDC teardown (stop/delete) for the persistent banner
        and the Start-over race-guard.

        ``kind`` is ``"stop"`` (remove connectors) or ``"delete"`` (tear down the
        whole cdc-stack); ``stack`` names the targeted cdc-stack for the banner copy;
        ``ctx`` carries the config a one-click retry needs after a session reset
        ({"region","role_arn","profile","cleanup_secret"}). Unlike
        :meth:`set_cdc_deploy_job_id`, this marker is PRESERVED across a Start-over
        :meth:`~DataMigrationStore.reset_in_place`, so a teardown fired by Start-over
        stays visible/guarded/retryable even though the session was wiped. Cleared
        with :meth:`clear_cdc_teardown` once the job settles (or the user dismisses a
        failed one).
        """
        with self._lock:
            self.cdc_teardown_job_id = job_id
            self.cdc_teardown_kind = kind if job_id is not None else None
            self.cdc_teardown_stack = stack if job_id is not None else None
            self.cdc_teardown_ctx = dict(ctx) if (job_id is not None and ctx) else {}

    def set_cdc_teardown_queue(self, entries: "list[tuple[str, str]]") -> None:
        """Record every ``(job_id, stack)`` this teardown launched, in teardown order.

        Start over can tear down several cdc-stacks at once, but the durable marker is a
        single slot -- so the banner tracked only the first and disappeared when it
        settled, leaving the rest deleting (and billing) unannounced. Keeping the full
        list lets the banner move to the next unfinished stack and show overall progress.
        """
        with self._lock:
            self.cdc_teardown_queue = [
                (str(job_id), str(stack)) for job_id, stack in entries if job_id
            ]

    def advance_cdc_teardown(self, job_id: str, stack: str) -> None:
        """Re-point the durable marker at another still-running teardown in the queue.

        Used when the tracked stack finishes while later ones are still going: the marker
        (and therefore the banner + the Start-over race guard) follows the next unfinished
        stack instead of being cleared. Deliberately keeps ``kind``/``ctx`` -- the same
        operation and the same retry context apply to every stack in one teardown.
        """
        with self._lock:
            self.cdc_teardown_job_id = job_id
            self.cdc_teardown_stack = stack

    def set_cdc_teardown_done(self, *, kind: Optional[str], stacks: "list[str]") -> None:
        """Record a teardown that FINISHED, so the banner can report it until dismissed.

        The completion signal was a ``ui.notify`` toast, which dies with the page: after a
        refresh there was nothing distinguishing "the 45-minute teardown finished" from
        "it never ran". This survives a refresh and is cleared only by
        :meth:`dismiss_cdc_teardown_done` (the banner's close button).
        """
        with self._lock:
            self.cdc_teardown_done = {
                "kind": kind,
                "stacks": [str(s) for s in stacks if s],
            }

    def dismiss_cdc_teardown_done(self) -> None:
        """Clear the finished-teardown notice (the operator closed the banner)."""
        with self._lock:
            self.cdc_teardown_done = {}

    def clear_cdc_teardown(self) -> None:
        """Clear the in-flight teardown marker (the stop/delete job settled or a
        failed one was dismissed)."""
        with self._lock:
            self.cdc_teardown_job_id = None
            self.cdc_teardown_kind = None
            self.cdc_teardown_stack = None
            self.cdc_teardown_ctx = {}
            self.cdc_teardown_queue = []

    def set_cdc_infra_inputs(self, inputs: dict[str, str]) -> None:
        """Replace the BYO-VPC infrastructure inputs (read-through to session)."""
        session = self._session
        if session is not None:
            try:
                session.set_cdc_infra_inputs(inputs)
                return
            except Exception:  # noqa: BLE001 - fall back to local storage
                pass
        with self._lock:
            self._cdc_infra_inputs = dict(inputs)

    def cdc_infra_inputs(self) -> dict[str, str]:
        """Return a copy of the entered BYO-VPC infrastructure inputs."""
        session = self._session
        if session is not None:
            try:
                return session.cdc_infra_inputs()
            except Exception:  # noqa: BLE001 - fall back to local storage
                pass
        with self._lock:
            return dict(self._cdc_infra_inputs)

    def set_cdc_stack_name(self, name: str) -> bool:
        """Set the cdc-stack name when valid; return True if accepted.

        The name must be inside the ``mysql-dsql-cdc-*`` family the deploy role grants
        (see :func:`cdc_stack_name_is_valid`). An invalid name is rejected and the
        current name is kept, so a typo never makes the tool deploy resources the
        deploy role cannot manage. One cdc-stack per source DB lets several
        migrations run concurrently in one account/region.
        """
        from dsql_migrator.core.cdc import cdc_stack_name_is_valid

        candidate = (name or "").strip()
        if not cdc_stack_name_is_valid(candidate):
            return False
        with self._lock:
            self.cdc_stack_name = candidate
        return True

    def set_cdc_stack_phase(
        self, phase: Optional[str], *, status: Optional[str] = None
    ) -> None:
        """Cache the probed cdc-stack lifecycle phase + raw status (best-effort)."""
        with self._lock:
            self.cdc_stack_phase = phase
            self.cdc_stack_phase_status = status
            self.cdc_stack_phase_checked = True

    def set_cdc_has_committed_offset(self, value: bool) -> None:
        """Cache whether the probed cdc-stack already holds a committed resume offset.

        Set from the SAME describe as the phase (see ``_probe_cdc_stack_phase``) so the
        two can never describe different stacks. When True, Start CDC must NOT require a
        watermark: the connector resumes from its own offsets topic.
        """
        with self._lock:
            self.cdc_has_committed_offset = bool(value)

    def set_cdc_other_stacks(self, stacks: list[tuple[str, str]]) -> None:
        """Cache other ``mysql-dsql-cdc-*`` stacks found in the account whose name is
        NOT the one this session targets, so the CDC screen can offer to adopt an
        existing pipeline instead of deploying a duplicate."""
        with self._lock:
            self.cdc_other_stacks = list(stacks)

    def set_cdc_other_stack_tables(self, tables_by_stack: "dict[str, list[str]]") -> None:
        """Cache each attach-candidate stack's replicated table set (``TableIncludeList``).

        Lets the attach offer be withheld when a candidate pipeline does not cover the
        tables this session loaded: attaching promotes Data Migration to DONE and unlocks
        Validation, so attaching a pipeline that streams a different table set would report
        the migration complete while every loaded table had no CDC at all. An entry missing
        or empty means "unknown", which the scope check treats as not-a-mismatch.
        """
        with self._lock:
            self.cdc_other_stack_tables = {
                name: list(tables) for name, tables in (tables_by_stack or {}).items()
            }

    def adopt_cdc_stack(self, name: str) -> bool:
        """Adopt an existing cdc-stack: point the session at ``name`` and force a
        fresh discovery so the CDC screen re-derives the TRUE live state (running /
        infra) from AWS rather than a stale probe. Returns ``False`` if ``name`` is
        invalid. Read/attach only -- it never mutates the live stack or connectors
        (starting fresh is the explicit Stop/Delete path, not adoption)."""
        if not self.set_cdc_stack_name(name):
            return False
        with self._lock:
            self._cdc_discovery_monotonic = None  # force the next render probe to re-run
            self.cdc_stack_phase = None
            self.cdc_stack_phase_status = None
            self.cdc_stack_phase_checked = False
            self.cdc_other_stacks = []
            # Belongs to the previously-targeted stack; the fresh probe repopulates.
            self.cdc_reconciled_table_names = []
            # Applied-ops + replication-lag metrics are per-stack too; drop them so the
            # adopted stack's poll repopulates rather than showing the prior stack's.
            self.cdc_applied_ops_by_table = {}
            self.cdc_replication_lag_by_table = {}
            self.cdc_replication_lag_series = []
        return True

    def append_cdc_deploy_log(self, when: datetime, message: str) -> None:
        """Append one timestamped line to the deploy step log (thread-safe)."""
        with self._lock:
            self._cdc_deploy_log.append((when, message))

    def get_cdc_deploy_log(self) -> list[tuple[datetime, str]]:
        """Return a copy of the deploy step log lines."""
        with self._lock:
            return list(self._cdc_deploy_log)

    def clear_cdc_deploy_log(self) -> None:
        """Empty the deploy step log (called when a new deploy starts)."""
        with self._lock:
            self._cdc_deploy_log = []

    def set_cdc_controller(self, controller: object) -> None:
        """Inject the MSK Connect controller used to poll connector status."""
        with self._lock:
            self.cdc_controller = controller

    def set_cdc_connector_names(self, names: Sequence[str]) -> None:
        """Record the connector names the CDC poller should track (read-only)."""
        with self._lock:
            self.cdc_connector_names = [n for n in names if n]

    def set_cdc_reconciled_table_names(self, names: Sequence[str]) -> None:
        """Record the table set reconciled from the live stack's TableIncludeList.

        Read-only reflection of the running pipeline's tables (each entry is a
        table's ``.name``), so an adopted / out-of-band pipeline resolves its
        replicated tables even when this session holds no watermark or selection.
        """
        with self._lock:
            self.cdc_reconciled_table_names = [n.strip() for n in names if n and n.strip()]

    def set_cdc_applied_ops_by_table(
        self, applied_ops: "dict[str, dict[str, int]]"
    ) -> None:
        """Record the per-table applied-ops the sink reported (Inserts/Updates/Deletes).

        Scan-free CDC progress read from CloudWatch on the live poll; drives the
        "Changes since Full Load" (I/U/D) column without any ``COUNT(*)``. Replaces
        the prior map (empty when the metrics are unavailable). Each value is
        normalized to ``{"inserts","updates","deletes"}`` with int counts.
        """
        with self._lock:
            normalized: dict[str, dict[str, int]] = {}
            for table, ops in dict(applied_ops or {}).items():
                ops = ops or {}
                normalized[str(table)] = {
                    "inserts": int(ops.get("inserts", 0) or 0),
                    "updates": int(ops.get("updates", 0) or 0),
                    "deletes": int(ops.get("deletes", 0) or 0),
                }
            self.cdc_applied_ops_by_table = normalized

    def set_cdc_replication_lag_by_table(self, lag_ms: "dict[str, int]") -> None:
        """Record per-table replication lag in ms (from ``ReplicationLagMs``).

        Time-based end-to-end lag read from CloudWatch on the live poll; drives the
        "Stream lag" column. Replaces the prior map (empty when the metric is
        unavailable or every table is idle/caught up).
        """
        with self._lock:
            self.cdc_replication_lag_by_table = {
                str(k): int(v) for k, v in dict(lag_ms or {}).items()
            }

    def record_cdc_lag_sample(
        self,
        *,
        current_ms: "Optional[int]",
        now_epoch: int,
        seed_series: "Optional[Sequence[tuple[int, int]]]" = None,
        window_seconds: int = 900,
        max_points: int = 400,
    ) -> None:
        """Append one live replication-lag sample to the rolling series that backs the
        live "Stream lag" chart (hybrid strategy):

        * on the FIRST sample (empty buffer, e.g. a fresh load or a page reload) the
          buffer is SEEDED from the CloudWatch 1-minute history (``seed_series``), so
          the chart shows immediate history and survives a reload;
        * every ~5s poll then APPENDS the current worst-across-tables lag
          (``current_ms``) at ``now_epoch`` so the line extends continuously (denser
          than CloudWatch's 1-minute cadence);
        * the buffer is trimmed to the last ``window_seconds`` and hard-capped at
          ``max_points`` so it stays bounded regardless of how long CDC runs.

        ``current_ms`` is the current max lag in ms, ``0`` when caught up, or ``None``
        to skip appending (metric unavailable / nothing to plot yet).
        """
        with self._lock:
            buf = list(self.cdc_replication_lag_series)
            if not buf and seed_series:
                buf = [(int(t), int(v)) for t, v in seed_series]
            if current_ms is not None:
                e = int(now_epoch)
                if buf and buf[-1][0] == e:
                    buf[-1] = (e, int(current_ms))  # coalesce same-second samples
                else:
                    buf.append((e, int(current_ms)))
            cutoff = int(now_epoch) - int(window_seconds)
            buf = [(t, v) for t, v in buf if t >= cutoff]
            if len(buf) > max_points:
                buf = buf[-max_points:]
            self.cdc_replication_lag_series = buf

    def set_cdc_connector_running_names(self, names: Sequence[str]) -> None:
        """Record which of my connectors are RUNNING (vs still provisioning)."""
        with self._lock:
            self.cdc_connector_running_names = [n for n in names if n]

    def set_row_counts(
        self,
        *,
        source: dict[str, Optional[int]],
        target: dict[str, Optional[int]],
        fetched_at: datetime,
        source_max_pk: "Optional[dict[str, Optional[int]]]" = None,
        target_max_pk: "Optional[dict[str, Optional[int]]]" = None,
    ) -> None:
        """Record the latest per-table source/target row counts + max-PK marks.

        ``source_max_pk`` / ``target_max_pk`` are the per-table stream high-water
        marks used by the CDC "is it behind?" check; omitted (None) leaves them
        empty, so callers that only have counts still work.
        """
        with self._lock:
            self.row_count_source = dict(source)
            self.row_count_target = dict(target)
            self.row_max_pk_source = dict(source_max_pk or {})
            self.row_max_pk_target = dict(target_max_pk or {})
            self.row_counts_fetched_at = fetched_at

    def set_tables_with_data(self, names: frozenset[str]) -> None:
        """Record the selected target tables the probe found already holding rows."""
        with self._lock:
            self._tables_with_data = names

    @property
    def tables_with_data(self) -> frozenset[str]:
        """Return the pre-existing (non-empty) selected target tables."""
        with self._lock:
            return self._tables_with_data

    def set_target_primary_keys(
        self, keys: Mapping[str, Optional[list[str]]]
    ) -> None:
        """Record each selected target's ACTUAL primary key from the pre-dialog probe.

        ``None`` for a table means it was probed and the key could not be read; a table
        missing entirely means it was never probed. Callers must treat both as unknown.
        """
        with self._lock:
            self._target_primary_keys = dict(keys)

    @property
    def target_primary_keys(self) -> dict[str, Optional[list[str]]]:
        """Return the probed target primary keys (empty before the first probe)."""
        with self._lock:
            return dict(self._target_primary_keys)

    def set_reload_mode(self, mode: str) -> None:
        """Set the run-wide reload choice for pre-existing tables.

        ``"drop"`` = DROP+recreate each first (clean reload); ``"append"`` = keep
        existing rows and load only the missing ones (idempotent). Any other value
        is coerced to the non-destructive ``"append"`` default.
        """
        with self._lock:
            self._reload_mode = "drop" if mode == "drop" else "append"

    @property
    def reload_mode(self) -> str:
        """Return the run-wide reload choice (``"append"`` default / ``"drop"``)."""
        with self._lock:
            return self._reload_mode

    def set_replace_targets(self, names: frozenset[str]) -> None:
        """Back-compat setter: record pre-existing tables and switch to drop mode.

        Retained for callers/tests that set the drop set directly. Setting a
        non-empty set implies the user chose to DROP+recreate those tables, so it
        records them as the tables-with-data and flips the mode to ``"drop"``; an
        empty set means append (nothing dropped).
        """
        with self._lock:
            self._tables_with_data = names
            self._reload_mode = "drop" if names else "append"

    @property
    def replace_targets(self) -> frozenset[str]:
        """Return the tables that will be DROPped & recreated on the next run.

        Derived: the pre-existing tables when the run-wide choice is ``"drop"``,
        else empty (append keeps existing rows). This is the run's replace set.
        """
        with self._lock:
            return self._tables_with_data if self._reload_mode == "drop" else frozenset()

    def set_selection(self, selection: TableSelection) -> None:
        """Replace the table selection for the sub-flow (marks it user-touched)."""
        with self._lock:
            self.selection = selection
            self.selection_touched = True

    def set_accept_quarantined_rows(self, accepted: bool) -> None:
        """Record whether permanently-quarantined rows are accepted (UI thread)."""
        with self._lock:
            self.accept_quarantined_rows = accepted

    def set_active_substep(self, substep: Optional[str]) -> None:
        """Record the active Prerequisites/Full Load/CDC sub-step (UI thread).

        ``None`` clears the explicit choice so the next render derives a sensible
        default for the current migration type/job state.
        """
        self.active_substep = substep

    def bind_session(self, session: object) -> None:
        """Bind the SessionConnectionState so mode/infra-inputs read-through to it.

        The Data Migration step chooses the mode and stores it on the
        session; binding here makes ``migration_type`` and ``cdc_infra_inputs``
        authoritative from the session while keeping the local fields as a
        fallback when no session is bound (unit tests, legacy call sites).
        """
        self._session = session

    @property
    def migration_type(self) -> "MigrationType":
        """The selected migration type (read-through to the bound session)."""
        session = self._session
        if session is not None:
            try:
                return session.migration_type
            except Exception:  # noqa: BLE001 - fall back to the local value
                pass
        return self._migration_type

    @migration_type.setter
    def migration_type(self, value: "MigrationType") -> None:
        self._migration_type = value
        session = self._session
        if session is not None:
            try:
                session.set_migration_type(value)
            except Exception:  # noqa: BLE001 - local value still updated
                pass

    def set_migration_type(self, migration_type: "MigrationType") -> None:
        """Record the user's Data Migration type selection (UI thread)."""
        self.migration_type = migration_type  # routes through the property setter

    def set_prereq_report(
        self, mode: MigrationMode, report: PrerequisiteReport
    ) -> None:
        """Record the latest prerequisite report for ``mode``.

        Also snapshots the LOB exclusion the checks ran against, so the run guard
        can detect a column excluded AFTER the checks (which could turn a passed
        loadability check into a mid-load failure) -- the exclusion analogue of
        ``prereq_scope_gap`` for a late table add. The snapshot is a plain
        ``{table: frozenset(cols)}`` copy so a later toggle cannot mutate it.
        """
        with self._lock:
            self._prereq_reports[mode] = report
            self._prereq_report_lob_exclusions[mode] = {
                table: frozenset(cols)
                for table, cols in self._excluded_lob_columns.items()
            }

    def get_prereq_report(self, mode: MigrationMode) -> Optional[PrerequisiteReport]:
        """Return the last prerequisite report for ``mode``, if any."""
        with self._lock:
            return self._prereq_reports.get(mode)

    def prereq_report_lob_exclusions(
        self, mode: MigrationMode
    ) -> dict[str, frozenset[str]]:
        """Return the LOB exclusion the last ``mode`` report was checked against.

        Empty when no report has been recorded for the mode. Used by the run guard
        to catch an exclusion changed after the checks ran.
        """
        with self._lock:
            return dict(self._prereq_report_lob_exclusions.get(mode, {}))

    def set_prereq_gated_mode(self, mode: Optional[MigrationMode]) -> None:
        """Record which prerequisite mode gated the most recent started Full Load."""
        with self._lock:
            self._prereq_gated_mode = mode

    @property
    def prereq_gated_mode(self) -> Optional[MigrationMode]:
        """The prerequisite mode that cleared the gate for the last started run."""
        with self._lock:
            return self._prereq_gated_mode

    def set_prereq_running(self, mode: MigrationMode) -> None:
        """Mark ``mode``'s prerequisite checks as running (for live feedback)."""
        with self._lock:
            self._prereq_running.add(mode)

    def clear_prereq_running(self, mode: MigrationMode) -> None:
        """Clear the running marker for ``mode``'s prerequisite checks."""
        with self._lock:
            self._prereq_running.discard(mode)

    def is_prereq_running(self, mode: MigrationMode) -> bool:
        """Return whether ``mode``'s prerequisite checks are currently running."""
        with self._lock:
            return mode in self._prereq_running

    def set_error(self, message: str) -> None:
        """Record a failure message for display.

        Stamps the CURRENTLY selected migration type alongside it. Without that stamp
        the screen cannot tell a live failure from one carried over: after a Full Load
        that quarantined rows, switching to CDC only kept rendering a red "Migration
        failed" banner beside a "Success" header. The renderer uses the stamp to demote
        such a message to carried-over context instead of dropping it (the gap it
        reports is real and CDC will not backfill it).

        Read ``migration_type`` OUTSIDE the lock: the property reads through to the
        bound session, and taking this lock around foreign code invites a deadlock.
        """
        recorded_type = self.migration_type
        with self._lock:
            self._error = message
            self._error_migration_type = recorded_type

    @property
    def error(self) -> Optional[str]:
        """Return the last failure message, if any."""
        with self._lock:
            return self._error

    @property
    def error_migration_type(self) -> "Optional[MigrationType]":
        """The migration type selected when :meth:`set_error` recorded the message.

        ``None`` when no error is held, or when one was restored from a session that
        predates this stamp -- callers must treat that as "unknown", not as "differs".
        """
        with self._lock:
            return self._error_migration_type

    def clear_outputs(self) -> None:
        """Discard the previous error before a (re-)run."""
        with self._lock:
            self._error = None
            self._error_migration_type = None


@dataclass
class DataMigrationStore:
    """Process-memory map of session id to :class:`DataMigrationState`.

    Mirrors :class:`~dsql_migrator.ui.evaluation.EvaluationStore`: each UI session
    sees only its own state and nothing is persisted to disk.
    """

    _states: dict[str, DataMigrationState] = field(default_factory=dict)

    def get_or_create(self, session_id: str) -> DataMigrationState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = DataMigrationState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[DataMigrationState]:
        """Return the state for ``session_id``, or ``None`` if absent."""
        return self._states.get(session_id)

    def clear(self, session_id: Optional[str]) -> None:
        """Remove the state for ``session_id`` (no-op if absent)."""
        if session_id is None:
            return
        self._states.pop(session_id, None)

    def reset_in_place(self, session_id: Optional[str]) -> None:
        """Reset the state WITHOUT replacing the object (no-op if absent).

        The workflow screen captures this state object in its builder closures at
        build time, so popping + recreating would orphan the captured reference.
        Re-initialising the SAME instance keeps every closure on the live object.
        The session binding (set once at build time via ``bind_session``, not
        per render) is preserved across the reset so ``migration_type`` keeps
        reading through to the live session.
        """
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            bound = getattr(state, "_session", None)
            # Preserve an in-flight CDC teardown marker across the reset. A Start-over
            # that chose stop/delete submits the teardown BEFORE this reset; the
            # persistent teardown banner and the Start-over race-guard both read this
            # marker, so wiping it would make the running teardown invisible (and, for
            # a custom stack name, unre-discoverable) -- exactly the gap this banner
            # closes. It is cleared by the banner getter once the job settles.
            teardown = (
                getattr(state, "cdc_teardown_job_id", None),
                getattr(state, "cdc_teardown_kind", None),
                getattr(state, "cdc_teardown_stack", None),
                getattr(state, "cdc_teardown_ctx", None),
            )
            state.__init__()  # type: ignore[misc]  # re-run init on the same object
            if bound is not None:
                state.bind_session(bound)
            if teardown[0] is not None:
                state.set_cdc_teardown(
                    teardown[0], kind=teardown[1], stack=teardown[2], ctx=teardown[3]
                )
