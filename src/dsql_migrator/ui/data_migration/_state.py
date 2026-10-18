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

import logging
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_STACK_NAME,
    CdcResumePoint,
)
from dsql_migrator.core.cdc_coords import normalize_gtid_set
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationMode,
    PrerequisiteReport,
    TableSelection,
)

from dsql_migrator.ui.data_migration._models import MigrationType
from dsql_migrator.ui.data_migration._cdc_status import CdcActivitySummary

_LOGGER = logging.getLogger(__name__)


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
        # How many rows that acceptance covered, so a LARGER later gap re-asks
        # instead of being waved through by a stale flag. None = no count recorded.
        self.accepted_quarantine_rows: Optional[int] = None
        # Provenance of a VpcId derived from the source DB's own DBSubnetGroup, e.g.
        # "the source cluster pgtest-ecommerce (subnet group ...)". Shown beside the
        # field so a prefilled value is never a SILENT substitution: the operator can
        # see where it came from and change it (the pipeline may belong in another VPC,
        # reached by peering / Transit Gateway / PrivateLink). None when not derived.
        self.cdc_vpc_provenance: Optional[str] = None
        # Why the VpcId prefill produced NOTHING, or None. The derivation is best effort
        # and its failure used to be discarded, so an unpermitted deployment looked
        # identical to one where the tool had never tried -- see derive_cdc_vpc_from_source.
        self.cdc_vpc_derivation_note: Optional[str] = None
        # Active sub-step of the Prerequisites -> Full Load -> CDC stepper. Held
        # here so it survives the content re-render driven by the progress poller
        # (None => derive a sensible default from the current job/prereq state).
        self.active_substep: Optional[str] = None
        # Transient, never persisted: the operator ran the prerequisite checks and has not
        # moved on yet, so Prerequisites stays open on their results even when a later
        # sub-step is pinned active (prerequisites_section_expanded). Set when a run ends;
        # cleared by the step's own navigation (Continue / Start Full Load), by collapsing
        # the section, and by a migration-type change.
        self.prereq_review_open: bool = False
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
        self._prereq_report_checked_at: dict[MigrationMode, datetime] = {}
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
        # Which selected tables EXIST on the target, from the SAME pre-dialog probe.
        # ``None`` means "not probed / probe failed" (treated conservatively: no
        # missing-table warning); a set means the probe read the target and any selected
        # table NOT in it is genuinely missing (deleted, or Schema Conversion not
        # applied) -- so an append load would fail it. Distinct from _tables_with_data
        # (rows), which treats a missing table the same as an empty one.
        self._target_tables_present: Optional[frozenset[str]] = None
        # The user's run-wide choice for those pre-existing tables: "append"
        # (keep existing rows, load only the missing ones -- idempotent
        # SKIP_EXISTING, the non-destructive default) or "drop" (DROP+recreate
        # each first, for a clean fresh load). Stored so a retry / per-table
        # Reload follows the SAME choice instead of silently reverting to append.
        self._reload_mode: str = "append"
        # Tables the Start dialog FORCES to Drop & reload for the NULL contract (table ->
        # one-line reason). Transient: re-derived at every probe, consumed when the run's
        # inputs are built, never persisted (its durable remainder is the job's
        # recreated_tables).
        self._exclusion_forced_replace: dict[str, str] = {}
        # Set by the Start dialog's confirm only: a forced set (and the overlay the dialog
        # disclosed) counts for the run being built only after the operator confirmed it,
        # so a dialog lost to a reload or disconnect leaves nothing a later run could take.
        self._exclusion_forced_confirmed: bool = False
        # What the dialog disclosed would be overlaid ({table: columns}); the run refuses to
        # start with a different set (a classification that settled meanwhile) and asks
        # for the dialog again.
        self._overlay_disclosed: Optional[dict[str, frozenset[str]]] = None
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
        # Exclusions restored from a session written before the NULL overlay (0.1.561)
        # whose data already moved under the old semantics: omitted only, never ALTERed.
        # Always a subset of _excluded_lob_columns. Persisted with the semantics stamp.
        self._legacy_exclusions: dict[str, set[str]] = {}
        # While a restored pre-overlay session is not yet classified (no evidence either
        # way yet), its entries behave as legacy. Transient.
        self._restored_exclusion_facts: Optional[object] = None
        # True once this session used the overlay ledger (an operator exclusion, or a
        # ledger read that found a record): a table can owe a DEFAULT even with no
        # exclusion left. Persisted; cleared only by Start over.
        self._overlay_ledger_in_use: bool = False
        # The exclusion RULES (core/exclusion_rules.py), kept for re-editing only: no
        # loader reads them -- every path reads the resolved set above and the selection.
        # ``None`` = never saved (the popup synthesizes lines from the stored set).
        self._exclusion_rules_text: Optional[str] = None
        # Tables the RULES unselected at the last Save (provenance): read only by the
        # popup, so deleting a rule re-selects only these, never a table the operator
        # unticked in the tree.
        self._rule_excluded_tables: set[str] = set()
        # Rule-excluded tables the operator re-ticked in the tree since the last Save (a
        # deliberate override the popup names). Persisted until the next Save.
        self._rule_reticked_tables: set[str] = set()
        # Suggested rule lines waiting for the popup (e.g. a quarantine or DLQ route while
        # the rules are locked); inserted on each open until a Save takes them.
        self._exclusion_pending_rules: list[str] = []
        # The popup's last resolution per slot ({slot: (key, held inputs, result)}): the
        # row and the popup re-resolve only when an input changed. Transient.
        self._exclusion_resolution_memo: dict = {}
        # Measured value sizes, (table, column) -> ("over" | "ok" | "unknown", when,
        # limit bytes or None): from the prerequisite run's SOURCE_VALUE_SIZE answers or
        # the popup's Check sizes. A verdict holds only for the limit it was measured
        # against. Transient.
        self._value_size_cache: dict[tuple[str, str], tuple[str, datetime, Optional[int]]] = {}
        # Every open popup's refresh (one per tab: tabs share this state), so a Check
        # sizes pass refreshes each popup on screen, not only the one that started it.
        # Transient.
        self._size_check_listeners: list[Callable[[], None]] = []
        # The row's last drift summary ("Your rules now match ... - review"), repeated in
        # the Start dialog; never applied, never a gate. Transient.
        self._exclusion_drift_text: Optional[str] = None
        # Check sizes is single-flight (a second click starts nothing) and cancellable
        # between columns. Transient.
        self._size_check_running: bool = False
        self._size_check_cancel: bool = False
        # Whether the CDC step's "What CDC receives" is expanded: that step re-renders on
        # a 5 s poll in some phases, which must not snap it shut. Transient.
        self.cdc_receives_open: bool = False
        # Composite-PK record-key override for CDC, keyed by db.table -> target key
        # columns [leading, pk...]. Set from the applied Schema Conversion when a
        # table's DSQL target has a composite key, so Debezium re-keys the change
        # record (message.key.columns) to match the target's ON CONFLICT/DELETE key --
        # or, on CDC infrastructure whose sink plugin reads SinkKeyColumns, keeps it keyed
        # on the source PK and the sink writes it by this key (G15, choose_rekey_routing).
        # Empty => every table keys on its source PK.
        self._cdc_message_key_columns: dict[str, list[str]] = {}
        self._cdc_preserved_foreign_keys: dict[str, list[str]] = {}
        # The APPLIED Schema Conversion (table -> TableConversion, user edits honoured) and
        # the foreign-key toggle, recomputed on each Data Migration render like the two maps
        # above, so the column-exclusion rule (core/column_exclusion.py) judges a column
        # against the SAME target DDL on every path -- including the CDC builders and
        # Validation, which do not hold the Schema Conversion state. Transient: never
        # persisted; a restored session has it again after its first render.
        self._applied_conversions: dict[str, object] = {}
        self._preserve_foreign_keys: bool = True
        # The table selection that render RESOLVED (effective_migration_selection: the
        # picker, else the Schema Conversion default) -- ``None`` before any render. The
        # exclusion card, its gates and the recovery actions judge the same tables with it
        # (``_models.exclusion_scope``); the stored ``selection`` alone is the empty "= all"
        # default until the picker is touched. Transient, like the two above.
        self._resolved_selection: "Optional[list[str]]" = None
        # The render's LIVE source for the three facts above (resolved selection, applied
        # conversion, live target columns): a function the Data Migration screen registers,
        # so every gate -- including the page header's Run, evaluated BEFORE the content
        # records anything, and a click right after a picker tick with no re-render --
        # judges the current inputs, not the previous render's snapshot. The recorded
        # values stay the fallback (no screen built yet: a restored session, a test).
        self._exclusion_inputs_resolver = None
        # How the operator chose the CDC start point: "auto" (gapless from the
        # Full Load watermark) or "manual" (an explicit GTID / binlog position).
        # An explicit mode (vs. inferring it from whether an override is set)
        # lets the UI show a clear radio choice and keep "Manual + empty inputs"
        # distinct from "Automatic".
        self._cdc_start_mode: str = "auto"
        # Set only when the operator takes the Start dialog's "Re-snapshot every table
        # Manual CDC start-position override the operator entered (a GTID set or
        # a binlog file:position). Only used when ``_cdc_start_mode == "manual"``.
        self._cdc_start_gtid: Optional[str] = None
        self._cdc_start_binlog_file: Optional[str] = None
        self._cdc_start_binlog_pos: Optional[int] = None
        # The captured tables' start-point fingerprints (core.cdc_catalog_drift) recorded
        # by "Fetch current position" for exactly the binlog file:position above. Bound to
        # those coordinates: every set_cdc_start_position call replaces them.
        self._cdc_start_fingerprints: dict[str, str] = {}
        # Live CDC monitoring (Phase 2). The status view is rebuilt by the CDC
        # poller from the controller's read-only signals; the controller talks to
        # the already-deployed MSK Connect connectors (decision-change 8 -- the UI
        # never deploys them). All transient/session-only -- not on MigrationJob.
        self.cdc_status_view: Optional[LoadStatusView] = None
        self.cdc_controller: Optional[object] = None  # MskConnectController
        # (region, aws_profile) the cached controller was built with. Nothing invalidated
        # the controller, so a profile/region change left CDC monitoring reading the OLD
        # identity while the operator believed the fix had been applied.
        self.cdc_controller_identity: Optional[tuple] = None
        # PINNED error-log key for every CDC surface. job_id is replaced by a Full Load
        # retry, and re-keying mid-stream made recorded dead-letters unreadable (see
        # cdc_error_log_key).
        self.cdc_log_key: Optional[str] = None
        # DLQ read cursor + already-audited event ids, held on the SESSION rather than on
        # the controller: the controller is rebuilt on any reset/new session, and its
        # in-memory cursor was the only thing keeping the durable activity log from
        # re-writing every dead letter in the blind 6 h look-back window each time.
        # Both keyed BY LOG GROUP: one cursor leaked across pipelines when attaching to
        # another CDC stack (see MskConnectController.seed_dlq_cursor).
        self.cdc_dlq_cursor_ms: dict = {}
        self.cdc_dlq_seen_ids: dict = {}
        # WHY the quarantine count may be incomplete (a first read with no cursor to
        # resume from can look back only its blind window; a read can hit its page cap),
        # or None when it is a true total. The count is what a cut-over decision turns on,
        # so "0 quarantined" must not render identically to "0 that we could see".
        self.cdc_dlq_coverage_bound: Optional[str] = None
        # Set once the bound has been written to the DURABLE activity log, so the
        # every-5s poll does not re-write the same line. On the state (not a module
        # global) so Start over clears it with everything else.
        self.cdc_dlq_bound_audited: bool = False
        # Identities of dead letters already written to the DURABLE activity log, so a
        # re-surfaced record is audited once (the write is append-only and permanent).
        self.cdc_dlq_audited_keys: set = set()
        # The per-table dead-letter LEDGER: {table: CdcDlqLedgerEntry} (count, latest time,
        # drift kinds), folded from each NEW dead letter as it is audited. PERSISTED on the
        # session snapshot: the records themselves live only in the in-memory error log and
        # the restored cursor stops them ever being re-read, so without this a restart made
        # every earlier dead letter vanish from the cut-over gate (ui/cdc_dlq_gate.py).
        # Copy-on-write (replaced, never mutated) so a worker thread can read the reference.
        # O(tables). Cleared only by Start over; deliberately NOT by an attach or a stack
        # rename -- dead letters are about TARGET tables, whichever stack produced them.
        self.cdc_dlq_ledger: dict = {}
        # Per log group, where the tool's continuous dead-letter coverage BEGINS (epoch ms):
        # the start of the first successful read. Persisted with the ledger; the gate proves
        # "no dead letter was missed" by comparing it with the CDC start. Copy-on-write.
        self.cdc_dlq_read_from_ms: dict = {}
        # The last dead-letter read's outcome (core.cdc_dlq.CdcDlqReadRecord), promoted only
        # AFTER its records were folded, so a render never sees an "ok" read whose records
        # are not counted yet. Deliberately transient: a restarted session must read again.
        self.cdc_dlq_last_read = None
        # The record the Data Migration poll's worker produced, waiting for the loop-side
        # fold in _apply_cdc_status to promote it.
        self.cdc_dlq_pending_read = None
        # Single-flight claim for the cut-over gate's background dead-letter read
        # (monotonic start time; None when no read runs). A claim older than the watchdog
        # bound is re-taken, so a hung read never pins "checking".
        self._cdc_dlq_check_started: Optional[float] = None
        # The controller the cut-over gate's read built when the session had none (identity,
        # controller). Kept HERE -- never as ``cdc_controller``, so Cut over and Validation
        # still have no discovery side effects -- so a page-capped read's continuation
        # survives to the next "Check again" instead of re-scanning the same pages. Transient.
        self.cdc_dlq_gate_reader: Optional[tuple] = None
        # Re-keyed PostgreSQL tables whose REPLICA IDENTITY the runtime re-check read below
        # FULL (ui/cdc_replica_identity_gate.py): {table: CdcReplicaIdentityFinding}. A
        # DELETE lost while it was below FULL is never replayed, so this is PERSISTED on the
        # session snapshot and cleared only by Start over -- never by an attach or a stack
        # rename (the loss is about TARGET tables). Copy-on-write: replaced, never mutated.
        self.cdc_replica_identity_findings: dict = {}
        # The DEPLOYED stack's re-keyed tables ({table: composite key columns}), from the same
        # discovery describe as cdc_deployed_slot_name: MessageKeyColumns united with
        # SinkKeyColumns (core.cdc.deployed_rekey_map), however each is routed. ``None`` =
        # unknown / no stack; ``{}`` = deployed with no re-key. Persisted so Validation and
        # Cut over can scope the re-check after a restore.
        self.cdc_deployed_message_key_columns: Optional[dict[str, list[str]]] = None
        # The last runtime REPLICA IDENTITY read (a CdcReplicaIdentityProbe). Transient: a
        # restarted session must read again (and needs the source password to).
        self.cdc_replica_identity_last_probe = None
        # Single-flight claim for that read (monotonic start; None when none runs), with a
        # watchdog of twice the TTL so a hung read never pins "re-reading".
        self._cdc_replica_identity_probe_started: Optional[float] = None
        # Captured tables whose SOURCE definition stopped matching the migration's baseline
        # while CDC ran (ui/cdc_catalog_drift_gate.py): {table: CdcCatalogDriftFinding}. A
        # change no rejected row reveals, so this is PERSISTED on the session snapshot and
        # cleared only by Start over -- never by an attach or a stack rename (it is about the
        # source and the TARGET tables). Copy-on-write: replaced, never mutated.
        self.cdc_catalog_drift_findings: dict = {}
        # Per compared table, the 16-hex digest of the baseline facts the last successful
        # read compared with ({table: digest}); a change means a Step 1 re-run redefined the
        # table. Persisted, copy-on-write.
        self.cdc_catalog_drift_digests: dict = {}
        # Columns added to Aurora DSQL through Fix target schema ({table: {column: source
        # catalog type}}), part of the re-check's baseline. Written only from the APPLIED
        # outcomes of that dialog. Persisted, copy-on-write.
        self.cdc_target_added_columns: dict = {}
        # The last runtime source-schema read (a CdcCatalogDriftProbe). Transient: a
        # restarted session must read again (and needs the source password to).
        self.cdc_catalog_drift_last_probe = None
        # Single-flight claim for that read (monotonic start; None when none runs), with a
        # watchdog of twice the TTL so a hung read never pins "re-reading".
        self._cdc_catalog_drift_probe_started: Optional[float] = None
        # Which migration a read belongs to: a fresh object per __init__, so Start over
        # (reset_in_place re-runs __init__ on this same object) gives it a new one, and a
        # read that started before it lands nowhere (never an object()-vs-int counter that
        # __init__ would reset to a value an old claim could share).
        self._cdc_catalog_drift_epoch: object = object()
        # Columns the SOURCE connector could not convert and therefore delivered as NULL
        # (the row itself was applied). Sticky, and keyed by (table, column, type): the
        # same value fails on every row that carries it, so the fact is "this column
        # arrives NULL", never a count. Held HERE rather than on the controller because
        # the controller is rebuilt on every CDC discovery probe. Deliberately separate
        # from every cdc_dlq_* field above -- these rows were NOT quarantined and must
        # never be added to the dead-letter depth a cut-over decision reads.
        self.cdc_value_conversion_failures: list = []
        # ...and the ones already written to the DURABLE activity log, so the 5 s poll
        # writes one line per column, once (same guard as cdc_dlq_audited_keys).
        self.cdc_value_conversion_audited_keys: set = set()
        # Why a connector stopped after it started, matched from its CloudWatch worker log
        # by the poll ({connector name: core.cdc_deployer.ConnectorLogDiagnosis}): the
        # cause on its Pipeline-health row and the runbook under it. Transient and scoped to
        # ONE failure episode -- never persisted (a restarted session re-reads it while the
        # task is still dead). Cleared when the connector runs again, at every Start/Retry
        # submission, and when the stack is gone or another one is targeted; deliberately
        # KEPT across Stop CDC, because the runbook's first step is Stop CDC and the cause
        # must still be on the Start card afterwards. Copy-on-write: replaced, never mutated.
        self.cdc_connector_diagnoses: dict = {}
        # Monotonic time of each connector's last log read (the throttle; single flight
        # across tabs, see claim_cdc_diagnosis_read), and the diagnoses already written to
        # the durable activity log (one line per episode).
        self._cdc_diagnosis_read_at: dict[str, float] = {}
        self._cdc_diagnoses_audited: frozenset = frozenset()
        # Which episode a read belongs to: every clear makes a new object, so a read that
        # was in flight when the episode ended lands nowhere (same idea as
        # _cdc_catalog_drift_epoch).
        self._cdc_diagnosis_epoch: object = object()
        # Outcome of the explicit "Apply foreign keys" action on this step:
        # (applied, skipped, failed), or None while it has not been run. Foreign keys are
        # no longer applied as part of the load (the orphan pre-check is O(child rows) and
        # was holding the END of the load for minutes), so the step has to SHOW that they
        # are still pending -- an unapplied constraint is invisible otherwise.
        self.fk_apply_result: Optional[tuple] = None
        # The FK action's OWN job id. It must NEVER be written to `job_id`: that slot is
        # what the Full Load panel and the CDC handoff read, so overwriting it made a
        # successful load's per-table rows, completion badge and EXPORT WATERMARK vanish the
        # moment the operator applied the foreign keys -- and CDC then reported "no usable
        # Full Load watermark", forcing a choice between CDC and referential integrity.
        self.fk_apply_job_id: Optional[str] = None
        # (done, total) foreign keys, for the action's progress while it runs.
        self.fk_apply_progress: Optional[tuple] = None
        # True when fk_apply_result was voided because the table set changed after the pass
        # (Full load only, G12): the keys it applied are still in place, so the card asks to
        # apply again for the changed set instead of saying they were never applied.
        self.fk_result_scope_changed: bool = False
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
        # The DEPLOYED PostgreSQL replication-slot name, read from the live stack's
        # ``PgSlotName`` parameter by the same phase probe. The derived name embeds a hash
        # of the stack name (``cdc_pg_slot.pg_slot_name``), so re-deriving it after the
        # stack name drifts -- an attach, a rename, a restore -- yields a DIFFERENT slot
        # and the health read then reports the real slot as missing. Every other PG path
        # already prefers the recorded/deployed name for exactly this reason
        # (``cdc_postgres.dispatch_source_config``,
        # ``cdc_deployer._drop_pg_source_replication``). Empty when no stack / no param.
        # Transient/session-only.
        self.cdc_deployed_slot_name: Optional[str] = None
        # The DEPLOYED stack's ColumnExcludeList, from the same phase probe. ``None`` when
        # there is no stack (or no such parameter); ``""`` is a real, empty value. Lets the
        # preview show what Start CDC will send: a stack deployed with the 0.1.561 form
        # keeps it (core.cdc.choose_column_exclude_value). Transient/session-only.
        self.cdc_deployed_column_exclude_list: Optional[str] = None
        # The DEPLOYED stack's PluginVersion, from the same phase probe. Start CDC never
        # re-registers the plugins, so the CDC step warns while it is older than the one this
        # version bundles (core.s3_provision.PLUGIN_VERSION). None: no stack, or not read.
        # Transient/session-only.
        self.cdc_deployed_plugin_version: Optional[str] = None
        # The binlog (file, pos) the stack's LAST Start offered the offset seeder, from the
        # same phase probe (Stop carries Watermark* forward). None: no stack / no seed.
        self.cdc_deployed_watermark: Optional[tuple[str, str]] = None
        # ...and the GTID set offered with it (WatermarkGtids, normalized; "" when it carried
        # none), from the same describe. The MySQL GTID start check judges a resume by it: a
        # stack offered a position with no GTID set streamed by file:position. None: no
        # recorded position, or not read (unknown).
        self.cdc_deployed_watermark_gtids: Optional[str] = None
        # MySQL: the source gtid_mode the stack's last Start positioned under (its
        # StartGtidMode, G20, upper-cased), from the same describe -- or the mode this
        # session's last Start sent. Kept apart from Watermark* (a Start with no start
        # point blanks those). The GTID start check reads it before any later Start: a
        # stack last started while gtid_mode was not ON cannot continue by GTID. None: no
        # record (an older stack, one never started) or not read. Transient.
        self.cdc_deployed_gtid_mode: Optional[str] = None
        # The LATEST binlog (file, pos) a Start offered the seeder on this stack. A Start
        # that fails after seeding leaves it in the offsets topic, and the seeder never
        # rewinds -- so a later, earlier start point would silently not apply.
        # Transient; cleared when the stack is gone.
        self.cdc_offered_seed: Optional[tuple[str, int]] = None
        # MySQL: the SnapshotMode of a stack whose connectors EXIST (MskBootstrapServers
        # set), from the same phase probe; None when stopped / infra-only / no stack. A
        # Start keeps it (core.cdc.mysql_snapshot_mode) so it never re-creates running
        # connectors just to change it. Transient.
        self.cdc_deployed_snapshot_mode: Optional[str] = None
        # MySQL: a schema_only resume of this stack failed because Debezium's schema
        # history is missing or incomplete, so the next Start runs recovery (after the
        # start-point check). Persisted; cleared by a successful Start and when the stack
        # is gone or another stack is targeted.
        self.cdc_history_missing: bool = False
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
        # Throttle markers for the logged applied-rows roll-up: the totals last WRITTEN to
        # the activity log and the monotonic clock reading when. On the state (not a module
        # global) so reset_in_place / Start over clears them -- a stale module-level marker
        # was exactly the v0.1.498 bug where CDC posted no events after a Start over.
        self.cdc_applied_logged_totals: Optional[tuple] = None
        self.cdc_applied_logged_at: Optional[float] = None
        # The dead-letter memos (_cdc_status.cdc_dlq_records / cdc_schema_drift_summary). On
        # the state so reset_in_place / Start over drops them: both key on the log key and a
        # record count, and a CDC-only "cdc:<stack>" key is the same across Start over, so a
        # memo that survived the reset would serve the previous migration's dead letters.
        self._cdc_dlq_records_cache: Optional[tuple] = None
        self._cdc_drift_summary_cache: Optional[tuple] = None
        # Start of the window the applied-ops above are summed over: the Full Load
        # watermark (this migration's gapless resume point), set when CDC starts. The
        # sink's *Applied CloudWatch metrics have a long retention, so a fixed trailing
        # window would sum ops from PRIOR CDC runs still inside it (e.g. earlier demo
        # workloads on the same stack) -- making a clean Full-Load->CDC run wrongly show
        # non-zero I/U/D. Windowing from here counts only post-Full-Load events. ``None``
        # -> the reader's default window (no watermark known yet).
        self.cdc_ops_window_start: Optional[datetime] = None
        # Per-table end-to-end replication lag (milliseconds) from the sink's
        # ``ReplicationLagMs`` CloudWatch metric (apply time minus source commit time).
        # Refreshed every CDC poll; drives the time-based "Stream lag" column (a far
        # more accurate signal than the MAX(pk) leading-edge fallback). name -> ms;
        # empty/absent when the metric is unavailable (older plugin) or the table is
        # idle/caught up.
        self.cdc_replication_lag_by_table: dict[str, int] = {}
        # PostgreSQL CDC only: the source logical replication slot's WAL-retention health
        # (a cdc_postgres.SlotHealth), refreshed when the source is read for the
        # consistency view. Drives the WAL-pressure warning panel; None when unknown or
        # for a MySQL source (which has no slot). Stored untyped to avoid importing the
        # PG-only model here.
        self.cdc_slot_health = None
        # When cdc_slot_health was READ (UTC). The slot is read only on the consistency-view
        # refresh -- never on the 5 s CDC poll -- so the panel re-renders a SNAPSHOT every
        # tick. Without the time it was presented as live state (see set_cdc_slot_health).
        self.cdc_slot_health_at: Optional[datetime] = None
        # When each table's SOURCE row estimate was last refreshed (UTC): table ->
        # datetime, or None when its statistics were never refreshed (PostgreSQL
        # reltuples = -1, the state behind a blank estimate). A table absent from the map
        # is unknown -- a MySQL source, or the read failed. Display-only metadata.
        self.source_estimate_stats_at: dict[str, Optional[datetime]] = {}
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
        # report "2 of 3". Preserved across a Start-over reset, like the marker (see
        # ``DataMigrationStore.reset_in_place`` -- it was NOT, until v0.1.482, and the
        # queue's only writer runs in the same handler as the reset, so every entry was
        # lost the moment it was written).
        self.cdc_teardown_queue: list[tuple[str, str]] = []
        # A FINISHED teardown the operator has not dismissed yet: {"kind", "stacks"}.
        # Completion used to be signalled only by a ui.notify toast, which hangs off a
        # ui.timer and so is gone after a refresh -- leaving no way to tell "finished"
        # from "never ran", for an operation that takes 15-45 min and is expected to be
        # left unattended. Held so the banner can report the result until it is closed,
        # a Start over clears it (``DataMigrationStore.reset_in_place``: it reports the
        # previous journey), or a deploy or Start of a stack it names begins: that stack
        # leaves it (``_drop_redeployed_teardown_done_locked``, G27).
        self.cdc_teardown_done: dict = {}
        # The stacks this session deployed or started since their latest teardown was
        # submitted (G27). The notice above leaves them out, also when it is recorded
        # later: the banner records it only on its next read of the settled job. Transient.
        self._cdc_redeployed_stacks: set[str] = set()
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
        # The credential-free reason the last CDC-state probe (describe_stacks) FAILED,
        # or None. Set when the read-only AWS check throws (bad creds/permissions/region
        # /throttle) so the "state not determined" notice can name the real cause and
        # offer a re-check, instead of always blaming a session restore and leaving the
        # user stuck. Cleared by a successful probe (``set_cdc_stack_phase``).
        self.cdc_probe_error: Optional[str] = None
        # True when the probed cdc-stack has ALREADY streamed, so its resume offset is
        # committed to the (Stop-surviving, fixed-name) offsets topic and Start CDC needs
        # NO watermark -- streaming resumes where it stopped. See
        # ``_status.cdc_has_committed_offset``. False until probed, so an unread stack
        # falls back to requiring a start point rather than claiming a resume point.
        self.cdc_has_committed_offset: bool = False
        # Other cdc-stacks (canonical ``dsql-cdc-*`` or legacy ``mysql-dsql-cdc-*``)
        # discovered in the account that the current session does NOT target
        # (name != ``cdc_stack_name``). Populated
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
        # Builder-time handles (not user state, never persisted; they outlive Start over,
        # see reset_in_place): the overlay-ledger store, the current effective target DDL
        # provider, the Start CDC notification, and the cut-over restore probe.
        self.overlay_ledger: Optional[object] = None
        self.effective_target_ddls: Optional[Callable[[], Optional[dict]]] = None
        self.on_cdc_start: Optional[Callable[[], None]] = None
        self.cutover_restore_in_flight: Optional[Callable[[], bool]] = None
        # The session's ValidationState (or None), so the CDC monitor can tell a REPLICA
        # IDENTITY finding already proven by a CHECKSUM from one still owed. Duck-typed:
        # data_migration never imports ui.validation.
        self.validation_state_provider: Optional[Callable[[], object]] = None
        # The Step 1 inventory's primary key of a table (``None`` when unknown), so the
        # REPLICA IDENTITY re-check leaves out a pure primary-key REORDER without a read --
        # the same judgement the Start prerequisite makes from the same inventory.
        self.source_primary_key_for: Optional[Callable[[str], Optional[list]]] = None
        # The source-schema re-check's baseline provider (a CatalogBaseline: the Step 1
        # inventory and the captured tables, or ``None`` without an inventory). Without it
        # the re-check is inert.
        self.cdc_catalog_baseline: Optional[Callable[[], object]] = None
        # Which CDC activity events have already been mirrored into the AI feed, per CDC
        # log key, so the ~5s monitor poll announces each transition ONCE. Lives HERE (not
        # in a module dict) so Start over's ``reset_in_place`` -> ``__init__`` clears it;
        # keyed by CDC log key so a Full Load retry that repins the key still re-announces.
        self.cdc_announced: "dict[str, set]" = {}

    def set_lob_exclusion(self, table: str, column: str, exclude: bool) -> None:
        """Toggle whether one oversized-LOB column is excluded from the migration.

        The selection is migration-wide (H13): it drops the column from both the
        Full Load INSERT list and CDC capture, so the two paths never disagree. This is
        the OPERATOR path: an exclusion made here is a new-semantics (overlay) entry;
        un-ticking a legacy entry drops its legacy mark (a re-tick makes it an overlay
        entry). A tick alone does not mark the ledger in use -- a run that overlays does.
        """
        with self._lock:
            current = self._excluded_lob_columns.setdefault(table, set())
            if exclude:
                current.add(column)
            else:
                current.discard(column)
                if not current:
                    self._excluded_lob_columns.pop(table, None)
            legacy = self._legacy_exclusions.get(table)
            if legacy is not None:
                legacy.discard(column)
                if not legacy:
                    self._legacy_exclusions.pop(table, None)
            facts = self._restored_exclusion_facts
            if facts is not None and (table, column) in facts.entries:
                self._restored_exclusion_facts = replace(
                    facts, entries=frozenset(facts.entries - {(table, column)})
                )

    def exclusion_edit_token(self) -> tuple:
        """What a popup opened against: the stored set, the rules and every fact a Save's
        legality depends on. A Save refuses when it changed (another tab saved, a stack was
        adopted or attached, a run or deploy started) -- reopen to review."""
        with self._lock:
            return self._edit_token()

    def _edit_token(self) -> tuple:
        """:meth:`exclusion_edit_token` with the lock already held."""
        return (
            tuple(sorted((t, tuple(sorted(c))) for t, c in self._excluded_lob_columns.items())),
            self._exclusion_rules_text,
            tuple(sorted(self._rule_excluded_tables)),
            tuple(self.selection.selected_tables),
            self.selection_touched,
            getattr(self, "job_id", None),
            getattr(self, "cdc_deploy_job_id", None),
            getattr(self, "cdc_teardown_job_id", None),
            getattr(self, "cdc_stack_name", None),
            getattr(self, "cdc_stack_phase", None),
        )

    def set_column_exclusions(
        self,
        resolution,
        *,
        text: str,
        opened_with: "Sequence[str]" = (),
        token: "Optional[tuple]" = None,
        lock_check: "Optional[Callable[[], Optional[str]]]" = None,
        convert_legacy: "Iterable[str]" = (),
        keep_selected: "Iterable[str]" = (),
    ) -> Optional[str]:
        """Store a popup Save: ``resolution`` (``core.exclusion_rules.RuleResolution``) is
        the ONLY input -- this is the rules' setter; ``set_lob_exclusion`` stays the raw
        path of the card and restore. Returns why it refused, or ``None`` once stored.

        Refuses a resolution that cannot be saved; ``lock_check`` (called first, outside
        the lock: it reads the lock state) reporting a lock; and -- under the lock, so the
        adoption/Attach/other-tab race cannot slip between check and write -- a change of
        :meth:`exclusion_edit_token` since the popup opened (``token``).

        Replaces the stored set with ``resolution.columns``; legacy marks and a pending
        classification keep only the entries still excluded. The TableSelection changes
        (touched) only when ``resolution.selected_after`` is set; the provenance and the
        rule text are stored; exactly ``opened_with`` leaves the pending lines (a line
        another tab added meanwhile stays). ``convert_legacy`` names tables a Change &
        reload drops and reloads so their omit-only entries become overlay entries (NULL).
        ``keep_selected`` names stored tables the popup cannot list (G16: loaded tables a
        stale target catalog hides, :func:`keep_unlisted_covered`): a selection the Save
        changes keeps them, so CDC does not silently stop capturing them. Never marks the
        overlay ledger in use: a run that overlays does.
        """
        if not resolution.can_save:
            return "The rules have errors: fix them before saving."
        if lock_check is not None:
            reason = lock_check()
            if reason:
                return reason
        before = (
            self._fk_scope_before() if resolution.selected_after is not None else None
        )
        with self._lock:
            if token is not None and token != self._edit_token():
                return (
                    "The exclusions changed since this window opened (another tab saved, a "
                    "CDC stack was attached, or a run started). Close it and open it again "
                    "to review the current set."
                )
            new = {t: set(c) for t, c in resolution.columns.items() if c}
            converted = set(convert_legacy or ())
            self._excluded_lob_columns = new
            self._legacy_exclusions = {
                t: c & new[t] for t, c in self._legacy_exclusions.items()
                if t in new and c & new[t] and t not in converted
            }
            facts = self._restored_exclusion_facts
            if facts is not None:
                self._restored_exclusion_facts = replace(facts, entries=frozenset(
                    (t, c) for t, c in facts.entries
                    if c in new.get(t, ()) and t not in converted))
            if resolution.selected_after is not None:
                after = list(resolution.selected_after)
                listed = set(after)
                after += [n for n in dict.fromkeys(keep_selected or ()) if n not in listed]
                self.selection = TableSelection(selected_tables=after)
                self.selection_touched = True
                self._clear_fk_result_on_scope_change(before, after)
            self._rule_excluded_tables = set(resolution.rule_excluded_after)
            self._rule_reticked_tables = set()
            self._exclusion_rules_text = text
            pending = list(self._exclusion_pending_rules)
            for line in opened_with:
                if line in pending:
                    pending.remove(line)
            self._exclusion_pending_rules = pending
            return None

    def exclusion_rules_text(self) -> Optional[str]:
        with self._lock:
            return self._exclusion_rules_text

    def rule_excluded_tables(self) -> frozenset:
        with self._lock:
            return frozenset(self._rule_excluded_tables)

    def rule_reticked_tables(self) -> frozenset:
        with self._lock:
            return frozenset(self._rule_reticked_tables)

    def note_tree_retick(self, tables: "Iterable[str]") -> None:
        """The operator re-ticked these in the tree: a rule-excluded one stops being
        rule-excluded (a deliberate override, named by the popup until the next Save)."""
        with self._lock:
            back = set(tables) & self._rule_excluded_tables
            self._rule_excluded_tables -= back
            self._rule_reticked_tables |= back

    def add_pending_rule(self, line: str) -> bool:
        """Queue a suggested rule line for the popup, unless the same rule (ignoring its
        comment and spacing) is already pending or saved. Returns whether it was added."""
        from dsql_migrator.core.exclusion_rules import split_rule_comment

        def _key(text: str) -> str:
            return " ".join(split_rule_comment(text)[0].split())

        if "\n" in line or "\r" in line:
            return False  # one rule per line: a line break would split it
        with self._lock:
            seen = {_key(x) for x in self._exclusion_pending_rules}
            seen |= {_key(x) for x in (self._exclusion_rules_text or "").splitlines()}
            if not _key(line) or _key(line) in seen:
                return False
            self._exclusion_pending_rules.append(line)
            return True

    def pending_rules(self) -> list[str]:
        with self._lock:
            return list(self._exclusion_pending_rules)

    def restore_exclusion_rules(
        self,
        text: Optional[str],
        rule_excluded_tables: "Iterable[str]" = (),
        pending: "Iterable[str]" = (),
        *,
        reticked: "Iterable[str]" = (),
    ) -> None:
        """Restore the re-editing fields from a snapshot (no operator semantics)."""
        with self._lock:
            self._exclusion_rules_text = text
            self._rule_excluded_tables = set(rule_excluded_tables or ())
            self._exclusion_pending_rules = list(pending or ())
            self._rule_reticked_tables = set(reticked or ())

    def restore_lob_exclusion(self, table: str, column: str) -> None:
        """Restore one stored exclusion from a snapshot (no operator semantics)."""
        with self._lock:
            self._excluded_lob_columns.setdefault(table, set()).add(column)

    def adopt_deployed_exclusions(
        self,
        columns: "Mapping[str, Sequence[str]]",
        *,
        legacy_columns: "Mapping[str, Sequence[str]]" = (),
        expect_stack: "Optional[str]" = None,
        expect_value: "Optional[str]" = None,
        startable_phases: "Sequence[str]" = (),
    ) -> bool:
        """Take an attached cdc-stack's excluded columns as this session's, in one step.

        Only into an EMPTY stored set with no classification pending and -- re-checked
        HERE, under the lock, because the caller decided after slow reads -- no Full Load
        job and no CDC operation started in the meantime: it never overwrites a selection a
        run used. ``legacy_columns`` (a subset) are marked omit-only; the rest are overlay
        entries, as if ticked. No operator side effects and no ledger-in-use mark -- the
        first run that overlays marks it. Returns whether anything was adopted.
        """
        with self._lock:
            if self._restored_exclusion_facts is not None:
                return False
            if getattr(self, "job_id", None) is not None:
                return False
            if getattr(self, "cdc_deploy_job_id", None):
                return False
            if any(self._excluded_lob_columns.values()):
                return False
            # Still the SAME attach the caller decided on: a Start over (or another
            # Attach) during its slow reads reset the stack name / value / phase.
            if expect_stack is not None and self.cdc_stack_name != expect_stack:
                return False
            if expect_value is not None and self.cdc_deployed_column_exclude_list != expect_value:
                return False
            if startable_phases and (
                self.cdc_stack_phase not in startable_phases
                or not self.cdc_stack_phase_checked
                or getattr(self, "cdc_teardown_job_id", None)
            ):
                return False
            adopted = {t: set(c) for t, c in (columns or {}).items() if c}
            if not adopted:
                return False
            self._excluded_lob_columns = adopted
            self._legacy_exclusions = {
                t: set(c) & adopted.get(t, set())
                for t, c in dict(legacy_columns or {}).items()
                if set(c) & adopted.get(t, set())
            }
            # True rule text for the adopted set (no false drift, no false legacy comment).
            from dsql_migrator.core.exclusion_rules import synthesize_rule_text

            lines = synthesize_rule_text(
                adopted, self._legacy_exclusions,
                adopted_from=expect_stack or self.cdc_stack_name or "the attached stack",
            )
            if lines:
                before = (self._exclusion_rules_text or "").rstrip("\n")
                self._exclusion_rules_text = f"{before}\n{lines}" if before else lines
            return True

    def legacy_exclusions(self) -> dict[str, frozenset[str]]:
        """The omit-only entries: the classified legacy set UNION a still-pending
        classification's entries (pending behaves as legacy), within the stored set."""
        with self._lock:
            merged: dict[str, set[str]] = {t: set(c) for t, c in self._legacy_exclusions.items()}
            facts = self._restored_exclusion_facts
            if facts is not None:
                for table, column in facts.entries:
                    merged.setdefault(table, set()).add(column)
            out: dict[str, frozenset[str]] = {}
            for table, columns in merged.items():
                kept = columns & self._excluded_lob_columns.get(table, set())
                if kept:
                    out[table] = frozenset(kept)
            return out

    def final_legacy_exclusions(self) -> dict[str, frozenset[str]]:
        """The classified legacy set only (what the snapshot stamps)."""
        with self._lock:
            return {
                t: frozenset(c & self._excluded_lob_columns.get(t, set()))
                for t, c in self._legacy_exclusions.items()
                if c & self._excluded_lob_columns.get(t, set())
            }

    def exclusion_classification_pending(self) -> bool:
        with self._lock:
            return self._restored_exclusion_facts is not None

    def restored_exclusion_facts(self) -> Optional[object]:
        with self._lock:
            return self._restored_exclusion_facts

    def set_restored_exclusion_facts(self, facts: Optional[object]) -> None:
        with self._lock:
            self._restored_exclusion_facts = facts

    def finalize_exclusion_classification(self, *, legacy: bool) -> None:
        """Settle a pending classification: legacy keeps the restored entries omit-only;
        an upgrade makes them overlay entries (the first run that overlays them marks the
        ledger in use)."""
        with self._lock:
            facts = self._restored_exclusion_facts
            if facts is None:
                return
            if legacy:
                self._legacy_exclusions = {}
                for table, column in facts.entries:
                    if column in self._excluded_lob_columns.get(table, set()):
                        self._legacy_exclusions.setdefault(table, set()).add(column)
            else:
                self._legacy_exclusions = {}
            self._restored_exclusion_facts = None

    def restore_legacy_exclusions(self, flat: "Sequence[str]") -> None:
        """Restore the stamped legacy list (``"table:column"``), within the stored set."""
        with self._lock:
            self._legacy_exclusions = {}
            for entry in flat or ():
                table, sep, column = entry.partition(":")
                if sep and table and column and column in self._excluded_lob_columns.get(table, set()):
                    self._legacy_exclusions.setdefault(table, set()).add(column)

    @property
    def overlay_ledger_in_use(self) -> bool:
        with self._lock:
            return self._overlay_ledger_in_use

    def mark_overlay_ledger_in_use(self) -> None:
        """A UI ledger read found a record for this target."""
        with self._lock:
            self._overlay_ledger_in_use = True

    def restore_overlay_ledger_in_use(self, value: bool) -> None:
        with self._lock:
            self._overlay_ledger_in_use = bool(value)

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

    def set_cdc_preserved_foreign_keys(
        self, preserved_foreign_keys: "dict[str, list[str]]"
    ) -> None:
        """Record which FK constraint names THIS migration owns (table -> names).

        Replaces the whole map (recomputed from the applied Schema Conversion on each
        render, like ``set_cdc_message_key_columns``). The CDC-start precondition needs
        it to tell an FK this migration rendered -- safe to remove before streaming and
        re-created at cut over -- from one the user created, which the tool must never
        drop. Empty means this migration accounts for no foreign keys.
        """
        with self._lock:
            self._cdc_preserved_foreign_keys = {
                table: list(names) for table, names in preserved_foreign_keys.items()
            }

    def set_applied_conversions(
        self, applied: "dict[str, object]", *, preserve_foreign_keys: bool
    ) -> None:
        """Record the applied Schema Conversion for the exclusion rule (see __init__)."""
        with self._lock:
            self._applied_conversions = dict(applied)
            self._preserve_foreign_keys = bool(preserve_foreign_keys)

    def applied_conversions(self) -> "tuple[Mapping[str, object], bool]":
        """``(applied conversions, preserve_foreign_keys)``: live, else the last render's."""
        inputs = self.exclusion_inputs()
        if inputs is not None:
            return inputs.applied, inputs.preserve_foreign_keys
        with self._lock:
            return dict(self._applied_conversions), self._preserve_foreign_keys

    def set_exclusion_inputs_resolver(self, resolver) -> None:
        """Register the screen's live exclusion-inputs function (see the attribute)."""
        with self._lock:
            self._exclusion_inputs_resolver = resolver

    def exclusion_inputs(self):
        """The live exclusion inputs, or ``None`` (no resolver, or it cannot resolve).

        Called OUTSIDE the state lock: the resolver reads this state itself.
        """
        resolver = self._exclusion_inputs_resolver
        if resolver is None:
            return None
        try:
            return resolver()
        except Exception:  # noqa: BLE001 - a gate must fall back, never crash a render
            _LOGGER.warning("exclusion inputs could not be resolved", exc_info=True)
            return None

    def live_target_columns(self) -> "Optional[Mapping[str, frozenset[str]]]":
        """Each live target table's columns (from the target catalog), or ``None``."""
        inputs = self.exclusion_inputs()
        return None if inputs is None else inputs.live_columns

    def set_resolved_selection(self, names: "Sequence[str]") -> None:
        """Record the table selection the render resolved (see ``_resolved_selection``)."""
        with self._lock:
            self._resolved_selection = list(names)

    def resolved_selection(self) -> "Optional[list[str]]":
        """The resolved table selection: live, else the last render's, else ``None``."""
        inputs = self.exclusion_inputs()
        if inputs is not None:
            return list(inputs.selected)
        with self._lock:
            return None if self._resolved_selection is None else list(self._resolved_selection)

    def cdc_preserved_foreign_keys(self) -> "dict[str, list[str]]":
        """Return a copy of the FK constraint names this migration owns."""
        with self._lock:
            return {
                table: list(names)
                for table, names in self._cdc_preserved_foreign_keys.items()
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
        fingerprints: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Record the manual CDC start-position override (GTID and/or binlog).

        Blank/``None`` values clear that field. The GTID set is stored normalized
        (:func:`~dsql_migrator.core.cdc_coords.normalize_gtid_set`: no whitespace or
        comments), the rest verbatim; the UI validates the strings before calling this
        (advisory) and :meth:`cdc_start_override` assembles a :class:`CdcResumePoint` from
        whatever is set. Start CDC offers the GTID set with the binlog file:position, and
        on a ``gtid_mode=ON`` source it is what Debezium starts after.

        ``fingerprints`` are the captured tables' start-point fingerprints recorded for
        exactly this binlog file:position (Fetch current position). They are REPLACED on
        every call -- cleared when omitted or when there is no binlog file:position -- so
        they can never describe a different point than the one stored.
        """
        file_value = (binlog_file or "").strip() or None
        with self._lock:
            self._cdc_start_gtid = normalize_gtid_set(gtid) or None
            self._cdc_start_binlog_file = file_value
            self._cdc_start_binlog_pos = binlog_pos
            self._cdc_start_fingerprints = (
                {str(k): str(v) for k, v in dict(fingerprints or {}).items()}
                if file_value and binlog_pos is not None
                else {}
            )

    def cdc_start_fingerprints(self) -> "dict[str, str]":
        """A copy of the Manual start point's recorded fingerprints (``{}``: none)."""
        with self._lock:
            return dict(self._cdc_start_fingerprints)

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
        progress UI can label the right stage set; cleared with the job id. A deploy
        (``"infra"``) or Start (``"start"``) job targets ``cdc_stack_name`` and takes that
        stack out of the finished-teardown notice (see
        :meth:`_drop_redeployed_teardown_done_locked`).
        """
        with self._lock:
            self.cdc_deploy_job_id = job_id
            self.cdc_action_kind = kind if job_id is not None else None
            if job_id is not None and kind in ("infra", "start") and self.cdc_stack_name:
                self._cdc_redeployed_stacks.add(self.cdc_stack_name)
                self._drop_redeployed_teardown_done_locked()

    def _drop_redeployed_teardown_done_locked(self) -> None:
        """Leave the stacks deployed or started since their teardown out of the
        finished-teardown notice (lock held).

        The notice ("CDC infrastructure deleted ... MSK / NAT billing has stopped", or
        "CDC connectors removed ... streaming has stopped") stayed on every screen after
        the SAME stack was deployed again and was streaming (live Seoul E2E, 2026-10-14).
        A stack leaves ``_cdc_redeployed_stacks`` when a new teardown of it is submitted
        (:meth:`set_cdc_teardown`, :meth:`set_cdc_teardown_queue`: the notice names only
        stacks from those two), so its next result is reported. Another stack's result
        stays. The in-flight teardown marker and queue are not touched.
        """
        done = self.cdc_teardown_done or {}
        stacks = list(done.get("stacks") or [])
        remaining = [s for s in stacks if s not in self._cdc_redeployed_stacks]
        if len(remaining) == len(stacks):
            return
        # Nothing left to report: clear it, or the copy falls back to "the cdc-stack".
        self.cdc_teardown_done = {**done, "stacks": remaining} if remaining else {}

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
            if job_id is not None and stack:
                # A deploy or Start before this teardown is older: its result is reported.
                self._cdc_redeployed_stacks.discard(stack)

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
            # As in set_cdc_teardown: every stack this teardown launched is reported.
            self._cdc_redeployed_stacks.difference_update(
                stack for _job_id, stack in self.cdc_teardown_queue
            )

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
        "it never ran". This survives a refresh and is cleared by
        :meth:`dismiss_cdc_teardown_done` (the banner's close button) or by Start over.

        The banner records this the next time it reads the settled job, so a deploy or
        Start of one of these stacks may already be under way: for example the first
        stack of a multi-stack teardown, deployed again while the next one is still
        deleting. That stack is left out (:meth:`_drop_redeployed_teardown_done_locked`).
        """
        with self._lock:
            self.cdc_teardown_done = {
                "kind": kind,
                "stacks": [str(s) for s in stacks if s],
            }
            self._drop_redeployed_teardown_done_locked()

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

        The name must be inside the ``dsql-cdc-*`` (canonical) or ``mysql-dsql-cdc-*``
        (legacy, still accepted) family the deploy role grants
        (see :func:`cdc_stack_name_is_valid`). An invalid name is rejected and the
        current name is kept, so a typo never makes the tool deploy resources the
        deploy role cannot manage. A previously-persisted legacy name stays valid and
        is reused as-is. One cdc-stack per source DB lets several migrations run
        concurrently in one account/region.
        """
        from dsql_migrator.core.cdc import cdc_stack_name_is_valid

        candidate = (name or "").strip()
        if not cdc_stack_name_is_valid(candidate):
            return False
        with self._lock:
            if candidate != self.cdc_stack_name:
                # The deployed re-key map belongs to the previously-targeted stack; the next
                # discovery reads this one's. REPLICA IDENTITY findings are kept: they are
                # about target tables, whichever stack produced the loss.
                self.cdc_deployed_message_key_columns = None
                # The snapshot-mode facts describe the previously-targeted stack too, and
                # so does its gtid_mode record (G20), and its plugin version.
                self.cdc_deployed_snapshot_mode = None
                self.cdc_deployed_gtid_mode = None
                self.cdc_deployed_plugin_version = None
                self.cdc_history_missing = False
                # ...and so does the start offset a Start offered its seeder (G26 repair): that
                # stack's offset must not refuse a seed on this one (Start's earlier-offer
                # check). The stack-phase probe reads a rolled-back stack's offer back from
                # its events.
                self.cdc_offered_seed = None
                # ...and so does why its connectors stopped (names embed the stack name).
                self._clear_cdc_connector_diagnoses_locked(None)
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
            self.cdc_probe_error = None  # a successful probe clears any prior failure
            if phase == "absent":
                self.cdc_offered_seed = None  # a new stack gets a new offsets topic
                # ...and a new schema history, and no connectors (nor why they stopped),
                # no gtid_mode record (G20), and the plugins its deploy registers.
                self.cdc_deployed_snapshot_mode = None
                self.cdc_deployed_gtid_mode = None
                self.cdc_deployed_plugin_version = None
                self.cdc_history_missing = False
                self._clear_cdc_connector_diagnoses_locked(None)

    def set_cdc_probe_error(self, message: Optional[str]) -> None:
        """Record why the read-only CDC-state probe failed (or clear it with None).

        Leaves ``cdc_stack_phase_checked`` False so the card stays "undetermined", but
        now with a real, actionable cause the notice can show + a re-check affordance.
        """
        with self._lock:
            self.cdc_probe_error = str(message)[:300] if message else None
            if message:
                # A failed probe makes the cached stack exclusion value unknown: judged as
                # "could not read" (re-check), never as an old value Start would refuse
                # with a destructive remedy.
                self.cdc_deployed_column_exclude_list = None

    def set_cdc_has_committed_offset(self, value: bool) -> None:
        """Cache whether the probed cdc-stack already holds a committed resume offset.

        Set from the SAME describe as the phase (see ``_probe_cdc_stack_phase``) so the
        two can never describe different stacks. When True, Start CDC must NOT require a
        watermark: the connector resumes from its own offsets topic.
        """
        with self._lock:
            self.cdc_has_committed_offset = bool(value)

    def set_cdc_other_stacks(self, stacks: list[tuple[str, str]]) -> None:
        """Cache other cdc-stacks (canonical ``dsql-cdc-*`` or legacy
        ``mysql-dsql-cdc-*``) found in the account whose name is NOT the one this
        session targets, so the CDC screen can offer to adopt an existing pipeline
        instead of deploying a duplicate."""
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
            self.cdc_deployed_slot_name = None
            # ...and so is its re-key map (REPLICA IDENTITY findings are kept, see above).
            self.cdc_deployed_message_key_columns = None
            self.cdc_deployed_column_exclude_list = None
            self.cdc_deployed_plugin_version = None
            self.cdc_deployed_watermark = None
            self.cdc_deployed_watermark_gtids = None
            self.cdc_deployed_gtid_mode = None
            self.cdc_offered_seed = None
            self.cdc_deployed_snapshot_mode = None
            self.cdc_history_missing = False
            self._clear_cdc_connector_diagnoses_locked(None)
            # Applied-ops + replication-lag metrics are per-stack too; drop them so the
            # adopted stack's poll repopulates rather than showing the prior stack's.
            self.cdc_applied_ops_by_table = {}
            self.cdc_applied_logged_totals = None
            self.cdc_applied_logged_at = None
            self.cdc_ops_window_start = None
            self.cdc_replication_lag_by_table = {}
            self.cdc_replication_lag_series = []
            # Slot health belongs to the previously-targeted stack's slot.
            self.cdc_slot_health = None
            self.cdc_slot_health_at = None
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

    def set_fk_apply_job(self, job_id: Optional[str]) -> None:
        """Record the FK action's job id -- deliberately NOT ``job_id`` (see that slot)."""
        with self._lock:
            self.fk_apply_job_id = job_id

    def set_fk_apply_progress(self, done: int, total: int) -> None:
        """Record how many foreign keys the running action has finished."""
        with self._lock:
            self.fk_apply_progress = (int(done), int(total))

    def set_fk_apply_result(self, result: Optional[tuple]) -> None:
        """Record the "Apply foreign keys" outcome ``(applied, skipped, failed)``. Any write
        ends the "table set changed" reading (a new load, a Drop & reload, a new pass): its
        own copy then applies."""
        with self._lock:
            self.fk_apply_result = result
            self.fk_result_scope_changed = False

    def set_cdc_controller(
        self, controller: object, identity: Optional[tuple] = None
    ) -> None:
        """Inject the MSK Connect controller used to poll connector status.

        ``identity`` is the ``(region, aws_profile)`` it was built with, so the cache can
        be dropped when either changes instead of silently serving the old account.
        """
        with self._lock:
            self.cdc_controller = controller
            self.cdc_controller_identity = identity if controller is not None else None

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

    def set_cdc_deployed_slot_name(self, slot_name: Optional[str]) -> None:
        """Record the DEPLOYED PostgreSQL slot name from the live stack's parameters.

        See :attr:`cdc_deployed_slot_name`: the slot-health read must use the name the
        connector actually holds, not one re-derived from a stack name that may have
        drifted since Full Load created the slot.
        """
        with self._lock:
            cleaned = (slot_name or "").strip()
            self.cdc_deployed_slot_name = cleaned or None

    def set_cdc_deployed_message_key_columns(
        self,
        value: "Optional[Mapping[str, Sequence[str]]]",
        now: Optional[datetime] = None,
    ) -> None:
        """Record the DEPLOYED stack's re-key map (``None``: no stack / neither parameter).

        When the map is known, an OPEN REPLICA IDENTITY finding whose table CDC no longer
        re-keys (absent from the map, or keyed within its source primary key) is closed as
        ``"not-rekeyed"`` at ``now``: no further DELETE can be lost, but the ones lost while
        it was below FULL still need a CHECKSUM after that time. One activity line each.
        """
        closed: list = []
        with self._lock:
            if value is None:
                self.cdc_deployed_message_key_columns = None
                return
            deployed = {
                str(table): [str(c) for c in (cols or ()) if c]
                for table, cols in dict(value).items()
            }
            self.cdc_deployed_message_key_columns = deployed
            stamp = now or datetime.now(timezone.utc)
            findings = dict(self.cdc_replica_identity_findings)
            for table, finding in list(findings.items()):
                if getattr(finding, "restored_at", None) is not None:
                    continue
                key = deployed.get(table) or []
                if key and not set(key) <= set(getattr(finding, "primary_key", None) or ()):
                    continue
                findings[table] = finding.model_copy(
                    update={"restored_at": stamp, "restored_by": "not-rekeyed"}
                )
                closed.append((table, findings[table]))
            if closed:
                self.cdc_replica_identity_findings = findings
        if closed:
            from dsql_migrator.ui.cdc_replica_identity_gate import (
                ReplicaIdentityTransition,
                log_replica_identity_transitions,
            )

            log_replica_identity_transitions(
                [
                    ReplicaIdentityTransition(kind="not-rekeyed", table=t, finding=f)
                    for t, f in closed
                ]
            )

    def apply_cdc_replica_identity_probe(self, probe: object) -> list:
        """Land a runtime REPLICA IDENTITY read (on the loop): store it as the last probe and
        fold it into the findings (copy-on-write). Returns the transitions for the caller to
        log AFTER the lock (one activity line each, never per poll)."""
        from dsql_migrator.ui.cdc_replica_identity_gate import fold_replica_identity_probe

        with self._lock:
            self.cdc_replica_identity_last_probe = probe
            findings, transitions = fold_replica_identity_probe(
                self.cdc_replica_identity_findings, probe
            )
            if transitions or findings != self.cdc_replica_identity_findings:
                self.cdc_replica_identity_findings = findings
        return list(transitions)

    def _cdc_replica_identity_probe_running_locked(self, now: float) -> bool:
        from dsql_migrator.ui.cdc_replica_identity_gate import (
            CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS,
        )

        started = self._cdc_replica_identity_probe_started
        return started is not None and now - started <= 2 * CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS

    def begin_cdc_replica_identity_probe(self, now_monotonic: Optional[float] = None) -> bool:
        """Claim the runtime REPLICA IDENTITY read (single flight across tabs)."""
        import time as _time

        now = _time.monotonic() if now_monotonic is None else now_monotonic
        with self._lock:
            if self._cdc_replica_identity_probe_running_locked(now):
                return False
            self._cdc_replica_identity_probe_started = now
            return True

    def finish_cdc_replica_identity_probe(self) -> None:
        with self._lock:
            self._cdc_replica_identity_probe_started = None

    @property
    def cdc_replica_identity_probe_running(self) -> bool:
        """A runtime REPLICA IDENTITY read is in flight."""
        import time as _time

        with self._lock:
            return self._cdc_replica_identity_probe_running_locked(_time.monotonic())

    def apply_cdc_catalog_drift_probe(self, probe: object, epoch: object = None) -> list:
        """Land a runtime source-schema read (on the loop): store it as the last probe and
        fold it into the findings and digests (copy-on-write). Returns the transitions for
        the caller to log AFTER the lock (one activity line per kind, never per poll).

        ``epoch`` is the migration the read was claimed for (its request's): a read that
        started before Start over is dropped -- nothing stored, nothing folded, nothing to
        log -- so it can never open a finding in the new migration."""
        from dsql_migrator.ui.cdc_catalog_drift_gate import fold_catalog_drift_probe

        with self._lock:
            if epoch is not None and epoch is not self._cdc_catalog_drift_epoch:
                return []
            self.cdc_catalog_drift_last_probe = probe
            findings, digests, transitions = fold_catalog_drift_probe(
                self.cdc_catalog_drift_findings, self.cdc_catalog_drift_digests, probe
            )
            if transitions or findings != self.cdc_catalog_drift_findings:
                self.cdc_catalog_drift_findings = findings
            if digests != self.cdc_catalog_drift_digests:
                self.cdc_catalog_drift_digests = digests
        return list(transitions)

    def record_cdc_target_added_columns(
        self, table: str, columns: "Mapping[str, str]"
    ) -> None:
        """Record columns Fix target schema APPLIED to ``table`` on Aurora DSQL ({column:
        source catalog type}), so the source-schema re-check counts them as baseline columns
        (resolved drift, not new drift). Copy-on-write; merged per table."""
        cleaned = {str(c): str(t) for c, t in dict(columns or {}).items() if c}
        if not table or not cleaned:
            return
        with self._lock:
            current = dict(self.cdc_target_added_columns)
            merged = dict(current.get(str(table)) or {})
            merged.update(cleaned)
            current[str(table)] = merged
            self.cdc_target_added_columns = current

    def _cdc_catalog_drift_probe_running_locked(self, now: float) -> bool:
        from dsql_migrator.ui.cdc_catalog_drift_gate import (
            CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS,
        )

        started = self._cdc_catalog_drift_probe_started
        return started is not None and now - started <= 2 * CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS

    def begin_cdc_catalog_drift_probe(self, now_monotonic: Optional[float] = None) -> bool:
        """Claim the runtime source-schema read (single flight across tabs)."""
        import time as _time

        now = _time.monotonic() if now_monotonic is None else now_monotonic
        with self._lock:
            if self._cdc_catalog_drift_probe_running_locked(now):
                return False
            self._cdc_catalog_drift_probe_started = now
            return True

    def finish_cdc_catalog_drift_probe(self, epoch: object = None) -> None:
        """Release the claim -- unless ``epoch`` names a migration Start over has replaced
        (that read's claim went with it; never release one taken since)."""
        with self._lock:
            if epoch is not None and epoch is not self._cdc_catalog_drift_epoch:
                return
            self._cdc_catalog_drift_probe_started = None

    @property
    def cdc_catalog_drift_probe_running(self) -> bool:
        """A runtime source-schema read is in flight."""
        import time as _time

        with self._lock:
            return self._cdc_catalog_drift_probe_running_locked(_time.monotonic())

    def set_cdc_deployed_column_exclude_list(self, value: Optional[str]) -> None:
        """Record the DEPLOYED stack's ColumnExcludeList (``None``: no stack/param)."""
        with self._lock:
            self.cdc_deployed_column_exclude_list = None if value is None else str(value)

    def set_cdc_deployed_plugin_version(self, value: Optional[str]) -> None:
        """Record the DEPLOYED stack's PluginVersion (blank or ``None``: no stack/param)."""
        text = str(value or "").strip()
        with self._lock:
            self.cdc_deployed_plugin_version = text or None

    def note_cdc_offered_seed(self, seed) -> None:
        """Record a seed a Start offers the seeder, keeping the LATEST one (binlog order)."""
        from dsql_migrator.core.cdc_kafka_prep import offset_already_at_or_past

        f = getattr(seed, "binlog_file", None)
        pos = getattr(seed, "binlog_position", None)
        if not f or pos is None:
            return
        with self._lock:
            cur = self.cdc_offered_seed
            if cur is None or offset_already_at_or_past(
                {"file": f, "pos": int(pos)}, {"file": cur[0], "pos": int(cur[1])}
            ):
                self.cdc_offered_seed = (str(f), int(pos))

    def set_cdc_deployed_watermark(
        self, value: Optional[tuple[str, str]], *, gtids: Optional[str] = None
    ) -> None:
        """Record the binlog (file, pos) the deployed stack was last started from, and the
        GTID set offered with it (``gtids``: ``""`` for none; ``None``: not known)."""
        with self._lock:
            self.cdc_deployed_watermark = tuple(value) if value else None  # type: ignore[assignment]
            self.cdc_deployed_watermark_gtids = (
                str(gtids) if value and gtids is not None else None
            )

    def set_cdc_deployed_gtid_mode(self, value: Optional[str]) -> None:
        """Record the source gtid_mode the stack's last MySQL Start positioned under (its
        StartGtidMode, G20; upper-cased, at most 32 characters). Blank or ``None``: no
        record."""
        text = str(value or "").strip().upper()[:32]
        with self._lock:
            self.cdc_deployed_gtid_mode = text or None

    def set_cdc_deployed_snapshot_mode(self, value: Optional[str]) -> None:
        """Record the SnapshotMode of a stack whose connectors exist (``None``: none)."""
        with self._lock:
            self.cdc_deployed_snapshot_mode = str(value) if value else None

    def note_cdc_history_missing(self, value: bool) -> None:
        """Record whether a schema_only resume found this stack's schema history missing."""
        with self._lock:
            self.cdc_history_missing = bool(value)

    # -- why a connector stopped (Pipeline-health diagnosis, one failure episode) --------

    def claim_cdc_diagnosis_read(
        self, name: str, *, now: float, interval: float
    ) -> Optional[object]:
        """Claim the next worker-log read for ``name``; the episode token, or ``None``.

        ``None`` when ``name`` already has a diagnosis (sticky: a dead task stops logging,
        so re-reading cannot improve it) or was read less than ``interval`` seconds ago.
        Otherwise stamps the read time -- before the read, so concurrent polls of other
        tabs skip it (single flight) -- and returns the current epoch, which
        :meth:`record_cdc_connector_diagnosis` checks.
        """
        with self._lock:
            if name in self.cdc_connector_diagnoses:
                return None
            last = self._cdc_diagnosis_read_at.get(name)
            if last is not None and now - last < interval:
                return None
            self._cdc_diagnosis_read_at = {**self._cdc_diagnosis_read_at, name: now}
            return self._cdc_diagnosis_epoch

    def record_cdc_connector_diagnosis(self, name: str, diagnosis, *, epoch) -> None:
        """Hold ``diagnosis`` for ``name`` -- unless its read began before the last clear."""
        with self._lock:
            if epoch is not self._cdc_diagnosis_epoch:
                return
            if name in self.cdc_connector_diagnoses:
                return
            self.cdc_connector_diagnoses = {**self.cdc_connector_diagnoses, name: diagnosis}

    def clear_cdc_connector_diagnoses(self, names: Optional[Iterable[str]] = None) -> None:
        """End the failure episode of ``names`` (every connector when ``None``)."""
        with self._lock:
            self._clear_cdc_connector_diagnoses_locked(names)

    def _clear_cdc_connector_diagnoses_locked(self, names: Optional[Iterable[str]]) -> None:
        # Caller holds self._lock. ALWAYS a new epoch, even when nothing was held, so a read
        # already in flight cannot land after the clear.
        self._cdc_diagnosis_epoch = object()
        if names is None:
            self.cdc_connector_diagnoses = {}
            self._cdc_diagnosis_read_at = {}
            self._cdc_diagnoses_audited = frozenset()
            return
        drop = set(names)
        if not drop:
            return
        self.cdc_connector_diagnoses = {
            k: v for k, v in self.cdc_connector_diagnoses.items() if k not in drop
        }
        self._cdc_diagnosis_read_at = {
            k: v for k, v in self._cdc_diagnosis_read_at.items() if k not in drop
        }
        self._cdc_diagnoses_audited = frozenset(self._cdc_diagnoses_audited - drop)

    def take_unaudited_cdc_diagnoses(self) -> list:
        """The held ``(name, diagnosis)`` pairs not yet written to the activity log.

        Marks them written, so each episode gets ONE durable line however often the poll
        applies; the written set is pruned to the names still held.
        """
        with self._lock:
            held = self.cdc_connector_diagnoses
            fresh = [(n, d) for n, d in held.items() if n not in self._cdc_diagnoses_audited]
            self._cdc_diagnoses_audited = frozenset(
                n for n in (set(self._cdc_diagnoses_audited) | {n for n, _ in fresh})
                if n in held
            )
            return fresh

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

    def set_cdc_slot_health(self, health) -> None:
        """Record the PostgreSQL source replication slot's WAL-retention health.

        ``health`` is a :class:`~dsql_migrator.core.cdc_postgres.SlotHealth` (or ``None``
        when unknown / not a PostgreSQL source). Thread-safe: written from the read-only
        source-read worker, read by the render on the loop.

        Stamps the read time. The slot is read only when the operator refreshes the
        source/target counts, but the panel re-renders on every 5 s CDC tick, so a
        snapshot outlives the state it described. Live-observed: counts refreshed while CDC
        was stopped captured ``active=False``; after Start CDC the panel kept saying
        "Replication slot has no consumer" -- and advising deletion of the CDC
        infrastructure -- while the real slot was ``active=True`` and draining its
        backlog. The time lets the panel say what it is actually showing.
        """
        with self._lock:
            self.cdc_slot_health = health
            self.cdc_slot_health_at = (
                datetime.now(timezone.utc) if health is not None else None
            )

    def set_cdc_ops_window_start(self, ts: "Optional[datetime]") -> None:
        """Pin the start of the window the per-table applied-ops (I/U/D) are summed over.

        The CDC counts must reflect only events applied AFTER the Full Load watermark
        (this migration's gapless resume point) -- otherwise the sink's long-retention
        ``*Applied`` metrics let a fixed trailing window sum ops from PRIOR CDC runs
        still inside it (e.g. earlier demo workloads on the same stack), so a clean
        Full-Load->CDC run wrongly shows non-zero counts. ``ts`` is the watermark's
        snapshot timestamp; ``None`` (CDC-only / no watermark) falls back to now, so
        counting starts at this CDC start.
        """
        with self._lock:
            self.cdc_ops_window_start = (
                ts if ts is not None else datetime.now(timezone.utc)
            )

    def fold_cdc_dlq_ledger(self, errors: "Sequence[object]") -> None:
        """Count NEW dead letters into the per-table ledger (copy-on-write, see __init__).

        Called on the event loop with only the records not audited before, so a record
        that re-surfaces (a rebuilt controller, a re-read) is never counted twice.
        """
        if not errors:
            return
        from dsql_migrator.core.cdc_dlq import fold_dlq_ledger

        with self._lock:
            self.cdc_dlq_ledger = fold_dlq_ledger(
                self.cdc_dlq_ledger, errors, now=datetime.now(timezone.utc)
            )

    def promote_cdc_dlq_read(self, record: object) -> None:
        """Publish a dead-letter read's outcome once its records are folded.

        Also records where the coverage of that log group begins, the FIRST time a read
        proves it -- an earlier, wider start is never replaced by a later one.
        """
        if record is None:
            return
        with self._lock:
            self.cdc_dlq_last_read = record
            group = getattr(record, "log_group", None)
            start = getattr(record, "history_start_ms", None)
            if (
                getattr(record, "ok", False)
                and group
                and start is not None
                and group not in self.cdc_dlq_read_from_ms
            ):
                updated = dict(self.cdc_dlq_read_from_ms)
                updated[group] = int(start)
                self.cdc_dlq_read_from_ms = updated

    def _cdc_dlq_check_running_locked(self, now: float) -> bool:
        from dsql_migrator.ui.cdc_dlq_gate import CDC_DLQ_CHECK_TTL_SECONDS

        started = self._cdc_dlq_check_started
        return started is not None and now - started <= 2 * CDC_DLQ_CHECK_TTL_SECONDS

    def begin_cdc_dlq_check(self, now_monotonic: Optional[float] = None) -> bool:
        """Claim the cut-over gate's background dead-letter read (single flight)."""
        import time as _time

        now = _time.monotonic() if now_monotonic is None else now_monotonic
        with self._lock:
            if self._cdc_dlq_check_running_locked(now):
                return False
            self._cdc_dlq_check_started = now
            return True

    def finish_cdc_dlq_check(self) -> None:
        with self._lock:
            self._cdc_dlq_check_started = None

    @property
    def cdc_dlq_check_running(self) -> bool:
        """A background dead-letter read for the cut-over gate is in flight."""
        import time as _time

        with self._lock:
            return self._cdc_dlq_check_running_locked(_time.monotonic())

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
        """Record which of my connectors are RUNNING (vs still provisioning).

        Also INVALIDATES the replication-slot snapshot when the pipeline starts or stops
        (none running <-> some running). A slot read taken in the other pipeline state
        describes a consumer that no longer exists -- or one that did not exist yet -- so
        showing it is not "slightly old", it is the opposite of the truth: after a
        restart a stopped-time read says "no consumer" and steers toward deleting the
        infrastructure, which would drop the slot and the changes it is still holding.
        Clearing it leaves the panel silent (unknown) until the next source read, which is
        honest. A failed MSK read never reaches here (the caller leaves the names
        untouched), so an outage cannot fake a transition.
        """
        with self._lock:
            new = [n for n in names if n]
            if bool(new) != bool(self.cdc_connector_running_names):
                self.cdc_slot_health = None
                self.cdc_slot_health_at = None
            self.cdc_connector_running_names = new

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

    def set_source_estimate_stats_at(
        self, stats_at: "Mapping[str, Optional[datetime]]"
    ) -> None:
        """Record when each table's source row ESTIMATE was last refreshed.

        Written by the counts-refresh worker alongside the estimate itself, so the status
        view can say how old the estimate is and label a never-analyzed table instead of
        showing a bare dash. Thread-safe (worker writes, render reads).
        """
        with self._lock:
            self.source_estimate_stats_at = dict(stats_at)

    def set_tables_with_data(self, names: frozenset[str]) -> None:
        """Record the selected target tables the probe found already holding rows."""
        with self._lock:
            self._tables_with_data = names

    @property
    def tables_with_data(self) -> frozenset[str]:
        """Return the pre-existing (non-empty) selected target tables."""
        with self._lock:
            return self._tables_with_data

    def set_target_tables_present(self, names: Optional[frozenset[str]]) -> None:
        """Record which selected target tables EXIST (probe result), or ``None``.

        ``None`` = not probed / probe failed (no missing-table warning); a set = the
        target was read, so any selected table not in it is genuinely missing.
        """
        with self._lock:
            self._target_tables_present = names

    @property
    def target_tables_present(self) -> Optional[frozenset[str]]:
        """Return the probed set of existing target tables, or ``None`` if unprobed."""
        with self._lock:
            return self._target_tables_present

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

        Derived: the pre-existing tables when the run-wide choice is ``"drop"`` (else
        none: append keeps existing rows), plus the tables the Start dialog forced to
        Drop & reload for the NULL contract. This is the run's replace set.
        """
        with self._lock:
            return self._replace_union()

    def _replace_union(self) -> frozenset[str]:
        base = self._tables_with_data if self._reload_mode == "drop" else frozenset()
        return frozenset(base) | frozenset(self._exclusion_forced_replace)

    def take_replace_targets(self) -> frozenset[str]:
        """The replace set for the run being built (see :meth:`take_run_choice`)."""
        return self.take_run_choice()[0]

    def take_run_choice(self) -> "tuple[frozenset[str], Optional[dict[str, frozenset[str]]]]":
        """``(replace set, disclosed overlay)`` for the run being built; consumes the
        Start dialog's forced set and pin, never the operator's own drop choice (a retry /
        per-table Reload honours it). The forced set and the pin count only when the
        dialog's confirm handed them over; otherwise the pin is ``None``."""
        with self._lock:
            base = self._tables_with_data if self._reload_mode == "drop" else frozenset()
            confirmed = self._exclusion_forced_confirmed
            forced = frozenset(self._exclusion_forced_replace) if confirmed else frozenset()
            pinned = self._overlay_disclosed if confirmed else None
            self._exclusion_forced_replace = {}
            self._exclusion_forced_confirmed = False
            self._overlay_disclosed = None
            return frozenset(base) | forced, pinned

    def confirm_exclusion_forced_replace(self) -> None:
        """The Start dialog's confirm: its forced set and disclosed overlay now count."""
        with self._lock:
            self._exclusion_forced_confirmed = True

    def set_overlay_disclosed(self, disclosed: "Optional[Mapping[str, Iterable[str]]]") -> None:
        with self._lock:
            self._overlay_disclosed = (
                None if disclosed is None
                else {t: frozenset(c) for t, c in disclosed.items() if c}
            )

    @property
    def exclusion_forced_replace(self) -> dict[str, str]:
        with self._lock:
            return dict(self._exclusion_forced_replace)

    def set_exclusion_forced_replace(self, forced: "Mapping[str, str]") -> None:
        with self._lock:
            self._exclusion_forced_replace = dict(forced)

    def clear_exclusion_forced_replace(self) -> None:
        with self._lock:
            self._exclusion_forced_replace = {}
            self._exclusion_forced_confirmed = False
            self._overlay_disclosed = None

    def set_selection(self, selection: TableSelection) -> None:
        """Replace the table selection for the sub-flow (marks it user-touched)."""
        before = self._fk_scope_before()
        with self._lock:
            self.selection = selection
            self.selection_touched = True
            self._clear_fk_result_on_scope_change(before, selection.selected_tables)

    def _fk_scope_before(self) -> "Optional[list[str]]":
        """The effective selection before a change, when a previous foreign-key result
        could go stale (Full load only after a Full Load, G12); else ``None``. Resolved
        OUTSIDE the (non-reentrant) lock: the resolver reads this state."""
        if (
            getattr(self, "fk_apply_result", None) is None
            or getattr(self, "job_id", None) is None
            or self.migration_type is not MigrationType.FULL_LOAD_ONLY
        ):
            return None
        return self.resolved_selection()

    def _clear_fk_result_on_scope_change(
        self, before: "Optional[list[str]]", after: "Sequence[str]"
    ) -> None:
        """With the lock held: a real change of the table set after an "Apply foreign
        keys" pass voids its result -- the card compared counts, so swapping a table with N
        keys for another with N read "in place" for keys never applied. The card then
        offers Apply again (idempotent: an existing constraint is a no-op), saying why
        (``fk_result_scope_changed``) instead of "not yet applied".

        A pure REMOVAL after a COMPLETE pass keeps the result: unticking a table can only
        shrink the keys in scope (``foreign_keys_in_scope``), every one of which that pass
        settled, and nothing on Aurora DSQL changes. A stopped pass (fewer settled than its
        own total) is voided even then: its unreached keys could all leave the scope while
        the count still reads "in place" for keys that do not exist."""
        if before is None or self.fk_apply_result is None:
            return
        before_set, after_set = set(before), set(after)
        if before_set == after_set:
            return
        settled = sum(int(n or 0) for n in tuple(self.fk_apply_result)[:3])
        progress = self.fk_apply_progress
        complete = progress is not None and settled >= int(tuple(progress)[1] or 0)
        # An EMPTY selection is "every table" to the foreign-key scope: never a shrink.
        if after_set and after_set < before_set and complete:
            return
        self.fk_apply_result = None
        self.fk_apply_progress = (0, 0)
        self.fk_result_scope_changed = True

    def selection_snapshot(self) -> "tuple[tuple[str, ...], bool]":
        """``(selected tables, touched)`` now, for :meth:`pin_checked_selection`."""
        with self._lock:
            return tuple(self.selection.selected_tables), bool(self.selection_touched)

    def pin_checked_selection(
        self, names: "Sequence[str]", *, expected: "tuple[tuple[str, ...], bool]"
    ) -> bool:
        """Pin the table set a prerequisite run covered -- only if the selection is still
        what it was when the run started (``expected``, from :meth:`selection_snapshot`).
        A Save of table rules or a tree tick during the run already left an explicit
        selection; pinning the click-time set over it would undo that change (a
        rule-unselected table loaded again). Atomic under the lock.

        Never after a Full Load in a CDC type with an explicit (touched) selection (G16):
        CDC captures the loaded tables still STORED, and ``names`` is filtered to the
        tables migratable now -- pinning it could silently drop a loaded table (a stale
        target catalog) from CDC, even from a started pipeline at its next Start or Retry.
        Only the operator narrows a CDC set; Full load only and pre-load pins are as
        before."""
        with self._lock:
            if (tuple(self.selection.selected_tables), bool(self.selection_touched)) != expected:
                return False
            if (
                getattr(self, "job_id", None) is not None
                and self.migration_type is not MigrationType.FULL_LOAD_ONLY
                and self.selection_touched
            ):
                return False
            self.selection = TableSelection(selected_tables=list(names))
            self.selection_touched = True
            return True

    def set_accept_quarantined_rows(
        self, accepted: bool, *, gap: "Optional[int]" = None
    ) -> None:
        """Record whether permanently-quarantined rows are accepted (UI thread).

        ``gap`` is the row count the operator actually consented to losing. Without it the
        flag was STICKY and UNSCOPED: one acceptance of a 3-row gap silently auto-accepted
        every later run's gap, of any size and in any table, so a second load that dropped
        thousands completed as a success the operator had never seen -- and the banner still
        said "you accepted that gap". Recording the number lets the finaliser re-ask when
        the gap GREW. Clearing the flag clears the number with it.
        """
        with self._lock:
            self.accept_quarantined_rows = accepted
            self.accepted_quarantine_rows = int(gap) if accepted and gap is not None else None

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
        # Its sub-steps differ, so a review of the previous type's checks is over.
        self.prereq_review_open = False

    def open_prereq_review(self) -> None:
        """A prerequisite run just ended: keep its results open (UI thread)."""
        self.prereq_review_open = True

    def close_prereq_review(self) -> None:
        """The operator moved on from the prerequisite results (UI thread)."""
        self.prereq_review_open = False

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
            self._prereq_report_checked_at[mode] = datetime.now(timezone.utc)
            # The value-size answers refresh the exclusion popup's size labels.
            for result in getattr(report, "results", None) or ():
                measured = getattr(result, "measured", None)
                if measured and result.target:
                    limits = getattr(result, "measured_limits", None) or {}
                    for column, over in measured.items():
                        self._value_size_cache[(result.target, column)] = (
                            "over" if over else "ok", self._prereq_report_checked_at[mode],
                            limits.get(column))
            self._prereq_report_lob_exclusions[mode] = {
                table: frozenset(cols)
                for table, cols in self._excluded_lob_columns.items()
            }

    def exclusion_drift_text(self) -> Optional[str]:
        return self._exclusion_drift_text

    def set_exclusion_drift_text(self, text: Optional[str]) -> None:
        self._exclusion_drift_text = text

    def value_sizes(self) -> "dict[tuple[str, str], tuple[str, datetime, Optional[int]]]":
        """The measured value sizes ((table, column) -> ("over" | "ok" | "unknown", when,
        the limit in bytes it was measured against, or None))."""
        with self._lock:
            return dict(self._value_size_cache)

    def record_value_sizes(
        self, table: str, answer: "Mapping[str, bool]", asked: "Iterable[str]" = (),
        limits: "Optional[Mapping[str, int]]" = None,
    ) -> None:
        """Record one table's measured columns (the popup's Check sizes), each with the
        limit it was measured against (``limits``). A column ``asked`` but absent from
        ``answer`` was not determined -- recorded as such only when no earlier measurement
        for the SAME limit exists: a pass that did not finish never undoes a result, and
        never keeps one measured against another limit."""
        now = datetime.now(timezone.utc)
        limits = limits or {}
        with self._lock:
            for column in asked or ():
                key = (table, column)
                earlier = self._value_size_cache.get(key)
                if column not in (answer or {}) and (
                        earlier is None or earlier[2] != limits.get(column)):
                    self._value_size_cache[key] = ("unknown", now, limits.get(column))
            for column, over in (answer or {}).items():
                self._value_size_cache[(table, column)] = (
                    "over" if over else "ok", now, limits.get(column))

    def add_size_check_listener(self, listener: "Callable[[], None]") -> None:
        with self._lock:
            if not any(x is listener for x in self._size_check_listeners):
                self._size_check_listeners.append(listener)

    def clear_size_check_listener(self, listener: "Callable[[], None]") -> None:
        with self._lock:
            self._size_check_listeners = [
                x for x in self._size_check_listeners if x is not listener]

    def size_check_listeners(self) -> "tuple[Callable[[], None], ...]":
        with self._lock:
            return tuple(self._size_check_listeners)

    def begin_size_check(self) -> bool:
        """Start a Check sizes pass unless one runs (single-flight)."""
        with self._lock:
            if self._size_check_running:
                return False
            self._size_check_running = True
            self._size_check_cancel = False
            return True

    def end_size_check(self) -> None:
        with self._lock:
            self._size_check_running = False
            self._size_check_cancel = False

    def cancel_size_check(self) -> None:
        with self._lock:
            self._size_check_cancel = True

    def size_check_running(self) -> bool:
        with self._lock:
            return self._size_check_running

    def size_check_cancelled(self) -> bool:
        with self._lock:
            return self._size_check_cancel

    def get_prereq_report(self, mode: MigrationMode) -> Optional[PrerequisiteReport]:
        """Return the last prerequisite report for ``mode``, if any."""
        with self._lock:
            return self._prereq_reports.get(mode)

    def prereq_report_checked_at(self, mode: MigrationMode) -> Optional[datetime]:
        """When the last ``mode`` report was recorded (UTC), or ``None``."""
        with self._lock:
            return self._prereq_report_checked_at.get(mode)

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
            # Preserve the CDC deploy-role ARN across the reset. It is a DEPLOYMENT
            # CONFIG value (env DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN, the CdcDeployRole the
            # TaskRole assumes for CloudFormation/MSK), set ONCE at screen-build time --
            # not per-render user state. __init__() below resets it to None, and the
            # builder does not re-run after Start over, so without this every CDC
            # operation (even the read-only state probe) would fall back to the bare
            # TaskRole -- which lacks cloudformation:DescribeStacks -- and fail with
            # AccessDenied on the deployed stack after any Start over.
            deploy_role_arn = getattr(state, "cdc_deploy_role_arn", None)
            # The optional CMK for the tool-managed source-credentials secret is the
            # SAME kind of value, set on the very next line of the builder
            # (data_migration/__init__.py:375-376) from the same process config -- and it
            # was left out of this preserve list. So a Start over silently dropped it and
            # the next CDC deploy created the secret under the account's default
            # aws/secretsmanager key instead of the operator's CMK, with no UI signal;
            # nobody would notice until they audited the secret's encryption key.
            secret_kms_key_id = getattr(state, "cdc_secret_kms_key_id", None)
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
            # The multi-stack QUEUE must survive too --
            # its own docstring claimed it did, and it did not. The queue's only
            # writer runs in the SAME synchronous Start-over handler as this reset (the
            # dialog calls on_reset_cdc(mode) and then on_reset()), so it was written and
            # wiped in one event, EVERY time: the banner never showed "1 of 3", the
            # advance to the next stack was gated on len(queue) > 1 and so never ran, and
            # a multi-stack teardown announced "MSK / NAT billing has stopped" after only
            # the FIRST stack -- while the rest kept deleting and kept billing. That made
            # teardown_queue_progress / next_unfinished_teardown / advance_cdc_teardown
            # dead code in production, reachable only from tests that seed the queue.
            teardown_queue = list(getattr(state, "cdc_teardown_queue", []) or [])
            # The FINISHED-teardown notice is not carried over: it reports a stack that is
            # already gone, from before this fresh journey, and Start over promises to clear
            # saved session state (live PG E2E, 2026-10-09: a previous journey's "CDC
            # infrastructure deleted (dsql-cdc-stack)" stayed on every screen of the next
            # one). A teardown this Start over submits finishes after the reset and records
            # its own notice then.
            # Builder-time handles (the ledger store and three callables), set once when
            # the screen is built -- the builder does not re-run after Start over.
            handles = {
                name: getattr(state, name, None)
                for name in (
                    "overlay_ledger", "effective_target_ddls", "on_cdc_start",
                    "cutover_restore_in_flight", "validation_state_provider",
                    "source_primary_key_for", "cdc_catalog_baseline",
                )
            }
            state.__init__()  # type: ignore[misc]  # re-run init on the same object
            if bound is not None:
                state.bind_session(bound)
            # Deploy-time config, not user state -- it must outlive Start over.
            state.cdc_deploy_role_arn = deploy_role_arn
            state.cdc_secret_kms_key_id = secret_kms_key_id
            for name, value in handles.items():
                setattr(state, name, value)
            if teardown[0] is not None:
                state.set_cdc_teardown(
                    teardown[0], kind=teardown[1], stack=teardown[2], ctx=teardown[3]
                )
            if teardown_queue:
                state.set_cdc_teardown_queue(teardown_queue)
