# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Step 4 (Validation) screen of the four-step migration workflow.

The Validation screen drives the consistency check the design maps to this step
(design.md "6. Validator"). From the source inventory produced by Step 1
(Evaluation), the configured source/target connections, and the export watermark
persisted by Step 3 (Data Migration), it:

1. runs validation (Requirement 8.2),
2. displays the consistency/validation report -- per-table row-count (and, in
   CHECKSUM mode, checksum) matches and mismatches, the overall verdict, and any
   orphan findings (Requirement 6.4) -- together with the drift report: the data
   is compared as-of the watermark consistency point, and the current source
   GTID is compared to the watermark's GTID to surface changes that occurred on
   the source since the snapshot (Requirement 6.5 / Property 11), and
3. lets the user export (download) the validation report as JSON or text
   (Requirement 8.4).

Because validation can be long-running (per-table counts/checksums against both
engines), the run executes on a background job via
:class:`~dsql_migrator.core.job_manager.JobManager` so the NiceGUI event loop is
never blocked (Requirement 9.3); the screen polls the job with a ``ui.timer`` and
updates the Validation step status in the per-session
:class:`~dsql_migrator.core.models.WorkflowState` (NOT_STARTED -> IN_PROGRESS ->
DONE/FAILED) through the workflow helpers.

Engine wiring. The actual comparison is the implemented Task 9
:class:`~dsql_migrator.core.validator.Validator`. It is reached through a small,
injectable factory seam so the run orchestration and the UI can be unit tested
with a fake validator (no real MySQL / DSQL). The export watermark is read
straight from the Step 3 migration job snapshot, so validation always compares
as-of the exact consistency point the data was exported at (Property 11).

As with the sibling step screens, the run orchestration, report assembly /
serialization, and drift/as-of presentation below are independent of NiceGUI so
they can be unit tested directly; only :func:`build_validation_screen` and its
render helpers touch NiceGUI.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol, Sequence

from dsql_migrator.config import SecretValue, load_config
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.assessment_strategist import AssessmentStrategist
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.core.models import (
    AiAssistConfig,
    DriftReport,
    OrphanFinding,
    SourceConnectionConfig,
    SourceInventory,
    StepStatus,
    TableDef,
    TableSelection,
    TableValidationResult,
    TargetConnectionConfig,
    ValidationMode,
    ValidationReport,
    Watermark,
)
from dsql_migrator.core.table_selection import TableSelectionError, TableSelector
from dsql_migrator.core.validator import ValidationCancelled, Validator
from dsql_migrator.core.validator import export_report as export_validation_report
from dsql_migrator.ui.ai_chat_drawer import build_chat_drawer
from dsql_migrator.ui.connect import make_source_engine_factory
from dsql_migrator.ui.data_migration import DataMigrationStore
from dsql_migrator.ui.design import (
    badge_classes,
    chip_group_quasar_color,
    chip_group_text_class,
    inline_hint,
    render_notice,
    section_header,
)

# Builds the AI strategist for on-demand validation-mismatch guidance (the AI
# chat drawer). Shares the one global AWS profile / credential context; the
# Bedrock client is built lazily, so constructing it performs no network call.
StrategistFactory = Callable[[AiAssistConfig, Optional[str]], AssessmentStrategist]


def _default_strategist_factory(
    config: AiAssistConfig, aws_profile: Optional[str]
) -> AssessmentStrategist:
    """Build the default Bedrock-backed strategist for the validation AI drawer."""
    return AssessmentStrategist(config, aws_profile=aws_profile)
from dsql_migrator.ui.evaluation import EvaluationStore
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    _dev_unlock_steps,
    get_status,
    with_status,
)

# Text shown when a replication coordinate (GTID or binlog file:pos) is absent.
_UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# Validator seam (Protocol keeps run orchestration testable with fakes)
# ---------------------------------------------------------------------------


class _ValidationRunner(Protocol):
    """Minimal contract used by :func:`run_validation` (read-only comparison)."""

    def validate(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode = ...,
        *,
        watermark: Optional[Watermark] = ...,
        check_orphans: bool = ...,
        reconcile: bool = ...,
        should_cancel: Optional[Callable[[], bool]] = ...,
    ) -> ValidationReport: ...


# ---------------------------------------------------------------------------
# Run orchestration (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationInputs:
    """Everything needed to run a validation for one session.

    ``inventory`` is the Step 1 (Evaluation) inventory, so the source schema is
    not re-introspected. ``watermark`` is the Step 3 (Data Migration) export
    watermark: when present, per-table source counts are taken as-of that
    consistency point and drift since the snapshot is reported (Property 11);
    when ``None``, validation runs against the live source and drift is not
    available.
    """

    source_config: SourceConnectionConfig
    source_password: Optional[SecretValue]
    target_config: TargetConnectionConfig
    inventory: SourceInventory
    mode: ValidationMode = ValidationMode.ROW_COUNT
    check_orphans: bool = False
    watermark: Optional[Watermark] = None
    # Full PK-set reconciliation (the pre-cut-over "no mismatched records" check):
    # stream every PK from both sides and report the exact missing/extra rows.
    # Defaults on -- Step 5 exists to verify cut-over readiness (Usability-first:
    # infer the thorough check rather than make the user opt in).
    reconcile: bool = True
    # Fast pre-cut-over sweep: run the expensive checksum/reconciliation only for
    # tables whose row counts differ. On a healthy migration (most tables match)
    # this skips the per-row scans on the tables that need them least. Off by
    # default so the thorough check stays the default; a count-matched table is
    # then reported as verified-by-count (deep checks not run), never a false match.
    deep_only_on_count_mismatch: bool = False
    # Columns EXCLUDED from the migration (per-table column names, e.g. the CDC
    # oversized-LOB exclusion) must be skipped by the checksum: they are not written
    # to the target, so comparing them would always "differ" (a false failure). Maps
    # table name -> set of excluded column names; empty (default) validates every
    # column. run_validation drops these from each table's column list before the
    # checksum builds its per-row concatenation.
    excluded_columns: dict[str, set[str]] = field(default_factory=dict)
    # Rows the migration PERMANENTLY DROPPED, per table (Full Load per-row quarantine).
    # Passed through so a target deficit can be attributed to a known drop instead of
    # leaving the operator to cross-check the error log by hand. Never changes a
    # verdict -- the rows really are missing -- it only explains the shortfall.
    quarantined_by_table: dict[str, int] = field(default_factory=dict)
    # Applied DSQL target types per table ({table: {column: target_type}}) in the
    # converter's postgres vocabulary (parse_target_column_types of the APPLIED DDL).
    # Set from the Schema-Conversion result so the CHECKSUM renders each column by how
    # it was actually STORED -- honoring a target-type remap (e.g. TINYINT(1) kept as
    # smallint). Empty (default) uses the source-derived default mapping. run_validation
    # stamps these onto each column's ``target_type`` before comparing.
    target_types: dict[str, dict[str, str]] = field(default_factory=dict)


# Builds a :class:`_ValidationRunner` bound to the run's inputs.
ValidatorFactory = Callable[[ValidationInputs], _ValidationRunner]


def _default_validator_factory(inputs: ValidationInputs) -> _ValidationRunner:
    """Build the default :class:`Validator` for ``inputs``.

    The source is reached through the read-only-guarded engine factory with the
    in-memory password injected (Property 1 / Property 7); the target uses the
    default IAM-authenticated DSQL connector. The dev-only row-level diff sample
    size is read from config (default 0 == off), so a developer can enable it via
    ``DSQL_MIGRATOR_VALIDATE_ROW_DIFF_SAMPLE_SIZE`` without any code change.
    """
    return Validator(
        source_engine_factory=make_source_engine_factory(inputs.source_password),
        row_diff_sample_size=load_config().validate_row_diff_sample_size,
    )


def _apply_column_exclusions(
    tables: "list[TableDef]", excluded: "dict[str, set[str]]"
) -> "list[TableDef]":
    """Return ``tables`` with each table's migration-excluded columns dropped.

    ``excluded`` maps table name -> excluded column names (the CDC oversized-LOB
    exclusion / ColumnExcludeList). Those columns are not written to the target, so
    the checksum must not compare them. Pure: returns model copies, leaves the input
    untouched, and never drops a primary-key column (PKs are never excludable and
    the checksum needs them to anchor each row).
    """
    if not excluded:
        return tables
    out: list[TableDef] = []
    for table in tables:
        drop = excluded.get(table.name)
        if not drop:
            out.append(table)
            continue
        pk = set(table.primary_key)
        kept = [c for c in table.columns if c.name not in drop or c.name in pk]
        out.append(table.model_copy(update={"columns": kept}) if len(kept) != len(table.columns) else table)
    return out


def _apply_target_types(
    tables: "list[TableDef]", target_types: "dict[str, dict[str, str]]"
) -> "list[TableDef]":
    """Stamp each column's APPLIED DSQL target type onto the TableDef (model copies).

    ``target_types`` maps table -> {column: applied_target_type} (converter postgres
    vocabulary, from parse_target_column_types of the applied DDL). The Validator's
    CHECKSUM render prefers ``column.target_type``, so a Schema-Conversion target-type
    remap (e.g. TINYINT(1) kept as smallint) is compared by how the value was STORED
    instead of the default source-derived mapping -- which would else false-mismatch
    every row. Pure; tables/columns absent from the map are returned unchanged.
    """
    if not target_types:
        return tables
    out: list[TableDef] = []
    for table in tables:
        types = target_types.get(table.name)
        if not types:
            out.append(table)
            continue
        cols = [
            c.model_copy(update={"target_type": types[c.name]}) if c.name in types else c
            for c in table.columns
        ]
        out.append(table.model_copy(update={"columns": cols}))
    return out


def run_validation(
    inputs: ValidationInputs,
    *,
    validator_factory: ValidatorFactory = _default_validator_factory,
    should_cancel: Optional[Callable[[], bool]] = None,
    on_progress: Optional[Callable[[str, int, int], None]] = None,
    max_workers: Optional[int] = None,
    deep_only_on_count_mismatch: bool = False,
) -> ValidationReport:
    """Compare the migrated target against the source and return a report.

    Per-table row counts are compared (and, in CHECKSUM mode, checksums); when a
    watermark is supplied the comparison is as-of the snapshot and drift since
    the snapshot is reported (Requirement 6.5 / Property 11). The validator is
    injectable so this orchestration can be unit tested without a database.
    ``should_cancel`` (polled between tables) lets a caller cooperatively stop a
    long run; the validator raises
    :class:`~dsql_migrator.core.validator.ValidationCancelled` rather than
    returning a partial report. ``on_progress(table, index, total)`` (when given)
    is invoked before each table so a caller can surface live progress.
    ``max_workers`` bounds table-level parallelism; ``None`` (default) reads the
    configured ``validate_max_workers`` so the whole app shares one tuning knob.
    ``deep_only_on_count_mismatch`` runs the expensive checksum/reconciliation only
    for tables whose row counts differ (a fast pre-cut-over sweep).
    """
    workers = max_workers if max_workers is not None else load_config().validate_max_workers
    validator = validator_factory(inputs)
    # Drop migration-excluded columns (e.g. oversized-LOB exclusion) so the checksum
    # never compares a column that was never written to the target -> no false "data
    # differs". Row counts + every other column are still validated.
    tables = _apply_column_exclusions(
        list(inputs.inventory.tables), inputs.excluded_columns
    )
    # Stamp the applied DSQL target types so the CHECKSUM honors a target-type remap
    # (no-op when target_types is empty / mode is ROW_COUNT).
    tables = _apply_target_types(tables, inputs.target_types)
    return validator.validate(
        inputs.source_config,
        inputs.target_config,
        tables,
        inputs.mode,
        watermark=inputs.watermark,
        check_orphans=inputs.check_orphans,
        reconcile=inputs.reconcile,
        should_cancel=should_cancel,
        on_progress=on_progress,
        max_workers=workers,
        deep_only_on_count_mismatch=deep_only_on_count_mismatch,
        quarantined_by_table=inputs.quarantined_by_table,
    )


def resync_identity_sequences(
    target_config: TargetConnectionConfig,
    table_names: "Sequence[str]",
    *,
    aws_profile: Optional[str] = None,
    sync: "Optional[Callable[..., dict]]" = None,
) -> dict[str, int]:
    """Advance target identity sequences past the CURRENT ``MAX(pk)``. Best-effort.

    Run right after a full validation -- the step the operator reaches just before
    cut-over, once CDC has drained and the source is frozen -- to close a gap the
    Full Load's own post-load sync cannot: with ``GENERATED BY DEFAULT AS IDENTITY``,
    Full Load and CDC both insert EXPLICIT ids, and an explicit id does NOT advance
    the identity sequence. Full Load fixes this at load time, but CDC keeps inserting
    afterwards, so by cut-over the sequence again lags the real MAX -- and the
    application's first auto-insert after cut-over would hit a duplicate key. This is
    the worst failure shape (counts/checksums MATCH, Validation passes, it only
    surfaces post-cut-over), so re-running the idempotent ``RESTART WITH max+1`` here,
    where the tool knows the target and the load has settled, closes it.

    Returns ``(advanced, failed)``: ``advanced`` = ``{table: restart_value}`` for the
    identity tables actually advanced (empty when none are identity / all already
    correct); ``failed`` = ``{table: reason}`` for tables whose ``RESTART WITH`` errored
    (empty when none did). Never raises: this is a follow-up to a completed comparison
    and must not turn a good report into an error -- but a FAILED sync must be surfaced
    by the caller (a swallowed one is a silent post-cut-over duplicate-key outage, audit
    finding D2), which the separate ``failed`` map makes possible. ``sync`` is an
    injectable seam (tests pass a fake; production uses the introspector over a DSQL
    connection).
    """
    if not table_names:
        return {}, {}
    try:
        if sync is None:
            from dsql_migrator.core.target_introspector import (
                sync_identity_sequences as sync,
            )
        from dsql_migrator.core.target_connection import DsqlConnector

        def _factory():
            return DsqlConnector(target_config, aws_profile=aws_profile).connect()

        result = sync(list(table_names), connection_factory=_factory) or {}
    except Exception:  # noqa: BLE001 - never fail a completed validation on this
        return {}, {}
    # Split advanced (int) from failed (str); None no-ops (no identity column / empty /
    # unreadable) belong to neither. This is the one place the raw result is classified.
    from dsql_migrator.core.target_introspector import partition_identity_sync

    return partition_identity_sync(result)


def _connection_prerequisite_notices(
    session: object, *, inventory_ready: bool
) -> "list[tuple[str, str, str]]":
    """Return ALL unmet prerequisites to show before validation can run.

    Validation reads BOTH ends live, so it needs the source inventory plus a
    CURRENT, verified connection to the source AND the target -- the same bar the
    runner enforces. This surfaces EVERY unmet prerequisite UPFRONT (each a
    ``(tone, header, body)``) so the user fixes them in one trip to Connect rather
    than discovering target only after fixing source (or hitting a post-click
    error). The target is treated symmetrically with the source. Inventory-missing
    is returned alone (the source schema is unknown until Step 1 runs). Returns an
    empty list when everything is ready. Pure (no NiceGUI), so it is unit-testable.
    """
    if not inventory_ready:
        return [
            (
                "warning",
                "Run Step 1 first",
                "No source inventory yet. Run Step 1 (Evaluation) to introspect "
                "the source schema.",
            )
        ]
    notices: list[tuple[str, str, str]] = []
    has_source = bool(getattr(session, "has_source", lambda: False)())
    source_verified = bool(getattr(session, "source_verified", False))
    if not has_source or not source_verified:
        notices.append(
            (
                "warning",
                "Source connection needed",
                "Validation reads the source live, so it needs a verified source "
                "connection. "
                + (
                    "Set up the source in the Connect section above, then test it."
                    if not has_source
                    else "Test the source connection on the Connect screen to "
                    "(re)connect, then run validation."
                ),
            )
        )
    has_target = bool(getattr(session, "has_target", lambda: False)())
    target_verified = bool(getattr(session, "target_verified", False))
    if not has_target or not target_verified:
        notices.append(
            (
                "warning",
                "Target connection needed",
                "Validation reads the target (Aurora DSQL) live and its access "
                "token is short-lived, so it needs a verified target connection. "
                + (
                    "Set up the target in the Connect section above, then test it."
                    if not has_target
                    else "Test the target connection on the Connect screen to "
                    "(re)connect, then run validation."
                ),
            )
        )
    return notices


def job_status_to_step_status(job_status: str) -> Optional[StepStatus]:
    """Map a :class:`JobManager` job status to the Validation step status.

    Returns ``DONE``/``FAILED`` for those terminal states, ``NOT_STARTED`` for
    ``CANCELLED`` (a cancelled run produced no report, so the step reads as "not
    run" and can be re-run — it is not a failure), and ``None`` while the job is
    still ``PENDING``/``RUNNING`` (the step stays ``IN_PROGRESS``).
    """
    if job_status == "DONE":
        return StepStatus.DONE
    if job_status == "FAILED":
        return StepStatus.FAILED
    if job_status == "CANCELLED":
        return StepStatus.NOT_STARTED
    return None


# ---------------------------------------------------------------------------
# Report summary + drift presentation (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


def cdc_active_connector_names(migration_state: object) -> tuple[str, ...]:
    """Return the CDC connectors believed to be RUNNING right now (best-effort).

    Used by the Validation screen to warn that a non-zero source/target count
    difference may just be CDC lag (the target is a moving target while the stream
    flows), not data loss -- so a confirmatory cut-over check should run with the
    source quiesced. Reads ``cdc_connector_running_names`` last recorded by the
    Data Migration screen's read-only poll; it can be stale on this screen (we do
    not poll MSK here), which is acceptable for an advisory banner. Pure: no AWS,
    no NiceGUI.
    """
    names = getattr(migration_state, "cdc_connector_running_names", None) or []
    return tuple(n for n in names if n)


@dataclass(frozen=True)
class ValidationSummary:
    """A snapshot summary of a validation report for display (Requirement 6.4).

    Beyond the per-table match counts, this carries the three pre-cut-over
    readiness checks the Validation report is built around: the data is identical
    (counts/checksum), there are no mismatched records (full PK reconciliation),
    and no table errored. ``ready_for_cutover`` is ``True`` only when all three
    pass, so a reviewer gets one clear go/no-go verdict.
    """

    total_tables: int
    matched_tables: int
    mismatched_tables: int
    orphan_count: int
    is_match: bool
    mode: str
    as_of: str
    # Pre-cut-over readiness checks.
    reconcile_performed: bool
    reconciled_tables: int
    inconsistent_tables: int
    missing_on_target: int
    extra_on_target: int
    errored_tables: int
    ready_for_cutover: bool
    # Names of the tables that did NOT pass (mismatch or error), in report order,
    # so the UI can list/jump to exactly the tables needing attention.
    failed_tables: tuple[str, ...] = ()
    # Tables whose ENTIRE shortfall is rows the migration is known to have dropped
    # (quarantined), i.e. deficit == rows_quarantined exactly. Already-explained
    # findings, not new data loss -- so the readiness panel can say so instead of
    # reporting them as unexplained mismatches beside a per-table blurb that says the
    # opposite. A table with any unaccounted-for shortfall is deliberately NOT here.
    quarantine_explained_tables: tuple[str, ...] = ()
    # Rows dropped across those tables (the size of the explained shortfall).
    quarantine_explained_rows: int = 0
    # Per-table columns the CHECKSUM could NOT value-compare (FLOAT/DOUBLE/JSON -- no
    # byte-identical cross-engine form). Surfaced so a "Data identical" verdict is read
    # as "every column EXCEPT these", not "every column verified" (a non-key value diff
    # confined to such a column is undetected by any mode). Empty outside CHECKSUM mode.
    checksum_excluded_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def unexplained_mismatched_tables(self) -> int:
        """Mismatched tables whose shortfall is NOT fully explained by dropped rows.

        This is the number a reviewer must actually investigate. ``mismatched_tables``
        counts every non-matching table, so on a run whose only finding is a known
        quarantine it reported work that does not exist.
        """
        return max(0, self.mismatched_tables - len(self.quarantine_explained_tables))


def cutover_release_state(
    summary: "Optional[ValidationSummary]", *, gap_accepted: bool = False
) -> str:
    """Whether the cut-over runbook is released, and if not, why. Pure.

    Returns one of:

    * ``"clean"``     -- validation matched outright; nothing to acknowledge.
    * ``"accepted"``  -- the only remaining differences are rows the migration already
      reported dropping, AND the operator has explicitly accepted that gap.
    * ``"acceptable"`` -- same difference profile, but not yet accepted: offer the
      acknowledgement rather than a dead end.
    * ``"blocked"``   -- something is unexplained (or there is no result yet), so
      cut-over must stay shut.

    Why ``"acceptable"`` has to exist: the gate previously released on ``is_match``
    alone, while its own copy promised cut-over was possible when "every difference is
    explained". No such path existed, so a run whose sole finding was a permanently
    quarantined row could never reach cut-over -- and reloading cannot fix it, because
    the value is one DSQL is unable to store. That made the step unreachable by design
    rather than by any operator decision.

    The acknowledgement is still required (never auto-released): rows really are absent
    from the target, and that is a call the operator owns. ``gap_accepted`` is ignored
    unless the difference profile genuinely qualifies, so an old acceptance cannot leak
    onto a later run that has a real mismatch.
    """
    if summary is None:
        return "blocked"
    if summary.ready_for_cutover:
        return "clean"
    # Nothing acknowledged-able: there must be a KNOWN gap to sign off on. Strictly this
    # is implied by the unexplained/errored test below (with no explained tables, every
    # mismatch is unexplained), but it is kept as the explicit statement of the
    # precondition -- "acceptable" is only ever about rows the migration already reported
    # dropping, never about a difference nobody has accounted for.
    if not summary.quarantine_explained_tables:
        return "blocked"
    if summary.unexplained_mismatched_tables or summary.errored_tables:
        return "blocked"
    return "accepted" if gap_accepted else "acceptable"


def summarize_validation(report: ValidationReport) -> ValidationSummary:
    """Summarize ``report`` into match counts and the three readiness checks."""
    matched = sum(1 for item in report.items if item.matched)
    as_of = humanize_as_of(report.snapshot_timestamp)
    reconciled = [item for item in report.items if item.reconcile is not None]
    inconsistent = [
        item for item in reconciled if not item.reconcile.consistent  # type: ignore[union-attr]
    ]
    errored = [item for item in report.items if item.error is not None]
    missing = sum(item.reconcile.missing_on_target for item in reconciled)  # type: ignore[union-attr]
    extra = sum(item.reconcile.extra_on_target for item in reconciled)  # type: ignore[union-attr]
    # Tables whose whole shortfall is rows the migration already reported as dropped.
    # ``deficit_explained_by_quarantine`` requires an EXACT match (deficit ==
    # rows_quarantined), so a table 4 rows short having dropped 1 stays unexplained --
    # that strictness is what keeps a real loss from hiding behind a known one.
    quarantine_explained = tuple(
        item.table for item in report.items if item.deficit_explained_by_quarantine
    )
    quarantine_rows = sum(
        item.rows_quarantined
        for item in report.items
        if item.deficit_explained_by_quarantine
    )
    return ValidationSummary(
        total_tables=len(report.items),
        matched_tables=matched,
        mismatched_tables=len(report.items) - matched,
        orphan_count=len(report.orphan_findings),
        is_match=report.is_match,
        mode=report.mode.value,
        as_of=as_of,
        reconcile_performed=bool(reconciled),
        reconciled_tables=len(reconciled),
        inconsistent_tables=len(inconsistent),
        missing_on_target=missing,
        extra_on_target=extra,
        errored_tables=len(errored),
        # Cut-over readiness == the overall sound verdict (which already folds in
        # reconciliation + per-table errors via ``matched``); named explicitly so
        # the report can phrase it as a go/no-go for cut-over.
        ready_for_cutover=report.is_match,
        failed_tables=failed_table_names(report),
        quarantine_explained_tables=quarantine_explained,
        quarantine_explained_rows=quarantine_rows,
        checksum_excluded_columns={
            item.table: tuple(item.checksum_excluded_columns)
            for item in report.items
            if item.checksum_excluded_columns
        },
    )


# Sentinel string shown for the as-of when validation ran against the live source.
_LIVE_SOURCE_AS_OF = "live source (no watermark)"


def humanize_as_of(snapshot_timestamp: object) -> str:
    """Format the as-of consistency point for display (human-readable UTC).

    Renders a watermark timestamp as e.g. ``"2026-01-02 03:04 UTC"`` (minute
    precision is enough for a consistency point) instead of a raw ISO-8601 string
    with microseconds/offset. Returns the live-source sentinel when there is no
    watermark. Accepts any object exposing ``strftime`` (a ``datetime``); anything
    else degrades to ``str`` so the UI never breaks on an unexpected value.
    """
    if snapshot_timestamp is None:
        return _LIVE_SOURCE_AS_OF
    strftime = getattr(snapshot_timestamp, "strftime", None)
    if callable(strftime):
        return strftime("%Y-%m-%d %H:%M UTC")
    return str(snapshot_timestamp)


def failed_table_names(report: ValidationReport) -> tuple[str, ...]:
    """Return the names of tables that did not pass, in report order.

    A table "failed" when it errored or did not match (count/checksum/record
    divergence). Used to list and jump to exactly the tables needing attention.
    """
    return tuple(
        item.table
        for item in report.items
        if item.error is not None or not item.matched
    )


def reconcile_skipped_tables(report: ValidationReport) -> tuple[str, ...]:
    """Return tables that were NOT record-reconciled while reconciliation ran.

    When the reconciliation pass ran, a table with no :class:`ReconcileResult`
    (and no error) was skipped because its primary key is composite/non-integer,
    so it was compared by count/checksum only. Fast-sweep "verified by count"
    tables are EXCLUDED here (they have their own honest label via
    :func:`count_verified_tables`) so the composite-PK footnote stays accurate.
    Empty when reconciliation was off (then no per-table reconcile is expected) --
    the UI explains the global off state separately.
    """
    any_reconciled = any(item.reconcile is not None for item in report.items)
    if not any_reconciled:
        return ()
    return tuple(
        item.table
        for item in report.items
        if item.reconcile is None
        and item.error is None
        and not item.deep_checks_skipped
    )


def _split_schema(table_name: str) -> tuple[str, str]:
    """Split a ``schema.table`` name into ``(schema, table)``.

    Falls back to a ``"(default)"`` schema when the name is not dotted, so a chip
    always has a stable group key. Only the FIRST dot splits, so a dotted table
    name keeps its remainder intact.
    """
    schema, separator, obj = table_name.partition(".")
    if separator and schema and obj:
        return schema, obj
    return "(default)", table_name


def group_objects_by_schema(
    table_names: "Sequence[str]",
) -> "list[tuple[str, list[str]]]":
    """Group fully-qualified table names by schema for the object picker.

    Returns ``[(schema, [full_name, ...]), ...]`` sorted by schema then name, so
    the validation object picker can render schema-colored, clickable chips in a
    stable order. Pure (no NiceGUI), so the grouping is unit-testable.
    """
    buckets: dict[str, list[str]] = {}
    for name in table_names:
        schema, _obj = _split_schema(name)
        buckets.setdefault(schema, []).append(name)
    return [(schema, sorted(buckets[schema])) for schema in sorted(buckets)]


def count_verified_tables(report: ValidationReport) -> tuple[str, ...]:
    """Return tables the fast sweep verified by ROW COUNT only (deep checks skipped).

    These had equal counts, so the fast sweep (deep-check-only-on-count-mismatch)
    skipped their checksum/reconciliation. Surfaced so the UI can state honestly
    that they were verified by count -- a count match, not a proven row-identical
    match -- distinct from a composite-PK reconcile skip.
    """
    return tuple(
        item.table
        for item in report.items
        if item.deep_checks_skipped and item.error is None
    )


# ---------------------------------------------------------------------------
# Re-checking individual tables (merge a fresh comparison into a prior report)
# ---------------------------------------------------------------------------


def merge_revalidated(
    report: ValidationReport,
    new_items: "Sequence[TableValidationResult]",
    *,
    orphan_findings: "Optional[Sequence[OrphanFinding]]" = None,
) -> ValidationReport:
    """Return ``report`` with the re-checked tables' results replaced.

    Backs the per-table "Re-check" action: after a mismatch is investigated and
    fixed, the user re-validates JUST that table instead of re-running the whole
    (potentially hour-long) comparison, and the WHOLE report -- every other
    table's verdict and the overall cut-over go/no-go -- is kept.

    The merge is deliberately narrow and sound:

    - Item ORDER is preserved (the report must read deterministically), so a
      replaced item stays in its original row; nothing is appended or reordered.
    - ``is_match`` is RECOMPUTED by :meth:`ValidationReport.build`, never carried
      over, so the verdict always reflects the current item set (Property 9). If
      the last failing table now passes, the verdict flips to a clean match on its
      own; if a previously-passing table now fails, it flips the other way.
    - A ``new_items`` entry naming a table NOT in ``report`` is IGNORED rather
      than appended: it can only come from a stale scope (e.g. the migration
      selection changed under a queued re-check), and silently widening the
      report's scope would change what "all tables match" means.
    - ``mode``, ``orphan_check_performed`` and ``snapshot_timestamp`` are carried
      over unchanged -- they describe the RUN, and a re-check is required to use
      the same options, so they still hold.
    - ``drift`` is carried over as-is. It is a whole-source signal that a
      single-table re-check does not re-measure, so overwriting it with a fresh
      reading would silently re-date the report's drift verdict.
    - ``orphan_findings`` (when given) REPLACE the findings for the re-checked
      tables only: findings for those tables are dropped and the new ones added,
      so a table whose orphans were fixed correctly loses its finding, while every
      other table's findings survive. Passing ``None`` leaves the findings alone
      (the re-check did not re-run the orphan check).

    Pure: builds a new report and leaves ``report`` untouched, so it is unit
    testable without NiceGUI or a database.
    """
    replacements = {item.table: item for item in new_items}
    known = {item.table for item in report.items}
    # Only tables actually present in the report can be replaced (see docstring).
    rechecked = {name for name in replacements if name in known}
    merged_items = [
        replacements[item.table] if item.table in rechecked else item
        for item in report.items
    ]

    findings = list(report.orphan_findings)
    if orphan_findings is not None:
        # Drop the re-checked tables' old findings, then add the fresh ones (a
        # table whose orphans are gone correctly ends up with no finding).
        findings = [f for f in findings if f.table not in rechecked]
        findings.extend(f for f in orphan_findings if f.table in rechecked)

    return ValidationReport.build(
        mode=report.mode,
        items=merged_items,
        orphan_findings=findings,
        orphan_check_performed=report.orphan_check_performed,
        drift=report.drift,
        snapshot_timestamp=report.snapshot_timestamp,
    )


@dataclass(frozen=True)
class RunOptions:
    """The comparison options a validation run used (mode + which checks ran)."""

    mode: ValidationMode
    reconcile: bool
    check_orphans: bool


def report_run_options(report: ValidationReport) -> RunOptions:
    """Recover the options a report was produced with, FROM the report itself.

    A re-check must compare the table the same way the original run did, or the
    merged report's derived summaries stop being truthful (e.g. splicing a
    reconciled item into a report where reconciliation never ran would make
    :func:`reconcile_skipped_tables` mislabel every OTHER table as composite-PK).

    The options are derived rather than remembered on purpose:

    - the live option toggles cannot be used -- the user may have changed them on
      screen after the run, and they describe the NEXT run, not this report;
    - a restored report (re-hydrated from a session snapshot) has no remembered
      options at all, yet must still be re-checkable.

    ``reconcile`` is taken as "at least one item carries a reconciliation result",
    which is exactly the ``reconcile_performed`` claim :func:`summarize_validation`
    already makes about the report -- so a re-check reproduces the checks the UI
    says the report contains, keeping the merged report self-consistent.
    """
    return RunOptions(
        mode=report.mode,
        reconcile=any(item.reconcile is not None for item in report.items),
        check_orphans=report.orphan_check_performed,
    )


def deep_recheck_adds_checks(report: ValidationReport) -> bool:
    """Whether re-checking a count-only table would actually run MORE checks.

    Gates the "verified by row count only" (fast sweep) re-check action. Those
    tables skipped their deep checks because their counts agreed, so re-checking
    them is only worth offering when a deep check EXISTS to run: a checksum (this
    report is in CHECKSUM mode) or a record reconciliation (this report shows
    reconciliation ran). In a ROW_COUNT-mode report with no reconciliation there is
    nothing deeper to do -- the re-check would repeat the identical count
    comparison -- so the action is withheld rather than offered as a no-op.
    """
    options = report_run_options(report)
    return options.mode is ValidationMode.CHECKSUM or options.reconcile


# ---------------------------------------------------------------------------
# Validation scope: WHAT is being validated (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedScope:
    """The concrete set of tables a validation run will compare.

    ``tables`` is the resolved list (inventory order). ``is_subset`` is ``True``
    when the Data Migration table selection narrowed the inventory (a partial
    migration), so the UI can say "selected N of M" rather than implying the whole
    schema is validated. ``total_in_inventory`` is the full inventory size for
    that "N of M" phrasing.
    """

    tables: tuple[TableDef, ...]
    total_in_inventory: int
    is_subset: bool


def resolve_validation_tables(
    inventory: SourceInventory, selection: TableSelection
) -> ResolvedScope:
    """Resolve which tables to validate from the inventory + migration selection.

    Validation must cover exactly what was migrated: the Data Migration step lets
    the user pick a subset of tables (an empty selection means "all"), and the
    export watermark is captured for that subset. Validating the whole inventory
    would flag every un-migrated table as "missing on target", so this narrows the
    inventory to the migration selection using the SAME
    :class:`~dsql_migrator.core.table_selection.TableSelector` semantics the
    migration used (empty => all; inventory order preserved). A selection that
    references names no longer in the inventory degrades to "all" rather than
    raising, so a stale selection never blocks validation.
    """
    selector = TableSelector()
    try:
        resolved = selector.resolve(inventory, selection)
    except TableSelectionError:
        resolved = list(inventory.tables)
    total = len(inventory.tables)
    is_subset = len(resolved) < total
    return ResolvedScope(
        tables=tuple(resolved),
        total_in_inventory=total,
        is_subset=is_subset,
    )


def apply_table_filter(
    scope_tables: "tuple[TableDef, ...] | list[TableDef]",
    table_filter: "set[str] | frozenset[str]",
) -> tuple[TableDef, ...]:
    """Narrow the migration-scope tables to a user-chosen object filter.

    The Validation step lets the user further filter the migration scope down to
    specific objects. ``table_filter`` is a set of table names to keep; an empty
    filter means "all in-scope tables" (the inferred default, so the common case
    needs no clicks). Names in the filter that are not in ``scope_tables`` are
    ignored (a stale filter never drops the scope), and the original scope order
    is preserved. When the filter would select nothing (only unknown names), it
    degrades to the full scope rather than an empty run.
    """
    if not table_filter:
        return tuple(scope_tables)
    kept = tuple(t for t in scope_tables if t.name in table_filter)
    return kept or tuple(scope_tables)


def included_from_exclusions(
    scope_table_names: "Sequence[str]", excluded: "set[str] | frozenset[str]"
) -> set[str]:
    """Convert the picker's EXCLUSION set into an include-set over the scope.

    The object picker stores which tables are turned OFF (``excluded``); the run
    path and scope summary still speak the include-set language of
    :func:`apply_table_filter` / :func:`build_validation_scope` (a set of names to
    KEEP, empty == all). This maps one to the other: keep every in-scope name not
    excluded. When nothing is excluded it returns an EMPTY set (the "all" sentinel,
    so no false "filtered" badge); when everything would be excluded it also falls
    back to empty (all), since validating nothing is never the intent. Pure.
    """
    names = [n for n in scope_table_names]
    kept = {n for n in names if n not in excluded}
    if not excluded or not kept or kept == set(names):
        return set()  # nothing excluded (or all/none) -> the "validate all" sentinel
    return kept


@dataclass(frozen=True)
class ValidationScope:
    """Human-facing description of WHAT a validation run covers (for the UI).

    Surfaces the run's identity before/independently of results: the source and
    target endpoints, the table scope (count + whether it is a migration-selected
    subset + a short name sample), and the as-of consistency point. Migration type
    is deliberately NOT here: validation is a pure source-vs-target comparison that
    behaves identically however the rows arrived (Full Load vs CDC), and a session
    only records the LAST-chosen type, so showing it here (e.g. "CDC only" after a
    Full Load → CDC run) was both irrelevant and misleading. All fields are display
    strings/derived counts so the render helper stays trivial.
    """

    source_label: str
    source_detail: str
    target_label: str
    target_detail: str
    table_count: int
    total_in_inventory: int
    is_subset: bool
    table_sample: tuple[str, ...]
    sample_overflow: int
    as_of: str
    # Object filter (a subset of the migration scope chosen on this screen).
    is_filtered: bool = False
    scope_count: int = 0  # tables in the migration scope (before the filter)


# How many table names to show inline as chips before collapsing to "+N more".
_SCOPE_SAMPLE_LIMIT = 8


def build_validation_scope(
    *,
    source_config: Optional[SourceConnectionConfig],
    target_config: Optional[TargetConnectionConfig],
    target_cluster_name: Optional[str],
    scope: ResolvedScope,
    watermark: Optional[Watermark],
    table_filter: "set[str] | frozenset[str] | None" = None,
) -> ValidationScope:
    """Build the :class:`ValidationScope` shown in the "Validating" context card.

    Labels are derived from the (non-secret) connection configs and the resolved
    scope; the as-of point is the watermark snapshot time (humanized) or the
    live-source sentinel. ``table_filter`` (when non-empty) narrows the migration
    scope to the user-chosen objects; the card then reports the filtered count and
    sample. Pure/derivation-only so it is unit-testable without NiceGUI or a
    database.
    """
    scope_count = len(scope.tables)
    effective = apply_table_filter(scope.tables, table_filter or set())
    is_filtered = bool(table_filter) and len(effective) < scope_count

    names = [table.name for table in effective]
    sample = tuple(names[:_SCOPE_SAMPLE_LIMIT])
    overflow = max(0, len(names) - len(sample))

    if source_config is not None:
        database = source_config.database or "—"
        source_label = f"Source · {database}"
        source_detail = source_config.host
    else:
        source_label = "Source MySQL"
        source_detail = "not connected"

    if target_config is not None:
        cluster = target_cluster_name or _dsql_cluster_id(target_config.cluster_endpoint)
        target_label = f"Target · {cluster}"
        target_detail = f"Aurora DSQL · {target_config.region}"
    else:
        target_label = "Target Aurora DSQL"
        target_detail = "not connected"

    return ValidationScope(
        source_label=source_label,
        source_detail=source_detail,
        target_label=target_label,
        target_detail=target_detail,
        table_count=len(names),
        total_in_inventory=scope.total_in_inventory,
        is_subset=scope.is_subset,
        table_sample=sample,
        sample_overflow=overflow,
        as_of=humanize_as_of(watermark.snapshot_timestamp if watermark else None),
        is_filtered=is_filtered,
        scope_count=scope_count,
    )


def _dsql_cluster_id(endpoint: str) -> str:
    """Derive a short DSQL cluster id from its endpoint (label before ``.dsql.``)."""
    if not endpoint:
        return "Aurora DSQL"
    head, marker, _rest = endpoint.partition(".dsql.")
    return head if marker else endpoint.split(".", 1)[0]


def _cdc_in_use(session: object) -> bool:
    """Return whether the chosen migration path includes streaming CDC.

    Branches the cut-over runbook (:func:`_render_cutover_section`): a CDC path
    needs a "let it drain to zero lag" step before the final check, whereas a
    Full-Load-only path just needs a source write-freeze. Degrades to ``False``
    (the simpler Full-Load-only runbook) if the type cannot be resolved, so the
    guidance is never wrong about a drain the user doesn't have.
    """
    try:
        from dsql_migrator.ui.data_migration import MigrationType

        return getattr(session, "migration_type", None) in (
            MigrationType.CDC_ONLY,
            MigrationType.FULL_LOAD_AND_CDC,
        )
    except Exception:  # noqa: BLE001 - decorative; never break the page
        return False


@dataclass(frozen=True)
class DriftDisplay:
    """The source-changes-since-snapshot report formatted for display (Req 6.5).

    ``available`` is ``False`` when validation ran without a watermark (drift is
    undefined). ``determinable`` is ``False`` when a watermark exists but NEITHER
    coordinate could be compared; the optional coordinate fields degrade to
    ``"unavailable"`` so the UI always renders a valid panel.

    ``basis`` records WHICH coordinate answered the question -- ``"gtid"``,
    ``"binlog"``, or ``""`` when undeterminable -- so the panel can name its evidence
    instead of always speaking of GTIDs. That matters because the primary supported
    source (RDS MySQL 8.0) cannot enable GTID, making ``"binlog"`` the normal basis.
    """

    available: bool
    determinable: bool
    drifted: bool
    watermark_gtid: str
    current_gtid: str
    detail: str
    summary: str
    basis: str = ""
    watermark_binlog: str = _UNAVAILABLE
    current_binlog: str = _UNAVAILABLE


def format_drift(report: ValidationReport) -> DriftDisplay:
    """Format ``report``'s source-change section for display (Requirement 6.5).

    Surfaces whether the source changed after the snapshot (Property 11), judged by
    GTID when both sides have one and otherwise by binlog ``file:position``. When no
    watermark was used, or neither coordinate is comparable, the panel says so rather
    than implying a clean or dirty result.
    """
    drift: Optional[DriftReport] = report.drift
    if drift is None:
        return DriftDisplay(
            available=False,
            determinable=False,
            drifted=False,
            watermark_gtid=_UNAVAILABLE,
            current_gtid=_UNAVAILABLE,
            detail=(
                "Validation ran without an export watermark, so changes since a "
                "snapshot cannot be reported."
            ),
            summary="Source changes since snapshot: not available (no watermark).",
            basis="",
        )

    basis = getattr(drift, "basis", "") or ""
    # Determinability follows the BASIS the core actually used, so a GTID-less source
    # that was compared by binlog position is (correctly) determinable. Older reports
    # predate `basis`; fall back to the GTID pair so they still render as before.
    determinable = bool(basis) if basis else (
        drift.watermark_gtid is not None and drift.current_gtid is not None
    )
    if not determinable:
        summary = (
            "Source changes since the snapshot could not be determined "
            "(no GTID or binlog position to compare)."
        )
    elif drift.drifted:
        evidence = "binlog position moved" if basis == "binlog" else "GTID changed"
        summary = f"Source has changed since the snapshot ({evidence})."
    else:
        summary = "No source changes since the snapshot."

    return DriftDisplay(
        available=True,
        determinable=determinable,
        drifted=drift.drifted,
        watermark_gtid=drift.watermark_gtid or _UNAVAILABLE,
        current_gtid=drift.current_gtid or _UNAVAILABLE,
        detail=drift.detail,
        summary=summary,
        basis=basis,
        watermark_binlog=getattr(drift, "watermark_binlog", None) or _UNAVAILABLE,
        current_binlog=getattr(drift, "current_binlog", None) or _UNAVAILABLE,
    )


# ---------------------------------------------------------------------------
# Report export serialization (NiceGUI-agnostic) -- Requirement 8.4
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReportDownload:
    """A serialized report ready to be downloaded: name, content, media type."""

    filename: str
    content: str
    media_type: str


_MEDIA_TYPES: dict[str, str] = {"json": "application/json", "text": "text/plain"}
_EXTENSIONS: dict[str, str] = {"json": "json", "text": "txt"}


def _download_parts(stem: str, fmt: str) -> tuple[str, str]:
    """Return the ``(filename, media_type)`` for ``stem`` in ``fmt``."""
    normalized = fmt.lower()
    if normalized not in _MEDIA_TYPES:
        raise ValueError(f"unsupported report format: {fmt!r} (use 'json' or 'text')")
    return f"{stem}.{_EXTENSIONS[normalized]}", _MEDIA_TYPES[normalized]


def validation_download(report: ValidationReport, fmt: str = "json") -> ReportDownload:
    """Serialize the validation report (incl. drift) for download (Req 8.4)."""
    content = export_validation_report(report, fmt)
    filename, media_type = _download_parts("validation_report", fmt)
    return ReportDownload(filename=filename, content=content, media_type=media_type)


# ---------------------------------------------------------------------------
# Per-session validation state
# ---------------------------------------------------------------------------


class ValidationState:
    """Per-session validation options/outputs and the running job id.

    ``mode``/``check_orphans`` are read/written only on the UI thread, while
    ``result``/``error`` are produced by a background worker and read by the UI
    poller, so those two are guarded by a lock to make the cross-thread handoff
    safe (mirroring the sibling step screens).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.mode: ValidationMode = ValidationMode.ROW_COUNT
        self.check_orphans: bool = False
        # Full PK-set reconciliation (the "no mismatched records" check) is on by
        # default for the pre-cut-over report; the user can switch it off for a
        # quick count-only pass.
        self.reconcile: bool = True
        # Fast sweep: run the expensive checksum/reconciliation only for tables
        # whose row counts differ. Off by default (thorough check is the default).
        self.deep_only_on_count_mismatch: bool = False
        # Optional object EXCLUSIONS: the migration-scope table names the user has
        # turned OFF in the object picker. Empty == validate every in-scope table
        # (the inferred default, no clicks needed). Stored as exclusions (not an
        # include-set) so the picker shows every object ON by default and a click
        # toggles it off/on -- the on-screen state always matches what will be
        # validated. Read/written only on the UI thread.
        self.table_exclude: set[str] = set()
        self.job_id: Optional[str] = None
        # Set when the user requested a cancel of the current/last run, so the UI
        # can show a "cancelled" notice once the worker stops. Cleared on a new run.
        self.cancel_requested: bool = False
        # Whether the operator has explicitly accepted a validation whose ONLY
        # remaining differences are rows the migration already reported dropping
        # (quarantined), so the cut-over runbook is released. Mirrors Full Load's
        # ``accept_quarantined_rows``: the gap is real and permanent, so the tool must
        # not wave it through on its own -- but nor can it be the reason cut-over is
        # unreachable forever, because reloading cannot fix a value DSQL is unable to
        # store. Only ever settable when every difference is accounted for (see
        # ``cutover_release_state``). Set on the UI thread.
        self.accept_explained_gap: bool = False
        self._result: Optional[ValidationReport] = None
        self._error: Optional[str] = None
        # Per-table RE-CHECK track (the same single job slot as a full run --
        # ``job_id`` -- so there is never more than one comparison in flight).
        # Non-empty while a re-check of exactly these tables is running/just ran;
        # it is what tells the worker to MERGE its result into the existing report
        # instead of replacing it. Written on the UI thread when a re-check starts
        # and read by the UI poller + the worker, so it is lock-guarded.
        self._recheck_tables: tuple[str, ...] = ()
        # Tables whose result in the CURRENT report came from a re-check, and when
        # the last re-check finished -- so the report can say honestly which rows
        # are newer than the rest of the run. Cleared whenever a full run replaces
        # the report (then every row is from the same run again).
        self._rechecked_tables: tuple[str, ...] = ()
        self._rechecked_at: Optional[datetime] = None
        # A re-check's own failure message, kept apart from ``_error`` (a full
        # run's failure) so a failed re-check never reads as "validation failed"
        # over a perfectly good report.
        self._recheck_error: Optional[str] = None
        # Live per-table progress for the running job, written by the background
        # worker (validator on_progress) and read by the UI poller -- guarded by
        # the same lock for the cross-thread handoff. ``(table, index, total)`` or
        # ``None`` before the first table / after the run finishes.
        self._progress: Optional[tuple[str, int, int]] = None
        # Wall-clock timing for the current/last run (monotonic clock, immune to
        # system clock changes): ``_run_started`` is stamped when the run starts and
        # ``_run_elapsed`` is the total seconds once it finishes, so the result can
        # report "Completed in Xm Ys". ``None`` until a run has started/finished.
        self._run_started: Optional[float] = None
        self._run_elapsed: Optional[float] = None
        # Wall-clock time the last run FINISHED (UTC), for the "Completed at"/
        # restored-as-of note and persistence. Distinct from the monotonic timing
        # above (which can't be turned into a calendar time). None until finished.
        self._completed_at: Optional[datetime] = None
        # True only when the current result was re-hydrated from a saved snapshot
        # (so the UI can show a "restored as-of <time>" note); a fresh run clears it.
        self._restored: bool = False
        # Outcome of the identity-sequence re-sync run alongside the last full
        # validation: ``{table: RESTART WITH value}`` for the identity tables whose
        # sequence was advanced past the current MAX(pk). WHY here: Full Load's own
        # sync runs at load time, but CDC keeps inserting explicit ids afterwards
        # (which do NOT advance a GENERATED BY DEFAULT sequence), so by cut-over the
        # sequence lags the real MAX and the app's first insert would collide. Re-running
        # it on validation -- the step users hit right before cut-over, after CDC has
        # drained -- closes that gap. Empty dict == ran, nothing to advance; None ==
        # not run (e.g. count-only path or a restored report). Written by the worker,
        # read by the UI poller, so lock-guarded.
        self._identity_sync: Optional[dict[str, int]] = None
        # Result of the operator-triggered "Sync identity sequences" action in the
        # cut-over runbook (an explicit button, NOT a render side-effect). ``None`` until
        # the operator clicks it; then a dict of {table: RESTART WITH value} (empty ==
        # ran, nothing to advance). Written by the button's background job, read by the
        # cut-over render to show the outcome. Lock-guarded for the cross-thread handoff;
        # cleared by a fresh validation run (a new verdict is a new pre-cut-over state).
        self._cutover_identity_sync: Optional[dict[str, int]] = None
        # Tables whose RESTART WITH FAILED on the last cut-over sync (table -> reason).
        # Distinct from the advanced dict so a failed ALTER is surfaced as an error
        # instead of being painted as "nothing to advance" (audit finding D2). None
        # until the button runs; then a dict (empty == every table advanced/no-op'd).
        self._cutover_identity_sync_failed: Optional[dict[str, str]] = None

    def set_cutover_identity_sync(
        self,
        advanced: "Optional[dict[str, int]]",
        failed: "Optional[dict[str, str]]" = None,
    ) -> None:
        """Record the cut-over identity-sync button's outcome (advanced + any failures)."""
        with self._lock:
            self._cutover_identity_sync = advanced
            self._cutover_identity_sync_failed = failed

    @property
    def cutover_identity_sync(self) -> "Optional[dict[str, int]]":
        """The cut-over identity-sync button's last outcome, or ``None`` if not yet run."""
        with self._lock:
            return (
                dict(self._cutover_identity_sync)
                if self._cutover_identity_sync is not None
                else None
            )

    @property
    def cutover_identity_sync_failed(self) -> "dict[str, str]":
        """Tables whose RESTART WITH failed on the last cut-over sync (empty if none)."""
        with self._lock:
            return dict(self._cutover_identity_sync_failed or {})

    def mark_run_started(self) -> None:
        """Stamp the start of a run (clears any prior elapsed time)."""
        import time

        with self._lock:
            self._run_started = time.monotonic()
            self._run_elapsed = None

    def mark_run_finished(self) -> None:
        """Record the elapsed time + finish wall-clock time for the just-finished run."""
        import time

        with self._lock:
            if self._run_started is not None:
                self._run_elapsed = max(0.0, time.monotonic() - self._run_started)
            self._run_started = None
            self._completed_at = datetime.now(timezone.utc)

    @property
    def elapsed_seconds(self) -> Optional[float]:
        """Total wall-clock seconds of the last finished run, or ``None``."""
        with self._lock:
            return self._run_elapsed

    @property
    def completed_at(self) -> Optional[datetime]:
        """UTC wall-clock time the last run finished, or ``None``."""
        with self._lock:
            return self._completed_at

    def restore(
        self,
        report: ValidationReport,
        completed_at: Optional[datetime],
        *,
        rechecked_tables: "Sequence[str]" = (),
        rechecked_at: Optional[datetime] = None,
    ) -> None:
        """Re-hydrate a persisted result on reconnect (no elapsed time available).

        Sets the report + finish time from the snapshot so the result page reopens;
        ``_run_elapsed`` stays ``None`` (a restored run has no live duration). The
        UI flags the result as restored-as-of ``completed_at`` so a stale verdict
        (source advanced since) prompts a re-validate.

        ``rechecked_tables``/``rechecked_at`` carry the per-table re-check marks
        from the snapshot, so a restored MERGED report still discloses that those
        rows are newer than the rest of the run (both default to empty, which is
        the ordinary "one uniform run" case and what older snapshots restore as).
        """
        with self._lock:
            self._result = report
            self._error = None
            self._progress = None
            self._completed_at = completed_at
            self._restored = True
            self._rechecked_tables = tuple(rechecked_tables)
            self._rechecked_at = rechecked_at
            self._recheck_tables = ()
            self._recheck_error = None

    @property
    def restored(self) -> bool:
        """True when the current result was re-hydrated from a snapshot."""
        with self._lock:
            return getattr(self, "_restored", False)

    def set_progress(self, table: str, index: int, total: int) -> None:
        """Record which table the running comparison is on (worker thread)."""
        with self._lock:
            self._progress = (table, index, total)

    @property
    def progress(self) -> Optional[tuple[str, int, int]]:
        """Return the current ``(table, index, total)`` being compared, if any."""
        with self._lock:
            return self._progress

    def clear_progress(self) -> None:
        """Forget any in-flight progress (before a new run / once finished)."""
        with self._lock:
            self._progress = None

    def set_result(self, result: ValidationReport) -> None:
        """Record a successful run's report (clears any prior error + progress).

        A full run replaces the whole report, so any earlier per-table re-check
        marks are cleared too -- every row is once again from one run.
        """
        with self._lock:
            self._result = result
            self._error = None
            self._progress = None
            self._restored = False  # a freshly-run result, not a restored one
            self._rechecked_tables = ()
            self._rechecked_at = None
            # A new verdict supersedes any earlier cut-over identity-sync outcome (the
            # target may have advanced since), so the runbook button offers a fresh run.
            self._cutover_identity_sync = None

    def set_identity_sync(self, advanced: "Optional[dict[str, int]]") -> None:
        """Record the identity-sequence re-sync outcome for the last full run.

        ``advanced`` maps each identity table to its new ``RESTART WITH`` value; an
        empty dict means the sync ran with nothing to advance, and ``None`` means it
        was not run. Set by the worker after a completed comparison.
        """
        with self._lock:
            self._identity_sync = advanced

    @property
    def identity_sync(self) -> "Optional[dict[str, int]]":
        """The last run's identity-sequence re-sync outcome (see ``set_identity_sync``)."""
        with self._lock:
            return dict(self._identity_sync) if self._identity_sync is not None else None

    def start_recheck(self, tables: "Sequence[str]") -> None:
        """Mark a per-table re-check of ``tables`` as starting (clears its error)."""
        with self._lock:
            self._recheck_tables = tuple(tables)
            self._recheck_error = None

    @property
    def recheck_tables(self) -> tuple[str, ...]:
        """Tables the in-flight (or just-finished) re-check covers; ``()`` if none."""
        with self._lock:
            return self._recheck_tables

    def finish_recheck(self) -> None:
        """Clear the in-flight re-check marker (the job reached a terminal state)."""
        with self._lock:
            self._recheck_tables = ()

    def merge_recheck_result(
        self,
        new_items: "Sequence[TableValidationResult]",
        *,
        orphan_findings: "Optional[Sequence[OrphanFinding]]" = None,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Splice a re-check's per-table results into the existing report.

        Returns ``False`` (and changes nothing) when there is no report to merge
        into -- e.g. a full re-run cleared it while the re-check was in flight --
        so a re-check can never resurrect a discarded report or create a
        single-table one that would read as the whole run.

        The merged report keeps the un-rechecked tables' verdicts and recomputes
        the overall one (see :func:`merge_revalidated`). The re-checked table names
        accumulate (re-checking table B after table A leaves BOTH marked as newer
        than the run) and the finish time is stamped so the UI can date them.
        """
        with self._lock:
            if self._result is None:
                return False
            merged = merge_revalidated(
                self._result, new_items, orphan_findings=orphan_findings
            )
            self._result = merged
            names = {item.table for item in new_items} & {
                item.table for item in merged.items
            }
            self._rechecked_tables = tuple(
                sorted(set(self._rechecked_tables) | names)
            )
            self._rechecked_at = completed_at or datetime.now(timezone.utc)
            # The report is no longer purely the restored snapshot -- part of it was
            # just measured live, so the "restored, may be stale" banner would be
            # misleading. The re-check note takes over the as-of story.
            self._restored = False
            return True

    @property
    def rechecked_tables(self) -> tuple[str, ...]:
        """Tables in the current report whose result came from a later re-check."""
        with self._lock:
            return self._rechecked_tables

    @property
    def rechecked_at(self) -> Optional[datetime]:
        """UTC time the last re-check finished, or ``None``."""
        with self._lock:
            return self._rechecked_at

    def set_recheck_error(self, message: str) -> None:
        """Record a re-check's failure (kept apart from a full run's error)."""
        with self._lock:
            self._recheck_error = message

    @property
    def recheck_error(self) -> Optional[str]:
        """Return the last re-check's failure message, if any."""
        with self._lock:
            return self._recheck_error

    def set_error(self, message: str) -> None:
        """Record a failure message for display."""
        with self._lock:
            self._error = message

    @property
    def result(self) -> Optional[ValidationReport]:
        """Return the last successful report, if any."""
        with self._lock:
            return self._result

    @property
    def error(self) -> Optional[str]:
        """Return the last failure message, if any."""
        with self._lock:
            return self._error

    def clear_outputs(self) -> None:
        """Discard the previous report/error before a (re-)run.

        Also drops the re-check marks: the report they annotated is gone, so
        keeping them would date rows in a report they don't belong to.
        """
        with self._lock:
            self._result = None
            self._error = None
            self._recheck_tables = ()
            self._rechecked_tables = ()
            self._rechecked_at = None
            self._recheck_error = None
            self._identity_sync = None
            self._cutover_identity_sync = None


@dataclass
class ValidationStore:
    """Process-memory map of session id to :class:`ValidationState`.

    Mirrors :class:`~dsql_migrator.ui.evaluation.EvaluationStore`: each UI session
    sees only its own validation state and nothing is persisted to disk.
    """

    _states: dict[str, ValidationState] = field(default_factory=dict)

    def get_or_create(self, session_id: str) -> ValidationState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = ValidationState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[ValidationState]:
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
        """
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            state.__init__()  # type: ignore[misc]  # re-run init on the same object


# ---------------------------------------------------------------------------
# NiceGUI screen
# ---------------------------------------------------------------------------

# How often the screen polls the background validation job (seconds).
_POLL_INTERVAL_SECONDS = 0.5


def build_validation_screen(
    store: SessionStore,
    session_id: str,
    *,
    job_manager: JobManager,
    eval_store: EvaluationStore,
    migration_store: DataMigrationStore,
    validation_store: ValidationStore,
    validator_factory: ValidatorFactory = _default_validator_factory,
    strategist_factory: StrategistFactory = _default_strategist_factory,
    sync_sequences: "Optional[Callable[..., dict]]" = None,
    conversion_store: "Optional[Any]" = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Validation screen, returning ``(content_builder, runner)``.

    ``content_builder`` renders the screen (status, options, validation report,
    drift, downloads) and is given the workflow shell's refresh callback so it
    can reflect background-job completion. ``runner`` is invoked by the step's
    Run/Re-run button: it validates the source/target connections and the Step 1
    inventory, reads the Step 3 export watermark, marks the step ``IN_PROGRESS``,
    and submits the validation to ``job_manager`` (returning immediately so the
    UI never blocks). Both plug into
    :func:`~dsql_migrator.ui.workflow.build_workflow_sidebar`.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    validation_state = validation_store.get_or_create(session_id)
    eval_state = eval_store.get_or_create(session_id)
    migration_state = migration_store.get_or_create(session_id)
    # Schema-Conversion state (optional): lets Validation resolve the APPLIED target
    # types so a CHECKSUM honors a target-type remap. None (e.g. no store wired / after a
    # reconnect) degrades to the source-derived default mapping.
    conv_state = conversion_store.get(session_id) if conversion_store is not None else None

    def _inventory() -> Optional[SourceInventory]:
        result = eval_state.result
        return result.inventory if result is not None else None

    def _migration_watermark() -> Optional[Watermark]:
        job_id = migration_state.job_id
        if job_id is None:
            return None
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return None
        return job.watermark

    def _migration_quarantined() -> dict[str, int]:
        """Per-table rows the Full Load permanently dropped, for deficit attribution.

        Same seam as :func:`_migration_watermark`: read from the live job. Empty after a
        reconnect (the job is gone and the counts are not persisted), which makes
        Validation report the deficit as unexplained rather than assuming it away.
        """
        from dsql_migrator.ui.data_migration import quarantined_rows_by_table

        job_id = migration_state.job_id
        if job_id is None:
            return {}
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return {}
        return quarantined_rows_by_table(job)

    def _applied_target_types(inv: "SourceInventory") -> dict[str, dict[str, str]]:
        """Per-table ``{column: applied DSQL target type}`` from the Schema-Conversion
        result, so the CHECKSUM renders each column by how it was actually STORED --
        honoring a target-type remap (e.g. TINYINT(1) kept as smallint) instead of the
        default source-derived mapping, which would else false-mismatch every row. Empty
        when the conversion state is unavailable (no store / reconnect) -> default mapping.
        """
        if conv_state is None:
            return {}
        try:
            from dsql_migrator.core.converter import (
                SchemaConverter,
                parse_target_column_types,
            )
            from dsql_migrator.ui.schema_conversion import applied_table_conversions

            applied = applied_table_conversions(
                SchemaConverter().convert(inv), conv_state.edited_target_ddls
            )
            out: dict[str, dict[str, str]] = {}
            for name, conversion in applied.items():
                types = parse_target_column_types(conversion.target_ddl)
                if types:
                    out[name] = types
            return out
        except Exception:  # noqa: BLE001 - never let target-type resolution break a run
            return {}

    def _run_prerequisite_error() -> Optional[str]:
        """Return why a comparison cannot start right now, or ``None`` if it can.

        Shared by the full run and the per-table re-check so both enforce exactly
        the same bar: a Step 1 inventory plus a CURRENTLY VERIFIED source and
        target. The target check matters just as much for a re-check as for a first
        run -- DSQL access is a short-lived IAM token, so a report that validated
        fine an hour ago can sit in front of an expired target connection.
        """
        inventory = _inventory()
        if inventory is None:
            return (
                "Run Step 1 (Evaluation) first to introspect the source schema, "
                "then run validation."
            )
        # Require a *verified* connection, not just a configured one: starting a
        # run against an unreachable/untested source or target would otherwise
        # block on connect and leave the step spinning. Fail fast with a clear
        # call to action instead of submitting a job.
        if not session.has_source() or not getattr(
            session, "source_verified", False
        ):
            return (
                "Source connection is not verified. Test the source connection on "
                "the Connect screen, then run validation."
            )
        if not session.has_target() or not getattr(
            session, "target_verified", False
        ):
            return (
                "Target connection is not verified. Test the target connection on "
                "the Connect screen, then run validation."
            )
        if not inventory.tables:
            return "The source inventory has no tables to validate."
        return None

    def runner() -> None:
        gate = _run_prerequisite_error()
        if gate is not None:
            validation_state.set_error(gate)
            return
        inventory = _inventory()
        assert inventory is not None  # guaranteed by the gate

        # Validate exactly what was migrated: narrow the inventory to the Data
        # Migration table selection (empty => all). Validating un-migrated tables
        # would flag them all as "missing on target".
        scope = resolve_validation_tables(inventory, migration_state.selection)
        if not scope.tables:
            validation_state.set_error(
                "No tables in scope to validate. Select tables in Step 3 (Data "
                "Migration) or check the source inventory."
            )
            return
        # Then apply the optional object exclusions chosen on this screen (none =>
        # all in-scope). Exclusions are converted to the include-set apply_table_
        # filter expects; excluding everything degrades to the full scope so a run
        # never ends up empty.
        scope_names = [t.name for t in scope.tables]
        include = included_from_exclusions(
            scope_names, validation_state.table_exclude
        )
        scoped_tables = apply_table_filter(scope.tables, include)
        scoped_inventory = inventory.model_copy(
            update={"tables": list(scoped_tables)}
        )

        source_config = session.source_config
        target_config = session.target_config
        assert source_config is not None  # guaranteed by has_source()
        assert target_config is not None  # guaranteed by has_target()
        inputs = ValidationInputs(
            source_config=source_config,
            source_password=session.source_password,
            target_config=target_config,
            inventory=scoped_inventory,
            mode=validation_state.mode,
            check_orphans=validation_state.check_orphans,
            watermark=_migration_watermark(),
            reconcile=validation_state.reconcile,
            deep_only_on_count_mismatch=(
                validation_state.deep_only_on_count_mismatch
            ),
            # Skip columns the migration excluded (CDC oversized-LOB exclusion), so a
            # column that was never written to the target can't cause a false checksum
            # mismatch. Empty for a migration that excluded nothing.
            excluded_columns=migration_state.lob_exclusions(),
            quarantined_by_table=_migration_quarantined(),
            # Applied target types so a CHECKSUM honors a Schema-Conversion remap.
            target_types=_applied_target_types(scoped_inventory),
        )

        validation_state.clear_outputs()
        validation_state.clear_progress()
        validation_state.cancel_requested = False
        validation_state.mark_run_started()
        session.set_workflow(
            with_status(
                session.workflow, WorkflowStep.VALIDATION, StepStatus.IN_PROGRESS
            )
        )

        def work(handle: object) -> None:
            # The worker polls the job handle's cancelled flag between tables. On a
            # cooperative stop the validator raises ValidationCancelled; we catch
            # it and RETURN normally (without storing a result) so the JobManager
            # records the job as CANCELLED, not FAILED -- and no partial report is
            # ever displayed. on_progress writes live per-table progress for the UI
            # poller (worker thread -> ValidationState lock).
            log_activity(
                ActivityCategory.VALIDATION,
                "validation started",
                status=ActivityStatus.STARTED,
                detail=(
                    f"{inputs.mode.value} comparison of "
                    f"{len(scoped_tables)} table(s), source vs target"
                ),
            )
            try:
                result = run_validation(
                    inputs,
                    validator_factory=validator_factory,
                    should_cancel=lambda: bool(getattr(handle, "cancelled", False)),
                    on_progress=validation_state.set_progress,
                    deep_only_on_count_mismatch=inputs.deep_only_on_count_mismatch,
                )
            except ValidationCancelled:
                return
            finally:
                # Stamp the elapsed time + clear progress whether the run finished,
                # was cancelled, or errored -- so a completed report can show how
                # long it took and a cancelled/failed run does not show a stale one.
                validation_state.mark_run_finished()
                validation_state.clear_progress()
            validation_state.set_result(result)
            # Record the verdict in the audit trail. Validation is the step that PROVES
            # source == target, so its outcome (match/mismatch, mode, how many tables
            # passed) is exactly what a reviewer attaches to a change ticket -- yet it
            # lived only in the UI. Logged as SUCCESS on a clean match, WARNING-tier
            # (INFO here; ActivityStatus has no WARNING) escalated to FAILURE when tables
            # mismatch, so a no-go verdict reads loud. Value-free (counts, not rows).
            _summary = summarize_validation(result)
            _recon = (
                f"; {_summary.missing_on_target} missing / "
                f"{_summary.extra_on_target} extra on target"
                if _summary.reconcile_performed
                else ""
            )
            log_activity(
                ActivityCategory.VALIDATION,
                "validation completed",
                status=ActivityStatus.SUCCESS if _summary.is_match else ActivityStatus.FAILURE,
                detail=(
                    f"{_summary.mode} verdict: "
                    + ("MATCH" if _summary.is_match else "MISMATCH")
                    + f" — {_summary.matched_tables}/{_summary.total_tables} table(s) "
                    f"matched, {_summary.mismatched_tables} mismatched"
                    + (f", {_summary.errored_tables} errored" if _summary.errored_tables else "")
                    + _recon
                    + (
                        f"; failing: {', '.join(_summary.failed_tables[:8])}"
                        + (" (+more)" if len(_summary.failed_tables) > 8 else "")
                        if _summary.failed_tables
                        else ""
                    )
                    + (
                        "; ready for cut-over" if _summary.ready_for_cutover else ""
                    )
                ),
            )
            # Re-sync identity sequences past the CURRENT target MAX(pk). Full Load's
            # own post-load sync cannot see rows CDC inserted afterwards (explicit ids
            # don't advance a GENERATED BY DEFAULT sequence), so by cut-over the
            # sequence lags and the app's first insert would collide. Validation is the
            # step just before cut-over (after CDC drains), so closing it here is the
            # right moment. Read-only comparison stays read-only; this is a separate,
            # reported target write that never fails the report.
            advanced, failed = resync_identity_sequences(
                target_config,
                [table.name for table in scoped_tables],
                aws_profile=session.aws_profile,
                sync=sync_sequences,
            )
            validation_state.set_identity_sync(advanced)
            if advanced:
                detail = ", ".join(
                    f"{name} -> RESTART WITH {value}"
                    for name, value in sorted(advanced.items())
                )
                log_activity(
                    ActivityCategory.VALIDATION,
                    "identity sequences re-synced",
                    status=ActivityStatus.SUCCESS,
                    detail=(
                        f"{len(advanced)} identity primary key(s) advanced past the "
                        f"current target rows so the application's first insert after "
                        f"cut-over cannot collide: {detail}"
                    ),
                )
            if failed:
                # A failed RESTART WITH is a post-cut-over duplicate-key risk, so log it
                # as an ERROR (never swallowed / painted as success). Reasons are
                # value-free (Property 7).
                fdetail = ", ".join(
                    f"{name}: {reason}" for name, reason in sorted(failed.items())
                )
                log_activity(
                    ActivityCategory.VALIDATION,
                    "identity sequence sync failed",
                    status=ActivityStatus.FAILURE,
                    detail=(
                        f"{len(failed)} identity sequence(s) could NOT be advanced; the "
                        f"application's first insert after cut-over may collide — retry "
                        f"the sync or advance them manually: {fdetail}"
                    ),
                )

        validation_state.job_id = job_manager.submit(work)

    def _recheck(tables: "Sequence[str]") -> None:
        """Re-validate ONLY ``tables`` and merge the result into the current report.

        The per-table "Re-check" action: after investigating (and fixing) a failing
        table, re-compare just that table instead of re-running the whole
        comparison, keeping every other table's verdict and letting the overall
        cut-over verdict update on its own.

        Deliberate choices:

        - The comparison OPTIONS come from the existing report
          (:func:`report_run_options`), not the live toggles, so the merged report
          stays internally consistent (and a restored report is re-checkable).
        - ``deep_only_on_count_mismatch`` is forced OFF: this table is already known
          to differ, so the fast sweep's "skip deep checks when counts agree" would
          be exactly the wrong economy -- if the counts now agree we specifically
          want the checksum/reconciliation to confirm the rows really match.
        - The step status stays as-is (DONE). Flipping it to IN_PROGRESS would hide
          the whole report mid-re-check and lock the shell's Re-run; the running
          state is shown inline on the affected rows instead.
        - It reuses the SINGLE ``job_id`` slot, so a re-check and a full run can
          never be in flight together.
        """
        report = validation_state.result
        if report is None:
            return  # nothing to merge into (the report was cleared)
        wanted = tuple(dict.fromkeys(tables))  # de-dup, keep order
        if not wanted:
            return
        gate = _run_prerequisite_error()
        if gate is not None:
            validation_state.set_recheck_error(gate)
            return
        inventory = _inventory()
        assert inventory is not None  # guaranteed by the gate

        # Compare the SAME table definitions the report covers: resolve from the
        # inventory (the source of truth for columns/PK) and keep only the requested
        # names. A name that is no longer in the inventory is dropped -- and if that
        # leaves nothing, say so instead of silently re-checking everything.
        by_name = {t.name: t for t in inventory.tables}
        scoped = [by_name[name] for name in wanted if name in by_name]
        if not scoped:
            validation_state.set_recheck_error(
                "These tables are no longer in the source inventory. Re-run "
                "Step 1 (Evaluation), then validate again."
            )
            return

        options = report_run_options(report)
        source_config = session.source_config
        target_config = session.target_config
        assert source_config is not None  # guaranteed by the gate
        assert target_config is not None  # guaranteed by the gate
        inputs = ValidationInputs(
            source_config=source_config,
            source_password=session.source_password,
            target_config=target_config,
            inventory=inventory.model_copy(update={"tables": scoped}),
            mode=options.mode,
            check_orphans=options.check_orphans,
            watermark=_migration_watermark(),
            reconcile=options.reconcile,
            # Always deep-check a re-check (see docstring).
            deep_only_on_count_mismatch=False,
            excluded_columns=migration_state.lob_exclusions(),
            quarantined_by_table=_migration_quarantined(),
            # Applied target types so a CHECKSUM honors a Schema-Conversion remap.
            target_types=_applied_target_types(
                inventory.model_copy(update={"tables": scoped})
            ),
        )

        validation_state.start_recheck([t.name for t in scoped])
        validation_state.cancel_requested = False

        def work(handle: object) -> None:
            try:
                fresh = run_validation(
                    inputs,
                    validator_factory=validator_factory,
                    should_cancel=lambda: bool(getattr(handle, "cancelled", False)),
                    deep_only_on_count_mismatch=False,
                )
            except ValidationCancelled:
                return  # cancelled: the report is simply left as it was
            # Merge under the state lock; a False return means a full re-run cleared
            # the report while this was in flight, so there is nothing to update.
            validation_state.merge_recheck_result(
                fresh.items,
                orphan_findings=(
                    fresh.orphan_findings if options.check_orphans else None
                ),
            )

        validation_state.job_id = job_manager.submit(work)

    def content(refresh: Callable[[], None]) -> None:
        status = get_status(session.workflow, WorkflowStep.VALIDATION)
        inventory = _inventory()

        with ui.column().classes("w-full gap-3"):
            ui.label(
                "The final pre-cut-over check: exact counts, checksums and per-table "
                "primary-key reconciliation confirm the target matches the source "
                "(as-of the export watermark). Results download as a report."
            ).classes("text-sm text-gray-500")

            # The step header + journey stepper already show the status badge, so
            # it is not repeated here (avoids a third copy of the same signal).
            # Prerequisites the RUN actually enforces, surfaced UPFRONT so the user
            # fixes them before clicking (not only as a post-click error): source
            # inventory, then a verified source connection, then a verified target
            # connection. Validation reads BOTH ends live, so -- exactly like the
            # source -- the target must have a current, verified connection; a
            # configured-but-unverified (or expired) target is called out the same
            # way the source is, since the run reconnects to it.
            conn_notices = _connection_prerequisite_notices(
                session, inventory_ready=inventory is not None
            )
            for tone, header, body in conn_notices:
                render_notice(ui, tone=tone, header=header, body=body)
            if not conn_notices and _migration_watermark() is None:
                # Expected/optional state (validation still runs live), so info —
                # not an alarming warning (severity calibration).
                render_notice(
                    ui,
                    tone="info",
                    header="No export watermark — comparing against the live source",
                    body=(
                        "Run Step 3 (Data Migration) first to validate as-of the "
                        "exact consistency point instead."
                    ),
                )

            # CDC-active advisory: when the stream is still RUNNING, the target is a
            # moving target, so a non-zero count/PK difference may be replication lag
            # rather than data loss. Validation is read-only and safe to run now
            # (and useful for watching convergence), but a CONFIRMATORY cut-over
            # check should run with the source quiesced so a clean MATCH truly means
            # zero loss. Info, not warning — running mid-CDC is expected and fine.
            active = cdc_active_connector_names(migration_state)
            if active:
                render_notice(
                    ui,
                    tone="info",
                    header="CDC is still streaming — differences may be lag, not loss",
                    body=(
                        "A small source/target difference is likely replication lag. "
                        "For a final cut-over check, stop source writes (let CDC "
                        "drain) so a clean match confirms zero data loss."
                    ),
                )

            # "Validating" context card: WHAT this run covers (source -> target,
            # which/how many tables, as-of point). Shown when the inventory is known
            # and a run is NOT in flight: during IN_PROGRESS the whole screen
            # re-renders on every ~poll, which would recreate the object chips and
            # make their hover tooltips flicker; the scope is also read-only then
            # (the filter is disabled) and the live progress panel already says
            # what's running. So the scope/filter is hidden while running and
            # returns once the run settles.
            if inventory is not None and status is not StepStatus.IN_PROGRESS:
                scope = resolve_validation_tables(
                    inventory, migration_state.selection
                )
                scope_view = build_validation_scope(
                    source_config=session.source_config,
                    target_config=session.target_config,
                    target_cluster_name=getattr(
                        session, "target_cluster_name", None
                    ),
                    scope=scope,
                    watermark=_migration_watermark(),
                    table_filter=included_from_exclusions(
                        [t.name for t in scope.tables],
                        validation_state.table_exclude,
                    ),
                )
                _render_scope_card(
                    ui,
                    scope_view,
                    scope_tables=[t.name for t in scope.tables],
                    validation_state=validation_state,
                    status=status,
                    refresh=refresh,
                )

            # Options (comparison mode, reconcile/orphan/fast-sweep) also carry
            # hover tooltips, so they are hidden during a run for the same reason as
            # the scope card -- they apply only to the NEXT run and are disabled
            # while one is in flight. Shown otherwise.
            if status is not StepStatus.IN_PROGRESS:
                _render_options(
                    ui,
                    validation_state,
                    status,
                    refresh=refresh,
                )

            error = validation_state.error
            if error and status is not StepStatus.IN_PROGRESS:
                render_notice(
                    ui,
                    tone="error",
                    header="Validation failed",
                    body=error,
                )

            # A per-table re-check that could not START (e.g. the target token
            # expired since the report was produced) reports separately from a full
            # run's failure -- the existing report is still perfectly valid, so this
            # must not read as "Validation failed".
            recheck_error = validation_state.recheck_error
            if recheck_error:
                render_notice(
                    ui,
                    tone="warning",
                    header="Could not re-check those tables",
                    body=recheck_error,
                )

            # A cancelled run leaves the step NOT_STARTED (no report); surface it
            # as a calm info notice (not an error) so the user knows it stopped.
            if (
                validation_state.cancel_requested
                and status is StepStatus.NOT_STARTED
                and validation_state.result is None
                and not error
            ):
                render_notice(
                    ui,
                    tone="info",
                    header="Validation cancelled",
                    body=(
                        "The run was stopped before completing, so no report was "
                        "produced. Adjust the scope/options if needed and Re-run to "
                        "validate again. Nothing on the target was changed "
                        "(validation is read-only)."
                    ),
                )

            _reconciled_late = False
            if status is StepStatus.IN_PROGRESS:
                # Guard against a "stuck spinner": the step is IN_PROGRESS but no
                # live job is actually running it. This happens when the persisted
                # session snapshot restored IN_PROGRESS after a process restart /
                # page reload (the validation job id is not persisted, and the
                # JobManager is fresh), so polling would never finalize -- OR when
                # the run finished while the user was on ANOTHER step (the poll timer
                # is torn down on navigation, so nothing flipped the status to DONE).
                if _running_job_alive(job_manager, validation_state):
                    _render_in_progress(
                        ui, job_manager, session, validation_state, refresh
                    )
                elif validation_state.result is not None:
                    # The run actually FINISHED (a report is present) but the step
                    # was left/restored as IN_PROGRESS without a live job (the DONE
                    # flip's persist did not land, or a reconnect restored a stale
                    # status). Reconcile to DONE so the completed report shows --
                    # never a misleading "not started"/"interrupted" for a run that
                    # produced a result.
                    session.set_workflow(
                        with_status(
                            session.workflow,
                            WorkflowStep.VALIDATION,
                            StepStatus.DONE,
                        )
                    )
                    status = StepStatus.DONE
                    _reconciled_late = True
                else:
                    # Truly orphaned with no result: reconcile to NOT_STARTED and
                    # tell the user to re-run, instead of spinning forever.
                    session.set_workflow(
                        with_status(
                            session.workflow,
                            WorkflowStep.VALIDATION,
                            StepStatus.NOT_STARTED,
                        )
                    )
                    status = StepStatus.NOT_STARTED
                    _reconciled_late = True
                    render_notice(
                        ui,
                        tone="warning",
                        header="Previous validation was interrupted",
                        body=(
                            "A validation run was in progress but is no longer "
                            "active (the app restarted or the page was reloaded). "
                            "No report was produced and nothing on the target was "
                            "changed (validation is read-only). Click Re-run to "
                            "validate again."
                        ),
                    )
            if _reconciled_late:
                # This in-content reconcile changed the PERSISTED workflow status, but
                # the workflow shell (step header badge + "Re-run validation" button)
                # already rendered with the stale IN_PROGRESS status THIS pass, and
                # nothing else re-renders it -- so the step would stay stuck showing an
                # "In progress" badge + a permanently-locked Re-run over the finished
                # report. Defer a one-shot refresh so the shell re-renders with the
                # reconciled status. The next pass reads DONE/NOT_STARTED (no reconcile,
                # so no further timer) -- a single extra render, never a loop.
                ui.timer(0.05, refresh, once=True)  # type: ignore[attr-defined]

            result = validation_state.result
            if result is not None and status is not StepStatus.IN_PROGRESS:
                # On-demand AI diagnosis of mismatches is opt-in (AI Assist on).
                # Build the shared chat drawer + a streamer bound to the validation
                # grounding; when AI is off, no opener is passed and the renderer
                # omits the AI buttons (the deterministic report stands on its own).
                diagnose_provider = None
                if session.ai_assist.enabled:
                    strategist = strategist_factory(
                        session.ai_assist, session.aws_profile
                    )
                    open_chat = build_chat_drawer(ui)

                    def diagnose_provider(  # noqa: E731 - small bound opener
                        *, title, subtitle, first_question, facts, scope
                    ):
                        open_chat(
                            title=title,
                            subtitle=subtitle,
                            first_question=first_question,
                            streamer=lambda messages, on_delta: (
                                strategist.stream_validation_chat(
                                    facts, messages, on_delta, scope=scope
                                )
                            ),
                        )

                # Per-table re-check: which tables are being re-compared right now
                # (inline spinner on those rows) and the action that starts one. The
                # action is withheld while ANY comparison is in flight so the single
                # job slot is never double-claimed.
                rechecking = validation_state.recheck_tables
                busy = bool(rechecking) and _running_job_alive(
                    job_manager, validation_state
                )
                if rechecking and not busy:
                    # The re-check job settled (merged, failed, or was cancelled)
                    # while we were elsewhere; clear the marker so the rows stop
                    # showing a spinner.
                    validation_state.finish_recheck()
                    rechecking = ()

                def _start_recheck(tables: "Sequence[str]") -> None:
                    _recheck(tables)
                    refresh()

                _render_result(
                    ui, result,
                    diagnose_provider=diagnose_provider,
                    restored=validation_state.restored,
                    completed_at=validation_state.completed_at,
                    recheck_provider=None if busy else _start_recheck,
                    rechecking_tables=rechecking,
                    rechecked_tables=validation_state.rechecked_tables,
                    rechecked_at=validation_state.rechecked_at,
                    cdc_in_use=_cdc_in_use(session),
                    identity_sync=validation_state.identity_sync,
                )
                if busy:
                    _install_recheck_poll_timer(
                        ui, job_manager, validation_state, refresh
                    )

    return content, runner


def _install_recheck_poll_timer(
    ui: object,
    job_manager: JobManager,
    validation_state: "ValidationState",
    refresh: Callable[[], None],
) -> None:
    """Poll an in-flight per-table re-check once, re-arming via the next render.

    Same one-shot discipline as :func:`_install_poll_timer` (each render installs
    exactly one timer, so timers never accumulate), but deliberately does NOT touch
    the workflow step status: a re-check runs on top of an already-``DONE`` step and
    must leave that verdict -- and the visible report -- in place. On a terminal
    state it clears the in-flight marker (surfacing a failure as the re-check's own
    error) and refreshes so the merged rows appear.
    """
    job_id = validation_state.job_id
    if job_id is None:
        return

    def poll() -> None:
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            validation_state.finish_recheck()
            refresh()
            return
        if job.status in ("PENDING", "RUNNING"):
            refresh()  # still running: re-render, which installs the next timer
            return
        if job.status == "FAILED":
            validation_state.set_recheck_error(
                job_manager.get_error(job_id)
                or "The re-check failed. Try again, or re-run the full validation."
            )
        validation_state.finish_recheck()
        refresh()

    ui.timer(_POLL_INTERVAL_SECONDS, poll, once=True)  # type: ignore[attr-defined]


def _cutover_summary_for_preview() -> "ValidationSummary":
    """A synthetic 'ready' summary used only for the dev-unlock cut-over preview.

    Never used in real flows (the real summary comes from a validation run); it
    exists so a developer reviewing the UI with DSQL_MIGRATOR_DEV_UNLOCK_STEPS on
    can see the go-path runbook without a verdict.
    """
    return ValidationSummary(
        total_tables=0,
        matched_tables=0,
        mismatched_tables=0,
        orphan_count=0,
        is_match=True,
        mode="checksum",
        as_of="preview",
        reconcile_performed=True,
        reconciled_tables=0,
        inconsistent_tables=0,
        missing_on_target=0,
        extra_on_target=0,
        errored_tables=0,
        ready_for_cutover=True,
    )


def _run_cutover_identity_sync(
    session: object,
    validation_state: "ValidationState",
    report: "Optional[ValidationReport]",
    *,
    job_manager: "Optional[JobManager]" = None,
    sync: "Optional[Callable[..., dict]]" = None,
    refresh: "Optional[Callable[[], None]]" = None,
) -> None:
    """Advance identity sequences past the current target ``MAX(pk)`` (operator action).

    Invoked by the explicit "Sync identity sequences" button in the cut-over runbook --
    NOT on render, so viewing the screen never writes to the target. Keyed off the
    CURRENT ``MAX(pk)`` over the validated tables, so any rows CDC delivered after the
    last Validation are covered before the operator repoints the app (the app's first
    insert would otherwise collide, 23505). Runs in the background (a target write) so
    the click never blocks the UI; the outcome is stored on ``validation_state`` and the
    screen refreshes to show it. Best-effort -- ``resync_identity_sequences`` never
    raises; the ``is_identity`` catalog filter skips non-identity tables.
    """
    target_config = getattr(session, "target_config", None)
    table_names = [item.table for item in report.items] if report is not None else []
    if target_config is None or not table_names:
        # Nothing to sync (no target / no tables): record an empty result so the button
        # reflects "ran, nothing to do" rather than staying in the un-run state.
        validation_state.set_cutover_identity_sync({})
        if refresh is not None:
            refresh()
        return
    aws_profile = getattr(session, "aws_profile", None)

    def _work(_handle: object = None) -> None:
        advanced, failed = resync_identity_sequences(
            target_config, table_names, aws_profile=aws_profile, sync=sync
        )
        if advanced:
            detail = ", ".join(
                f"{name} -> RESTART WITH {value}"
                for name, value in sorted(advanced.items())
            )
            log_activity(
                ActivityCategory.VALIDATION,
                "identity sequences re-synced (cut-over)",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{len(advanced)} identity primary key(s) advanced past the current "
                    "target rows before cut-over, so the application's first insert "
                    f"after cut-over cannot collide: {detail}"
                ),
            )
        if failed:
            # Surface a failed RESTART WITH as an ERROR — the cut-over runbook renders it
            # so the operator cannot repoint the app believing the sequences are safe
            # (audit finding D2). Reasons are value-free (Property 7).
            fdetail = ", ".join(
                f"{name}: {reason}" for name, reason in sorted(failed.items())
            )
            log_activity(
                ActivityCategory.VALIDATION,
                "identity sequence sync failed (cut-over)",
                status=ActivityStatus.FAILURE,
                detail=(
                    f"{len(failed)} identity sequence(s) could NOT be advanced before "
                    f"cut-over; the application's first insert may collide — retry the "
                    f"sync or advance them manually before repointing: {fdetail}"
                ),
            )
        validation_state.set_cutover_identity_sync(advanced, failed)
        if refresh is not None:
            refresh()

    if job_manager is not None:
        job_manager.submit(_work)
    else:
        _work()


def build_cutover_screen(
    store: SessionStore,
    session_id: str,
    *,
    validation_store: ValidationStore,
    job_manager: Optional[JobManager] = None,
    sync_sequences: "Optional[Callable[..., dict]]" = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Cut over step (step 6), returning ``(content_builder, runner)``.

    Cut-over is the one step the tool does NOT perform: repointing the application
    from MySQL to DSQL is an operational act only the operator can do. So this step
    has no job to run — the ``runner`` simply marks the step ``DONE`` when the user
    acknowledges they have cut over (the workflow shell's Run button is hidden for
    this step; a dedicated in-content button drives the acknowledgement).

    The content reflects the *last validation verdict* so the guidance is honest:
    on a clean MATCH it shows the cut-over runbook (tailored to whether CDC is in
    use); otherwise it tells the user to get a clean Validation first and offers no
    "I've cut over" affordance. This keeps cut-over gated on evidence, not vibes.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    validation_state = validation_store.get_or_create(session_id)

    def runner() -> None:
        # No job: mark DONE as the user's acknowledgement that they've cut over.
        session.set_workflow(
            with_status(session.workflow, WorkflowStep.CUT_OVER, StepStatus.DONE)
        )
        # Record the cut-over decision -- the journey's conclusion. Which release state
        # the operator acknowledged (a clean match vs an explicitly ACCEPTED gap of
        # permanently-dropped rows) is the single most important audit fact, yet nothing
        # was logged when "I've cut over" was clicked. Compute the same release state the
        # runbook gated on so the log records exactly what was signed off.
        _report = validation_state.result
        _summary = summarize_validation(_report) if _report is not None else None
        _release = cutover_release_state(
            _summary, gap_accepted=validation_state.accept_explained_gap
        )
        _accepted_gap = _release == "accepted"
        log_activity(
            ActivityCategory.VALIDATION,
            "cut over acknowledged",
            status=ActivityStatus.SUCCESS,
            detail=(
                "operator acknowledged cut-over to Aurora DSQL ("
                + (
                    "clean match" if _release == "clean"
                    else "ACCEPTED gap: some rows were permanently dropped and the "
                         "operator signed off on migrating without them"
                    if _accepted_gap
                    else f"release state: {_release}"
                )
                + ")"
            ),
        )

    def content(refresh: Callable[[], None]) -> None:
        with _section(ui, icon="rocket_launch", title="Cut over to Aurora DSQL"):
            render_notice(
                ui,
                tone="info",
                header="The final step is yours to perform",
                body=(
                    "Cut-over is the moment your application stops writing to MySQL "
                    "and starts using Aurora DSQL. The tool has done its job — read "
                    "the source, converted the schema, loaded the data, and proven "
                    "consistency — but repointing your application is an operational "
                    "act only you can do. This step is the runbook for doing it "
                    "safely, with a rollback path."
                ),
            )

        report = validation_state.result
        summary = summarize_validation(report) if report is not None else None
        drift = format_drift(report) if report is not None else None

        # The runbook's "Sync identity sequences" button calls this: advance identity
        # keys past the current target MAX(pk), in the background, then refresh to show
        # the outcome. Defined here so it closes over this render's report/refresh.
        def _identity_sync_provider() -> None:
            _run_cutover_identity_sync(
                session, validation_state, report,
                job_manager=job_manager, sync=sync_sequences, refresh=refresh,
            )

        # Dev-only UI review: with no clean verdict, synthesize a ready summary so
        # the runbook itself can be reviewed without running the whole workflow.
        # Gated on the same dev flag as the nav unlock; never reached in real use.
        if (summary is None or not summary.ready_for_cutover) and _dev_unlock_steps():
            render_notice(
                ui,
                tone="warning",
                header="Developer preview (no real validation verdict)",
                body=(
                    "DSQL_MIGRATOR_DEV_UNLOCK_STEPS is on, so the cut-over runbook "
                    "below is shown for UI review only. In real use it appears only "
                    "after Validation reports a clean MATCH."
                ),
            )
            summary = summary or _cutover_summary_for_preview()
            _render_cutover_section(
                ui, summary, drift, cdc_in_use=_cdc_in_use(session)
            )
            return

        release = cutover_release_state(
            summary, gap_accepted=validation_state.accept_explained_gap
        )

        # The gate's own copy has always promised cut-over is reachable when "every
        # difference is explained" -- but the gate tested ``ready_for_cutover``, which is
        # a bare match. So a run whose ONLY finding was a permanently quarantined row
        # could never reach cut-over, and no amount of reloading would change that: the
        # value is one DSQL cannot store. The step was unreachable by design rather than
        # by any decision the operator made. Offer the acknowledgement instead.
        if release == "acceptable":
            rows = summary.quarantine_explained_rows  # type: ignore[union-attr]
            tables = summary.quarantine_explained_tables  # type: ignore[union-attr]
            row_noun = "row" if rows == 1 else "rows"
            table_noun = "table" if len(tables) == 1 else "tables"
            with _section(ui, icon="fact_check", title="Acknowledge the known gap"):
                render_notice(
                    ui,
                    tone="warning",
                    header="Every difference is explained — cut-over needs your sign-off",
                    body=(
                        f"Validation found no unexplained difference. The {rows} "
                        f"{row_noun} missing from {len(tables)} {table_noun} "
                        f"({', '.join(tables)}) are exactly the rows the migration "
                        "already reported dropping, because DSQL could not store those "
                        "values — reloading will not change that. Cutting over means "
                        f"accepting that those {row_noun} will not exist on the target. "
                        "Confirm below to unlock the runbook, or fix the source value(s) "
                        "and re-run Validation for a full match instead."
                    ),
                )

                def _accept() -> None:
                    validation_state.accept_explained_gap = True
                    refresh()

                ui.button(  # type: ignore[attr-defined]
                    f"Accept the {rows}-{row_noun} gap and continue to cut-over",
                    icon="check",
                    on_click=_accept,
                ).props("no-caps color=primary")
            return

        if release == "blocked":
            with _section(ui, icon="fact_check", title="Validate first"):
                render_notice(
                    ui,
                    tone="warning",
                    header="Get a clean validation before you cut over",
                    body=(
                        "Cut over only when Validation reports a clean MATCH, or when "
                        "every difference is explained by rows the migration already "
                        "reported dropping (you can then acknowledge that gap here). Go "
                        "to the Validation step and run it; the cut-over runbook appears "
                        "here once there is nothing unexplained left."
                        if summary is not None
                        else
                        "No validation result yet. Run the Validation step first — "
                        "the cut-over runbook appears here once it reports a clean "
                        "MATCH."
                    ),
                )
            return

        # Released with an accepted gap: the runbook follows, but it must not read as a
        # clean match -- the operator is cutting over to a target that is knowingly short.
        if release == "accepted":
            rows = summary.quarantine_explained_rows  # type: ignore[union-attr]
            tables = summary.quarantine_explained_tables  # type: ignore[union-attr]
            row_noun = "row" if rows == 1 else "rows"
            render_notice(
                ui,
                tone="warning",
                header="Cutting over with an accepted gap",
                body=(
                    f"You accepted that {rows} {row_noun} could not be migrated "
                    f"({', '.join(tables)}). Everything else matched. Those {row_noun} "
                    "will be absent on DSQL after cut-over — keep the source available "
                    "if you may still need them."
                ),
            )

        # Go path: the verdict is clean — show the tailored runbook + an explicit
        # acknowledgement that marks the step (and the whole journey) Done. The runbook
        # includes an explicit "Sync identity sequences" action (an operator step, not a
        # render side-effect): identity keys must be advanced past the current target
        # MAX(pk) BEFORE the app repoints, or its first insert collides (23505).
        _render_cutover_section(
            ui, summary, drift,
            cdc_in_use=_cdc_in_use(session),
            identity_sync_provider=_identity_sync_provider,
            identity_sync_result=validation_state.cutover_identity_sync,
            identity_sync_failed=validation_state.cutover_identity_sync_failed,
        )

        done = get_status(session.workflow, WorkflowStep.CUT_OVER) is StepStatus.DONE
        with _section(ui, icon="flag", title="Finish"):
            if done:
                render_notice(
                    ui,
                    tone="success",
                    header="Cut-over complete",
                    body=(
                        "You've marked the cut-over done — your application is live "
                        "on Aurora DSQL. Keep the source as a read-only rollback "
                        "anchor until you've signed off."
                    ),
                )
            else:
                ui.label(  # type: ignore[attr-defined]
                    "When your application is live on DSQL and smoke-tested, mark "
                    "the cut-over complete to finish the migration journey."
                ).classes("text-sm text-gray-600")
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    def _acknowledge() -> None:
                        runner()
                        refresh()

                    ui.button(  # type: ignore[attr-defined]
                        "I've cut over to DSQL",
                        icon="check_circle",
                        on_click=_acknowledge,
                    ).props("color=primary")

    return content, runner


def validation_run_guard_reason(
    job_manager: JobManager, validation_state: "ValidationState"
) -> Optional[str]:
    """Return a disable reason for the step's Re-run button, or ``None``.

    A per-table re-check and a full run share ONE job slot, so while a re-check is
    in flight the shell's "Re-run validation" must be disabled -- otherwise a
    full run would overwrite ``job_id``, orphaning the re-check job and (worse)
    clearing the very report the re-check is about to merge into. The re-check
    buttons are withheld symmetrically while any comparison runs, so the two can
    never collide in either direction.

    NiceGUI-agnostic (takes the manager + state), so the guard is unit-testable.
    """
    if validation_state.recheck_tables and _running_job_alive(
        job_manager, validation_state
    ):
        return (
            "A table re-check is running. Wait for it to finish, then re-run the "
            "full validation."
        )
    return None


def _running_job_alive(
    job_manager: JobManager, validation_state: "ValidationState"
) -> bool:
    """Return whether a live (PENDING/RUNNING) validation job actually exists.

    The step status alone can read IN_PROGRESS without a backing job after a
    restart/reload (the validation job id is not persisted). This confirms there
    is a real in-flight job before the UI shows the running spinner, so an
    orphaned status is reconciled instead of spinning forever.
    """
    job_id = validation_state.job_id
    if job_id is None:
        return False
    try:
        job = job_manager.get_status(job_id)
    except JobNotFoundError:
        return False
    return job.status in ("PENDING", "RUNNING")


def _render_in_progress(
    ui: object,
    job_manager: JobManager,
    session: object,
    validation_state: "ValidationState",
    refresh: Callable[[], None],
) -> None:
    """Render the running state: progress text + a Cancel action (AWS-styled).

    The Cancel button is a Cloudscape "normal/secondary" destructive-intent action
    (outlined, negative color) placed beside the spinner. It requests a
    cooperative stop via the JobManager and the run ends CANCELLED with no partial
    report. Once requested, the button switches to a disabled "Stopping…" state so
    the click is acknowledged immediately.

    The stop is only as prompt as the validator's polling points: before each table,
    and every few thousand merged rows inside a PK reconciliation. A ``COUNT(*)`` or
    checksum already executing on a large table has NO interruption point, so it
    runs to completion first (minutes), as does every table being compared
    concurrently. The stopping copy therefore states what is being waited on rather
    than implying an immediate halt -- a bare "Stopping…" beside the unchanged
    "safe to leave running" panel read as a cancel that had been ignored.
    """
    # Elements are built ONCE and then updated IN PLACE by the poll, instead of the
    # whole panel being re-rendered every _POLL_INTERVAL_SECONDS. A q-tooltip is a
    # CHILD of its anchor, so rebuilding the button destroyed the element the pointer
    # was over -- Quasar closed the tooltip and it only came back on a fresh hover. At
    # a 0.5s tick the Cancel tooltip flickered and could not be read at all. Only
    # three things actually change while a run is in flight (the progress label, the
    # progress bar, and the cancel/stopping state), so those are the only things the
    # poll touches. Mirrors ui/connect.py's update_next_state().
    def _is_stopping() -> bool:
        return bool(
            validation_state.cancel_requested
            or (
                validation_state.job_id is not None
                and job_manager.is_cancel_requested(validation_state.job_id)
            )
        )

    def _cancel() -> None:
        job_id = validation_state.job_id
        if job_id is not None:
            job_manager.request_cancel(job_id)
        validation_state.cancel_requested = True
        _sync()  # reflect it immediately, without rebuilding the panel

    _STOPPING_TIP = (
        "Cancel already requested — tables not yet started are skipped, and a long "
        "reconciliation stops within a few thousand rows. A COUNT(*) or checksum "
        "already running on a large table has no interruption point, so it finishes "
        "first (this can take minutes). Nothing is being written either way — "
        "validation is read-only."
    )
    _RUNNING_TIP = (
        "Stop the comparison. Tables not yet started are skipped; the ones already "
        "running finish first. Read-only, so nothing is left half-changed."
    )
    _STOPPING_NOTICE = (
        "Cancelling — finishing the comparisons already running",
        "Tables not yet started are skipped. A COUNT(*) or checksum already running "
        "on a large table cannot be interrupted mid-query, so it completes first — "
        "minutes on a large table, and every table running concurrently does the "
        "same. No partial report is produced, and nothing is modified: validation "
        "only reads both engines.",
    )
    _RUNNING_NOTICE = (
        "Comparison in progress — safe to leave running",
        "Exact COUNT(*)/checksum + per-table PK reconciliation on both engines — "
        "minutes for a large database. Runs in the background; read-only (the target "
        "is never modified).",
    )

    with ui.column().classes("w-full gap-2"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-3 no-wrap"):  # type: ignore[attr-defined]
            ui.spinner(size="sm")  # type: ignore[attr-defined]
            status_label = ui.label().classes("text-sm text-gray-700")  # type: ignore[attr-defined]
            # AWS/Cloudscape: cancelling a read-only run is a *normal* (secondary)
            # action, not a destructive (red) one -- red is reserved for
            # irreversible deletes. Use a calm outlined grey button.
            #
            # NB: do NOT use Quasar's button ``loading`` prop here. On an outlined
            # (transparent) button the loading spinner sits over the visible
            # border and reads as a "spinning border" artifact. The in-progress
            # cue is the disabled state + the "Stopping…" label instead; the
            # surrounding spinner above already shows the run is active.
            # The button keeps its own name while stopping (like Full Load's "Stop
            # Full Load"): the label beside it already says "Stopping… waiting for
            # …", so repeating "Stopping…" on the button said it twice and dropped
            # the only mention of WHICH action was requested.
            cancel_button = ui.button(  # type: ignore[attr-defined]
                "Cancel validation",
                icon="stop_circle",
                on_click=_cancel,
            ).props("outline color=grey-8 no-caps")
            # ONE tooltip element whose TEXT is swapped, so the anchor survives the
            # poll and the tooltip stays open while hovered.
            #
            # Build it as a CHILD element -- NOT via ``cancel_button.tooltip(...)``.
            # NiceGUI's Element.tooltip() constructs the Tooltip and then returns
            # ``self`` (the button) for chaining, so binding its result and calling
            # set_text() on it rewrote the BUTTON'S OWN LABEL: the whole tooltip
            # sentence rendered as the button caption, blowing the row out to the full
            # panel width. Entering the tooltip's context and creating ui.tooltip()
            # inside gives a handle on the tooltip element itself.
            with cancel_button:
                cancel_tip = ui.tooltip("")  # type: ignore[attr-defined]
        # Determinate progress bar once the worker reports its first table, so the
        # user sees how far along a long multi-table run is (not just a spinner).
        # Created up front and shown/hidden, since creating it later would mean
        # rebuilding this region.
        progress_bar = ui.linear_progress(  # type: ignore[attr-defined]
            value=0.0, show_value=False
        ).props("rounded color=primary").classes("w-full")
        # What this run is doing + the reassurances (background-safe, read-only,
        # cancellable) -- wrapped in an info notice so it reads as the calm "here is
        # what's happening" panel rather than loose gray text that gets skipped.
        #
        # Once a cancel is requested the panel must stop saying "in progress -- safe
        # to leave running": that is the pre-cancel reassurance, and leaving it up
        # made the requested stop look ignored. Explain the wind-down instead.
        # Built with placeholder text; _sync() fills it and swaps it on cancel. Using
        # the shared render_notice keeps the tone/border/icon from design.py.
        notice_header, notice_body = render_notice(
            ui, tone="info", header=_RUNNING_NOTICE[0], body=_RUNNING_NOTICE[1]
        )

    def _sync() -> None:
        """Push the current job state onto the existing elements (no re-render)."""
        if getattr(status_label, "is_deleted", False):
            return  # the page was rebuilt under us (e.g. the run finished)
        stopping = _is_stopping()
        progress = validation_state.progress
        status_label.set_text(
            "Stopping… waiting for the in-flight table comparisons to finish."
            if stopping
            else _in_progress_label(progress)
        )
        cancel_button.set_enabled(not stopping)
        cancel_tip.set_text(_STOPPING_TIP if stopping else _RUNNING_TIP)
        # The bar tracks tables COMPLETING, so during a wind-down it would keep
        # advancing and contradict "Cancelling" -- hide it instead.
        show_bar = progress is not None and not stopping
        progress_bar.set_visibility(show_bar)
        if show_bar:
            _table, index, total = progress
            progress_bar.set_value(min(1.0, max(0.0, (index / total) if total else 0.0)))
        header, body = _STOPPING_NOTICE if stopping else _RUNNING_NOTICE
        notice_header.set_text(header)
        notice_body.set_text(body)

    _sync()
    _install_poll_timer(
        ui, job_manager, session, validation_state, refresh, on_tick=_sync
    )


def _in_progress_label(progress: Optional[tuple[str, int, int]]) -> str:
    """Build the running-state label, naming the table being compared if known.

    Before the worker reports its first table (or after a restart-orphaned run),
    ``progress`` is ``None`` and we fall back to the generic comparing message.
    """
    if not progress:
        return "Comparing source and target…"
    table, index, total = progress
    return f"Checking table {index} of {total}: {table}"


def _render_scope_card(
    ui: object,
    scope: "ValidationScope",
    *,
    scope_tables: "list[str]",
    validation_state: "ValidationState",
    status: StepStatus,
    refresh: Callable[[], None],
) -> None:
    """Render the "Validating" context card: WHAT this run covers, with a filter.

    A Cloudscape "Container" whose body identifies source -> target, the table
    scope (count + subset/filtered note + a short chip sample), the as-of
    consistency point, and an object filter picker so the user can validate only
    specific tables within the migration scope. Migration type is intentionally not
    shown: it does not affect the source-vs-target comparison and would only
    misreport the last-chosen type. ``scope_tables`` is the full migration scope
    (the filter's option list); the filter narrows it.
    """
    # Table scope summary line (count + whether it is filtered / a migration subset).
    if scope.is_filtered:
        tables_value = f"{scope.table_count} of {scope.scope_count}"
        tables_note = "filtered below"
    elif scope.is_subset:
        tables_value = f"{scope.table_count} of {scope.total_in_inventory}"
        tables_note = "selected in Data Migration"
    else:
        tables_value = f"All {scope.table_count}"
        tables_note = ""

    with _section(ui, icon="checklist", title="Validating"):  # type: ignore[misc]
        # Key-value pairs grid (Cloudscape "Container" key-value pairs): each label
        # is a header bound to the value directly below it; the generous inter-pair
        # gap (gap-y-5) separates one pair from the next, so a header is read with
        # ITS value, not the one above/below.
        with ui.element("div").classes(  # type: ignore[attr-defined]
            "grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-5 w-full"
        ):
            _kv(ui, "As-of (consistency point)", scope.as_of)
            _kv(ui, scope.source_label, scope.source_detail, mono=True)
            _kv(ui, scope.target_label, scope.target_detail, mono=True)

        # Tables: just the COUNT summary here (no chip sample). The full, clickable
        # object list lives only in "Objects to validate" below -- duplicating the
        # names here (truncated to "+N more") was confusing and the truncated ones
        # could not be clicked. One place to see/select objects, no overlap.
        with ui.column().classes(  # type: ignore[attr-defined]
            "gap-1 min-w-0 pl-2 border-l-2 border-gray-200 w-full"
        ):
            _kv_label(ui, "Tables")
            with ui.row().classes("items-baseline gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.label(tables_value).classes(  # type: ignore[attr-defined]
                    "text-sm text-gray-900 leading-snug"
                )
                if tables_note:
                    ui.label(f"· {tables_note}").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-500"
                    )

        # Divider, then the interactive object filter — the single place objects are
        # listed and selected (every object as a clickable chip, none truncated).
        ui.separator().classes("my-1")  # type: ignore[attr-defined]
        _render_object_filter(ui, scope_tables, validation_state, status, refresh)


def _render_object_filter(
    ui: object,
    scope_tables: "list[str]",
    validation_state: "ValidationState",
    status: StepStatus,
    refresh: Callable[[], None],
) -> None:
    """Render the object filter as clickable, schema-colored object chips.

    Every in-scope object starts INCLUDED (chip ON, filled in its schema color);
    clicking a chip toggles it OFF (excluded, quiet gray) or back ON, so the
    on-screen ON/OFF state always matches exactly what will be validated -- no
    confusing "everything is validated but every chip looks off" inversion, and
    re-including an object is just another click. Exclusions are stored in
    ``validation_state.table_exclude`` (empty == validate all). Chips are grouped
    and colored by schema; disabled while a run is in flight. Uses ``ui.button``
    (not a label) so the click reliably toggles in both directions.
    """
    disabled = status is StepStatus.IN_PROGRESS
    in_scope = set(scope_tables)
    # Keep exclusions consistent with the current scope (drop names no longer in
    # scope, e.g. after the migration selection changed).
    excluded = validation_state.table_exclude & in_scope
    included_count = len(in_scope) - len(excluded)
    groups = group_objects_by_schema(scope_tables)

    def _toggle(name: str) -> Callable[[], None]:
        def _handler() -> None:
            if disabled:
                return
            current = validation_state.table_exclude & in_scope
            if name in current:
                current.discard(name)  # re-include
            else:
                current.add(name)  # exclude
            validation_state.table_exclude = current
            refresh()

        return _handler

    def _set_excluded(value: set[str]) -> Callable[[], None]:
        def _handler() -> None:
            if disabled:
                return
            validation_state.table_exclude = value
            refresh()

        return _handler

    with ui.column().classes("gap-2 w-full"):  # type: ignore[attr-defined]
        # Header row: label + Include all / Exclude all shortcuts + live count.
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            _kv_label(ui, "Objects to validate")
            ui.space()  # type: ignore[attr-defined]
            # Same props as the identical shortcuts on the other object pickers
            # (Schema Conversion's and Data Migration's "Select all"/"Unselect all"):
            # the affirmative action is primary + done_all, the clearing one grey-7 +
            # remove_done. These carried only "flat dense no-caps size=sm", so beside
            # those screens they lost both the color and the icon and read as a
            # different app.
            ui.button(  # type: ignore[attr-defined]
                "Include all", on_click=_set_excluded(set())
            ).props(
                "flat dense no-caps size=sm color=primary icon=done_all"
            ).set_enabled(not disabled and bool(excluded))
            ui.button(  # type: ignore[attr-defined]
                "Exclude all", on_click=_set_excluded(set(in_scope))
            ).props(
                "flat dense no-caps size=sm color=grey-7 icon=remove_done"
            ).set_enabled(not disabled and included_count > 0)

        # One chip row per schema. A chip is ON (included) unless excluded; clicking
        # toggles it. Schema heading shown only for a multi-schema scope.
        multi_schema = len(groups) > 1
        for schema, names in groups:
            with ui.column().classes("gap-1 w-full"):  # type: ignore[attr-defined]
                if multi_schema:
                    ui.label(schema).classes(  # type: ignore[attr-defined]
                        "text-[11px] font-mono " + chip_group_text_class(schema)
                    )
                color = chip_group_quasar_color(schema)
                with ui.row().classes("items-center gap-1.5 flex-wrap"):  # type: ignore[attr-defined]
                    for name in names:
                        _schema, obj = _split_schema(name)
                        is_on = name not in excluded
                        # Color comes from the Quasar `color` prop (reliable on a
                        # q-btn) -- filled when included, outline when excluded -- so
                        # both states always show the label (an excluded chip is NOT
                        # a blank box) and the schema color is kept. An icon also
                        # signals the state for accessibility / color-blind users.
                        chip_props = (
                            f"dense no-caps size=sm color={color} "
                            f"icon={'check' if is_on else 'add'}"
                            + ("" if is_on else " outline")
                        )
                        chip = ui.button(  # type: ignore[attr-defined]
                            obj, on_click=_toggle(name)
                        ).props(chip_props).classes("font-mono normal-case")
                        chip.tooltip(  # type: ignore[attr-defined]
                            f"{name} — {'included' if is_on else 'excluded'} "
                            "(click to toggle)"
                        )
                        if disabled:
                            chip.set_enabled(False)  # type: ignore[attr-defined]

        # Hint: spell out exactly what will be validated + the all-by-default rule.
        total = len(scope_tables)
        if not excluded:
            hint = f"All {total} object(s) will be validated. Click an object to exclude it."
        elif disabled:
            hint = (
                f"{included_count} of {total} object(s) selected — applies to the "
                "next run."
            )
        else:
            hint = (
                f"Validating {included_count} of {total} object(s) "
                f"({len(excluded)} excluded). Click an excluded object to re-include; "
                "Re-run to apply."
            )
        inline_hint(ui, hint, tone="neutral")


def _kv_label(ui: object, label: str) -> None:
    """Render a Cloudscape key-value-pair LABEL: a clear, anchoring header.

    Semibold and gray-700 (not a faint gray-500) so the label reads as the HEADER
    that owns the value below it -- the header↔value bond is what makes the pair
    scannable. Uppercase + tracking keeps it compact and distinct from the value.
    """
    ui.label(label).classes(  # type: ignore[attr-defined]
        "text-[11px] font-bold uppercase tracking-wider text-gray-700"
    )


def _kv(ui: object, label: str, value: str, *, mono: bool = False) -> None:
    """Render one Cloudscape key-value pair: a header label tightly bound to its value.

    The label sits directly on its value (no gap) behind a thin accent rule, so a
    header and its value read as ONE unit; the grid's larger inter-pair gap then
    separates pairs from each other. This makes "which header owns which value"
    unambiguous -- the relationship the layout is built around.
    """
    value_classes = "text-sm text-gray-900 break-all leading-snug"
    if mono:
        value_classes += " font-mono text-[13px]"
    # Left accent rule visually ties the stacked label+value into one pair.
    with ui.column().classes(  # type: ignore[attr-defined]
        "gap-0 min-w-0 pl-2 border-l-2 border-gray-200"
    ):
        _kv_label(ui, label)
        ui.label(value).classes(value_classes)  # type: ignore[attr-defined]


# Comparison-mode tiles: (mode, icon, title, description). The order is the tile
# order; descriptions say what each mode actually compares.
_MODE_TILES: tuple[tuple[ValidationMode, str, str, str], ...] = (
    (
        ValidationMode.ROW_COUNT,
        "tag",
        "Row count",
        "Compare COUNT(*) per table. Fastest; confirms the row totals match.",
    ),
    (
        ValidationMode.CHECKSUM,
        "fingerprint",
        "Row count + checksum",
        "Also compares an order-independent per-row checksum, so a match means "
        "the data itself is equal — not just the totals.",
    ),
)


def _render_mode_tiles(
    ui: object,
    validation_state: ValidationState,
    *,
    disabled: bool,
    refresh: Optional[Callable[[], None]] = None,
) -> None:
    """Render the comparison mode as AWS Cloudscape selectable radio tiles.

    Each mode is a bordered card (radio + icon + title + description); the selected
    tile gets the primary border + tint, mirroring the Data Migration migration-type
    selector so the two choices look and feel identical across the journey. Disabled
    (muted, non-interactive) while a run is in flight.
    """
    selected = validation_state.mode

    def _select(mode: ValidationMode) -> Callable[[], None]:
        def _handler() -> None:
            if disabled or mode is validation_state.mode:
                return
            validation_state.mode = mode
            if refresh is not None:
                refresh()  # re-render so the chosen tile highlights

        return _handler

    with ui.column().classes("gap-1 w-full"):  # type: ignore[attr-defined]
        _kv_label(ui, "Comparison mode")
        with ui.row().classes("w-full gap-3 items-stretch no-wrap"):  # type: ignore[attr-defined]
            for mode, icon, title, desc in _MODE_TILES:
                is_selected = mode is selected
                border = "border-blue-500" if is_selected else "border-gray-300"
                bg = "bg-blue-50" if is_selected else "bg-white"
                interactivity = (
                    "opacity-60 cursor-not-allowed"
                    if disabled
                    else "cursor-pointer hover:border-blue-400"
                )
                tile = ui.card().classes(  # type: ignore[attr-defined]
                    f"flex-1 p-3 rounded-lg border {border} {bg} {interactivity} "
                    "transition-colors gap-1"
                )
                tile.on("click", _select(mode))  # type: ignore[attr-defined]
                with tile:
                    with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                        ui.icon(  # type: ignore[attr-defined]
                            "radio_button_checked"
                            if is_selected
                            else "radio_button_unchecked",
                            color="primary" if is_selected else "grey-6",
                        ).classes("text-lg")
                        ui.icon(  # type: ignore[attr-defined]
                            icon, color="primary" if is_selected else "grey-7"
                        ).classes("text-lg")
                        ui.label(title).classes("text-sm font-semibold")  # type: ignore[attr-defined]
                    ui.label(desc).classes("text-xs text-gray-600")  # type: ignore[attr-defined]


def _render_options(
    ui: object,
    validation_state: ValidationState,
    status: StepStatus,
    *,
    refresh: Optional[Callable[[], None]] = None,
) -> None:
    """Render the validation mode select, reconcile, and orphan-check switches."""
    disabled = status is StepStatus.IN_PROGRESS
    # Comparison mode as AWS Cloudscape "tiles" (selectable radio cards), matching
    # the Data Migration migration-type selector so the journey stays consistent.
    _render_mode_tiles(ui, validation_state, disabled=disabled, refresh=refresh)
    with ui.row().classes("items-center gap-4 flex-wrap"):  # type: ignore[attr-defined]
        ui.switch(  # type: ignore[attr-defined]
            "Reconcile every record (find missing / extra rows)",
            value=validation_state.reconcile,
            on_change=lambda e: setattr(
                validation_state, "reconcile", bool(e.value)
            ),
        ).tooltip(  # type: ignore[attr-defined]
            "Streams and compares every primary key on both sides to find the "
            "exact rows missing on the target (lost / not-yet-replicated) or extra "
            "on the target (a delete CDC has not applied). Single-column integer "
            "keys only; other tables fall back to count/checksum."
        )
        ui.switch(  # type: ignore[attr-defined]
            "Check for orphan records",
            value=validation_state.check_orphans,
            on_change=lambda e: setattr(
                validation_state, "check_orphans", bool(e.value)
            ),
        )
        ui.switch(  # type: ignore[attr-defined]
            "Fast sweep (deep-check only tables whose counts differ)",
            value=validation_state.deep_only_on_count_mismatch,
            on_change=lambda e: setattr(
                validation_state, "deep_only_on_count_mismatch", bool(e.value)
            ),
        ).tooltip(  # type: ignore[attr-defined]
            "Speeds up a large run: compares row counts for every table but runs "
            "the expensive checksum / full primary-key reconciliation only for "
            "tables whose counts disagree. A count-matched table is reported as "
            "verified by row count (deep checks not run) — never a false match."
        )
    # While a run is IN FLIGHT the toggles are greyed out; say why they're inert
    # (they'll apply to the next run). When a report is already on screen no caption
    # is shown: the options block is plainly a pre-run config area and Re-run sits
    # top-right, so a "changing options applies on the next run" line was just noise.
    if disabled:
        ui.label(  # type: ignore[attr-defined]
            "Options apply to the next run."
        ).classes("text-xs text-gray-400")


def _install_poll_timer(
    ui: object,
    job_manager: JobManager,
    session: object,
    validation_state: ValidationState,
    refresh: Callable[[], None],
    on_tick: Optional[Callable[[], None]] = None,
) -> None:
    """Poll the running validation job once and re-arm via the next render.

    ``on_tick`` (when given) updates the live panel IN PLACE while the job is still
    running, and this function re-arms its own timer -- so the region, and any tooltip
    hovered inside it, is never destroyed mid-run. Without it the poll falls back to
    ``refresh()`` (a full re-render) as before. A terminal status always re-renders,
    since the whole screen changes to the result view.

    Uses a ONE-SHOT timer (``once=True``): each IN_PROGRESS render installs a
    single timer that fires once. While the job is still running the timer calls
    ``refresh()``, which re-renders the in-progress state and installs the next
    one-shot timer -- so polling stays alive without accumulating timers (a
    repeating timer would re-install another repeating timer on every refresh and
    multiply). When the job reaches a terminal state the status is updated, the
    next render shows the result/cancelled state and installs no new timer.
    """
    job_id = validation_state.job_id
    if job_id is None:
        return

    def poll() -> None:
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(job.status)
        if mapped is None:
            # STILL RUNNING. Update the existing elements in place and re-arm, rather
            # than calling refresh(): a full re-render recreates the Cancel button, and
            # a q-tooltip is a child of its anchor, so the tooltip the user is hovering
            # is destroyed on every tick (0.5s -> unreadable flicker). Falls back to
            # refresh() when no in-place updater was supplied.
            if on_tick is None:
                refresh()
                return
            on_tick()
            ui.timer(_POLL_INTERVAL_SECONDS, poll, once=True)  # type: ignore[attr-defined]
            return
        if mapped is StepStatus.FAILED:
            validation_state.set_error(
                job_manager.get_error(job_id) or "Validation failed."
            )
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(
                session.workflow,  # type: ignore[attr-defined]
                WorkflowStep.VALIDATION,
                mapped,
            )
        )
        refresh()

    ui.timer(_POLL_INTERVAL_SECONDS, poll, once=True)  # type: ignore[attr-defined]


def _render_result(
    ui: object,
    report: ValidationReport,
    *,
    diagnose_provider=None,
    restored: bool = False,
    completed_at: "Optional[datetime]" = None,
    recheck_provider=None,
    rechecking_tables: "Sequence[str]" = (),
    rechecked_tables: "Sequence[str]" = (),
    rechecked_at: "Optional[datetime]" = None,
    cdc_in_use: bool = False,
    identity_sync: "Optional[dict[str, int]]" = None,
) -> None:
    """Render the cut-over readiness report: verdict, checks, then the details.

    ``diagnose_provider`` (when given -- AI Assist on) opens the AI chat drawer
    for a mismatch; it is threaded to the verdict (run-level "Diagnose with AI")
    and to the failing-tables section (per-table "Explain with AI").
    ``restored`` shows a "restored from a saved session" note (the result was
    re-hydrated on reconnect, not run now), with its ``completed_at`` time, so a
    stale verdict prompts a re-validate. The go-path "how to cut over" runbook
    lives on the dedicated Cut over step, not in this result.

    ``recheck_provider`` (when given) starts a per-table re-check for the table
    names it is called with; it is withheld while any comparison is in flight, and
    the failing-tables section then shows the tables in ``rechecking_tables`` as
    busy instead. ``rechecked_tables``/``rechecked_at`` describe which rows in this
    report are NEWER than the rest of the run, so the mixed as-of is stated
    plainly rather than left for the reader to infer.
    """
    # Restored-from-snapshot banner FIRST, so the user knows this verdict is as-of
    # a past run (the source may have changed since) before reading it.
    if restored:
        when = (
            completed_at.strftime("%Y-%m-%d %H:%M UTC")
            if completed_at is not None
            else "a previous session"
        )
        render_notice(
            ui,
            tone="info",
            header="Restored from your last session",
            body=(
                f"This is the validation result from {when}, reloaded after a "
                "reconnect — it was not just re-run. If the source has changed "
                "since then, click Re-run validation for a current verdict."
            ),
        )
    summary = summarize_validation(report)
    drift = format_drift(report)
    _render_verdict(ui, summary, drift)
    # Mixed as-of honesty: when part of this report came from a later per-table
    # re-check, say which tables and when, right under the verdict -- the verdict
    # above is computed over BOTH vintages, so the reader must know that before
    # acting on it. Info tone: a re-check is a normal, expected action.
    _render_recheck_note(ui, rechecked_tables, rechecked_at)
    # Identity-sequence re-sync: when this run advanced one or more GENERATED-identity
    # keys past the current target MAX(pk), say so. This is the fix for the CDC gap --
    # Full Load's own sync can't see rows CDC inserted afterwards -- so surfacing it
    # here (right under the verdict, before cut-over) makes the repair visible instead
    # of silent. Only shown when something was actually advanced.
    if identity_sync:
        detail = ", ".join(
            f"{name} → {value}" for name, value in sorted(identity_sync.items())
        )
        render_notice(
            ui,
            tone="info",
            header="Identity sequences advanced for cut-over",
            body=(
                f"{len(identity_sync)} identity primary key(s) had their sequence "
                "advanced past the current target rows, so the application's first "
                "insert after cut-over will not collide with a migrated id (Full Load "
                "and CDC insert explicit ids, which do not move a GENERATED BY DEFAULT "
                f"sequence). New RESTART WITH: {detail}."
            ),
        )
    # On a no-go, the recovery path is its own prominent section right under the
    # verdict (how to fix it + ordered steps + the AI diagnosis action). The go
    # path's "how to actually cut over" runbook lives on the dedicated Cut over
    # step (step 6), reached via the Next button — it is not duplicated here.
    # Section order follows the reader's questions: the verdict (above) answers "can I
    # cut over?"; on a no-go the recovery path answers "what do I do now?" right under
    # it (the two are a pair); THEN the evidence answers "why?" -- the failing tables,
    # the full per-table comparison, orphans, and source drift. "Cut-over readiness" is
    # a checklist SUMMARISED from that evidence, so it now sits AFTER it (just before
    # Export), not above it: leading with a summary of numbers the reader has not seen
    # yet -- and which the top verdict already condenses -- made the recovery advice
    # arrive before its own justification.
    if not summary.ready_for_cutover:
        _render_recovery_section(
            ui, summary, drift, diagnose_provider=diagnose_provider
        )
    _render_failing_tables(
        ui, report, summary, drift,
        diagnose_provider=diagnose_provider,
        recheck_provider=recheck_provider,
        rechecking_tables=rechecking_tables,
    )
    with _section(ui, icon="table_view", title="Per-table results"):
        _render_tables(
            ui, report,
            recheck_provider=recheck_provider,
            rechecking_tables=rechecking_tables,
        )
    _render_orphans(ui, report)
    # "Drift since snapshot" was jargon: "drift" is a replication term and "snapshot"
    # is the tool's internal name for the watermark. The section answers "did the
    # source keep changing while/after we compared?", so it is titled that way.
    with _section(ui, icon="schedule", title="Source changes since the comparison"):
        _render_drift(ui, drift, cdc_in_use=cdc_in_use)
    # The readiness checklist is a roll-up of everything above (row-count parity,
    # orphans, drift), so it reads as a final tally right before Export rather than a
    # preamble before the evidence exists on screen.
    with _section(ui, icon="fact_check", title="Cut-over readiness"):
        _render_readiness_checks(ui, summary, drift)
    with _section(ui, icon="download", title="Export report"):
        _render_downloads(ui, report)


class _Section:
    """Context manager: a titled section card, identical to every other page.

    The app-wide section container is a plain ``ui.card().classes("w-full")`` with
    a :func:`section_header` at the top and the body content directly inside (see
    e.g. Evaluation's "Migration readiness" card). Validation used to draw its own
    bordered header-band variant, which read as a different page; this now produces
    the exact same DOM/spacing as the other steps so Validation is visually unified.
    """

    def __init__(self, ui: object, *, icon: str, title: str) -> None:
        self._ui = ui
        self._icon = icon
        self._title = title
        self._card = None

    def __enter__(self):
        ui = self._ui
        self._card = ui.card().classes("w-full")  # type: ignore[attr-defined]
        self._card.__enter__()
        # Same header as every other page: shared section_header at the top of a
        # default card; the caller's content lands directly in the card with the
        # card's standard padding/gap.
        section_header(ui, icon=self._icon, title=self._title)
        return self

    def __exit__(self, *exc: object) -> None:
        self._card.__exit__(*exc)  # type: ignore[attr-defined]


def _section(ui: object, *, icon: str, title: str) -> _Section:
    """Open a titled section container (see :class:`_Section`)."""
    return _Section(ui, icon=icon, title=title)


def _render_verdict(
    ui: object, summary: ValidationSummary, drift: DriftDisplay,
) -> None:
    """Render the overall go/no-go cut-over verdict as a Cloudscape notice (hero).

    The verdict body also surfaces the two qualifiers that the strict
    ``ready_for_cutover`` flag does not capture on its own: record-level
    reconciliation being off (so "ready" is count/checksum-only), and the source
    having advanced since the snapshot (fine under live CDC, but a re-verify
    signal otherwise). The recovery guidance on a no-go is a separate section
    (:func:`_render_recovery_section`), not part of the verdict.
    """
    if summary.ready_for_cutover:
        body = (
            f"All {summary.total_tables} table(s) match and no issues were found. "
            f"Source and target are consistent as-of {summary.as_of}."
        )
        caveats: list[str] = []
        if not summary.reconcile_performed:
            caveats.append(
                "record-level reconciliation was off, so this is a "
                "count/checksum match only"
            )
        if drift.available and drift.determinable and drift.drifted:
            caveats.append(
                "the source has advanced since the snapshot — expected under live "
                "CDC, but re-verify before cut-over if CDC is not running"
            )
        if caveats:
            body += " Note: " + "; ".join(caveats) + "."
        render_notice(ui, tone="success", header="Ready for cut-over", body=body)
        return

    # When EVERY non-matching table is short by exactly the rows the migration already
    # reported dropping, "N of M did not pass — review the failing checks" sends the
    # reviewer hunting for a defect that was already found, reported and (in the Full Load
    # step) explicitly accepted. Say what is actually outstanding instead. Still not a
    # "Ready" verdict: rows really are absent from the target, so this stays a hold that
    # asks for a decision rather than an investigation.
    explained_tables = summary.quarantine_explained_tables
    if explained_tables and summary.unexplained_mismatched_tables == 0:
        rows = summary.quarantine_explained_rows
        noun = "table" if len(explained_tables) == 1 else "tables"
        row_noun = "row" if rows == 1 else "rows"
        render_notice(
            ui,
            tone="warning",
            # NOT "blocked": the tool classifies this exact state as "acceptable" --
            # cut-over can proceed once the gap is acknowledged (cutover_release_state).
            # "blocked" is a red-tier, full-stop word reserved for unexplained/errored
            # mismatches, so using it here contradicted the tool's own gate and read as
            # more severe than the actual "Not ready" red verdict. The header now names
            # the DECISION, and matches the Cut over step's "Every difference is
            # explained" wording so the two screens read as the same situation.
            header="Every difference is explained — accept the gap or fix the source and reload",
            body=(
                f"Nothing unexplained: every difference is in {len(explained_tables)} "
                f"{noun} ({', '.join(explained_tables)}) and is exactly the {rows} "
                f"{row_noun} the migration could not store — already reported, not new "
                "data loss. Two paths: reduce the offending source value(s) below the "
                "limit first and then reload those tables to reach a full match — "
                "reloading alone will not help, since DSQL still cannot store the "
                "original values — or accept the gap deliberately and cut over knowing "
                f"those {row_noun} will be absent from the target."
            ),
        )
        return

    render_notice(
        ui,
        tone="error",
        header="Not ready for cut-over",
        body=(
            f"{summary.mismatched_tables} of {summary.total_tables} table(s) "
            "did not pass. Review the failing checks and tables below before "
            "switching over."
            + (
                f" ({len(explained_tables)} of those is short by exactly the rows "
                "dropped during the migration — already reported.)"
                if explained_tables
                else ""
            )
        ),
    )


# How many re-checked table names to spell out in the mixed-as-of note before
# collapsing to a count (keeps the notice one readable line).
_RECHECK_NOTE_SAMPLE = 6


def _render_recheck_note(
    ui: object,
    rechecked_tables: "Sequence[str]",
    rechecked_at: "Optional[datetime]",
) -> None:
    """Note which tables in this report were re-checked later than the rest.

    A merged report holds two vintages: most rows are from the full run, the
    re-checked ones were measured just now. The verdict is computed over both, so
    the mix is stated explicitly instead of leaving a reader to assume one as-of.
    Renders nothing when no table was re-checked (the ordinary single-run case).
    """
    names = [n for n in rechecked_tables if n]
    if not names:
        return
    shown = ", ".join(names[:_RECHECK_NOTE_SAMPLE])
    overflow = len(names) - min(len(names), _RECHECK_NOTE_SAMPLE)
    listed = f"{shown} and {overflow} more" if overflow else shown
    when = (
        rechecked_at.strftime("%Y-%m-%d %H:%M UTC")
        if rechecked_at is not None
        else "just now"
    )
    render_notice(
        ui,
        tone="info",
        header=(
            f"{len(names)} table(s) re-checked at {when} — newer than the rest "
            "of this run"
        ),
        body=(
            f"Re-checked: {listed}. Those rows were compared just now with the same "
            "options as the original run; every other table's result is from that "
            "earlier run, and the verdict above covers both. For a single "
            "consistency point across all tables, re-run the full validation."
        ),
    )


# How many missing/extra example PKs to summarize into the AI facts (range only,
# never the full list -- Property 7: a bounded, non-enumerated hint).
_FACTS_PK_SAMPLE = 5


def _render_recovery_section(
    ui: object, summary: ValidationSummary, drift: DriftDisplay, *,
    diagnose_provider=None,
) -> None:
    """Render the mismatch-recovery guidance as its OWN titled section card.

    Grouping it in a Cloudscape "Container" (the same `_section` every other block
    uses) makes the recovery path a first-class, scannable unit rather than loose
    notices under the verdict: a one-line why, the ordered click-path (Stop CDC
    FIRST → re-run Full Load → resume CDC → re-validate), the quiesce-source
    caveat, and an optional "Diagnose with AI" action.
    """
    # Two genuinely different recoveries share this "no-go" gate, and conflating them
    # is the defect. When the WHOLE shortfall is rows the migration already reported
    # dropping (fully_explained -> release state "acceptable"), those rows hit a
    # PERMANENT limit -- e.g. a value over DSQL's ~1 MiB cap -- so re-running Full Load
    # just re-quarantines them: "backfill by re-running the migration, the Full Load
    # only fills missing rows" is false for them, and it contradicts the verdict banner
    # right above ("accept the gap or fix the source and reload"). The idempotent-reload
    # runbook is only correct for an UNEXPLAINED mismatch (rows that CAN load but did
    # not). So branch on fully_explained.
    fully_explained = bool(
        summary.quarantine_explained_tables
        and summary.unexplained_mismatched_tables == 0
    )
    # Title + icon follow the same branch as the content. "How to recover" with a wrench
    # (build) frames the state as something to REPAIR -- correct for an unexplained,
    # loadable gap, but wrong for a fully-explained one, where the rows can't be stored
    # at all and "accept the gap and cut over" is a legitimate final choice. Naming that
    # "recover" fought the verdict banner and this section's own body ("shrink the value
    # or accept the gap"). For the fully-explained case reuse the Cut over step's exact
    # heading/icon ("Acknowledge the known gap" / fact_check), the same alignment
    # v0.1.256 made for the banner header, so the two screens speak with one voice.
    _title = "Acknowledge the known gap" if fully_explained else "How to recover"
    _icon = "fact_check" if fully_explained else "build"
    with _section(ui, icon=_icon, title=_title):  # type: ignore[misc]
        if fully_explained:
            render_notice(
                ui,
                tone="info",
                header="These rows can't be stored as-is — shrink the value or accept the gap",
                body=(
                    "Every missing row was isolated because its value exceeds a "
                    "permanent Aurora DSQL limit (e.g. the ~1 MiB per-value cap), so "
                    "re-running Full Load alone just isolates them again — a plain "
                    "reload cannot bring them in. Two real paths: reduce the offending "
                    "source value(s) below the limit first — for example move a large "
                    "object out to Amazon S3 and store a reference — then reload those "
                    "tables; or accept the gap deliberately and cut over knowing those "
                    "rows will be absent from the target."
                ),
            )
        else:
            render_notice(
                ui,
                tone="info",
                header="Re-run Full Load + CDC to backfill the gap",
                body=(
                    "These differences do not shrink over time, so they are a standing "
                    "gap (rows CDC won't re-deliver), not lag. Backfill them by "
                    "re-running the migration. The Full Load is idempotent "
                    "(INSERT ... ON CONFLICT) — it only fills missing rows and never "
                    "creates duplicates."
                ),
            )
            # The exact, ordered click-path. STOP CDC FIRST is the critical step (a
            # live CDC sink + a fresh Full Load collide; CDC resumes from the old
            # watermark). Only shown for an unexplained gap -- the reload is what fixes
            # THAT case; for permanently-quarantined rows it would not help.
            steps = (
                ("1", "Go to the Data Migration step (left nav)."),
                ("2", "Open the CDC sub-step and click Stop CDC — do this FIRST. "
                 "Re-running Full Load while CDC is live collides with the stream and "
                 "leaves a gap/overlap (CDC would resume from the old snapshot point)."),
                ("3", "In the Full Load sub-step, click Start/Re-run to backfill the "
                 "missing rows (safe and duplicate-free)."),
                ("4", "When it finishes, click Continue to CDC and start CDC again — it "
                 "resumes gaplessly from the new snapshot."),
                ("5", "Come back here and click Re-run validation to confirm a clean "
                 "match."),
            )
            ui.label("Steps to recover").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-900 mt-1"
            )
            with ui.column().classes("w-full gap-1.5"):  # type: ignore[attr-defined]
                for num, text in steps:
                    with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                        ui.label(num).classes(  # type: ignore[attr-defined]
                            "shrink-0 w-5 h-5 rounded-full bg-blue-100 text-blue-700 "
                            "text-[11px] font-semibold flex items-center justify-center"
                        )
                        ui.label(text).classes("text-xs text-gray-700 leading-snug")  # type: ignore[attr-defined]

        # No quiesce/freeze caveat here: whether the source has drifted since the
        # snapshot -- and what to do about it -- is the whole subject of the dedicated
        # "Source changes since the comparison" section below, which already tells the
        # reader to freeze source writes / let CDC drain and re-validate before cut-over.
        # Repeating it in this gap-recovery card was a cross-section duplicate; the drift
        # section is where that guidance belongs.
        if diagnose_provider is not None:
            with ui.row().classes("w-full mt-1"):  # type: ignore[attr-defined]
                ui.button(  # type: ignore[attr-defined]
                    "Diagnose with AI",
                    icon="auto_awesome",
                    on_click=lambda: diagnose_provider(
                        title="AI mismatch diagnosis",
                        subtitle="Whole validation run",
                        first_question=(
                            "Diagnose these mismatches — is this replication lag, a "
                            "standing gap, or extra rows, and exactly how do I "
                            "reconcile to a clean cut-over?"
                        ),
                        facts=_validation_run_facts(summary, drift),
                        scope="run",
                    ),
                ).props("color=primary outline no-caps")


def _render_cutover_section(
    ui: object, summary: ValidationSummary, drift: DriftDisplay, *,
    cdc_in_use: bool = False,
    identity_sync_provider: "Optional[Callable[[], None]]" = None,
    identity_sync_result: "Optional[dict[str, int]]" = None,
    identity_sync_failed: "Optional[dict[str, str]]" = None,
) -> None:
    """Render the go-path cut-over runbook as its OWN titled section card.

    The verdict says the target is consistent; this tells the user how to actually
    finish — repoint the application to DSQL — which is the one step the tool does
    not do for them. Mirrors :func:`_render_recovery_section` (same `_section`
    container, info notice, numbered steps, closing caveat) so go and no-go feel
    symmetric. The steps branch on ``cdc_in_use``: a CDC path drains the stream to
    zero lag and tears the cdc-stack down afterwards; a Full-Load-only path just
    needs a brief source write-freeze. Rollback is called out either way, because
    once the application writes to DSQL those rows live only on DSQL (this tool
    replicates MySQL -> DSQL, not the reverse).

    ``identity_sync_provider`` (when given) wires an explicit "Sync identity
    sequences" button, shown just before the repoint step: it advances identity keys
    past the current target MAX(pk) so the app's first insert after cut-over cannot
    collide. It is a deliberate operator action (a target write), never a render
    side-effect. ``identity_sync_result`` is the last outcome to display ({} == ran,
    nothing to advance; a dict names the advanced sequences; None == not run yet).
    """
    with _section(ui, icon="rocket_launch", title="How to cut over"):  # type: ignore[misc]
        render_notice(
            ui,
            tone="success",
            header="Validation passed — you're ready to switch your app to DSQL",
            body=(
                "Cut-over is the moment your application stops writing to MySQL and "
                "starts using Aurora DSQL. The tool has proven the target is "
                "consistent; repointing the application is the final operational "
                "step, and only you can do it. Follow the runbook below for a safe "
                "switch with a rollback path."
            ),
        )
        if cdc_in_use:
            steps = (
                ("1", "Let CDC catch up: on the Data Migration step, watch the CDC "
                 "status until replication lag is at (or near) zero — DSQL is "
                 "tracking the source's live writes."),
                ("2", "Freeze writes on the source: briefly put your application in "
                 "read-only / maintenance mode so MySQL stops taking new writes."),
                ("3", "Wait for the final drain: let CDC apply the last in-flight "
                 "change events until lag is zero again. MySQL and DSQL now hold "
                 "the same rows."),
                ("4", "Re-run validation here for the final go/no-go — do this AFTER "
                 "the drain (step 3). Cut over only on a clean MATCH (or differences "
                 "you can fully explain). This final run also advances any identity "
                 "(AUTO_INCREMENT) sequence past the rows CDC delivered, so the app's "
                 "first insert after cut-over cannot collide with a migrated id."),
                ("5", "Repoint your application to the Aurora DSQL endpoint "
                 "(PostgreSQL wire, IAM-token auth — no password) and smoke-test "
                 "the critical read/write paths."),
                ("6", "Once you're confident, tear the CDC pipeline down: click "
                 "Start over (top right) and choose \"Delete all CDC "
                 "infrastructure\". Do this LAST — it ends replication. It stops "
                 "MSK / MSK Connect / NAT cost AND clears the old stack, which a "
                 "future fresh Full Load or CDC needs removed before it can deploy. "
                 "(If that option isn't shown, CDC is already torn down — nothing "
                 "left to remove.)"),
            )
        else:
            steps = (
                ("1", "Freeze writes on the source: put your application in "
                 "read-only / maintenance mode so no new rows are written to "
                 "MySQL while you switch."),
                ("2", "If the source took writes since the snapshot, re-run Full "
                 "Load once more (it's idempotent — only fills the unfinished "
                 "work, never duplicates), then re-run validation here."),
                ("3", "Confirm a clean MATCH on this screen — that's the go/no-go "
                 "gate."),
                ("4", "Repoint your application to the Aurora DSQL endpoint "
                 "(PostgreSQL wire, IAM-token auth — no password) and smoke-test "
                 "the critical read/write paths."),
                ("5", "Lift the freeze — your application is now live on DSQL."),
            )
        steps_heading = (
            "Steps to cut over (with CDC)"
            if cdc_in_use
            else "Steps to cut over (Full Load)"
        )
        ui.label(steps_heading).classes(  # type: ignore[attr-defined]
            "text-base font-semibold text-gray-900 mt-1"
        )
        with ui.column().classes("w-full gap-2"):  # type: ignore[attr-defined]
            for num, text in steps:
                with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    ui.label(num).classes(  # type: ignore[attr-defined]
                        "shrink-0 w-6 h-6 rounded-full bg-green-100 text-green-700 "
                        "text-xs font-semibold flex items-center justify-center"
                    )
                    ui.label(text).classes("text-sm text-gray-700 leading-snug")  # type: ignore[attr-defined]

        # Explicit identity-sequence sync: an OPERATOR ACTION, not a render side-effect.
        # Identity (AUTO_INCREMENT) keys are loaded/replicated with explicit ids, which do
        # not advance a GENERATED BY DEFAULT sequence, so before repointing the app the
        # sequence must be moved past the current target MAX(pk) or the app's first insert
        # collides (23505). The button does that on demand (idempotent, safe to re-click);
        # do it after the final drain (CDC) / reload (Full Load) and before repointing.
        if identity_sync_provider is not None:
            with ui.column().classes(  # type: ignore[attr-defined]
                "w-full gap-1 mt-1 pt-2 border-t border-gray-100"
            ):
                ui.label(  # type: ignore[attr-defined]
                    "Before you repoint: sync identity sequences"
                ).classes("text-sm font-semibold text-gray-900")
                ui.label(  # type: ignore[attr-defined]
                    "If any table uses a server-generated (AUTO_INCREMENT / IDENTITY) "
                    "key, advance its sequence past the migrated rows so your "
                    "application's first insert after cut-over does not hit a duplicate "
                    "key. Run this after the final drain/reload and before repointing. "
                    "Safe to run more than once."
                ).classes("text-xs text-gray-600 leading-snug")
                with ui.row().classes("items-center gap-3 mt-1"):  # type: ignore[attr-defined]
                    ui.button(  # type: ignore[attr-defined]
                        "Sync identity sequences",
                        icon="sync",
                        on_click=lambda: identity_sync_provider(),
                    ).props("color=primary outline no-caps")
                    if identity_sync_result is not None:
                        if identity_sync_result:
                            advanced = ", ".join(
                                f"{name} → {value}"
                                for name, value in sorted(identity_sync_result.items())
                            )
                            inline_hint(
                                ui,
                                f"Advanced {len(identity_sync_result)} sequence(s): "
                                f"{advanced}.",
                                tone="success",
                            )
                        elif not identity_sync_failed:
                            # Genuinely nothing to advance (no server-generated keys, or
                            # all already correct) AND nothing failed -> the reassuring
                            # success line. Only reachable when the sync had no failures.
                            inline_hint(
                                ui,
                                "Done — no server-generated key needed advancing.",
                                tone="success",
                            )
                # A FAILED RESTART WITH must never read as done: surface it as an ERROR
                # so the operator does not repoint the app onto lagging sequences (the
                # app's first insert would collide, 23505). Audit finding D2 — this used
                # to be swallowed and painted as the success line above.
                if identity_sync_failed:
                    failed_detail = "; ".join(
                        f"{name} ({reason})"
                        for name, reason in sorted(identity_sync_failed.items())
                    )
                    render_notice(
                        ui,
                        tone="error",
                        header="Identity sequence sync failed — do not cut over yet",
                        body=(
                            f"{len(identity_sync_failed)} identity sequence(s) could "
                            "NOT be advanced past the migrated rows, so your "
                            "application's first insert after cut-over may hit a "
                            "duplicate key. Retry the sync (it is idempotent); if it "
                            "keeps failing, advance the sequence(s) manually with "
                            "ALTER TABLE … ALTER COLUMN … RESTART WITH before "
                            f"repointing. Affected: {failed_detail}."
                        ),
                    )

        # Rollback caveat: once the app writes to DSQL, those rows exist only there
        # (this tool does not replicate DSQL -> MySQL), so keep the source as a
        # read-only rollback anchor until sign-off.
        render_notice(
            ui,
            tone="info",
            header="Keep the source as your rollback anchor",
            body=(
                "Until you've signed off on DSQL, keep the MySQL source frozen "
                "(read-only) rather than dropping it. Before you repoint, rollback "
                "is trivial — the source is untouched and still authoritative. "
                "After the application writes to DSQL, those new rows live only on "
                "DSQL (this tool replicates MySQL -> DSQL, not the reverse), so "
                "rolling back then means reconciling them yourself first."
            ),
        )


def _render_readiness_checks(
    ui: object, summary: ValidationSummary, drift: DriftDisplay
) -> None:
    """Render the pre-cut-over checks as a compact, uniform pass/fail panel.

    1. Data identical -- per-table row counts (and, in checksum mode, checksums).
    2. No mismatched records -- full PK reconciliation (missing / extra rows).
    3. No table errors -- every table could be compared.
    4. No source drift -- the source has not advanced since the snapshot.
    """
    # Rows the migration is KNOWN to have dropped are not an unexplained difference, and
    # the readiness panel used to report them as one: two red "Failed" rows counting the
    # very rows the per-table entry beside them called "expected, not new data loss". That
    # is the one place a reviewer looks for "is anything unaccounted for?", so a
    # self-contradicting answer is worse than a blunt one. Name the cause in the detail
    # and, when the difference is ENTIRELY explained, drop the alarm to a warning -- never
    # to a pass: rows really are missing on the target, the operator accepted that, and
    # cut-over readiness must keep saying so.
    explained_tables = summary.quarantine_explained_tables
    explained_rows = summary.quarantine_explained_rows
    fully_explained = bool(explained_tables) and summary.unexplained_mismatched_tables == 0
    # Lead-in tying this panel back to the verdict. Since 0.1.255 the readiness panel
    # renders LAST (after the evidence, before Export), so a reader arriving here has
    # left the verdict far above -- and a "Heads-up" row could read as a new, weaker
    # signal. When the whole difference is the known dropped rows, say up front that the
    # conclusion is unchanged and the Heads-up items are those same rows, not a new find.
    if fully_explained:
        row_noun = "row" if explained_rows == 1 else "rows"
        ui.label(  # type: ignore[attr-defined]
            "Same conclusion as the verdict above — nothing unexplained. Each "
            f"'Heads-up' item below is the same {explained_rows} {row_noun} the "
            "migration already reported dropping, not a new problem."
        ).classes("text-xs text-gray-600")
    # Per-check "…is exactly the N rows dropped…" tail. Only when the gap is PARTIALLY
    # explained: there is no lead-in in that case, so each affected check must carry its
    # own cause. When it is FULLY explained the lead-in above already says every Heads-up
    # item is those same rows, so repeating it on Data identical AND No mismatched records
    # would state the one fact three times in one card. Suppressed there.
    if explained_tables and not fully_explained:
        noun = "table" if len(explained_tables) == 1 else "tables"
        row_noun = "row" if explained_rows == 1 else "rows"
        explained_note = (
            f" The difference in {len(explained_tables)} {noun} "
            f"({', '.join(explained_tables)}) is exactly the {explained_rows} "
            f"{row_noun} dropped during the migration — already reported, not new "
            "data loss."
        )
    else:
        explained_note = ""

    # Check 1: data match. The LABEL is mode-aware -- in ROW_COUNT mode only row
    # counts are compared (non-PK column VALUES are never read), so calling that
    # "Data identical" overstates what was verified; it is "Row counts match". Only
    # CHECKSUM mode actually value-compares, so only it earns "Data identical".
    _is_checksum_mode = str(summary.mode).upper().endswith("CHECKSUM")
    _match_label = "Data identical" if _is_checksum_mode else "Row counts match"
    # In CHECKSUM mode, FLOAT/DOUBLE and JSON columns have no byte-identical
    # cross-engine text form and are EXCLUDED from the checksum. Disclose it so a
    # "match" is not read as "every column value verified" (generic -- the per-table
    # column types are not carried on the report).
    _excluded_note = (
        " FLOAT/DOUBLE and JSON columns are not value-compared (no byte-identical "
        "cross-engine form); their row counts are still checked."
        if _is_checksum_mode
        else ""
    )
    _render_check_row(
        ui,
        passed=summary.is_match,
        label=_match_label,
        detail=(
            f"{summary.matched_tables}/{summary.total_tables} tables matched"
            + (
                f", {summary.mismatched_tables} mismatched"
                if summary.mismatched_tables
                else ""
            )
            + f" (mode: {summary.mode})."
            + explained_note
            + _excluded_note
        ),
        warn_on_fail=fully_explained,
    )

    # Check 2: no mismatched records (only meaningful when reconciliation ran).
    if summary.reconcile_performed:
        _render_check_row(
            ui,
            passed=summary.inconsistent_tables == 0,
            label="No missing or extra records",
            detail=(
                f"{summary.reconciled_tables} table(s) reconciled; "
                f"{summary.missing_on_target:,} record(s) missing on target, "
                f"{summary.extra_on_target:,} extra on target."
                + explained_note
            ),
            warn_on_fail=fully_explained,
        )
    else:
        _render_check_row(
            ui,
            passed=True,
            label="No missing or extra records",
            detail="Record-level reconciliation was turned off for this run.",
            neutral=True,
        )

    # Check 3: no table errors.
    _render_check_row(
        ui,
        passed=summary.errored_tables == 0,
        label="No table errors",
        detail=(
            "Every table was compared successfully."
            if summary.errored_tables == 0
            else f"{summary.errored_tables} table(s) could not be compared."
        ),
    )

    # Check 4: source drift since the snapshot (uniform with the others). Drift is
    # not a hard failure (it is expected under live CDC), so an advanced source is
    # a warning, an undeterminable/absent watermark is neutral.
    if not drift.available or not drift.determinable:
        _render_check_row(
            ui,
            passed=True,
            label="No source drift since snapshot",
            detail=drift.summary,
            neutral=True,
        )
    else:
        _render_check_row(
            ui,
            passed=not drift.drifted,
            label="No source drift since snapshot",
            detail=drift.summary,
            warn_on_fail=True,
        )

    ui.label(f"As-of (consistency point): {summary.as_of}").classes(  # type: ignore[attr-defined]
        "text-xs text-gray-500"
    )

    # Honesty caveat: FLOAT/DOUBLE and JSON columns have no byte-identical cross-engine
    # form, so the checksum omits them -- a "Data identical" pass means every OTHER
    # column was value-compared. Surfaced so the pass is not read as "every column
    # verified" (a non-key value diff confined to such a column is undetected).
    if summary.checksum_excluded_columns:
        detail = "; ".join(
            f"{table} ({', '.join(cols)})"
            for table, cols in summary.checksum_excluded_columns.items()
        )
        render_notice(
            ui,
            tone="info",
            header="Some columns were not value-compared",
            body=(
                "FLOAT/DOUBLE and JSON columns have no byte-identical cross-engine text "
                "form, so the checksum omits them — a 'Data identical' result means every "
                "OTHER column was value-compared, and a non-key value difference confined "
                f"to one of these columns would not be detected. Omitted: {detail}."
            ),
        )


def _render_check_row(
    ui: object,
    *,
    passed: bool,
    label: str,
    detail: str,
    neutral: bool = False,
    warn_on_fail: bool = False,
) -> None:
    """Render one readiness check as an icon + bold label + status chip + detail.

    ``neutral`` renders a quiet "Not run / N/A" row; ``warn_on_fail`` renders a
    failed check as a non-blocking amber warning (used for drift) rather than a
    blocking red failure.
    """
    if neutral:
        icon, color, tone, status = "remove_circle_outline", "grey-6", "neutral", "N/A"
    elif passed:
        icon, color, tone, status = "check_circle", "green-6", "ok", "Passed"
    elif warn_on_fail:
        icon, color, tone, status = "warning", "amber-6", "reconnect", "Heads-up"
    else:
        icon, color, tone, status = "cancel", "red-6", "bad", "Failed"
    with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        ui.icon(icon, color=color).classes("text-lg mt-0.5")  # type: ignore[attr-defined]
        with ui.column().classes("gap-0 min-w-0 flex-1"):  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.label(label).classes("text-sm font-semibold text-gray-900")  # type: ignore[attr-defined]
                ui.label(status).classes(  # type: ignore[attr-defined]
                    "text-[10px] leading-tight border rounded px-2 py-0.5 "
                    + badge_classes(tone)
                )
            ui.label(detail).classes("text-xs text-gray-600")  # type: ignore[attr-defined]


def _render_failing_tables(
    ui: object,
    report: ValidationReport,
    summary: ValidationSummary,
    drift: "Optional[DriftDisplay]" = None,
    *,
    diagnose_provider=None,
    recheck_provider=None,
    rechecking_tables: "Sequence[str]" = (),
) -> None:
    """Surface the failing tables (chips) + WHICH rows diverge (sample PKs).

    The core triage aid for a no-go: instead of scanning the whole per-table
    table, the reviewer sees exactly which tables need attention and, for each, a
    short explanation plus example diverging primary keys (from reconciliation /
    the dev row-diff sample). When AI Assist is on, each failing table gets an
    "Explain with AI" button (``diagnose_provider``). Nothing renders when every
    table passed.

    ``recheck_provider`` (when given) adds the re-validate actions: one per table
    plus a "Re-check all" for the whole failing set, so a fixed table can be
    re-confirmed without re-running the entire comparison. Tables named in
    ``rechecking_tables`` render as busy instead of offering the action.
    """
    failed = [
        item
        for item in report.items
        if item.error is not None or not item.matched
    ]
    if not failed:
        return
    busy = {name for name in rechecking_tables}
    with _section(
        ui, icon="error_outline", title=f"Tables needing attention ({len(failed)})"
    ):
        # Re-check the whole failing set in one click -- the common move after a
        # backfill. Offered only when more than one table failed (for a single
        # table the per-row action already is "re-check all").
        if recheck_provider is not None and len(failed) > 1:
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                names = [item.table for item in failed]
                ui.button(  # type: ignore[attr-defined]
                    f"Re-check all {len(failed)} tables",
                    icon="refresh",
                    on_click=lambda _e=None, _n=names: recheck_provider(_n),
                ).props("outline no-caps size=sm color=primary")
                ui.label(  # type: ignore[attr-defined]
                    "Re-compares only these tables and updates their rows in this "
                    "report."
                ).classes("text-xs text-gray-500")
        for item in failed:
            _render_failing_table(
                ui, item,
                diagnose_provider=diagnose_provider,
                recheck_provider=recheck_provider,
                busy=item.table in busy,
            )


def _render_failing_table(
    ui: object,
    item: TableValidationResult,
    *,
    diagnose_provider=None,
    recheck_provider=None,
    busy: bool = False,
) -> None:
    """Render one failing table: name + reason + example diverging PKs (+ actions).

    ``busy`` renders the inline "Re-checking…" state for a table whose re-check is
    in flight (the action is not offered again while it runs).
    """
    with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        ui.icon("cancel", color="red-6").classes("text-base mt-0.5")  # type: ignore[attr-defined]
        with ui.column().classes("gap-0 min-w-0 flex-1"):  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.label(item.table).classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-900 font-mono break-all"
                )
                ui.space()  # type: ignore[attr-defined]
                if busy:
                    ui.spinner(size="sm")  # type: ignore[attr-defined]
                    ui.label("Re-checking…").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-600"
                    )
                elif recheck_provider is not None:
                    ui.button(  # type: ignore[attr-defined]
                        "Re-check",
                        icon="refresh",
                        on_click=lambda _e=None, _n=item.table: recheck_provider(
                            [_n]
                        ),
                    ).props("flat dense no-caps size=sm color=primary").tooltip(
                        "Re-compare only this table (same options as this run) and "
                        "update its row in this report."
                    )
                if diagnose_provider is not None:
                    ui.button(  # type: ignore[attr-defined]
                        "Explain with AI",
                        icon="auto_awesome",
                        on_click=lambda _e=None, _it=item: diagnose_provider(
                            title="AI mismatch diagnosis",
                            subtitle=f"{_it.table} · table",
                            first_question=(
                                f"Why does `{_it.table}` not match, and exactly how "
                                "do I fix it?"
                            ),
                            facts=_validation_table_facts(_it),
                            scope="table",
                        ),
                    ).props("flat dense no-caps size=sm color=indigo-6")
            for line in _failure_reasons(item):
                ui.label(line).classes("text-xs text-gray-600")  # type: ignore[attr-defined]
            for label, pks, truncated in _sample_pk_lines(item):
                if not pks:
                    continue
                shown = ", ".join(pks)
                suffix = " …(more)" if truncated else ""
                ui.label(f"{label}: {shown}{suffix}").classes(  # type: ignore[attr-defined]
                    "text-[11px] text-gray-500 font-mono break-all"
                )


def _validation_table_facts(item: TableValidationResult) -> str:
    """Build the credential-free fact block for ONE failing table's AI chat.

    Counts, the match flags, a missing/extra SUMMARY (counts + a SHORT PK sample
    as a hint, never the full row set), and any per-table error -- the same facts
    shown on screen, formatted for grounding. No row values (Property 7).
    """
    lines = [
        f"Table: {item.table}",
        f"Source row count: {item.source_row_count:,}",
        f"Target row count: {item.target_row_count:,}",
        f"Row counts match: {item.row_count_match}",
    ]
    if item.error is not None:
        lines.append(f"Could not be compared (error): {item.error}")
    if item.checksum_match is not None:
        lines.append(f"Checksum match: {item.checksum_match}")
    if item.deep_checks_skipped:
        lines.append(
            "Deep checks (checksum / reconciliation) were skipped (fast sweep, "
            "counts matched)."
        )
    rec = item.reconcile
    if rec is not None:
        lines.append(
            f"Record reconciliation: {rec.missing_on_target:,} missing on target, "
            f"{rec.extra_on_target:,} extra on target (PK column {rec.pk_column})."
        )
        miss = list(getattr(rec, "missing_sample", []) or [])[:_FACTS_PK_SAMPLE]
        extra = list(getattr(rec, "extra_sample", []) or [])[:_FACTS_PK_SAMPLE]
        if miss:
            lines.append(f"Example missing PKs: {', '.join(map(str, miss))}")
        if extra:
            lines.append(f"Example extra PKs: {', '.join(map(str, extra))}")
    return "\n".join(lines)


def _validation_run_facts(summary: ValidationSummary, drift: DriftDisplay) -> str:
    """Build the credential-free fact block for the whole-run AI diagnosis.

    Roll-up counts (matched/mismatched, missing/extra totals, errored tables),
    whether reconciliation ran, and the drift signal -- enough for the model to
    judge lag vs standing gap vs extra rows and recommend recovery. No row values.
    """
    lines = [
        f"Tables total: {summary.total_tables}",
        f"Matched: {summary.matched_tables}; mismatched: {summary.mismatched_tables}",
        f"Comparison mode: {summary.mode}",
        f"Reconciliation ran: {summary.reconcile_performed}",
        f"Records missing on target (total): {summary.missing_on_target:,}",
        f"Records extra on target (total): {summary.extra_on_target:,}",
        f"Tables that errored: {summary.errored_tables}",
        f"As-of (consistency point): {summary.as_of}",
    ]
    if summary.failed_tables:
        shown = ", ".join(summary.failed_tables[:10])
        more = "" if len(summary.failed_tables) <= 10 else " …(more)"
        lines.append(f"Failing tables: {shown}{more}")
    if drift.available:
        if not drift.determinable:
            lines.append(
                "Source changes since snapshot: undeterminable (no GTID or binlog "
                "position to compare)."
            )
        else:
            lines.append(
                "Drift since snapshot: source HAS advanced (likely still live)."
                if drift.drifted
                else "Drift since snapshot: none (source unchanged since snapshot)."
            )
    return "\n".join(lines)


def _failure_reasons(item: TableValidationResult) -> list[str]:
    """Return human reasons this table failed (error / counts / records)."""
    if item.error is not None:
        return [f"Could not be compared: {item.error}"]
    reasons: list[str] = []
    if not item.row_count_match:
        reasons.append(
            f"Row count differs — source {item.source_row_count:,}, "
            f"target {item.target_row_count:,}."
        )
        # ATTRIBUTE the shortfall when the migration is known to have dropped exactly
        # that many rows. Without this the operator was told (by the manual, no less) to
        # cross-check the deficit against the Full Load error log by hand -- information
        # the tool already had. Requires an EXACT match: a table 4 rows short that
        # dropped 1 still has 3 rows unaccounted for, and calling that "expected" would
        # let real loss through the one check meant to catch it.
        if item.deficit_explained_by_quarantine:
            row_noun = "row was" if item.rows_quarantined == 1 else "rows were"
            reasons.append(
                f"Fully explained: {item.rows_quarantined:,} {row_noun} permanently "
                "dropped during the migration (a value DSQL could not store, e.g. over "
                "its ~1 MiB per-value limit) — this deficit is expected, not new data "
                "loss. Fix the source value(s) and reload that table to close it."
            )
        elif item.rows_quarantined > 0 and item.deficit > item.rows_quarantined:
            unexplained = item.deficit - item.rows_quarantined
            reasons.append(
                f"Partly explained: {item.rows_quarantined:,} row(s) were permanently "
                f"dropped during the migration, but {unexplained:,} more are missing "
                "and are NOT accounted for — investigate those."
            )
    if item.checksum_match is False:
        reasons.append("Checksum differs (row counts equal, but data is not).")
    reconcile = item.reconcile
    if reconcile is not None and not reconcile.consistent:
        reasons.append(
            f"{reconcile.missing_on_target:,} record(s) missing on target, "
            f"{reconcile.extra_on_target:,} extra on target."
        )
    if not reasons:
        reasons.append("Did not match.")
    return reasons


def _sample_pk_lines(
    item: TableValidationResult,
) -> list[tuple[str, list[str], bool]]:
    """Return ``(label, pk_values, truncated)`` example-PK lines for a table.

    Prefers the full reconciliation's missing/extra PK samples (Property 7: PK
    values only); falls back to the dev row-diff sample's PKs when present. Empty
    when the table carries no PK-level samples (e.g. a composite-PK table, or a
    checksum-only mismatch).
    """
    lines: list[tuple[str, list[str], bool]] = []
    reconcile = item.reconcile
    if reconcile is not None:
        if reconcile.missing_sample:
            lines.append(
                (
                    f"Missing on target (pk {reconcile.pk_column})",
                    list(reconcile.missing_sample),
                    reconcile.sample_truncated,
                )
            )
        if reconcile.extra_sample:
            lines.append(
                (
                    f"Extra on target (pk {reconcile.pk_column})",
                    list(reconcile.extra_sample),
                    reconcile.sample_truncated,
                )
            )
    sample = item.row_diff_sample
    if not lines and sample is not None and sample.findings:
        pks = [f.pk for f in sample.findings]
        lines.append((f"Diverging rows (pk {sample.pk_column})", pks, sample.truncated))
    return lines


# Per-table result-cell metadata: (display text, badge tone) for the colored
# Quasar body-cell badges, so a match/mismatch reads at a glance (no plain text).
def _cell(text: str, tone: str) -> dict[str, str]:
    """Return a ``{text, color}`` cell payload for a colored Quasar badge."""
    return {"text": text, "color": _QUASAR_BADGE_COLOR.get(tone, "grey-5")}


# Map our semantic tone to a Quasar badge color (kept local; the per-cell badges
# are rendered by Quasar, which wants its own color names).
_QUASAR_BADGE_COLOR: dict[str, str] = {
    "ok": "green-6",
    "bad": "red-6",
    "neutral": "grey-5",
}


def _render_tables(
    ui: object,
    report: ValidationReport,
    *,
    recheck_provider=None,
    rechecking_tables: "Sequence[str]" = (),
) -> None:
    """Render the per-table comparison results, sortable + filterable (6.1, 6.2).

    Failed tables sort first by default and a search box filters to a table name
    or status, so a reviewer can isolate problems instead of scrolling. Status
    cells render as colored Quasar badges (no plain colored text), and counts are
    thousands-separated.

    ``recheck_provider`` (when given) also lets the fast-sweep footnote offer a
    deep re-check of the tables that were verified by ROW COUNT only -- the one
    "passing" case where re-validating is genuinely useful, since those tables were
    never checksum/record-compared.
    """
    if not report.items:
        ui.label("No tables compared.").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
        return

    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left",
         "sortable": True},
        {"name": "source_rows", "label": "Source rows", "field": "source_rows",
         "align": "right", "sortable": True},
        {"name": "target_rows", "label": "Target rows", "field": "target_rows",
         "align": "right", "sortable": True},
        {"name": "row_count", "label": "Row count", "field": "row_count"},
        {"name": "checksum", "label": "Checksum", "field": "checksum"},
        {"name": "missing", "label": "Missing", "field": "missing",
         "align": "right", "sortable": True},
        {"name": "extra", "label": "Extra", "field": "extra",
         "align": "right", "sortable": True},
        {"name": "result", "label": "Result", "field": "result_sort",
         "sortable": True},
    ]
    rows = [_table_row(item) for item in report.items]
    table = ui.table(  # type: ignore[attr-defined]
        columns=columns,
        rows=rows,
        row_key="table",
        pagination=20,
    ).classes("w-full")
    # Default sort: failures first (result_sort: 0=fail/error, 1=pass).
    table.props("sort-by=result_sort")  # type: ignore[attr-defined]
    # Search box filters by any visible value (table name or status text).
    with table.add_slot("top-left"):  # type: ignore[attr-defined]
        ui.input(placeholder="Filter tables…").props(  # type: ignore[attr-defined]
            "dense outlined clearable"
        ).classes("min-w-64").bind_value(table, "filter")
    # Colored status badges (match/mismatch/error) instead of plain text. The slot
    # reads props.value, i.e. the column's ``field`` value -- fine for row_count /
    # checksum, whose field IS the {text,color} badge payload. The Result column is
    # different: its field is ``result_sort`` (an int, so "failures first" sorting
    # works), NOT the payload, so props.value has no ``.text`` and the badge always
    # fell through to "—". Result therefore gets its OWN slot that reads the payload
    # off ``props.row.result`` directly, leaving the sort key untouched.
    for col in ("row_count", "checksum"):
        table.add_slot(  # type: ignore[attr-defined]
            f"body-cell-{col}",
            r"""
            <q-td :props="props">
              <q-badge v-if="props.value && props.value.text"
                       :color="props.value.color" :label="props.value.text" />
              <span v-else>—</span>
            </q-td>
            """,
        )
    table.add_slot(  # type: ignore[attr-defined]
        "body-cell-result",
        r"""
        <q-td :props="props">
          <q-badge v-if="props.row.result && props.row.result.text"
                   :color="props.row.result.color" :label="props.row.result.text" />
          <span v-else>—</span>
        </q-td>
        """,
    )
    # Footnote: explain the "n/a" missing/extra cells when reconciliation ran but
    # could not cover every table (composite / non-integer PK).
    skipped = reconcile_skipped_tables(report)
    if skipped:
        # Mode-aware: in ROW_COUNT mode no checksum runs, so those tables are verified
        # by COUNT ALONE -- saying "count/checksum" would overstate what was checked
        # (audit finding C6). Only CHECKSUM mode value-compares them.
        _by = (
            "count and checksum"
            if str(report.mode).upper().endswith("CHECKSUM")
            else "row count ONLY (no value comparison in this mode)"
        )
        ui.label(  # type: ignore[attr-defined]
            f"“n/a” in Missing/Extra: {len(skipped)} table(s) have a composite or "
            f"non-integer primary key, so they are compared by {_by} "
            "(record-level reconciliation needs a single integer key)."
        ).classes("text-xs text-gray-500")
    # Fast sweep honesty: tables whose counts matched were NOT deep-checked, so the
    # operator knows the "match" for them is by row count, not a proven identical
    # row set. An info notice (expected state), not a warning.
    count_only = count_verified_tables(report)
    if count_only:
        # Whether a deep re-check would actually add a check (a checksum or a record
        # reconciliation). In a ROW_COUNT-mode report with no reconciliation there is
        # nothing deeper to run, so we keep the plain "turn off Fast sweep and re-run"
        # advice instead of offering a no-op button.
        deepen = recheck_provider is not None and deep_recheck_adds_checks(report)
        busy = {name for name in rechecking_tables}
        pending = [name for name in count_only if name not in busy]
        render_notice(
            ui,
            tone="info",
            header=(
                f"{len(count_only)} table(s) verified by row count only "
                "(fast sweep)"
            ),
            body=(
                "Fast sweep skipped the checksum / record reconciliation for tables "
                "whose row counts matched, so those are confirmed equal by count but "
                "not proven row-for-row identical. "
                + (
                    "Deep-check them below without re-running the whole validation."
                    if deepen
                    else
                    "For a full record-level guarantee before cut-over, turn off "
                    "Fast sweep and re-run."
                )
            ),
        )
        if deepen:
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                if pending:
                    ui.button(  # type: ignore[attr-defined]
                        f"Deep-check {len(pending)} count-only table(s)",
                        icon="fact_check",
                        on_click=lambda _e=None, _n=list(pending): recheck_provider(
                            _n
                        ),
                    ).props("outline no-caps size=sm color=primary").tooltip(
                        "Re-compare these tables with the checksum / record "
                        "reconciliation this run skipped, and update their rows here."
                    )
                else:
                    ui.spinner(size="sm")  # type: ignore[attr-defined]
                    ui.label("Deep-checking…").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-600"
                    )


def _table_row(item: TableValidationResult) -> dict[str, object]:
    """Build one per-table row, with colored-badge cells + sortable scalars."""
    if item.error is not None:
        # An errored table cannot offer counts; flag it clearly and sort first.
        return {
            "table": item.table,
            "source_rows": "—",
            "target_rows": "—",
            "row_count": _cell("error", "bad"),
            "checksum": None,
            "missing": "—",
            "extra": "—",
            "result": _cell("ERROR", "bad"),
            "result_sort": 0,
        }
    return {
        "table": item.table,
        "source_rows": f"{item.source_row_count:,}",
        "target_rows": f"{item.target_row_count:,}",
        "row_count": _cell(
            "match" if item.row_count_match else "mismatch",
            "ok" if item.row_count_match else "bad",
        ),
        "checksum": _checksum_cell(item),
        "missing": _reconcile_cell(item, "missing"),
        "extra": _reconcile_cell(item, "extra"),
        "result": _cell(
            "match" if item.matched else "mismatch",
            "ok" if item.matched else "bad",
        ),
        "result_sort": 1 if item.matched else 0,
    }


def _reconcile_cell(item: TableValidationResult, which: str) -> object:
    """Return the missing/extra reconciliation cell (``n/a`` when not reconciled)."""
    if item.reconcile is None:
        return "n/a"
    value = (
        item.reconcile.missing_on_target
        if which == "missing"
        else item.reconcile.extra_on_target
    )
    return f"{value:,}"


def _checksum_cell(item: TableValidationResult) -> Optional[dict[str, str]]:
    """Return the per-table checksum badge cell (``None`` => ``—`` in row-count mode)."""
    if item.checksum_match is None:
        return None
    return _cell(
        "match" if item.checksum_match else "mismatch",
        "ok" if item.checksum_match else "bad",
    )


def _render_orphans(ui: object, report: ValidationReport) -> None:
    """Render the orphan-record findings, if the orphan check was performed."""
    if not report.orphan_check_performed:
        return
    with _section(ui, icon="link_off", title="Orphan records"):
        if not report.orphan_findings:
            ui.label("No orphan records found.").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-500"
            )
            return
        columns = [
            {"name": "table", "label": "Table", "field": "table", "align": "left"},
            {"name": "foreign_key", "label": "Foreign key", "field": "foreign_key"},
            {
                "name": "referenced_table",
                "label": "Referenced table",
                "field": "referenced_table",
                "align": "left",
            },
            {"name": "orphan_count", "label": "Orphans", "field": "orphan_count",
             "align": "right"},
        ]
        rows = [
            {
                "table": finding.table,
                "foreign_key": finding.foreign_key,
                "referenced_table": finding.referenced_table,
                "orphan_count": f"{finding.orphan_count:,}",
            }
            for finding in report.orphan_findings
        ]
        ui.table(columns=columns, rows=rows).classes("w-full")  # type: ignore[attr-defined]


def drift_verdict(
    drift: DriftDisplay, *, cdc_in_use: bool
) -> tuple[str, str, str]:
    """Return the ``(tone, header, body)`` notice for the drift section.

    The raw fact -- "the source GTID differs from the watermark" -- is not what the
    user needs; what they need is whether it threatens the cut-over, and THAT depends
    on whether CDC is replicating. The section used to state the fact with no regard
    for the migration type, so a perfectly healthy CDC run was told its source "has
    advanced since the snapshot", which reads as a problem when it is the normal,
    expected state. Per the design system's severity calibration an expected state is
    ``info``, never ``warning``.

    * drift + CDC        -> ``info``: normal, CDC is carrying those writes across.
    * drift, no CDC      -> ``warning``: real, non-blocking -- post-snapshot writes
      are NOT on the target, so a cut-over now would lose them.
    * no drift           -> ``success``: the comparison is still current.
    * undeterminable     -> ``info``: no comparable coordinate; say what to do
      instead of alarming.

    Pure, so the calibration is unit-testable without NiceGUI.
    """
    if not drift.available:
        # No watermark at all: the run compared against the LIVE source, so there is
        # no consistency point to have drifted from. Distinguished from the
        # GTID-missing case below -- naming a GTID here would misdescribe the cause.
        return (
            "info",
            "Compared against the live source (no snapshot)",
            "This run had no export watermark, so it compared against the source as "
            "it was during the run rather than as-of a consistency point. For a "
            "definitive pre-cut-over check, freeze source writes and re-validate.",
        )
    if not drift.determinable:
        return (
            "info",
            "Could not tell whether the source changed",
            "The source reported neither a GTID nor a comparable binlog position, so "
            "this run cannot tell whether the source changed after the snapshot. "
            "Freeze source writes and re-validate for a definitive pre-cut-over "
            "check.",
        )
    if not drift.drifted:
        return (
            "success",
            "No source changes since the snapshot",
            "The source has not advanced since the consistency point, so this "
            "comparison still reflects the live source.",
        )
    if cdc_in_use:
        return (
            "info",
            "Source has advanced since the snapshot — expected with CDC",
            "New writes have landed on the source since the consistency point. CDC "
            "is replicating them to the target, so this is the normal steady state, "
            "not a gap. Before the final cut-over check, let CDC drain to zero lag "
            "with source writes frozen, then re-validate.",
        )
    return (
        "warning",
        "Source has advanced since the snapshot — not replicated",
        "New writes have landed on the source since the consistency point, and this "
        "migration has no CDC stream carrying them to the target. Those rows are "
        "therefore NOT on the target: cutting over now would lose them. Re-run the "
        "data migration (or freeze source writes and re-validate) before cut-over.",
    )


def _render_drift(ui: object, drift: DriftDisplay, *, cdc_in_use: bool = False) -> None:
    """Render the drift-since-watermark verdict, with the GTIDs as opt-in detail.

    The verdict notice carries the meaning (see :func:`drift_verdict`). The raw GTID
    pair is diagnostic -- the values cannot be read as "how far behind" (GTIDs are
    not a distance) and every actionable conclusion is already in the notice -- so it
    is collapsed rather than presented as the primary content.
    """
    tone, header, body = drift_verdict(drift, cdc_in_use=cdc_in_use)
    render_notice(ui, tone=tone, header=header, body=body)
    with ui.expansion(  # type: ignore[attr-defined]
        "Technical detail (replication coordinates)", icon="fingerprint"
    ).classes("w-full").props("dense expand-separator"):
        columns = [
            {"name": "field", "label": "Field", "field": "field", "align": "left"},
            {"name": "value", "label": "Value", "field": "value", "align": "left"},
        ]
        # Lead with the coordinate that ANSWERED the question, and label it as such.
        # Listing GTIDs first when the verdict came from binlog positions (the normal
        # case on RDS MySQL, where GTID is off) put two "unavailable" rows at the top
        # and buried the evidence that was actually used.
        if drift.basis == "binlog":
            rows = [
                {
                    "field": "Compared using",
                    "value": "Binlog position (GTID not enabled on the source)",
                },
                {"field": "At snapshot", "value": drift.watermark_binlog},
                {"field": "Now", "value": drift.current_binlog},
            ]
        elif drift.basis == "gtid":
            rows = [
                {"field": "Compared using", "value": "GTID"},
                {"field": "At snapshot", "value": drift.watermark_gtid},
                {"field": "Now", "value": drift.current_gtid},
            ]
        else:
            rows = [
                {"field": "GTID at snapshot", "value": drift.watermark_gtid},
                {"field": "GTID now", "value": drift.current_gtid},
                {"field": "Binlog at snapshot", "value": drift.watermark_binlog},
                {"field": "Binlog now", "value": drift.current_binlog},
            ]
        rows.append({"field": "Detail", "value": drift.detail})
        ui.table(columns=columns, rows=rows).classes("w-full")  # type: ignore[attr-defined]


def _render_downloads(ui: object, report: ValidationReport) -> None:
    """Render the export (download) buttons for the validation report (Req 8.4)."""

    def _download(download: ReportDownload) -> None:
        ui.download.content(  # type: ignore[attr-defined]
            download.content, download.filename, download.media_type
        )

    with ui.row().classes("gap-4 flex-wrap"):  # type: ignore[attr-defined]
        with ui.column().classes("gap-0"):  # type: ignore[attr-defined]
            ui.button(  # type: ignore[attr-defined]
                "JSON",
                icon="data_object",
                on_click=lambda: _download(validation_download(report, "json")),
            ).props("outline")
            ui.label("Machine-readable, for automation/archive.").classes(  # type: ignore[attr-defined]
                "text-xs text-gray-500"
            )
        with ui.column().classes("gap-0"):  # type: ignore[attr-defined]
            ui.button(  # type: ignore[attr-defined]
                "Text",
                icon="description",
                on_click=lambda: _download(validation_download(report, "text")),
            ).props("outline")
            ui.label("Readable summary incl. sample diverging PKs.").classes(  # type: ignore[attr-defined]
                "text-xs text-gray-500"
            )


__all__ = [
    "ValidationInputs",
    "ValidatorFactory",
    "run_validation",
    "job_status_to_step_status",
    "ValidationSummary",
    "summarize_validation",
    "humanize_as_of",
    "failed_table_names",
    "reconcile_skipped_tables",
    "count_verified_tables",
    "merge_revalidated",
    "RunOptions",
    "report_run_options",
    "deep_recheck_adds_checks",
    "validation_run_guard_reason",
    "group_objects_by_schema",
    "ResolvedScope",
    "resolve_validation_tables",
    "apply_table_filter",
    "included_from_exclusions",
    "ValidationScope",
    "build_validation_scope",
    "DriftDisplay",
    "format_drift",
    "ReportDownload",
    "validation_download",
    "ValidationState",
    "ValidationStore",
    "build_validation_screen",
    "build_cutover_screen",
]
