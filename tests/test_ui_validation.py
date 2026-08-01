# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Step 4 (Validation) screen's NiceGUI-agnostic logic.

These cover the parts of the Validation screen that do not touch NiceGUI:

- Run orchestration (:func:`run_validation`) wiring the validator with the
  inventory tables, mode, orphan flag, and the as-of export watermark, using an
  injected fake validator (no database) (Requirements 8.2, 6.5 / Property 11).
- Job-status -> step-status mapping used to drive the workflow transitions.
- Report summary and the drift/as-of-watermark presentation (Requirement 6.5).
- Report export serialization (JSON/text) for download (Requirement 8.4).
- Per-session validation state and its store (isolation, result/error handoff).
- End-to-end background run through the real :class:`JobManager`, asserting the
  Validation step transitions NOT_STARTED -> DONE / FAILED.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import (
    ColumnDef,
    DriftReport,
    OrphanFinding,
    ReconcileResult,
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
from dsql_migrator.ui.validation import (
    ValidationInputs,
    ValidationState,
    ValidationStore,
    apply_table_filter,
    build_validation_scope,
    failed_table_names,
    format_drift,
    humanize_as_of,
    job_status_to_step_status,
    reconcile_skipped_tables,
    resolve_validation_tables,
    run_validation,
    summarize_validation,
    validation_download,
)
from dsql_migrator.ui.validation import build_validation_screen


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _tables() -> list[TableDef]:
    return [
        TableDef(
            name="orders",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        ),
        TableDef(
            name="customers",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        ),
    ]


def _inventory() -> SourceInventory:
    return SourceInventory(tables=_tables())


def _watermark() -> Watermark:
    return Watermark(
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
        snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        table_row_counts={"orders": 10, "customers": 3},
    )


def _report(
    *,
    matched: bool = True,
    drift: DriftReport | None = None,
    snapshot: datetime | None = None,
    mode: ValidationMode = ValidationMode.ROW_COUNT,
    orphan_check: bool = False,
    orphans: list[OrphanFinding] | None = None,
) -> ValidationReport:
    """Build a small validation report for the presentation/export tests."""
    items = [
        TableValidationResult(
            table="orders",
            source_row_count=10,
            target_row_count=10 if matched else 9,
            row_count_match=matched,
            matched=matched,
        ),
        TableValidationResult(
            table="customers",
            source_row_count=3,
            target_row_count=3,
            row_count_match=True,
            matched=True,
        ),
    ]
    return ValidationReport.build(
        mode=mode,
        items=items,
        orphan_findings=orphans,
        orphan_check_performed=orphan_check,
        drift=drift,
        snapshot_timestamp=snapshot,
    )


class _FakeValidator:
    """A fake validator returning a canned report and recording its call args."""

    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        self.calls: list[dict[str, object]] = []

    def validate(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode = ValidationMode.ROW_COUNT,
        *,
        watermark: Watermark | None = None,
        check_orphans: bool = False,
        reconcile: bool = False,
        should_cancel=None,
        on_progress=None,
        max_workers: int = 1,
        deep_only_on_count_mismatch: bool = False,
        quarantined_by_table: dict[str, int] | None = None,
    ) -> ValidationReport:
        # Drive the progress callback like the real validator (before each table),
        # so a test can assert progress is reported.
        if on_progress is not None:
            for index, table in enumerate(tables, start=1):
                on_progress(table.name, index, len(tables))
        self.calls.append(
            {
                "tables": [table.name for table in tables],
                "mode": mode,
                "watermark": watermark,
                "check_orphans": check_orphans,
                "reconcile": reconcile,
                "should_cancel": should_cancel,
                "on_progress": on_progress,
                "max_workers": max_workers,
                "deep_only_on_count_mismatch": deep_only_on_count_mismatch,
                "quarantined_by_table": quarantined_by_table,
            }
        )
        return self.report


def _inputs(
    *,
    mode: ValidationMode = ValidationMode.ROW_COUNT,
    check_orphans: bool = False,
    watermark: Watermark | None = None,
) -> ValidationInputs:
    return ValidationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=SecretValue("pw"),
        target_config=TargetConnectionConfig(
            cluster_endpoint="cluster.dsql.example", region="us-east-1"
        ),
        inventory=_inventory(),
        mode=mode,
        check_orphans=check_orphans,
        watermark=watermark,
    )


# ---------------------------------------------------------------------------
# run_validation orchestration (Requirements 8.2, 6.5 / Property 11)
# ---------------------------------------------------------------------------


def test_run_validation_passes_tables_mode_and_options() -> None:
    fake = _FakeValidator(_report())
    inputs = _inputs(mode=ValidationMode.CHECKSUM, check_orphans=True)

    result = run_validation(inputs, validator_factory=lambda _i: fake)

    assert result is fake.report
    call = fake.calls[0]
    assert call["tables"] == ["orders", "customers"]
    assert call["mode"] is ValidationMode.CHECKSUM
    assert call["check_orphans"] is True


def test_run_validation_passes_watermark_for_as_of_comparison() -> None:
    fake = _FakeValidator(_report())
    watermark = _watermark()
    inputs = _inputs(watermark=watermark)

    run_validation(inputs, validator_factory=lambda _i: fake)

    assert fake.calls[0]["watermark"] is watermark


def test_run_validation_without_watermark_passes_none() -> None:
    fake = _FakeValidator(_report())
    run_validation(_inputs(), validator_factory=lambda _i: fake)
    assert fake.calls[0]["watermark"] is None


def test_run_validation_defaults_reconcile_on() -> None:
    # The pre-cut-over report reconciles every record by default (Step 5's job).
    fake = _FakeValidator(_report())
    run_validation(_inputs(), validator_factory=lambda _i: fake)
    assert fake.calls[0]["reconcile"] is True


def test_run_validation_passes_reconcile_off_when_disabled() -> None:
    fake = _FakeValidator(_report())
    inputs = _inputs()
    inputs = ValidationInputs(
        source_config=inputs.source_config,
        source_password=inputs.source_password,
        target_config=inputs.target_config,
        inventory=inputs.inventory,
        reconcile=False,
    )
    run_validation(inputs, validator_factory=lambda _i: fake)
    assert fake.calls[0]["reconcile"] is False


def test_run_validation_propagates_validator_failure() -> None:
    class _Boom:
        def validate(self, *args: object, **kwargs: object) -> ValidationReport:
            raise RuntimeError("target unreachable")

    with pytest.raises(RuntimeError, match="target unreachable"):
        run_validation(_inputs(), validator_factory=lambda _i: _Boom())


def test_run_validation_forwards_should_cancel() -> None:
    fake = _FakeValidator(_report())
    sentinel = lambda: True  # noqa: E731 - identity check only
    run_validation(
        _inputs(), validator_factory=lambda _i: fake, should_cancel=sentinel
    )
    assert fake.calls[0]["should_cancel"] is sentinel


def test_run_validation_forwards_on_progress_per_table() -> None:
    # The progress callback must fire once per table so the UI can show
    # "Checking table i of N: <name>" while a long run streams.
    from dsql_migrator.ui.validation import run_validation as _rv

    fake = _FakeValidator(_report())
    seen: list[tuple[str, int, int]] = []
    _rv(
        _inputs(),
        validator_factory=lambda _i: fake,
        on_progress=lambda t, i, n: seen.append((t, i, n)),
    )
    assert seen == [("orders", 1, 2), ("customers", 2, 2)]
    assert fake.calls[0]["on_progress"] is not None


def test_in_progress_label_names_current_table() -> None:
    from dsql_migrator.ui.validation import _in_progress_label

    assert _in_progress_label(None) == "Comparing source and target…"
    assert _in_progress_label(("orders", 3, 10)) == "Checking table 3 of 10: orders"


def test_validation_state_progress_roundtrip_and_clear() -> None:
    from dsql_migrator.ui.validation import ValidationState

    st = ValidationState()
    assert st.progress is None
    st.set_progress("orders", 2, 5)
    assert st.progress == ("orders", 2, 5)
    st.clear_progress()
    assert st.progress is None


def test_run_validation_default_should_cancel_is_none() -> None:
    fake = _FakeValidator(_report())
    run_validation(_inputs(), validator_factory=lambda _i: fake)
    assert fake.calls[0]["should_cancel"] is None


# ---------------------------------------------------------------------------
# Job-status -> step-status mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_status", "expected"),
    [
        ("DONE", StepStatus.DONE),
        ("FAILED", StepStatus.FAILED),
        # A cancelled run produced no report -> the step reads as "not run" (it is
        # re-runnable), not a failure.
        ("CANCELLED", StepStatus.NOT_STARTED),
        ("PENDING", None),
        ("RUNNING", None),
    ],
)
def test_job_status_to_step_status(job_status: str, expected) -> None:
    assert job_status_to_step_status(job_status) is expected


# ---------------------------------------------------------------------------
# Report summary (Requirement 6.4)
# ---------------------------------------------------------------------------


def test_summarize_validation_counts_matches_and_as_of() -> None:
    snapshot = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    summary = summarize_validation(
        _report(matched=True, snapshot=snapshot, mode=ValidationMode.CHECKSUM)
    )
    assert summary.total_tables == 2
    assert summary.matched_tables == 2
    assert summary.mismatched_tables == 0
    assert summary.is_match is True
    assert summary.mode == "CHECKSUM"
    assert summary.as_of == "2026-01-02 03:04 UTC"


def test_summarize_validation_reports_mismatch_and_live_source() -> None:
    summary = summarize_validation(_report(matched=False))
    assert summary.matched_tables == 1
    assert summary.mismatched_tables == 1
    assert summary.is_match is False
    assert summary.as_of == "live source (no watermark)"


def test_summarize_validation_counts_orphans() -> None:
    orphans = [
        OrphanFinding(
            table="orders",
            foreign_key="fk_customer",
            referenced_table="customers",
            orphan_count=2,
        )
    ]
    summary = summarize_validation(
        _report(orphan_check=True, orphans=orphans)
    )
    assert summary.orphan_count == 1
    # An orphan finding makes the overall verdict a mismatch (Property 9).
    assert summary.is_match is False


# ---------------------------------------------------------------------------
# Cut-over readiness summary: reconciliation + per-table errors
# ---------------------------------------------------------------------------


def _reconciled_report(
    *,
    missing: int = 0,
    extra: int = 0,
    error: str | None = None,
) -> ValidationReport:
    """Build a one-table report carrying a reconciliation result and/or an error."""
    if error is not None:
        item = TableValidationResult(
            table="orders",
            source_row_count=0,
            target_row_count=0,
            row_count_match=False,
            matched=False,
            error=error,
        )
    else:
        consistent = missing == 0 and extra == 0
        item = TableValidationResult(
            table="orders",
            source_row_count=10,
            target_row_count=10 - missing + extra,
            row_count_match=missing == 0 and extra == 0,
            matched=consistent,
            reconcile=ReconcileResult(
                pk_column="id",
                source_count=10,
                target_count=10 - missing + extra,
                missing_on_target=missing,
                extra_on_target=extra,
                missing_sample=[str(i) for i in range(missing)],
                extra_sample=[str(i) for i in range(extra)],
                consistent=consistent,
            ),
        )
    return ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[item])


def test_summarize_validation_reports_clean_reconciliation() -> None:
    summary = summarize_validation(_reconciled_report())
    assert summary.reconcile_performed is True
    assert summary.reconciled_tables == 1
    assert summary.inconsistent_tables == 0
    assert summary.missing_on_target == 0
    assert summary.extra_on_target == 0
    assert summary.errored_tables == 0
    assert summary.ready_for_cutover is True


def test_summarize_validation_reports_missing_and_extra_records() -> None:
    summary = summarize_validation(_reconciled_report(missing=2, extra=3))
    assert summary.reconcile_performed is True
    assert summary.inconsistent_tables == 1
    assert summary.missing_on_target == 2
    assert summary.extra_on_target == 3
    # A record-level divergence blocks cut-over even though no table errored.
    assert summary.ready_for_cutover is False
    assert summary.errored_tables == 0


def test_summarize_validation_reports_table_error() -> None:
    summary = summarize_validation(
        _reconciled_report(error='relation "orders" does not exist')
    )
    assert summary.errored_tables == 1
    assert summary.ready_for_cutover is False
    # An errored table did not reconcile, so the reconcile check is "not run".
    assert summary.reconcile_performed is False


def test_summarize_validation_reconcile_off_is_not_performed() -> None:
    # The default _report() carries no reconcile results (count-only).
    summary = summarize_validation(_report())
    assert summary.reconcile_performed is False
    assert summary.inconsistent_tables == 0
    assert summary.missing_on_target == 0


# ---------------------------------------------------------------------------
# UX helpers: humanized as-of, failing-table list, reconcile-skipped tables
# ---------------------------------------------------------------------------


def test_humanize_as_of_formats_timestamp_and_live_source() -> None:
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert humanize_as_of(ts) == "2026-01-02 03:04 UTC"
    # No watermark -> the live-source sentinel.
    assert humanize_as_of(None) == "live source (no watermark)"


def test_summary_as_of_uses_humanized_form() -> None:
    snapshot = datetime(2026, 6, 26, 14, 30, 0, tzinfo=timezone.utc)
    summary = summarize_validation(_report(snapshot=snapshot))
    assert summary.as_of == "2026-06-26 14:30 UTC"


def test_failed_table_names_lists_only_failing_in_order() -> None:
    # orders mismatches, customers matches -> only orders is "failing".
    report = _report(matched=False)
    assert failed_table_names(report) == ("orders",)
    summary = summarize_validation(report)
    assert summary.failed_tables == ("orders",)
    # A clean report has no failing tables.
    assert failed_table_names(_report(matched=True)) == ()


def test_failed_table_names_includes_errored_tables() -> None:
    report = _reconciled_report(error='relation "orders" does not exist')
    assert failed_table_names(report) == ("orders",)


def test_reconcile_skipped_tables_flags_unreconciled_when_pass_ran() -> None:
    # One reconciled table + one table that was compared by count only (no
    # reconcile result, no error) while the reconciliation pass ran.
    reconciled = TableValidationResult(
        table="orders",
        source_row_count=10,
        target_row_count=10,
        row_count_match=True,
        matched=True,
        reconcile=ReconcileResult(
            pk_column="id",
            source_count=10,
            target_count=10,
            consistent=True,
        ),
    )
    skipped = TableValidationResult(
        table="audit_log",  # composite/non-integer PK -> reconcile is None
        source_row_count=5,
        target_row_count=5,
        row_count_match=True,
        matched=True,
    )
    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=[reconciled, skipped]
    )
    assert reconcile_skipped_tables(report) == ("audit_log",)


def test_reconcile_skipped_tables_empty_when_reconcile_off() -> None:
    # No table has a reconcile result -> the pass did not run -> nothing skipped.
    assert reconcile_skipped_tables(_report()) == ()


def test_count_verified_and_reconcile_skipped_are_distinct() -> None:
    # Fast sweep: a count-matched table is verified-by-count (deep_checks_skipped),
    # which must be reported SEPARATELY from a composite-PK reconcile skip so the
    # composite-PK footnote never mislabels a fast-sweep skip.
    from dsql_migrator.ui.validation import count_verified_tables

    reconciled = TableValidationResult(
        table="orders", source_row_count=10, target_row_count=10,
        row_count_match=True, matched=True,
        reconcile=ReconcileResult(
            pk_column="id", source_count=10, target_count=10, consistent=True
        ),
    )
    composite_pk_skip = TableValidationResult(
        table="audit_log", source_row_count=5, target_row_count=5,
        row_count_match=True, matched=True,  # composite PK -> reconcile None
    )
    fast_sweep_skip = TableValidationResult(
        table="events", source_row_count=7, target_row_count=7,
        row_count_match=True, matched=True, deep_checks_skipped=True,
    )
    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[reconciled, composite_pk_skip, fast_sweep_skip],
    )
    # The fast-sweep table is "verified by count", NOT a composite-PK skip.
    assert count_verified_tables(report) == ("events",)
    assert reconcile_skipped_tables(report) == ("audit_log",)


# ---------------------------------------------------------------------------
# Validation scope: WHAT is validated (tables + identity context card)
# ---------------------------------------------------------------------------


def test_resolve_validation_tables_empty_selection_is_all() -> None:
    scope = resolve_validation_tables(_inventory(), TableSelection())
    assert [t.name for t in scope.tables] == ["orders", "customers"]
    assert scope.total_in_inventory == 2
    assert scope.is_subset is False


def test_resolve_validation_tables_narrows_to_selection() -> None:
    scope = resolve_validation_tables(
        _inventory(), TableSelection(selected_tables=["customers"])
    )
    assert [t.name for t in scope.tables] == ["customers"]
    assert scope.total_in_inventory == 2
    assert scope.is_subset is True


def test_resolve_validation_tables_unknown_selection_degrades_to_all() -> None:
    # A stale selection naming a table no longer in the inventory must not raise;
    # it falls back to validating all tables.
    scope = resolve_validation_tables(
        _inventory(), TableSelection(selected_tables=["ghost_table"])
    )
    assert [t.name for t in scope.tables] == ["orders", "customers"]
    assert scope.is_subset is False


def test_build_validation_scope_labels_source_target_and_subset() -> None:
    scope = resolve_validation_tables(
        _inventory(), TableSelection(selected_tables=["customers"])
    )
    view = build_validation_scope(
        migration_type="Full load + CDC",
        source_config=SourceConnectionConfig(
            host="db.abc.us-east-1.rds.amazonaws.com", database="app"
        ),
        target_config=TargetConnectionConfig(
            cluster_endpoint="mycluster.dsql.us-east-1.on.aws", region="us-east-1"
        ),
        target_cluster_name=None,
        scope=scope,
        watermark=_watermark(),
    )
    assert view.migration_type == "Full load + CDC"
    assert "app" in view.source_label
    assert view.source_detail == "db.abc.us-east-1.rds.amazonaws.com"
    # Cluster id derived from the endpoint when no friendly name is set.
    assert "mycluster" in view.target_label
    assert "us-east-1" in view.target_detail
    # Subset scope is surfaced as N of M.
    assert view.table_count == 1
    assert view.total_in_inventory == 2
    assert view.is_subset is True
    assert view.table_sample == ("customers",)
    assert view.sample_overflow == 0
    assert view.as_of == "2026-01-02 03:04 UTC"


def test_build_validation_scope_prefers_cluster_name_and_live_as_of() -> None:
    scope = resolve_validation_tables(_inventory(), TableSelection())
    view = build_validation_scope(
        migration_type="Full load only",
        source_config=SourceConnectionConfig(host="h", database="app"),
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        ),
        target_cluster_name="prod-orders",
        scope=scope,
        watermark=None,
    )
    # A friendly cluster name wins over the endpoint-derived id.
    assert "prod-orders" in view.target_label
    # No watermark -> live-source as-of.
    assert view.as_of == "live source (no watermark)"
    assert view.is_subset is False


def test_build_validation_scope_samples_and_overflows_many_tables() -> None:
    many = SourceInventory(
        tables=[
            TableDef(
                name=f"t{i}",
                columns=[ColumnDef(name="id", mysql_type="int")],
                primary_key=["id"],
            )
            for i in range(12)
        ]
    )
    scope = resolve_validation_tables(many, TableSelection())
    view = build_validation_scope(
        migration_type="Full load only",
        source_config=SourceConnectionConfig(host="h", database="app"),
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        ),
        target_cluster_name=None,
        scope=scope,
        watermark=None,
    )
    # Inline chip sample is capped; the rest collapse into '+N more'.
    assert view.table_count == 12
    assert len(view.table_sample) == 8
    assert view.sample_overflow == 4


# ---------------------------------------------------------------------------
# Object filter: validate only chosen tables within the migration scope
# ---------------------------------------------------------------------------


def test_apply_table_filter_empty_keeps_all_in_order() -> None:
    tables = _tables()  # orders, customers
    assert [t.name for t in apply_table_filter(tables, set())] == [
        "orders",
        "customers",
    ]


def test_apply_table_filter_narrows_and_preserves_scope_order() -> None:
    tables = _tables()
    # Filter set order is irrelevant; scope order (orders, customers) is kept.
    kept = apply_table_filter(tables, {"customers", "orders"})
    assert [t.name for t in kept] == ["orders", "customers"]
    kept_one = apply_table_filter(tables, {"customers"})
    assert [t.name for t in kept_one] == ["customers"]


def test_apply_table_filter_unknown_only_degrades_to_all() -> None:
    tables = _tables()
    # A stale filter naming only out-of-scope tables must not empty the run.
    assert [t.name for t in apply_table_filter(tables, {"ghost"})] == [
        "orders",
        "customers",
    ]
    # Mixed known + unknown keeps just the known ones.
    assert [t.name for t in apply_table_filter(tables, {"orders", "ghost"})] == [
        "orders"
    ]


def test_validation_state_table_exclude_defaults_empty() -> None:
    # No exclusions by default -> validate every in-scope object.
    state = ValidationState()
    assert state.table_exclude == set()


def test_included_from_exclusions_semantics() -> None:
    from dsql_migrator.ui.validation import included_from_exclusions

    scope = ["s.a", "s.b", "s.c"]
    # Nothing excluded -> empty include-set (the "validate all" sentinel).
    assert included_from_exclusions(scope, set()) == set()
    # One excluded -> include-set is the remaining names.
    assert included_from_exclusions(scope, {"s.b"}) == {"s.a", "s.c"}
    # Excluding everything degrades to "all" (never an empty run).
    assert included_from_exclusions(scope, {"s.a", "s.b", "s.c"}) == set()


def test_build_validation_scope_reports_filtered_counts() -> None:
    scope = resolve_validation_tables(_inventory(), TableSelection())  # 2 tables
    view = build_validation_scope(
        migration_type="Full load only",
        source_config=SourceConnectionConfig(host="h", database="app"),
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        ),
        target_cluster_name=None,
        scope=scope,
        watermark=None,
        table_filter={"customers"},
    )
    assert view.is_filtered is True
    assert view.table_count == 1  # filtered set
    assert view.scope_count == 2  # migration scope before the filter
    assert view.table_sample == ("customers",)


def test_build_validation_scope_not_filtered_when_filter_covers_scope() -> None:
    scope = resolve_validation_tables(_inventory(), TableSelection())
    view = build_validation_scope(
        migration_type="Full load only",
        source_config=SourceConnectionConfig(host="h", database="app"),
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        ),
        target_cluster_name=None,
        scope=scope,
        watermark=None,
        table_filter={"orders", "customers"},  # == whole scope
    )
    # Selecting everything is not a "filtered" subset.
    assert view.is_filtered is False
    assert view.table_count == 2


# ---------------------------------------------------------------------------
# Drift / as-of-watermark presentation (Requirement 6.5 / Property 11)
# ---------------------------------------------------------------------------


def test_format_drift_reports_advance_since_snapshot() -> None:
    drift = DriftReport(
        watermark_gtid="uuid:1-5",
        current_gtid="uuid:1-9",
        drifted=True,
        detail="Source advanced since the snapshot (GTID changed).",
        basis="gtid",
    )
    display = format_drift(_report(drift=drift))
    assert display.available is True
    assert display.determinable is True
    assert display.drifted is True
    assert display.watermark_gtid == "uuid:1-5"
    assert display.current_gtid == "uuid:1-9"
    # The summary names its evidence, so a reader knows what "changed" was derived from.
    assert "changed since the snapshot" in display.summary
    assert "GTID changed" in display.summary
    assert display.basis == "gtid"


def test_format_drift_reports_no_change_since_snapshot() -> None:
    drift = DriftReport(
        watermark_gtid="uuid:1-5",
        current_gtid="uuid:1-5",
        drifted=False,
        detail="No source changes since the snapshot.",
    )
    display = format_drift(_report(drift=drift))
    assert display.determinable is True
    assert display.drifted is False
    assert "No source changes since the snapshot." in display.summary


def test_format_drift_degrades_when_gtid_unavailable() -> None:
    drift = DriftReport(
        watermark_gtid=None,
        current_gtid=None,
        drifted=False,
        detail="GTID unavailable; drift since snapshot could not be determined.",
    )
    display = format_drift(_report(drift=drift))
    assert display.available is True
    assert display.determinable is False
    assert display.watermark_gtid == "unavailable"
    assert display.current_gtid == "unavailable"
    assert "could not be determined" in display.summary


def test_format_drift_not_available_without_watermark() -> None:
    display = format_drift(_report(drift=None))
    assert display.available is False
    assert display.determinable is False
    assert display.watermark_gtid == "unavailable"
    assert "not available" in display.summary


# ---------------------------------------------------------------------------
# Report export serialization (Requirement 8.4)
# ---------------------------------------------------------------------------


def test_validation_download_json_is_valid_and_named() -> None:
    drift = DriftReport(
        watermark_gtid="uuid:1-5",
        current_gtid="uuid:1-9",
        drifted=True,
        detail="Source advanced since the snapshot (GTID changed).",
    )
    download = validation_download(_report(drift=drift), "json")
    assert download.filename == "validation_report.json"
    assert download.media_type == "application/json"
    parsed = json.loads(download.content)
    assert "items" in parsed
    assert parsed["drift"]["drifted"] is True


def test_validation_download_text_is_human_readable() -> None:
    download = validation_download(_report(matched=False), "text")
    assert download.filename == "validation_report.txt"
    assert download.media_type == "text/plain"
    assert "Validation Report" in download.content
    assert "Overall: MISMATCH" in download.content


def test_validation_download_text_includes_drift_section() -> None:
    drift = DriftReport(
        watermark_gtid="uuid:1-5",
        current_gtid="uuid:1-9",
        drifted=True,
        detail="Source advanced since the snapshot (GTID changed).",
    )
    download = validation_download(_report(drift=drift), "text")
    assert "Drift since snapshot:" in download.content
    assert "Current source GTID: uuid:1-9" in download.content


def test_validation_download_rejects_unknown_format() -> None:
    with pytest.raises(ValueError):
        validation_download(_report(), "yaml")


def test_validation_download_text_includes_readiness_and_reconciliation() -> None:
    # The text report carries the cut-over readiness summary and the per-table
    # missing/extra reconciliation detail.
    download = validation_download(_reconciled_report(missing=2, extra=1), "text")
    assert "Cut-over readiness:" in download.content
    assert "No mismatched records: NO" in download.content
    assert "missing on target" in download.content
    assert "extra on target" in download.content


def test_validation_download_text_reports_table_error() -> None:
    download = validation_download(
        _reconciled_report(error='relation "orders" does not exist'), "text"
    )
    assert "No table errors: NO" in download.content
    assert "ERROR" in download.content
    assert "does not exist" in download.content


def test_validation_download_json_includes_reconcile_and_error() -> None:
    parsed = json.loads(
        validation_download(_reconciled_report(missing=2), "json").content
    )
    assert parsed["items"][0]["reconcile"]["missing_on_target"] == 2
    err = json.loads(
        validation_download(_reconciled_report(error="boom"), "json").content
    )
    assert err["items"][0]["error"] == "boom"


# ---------------------------------------------------------------------------
# Per-table re-check: merging a fresh comparison into an existing report
# ---------------------------------------------------------------------------


def _item(
    table: str,
    *,
    matched: bool = True,
    source: int = 10,
    target: int = 10,
    error: str | None = None,
    reconcile: ReconcileResult | None = None,
) -> TableValidationResult:
    """Build one per-table result for the merge tests."""
    return TableValidationResult(
        table=table,
        source_row_count=source,
        target_row_count=target,
        row_count_match=source == target,
        matched=matched,
        error=error,
        reconcile=reconcile,
    )


def _mismatched_report() -> ValidationReport:
    """A 3-table report where 'orders' fails on row count (the others pass)."""
    from dsql_migrator.ui.validation import merge_revalidated  # noqa: F401

    return ValidationReport.build(
        mode=ValidationMode.CHECKSUM,
        items=[
            _item("orders", matched=False, source=10, target=9),
            _item("customers"),
            _item("products"),
        ],
        snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    )


def test_merge_revalidated_replaces_only_the_rechecked_table_in_place() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = _mismatched_report()
    merged = merge_revalidated(report, [_item("orders", source=10, target=10)])
    # Order preserved; only 'orders' changed; the other verdicts survive untouched.
    assert [i.table for i in merged.items] == ["orders", "customers", "products"]
    assert merged.items[0].target_row_count == 10
    assert merged.items[0].matched is True
    assert merged.items[1] == report.items[1]
    assert merged.items[2] == report.items[2]
    # The run-level fields describe the RUN and are carried over.
    assert merged.mode is ValidationMode.CHECKSUM
    assert merged.snapshot_timestamp == report.snapshot_timestamp
    # The original report is untouched (pure).
    assert report.items[0].matched is False
    assert report.is_match is False


def test_merge_revalidated_recomputes_verdict_in_both_directions() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = _mismatched_report()
    assert report.is_match is False
    # The last failing table now passes -> the overall verdict flips to a match.
    fixed = merge_revalidated(report, [_item("orders")])
    assert fixed.is_match is True
    assert summarize_validation(fixed).ready_for_cutover is True
    # And a previously-passing table that now fails flips it back (never a stale
    # carried-over True).
    broken = merge_revalidated(fixed, [_item("customers", matched=False, target=4)])
    assert broken.is_match is False


def test_merge_revalidated_ignores_tables_absent_from_the_report() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = _mismatched_report()
    merged = merge_revalidated(
        report, [_item("orders"), _item("not_in_scope", matched=False)]
    )
    # A stale name is dropped, never appended -- the report's scope cannot widen
    # (that would change what "all tables match" means).
    assert [i.table for i in merged.items] == ["orders", "customers", "products"]
    assert merged.is_match is True


def test_merge_revalidated_replaces_orphans_for_rechecked_tables_only() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[_item("orders", matched=False, target=9), _item("customers")],
        orphan_findings=[
            OrphanFinding(
                table="orders",
                foreign_key="fk_o",
                referenced_table="customers",
                orphan_count=5,
            ),
            OrphanFinding(
                table="customers",
                foreign_key="fk_c",
                referenced_table="regions",
                orphan_count=2,
            ),
        ],
        orphan_check_performed=True,
    )
    # 'orders' orphans were fixed: re-checking it with an empty finding list drops
    # its finding, while 'customers' keeps its own.
    merged = merge_revalidated(report, [_item("orders")], orphan_findings=[])
    assert [f.table for f in merged.orphan_findings] == ["customers"]
    assert merged.orphan_check_performed is True
    # Still no overall match: an orphan finding remains (Property 9).
    assert merged.is_match is False

    # Passing None leaves the findings alone (the re-check did not re-run orphans).
    untouched = merge_revalidated(report, [_item("orders")])
    assert [f.table for f in untouched.orphan_findings] == ["orders", "customers"]


def test_merge_revalidated_carries_drift_over_unchanged() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    drift = DriftReport(
        watermark_gtid="uuid:1-5", current_gtid="uuid:1-9", drifted=True
    )
    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[_item("orders", matched=False, target=9)],
        drift=drift,
    )
    merged = merge_revalidated(report, [_item("orders")])
    # Drift is a whole-source signal a single-table re-check does not re-measure,
    # so it must be carried over rather than silently re-dated.
    assert merged.drift == drift


def test_merge_revalidated_clears_a_resolved_error() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[_item("orders", matched=False, error="relation does not exist")],
    )
    assert summarize_validation(report).errored_tables == 1
    merged = merge_revalidated(report, [_item("orders")])
    # An errored table is re-checkable and its error clears when it succeeds.
    assert merged.items[0].error is None
    assert summarize_validation(merged).errored_tables == 0
    assert merged.is_match is True


def test_report_run_options_recovers_mode_reconcile_and_orphans() -> None:
    from dsql_migrator.ui.validation import report_run_options

    # A checksum + reconciled + orphan-checked run.
    reconciled = ReconcileResult(
        pk_column="id",
        source_count=10,
        target_count=10,
        missing_on_target=0,
        extra_on_target=0,
        consistent=True,
    )
    report = ValidationReport.build(
        mode=ValidationMode.CHECKSUM,
        items=[_item("orders", reconcile=reconciled), _item("customers")],
        orphan_check_performed=True,
    )
    options = report_run_options(report)
    assert options.mode is ValidationMode.CHECKSUM
    assert options.reconcile is True
    assert options.check_orphans is True
    # Matches the claim summarize_validation makes about the same report, so a
    # re-check reproduces exactly the checks the UI says the report contains.
    assert options.reconcile is summarize_validation(report).reconcile_performed

    # A plain row-count run with no reconciliation and no orphan check.
    plain = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=[_item("orders")]
    )
    plain_options = report_run_options(plain)
    assert plain_options.mode is ValidationMode.ROW_COUNT
    assert plain_options.reconcile is False
    assert plain_options.check_orphans is False


def test_recheck_options_come_from_report_not_live_toggles() -> None:
    # The screen must re-check with the REPORT's options, not whatever the user has
    # since selected on screen -- otherwise a reconciled item spliced into a
    # never-reconciled report would make reconcile_skipped_tables mislabel every
    # other table as composite-PK.
    from dsql_migrator.ui.validation import report_run_options

    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=[_item("orders", matched=False, target=9)]
    )
    state = ValidationState()
    state.mode = ValidationMode.CHECKSUM  # user changed the toggles after the run
    state.reconcile = True
    state.check_orphans = True
    options = report_run_options(report)
    assert options.mode is ValidationMode.ROW_COUNT
    assert options.reconcile is False
    assert options.check_orphans is False


def test_merge_recheck_result_accumulates_marks_and_stamps_time() -> None:
    state = ValidationState()
    state.set_result(_mismatched_report())
    assert state.rechecked_tables == ()
    assert state.rechecked_at is None

    assert state.merge_recheck_result([_item("orders")]) is True
    assert state.rechecked_tables == ("orders",)
    assert state.rechecked_at is not None
    assert state.result is not None and state.result.is_match is True

    # A second re-check accumulates (both rows are newer than the run).
    assert state.merge_recheck_result([_item("customers")]) is True
    assert state.rechecked_tables == ("customers", "orders")


def test_merge_recheck_result_is_a_noop_without_a_report() -> None:
    # A full re-run clears the report while a re-check is in flight: the late merge
    # must NOT resurrect it (or leave a single-table report reading as the whole run).
    state = ValidationState()
    state.set_result(_mismatched_report())
    state.clear_outputs()
    assert state.merge_recheck_result([_item("orders")]) is False
    assert state.result is None
    assert state.rechecked_tables == ()


def test_full_run_result_clears_prior_recheck_marks() -> None:
    state = ValidationState()
    state.set_result(_mismatched_report())
    state.merge_recheck_result([_item("orders")])
    assert state.rechecked_tables == ("orders",)
    # A full run replaces the whole report -> every row is from one run again.
    state.set_result(_mismatched_report())
    assert state.rechecked_tables == ()
    assert state.rechecked_at is None


def test_merge_recheck_result_drops_the_restored_banner() -> None:
    # Part of the report was just measured live, so the "restored, may be stale"
    # banner would be misleading; the re-check note takes over the as-of story.
    state = ValidationState()
    state.restore(_mismatched_report(), datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert state.restored is True
    state.merge_recheck_result([_item("orders")])
    assert state.restored is False


def test_recheck_track_start_and_finish() -> None:
    state = ValidationState()
    assert state.recheck_tables == ()
    state.start_recheck(["orders", "customers"])
    assert state.recheck_tables == ("orders", "customers")
    state.set_recheck_error("token expired")
    assert state.recheck_error == "token expired"
    # Starting another re-check clears the previous error.
    state.start_recheck(["orders"])
    assert state.recheck_error is None
    state.finish_recheck()
    assert state.recheck_tables == ()


def test_restore_rehydrates_recheck_marks_and_defaults_empty() -> None:
    state = ValidationState()
    completed = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    rechecked = datetime(2026, 1, 2, 4, 0, tzinfo=timezone.utc)
    state.restore(
        _mismatched_report(),
        completed,
        rechecked_tables=("orders",),
        rechecked_at=rechecked,
    )
    assert state.rechecked_tables == ("orders",)
    assert state.rechecked_at == rechecked
    # Older snapshots (no marks) restore as a plain uniform run.
    plain = ValidationState()
    plain.restore(_mismatched_report(), completed)
    assert plain.rechecked_tables == ()
    assert plain.rechecked_at is None


def test_reset_in_place_clears_the_recheck_track() -> None:
    store = ValidationStore()
    state = store.get_or_create("s")
    state.set_result(_mismatched_report())
    state.merge_recheck_result([_item("orders")])
    state.start_recheck(["customers"])
    store.reset_in_place("s")
    # Same object (closures keep their reference), fully re-initialised.
    assert store.get_or_create("s") is state
    assert state.result is None
    assert state.recheck_tables == ()
    assert state.rechecked_tables == ()
    assert state.rechecked_at is None
    assert state.recheck_error is None


# ---------------------------------------------------------------------------
# Per-session validation state and store
# ---------------------------------------------------------------------------


def test_validation_state_defaults_and_result_error_handoff() -> None:
    state = ValidationState()
    assert state.mode is ValidationMode.ROW_COUNT
    assert state.check_orphans is False
    assert state.result is None
    assert state.error is None

    report = _report()
    state.set_result(report)
    assert state.result is report
    assert state.error is None

    state.set_error("boom")
    assert state.error == "boom"
    state.clear_outputs()
    assert state.result is None
    assert state.error is None


def test_validation_state_set_result_clears_prior_error() -> None:
    state = ValidationState()
    state.set_error("previous failure")
    state.set_result(_report())
    assert state.error is None


def test_validation_store_is_isolated_per_session() -> None:
    store = ValidationStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")
    assert a is not b
    assert store.get_or_create("session-a") is a

    a.mode = ValidationMode.CHECKSUM
    assert b.mode is ValidationMode.ROW_COUNT


def test_validation_store_clear_removes_only_target_session() -> None:
    store = ValidationStore()
    store.get_or_create("session-a")
    store.get_or_create("session-b")

    store.clear("session-a")
    assert store.get("session-a") is None
    assert store.get("session-b") is not None
    store.clear("missing")
    store.clear(None)


# ---------------------------------------------------------------------------
# End-to-end background run through the real JobManager
# ---------------------------------------------------------------------------


def test_background_run_finishes_done_and_records_result() -> None:
    manager = JobManager()
    state = ValidationState()
    fake = _FakeValidator(_report(snapshot=_watermark().snapshot_timestamp))
    inputs = _inputs(watermark=_watermark())

    def work(_handle: object) -> None:
        state.set_result(run_validation(inputs, validator_factory=lambda _i: fake))

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    job = manager.get_status(job_id)
    assert job_status_to_step_status(job.status) is StepStatus.DONE
    assert state.result is not None
    assert state.result.is_match is True


def test_background_run_failure_maps_to_failed_status() -> None:
    manager = JobManager()

    def work(_handle: object) -> None:
        raise RuntimeError("validation failed")

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    job = manager.get_status(job_id)
    assert job_status_to_step_status(job.status) is StepStatus.FAILED
    assert "validation failed" in (manager.get_error(job_id) or "")


def test_background_run_cancel_maps_to_cancelled_and_no_result() -> None:
    # Mirrors the screen's work(): a validator that honors should_cancel by
    # raising ValidationCancelled, caught so the job ends CANCELLED (not FAILED)
    # and no report is stored.
    from dsql_migrator.core.validator import ValidationCancelled

    import time

    manager = JobManager()
    state = ValidationState()
    inputs = _inputs()

    class _CancellingValidator:
        """Waits until a cooperative cancel is observed, then raises (no race)."""

        def validate(self, *args, should_cancel=None, **kwargs):
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if should_cancel is not None and should_cancel():
                    raise ValidationCancelled("cancelled")
                time.sleep(0.01)
            return _report()  # pragma: no cover - cancel should always arrive

    def work(handle: object) -> None:
        try:
            result = run_validation(
                inputs,
                validator_factory=lambda _i: _CancellingValidator(),
                should_cancel=lambda: bool(getattr(handle, "cancelled", False)),
            )
        except ValidationCancelled:
            return
        state.set_result(result)

    job_id = manager.submit(work)
    # Request cancel; the worker is polling and will observe it and stop.
    manager.request_cancel(job_id)
    assert manager.wait(job_id, timeout=5.0) is True

    job = manager.get_status(job_id)
    assert job.status == "CANCELLED"
    assert job_status_to_step_status(job.status) is StepStatus.NOT_STARTED
    # No partial report was stored on cancel.
    assert state.result is None


# ---------------------------------------------------------------------------
# runner() connection gating: never start a run on an unverified connection
# ---------------------------------------------------------------------------


def _build_runner_with_session(*, source_verified: bool, target_verified: bool):
    """Build the validation screen over real stores with a seeded session.

    Returns (runner, validation_state, job_manager) so a test can call runner()
    and assert whether a job was submitted. A fake validator is injected so a run,
    if it were (wrongly) started, would not touch a real database.
    """
    from dsql_migrator.core.models import AssessmentReport, TargetInventory
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore

    session_id = "s1"
    store = SessionStore()
    eval_store = EvaluationStore()
    migration_store = DataMigrationStore()
    validation_store = ValidationStore()
    manager = JobManager()

    session = store.get_or_create(session_id)
    session.set_source(
        SourceConnectionConfig(host="db", database="app"), SecretValue("pw")
    )
    session.set_target(
        TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        )
    )
    session.set_source_verified(source_verified)
    session.set_target_verified(target_verified)

    eval_state = eval_store.get_or_create(session_id)
    eval_state.set_result(
        EvaluationResult(
            inventory=_inventory(),
            assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(),
            target_conflicts=[],
        )
    )

    _content, runner = build_validation_screen(
        store,
        session_id,
        job_manager=manager,
        eval_store=eval_store,
        migration_store=migration_store,
        validation_store=validation_store,
        validator_factory=lambda _i: _FakeValidator(_report()),
    )
    return runner, validation_store.get_or_create(session_id), manager


def test_runner_blocks_when_source_unverified() -> None:
    runner, state, manager = _build_runner_with_session(
        source_verified=False, target_verified=True
    )
    runner()
    # No job was submitted and a clear, actionable error is set.
    assert state.job_id is None
    assert state.error is not None
    assert "Source connection is not verified" in state.error


def test_runner_blocks_when_target_unverified() -> None:
    runner, state, manager = _build_runner_with_session(
        source_verified=True, target_verified=False
    )
    runner()
    assert state.job_id is None
    assert state.error is not None
    assert "Target connection is not verified" in state.error


def test_runner_submits_when_both_verified() -> None:
    runner, state, manager = _build_runner_with_session(
        source_verified=True, target_verified=True
    )
    runner()
    # Both connections verified -> a job is submitted (no gating error).
    assert state.job_id is not None
    assert manager.wait(state.job_id, timeout=5.0) is True


def test_source_engine_kwargs_set_bounded_connect_timeout() -> None:
    # An unreachable source must fail fast, not hang: the shared engine settings
    # carry a bounded connect_timeout (and keep pool_pre_ping).
    from dsql_migrator.core.introspector import (
        SOURCE_CONNECT_TIMEOUT_SECONDS,
        source_engine_kwargs,
    )

    kwargs = source_engine_kwargs()
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["connect_args"]["connect_timeout"] == SOURCE_CONNECT_TIMEOUT_SECONDS
    assert SOURCE_CONNECT_TIMEOUT_SECONDS > 0
    # The default (introspection/validation) must NOT carry a per-socket read
    # timeout: a legitimate long COUNT(*)/checksum query must not be killed.
    assert "read_timeout" not in kwargs["connect_args"]


def test_source_engine_kwargs_opt_in_read_timeout() -> None:
    # The Full Load stream opts into a per-socket read/write timeout so a
    # connected-but-stalled read raises instead of blocking forever.
    from dsql_migrator.core.introspector import source_engine_kwargs

    kwargs = source_engine_kwargs(read_timeout_seconds=120)
    assert kwargs["connect_args"]["read_timeout"] == 120
    assert kwargs["connect_args"]["write_timeout"] == 120
    # connect_timeout is still present (unchanged).
    assert "connect_timeout" in kwargs["connect_args"]


# ---------------------------------------------------------------------------
# Orphaned IN_PROGRESS: no live job -> not "alive" (prevents stuck spinner)
# ---------------------------------------------------------------------------


def test_running_job_alive_false_when_no_job_id() -> None:
    # A restored IN_PROGRESS with no in-memory job id (job id is not persisted)
    # must NOT be treated as a live run.
    from dsql_migrator.ui.validation import _running_job_alive

    manager = JobManager()
    state = ValidationState()  # job_id is None
    assert _running_job_alive(manager, state) is False


def test_running_job_alive_false_when_job_unknown() -> None:
    # A job id that the (fresh) JobManager does not know about -> not alive.
    from dsql_migrator.ui.validation import _running_job_alive

    manager = JobManager()
    state = ValidationState()
    state.job_id = "ghost-job-id"
    assert _running_job_alive(manager, state) is False


def test_running_job_alive_false_when_job_terminal() -> None:
    # A finished (DONE) job is not a live in-flight run.
    from dsql_migrator.ui.validation import _running_job_alive

    manager = JobManager()
    state = ValidationState()
    state.job_id = manager.submit(lambda _h: None)
    assert manager.wait(state.job_id, timeout=5.0) is True
    assert _running_job_alive(manager, state) is False


def test_running_job_alive_true_for_in_flight_job() -> None:
    # A genuinely running job is reported alive so the spinner is shown.
    import threading

    from dsql_migrator.ui.validation import _running_job_alive

    manager = JobManager()
    state = ValidationState()
    release = threading.Event()

    state.job_id = manager.submit(lambda _h: release.wait(5.0))
    try:
        # Give the worker a moment to enter RUNNING.
        import time

        for _ in range(50):
            if _running_job_alive(manager, state):
                break
            time.sleep(0.01)
        assert _running_job_alive(manager, state) is True
    finally:
        release.set()
        manager.wait(state.job_id, timeout=5.0)


# ---------------------------------------------------------------------------
# AI diagnosis fact builders (credential-free grounding)
# ---------------------------------------------------------------------------


def test_validation_table_facts_includes_counts_and_reconcile_summary() -> None:
    from dsql_migrator.ui.validation import _validation_table_facts

    item = TableValidationResult(
        table="customers_sample_new.orders",
        source_row_count=72590,
        target_row_count=72516,
        row_count_match=False,
        matched=False,
        reconcile=ReconcileResult(
            pk_column="order_id",
            source_count=72590,
            target_count=72516,
            missing_on_target=74,
            extra_on_target=0,
            missing_sample=["1001", "1002", "1003"],
        ),
    )
    facts = _validation_table_facts(item)
    assert "72,590" in facts and "72,516" in facts
    assert "74 missing on target" in facts
    assert "order_id" in facts
    # A SHORT PK sample is included as a hint (not the full set).
    assert "1001" in facts


def test_validation_run_facts_rolls_up_and_reports_drift() -> None:
    from dsql_migrator.ui.validation import _validation_run_facts, format_drift

    report = _report(matched=False)  # orders mismatches, customers matches
    summary = summarize_validation(report)
    facts = _validation_run_facts(summary, format_drift(report))
    assert "Tables total: 2" in facts
    assert "mismatched: 1" in facts
    assert "orders" in facts  # failing table named


# ---------------------------------------------------------------------------
# Connection prerequisite notice (source + target, symmetric)
# ---------------------------------------------------------------------------


class _FakeSession:
    def __init__(self, *, src=True, src_ok=True, tgt=True, tgt_ok=True):
        self._src, self._tgt = src, tgt
        self.source_verified, self.target_verified = src_ok, tgt_ok

    def has_source(self):
        return self._src

    def has_target(self):
        return self._tgt


# ---------------------------------------------------------------------------
# Cut-over guidance (go-path runbook)
# ---------------------------------------------------------------------------


class _CutoverEl:
    """Chainable no-op element double for the cut-over render test."""

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        return self

    def tooltip(self, *_a, **_k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _CutoverUi:
    """Minimal NiceGUI stand-in capturing emitted label/notice/section text."""

    def __init__(self):
        self.texts: list[str] = []
        self.icons: list[str] = []

    def card(self, *_a, **_k):
        return _CutoverEl()

    def row(self, *_a, **_k):
        return _CutoverEl()

    def column(self, *_a, **_k):
        return _CutoverEl()

    def space(self, *_a, **_k):
        return _CutoverEl()

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _CutoverEl()

    def icon(self, name="", *_a, **_k):
        if name:
            self.icons.append(str(name))
        return _CutoverEl()

    def badge(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _CutoverEl()

    def button(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _CutoverEl()


def _cutover_summary(ready: bool = True):
    """A minimal ValidationSummary for the cut-over render (drift not needed)."""
    from dsql_migrator.ui.validation import ValidationSummary

    return ValidationSummary(
        total_tables=3,
        matched_tables=3,
        mismatched_tables=0,
        orphan_count=0,
        is_match=ready,
        mode="checksum",
        as_of="just now",
        reconcile_performed=True,
        reconciled_tables=3,
        inconsistent_tables=0,
        missing_on_target=0,
        extra_on_target=0,
        errored_tables=0,
        ready_for_cutover=ready,
    )


def _no_drift():
    from dsql_migrator.ui.validation import DriftDisplay

    return DriftDisplay(
        available=False,
        determinable=False,
        drifted=False,
        watermark_gtid="unavailable",
        current_gtid="unavailable",
        detail="",
        summary="",
    )


def test_cutover_section_full_load_only_has_freeze_no_drain() -> None:
    from dsql_migrator.ui.validation import _render_cutover_section

    ui = _CutoverUi()
    _render_cutover_section(
        ui, _cutover_summary(), _no_drift(), cdc_in_use=False
    )
    blob = " ".join(ui.texts)
    # Full-Load-only runbook: a write-freeze + repoint, and NO CDC drain wording.
    assert "How to cut over" in blob
    assert "Steps to cut over (Full Load)" in blob
    assert "read-only" in blob and "DSQL endpoint" in blob
    assert "lag" not in blob and "Stop CDC" not in blob
    # Rollback anchor is always called out.
    assert "rollback" in blob.lower()


def test_cutover_section_cdc_includes_drain_and_teardown() -> None:
    from dsql_migrator.ui.validation import _render_cutover_section

    ui = _CutoverUi()
    _render_cutover_section(
        ui, _cutover_summary(), _no_drift(), cdc_in_use=True
    )
    blob = " ".join(ui.texts)
    # CDC runbook: drain to zero lag, then tear the pipeline down LAST via the
    # exact Start over option ("Delete all CDC infrastructure").
    assert "Steps to cut over (with CDC)" in blob
    assert "lag" in blob
    assert "Start over" in blob and "Delete all CDC infrastructure" in blob
    assert "rollback" in blob.lower()


def test_cdc_in_use_resolves_from_migration_type() -> None:
    from dsql_migrator.ui.data_migration import MigrationType
    from dsql_migrator.ui.validation import _cdc_in_use

    class _S:
        def __init__(self, mt):
            self.migration_type = mt

    assert _cdc_in_use(_S(MigrationType.FULL_LOAD_AND_CDC)) is True
    assert _cdc_in_use(_S(MigrationType.CDC_ONLY)) is True
    assert _cdc_in_use(_S(MigrationType.FULL_LOAD_ONLY)) is False
    # Unresolvable type degrades to the simpler (Full-Load-only) runbook.
    assert _cdc_in_use(object()) is False


def test_cutover_runner_marks_step_done() -> None:
    # The Cut over step has no job: its runner records the user's acknowledgement
    # by marking WorkflowStep.CUT_OVER DONE on the session's workflow state.
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen
    from dsql_migrator.ui.workflow import WorkflowStep, get_status
    from dsql_migrator.core.models import StepStatus

    store = SessionStore()
    val_store = ValidationStore()
    _content, runner = build_cutover_screen(
        store, "sess-cutover", validation_store=val_store
    )
    session = store.get_or_create("sess-cutover")
    assert get_status(session.workflow, WorkflowStep.CUT_OVER) is StepStatus.NOT_STARTED
    runner()
    assert get_status(session.workflow, WorkflowStep.CUT_OVER) is StepStatus.DONE


def test_connection_prereq_inventory_first() -> None:
    from dsql_migrator.ui.validation import _connection_prerequisite_notices

    notices = _connection_prerequisite_notices(_FakeSession(), inventory_ready=False)
    # Inventory-missing is returned ALONE (source schema unknown until Step 1).
    assert len(notices) == 1 and "Step 1" in notices[0][1]


def test_connection_prereq_shows_both_source_and_target_at_once() -> None:
    from dsql_migrator.ui.validation import _connection_prerequisite_notices

    # Both unverified -> BOTH notices, together (not one-then-the-other).
    both = _connection_prerequisite_notices(
        _FakeSession(src_ok=False, tgt_ok=False), inventory_ready=True
    )
    headers = [h for _t, h, _b in both]
    assert headers == ["Source connection needed", "Target connection needed"]

    # Only target unverified -> just the target notice (symmetric to source).
    only_tgt = _connection_prerequisite_notices(
        _FakeSession(tgt_ok=False), inventory_ready=True
    )
    assert [h for _t, h, _b in only_tgt] == ["Target connection needed"]
    assert "Aurora DSQL" in only_tgt[0][2] and "short-lived" in only_tgt[0][2]


def test_connection_prereq_empty_when_all_ready() -> None:
    from dsql_migrator.ui.validation import _connection_prerequisite_notices

    assert _connection_prerequisite_notices(_FakeSession(), inventory_ready=True) == []


def test_validation_state_run_timing_roundtrip() -> None:
    # mark_run_started -> mark_run_finished records a non-negative elapsed time;
    # a fresh run clears the prior elapsed until it finishes again.
    state = ValidationState()
    assert state.elapsed_seconds is None
    state.mark_run_started()
    assert state.elapsed_seconds is None  # not finished yet
    state.mark_run_finished()
    assert state.elapsed_seconds is not None and state.elapsed_seconds >= 0.0
    # Starting a new run clears the previous elapsed until it finishes.
    state.mark_run_started()
    assert state.elapsed_seconds is None


def test_source_engine_kwargs_pins_session_time_zone_to_utc() -> None:
    # Hardening: every source MySQL engine pins the session to UTC so TIMESTAMP
    # columns (stored UTC, displayed in the session tz) can't drift vs the target's
    # UTC rendering in the checksum. DATETIME is a wall-clock and unaffected.
    from dsql_migrator.core.introspector import source_engine_kwargs

    ca = source_engine_kwargs()["connect_args"]
    assert ca["init_command"] == "SET time_zone = '+00:00'"
    # Still present when the Full Load stream opts into per-socket timeouts.
    ca2 = source_engine_kwargs(read_timeout_seconds=30)["connect_args"]
    assert ca2["init_command"] == "SET time_zone = '+00:00'"


# ---------------------------------------------------------------------------
# Per-table re-check: run guard, render output, and the screen's recheck runner
# ---------------------------------------------------------------------------


def test_validation_run_guard_blocks_rerun_while_a_recheck_runs() -> None:
    # A re-check and a full run share ONE job slot: while a re-check is in flight the
    # shell's Re-run must be disabled, or a full run would orphan the re-check job
    # AND clear the very report it is about to merge into.
    import time

    from dsql_migrator.ui.validation import validation_run_guard_reason

    manager = JobManager()
    state = ValidationState()
    # No re-check in flight -> runnable.
    assert validation_run_guard_reason(manager, state) is None

    release = threading.Event()
    state.start_recheck(["orders"])
    state.job_id = manager.submit(lambda _h: release.wait(5.0))
    # Wait for the job to actually be observable as in-flight.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if manager.get_status(state.job_id).status in ("PENDING", "RUNNING"):
            break
        time.sleep(0.01)
    reason = validation_run_guard_reason(manager, state)
    assert reason is not None and "re-check is running" in reason

    release.set()
    assert manager.wait(state.job_id, timeout=5.0) is True
    # Settled job -> no longer blocking, even before the marker is cleared.
    assert validation_run_guard_reason(manager, state) is None
    # And a cleared marker alone is enough.
    state.finish_recheck()
    assert validation_run_guard_reason(manager, state) is None


class _TableEl(_CutoverEl):
    """A ui.table double supporting the slot/bind chaining _render_tables uses."""

    def add_slot(self, *_a, **_k):
        return _CutoverEl()

    def bind_value(self, *_a, **_k):
        return self


class _RecheckUi(_CutoverUi):
    """A NiceGUI double that also records button callbacks, spinners and tooltips."""

    def __init__(self):
        super().__init__()
        self.buttons: list[tuple[str, object]] = []
        self.spinners = 0

    def button(self, text="", *_a, **kwargs):
        if text:
            self.texts.append(str(text))
        self.buttons.append((str(text), kwargs.get("on_click")))
        return _CutoverEl()

    def spinner(self, *_a, **_k):
        self.spinners += 1
        return _CutoverEl()

    def separator(self, *_a, **_k):
        return _CutoverEl()

    def table(self, *_a, **_k):
        return _TableEl()

    def input(self, *_a, **_k):
        return _TableEl()


def _failing_report_for_render() -> ValidationReport:
    return ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="orders",
                source_row_count=10,
                target_row_count=9,
                row_count_match=False,
                matched=False,
            ),
            TableValidationResult(
                table="customers",
                source_row_count=3,
                target_row_count=1,
                row_count_match=False,
                matched=False,
            ),
        ],
    )


def test_failing_tables_render_recheck_actions_and_invoke_provider() -> None:
    from dsql_migrator.ui.validation import _render_failing_tables

    report = _failing_report_for_render()
    called: list[list[str]] = []
    ui = _RecheckUi()
    _render_failing_tables(
        ui,
        report,
        summarize_validation(report),
        recheck_provider=lambda tables: called.append(list(tables)),
    )
    blob = " ".join(ui.texts)
    # A per-table action for each failing table + a bulk action for the whole set.
    assert "Re-check" in blob
    assert "Re-check all 2 tables" in blob
    labels = [text for text, _cb in ui.buttons]
    assert labels.count("Re-check") == 2

    # The bulk button passes ALL failing table names; a row button passes just its own.
    bulk = next(cb for text, cb in ui.buttons if text.startswith("Re-check all"))
    bulk()
    assert called == [["orders", "customers"]]
    row = next(cb for text, cb in ui.buttons if text == "Re-check")
    row()
    assert called[-1] == ["orders"]


def test_failing_tables_show_busy_row_instead_of_action_while_rechecking() -> None:
    from dsql_migrator.ui.validation import _render_failing_tables

    report = _failing_report_for_render()
    ui = _RecheckUi()
    _render_failing_tables(
        ui,
        report,
        summarize_validation(report),
        recheck_provider=lambda _t: None,
        rechecking_tables=("orders",),
    )
    blob = " ".join(ui.texts)
    assert "Re-checking…" in blob and ui.spinners >= 1
    # Only the OTHER table still offers the per-row action.
    assert [t for t, _cb in ui.buttons].count("Re-check") == 1


def test_failing_tables_omit_recheck_actions_without_a_provider() -> None:
    from dsql_migrator.ui.validation import _render_failing_tables

    report = _failing_report_for_render()
    ui = _RecheckUi()
    _render_failing_tables(ui, report, summarize_validation(report))
    blob = " ".join(ui.texts)
    assert "Re-check" not in blob
    assert ui.spinners == 0


def test_recheck_note_states_which_tables_are_newer_and_when() -> None:
    from dsql_migrator.ui.validation import _render_recheck_note

    ui = _RecheckUi()
    _render_recheck_note(
        ui, ("orders",), datetime(2026, 3, 4, 5, 6, tzinfo=timezone.utc)
    )
    blob = " ".join(ui.texts)
    assert "1 table(s) re-checked at 2026-03-04 05:06 UTC" in blob
    assert "newer than the rest" in blob
    assert "orders" in blob
    # And it says how to get one uniform consistency point again.
    assert "re-run the full validation" in blob

    # Nothing renders for an ordinary single-run report.
    quiet = _RecheckUi()
    _render_recheck_note(quiet, (), None)
    assert quiet.texts == []


def test_recheck_note_collapses_a_long_table_list() -> None:
    from dsql_migrator.ui.validation import _render_recheck_note

    ui = _RecheckUi()
    _render_recheck_note(ui, tuple(f"t{i}" for i in range(9)), None)
    blob = " ".join(ui.texts)
    assert "9 table(s) re-checked at just now" in blob
    assert "and 3 more" in blob  # 6 shown + 3 collapsed


def _fast_sweep_report(
    *, mode: ValidationMode = ValidationMode.CHECKSUM, reconciled: bool = False
) -> ValidationReport:
    """A report where 'customers' passed by ROW COUNT only (fast sweep skipped deep).

    'orders' carries the run's deep-check evidence (a reconcile result when
    ``reconciled``), so report_run_options can recover what the run actually ran.
    """
    orders = TableValidationResult(
        table="orders",
        source_row_count=10,
        target_row_count=10,
        row_count_match=True,
        matched=True,
        checksum_match=True if mode is ValidationMode.CHECKSUM else None,
        reconcile=(
            ReconcileResult(
                pk_column="id",
                source_count=10,
                target_count=10,
                missing_on_target=0,
                extra_on_target=0,
                consistent=True,
            )
            if reconciled
            else None
        ),
    )
    customers = TableValidationResult(
        table="customers",
        source_row_count=3,
        target_row_count=3,
        row_count_match=True,
        matched=True,
        deep_checks_skipped=True,  # fast sweep: counts agreed, deep checks skipped
    )
    return ValidationReport.build(mode=mode, items=[orders, customers])


def test_deep_recheck_adds_checks_only_when_a_deeper_check_exists() -> None:
    from dsql_migrator.ui.validation import deep_recheck_adds_checks

    # CHECKSUM mode -> a checksum can be run for the count-only table.
    assert deep_recheck_adds_checks(_fast_sweep_report()) is True
    # ROW_COUNT mode but reconciliation ran -> a record reconciliation can be run.
    assert (
        deep_recheck_adds_checks(
            _fast_sweep_report(mode=ValidationMode.ROW_COUNT, reconciled=True)
        )
        is True
    )
    # ROW_COUNT mode, no reconciliation -> nothing deeper exists; re-checking would
    # repeat the identical count comparison, so the action must be withheld.
    assert (
        deep_recheck_adds_checks(_fast_sweep_report(mode=ValidationMode.ROW_COUNT))
        is False
    )


def test_tables_offer_deep_check_for_count_only_tables() -> None:
    from dsql_migrator.ui.validation import _render_tables

    report = _fast_sweep_report()
    called: list[list[str]] = []
    ui = _RecheckUi()
    _render_tables(ui, report, recheck_provider=lambda t: called.append(list(t)))
    blob = " ".join(ui.texts)
    assert "verified by row count only" in blob
    assert "Deep-check 1 count-only table(s)" in blob
    # It re-checks exactly the count-only tables, not the whole report.
    next(cb for text, cb in ui.buttons if text.startswith("Deep-check"))()
    assert called == [["customers"]]


def test_tables_withhold_deep_check_when_nothing_deeper_can_run() -> None:
    from dsql_migrator.ui.validation import _render_tables

    ui = _RecheckUi()
    _render_tables(
        ui,
        _fast_sweep_report(mode=ValidationMode.ROW_COUNT),
        recheck_provider=lambda _t: None,
    )
    blob = " ".join(ui.texts)
    # No no-op button; the honest fallback advice stands instead.
    assert "Deep-check" not in blob
    assert "turn off " in blob and "re-run" in blob


def test_tables_show_busy_while_deep_checking() -> None:
    from dsql_migrator.ui.validation import _render_tables

    ui = _RecheckUi()
    _render_tables(
        ui,
        _fast_sweep_report(),
        recheck_provider=lambda _t: None,
        rechecking_tables=("customers",),
    )
    blob = " ".join(ui.texts)
    assert "Deep-checking…" in blob and ui.spinners >= 1
    assert "Deep-check 1" not in blob


def test_tables_have_no_recheck_action_without_a_provider() -> None:
    from dsql_migrator.ui.validation import _render_tables

    ui = _RecheckUi()
    _render_tables(ui, _fast_sweep_report())
    blob = " ".join(ui.texts)
    # A passing table never grows a button on its own (only the fast-sweep footnote
    # offers one, and only when a provider is supplied).
    assert "Deep-check" not in blob
    assert ui.spinners == 0


def test_passing_tables_get_no_recheck_button_in_the_failing_section() -> None:
    from dsql_migrator.ui.validation import _render_failing_tables

    # An all-passing report renders NO "tables needing attention" section at all, so
    # a clean run shows no re-check affordance anywhere in it.
    ui = _RecheckUi()
    _render_failing_tables(
        ui,
        _fast_sweep_report(),
        summarize_validation(_fast_sweep_report()),
        recheck_provider=lambda _t: None,
    )
    assert ui.texts == []
    assert ui.buttons == []


def _build_screen_for_recheck(*, target_verified: bool = True):
    """Build the validation screen with a fake validator that returns fixed items.

    Returns ``(content, runner, state, manager, fake)`` so a test can drive the
    re-check through the real screen closures without a database.
    """
    from dsql_migrator.core.models import AssessmentReport, TargetInventory
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore

    session_id = "s-recheck"
    store = SessionStore()
    eval_store = EvaluationStore()
    migration_store = DataMigrationStore()
    validation_store = ValidationStore()
    manager = JobManager()

    session = store.get_or_create(session_id)
    session.set_source(
        SourceConnectionConfig(host="db", database="app"), SecretValue("pw")
    )
    session.set_target(
        TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        )
    )
    session.set_source_verified(True)
    session.set_target_verified(target_verified)

    eval_store.get_or_create(session_id).set_result(
        EvaluationResult(
            inventory=_inventory(),
            assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(),
            target_conflicts=[],
        )
    )
    # The fake returns a MATCHING 'orders' result, as a successful re-check would.
    fake = _FakeValidator(
        ValidationReport.build(
            mode=ValidationMode.ROW_COUNT,
            items=[
                TableValidationResult(
                    table="orders",
                    source_row_count=10,
                    target_row_count=10,
                    row_count_match=True,
                    matched=True,
                )
            ],
        )
    )
    content, runner = build_validation_screen(
        store,
        session_id,
        job_manager=manager,
        eval_store=eval_store,
        migration_store=migration_store,
        validation_store=validation_store,
        validator_factory=lambda _i: fake,
    )
    return content, runner, validation_store.get_or_create(session_id), manager, fake


def _recheck_closure(content):
    """Extract the screen's bound ``_recheck`` closure for direct driving.

    ``content`` is the screen's content builder; ``_recheck`` is one of its
    co-closures, reachable through the shared closure cells (the screen returns only
    (content, runner), and the re-check is wired into the render tree via a
    provider). Grabbing it here keeps the test on the REAL closure rather than a
    re-implementation.
    """
    cells = {
        name: cell.cell_contents
        for name, cell in zip(
            content.__code__.co_freevars, content.__closure__ or ()
        )
    }
    return cells["_recheck"]


def test_recheck_merges_into_the_existing_report_keeping_other_tables() -> None:
    content, _runner, state, manager, fake = _build_screen_for_recheck()
    # Seed a completed report where BOTH tables failed.
    state.set_result(_failing_report_for_render())
    recheck = _recheck_closure(content)

    recheck(["orders"])
    assert state.job_id is not None
    assert manager.wait(state.job_id, timeout=5.0) is True

    report = state.result
    assert report is not None
    # 'orders' now matches; 'customers' keeps its earlier failing verdict.
    assert [i.table for i in report.items] == ["orders", "customers"]
    assert report.items[0].matched is True
    assert report.items[1].matched is False
    assert report.is_match is False
    assert state.rechecked_tables == ("orders",)
    # Only the requested table was compared.
    assert fake.calls[-1]["tables"] == ["orders"]


def test_recheck_uses_report_options_and_forces_deep_checks() -> None:
    content, _runner, state, manager, fake = _build_screen_for_recheck()
    # A CHECKSUM report with orphan checking on; the live toggles say otherwise.
    state.set_result(
        ValidationReport.build(
            mode=ValidationMode.CHECKSUM,
            items=[
                TableValidationResult(
                    table="orders",
                    source_row_count=10,
                    target_row_count=9,
                    row_count_match=False,
                    matched=False,
                    reconcile=ReconcileResult(
                        pk_column="id",
                        source_count=10,
                        target_count=9,
                        missing_on_target=1,
                        extra_on_target=0,
                        consistent=False,
                    ),
                )
            ],
            orphan_check_performed=True,
        )
    )
    state.mode = ValidationMode.ROW_COUNT  # live toggles differ from the report
    state.reconcile = False
    state.check_orphans = False
    state.deep_only_on_count_mismatch = True

    _recheck_closure(content)(["orders"])
    assert manager.wait(state.job_id, timeout=5.0) is True

    call = fake.calls[-1]
    # The REPORT's options are reproduced, not the live toggles...
    assert call["mode"] is ValidationMode.CHECKSUM
    assert call["reconcile"] is True
    assert call["check_orphans"] is True
    # ...and the fast sweep is forced OFF (this table is known to differ, so its
    # deep checks are exactly what we want to run).
    assert call["deep_only_on_count_mismatch"] is False


def test_recheck_does_not_touch_the_step_status() -> None:
    from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status
    from dsql_migrator.ui.session import SessionStore  # noqa: F401

    content, _runner, state, manager, _fake = _build_screen_for_recheck()
    state.set_result(_failing_report_for_render())
    # Put the step where a finished run leaves it.
    cells = {
        name: cell.cell_contents
        for name, cell in zip(
            content.__code__.co_freevars, content.__closure__ or ()
        )
    }
    session = cells["session"]
    session.set_workflow(
        with_status(session.workflow, WorkflowStep.VALIDATION, StepStatus.DONE)
    )
    _recheck_closure(content)(["orders"])
    # DONE throughout: flipping to IN_PROGRESS would hide the whole report and lock
    # the shell's Re-run over a perfectly readable result.
    assert get_status(session.workflow, WorkflowStep.VALIDATION) is StepStatus.DONE
    assert manager.wait(state.job_id, timeout=5.0) is True
    assert get_status(session.workflow, WorkflowStep.VALIDATION) is StepStatus.DONE


def test_recheck_blocks_on_an_unverified_target_without_touching_the_report() -> None:
    # DSQL access is a short-lived IAM token, so a report that validated fine an hour
    # ago can face an expired target. The re-check must fail fast with its OWN error
    # (never "Validation failed") and leave the existing report intact.
    content, _runner, state, manager, _fake = _build_screen_for_recheck(
        target_verified=False
    )
    seeded = _failing_report_for_render()
    state.set_result(seeded)
    _recheck_closure(content)(["orders"])
    assert state.job_id is None  # no job submitted
    assert state.recheck_error is not None
    assert "Target connection is not verified" in state.recheck_error
    assert state.error is None  # NOT reported as a full-run failure
    assert state.result is seeded  # report untouched


def test_recheck_is_a_noop_without_a_report_or_tables() -> None:
    content, _runner, state, _manager, _fake = _build_screen_for_recheck()
    recheck = _recheck_closure(content)
    # No report to merge into.
    recheck(["orders"])
    assert state.job_id is None
    # A report but an empty request.
    state.set_result(_failing_report_for_render())
    recheck([])
    assert state.job_id is None


def test_recheck_reports_unknown_tables_instead_of_validating_everything() -> None:
    content, _runner, state, _manager, _fake = _build_screen_for_recheck()
    state.set_result(
        ValidationReport.build(
            mode=ValidationMode.ROW_COUNT,
            items=[
                TableValidationResult(
                    table="dropped_table",
                    source_row_count=1,
                    target_row_count=0,
                    row_count_match=False,
                    matched=False,
                )
            ],
        )
    )
    _recheck_closure(content)(["dropped_table"])
    # Not in the inventory any more -> a clear message, and crucially NO job that
    # would silently re-validate the whole scope instead.
    assert state.job_id is None
    assert state.recheck_error is not None
    assert "no longer in the source inventory" in state.recheck_error


def test_validation_sig_changes_when_a_recheck_merges() -> None:
    # A merge does not change the run's completed_at, so the persistence signature
    # must fold in the re-check time or a merged report would never be saved (the
    # reconnect would restore the PRE-re-check verdict).
    from dsql_migrator.ui.session_persistence import _validation_sig

    state = ValidationState()
    state.set_result(_failing_report_for_render())
    state.mark_run_finished()
    before = _validation_sig(state)
    state.merge_recheck_result(
        [
            TableValidationResult(
                table="orders",
                source_row_count=10,
                target_row_count=10,
                row_count_match=True,
                matched=True,
            )
        ]
    )
    assert _validation_sig(state) != before


def test_snapshot_roundtrip_preserves_recheck_marks() -> None:
    # The "these rows are newer" disclosure must survive a reconnect, or a merged
    # report would silently read as one uniform comparison.
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.session_persistence import apply_session_snapshot

    state = ValidationState()
    state.set_result(_failing_report_for_render())
    state.merge_recheck_result(
        [
            TableValidationResult(
                table="orders",
                source_row_count=10,
                target_row_count=10,
                row_count_match=True,
                matched=True,
            )
        ]
    )
    snapshot = SessionSnapshot(
        session_id="s",
        validation_report=state.result,
        validation_completed_at=state.completed_at,
        validation_rechecked_tables=list(state.rechecked_tables),
        validation_rechecked_at=state.rechecked_at,
    )
    # Survives serialization (the snapshot is persisted as JSON).
    revived = SessionSnapshot.model_validate_json(snapshot.model_dump_json())
    assert revived.validation_rechecked_tables == ["orders"]

    fresh = ValidationState()
    apply_session_snapshot(
        revived,
        _RestoreSession(),
        _RestoreEvalState(),
        _RestoreConvState(),
        _RestoreMigrationState(),
        validation_state=fresh,
    )
    assert fresh.rechecked_tables == ("orders",)
    assert fresh.rechecked_at == state.rechecked_at
    assert fresh.result is not None and fresh.result.items[0].matched is True


class _RestoreSession:
    """Minimal session double for the snapshot-restore path."""

    def __init__(self):
        from dsql_migrator.core.models import WorkflowState

        self.workflow = WorkflowState()
        self.ai_assist = None

    def set_workflow(self, workflow):
        self.workflow = workflow

    def set_active_view(self, _view):
        pass

    def set_migration_type(self, _t):
        pass


class _RestoreEvalState:
    result = None

    def set_result(self, _r):
        pass


class _RestoreConvState:
    generated_node_ids = None
    ticked_node_ids = None
    edited_target_ddls: dict = {}


class _RestoreMigrationState:
    """Migration-state double accepting the restore path's setters."""

    def __init__(self):
        from dsql_migrator.core.models import TableSelection as _TS
        from dsql_migrator.ui.data_migration import MigrationType

        self.job_id = None
        self.selection = _TS()
        self.selection_touched = False
        self.active_substep = None
        self.migration_type = MigrationType.FULL_LOAD_ONLY

    def bind_session(self, _s):
        pass

    def set_cdc_start_position(self, **_k):
        pass

    def set_cdc_start_mode(self, _m):
        pass

    def set_cdc_lob_exclusion(self, *_a):
        pass


def test_apply_column_exclusions_drops_excluded_but_keeps_pk() -> None:
    # Migration-excluded columns (CDC oversized-LOB exclusion) must be dropped from
    # the checksum's column set -- they're not on the target, so comparing them is a
    # false failure. A PK is never dropped (it anchors each row) even if listed.
    from dsql_migrator.core.models import ColumnDef, TableDef
    from dsql_migrator.ui.validation import _apply_column_exclusions

    t = TableDef(
        name="products",
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="tags", mysql_type="json"),
            ColumnDef(name="notes", mysql_type="longtext"),
        ],
        primary_key=["id"],
    )
    (out,) = _apply_column_exclusions([t], {"products": {"notes"}})
    assert [c.name for c in out.columns] == ["id", "tags"]  # 'notes' excluded

    # No exclusions -> the SAME objects pass through untouched (no needless copies).
    assert _apply_column_exclusions([t], {}) == [t]
    assert _apply_column_exclusions([t], {"other": {"x"}})[0] is t

    # A PK listed for exclusion is still kept.
    (out2,) = _apply_column_exclusions([t], {"products": {"id", "notes"}})
    assert "id" in [c.name for c in out2.columns]
    assert "notes" not in [c.name for c in out2.columns]


# ---------------------------------------------------------------------------
# Cancel copy: the stop is cooperative, so the running panel must say WHAT it is
# waiting on instead of implying an immediate halt.
# ---------------------------------------------------------------------------


class _Tip:
    """A stand-in for NiceGUI's tooltip element: records every text it is given."""

    def __init__(self, owner, text="") -> None:
        self._owner = owner
        self.is_deleted = False
        self.text = ""
        if text:
            self.set_text(text)

    def set_text(self, text="", *_a, **_k):
        self.text = str(text)
        self._owner.tooltips.append(str(text))
        return self

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        return self


class _CopyUi:
    """A minimal NiceGUI stand-in that records every emitted string.

    ``_render_in_progress`` only needs row/column/label/button/spinner/
    linear_progress; ``render_notice`` adds icon/badge. Buttons record their
    tooltips separately so a test can assert the disabled-state explanation.
    """

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.tooltips: list[str] = []

    class _El:
        def __init__(self, owner) -> None:
            self._owner = owner
            self.text = ""
            self.enabled = True
            self.visible = True
            self.value = None
            self.is_deleted = False

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, text="", *_a, **_k):
            # NiceGUI's Element.tooltip() creates the Tooltip and returns ``self`` (the
            # OWNING element) for chaining -- it does NOT hand back the tooltip. This
            # double used to return a _Tip, which made ``x = btn.tooltip("")`` followed by
            # ``x.set_text(...)`` look like it retargeted the tooltip when in the real UI
            # it rewrites the BUTTON'S LABEL. That fiction hid a visible bug (the whole
            # tooltip sentence rendered as the button caption). Mirroring the real return
            # value means such a call now shows up as a label change, where it is caught.
            if text:
                self._owner.tooltips.append(str(text))
            return self

        def set_text(self, text="", *_a, **_k):
            self.text = str(text)
            self._owner.texts.append(str(text))
            return self

        def set_value(self, value=None, *_a, **_k):
            self.value = value
            return self

        def set_visibility(self, visible=True, *_a, **_k):
            self.visible = bool(visible)
            return self

        def set_enabled(self, value=True, *_a, **_k):
            self.enabled = bool(value)
            return self

        def disable(self, *_a, **_k):
            self.enabled = False
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

    def tooltip(self, text="", *_a, **_k):
        # ui.tooltip() (the standalone factory) DOES return the tooltip element, which is
        # the supported way to get a handle whose text can be swapped later. Distinct from
        # Element.tooltip() above, which returns the owning element.
        return _Tip(self, text)

    def _record(self, text):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def label(self, text="", *_a, **_k):
        return self._record(text)

    def badge(self, text="", *_a, **_k):
        return self._record(text)

    def button(self, text="", *_a, **_k):
        return self._record(text)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def spinner(self, *_a, **_k):
        return self._El(self)

    def icon(self, *_a, **_k):
        return self._El(self)

    def linear_progress(self, *_a, **_k):
        return self._El(self)

    def timer(self, *_a, **_k):
        return self._El(self)

    def body(self) -> str:
        return "\n".join(self.texts)

    def tooltip_body(self) -> str:
        return "\n".join(self.tooltips)


class _PanelUi(_CopyUi):
    """Records the panel's live elements so a test can read their FINAL state.

    The panel is built once with placeholder copy and then updated in place, so the
    plain text recorder accumulates both the placeholder and the update. What matters
    is what the user ends up seeing, i.e. each element's last value.
    """

    def __init__(self) -> None:
        super().__init__()
        self.labels: list = []
        self.buttons: list = []
        self.bars: list = []

    def label(self, text="", *_a, **_k):
        element = super().label(text)
        if not isinstance(element, _Tip):
            self.labels.append(element)
        return element

    def button(self, text="", *_a, **_k):
        element = super().button(text)
        self.buttons.append(element)
        return element

    def linear_progress(self, *_a, **_k):
        element = self._El(self)
        self.bars.append(element)
        return element

    def final_text(self) -> str:
        """Every element's CURRENT text (what is actually on screen)."""
        return "\n".join(e.text for e in self.labels if getattr(e, "text", ""))


def _render_running(*, cancel_requested: bool):
    from dsql_migrator.ui.validation import ValidationState, _render_in_progress

    state = ValidationState()
    state.set_progress("orders", 2, 7)
    state.cancel_requested = cancel_requested

    ui = _PanelUi()
    _render_in_progress(ui, JobManager(), object(), state, lambda: None)
    return ui


def test_running_panel_before_cancel_reassures_it_can_be_left_alone() -> None:
    ui = _render_running(cancel_requested=False)
    body = ui.body()
    assert "Cancel validation" in body
    assert "safe to leave running" in body
    # The pre-cancel tooltip sets the expectation BEFORE the click: already-running
    # tables finish, so the stop is not instant.
    assert "already running finish first" in ui.tooltip_body()


def test_stopping_panel_says_what_the_cancel_is_waiting_for() -> None:
    """A bare "Stopping…" read as a cancel that had been ignored.

    The stop is cooperative and only polled before each table and every few thousand
    merged rows, so a COUNT(*)/checksum already running on a large table finishes
    first -- minutes, with concurrent tables doing the same. The copy has to say that,
    or the user retries or assumes the button is broken.
    """
    ui = _render_running(cancel_requested=True)
    body = ui.final_text()  # what is on screen after the in-place update

    # The label names what is being awaited, not just that it is stopping.
    assert "waiting for the in-flight table comparisons to finish" in body
    # The pre-cancel reassurance must be GONE -- it now describes the wind-down.
    assert "safe to leave running" not in body
    assert "Cancelling" in body
    # And it explains the delay: an in-flight query cannot be interrupted.
    assert "cannot be interrupted" in body
    # Read-only is still stated, so a slow cancel never reads as risky.
    assert "reads both engines" in body


def test_stopping_tooltip_explains_the_delay_and_that_nothing_is_written() -> None:
    tooltips = _render_running(cancel_requested=True).tooltip_body()
    assert "Cancel already requested" in tooltips
    # Names the two prompt cases and the one slow case, so the wait is predictable.
    assert "not yet started are skipped" in tooltips
    assert "no interruption point" in tooltips
    assert "read-only" in tooltips


def test_stopping_state_hides_the_progress_bar() -> None:
    # The determinate bar tracks tables COMPLETING; during a wind-down it would keep
    # advancing and contradict "Cancelling". It is created once (so the poll never has
    # to rebuild this region) and hidden instead.
    for cancel_requested, expect_visible in ((False, True), (True, False)):
        ui = _render_running(cancel_requested=cancel_requested)
        assert len(ui.bars) == 1, "one bar, created once -- not per poll tick"
        assert ui.bars[0].visible is expect_visible, cancel_requested


def test_cancel_button_keeps_its_name_while_stopping() -> None:
    # The button used to relabel itself "Stopping…", which duplicated the adjacent
    # status label and left nothing naming the requested action. It stays "Cancel
    # validation" (disabled) -- the same shape as Full Load's "Stop Full Load".
    body = _render_running(cancel_requested=True).body()
    assert "Cancel validation" in body
    assert body.count("Stopping…") == 1  # the status label only, not the button


def test_object_picker_shortcuts_match_the_other_screens_convention() -> None:
    """The bulk include/exclude shortcuts must look like every other object picker.

    Schema Conversion and Data Migration both render their "Select all"/"Unselect all"
    with the same props -- primary + done_all for the affirmative action, grey-7 +
    remove_done for the clearing one. Validation's equivalents carried only
    "flat dense no-caps size=sm", so beside those screens they lost both the color and
    the icon. Asserted against the OTHER screens' source, so changing the convention in
    one place cannot silently leave Validation behind.
    """
    import inspect

    from dsql_migrator.ui import data_migration, schema_conversion, validation

    affirmative = "flat dense no-caps size=sm color=primary icon=done_all"
    clearing = "flat dense no-caps size=sm color=grey-7 icon=remove_done"

    # The convention is what the other two screens actually use.
    for module in (schema_conversion, data_migration):
        src = inspect.getsource(module)
        assert affirmative in src, module.__name__
        assert clearing in src, module.__name__

    # Validation now follows it.
    validation_src = inspect.getsource(validation)
    assert affirmative in validation_src
    assert clearing in validation_src


def test_object_picker_shortcuts_are_disabled_while_a_run_is_in_flight() -> None:
    # Restyling must not change the gating: both shortcuts stay locked during a run,
    # and each is only enabled when it would actually do something.
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.validation import ValidationState, _render_object_filter

    class _Btn(_CopyUi._El):
        def __init__(self, owner, label) -> None:
            super().__init__(owner)
            self.label = label
            self.enabled = True

        def set_enabled(self, value, *_a, **_k):
            self.enabled = bool(value)
            return self

    class _Ui(_CopyUi):
        def __init__(self) -> None:
            super().__init__()
            self.buttons: list = []

        def button(self, text="", *_a, **_k):
            self.texts.append(str(text))
            element = _Btn(self, str(text))
            self.buttons.append(element)
            return element

        def separator(self, *_a, **_k):
            return self._El(self)

        def space(self, *_a, **_k):
            return self._El(self)

        def element(self, *_a, **_k):
            return self._El(self)

    def _render(*, status, excluded):
        state = ValidationState()
        state.table_exclude = set(excluded)
        ui = _Ui()
        _render_object_filter(ui, ["app.a", "app.b"], state, status, lambda: None)
        return {b.label: b.enabled for b in ui.buttons if "all" in b.label}

    running = _render(status=StepStatus.IN_PROGRESS, excluded=["app.a"])
    assert running["Include all"] is False and running["Exclude all"] is False

    idle = _render(status=StepStatus.NOT_STARTED, excluded=["app.a"])
    assert idle["Include all"] is True  # something to re-include
    assert idle["Exclude all"] is True  # something still included

    nothing_excluded = _render(status=StepStatus.NOT_STARTED, excluded=[])
    assert nothing_excluded["Include all"] is False  # already all included


# ---------------------------------------------------------------------------
# drift_verdict -- drift only threatens a cut-over when NO CDC is carrying the
# post-snapshot writes, so the notice must be read through the migration type.
# ---------------------------------------------------------------------------


def _drifted_display(
    *, drifted=True, determinable=True, available=True, basis="gtid"
):
    from dsql_migrator.ui.validation import DriftDisplay

    return DriftDisplay(
        available=available,
        determinable=determinable,
        drifted=drifted,
        watermark_gtid="uuid:1-5",
        current_gtid="uuid:1-9" if drifted else "uuid:1-5",
        detail="detail",
        summary="summary",
        basis=basis if determinable else "",
        watermark_binlog="mysql-bin.000004:1120",
        current_binlog="mysql-bin.000004:8450" if drifted else "mysql-bin.000004:1120",
    )


def test_drift_with_cdc_is_info_not_a_warning() -> None:
    """An advancing source under live CDC is the NORMAL state, so it must stay calm.

    The section used to state the bare fact regardless of migration type, so a healthy
    CDC run was told its source "has advanced since the snapshot" -- which reads as a
    problem. Per the design system's severity calibration an expected state is info.
    """
    from dsql_migrator.ui.validation import drift_verdict

    tone, header, body = drift_verdict(_drifted_display(), cdc_in_use=True)
    assert tone == "info"
    assert "expected with CDC" in header
    # Says why it is fine AND what the pre-cut-over step is.
    assert "replicating" in body
    assert "zero lag" in body


def test_drift_without_cdc_warns_that_those_rows_are_not_on_the_target() -> None:
    # Full-Load-only: nothing is carrying post-snapshot writes across, so cutting over
    # now would lose them. Real but non-blocking -> warning, not error.
    from dsql_migrator.ui.validation import drift_verdict

    tone, header, body = drift_verdict(_drifted_display(), cdc_in_use=False)
    assert tone == "warning"
    assert "not replicated" in header
    assert "NOT on the target" in body
    assert "lose them" in body


def test_no_drift_is_success_regardless_of_cdc() -> None:
    from dsql_migrator.ui.validation import drift_verdict

    for cdc in (True, False):
        tone, header, _body = drift_verdict(
            _drifted_display(drifted=False), cdc_in_use=cdc
        )
        assert tone == "success", cdc
        assert "No source changes" in header


def test_undeterminable_drift_is_info_and_says_what_to_do() -> None:
    # No GTID -> the tool cannot judge. Alarming here would be wrong (nothing is known
    # to be broken), but it must still name the definitive check.
    from dsql_migrator.ui.validation import drift_verdict

    tone, header, body = drift_verdict(
        _drifted_display(determinable=False), cdc_in_use=False
    )
    assert tone == "info"
    assert "Could not tell whether the source changed" == header
    assert "Freeze source writes and re-validate" in body


def test_no_watermark_is_described_as_a_live_comparison_not_a_missing_gtid() -> None:
    """No watermark and no GTID are different causes and must not share one message.

    Without a watermark there is no consistency point to have drifted FROM -- the run
    compared against the live source. Blaming a missing GTID would misdescribe it.
    """
    from dsql_migrator.ui.validation import drift_verdict

    tone, header, body = drift_verdict(
        _drifted_display(available=False, determinable=False), cdc_in_use=False
    )
    assert tone == "info"
    assert "live source" in header
    assert "no export watermark" in body
    assert "GTID" not in body  # the cause is the watermark, not the GTID


def test_drift_section_keeps_the_gtids_available_but_collapsed() -> None:
    """The GTID pair is diagnostic, not the primary content.

    Its values cannot be read as "how far behind" (a GTID is not a distance) and every
    actionable conclusion is in the notice -- but it must remain reachable for an audit
    trail, so it is collapsed rather than removed.
    """
    from dsql_migrator.ui.validation import _render_drift

    expansions: list[str] = []
    tables: list[list] = []

    class _Ui(_CopyUi):
        def expansion(self, text="", *_a, **_k):
            expansions.append(str(text))
            return self._El(self)

        def table(self, *_a, columns=None, rows=None, **_k):
            tables.append(rows or [])
            return self._El(self)

    ui = _Ui()
    _render_drift(ui, _drifted_display(), cdc_in_use=True)

    # The verdict is rendered as a notice (its header text lands in the recorder).
    assert any("expected with CDC" in t for t in ui.texts)
    # The coordinates live behind one collapsed section, and are still present.
    assert any("replication coordinates" in t for t in expansions)
    fields = {row["field"] for row in tables[0]}
    assert {"Compared using", "At snapshot", "Now"} <= fields


def test_cdc_in_use_is_actually_threaded_from_the_screen_to_the_drift_section() -> None:
    """The verdict is only correct if the screen passes the real migration type down.

    drift_verdict can be perfect and the section still wrong if cdc_in_use never
    reaches it, so assert the whole chain: the CDC-only / Full-Load-and-CDC types
    resolve to True, and _render_result forwards it to the drift notice.
    """
    import inspect

    from dsql_migrator.ui.data_migration import MigrationType
    from dsql_migrator.ui.validation import _cdc_in_use, _render_result

    class _Session:
        def __init__(self, migration_type) -> None:
            self.migration_type = migration_type

    assert _cdc_in_use(_Session(MigrationType.CDC_ONLY)) is True
    assert _cdc_in_use(_Session(MigrationType.FULL_LOAD_AND_CDC)) is True
    assert _cdc_in_use(_Session(MigrationType.FULL_LOAD_ONLY)) is False
    assert _cdc_in_use(object()) is False  # unresolvable -> the safer Full-Load path

    # _render_result accepts it and hands it to the drift section.
    assert "cdc_in_use" in inspect.signature(_render_result).parameters
    body = inspect.getsource(_render_result)
    assert "_render_drift(ui, drift, cdc_in_use=cdc_in_use)" in body

    # ...and the screen supplies it from the session (not a hardcoded default).
    screen_src = inspect.getsource(inspect.getmodule(_render_result))
    assert "cdc_in_use=_cdc_in_use(session)" in screen_src


def test_drift_panel_works_on_a_source_without_gtid() -> None:
    """The RDS MySQL 8.0 path must produce a real verdict, not "unavailable".

    GTID cannot be enabled on RDS MySQL 8.0, so before the binlog fallback this panel
    showed two "unavailable" rows and "could not be determined" on EVERY run -- the
    section could never answer its own question on the tool's primary source.
    """
    from dsql_migrator.core.models import DriftReport
    from dsql_migrator.ui.validation import drift_verdict, format_drift

    drift = DriftReport(
        watermark_gtid=None,
        current_gtid=None,
        drifted=True,
        detail=(
            "Source advanced since the snapshot (binlog position moved from "
            "mysql-bin.000004:1120 to mysql-bin.000004:8450)."
        ),
        basis="binlog",
        watermark_binlog="mysql-bin.000004:1120",
        current_binlog="mysql-bin.000004:8450",
    )
    display = format_drift(_report(drift=drift))

    # Determinable DESPITE having no GTID -- that is the whole point of the fallback.
    assert display.determinable is True
    assert display.basis == "binlog"
    assert display.drifted is True
    # The summary names the evidence actually used, not a GTID it never had.
    assert "binlog position moved" in display.summary
    assert "GTID" not in display.summary

    # And the verdict still reads through the CDC lens.
    assert drift_verdict(display, cdc_in_use=True)[0] == "info"
    assert drift_verdict(display, cdc_in_use=False)[0] == "warning"


def test_drift_detail_leads_with_the_coordinate_that_was_compared() -> None:
    # Listing GTIDs first when the verdict came from binlog positions put two
    # "unavailable" rows at the top and buried the evidence that was actually used.
    from dsql_migrator.ui.validation import _render_drift

    tables: list[list] = []

    class _Ui(_CopyUi):
        def expansion(self, text="", *_a, **_k):
            return self._El(self)

        def table(self, *_a, columns=None, rows=None, **_k):
            tables.append(rows or [])
            return self._El(self)

    _render_drift(
        _Ui(), _drifted_display(basis="binlog"), cdc_in_use=False
    )
    rows = tables[0]
    assert rows[0]["field"] == "Compared using"
    assert "Binlog position" in rows[0]["value"]
    assert "GTID not enabled" in rows[0]["value"]  # says WHY, so it reads as normal
    assert rows[1]["value"] == "mysql-bin.000004:1120"  # at snapshot
    assert rows[2]["value"] == "mysql-bin.000004:8450"  # now


def test_drift_section_header_avoids_replication_jargon() -> None:
    """"Drift since snapshot" was jargon on both words.

    "Drift" is a replication term and "snapshot" is the tool's internal name for the
    watermark, so the old title did not say what the section answers.
    """
    import inspect

    from dsql_migrator.ui import validation

    src = inspect.getsource(validation._render_result)
    assert 'title="Source changes since the comparison"' in src
    assert 'title="Drift since snapshot"' not in src


def test_poll_updates_the_panel_in_place_so_a_hovered_tooltip_survives() -> None:
    """A poll tick must NOT recreate the Cancel button, or its tooltip flickers.

    A q-tooltip is a CHILD of its anchor, so re-rendering the panel destroys the element
    the pointer is over and Quasar closes the tooltip; it only reopens on a fresh hover.
    At the 0.5s validation poll that made the tooltip unreadable. The tick therefore
    updates the existing elements (set_text / set_enabled / set_value) and re-arms its
    own timer instead of calling refresh().
    """
    from dsql_migrator.ui.validation import ValidationState, _render_in_progress

    timers: list = []
    refreshes = {"n": 0}

    class _Ui(_PanelUi):
        def timer(self, interval, callback=None, *_a, **_k):
            timers.append((interval, callback))
            return self._El(self)

    class _RunningJobs:
        """A job manager whose job stays RUNNING, so the poll takes the live path."""

        status = "RUNNING"

        def get_status(self, _job_id):
            return type("_Job", (), {"status": self.status})()

        def is_cancel_requested(self, _job_id):
            return False

    state = ValidationState()
    state.job_id = "job-1"
    state.set_progress("orders", 2, 7)

    ui = _Ui()
    _render_in_progress(
        ui,
        _RunningJobs(),
        object(),
        state,
        lambda: refreshes.__setitem__("n", refreshes["n"] + 1),
    )

    buttons_after_build = list(ui.buttons)
    labels_after_build = list(ui.labels)
    assert timers, "the running panel must arm a poll timer"

    # Fire the poll a few times, as the running job would.
    for _ in range(3):
        interval, callback = timers[-1]
        callback()

    # No element was recreated -> a hovered tooltip is never destroyed.
    assert ui.buttons == buttons_after_build
    assert ui.labels == labels_after_build
    # ...and the panel was NOT re-rendered wholesale.
    assert refreshes["n"] == 0
    # The poll keeps itself alive by re-arming.
    assert len(timers) >= 4


def test_cancel_tooltip_text_is_swapped_not_recreated() -> None:
    # One tooltip element whose text changes, rather than a new tooltip per state: a new
    # element would again mean the hovered one is gone.
    #
    # The handle must come from ui.tooltip() (the standalone factory, which returns the
    # TOOLTIP), not from button.tooltip() (which returns the BUTTON). Tracking the factory
    # is what keeps this test honest: hooking button.tooltip would pass even if the code
    # went back to swapping the button's own label.
    from dsql_migrator.ui.validation import ValidationState, _render_in_progress

    tips: list = []

    class _Ui(_PanelUi):
        def tooltip(self, text="", *_a, **_k):
            tip = super().tooltip(text)
            tips.append(tip)
            return tip

    state = ValidationState()
    state.set_progress("orders", 2, 7)
    ui = _Ui()
    _render_in_progress(ui, JobManager(), object(), state, lambda: None)

    # Exactly one tooltip element for the Cancel button.
    assert len(tips) == 1
    # And it ends up holding the running-state wording.
    assert "Tables not yet started are skipped" in tips[0].text
    # THE regression: that wording must live in the tooltip ONLY -- never become the
    # button's caption. (It did: the whole sentence rendered as the button label and blew
    # the row out to the full panel width.) The button's own text -- its creation label
    # plus any later set_text -- is recorded in `texts`, so the tooltip copy must be absent
    # from every one of them.
    assert "Cancel validation" in ui.texts
    for rendered in ui.texts:
        assert "Tables not yet started are skipped" not in rendered, rendered
        assert "Cancel already requested" not in rendered, rendered


# ---------------------------------------------------------------------------
# Attributing a target deficit to rows the migration permanently dropped
# ---------------------------------------------------------------------------


def _quarantine_item(*, source: int, target: int, dropped: int):
    from dsql_migrator.core.models import TableValidationResult

    return TableValidationResult(
        table="ecommerce.product_media",
        source_row_count=source,
        target_row_count=target,
        row_count_match=(source == target),
        matched=(source == target),
        rows_quarantined=dropped,
    )


def test_deficit_matching_the_dropped_rows_is_reported_as_explained() -> None:
    """The operator must be told a shortfall is the known quarantine, not new loss.

    Validation had no knowledge of quarantine at all, so a table that dropped a row
    simply read as MISMATCH / "investigate" -- and the manual instructed the operator to
    "cross-check the deficit against the Full Load error log / CDC DLQ" by hand,
    information the tool already had. Worst case that trains people to wave off
    mismatches, which is what Validation exists to catch.
    """
    from dsql_migrator.ui.validation import _failure_reasons

    item = _quarantine_item(source=15, target=14, dropped=1)

    assert item.deficit == 1
    assert item.deficit_explained_by_quarantine is True
    reasons = " ".join(_failure_reasons(item))
    assert "Fully explained" in reasons
    assert "permanently dropped" in reasons
    assert "not new data loss" in reasons
    # Still reports the raw counts -- the attribution adds context, never hides it.
    assert "source 15, target 14" in reasons


def test_a_deficit_larger_than_the_drop_is_only_partly_explained() -> None:
    # The critical guard: 4 rows short with 1 dropped leaves 3 unaccounted for. Calling
    # that "expected" would let real loss through the one check meant to catch it.
    from dsql_migrator.ui.validation import _failure_reasons

    item = _quarantine_item(source=15, target=11, dropped=1)

    assert item.deficit_explained_by_quarantine is False
    reasons = " ".join(_failure_reasons(item))
    assert "Partly explained" in reasons
    assert "3 more are missing" in reasons
    assert "NOT accounted for" in reasons
    assert "Fully explained" not in reasons


def test_a_deficit_with_no_known_drop_stays_unexplained() -> None:
    from dsql_migrator.ui.validation import _failure_reasons

    item = _quarantine_item(source=15, target=14, dropped=0)

    assert item.deficit_explained_by_quarantine is False
    reasons = " ".join(_failure_reasons(item))
    assert "explained" not in reasons.lower()


def test_quarantine_never_flips_a_table_to_matched() -> None:
    # The rows really are absent from the target, so the verdict must keep failing --
    # the attribution explains WHY, it does not excuse it. (A future change that made
    # this "expected" gap pass would silently unlock cut-over on missing data.)
    item = _quarantine_item(source=15, target=14, dropped=1)

    assert item.matched is False
    assert item.row_count_match is False


def test_count_match_despite_a_drop_is_not_claimed_as_explained() -> None:
    # Source drift can offset a drop so the counts happen to agree. There is no deficit
    # to attribute, so nothing may claim the gap is "expected" -- that would be false
    # reassurance about a table whose rows are provably not identical.
    item = _quarantine_item(source=15, target=15, dropped=1)

    assert item.deficit == 0
    assert item.deficit_explained_by_quarantine is False


def test_quarantine_counts_are_attached_to_a_finished_report() -> None:
    """The validator's seam: counts come from the job, not from comparing databases."""
    from dsql_migrator.core.models import ValidationMode, ValidationReport
    from dsql_migrator.core.validator import _with_quarantine_counts

    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            _quarantine_item(source=15, target=14, dropped=0),
            TableValidationResult(
                table="ecommerce.orders", source_row_count=500, target_row_count=500,
                row_count_match=True, matched=True,
            ),
        ],
    )

    updated = _with_quarantine_counts(report, {"ecommerce.product_media": 1})
    by_table = {i.table: i for i in updated.items}

    assert by_table["ecommerce.product_media"].rows_quarantined == 1
    assert by_table["ecommerce.product_media"].deficit_explained_by_quarantine is True
    # A table with no drop is untouched.
    assert by_table["ecommerce.orders"].rows_quarantined == 0
    # No counts to attach -> the report is returned as-is.
    assert _with_quarantine_counts(report, {}) is report
    assert _with_quarantine_counts(report, None) is report


def test_quarantined_rows_by_table_reads_the_job_chunks() -> None:
    # The engine records the drop on the chunk; this is what carries it to Validation.
    # Empty for an absent job (a reconnect -- the counts are not persisted), so the
    # deficit is reported unexplained rather than assumed away.
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import quarantined_rows_by_table

    job = MigrationJob(job_id="j1")
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=1, attempts=1),
        ChunkState(chunk_id="ecommerce.orders", status="DONE", rows_loaded=500,
                   attempts=1),
    ]

    assert quarantined_rows_by_table(job) == {"ecommerce.product_media": 1}
    assert quarantined_rows_by_table(None) == {}
