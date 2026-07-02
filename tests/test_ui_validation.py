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
    )
    display = format_drift(_report(drift=drift))
    assert display.available is True
    assert display.determinable is True
    assert display.drifted is True
    assert display.watermark_gtid == "uuid:1-5"
    assert display.current_gtid == "uuid:1-9"
    assert "advanced since the snapshot" in display.summary


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
