# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Step 1 (Evaluation) screen's NiceGUI-agnostic logic.

These tests cover the parts of the Evaluation screen that do not touch NiceGUI:

- Run orchestration (:func:`run_evaluation`) wiring source introspection,
  compatibility assessment (with effort), and target catalog introspection
  (conflict detection), using injected fakes (no database / no AWS).
- Job-status -> step-status mapping used to drive the workflow transitions.
- Report export serialization (JSON/text) for download (Requirement 8.4).
- Per-session evaluation state and its store (isolation, result/error handoff).
- End-to-end background run through the real :class:`JobManager`, asserting the
  Evaluation step transitions NOT_STARTED -> DONE / FAILED.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    EffortLevel,
    ForeignKeyDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    TargetInventory,
    TargetObjectKind,
    TargetRelation,
    TargetSchemaNode,
)
from dsql_migrator.ui.evaluation import (
    EvaluationInputs,
    EvaluationResult,
    EvaluationState,
    EvaluationStore,
    assessment_download,
    job_status_to_step_status,
    run_evaluation,
    _find_target_conflicts,
)


# ---------------------------------------------------------------------------
# Fakes / fixtures
# ---------------------------------------------------------------------------


class _FakeIntrospector:
    """Returns a canned inventory and records the config it was asked about."""

    def __init__(self, inventory: SourceInventory) -> None:
        self.inventory = inventory
        self.calls: list[SourceConnectionConfig] = []

    def introspect(self, conn: SourceConnectionConfig, *, progress=None) -> SourceInventory:
        self.calls.append(conn)
        if progress is not None:
            for read in range(1, len(self.inventory.tables) + 1):
                progress(read, len(self.inventory.tables))
        return self.inventory


class _FakeTargetBrowser:
    """Returns a canned target inventory and records the config it browsed."""

    def __init__(self, inventory: TargetInventory) -> None:
        self.inventory = inventory
        self.calls: list[TargetConnectionConfig] = []

    def browse(self, conn: TargetConnectionConfig) -> TargetInventory:
        self.calls.append(conn)
        return self.inventory


def _source_config() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="h", port=3306, database="app", username="u")


def _target_config() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )


def _inventory_with_fk() -> SourceInventory:
    # A table whose only finding is a foreign key converts automatically (the FK is
    # advisory/RECOMMENDED, re-created after load), so it is AUTO; a table without a
    # primary key is UNSUPPORTED -- giving a non-trivial report (AUTO + UNSUPPORTED).
    return SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKeyDef(
                        name="fk_customer",
                        columns=["customer_id"],
                        referenced_table="customers",
                        referenced_columns=["id"],
                    )
                ],
            ),
            TableDef(
                name="audit_log",
                columns=[ColumnDef(name="payload", mysql_type="text")],
                primary_key=[],
            ),
        ]
    )


def _empty_target() -> TargetInventory:
    return TargetInventory()


def _target_with_tables(*names: str) -> TargetInventory:
    return TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="public",
                tables=[
                    TargetRelation(
                        schema_name="public", name=name, kind=TargetObjectKind.TABLE
                    )
                    for name in names
                ],
            )
        ]
    )


def _inputs() -> EvaluationInputs:
    return EvaluationInputs(
        source_config=_source_config(),
        source_password=SecretValue("pw"),
        target_config=_target_config(),
        aws_profile=None,
    )


def _source_factory(inventory: SourceInventory):
    introspector = _FakeIntrospector(inventory)

    def make(_password: object) -> _FakeIntrospector:
        return introspector

    return make, introspector


def _target_factory(inventory: TargetInventory):
    browser = _FakeTargetBrowser(inventory)

    def make(_profile: object) -> _FakeTargetBrowser:
        return browser

    return make, browser


# ---------------------------------------------------------------------------
# run_evaluation orchestration
# ---------------------------------------------------------------------------


def test_run_evaluation_introspects_source_and_target_and_assesses() -> None:
    src_factory, introspector = _source_factory(_inventory_with_fk())
    tgt_factory, browser = _target_factory(_empty_target())

    result = run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )

    # Both source and target were introspected with the provided configs.
    assert introspector.calls == [_source_config()]
    assert browser.calls == [_target_config()]
    assert isinstance(result, EvaluationResult)
    # Every object is classified (Property 8): 2 tables -> 2 items.
    assert len(result.assessment.items) == 2
    classifications = {item.classification for item in result.assessment.items}
    # orders has only an advisory FK finding -> AUTO; audit_log has no PK -> UNSUPPORTED.
    assert Classification.AUTO in classifications
    assert Classification.UNSUPPORTED in classifications


def test_run_evaluation_reports_progress_to_callback() -> None:
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())

    events: list[tuple[int, str]] = []

    run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
        progress_cb=lambda pct, msg: events.append((pct, msg)),
    )

    percents = [pct for pct, _ in events]
    # Progress is reported, monotonically non-decreasing, and ends at 100%.
    assert percents, "expected at least one progress report"
    assert percents == sorted(percents)
    assert percents[-1] == 100
    # Each report carries a non-empty phase message.
    assert all(msg for _, msg in events)


def test_run_evaluation_assessment_includes_effort_estimate() -> None:
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())

    result = run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )

    # The FK table is advisory now (Aurora DSQL enforces FKs), so it carries NO required
    # effort; the no-PK table is a real MEDIUM gap. Only the latter counts in the summary.
    efforts = {item.object_name: item.effort for item in result.assessment.items}
    assert efforts["orders"] is None
    assert efforts["audit_log"] is EffortLevel.MEDIUM
    assert result.assessment.effort_summary[EffortLevel.SIMPLE] == 0
    assert result.assessment.effort_summary[EffortLevel.MEDIUM] == 1
    assert result.assessment.effort_summary[EffortLevel.SIGNIFICANT] == 0


def test_run_evaluation_counts_the_target_against_the_database_limits() -> None:
    """The target is read before the assessment, so 700 new tables into a cluster that
    already holds 400 others are reported as over Aurora DSQL's 1,000-table limit."""
    from dsql_migrator.core.models import (
        TargetInventory,
        TargetObjectKind,
        TargetRelation,
        TargetSchemaNode,
    )

    src_factory, _ = _source_factory(SourceInventory(tables=_clean_tables(700)))
    occupied = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="other",
                tables=[
                    TargetRelation(schema_name="other", name=f"x{i}", kind=TargetObjectKind.TABLE)
                    for i in range(400)
                ],
            )
        ]
    )
    tgt_factory, _ = _target_factory(occupied)

    result = run_evaluation(
        _inputs(), introspector_factory=src_factory, target_browser_factory=tgt_factory
    )
    (limit,) = [i for i in result.assessment.items if i.kind == "DATABASE"]
    assert (limit.rule_id, limit.object_name) == ("TABLE_COUNT_LIMIT", "1,100 tables")


def test_run_evaluation_reports_no_conflicts_for_empty_target() -> None:
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())

    result = run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )
    assert result.target_conflicts == []


def test_run_evaluation_flags_objects_existing_on_target() -> None:
    src_factory, _ = _source_factory(_inventory_with_fk())
    # The target already has an "orders" table (case-insensitive match).
    tgt_factory, _ = _target_factory(_target_with_tables("ORDERS"))

    result = run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )
    assert result.target_conflicts == ["orders"]


def test_find_target_conflicts_matches_qualified_schema_and_table() -> None:
    # Source objects are qualified as database.table; a conflict is reported only
    # when the target has the SAME schema and table name (the user's scenario).
    source = SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="app.customers",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ]
    )
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="app",
                tables=[
                    TargetRelation(
                        schema_name="app", name="orders",
                        kind=TargetObjectKind.TABLE,
                    )
                ],
            )
        ]
    )

    # app.orders exists on the target under the same schema -> conflict.
    assert _find_target_conflicts(source, target) == ["app.orders"]


def test_find_target_conflicts_ignores_same_table_in_other_schema() -> None:
    source = SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            )
        ]
    )
    # Same table name but under a DIFFERENT schema -> not a conflict.
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="reporting",
                tables=[
                    TargetRelation(
                        schema_name="reporting", name="orders",
                        kind=TargetObjectKind.TABLE,
                    )
                ],
            )
        ]
    )

    assert _find_target_conflicts(source, target) == []


def test_run_evaluation_propagates_introspection_failure() -> None:
    class _Boom:
        def introspect(self, conn: SourceConnectionConfig, *, progress=None) -> SourceInventory:
            raise RuntimeError("connection refused")

    def make(_password: object) -> _Boom:
        return _Boom()

    tgt_factory, _ = _target_factory(_empty_target())

    with pytest.raises(RuntimeError, match="connection refused"):
        run_evaluation(
            _inputs(),
            introspector_factory=make,
            target_browser_factory=tgt_factory,
        )


def test_build_assessment_chart_data_groups_by_kind_and_classification() -> None:
    from dsql_migrator.core.models import AssessmentItem, AssessmentReport
    from dsql_migrator.ui.evaluation import build_assessment_chart_data

    report = AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="t1",
                rule_id="COMPATIBLE",
                classification=Classification.AUTO,
                kind="TABLE",
            ),
            AssessmentItem(
                object_name="t2",
                rule_id="FK_PRESERVED",
                classification=Classification.MANUAL,
                effort=EffortLevel.SIMPLE,
                kind="TABLE",
            ),
            AssessmentItem(
                object_name="trg",
                rule_id="TRIGGER_UNSUPPORTED",
                classification=Classification.MANUAL,
                effort=EffortLevel.SIGNIFICANT,
                kind="TRIGGER",
            ),
        ]
    )
    data = build_assessment_chart_data(report)
    # Kinds are ordered by TOTAL COUNT descending, so the bars step down in length:
    # TABLE (2 objects) outranks TRIGGER (1), even though TRIGGER is 100% MANUAL.
    assert data.kinds == ["TABLE", "TRIGGER"]
    # Split by CLASSIFICATION, not effort -- the SIMPLE/SIGNIFICANT efforts above
    # are both MANUAL and so land in the same series.
    assert data.auto == [1, 0]
    assert data.manual == [1, 1]
    assert data.unsupported == [0, 0]


def test_build_assessment_chart_data_empty_report() -> None:
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import build_assessment_chart_data

    data = build_assessment_chart_data(AssessmentReport.from_items([]))
    assert data.kinds == []


def test_sort_assessment_items_orders_by_importance() -> None:
    from dsql_migrator.core.models import AssessmentItem, Classification, EffortLevel
    from dsql_migrator.ui.evaluation import sort_assessment_items

    items = [
        AssessmentItem(
            object_name="clean",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="TABLE",
        ),
        AssessmentItem(
            object_name="fk_table",
            rule_id="FK_PRESERVED",
            classification=Classification.MANUAL,
            effort=EffortLevel.SIMPLE,
            kind="TABLE",
        ),
        AssessmentItem(
            object_name="no_pk",
            rule_id="NO_PRIMARY_KEY",
            classification=Classification.UNSUPPORTED,
            effort=EffortLevel.MEDIUM,
            kind="TABLE",
        ),
        AssessmentItem(
            object_name="trigger_a",
            rule_id="TRIGGER_UNSUPPORTED",
            classification=Classification.MANUAL,
            effort=EffortLevel.SIGNIFICANT,
            kind="TRIGGER",
        ),
    ]
    ordered = [item.object_name for item in sort_assessment_items(items)]
    # UNSUPPORTED first, then MANUAL by effort (SIGNIFICANT before SIMPLE),
    # then AUTO last.
    assert ordered == ["no_pk", "trigger_a", "fk_table", "clean"]


def _filter_items() -> list:
    from dsql_migrator.core.models import AssessmentItem

    return [
        AssessmentItem(
            object_name="clean",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="TABLE",
        ),
        AssessmentItem(
            object_name="fk_table",
            rule_id="FK_PRESERVED",
            classification=Classification.MANUAL,
            effort=EffortLevel.SIMPLE,
            kind="TABLE",
        ),
        AssessmentItem(
            object_name="no_pk",
            rule_id="NO_PRIMARY_KEY",
            classification=Classification.UNSUPPORTED,
            effort=EffortLevel.MEDIUM,
            kind="TABLE",
        ),
    ]


def test_filter_assessment_items_all_keeps_everything() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _filter_items()
    assert filter_assessment_items(items) == items
    assert filter_assessment_items(items, classification="ALL", effort="ALL") == items
    # An unknown value on either axis falls back to ALL (nothing hidden).
    assert filter_assessment_items(items, classification="bogus") == items
    assert filter_assessment_items(items, effort="bogus") == items


def test_filter_assessment_items_specific_classification() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _filter_items()
    assert [
        i.object_name for i in filter_assessment_items(items, classification="AUTO")
    ] == ["clean"]
    assert [
        i.object_name
        for i in filter_assessment_items(items, classification="UNSUPPORTED")
    ] == ["no_pk"]
    assert [
        i.object_name for i in filter_assessment_items(items, classification="MANUAL")
    ] == ["fk_table"]


def test_filter_assessment_items_specific_effort_excludes_auto() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _filter_items()
    # SIMPLE keeps only the MANUAL/SIMPLE object; AUTO (no effort) is excluded.
    assert [
        i.object_name for i in filter_assessment_items(items, effort="SIMPLE")
    ] == ["fk_table"]
    assert [
        i.object_name for i in filter_assessment_items(items, effort="MEDIUM")
    ] == ["no_pk"]
    # No SIGNIFICANT items in the fixture.
    assert filter_assessment_items(items, effort="SIGNIFICANT") == []


def test_filter_assessment_items_combines_classification_and_effort() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _filter_items()
    # MANUAL + SIMPLE matches fk_table; MANUAL + MEDIUM matches nothing (AND).
    assert [
        i.object_name
        for i in filter_assessment_items(items, classification="MANUAL", effort="SIMPLE")
    ] == ["fk_table"]
    assert (
        filter_assessment_items(items, classification="MANUAL", effort="MEDIUM") == []
    )


def test_evaluation_state_filters_default_to_all() -> None:
    state = EvaluationState()
    assert state.classification_filter == "ALL"
    assert state.effort_filter == "ALL"


# ---------------------------------------------------------------------------
# AI chat guardrails (turn limit)
# ---------------------------------------------------------------------------


def test_chat_turns_remaining_counts_only_user_turns() -> None:
    from dsql_migrator.ui.evaluation import chat_turns_remaining

    # The auto first question counts as one user turn; assistant turns do not.
    messages = [
        {"role": "user", "text": "auto question"},
        {"role": "assistant", "text": "answer"},
        {"role": "user", "text": "follow-up"},
    ]
    assert chat_turns_remaining(messages, max_turns=10) == 8


def test_chat_turns_remaining_floors_at_zero() -> None:
    from dsql_migrator.ui.evaluation import chat_turns_remaining

    messages = [{"role": "user", "text": f"q{i}"} for i in range(5)]
    assert chat_turns_remaining(messages, max_turns=3) == 0
    assert chat_turns_remaining([], max_turns=3) == 3


# ---------------------------------------------------------------------------
# Migration readiness score
# ---------------------------------------------------------------------------


def _score_report(*specs):
    """Build an AssessmentReport from (classification, effort) specs."""
    from dsql_migrator.core.models import AssessmentItem, AssessmentReport

    items = [
        AssessmentItem(
            object_name=f"obj{i}",
            rule_id="R",
            classification=classification,
            effort=effort,
            kind="TABLE",
        )
        for i, (classification, effort) in enumerate(specs)
    ]
    return AssessmentReport.from_items(items)


def test_compute_migration_score_empty_report_is_none() -> None:
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import compute_migration_score

    assert compute_migration_score(AssessmentReport.from_items([])) is None


def test_compute_migration_score_all_auto_is_perfect_ready() -> None:
    from dsql_migrator.ui.evaluation import compute_migration_score

    score = compute_migration_score(
        _score_report(
            (Classification.AUTO, None),
            (Classification.AUTO, None),
            (Classification.AUTO, None),
        )
    )
    assert score is not None
    assert score.score == 100
    assert score.band == "Ready"
    assert score.tone == "success"
    assert (score.total, score.auto, score.manual, score.unsupported) == (3, 3, 0, 0)


def test_compute_migration_score_all_hardest_is_zero_high_effort() -> None:
    from dsql_migrator.ui.evaluation import compute_migration_score

    # Every object UNSUPPORTED at SIGNIFICANT effort -> worst case -> 0.
    score = compute_migration_score(
        _score_report(
            (Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT),
            (Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT),
        )
    )
    assert score is not None
    assert score.score == 0
    assert score.band == "High effort"
    assert score.tone == "error"
    assert score.unsupported == 2


def test_compute_migration_score_unsupported_without_effort_uses_max_penalty() -> None:
    from dsql_migrator.ui.evaluation import compute_migration_score

    # A single UNSUPPORTED object with no effort estimate is treated as the
    # hardest single-object cost, so a lone such object scores 0.
    score = compute_migration_score(
        _score_report((Classification.UNSUPPORTED, None))
    )
    assert score is not None
    assert score.score == 0


def test_compute_migration_score_mixed_lands_in_a_middle_band() -> None:
    from dsql_migrator.ui.evaluation import compute_migration_score

    # Mostly automatic with one simple manual object -> high but not perfect.
    score = compute_migration_score(
        _score_report(
            (Classification.AUTO, None),
            (Classification.AUTO, None),
            (Classification.AUTO, None),
            (Classification.MANUAL, EffortLevel.SIMPLE),
        )
    )
    assert score is not None
    assert 0 < score.score < 100
    # 1 simple penalty (1.0) over worst case (7*4=28) -> ~96 -> "Ready" band.
    assert score.tone in {"success", "warning"}
    assert score.manual == 1


def _clean_tables(n: int, *, schema: str = "app") -> list:
    return [
        TableDef(
            name=f"{schema}.t{i}",
            columns=[ColumnDef(name="id", mysql_type="int")],
            primary_key=["id"],
        )
        for i in range(n)
    ]


def test_a_source_over_a_database_limit_reads_blocked_not_ready() -> None:
    """1,001 otherwise-clean tables scored 100 'Ready -- a straightforward migration', a
    green all-clear for a source whose 1,001st CREATE TABLE fails at apply. A database-
    level limit decides the band before the ratio does."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.ui.evaluation import compute_migration_score

    score = compute_migration_score(
        CompatibilityAssessor().assess(SourceInventory(tables=_clean_tables(1001)))
    )
    assert (score.band, score.tone) == ("Blocked", "error")
    # The limit item sets the band, not the per-object score (every table is AUTO here).
    assert score.score == 100
    assert "1,001 tables (Aurora DSQL allows 1,000 tables per database)" in score.summary


def test_unsupported_objects_never_read_as_a_success_band() -> None:
    """2 of 10 tables without a primary key scored 91 'Ready' (success)."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.ui.evaluation import compute_migration_score

    no_pk = [
        TableDef(name=f"app.nopk{i}", columns=[ColumnDef(name="v", mysql_type="int")])
        for i in range(2)
    ]
    score = compute_migration_score(
        CompatibilityAssessor().assess(SourceInventory(tables=_clean_tables(8) + no_pk))
    )
    assert (score.band, score.tone) == ("Unsupported objects", "warning")
    assert score.summary.startswith("2 objects cannot move to Aurora DSQL as-is")
    # A report whose ratio is already a warning/error band keeps its own label.
    heavy = compute_migration_score(
        _score_report(
            (Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT),
            (Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT),
        )
    )
    assert (heavy.band, heavy.tone) == ("High effort", "error")


def test_the_score_is_never_100_while_any_object_needs_work() -> None:
    """One SIGNIFICANT object among 200 used to round 99.5 up to a perfect 100."""
    from dsql_migrator.ui.evaluation import compute_migration_score

    auto = [(Classification.AUTO, None)] * 199
    manual = compute_migration_score(
        _score_report(*auto, (Classification.MANUAL, EffortLevel.SIGNIFICANT))
    )
    assert (manual.score, manual.band, manual.tone) == (99, "Ready", "success")
    unsupported = compute_migration_score(
        _score_report(*auto, (Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT))
    )
    assert unsupported.score == 99
    assert (unsupported.band, unsupported.tone) == ("Unsupported objects", "warning")


def test_a_mysql_json_table_grades_and_scores_like_a_postgres_json_table() -> None:
    """G13: an identical document column must not read "Ready" on MySQL while it reads
    "Moderate effort" on PostgreSQL (the asymmetry v0.1.497 removed for bytea/longblob).
    MySQL json grades -- and so scores -- exactly like PostgreSQL json and MySQL LONGTEXT.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.ui.evaluation import compute_migration_score

    def inventory(doc_type: str) -> SourceInventory:
        return SourceInventory(
            tables=[
                TableDef(
                    name=f"app.t{i}",
                    primary_key=["id"],
                    columns=[
                        ColumnDef(name="id", mysql_type="bigint"),
                        ColumnDef(name="doc", mysql_type=doc_type),
                    ],
                )
                for i in range(10)
            ]
        )

    def graded(source_type: SourceType, doc_type: str):
        report = CompatibilityAssessor(source_type=source_type).assess(inventory(doc_type))
        per_item = {
            (i.classification, i.effort, tuple(c.note_kind for c in i.concerns))
            for i in report.items
        }
        score = compute_migration_score(report)
        return per_item, (score.score, score.band)

    mysql_json = graded(SourceType.MYSQL, "json")
    assert (
        mysql_json
        == graded(SourceType.POSTGRES, "json")
        == graded(SourceType.MYSQL, "longtext")
    )
    assert mysql_json[1] == (57, "Moderate effort")


# ---------------------------------------------------------------------------
# Job-status -> step-status mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_status", "expected"),
    [
        ("DONE", StepStatus.DONE),
        ("FAILED", StepStatus.FAILED),
        ("PENDING", None),
        ("RUNNING", None),
    ],
)
def test_job_status_to_step_status(job_status: str, expected) -> None:
    assert job_status_to_step_status(job_status) is expected


def test_reconcile_evaluation_step_recovers_an_interrupted_run() -> None:
    from dsql_migrator.ui.evaluation import reconcile_evaluation_step

    # IN_PROGRESS with no live job and no result = interrupted by a restart -> reset to
    # NOT_STARTED so the spinner stops and Run returns.
    assert (
        reconcile_evaluation_step(
            StepStatus.IN_PROGRESS, job_alive=False, has_result=False
        )
        is StepStatus.NOT_STARTED
    )
    # A genuinely running job (alive) is left IN_PROGRESS (keeps spinning).
    assert (
        reconcile_evaluation_step(
            StepStatus.IN_PROGRESS, job_alive=True, has_result=False
        )
        is None
    )
    # A finished run (result present) whose completion only the screen's poll would have
    # recorded -- the operator was on another step, then a restart -- is DONE, not a
    # permanent spinner.
    assert (
        reconcile_evaluation_step(
            StepStatus.IN_PROGRESS, job_alive=False, has_result=True
        )
        is StepStatus.DONE
    )
    # Terminal steps are never reconciled.
    assert (
        reconcile_evaluation_step(
            StepStatus.DONE, job_alive=False, has_result=True
        )
        is None
    )


# ---------------------------------------------------------------------------
# Report export serialization (Requirement 8.4)
# ---------------------------------------------------------------------------


def _result() -> EvaluationResult:
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())
    return run_evaluation(
        _inputs(),
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )


_EXPORTED_AT = "2026-10-17 01:05:09 UTC"


def test_assessment_download_json_is_valid_and_named() -> None:
    download = assessment_download(_result(), "json", exported_at=_EXPORTED_AT)
    assert download.filename == "compatibility_assessment_app_20261017-010509.json"
    assert download.media_type == "application/json"
    parsed = json.loads(download.content)
    assert "items" in parsed and "summary" in parsed
    assert "effort_summary" in parsed


def test_assessment_download_text_is_human_readable() -> None:
    download = assessment_download(_result(), "text", exported_at=_EXPORTED_AT)
    assert download.filename == "compatibility_assessment_app_20261017-010509.txt"
    assert download.media_type == "text/plain"
    assert "Compatibility Assessment Report" in download.content
    assert "Estimated manual effort" in download.content


def test_assessment_download_html_is_named_and_typed() -> None:
    download = assessment_download(_result(), "html", exported_at=_EXPORTED_AT)
    assert download.filename == "compatibility_assessment_app_20261017-010509.html"
    assert download.media_type == "text/html"
    assert download.content.startswith("<!DOCTYPE html>")


def test_assessment_download_html_omits_ai_section() -> None:
    # AI assistance is on-demand per object (the screen drawer) and is never part
    # of the exported, shareable report, so no format carries an AI section.
    result = _result()
    markup = assessment_download(result, "html").content
    assert "AI-led migration assessment" not in markup


def test_download_rejects_unknown_format() -> None:
    with pytest.raises(ValueError):
        assessment_download(_result(), "yaml")


# ---------------------------------------------------------------------------
# Per-session evaluation state and store
# ---------------------------------------------------------------------------


def test_evaluation_state_result_error_handoff() -> None:
    state = EvaluationState()
    assert state.result is None
    assert state.error is None

    result = _result()
    state.set_result(result)
    assert state.result is result
    assert state.error is None

    state.set_error("boom")
    assert state.error == "boom"
    # An error does not erase an existing result; clear_outputs does.
    state.clear_outputs()
    assert state.result is None
    assert state.error is None


def test_evaluation_state_set_result_clears_prior_error() -> None:
    state = EvaluationState()
    state.set_error("previous failure")
    state.set_result(_result())
    assert state.error is None


def test_evaluation_store_is_isolated_per_session() -> None:
    store = EvaluationStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")
    assert a is not b
    assert store.get_or_create("session-a") is a

    a.set_error("session-a failure")
    assert b.error is None


def test_evaluation_store_clear_removes_only_target_session() -> None:
    store = EvaluationStore()
    store.get_or_create("session-a")
    store.get_or_create("session-b")

    store.clear("session-a")

    assert store.get("session-a") is None
    assert store.get("session-b") is not None
    # Clearing unknown / None ids is a no-op.
    store.clear("missing")
    store.clear(None)


# ---------------------------------------------------------------------------
# End-to-end background run through the real JobManager
# ---------------------------------------------------------------------------


def test_background_run_finishes_done_and_records_result() -> None:
    manager = JobManager()
    eval_state = EvaluationState()
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())
    inputs = _inputs()

    def work(_handle: object) -> None:
        eval_state.set_result(
            run_evaluation(
                inputs,
                introspector_factory=src_factory,
                target_browser_factory=tgt_factory,
            )
        )

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    job = manager.get_status(job_id)
    assert job_status_to_step_status(job.status) is StepStatus.DONE
    assert eval_state.result is not None
    assert len(eval_state.result.assessment.items) == 2


def test_assessment_logs_a_started_event_before_running() -> None:
    # The assessment logged only success/failure; a "started" event now brackets the run
    # so the audit trail shows when it began (mirrors the other stages' started events).
    import inspect

    from dsql_migrator.ui import evaluation as _e

    src = inspect.getsource(_e)
    # A STARTED "run assessment" event exists in the work() closure.
    assert 'status=ActivityStatus.STARTED' in src
    assert src.count('"run assessment"') >= 3  # started + success + failure


def test_background_run_failure_maps_to_failed_status() -> None:
    manager = JobManager()

    def work(_handle: object) -> None:
        raise RuntimeError("introspection failed")

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    job = manager.get_status(job_id)
    assert job_status_to_step_status(job.status) is StepStatus.FAILED
    assert "introspection failed" in (manager.get_error(job_id) or "")


# ---------------------------------------------------------------------------
# Assessment grouping by object kind (Tables / Views / Triggers / Routines)
# ---------------------------------------------------------------------------


def _assessment_item(name: str, kind: str, classification: Classification):
    from dsql_migrator.core.models import AssessmentItem

    return AssessmentItem(
        object_name=name,
        rule_id="R1",
        classification=classification,
        kind=kind,
    )


def test_group_assessment_items_by_kind_orders_kinds_and_keeps_input_order() -> None:
    from dsql_migrator.ui.evaluation import group_assessment_items_by_kind

    items = [
        _assessment_item("v1", "VIEW", Classification.MANUAL),
        _assessment_item("t1", "TABLE", Classification.AUTO),
        _assessment_item("trg1", "TRIGGER", Classification.UNSUPPORTED),
        _assessment_item("t2", "TABLE", Classification.UNSUPPORTED),
    ]
    groups = group_assessment_items_by_kind(items)

    # Tables first, then Views, then Triggers (display order); within a kind the
    # input order is preserved (t1 before t2).
    assert [kind for kind, _ in groups] == ["TABLE", "VIEW", "TRIGGER"]
    table_items = dict(groups)["TABLE"]
    assert [i.object_name for i in table_items] == ["t1", "t2"]


def test_group_assessment_items_unknown_kind_follows_in_first_seen_order() -> None:
    from dsql_migrator.ui.evaluation import group_assessment_items_by_kind

    items = [
        _assessment_item("seq1", "SEQUENCE", Classification.MANUAL),
        _assessment_item("t1", "TABLE", Classification.AUTO),
    ]
    groups = group_assessment_items_by_kind(items)
    # Known kinds (TABLE) come first; unknown kinds (SEQUENCE) follow.
    assert [kind for kind, _ in groups] == ["TABLE", "SEQUENCE"]


def test_assessment_kind_summary_orders_counts_by_severity() -> None:
    from dsql_migrator.ui.evaluation import assessment_kind_summary

    items = [
        _assessment_item("t1", "TABLE", Classification.AUTO),
        _assessment_item("t2", "TABLE", Classification.AUTO),
        _assessment_item("t3", "TABLE", Classification.UNSUPPORTED),
    ]
    # Severity order preserved (Unsupported first); user-facing labels shown.
    assert assessment_kind_summary(items) == "1 Unsupported · 2 Automatic"


def test_classification_label_maps_to_user_facing_text() -> None:
    from dsql_migrator.ui.evaluation import classification_label

    # Internal enum values map to the one-axis, user-facing labels.
    assert classification_label("AUTO") == "Automatic"
    assert classification_label("MANUAL") == "Review needed"
    assert classification_label("UNSUPPORTED") == "Unsupported"
    # Unknown values pass through unchanged so nothing is hidden.
    assert classification_label("FUTURE") == "FUTURE"


def test_kind_section_label_friendly_names() -> None:
    from dsql_migrator.ui.evaluation import kind_section_label

    assert kind_section_label("TABLE") == "Tables"
    assert kind_section_label("VIEW") == "Views"
    assert kind_section_label("TRIGGER") == "Triggers"
    assert kind_section_label("ROUTINE") == "Routines"
    # A PostgreSQL sequence no column owns is reported as an object of its own.
    assert kind_section_label("SEQUENCE") == "Sequences"
    # Unknown kinds fall back to a title-cased label.
    assert kind_section_label("WIDGET") == "Widget"


# ---------------------------------------------------------------------------
# The Evaluation row renders ONE block per matched rule. The joined
# risk/recommendation strings made a five-rule table unreadable: two run-on
# paragraphs, with the Nth risk in one and its fix in the other.
# ---------------------------------------------------------------------------


class _Changed:
    """The event a value change hands its handler (``event.value``)."""

    def __init__(self, value) -> None:
        self.value = value


class _ItemUi:
    """A NiceGUI stand-in that records emitted text, badges, and separators.

    It also counts every element it creates (``created``) and keeps the handlers the
    browser would trigger -- an expansion opening, a page turning, a filter changing, a
    button click -- so a test can drive the screen the way the operator does.
    """

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.badges: list[str] = []
        self.separators = 0
        self.icons: list[str] = []
        self.classes: list[str] = []
        self.created = 0
        # (element, on_value_change, index into texts where its header starts)
        self.expansions: list[tuple] = []
        self._opened: set[int] = set()
        self.paginations: list[tuple] = []  # (element, on_change, (min, max), value)
        self.inputs: list[tuple] = []  # (element, on_change, placeholder)
        self.selects: dict[str, object] = {}  # label -> on_change
        self.select_elements: dict[str, object] = {}  # label -> element (has .value)
        self.buttons: dict[str, list] = {}  # text -> [on_click, ...]
        self.refreshes = 0

    class _El:
        def __init__(self, owner) -> None:
            self._owner = owner

        def classes(self, value="", *_a, **_k):
            if value:
                self._owner.classes.append(str(value))
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def add_slot(self, *_a, **_k):
            return self

        def set_enabled(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def __getattr__(self, _name):
            # Any other chainable no-op (disable/enable/style/on/...). Keeps the double
            # from having to mirror NiceGUI's whole element surface.
            return lambda *_a, **_k: self

    def _el(self):
        self.created += 1
        return self._El(self)

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._el()

    def badge(self, text="", *_a, **_k):
        if text:
            self.badges.append(str(text))
            self.texts.append(str(text))
        return self._el()

    def separator(self, *_a, **_k):
        self.separators += 1
        return self._el()

    def icon(self, name="", *_a, **_k):
        if name:
            self.icons.append(str(name))
        return self._el()

    def expansion(self, *_a, on_value_change=None, **_k):
        element = self._el()
        self.expansions.append((element, on_value_change, len(self.texts)))
        return element

    def pagination(self, *bounds, on_change=None, value=None, **_k):
        element = self._el()
        self.paginations.append((element, on_change, bounds, value))
        return element

    def _valued(self, value, on_change):
        # A control holding a value; ``set_value`` fires its handler on a change, as
        # NiceGUI's does.
        element = self._el()
        element.value = value

        def _set_value(new):
            changed = element.value != new
            element.value = new
            if changed and on_change is not None:
                on_change(_Changed(new))

        element.set_value = _set_value
        return element

    def input(self, *_a, on_change=None, placeholder=None, value=None, **_k):
        element = self._valued(value, on_change)
        self.inputs.append((element, on_change, placeholder))
        return element

    def select(self, *_a, label=None, on_change=None, value=None, **_k):
        self.selects[str(label)] = on_change
        element = self._valued(value, on_change)
        self.select_elements[str(label)] = element
        return element

    def button(self, text="", *_a, on_click=None, **_k):
        self.buttons.setdefault(str(text), []).append(on_click)
        return self._el()

    def refreshable(self, fn):
        # NiceGUI's decorator: calling the result renders; ``.refresh()`` renders again.
        ui = self

        def _call(*a, **k):
            return fn(*a, **k)

        def _refresh(*a, **k):
            ui.refreshes += 1
            return fn(*a, **k)

        _call.refresh = _refresh
        return _call

    def open(self, header: str) -> None:
        """Open the first not-yet-opened expansion whose header starts with ``header``."""
        for index, (_element, handler, start) in enumerate(self.expansions):
            if index in self._opened or start >= len(self.texts):
                continue
            if self.texts[start] == header and handler is not None:
                self._opened.add(index)
                handler(_Changed(True))
                return
        raise AssertionError(f"no unopened expansion headed {header!r}")

    def open_expansions(self) -> None:
        """Open every expansion not opened yet, including those built while opening."""
        index = 0
        while index < len(self.expansions):
            _element, handler, _start = self.expansions[index]
            if index not in self._opened and handler is not None:
                self._opened.add(index)
                handler(_Changed(True))
            index += 1

    def __getattr__(self, _name):
        return lambda *_a, **_k: self._el()


def _five_rule_item():
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import (
        ColumnDef,
        ForeignKeyDef,
        SourceInventory,
        TableDef,
    )

    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="user_id", mysql_type="int", nullable=False),
            ColumnDef(
                name="status",
                mysql_type="enum('pending','shipped')",
                collation="utf8mb4_general_ci",
            ),
            ColumnDef(name="updated_at", mysql_type="datetime", auto_update_timestamp=True),
        ],
        primary_key=["id"],
        auto_increment_column="id",
        foreign_keys=[
            ForeignKeyDef(
                name="fk_orders_users",
                columns=["user_id"],
                referenced_table="users",
                referenced_columns=["id"],
            )
        ],
    )
    report = CompatibilityAssessor().assess(SourceInventory(tables=[table]))
    return next(i for i in report.items if i.object_name == "orders")


def test_assessment_row_renders_one_block_per_concern() -> None:
    from dsql_migrator.ui.evaluation import _render_assessment_item

    item = _five_rule_item()
    ui = _ItemUi()
    _render_assessment_item(ui, item)
    ui.open(item.object_name)

    body = "\n".join(ui.texts)
    # Every rule id is shown, so the reader can see WHICH rules fired.
    for rule_id in ("FK_PRESERVED", "AUTO_INCREMENT", "CI_COLLATION",
                    "ENUM_SET_TYPE", "ON_UPDATE_TIMESTAMP"):
        assert rule_id in body, rule_id
    # Each concern is its own bordered card, indented behind one vertical spine, so the
    # body reads as children of the row above rather than as a flat run of text.
    boxed = [c for c in ui.classes if "rounded-md" in c and "border" in c]
    assert len(boxed) == len(item.concerns), (len(boxed), len(item.concerns))
    assert any("border-l-2" in c for c in ui.classes), "needs the tree spine"
    # The joined run-on string is NOT what gets rendered.
    assert "Aurora DSQL.; AUTO_INCREMENT column" not in body


def test_each_rendered_concern_shows_its_own_class_and_fix() -> None:
    """The header badge shows only the GOVERNING class, which hides the others.

    A per-concern badge is what lets a reader see that (say) one finding is UNSUPPORTED
    while the rest are MANUAL, instead of inferring it from the single worst-case badge.
    """
    from dsql_migrator.ui.evaluation import _render_assessment_item

    item = _five_rule_item()
    ui = _ItemUi()
    _render_assessment_item(ui, item)
    ui.open(item.object_name)

    body = "\n".join(ui.texts)
    for concern in item.concerns:
        assert concern.risk in body, concern.rule_id
        assert concern.recommendation in body, concern.rule_id
    # One class badge per NON-ADVISORY concern (an advisory one says RECOMMENDED
    # instead), plus the header's own badge and the kind badge.
    from dsql_migrator.ui.evaluation import classification_label

    gaps = [c for c in item.concerns if not c.is_advisory]
    advice = [c for c in item.concerns if c.is_advisory]
    assert gaps and advice, "fixture must cover both kinds"
    label = classification_label(item.classification.value)
    assert ui.badges.count(label) >= len(gaps)
    assert ui.badges.count("RECOMMENDED") == len(advice)
    # Problem and fix are LABELED, not merely arrowed: each carries its own caption and
    # glyph so the reader never has to infer which line is the remedy. An advisory
    # finding is captioned "Note", not "Risk" -- nothing is wrong with the object.
    fixes = [c for c in item.concerns if c.recommendation]
    assert ui.texts.count("Risk") == len([c for c in gaps if c.risk])
    assert ui.texts.count("Note") == len([c for c in advice if c.risk])
    assert ui.texts.count("Recommendation") == len(fixes)
    assert ui.icons.count("warning") == len([c for c in gaps if c.risk])
    assert ui.icons.count("lightbulb") == len(fixes)
    # The fix sits on its own tinted panel, which is what separates it from the risk at
    # a glance rather than relying on a fainter text color.
    assert sum("bg-green-50" in c for c in ui.classes) == len(fixes)
    # Advisory cards use the calm info-blue surface, not the neutral/amber problem one.
    assert sum("bg-sky-50" in c for c in ui.classes) == len(advice)


def test_row_falls_back_to_joined_text_for_a_pre_concerns_report() -> None:
    # A session persisted before `concerns` existed must still render its guidance
    # rather than showing an empty body.
    from dsql_migrator.core.models import AssessmentItem, Classification
    from dsql_migrator.ui.evaluation import _render_assessment_item

    legacy = AssessmentItem(
        object_name="orders",
        rule_id="FK_PRESERVED",
        classification=Classification.MANUAL,
        risk="a; b",
        recommendation="fix a; fix b",
        kind="TABLE",
    )
    ui = _ItemUi()
    _render_assessment_item(ui, legacy)
    ui.open(legacy.object_name)
    body = "\n".join(ui.texts)
    assert "a; b" in body
    assert "fix a; fix b" in body


def test_ui_chart_and_html_export_agree_on_kind_order() -> None:
    """The exported report must not order its bars differently from the screen.

    Both are built from ``classification_stats_by_kind``, so this pins the contract: if
    someone re-sorts one side, this fails instead of the two silently drifting apart.
    """
    import re

    from dsql_migrator.core.assessor import render_html_report
    from dsql_migrator.core.models import AssessmentItem, AssessmentReport
    from dsql_migrator.ui.evaluation import build_assessment_chart_data

    items = [
        AssessmentItem(
            object_name=f"t{i}",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="TABLE",
        )
        for i in range(6)
    ] + [
        AssessmentItem(
            object_name=f"sp{i}",
            rule_id="ROUTINE_UNSUPPORTED",
            classification=Classification.UNSUPPORTED,
            kind="PROCEDURE",
        )
        for i in range(2)
    ] + [
        AssessmentItem(
            object_name="trg",
            rule_id="TRIGGER_UNSUPPORTED",
            classification=Classification.UNSUPPORTED,
            kind="TRIGGER",
        ),
    ]
    from dsql_migrator.ui.evaluation import kind_section_label

    report = AssessmentReport.from_items(items)
    # build_assessment_chart_data carries raw kinds; both charts label them for display.
    ui_order = [kind_section_label(k) for k in build_assessment_chart_data(report).kinds]
    html_order = re.findall(
        r'<div class="bar-label">([^<]+)</div>', render_html_report(report)
    )
    assert ui_order == html_order == ["Tables", "Stored procedures", "Triggers"]


def _mixed_effort_report():
    """A report where some objects need work and some do not.

    ``t4``/``t5`` match only the AUTO_INCREMENT recommendation, which v0.1.173 excluded
    from the effort estimate -- so they carry no effort and land in no effort bucket.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import (
        ColumnDef,
        ForeignKeyDef,
        SourceInventory,
        TableDef,
    )

    tables = []
    for i in range(6):
        tables.append(
            TableDef(
                name=f"t{i}",
                columns=[
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                    ColumnDef(
                        name="s",
                        mysql_type="varchar(20)",
                        collation="utf8mb4_general_ci" if i < 3 else None,
                    ),
                ],
                primary_key=["id"],
                auto_increment_column="id",
                foreign_keys=(
                    [
                        ForeignKeyDef(
                            name=f"fk{i}",
                            columns=["id"],
                            referenced_table="o",
                            referenced_columns=["id"],
                        )
                    ]
                    if i < 4
                    else []
                ),
            )
        )
    return CompatibilityAssessor().assess(SourceInventory(tables=tables))


def test_effort_summary_sits_with_the_object_list_not_with_the_chart() -> None:
    """The chart splits by classification, so only classification belongs above it.

    The effort row used to sit directly under the classification row, which put a summary
    the chart says nothing about beside the one the chart is built from -- and the two
    looked identical yet did not add up to the same total. Effort is a tool for working the
    object list, so it belongs with that list and its effort filter.
    """
    from dsql_migrator.ui.evaluation import _render_assessment

    report = _mixed_effort_report()
    ui = _ItemUi()
    _render_assessment(ui, report)

    order = ui.texts
    chart_title = order.index("Compatibility by object kind")
    effort_label = order.index("Estimated manual effort")
    list_heading = next(
        i for i, t in enumerate(order) if t.startswith("Objects by importance")
    )
    assert order.index("Classification") < chart_title, "classification stays above chart"
    assert chart_title < list_heading < effort_label, order[:12]


def test_effort_summary_states_how_many_objects_need_work() -> None:
    # The buckets do not sum to the object total, so the row says so itself rather than
    # leaving the reader to wonder which objects went missing.
    from dsql_migrator.ui.evaluation import _render_assessment

    report = _mixed_effort_report()
    needing = sum(report.effort_summary.values())
    assert needing < len(report.items), "fixture must include effort-less objects"

    ui = _ItemUi()
    _render_assessment(ui, report)
    assert f"({needing} of {len(report.items)} objects need work)" in ui.texts


def test_effort_summary_is_omitted_when_no_object_needs_work() -> None:
    # An all-clean schema has nothing to estimate; an empty bucket row would be noise.
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import _render_assessment

    report = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name="clean",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
            ]
        )
    )
    assert all(item.effort is None for item in report.items)
    ui = _ItemUi()
    _render_assessment(ui, report)
    assert "Estimated manual effort" not in ui.texts


def test_every_effort_badge_uses_the_same_neutral_color() -> None:
    """Effort must not borrow the compatibility ramp, and must not vary by surface.

    The green/amber/red ramp means COMPATIBILITY on this screen -- the chart, the
    classification badges and the Risk/Recommendation panels all use it. Effort is a
    different axis (ordered hours, not severity), and a per-level ramp both diluted that
    meaning and collided on object rows: an amber "Review needed" badge sat beside an amber
    "effort: MEDIUM", a red "Unsupported" beside a red "effort: SIGNIFICANT". It was also
    applied inconsistently -- colored in the summary row, gray on the rows and cards.
    """
    from dsql_migrator.ui import evaluation

    # A single constant, not a per-level map: nothing can diverge per level again.
    assert isinstance(evaluation._EFFORT_BADGE_COLOR, str)
    # Outside the severity ramp used by classification.
    assert evaluation._EFFORT_BADGE_COLOR not in set(
        evaluation._CLASS_BADGE_COLOR.values()
    )
    for ramp_color in ("green", "amber", "red", "orange"):
        assert ramp_color not in evaluation._EFFORT_BADGE_COLOR

    # No surface may hard-code the color: the literal must appear EXACTLY ONCE in the
    # module -- at the constant's definition. Every badge then references the constant, so
    # changing it once changes all three. (Grepping badge lines for the literal instead
    # would pass while the hard-coded value happens to match, then drift silently.)
    import pathlib

    source = pathlib.Path(evaluation.__file__).read_text()
    literal = f'"{evaluation._EFFORT_BADGE_COLOR}"'
    assert source.count(literal) == 1, (
        f"{literal} appears {source.count(literal)}x; it must only be the constant's "
        "definition, with every badge referencing _EFFORT_BADGE_COLOR"
    )
    # And the badges do reference it. Two surfaces carry an effort badge: the summary row
    # above the object list, and each finding inside an expanded object. The collapsed
    # object row deliberately has none -- see
    # test_collapsed_row_shows_no_object_level_effort_badge.
    assert source.count("color={_EFFORT_BADGE_COLOR} outline") == 2, source.count(
        "color={_EFFORT_BADGE_COLOR} outline"
    )


def test_effort_summary_badges_render_without_a_severity_color() -> None:
    from dsql_migrator.ui import evaluation
    from dsql_migrator.ui.evaluation import _render_assessment

    report = _mixed_effort_report()
    ui = _ItemUi()
    _render_assessment(ui, report)
    # The summary row still renders its counts -- it just does so neutrally.
    assert any(b.startswith("MEDIUM: ") for b in ui.badges), ui.badges
    assert evaluation._EFFORT_BADGE_COLOR == "blue-grey-6"


def test_collapsed_row_summarises_the_findings_the_badge_hides() -> None:
    """The header badge names only the governing class, which is silent about the rest.

    Measured on a real source, 16 of 18 tables carried a mix (a real gap plus the
    AUTO_INCREMENT recommendation) behind one badge, and a header reading "Unsupported"
    could hide six findings of which four were merely review-needed and one optional -- so
    the object looked wholly blocked when most of it was not.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import _render_assessment_item

    item = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name="wide",
                    columns=[
                        ColumnDef(name="id", mysql_type="int", nullable=False),
                        # Past DSQL's numeric ceiling -> UNSUPPORTED, so it governs.
                        # Beyond the DOCUMENTED maximum (1000); 65,30 now fits DSQL.
                        ColumnDef(name="amt", mysql_type="decimal(1001,30)"),
                        ColumnDef(
                            name="sku",
                            mysql_type="varchar(40)",
                            collation="utf8mb4_general_ci",
                        ),
                    ],
                    primary_key=["id"],
                    auto_increment_column="id",
                )
            ]
        )
    ).items[0]
    assert item.classification.value == "UNSUPPORTED"

    ui = _ItemUi()
    _render_assessment_item(ui, item)
    joined = "\n".join(ui.texts)
    # Every class present is named with its count, advisory findings included.
    assert "1 Unsupported" in joined, joined
    assert "1 Review needed" in joined, joined
    assert "1 Recommended" in joined, joined


def test_collapsed_row_shows_no_object_level_effort_badge() -> None:
    """Effort belongs to each finding, not to the collapsed row.

    A per-object estimate described the object as a whole while the row now summarises its
    findings; and one SIMPLE fix beside one SIGNIFICANT one does not average into a single
    useful number. Each finding carries its own effort inside, and the schema-wide
    distribution sits in the summary above the list.
    """
    from dsql_migrator.ui.evaluation import _render_assessment_item

    item = _five_rule_item()
    assert item.effort is not None, "fixture must have an object-level effort to omit"

    ui = _ItemUi()
    _render_assessment_item(ui, item)
    # Collapsed, the row carries no effort badge at all.
    assert not [b for b in ui.badges if b.startswith("effort")], ui.badges
    ui.open(item.object_name)
    # Exactly one effort badge per finding that has an estimate -- no extra one for the
    # object itself. Counting is what catches a re-added row badge: the object's governing
    # effort equals some finding's, so checking for the text alone would still pass.
    per_finding = [b for b in ui.badges if b.startswith("effort")]
    expected = [c for c in item.concerns if c.effort is not None]
    assert expected, "fixture must have findings carrying an effort"
    assert len(per_finding) == len(expected), (per_finding, len(expected))


def test_a_single_finding_still_gets_its_own_labeled_badge() -> None:
    """With no separate governing badge, the one finding IS the row's badge.

    An earlier revision suppressed the breakdown when it would repeat a governing badge.
    That badge is gone -- the categories replaced it -- so suppressing anything here would
    leave the row with only a name.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import (
        assessment_concern_counts,
        _render_assessment_item,
    )

    item = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name="one",
                    columns=[
                        ColumnDef(name="id", mysql_type="int", nullable=False),
                        ColumnDef(
                            name="s",
                            mysql_type="varchar(9)",
                            collation="utf8mb4_general_ci",
                        ),
                    ],
                    primary_key=["id"],
                )
            ]
        )
    ).items[0]
    assert len(item.concerns) == 1
    assert assessment_concern_counts(item) == [("Review needed", 1, "warning")]

    ui = _ItemUi()
    _render_assessment_item(ui, item)
    assert "1 Review needed" in ui.badges, ui.badges


def test_a_clean_object_falls_back_to_its_classification_badge() -> None:
    # No findings to count, so the row would otherwise carry nothing but a name.
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import (
        assessment_concern_counts,
        classification_label,
        _render_assessment_item,
    )

    item = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name="clean",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
            ]
        )
    ).items[0]
    assert assessment_concern_counts(item) == []

    ui = _ItemUi()
    _render_assessment_item(ui, item)
    assert classification_label(item.classification.value) in ui.badges, ui.badges


def test_every_category_badge_carries_its_label_not_a_bare_count() -> None:
    """Color must not be the only signal -- a bare "1" needs the chart legend to decode.

    Same rule the diff gutter follows: a monochrome screenshot and a colorblind reader must
    both still be able to read the row.
    """
    from dsql_migrator.ui.evaluation import assessment_concern_counts

    item = _five_rule_item()
    entries = assessment_concern_counts(item)
    assert entries, "fixture must produce findings"
    for label, count, color in entries:
        assert label and not label.isdigit(), entries
        assert isinstance(count, int) and count > 0, entries
        assert color, entries
    # Advisory findings take the calm info tone, outside the severity ramp.
    advisory = [e for e in entries if e[0] == "Recommended"]
    assert advisory and advisory[0][2] == "info", entries


def test_category_badges_lead_with_the_governing_classification() -> None:
    """Worst-first, so the leading badge is what the old single governing badge showed."""
    from dsql_migrator.ui.evaluation import (
        assessment_concern_counts,
        classification_label,
    )

    item = _five_rule_item()
    entries = assessment_concern_counts(item)
    governing = classification_label(item.classification.value)
    assert entries[0][0] == governing, entries
    # Its count is the number of non-advisory findings of that class -- advisory ones are
    # counted separately under "Recommended" even though they carry the same classification.
    expected = len(
        [
            c
            for c in item.concerns
            if not c.is_advisory and c.classification is item.classification
        ]
    )
    assert entries[0][1] == expected, entries
    # Advice sorts after every real gap, matching the order inside the expanded card.
    labels = [label for label, _count, _color in entries]
    assert labels.index("Recommended") == len(labels) - 1, labels


def test_a_cluster_level_row_renders_like_every_table_row() -> None:
    """One finding is still a finding -- the list must not change shape for it.

    The database-level row used to fall through to the pre-concerns rendering (bare "Risk"
    and "Recommendation" labels, no card, no spine) purely because its item shipped with an
    empty concerns list, so it looked like a different application beside the tables.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import _render_assessment_item

    # Eleven databases need eleven target schemas: the database-level SCHEMA_COUNT_LIMIT
    # blocker (the multi-database note itself is advice now, and renders as such).
    report = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name=f"db{n}.t",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
                for n in range(11)
            ]
        )
    )
    cluster = next(i for i in report.items if i.kind == "DATABASE")
    assert cluster.rule_id == "SCHEMA_COUNT_LIMIT", cluster

    ui = _ItemUi()
    _render_assessment_item(ui, cluster)
    ui.open(cluster.object_name)
    # The category badge treatment, same as a table row.
    assert "1 Unsupported" in ui.badges, ui.badges
    # The card treatment: labeled Risk / Recommendation panels behind a spine. (The caps
    # in the rendered UI come from a `uppercase` class, not from the label text.)
    assert "Risk" in ui.texts and "Recommendation" in ui.texts, ui.texts
    assert any("border-l-2" in c for c in ui.classes), "needs the tree spine"
    assert any("bg-green-50" in c for c in ui.classes), "needs the fix panel"
    # NOT the legacy fallback, which emitted a bare "Rule: X" line.
    assert not any(t.startswith("Rule: ") for t in ui.texts), ui.texts


def test_source_tallies_use_the_same_words_as_the_assessed_list() -> None:
    """A tile reading "3 Routines" sent the reader hunting for a heading that never exists.

    MySQL groups procedures and functions under information_schema.ROUTINES, so the
    inventory field is named correctly -- but the assessment splits them (DSQL treats them
    differently), and the list and chart below say "Stored procedures" and "Functions". The
    tally row must speak the vocabulary of the list it summarises.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import ObjectRef, ObjectType, SourceInventory
    from dsql_migrator.ui.evaluation import (
        group_assessment_items_by_kind,
        kind_section_label,
        source_inventory_tallies,
        sort_assessment_items,
    )

    inventory = SourceInventory(
        routines=[
            ObjectRef(name="sp_a", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="sp_b", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="fn_x", object_type=ObjectType.FUNCTION),
        ]
    )
    tiles = dict(source_inventory_tallies(inventory))
    assert tiles.get("Stored procedures") == 2, tiles
    assert tiles.get("Functions") == 1, tiles
    # The generic term is gone when the subtypes are known.
    assert "Routines" not in tiles, tiles

    # Every tile label must match a heading the assessed list actually renders.
    report = CompatibilityAssessor().assess(inventory)
    headings = {
        kind_section_label(kind)
        for kind, _items in group_assessment_items_by_kind(
            sort_assessment_items(report.items)
        )
    }
    for label, count in source_inventory_tallies(inventory):
        if count:
            assert label in headings, (label, headings)


def test_source_tallies_fall_back_to_routines_without_a_subtype() -> None:
    # A routine collected without PROCEDURE/FUNCTION keeps the generic kind, matching how
    # the assessor categorises it -- dropping it would hide an object.
    from dsql_migrator.core.models import ObjectRef, ObjectType, SourceInventory
    from dsql_migrator.ui.evaluation import source_inventory_tallies

    tiles = dict(
        source_inventory_tallies(
            SourceInventory(
                routines=[ObjectRef(name="r", object_type=ObjectType.ROUTINE)]
            )
        )
    )
    assert tiles.get("Routines") == 1, tiles


def test_source_tallies_drop_empty_kinds_but_always_show_tables() -> None:
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.evaluation import source_inventory_tallies

    # Nothing but tables: no zero tiles for views/triggers/routines/events.
    only_tables = SourceInventory(
        tables=[
            TableDef(
                name="t",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            )
        ]
    )
    assert source_inventory_tallies(only_tables) == [("Tables", 1)]
    # An empty source still says so, rather than rendering an empty row.
    assert source_inventory_tallies(SourceInventory()) == [("Tables", 0)]


def test_one_kind_label_map_serves_the_list_the_chart_and_the_export() -> None:
    """The same object must not be named two ways on one screen.

    The map was UI-only, so the chart axes showed the raw enum ("PROCEDURE") beside a list
    heading reading "Stored procedures", and the HTML export's chart did the same. It now
    lives in core.assessor and all three read from it.
    """
    from dsql_migrator.core import assessor
    from dsql_migrator.ui import evaluation

    # Not a copy -- the same object, so an edit cannot reach one surface and miss another.
    assert evaluation._KIND_LABELS is assessor.KIND_LABELS
    # Both label helpers agree, including the fallback for an unknown kind.
    for kind in list(assessor.KIND_LABELS) + ["SEQUENCE"]:
        assert evaluation.kind_section_label(kind) == assessor.kind_label(kind), kind
    # The label that motivated this: MySQL's ROUTINES splits into these two.
    assert assessor.KIND_LABELS["PROCEDURE"] == "Stored procedures"
    assert assessor.KIND_LABELS["FUNCTION"] == "Functions"


def test_ui_chart_axis_uses_friendly_kind_labels() -> None:
    from dsql_migrator.core.models import AssessmentItem, AssessmentReport
    from dsql_migrator.ui.evaluation import _render_assessment_chart

    report = AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="sp",
                rule_id="PROC_PLPGSQL",
                classification=Classification.UNSUPPORTED,
                kind="PROCEDURE",
            )
        ]
    )

    captured = {}

    class _ChartUi(_ItemUi):
        def echart(self, option, *_a, **_k):
            captured["option"] = option
            return self._el()

    _render_assessment_chart(_ChartUi(), report)
    assert captured["option"]["yAxis"]["data"] == ["Stored procedures"], captured


def test_run_evaluation_threads_postgres_source_type_to_assessor() -> None:
    # Tier-3 #25: a PostgreSQL array column is graded by the PG assessor rules (re-typed to
    # jsonb: MANUAL) but would be assessed differently under MySQL rules, so this proves
    # run_evaluation built the assessor with the SESSION's source_type (POSTGRES).
    inv = SourceInventory(tables=[TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="tags", mysql_type="integer[]", nullable=True),
        ],
        primary_key=["id"],
    )])
    src_factory, _ = _source_factory(inv)
    tgt_factory, _ = _target_factory(_empty_target())
    pg_inputs = EvaluationInputs(
        source_config=SourceConnectionConfig(
            host="h", port=5432, database="app", username="u",
            source_type=SourceType.POSTGRES,
        ),
        source_password=SecretValue("pw"),
        target_config=_target_config(),
        aws_profile=None,
    )
    result = run_evaluation(
        pg_inputs, introspector_factory=src_factory, target_browser_factory=tgt_factory
    )
    rule_ids = {it.rule_id for it in result.assessment.items} | {
        c.rule_id for it in result.assessment.items for c in (it.concerns or [])
    }
    assert "PG_UNSUPPORTED_TYPE" in rule_ids  # PG rules ran (not MySQL)
    item = next(it for it in result.assessment.items if it.object_name == "t")
    assert item.classification is Classification.MANUAL


def test_an_evaluation_over_zero_tables_says_check_the_database() -> None:
    """Zero migratable tables is almost never a finding -- it is the wrong database.

    The old behaviour was the dangerous kind of quiet: Evaluation ran to completion over
    nothing and printed a confident readiness band, so the operator's next signal was an
    empty migration. PostgreSQL is where this bites (a connection is scoped to ONE database
    and Aurora always provides an empty ``postgres`` beside the real one), and it is worst
    when the database and the schema share a name -- the existing hint cannot tell the
    operator which of the two they typed.
    """
    from dsql_migrator.core.assessor import AssessmentReport
    from dsql_migrator.core.models import (
        ColumnDef,
        SourceInventory,
        TableDef,
        TargetInventory,
    )
    from dsql_migrator.ui.evaluation import EvaluationResult, empty_inventory_message

    def result(tables, siblings):
        return EvaluationResult(
            inventory=SourceInventory(
                tables=tables, views=[], sibling_databases=siblings
            ),
            assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(),
            target_conflicts=[],
        )

    body = empty_inventory_message(result([], ["ecommerce", "analytics"]))
    assert body is not None
    assert "nothing to migrate" in body
    # It must name the DATABASE/schema confusion, which is the actual mistake.
    assert "not the schema name" in body
    # ...and the alternatives, so the operator has a next step rather than a dead end.
    assert "ecommerce" in body and "analytics" in body

    # Unreadable siblings must not block the warning -- the cause still needs saying.
    bare = empty_inventory_message(result([], []))
    assert bare is not None and "not the schema name" in bare
    assert "Other databases" not in bare

    # A populated inventory says nothing at all (no alarm on the normal case).
    table = TableDef(
        name="app.t",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)],
        primary_key=["id"],
        indexes=[],
    )
    assert empty_inventory_message(result([table], ["postgres"])) is None


def test_the_empty_inventory_notice_is_rendered_under_the_score() -> None:
    """Pins the WIRING: the message helper is useless if nothing renders it.

    Placed under the score card because it is the SCORE that needs qualifying -- an
    evaluation over zero tables still produces a confident band.
    """
    import inspect

    from dsql_migrator.ui import evaluation as ev

    body = inspect.getsource(ev._render_result)
    assert "_render_empty_inventory_notice(ui, result)" in body, (
        "the empty-inventory warning is no longer rendered, so a wrong-database run is "
        "silent again"
    )
    notice = inspect.getsource(ev._render_empty_inventory_notice)
    assert 'tone="warning"' in notice, "a wrong-database run must not read as informational"


def test_postgres_reports_sibling_databases_and_mysql_reports_none() -> None:
    """Only a one-database-per-connection engine has this trap; MySQL must stay silent."""
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.source_dialect import dialect_for

    class _Rows:
        def mappings(self):
            return [{"datname": "ecommerce"}, {"datname": "analytics"}]

    class _Conn:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement, *a, **k):
            self.statements.append(str(statement))
            return _Rows()

    pg_conn = _Conn()
    assert dialect_for(SourceType.POSTGRES).sibling_databases(pg_conn) == [
        "ecommerce",
        "analytics",
    ]
    sql = pg_conn.statements[0]
    # Only connectable, non-template databases, and never the one already connected.
    assert "datallowconn" in sql and "NOT datistemplate" in sql
    assert "current_database()" in sql

    # MySQL assesses the whole cluster when Database is blank, so it has no equivalent.
    assert dialect_for(SourceType.MYSQL).sibling_databases(_Conn()) == []

    # A failure is best-effort: a hint must never break introspection.
    class _Broken:
        def execute(self, *a, **k):
            raise RuntimeError("permission denied for table pg_database")

    assert dialect_for(SourceType.POSTGRES).sibling_databases(_Broken()) == []


# ---------------------------------------------------------------------------
# A target catalog read from a DIFFERENT cluster must not be trusted
# ---------------------------------------------------------------------------


class _NoticeUi:
    """Fake NiceGUI covering what render_notice touches (icon / column / spinner)."""

    def __init__(self) -> None:
        self.text: list[str] = []

    def label(self, value=""):
        self.text.append(str(value))
        return self

    def badge(self, value=""):
        self.text.append(str(value))
        return self

    def icon(self, *_a, **_k):
        return self

    def spinner(self, *_a, **_k):
        return self

    def button(self, value="", **_k):
        self.text.append(str(value))
        return self

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        return self

    def tooltip(self, *_a, **_k):
        return self

    def row(self, *_a, **_k):
        return self

    def column(self, *_a, **_k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


class _Endpoint:
    def __init__(self, cluster_endpoint) -> None:
        self.cluster_endpoint = cluster_endpoint


def _result_stamped(endpoint, *, target=None, conflicts=None) -> EvaluationResult:
    from dsql_migrator.core.models import AssessmentReport, SourceInventory

    return EvaluationResult(
        inventory=SourceInventory(tables=[], views=[]),
        assessment=AssessmentReport(items=[]),
        target_inventory=target if target is not None else _empty_target(),
        target_conflicts=list(conflicts or []),
        target_endpoint=endpoint,
    )


def test_stale_when_the_stamped_cluster_is_not_the_connected_one() -> None:
    from dsql_migrator.ui.evaluation import target_inventory_is_stale

    result = _result_stamped("a.dsql.us-east-1.on.aws")
    assert target_inventory_is_stale(result, _Endpoint("b.dsql.us-east-1.on.aws"))
    assert not target_inventory_is_stale(result, _Endpoint("a.dsql.us-east-1.on.aws"))
    # Normalised the same way the Connect screen normalises what is typed -- on BOTH
    # sides. A one-sided normalisation passes whenever the fixture happens to be
    # lowercase already, so the stamp here is deliberately mixed-case and padded.
    mixed = _result_stamped("  A.Dsql.US-East-1.On.AWS  ")
    assert not target_inventory_is_stale(mixed, _Endpoint("a.dsql.us-east-1.on.aws"))
    assert not target_inventory_is_stale(
        mixed, _Endpoint("  A.DSQL.US-EAST-1.ON.AWS  ")
    )
    assert not target_inventory_is_stale(
        result, _Endpoint("  A.DSQL.US-EAST-1.ON.AWS  ")
    )
    assert target_inventory_is_stale(mixed, _Endpoint("b.dsql.us-east-1.on.aws"))


def test_an_unanswerable_question_is_never_reported_as_stale() -> None:
    """Never invalidate a catalog we merely failed to identify."""
    from dsql_migrator.ui.evaluation import target_inventory_is_stale

    assert not target_inventory_is_stale(None, _Endpoint("b.dsql.us-east-1.on.aws"))
    # No stamp = unknown provenance (a pre-0.1.524 snapshot or a test fixture).
    assert not target_inventory_is_stale(
        _result_stamped(None), _Endpoint("b.dsql.us-east-1.on.aws")
    )
    assert not target_inventory_is_stale(_result_stamped("a.dsql..."), None)
    assert not target_inventory_is_stale(_result_stamped("a.dsql..."), _Endpoint(""))


def test_an_evaluation_run_stamps_the_cluster_it_browsed() -> None:
    """Everything downstream keys off this stamp, so an unstamped run disables it all."""
    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_target_with_tables("orders"))
    inputs = _inputs()

    result = run_evaluation(
        inputs,
        introspector_factory=src_factory,
        target_browser_factory=tgt_factory,
    )
    assert result.target_endpoint == inputs.target_config.cluster_endpoint
    assert result.target_endpoint, "must not be blank"
    # ...and the freshly-stamped result is not reported stale against that same target.
    from dsql_migrator.ui.evaluation import target_inventory_is_stale

    assert not target_inventory_is_stale(result, inputs.target_config)


def test_a_stale_target_suppresses_the_false_all_clear() -> None:
    """The worst display symptom: a green "No conflicts" about a cluster never read.

    Conflicts are computed against the ASSESSED cluster, so an empty list from cluster A
    rendered "No source objects conflict with existing target objects" even when the
    connected cluster B already held those very tables.
    """
    from dsql_migrator.ui.evaluation import _render_target

    ui = _NoticeUi()
    _render_target(
        ui,
        _result_stamped("a.dsql.us-east-1.on.aws", conflicts=[]),
        live_endpoint="b.dsql.us-east-1.on.aws",
    )
    joined = " ".join(ui.text)
    assert "No conflicts on the target" not in joined, joined
    assert "out of date" in joined, joined
    # Both endpoints named, so the operator knows which cluster they are looking at.
    assert "a.dsql.us-east-1.on.aws" in joined and "b.dsql.us-east-1.on.aws" in joined
    # The counts would describe the wrong cluster, so they are suppressed too.
    assert "Target catalog:" not in joined, joined


def test_a_current_target_still_renders_the_normal_subsection() -> None:
    from dsql_migrator.ui.evaluation import _render_target

    ui = _NoticeUi()
    _render_target(
        ui,
        _result_stamped("a.dsql.us-east-1.on.aws", conflicts=[]),
        live_endpoint="a.dsql.us-east-1.on.aws",
    )
    joined = " ".join(ui.text)
    assert "Target catalog:" in joined
    assert "out of date" not in joined


def _screen_runner(introspector_factory, target_browser_factory, *, manager=None):
    """The REAL Evaluation runner (not a re-implementation), over fakes."""
    from dsql_migrator.ui.evaluation import EvaluationStore, build_evaluation_screen
    from dsql_migrator.ui.session import SessionStore

    store = SessionStore()
    session = store.get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="db", database="shop"), None)
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    eval_store = EvaluationStore()
    manager = manager or JobManager()
    _content, runner = build_evaluation_screen(
        store,
        "s1",
        job_manager=manager,
        eval_store=eval_store,
        introspector_factory=introspector_factory,
        target_browser_factory=target_browser_factory,
    )
    return runner, manager, eval_store.get_or_create("s1"), session


def test_the_real_runner_reports_each_source_table_and_records_done() -> None:
    from dsql_migrator.ui.workflow import WorkflowStep, get_status

    src_factory, _ = _source_factory(_inventory_with_fk())
    tgt_factory, _ = _target_factory(_empty_target())
    runner, manager, state, session = _screen_runner(src_factory, tgt_factory)
    seen: list[str] = []
    original = state.set_progress

    def _record(pct, message, *, token=None):
        seen.append(message)
        original(pct, message, token=token)

    state.set_progress = _record  # type: ignore[method-assign]
    runner()
    assert manager.wait(state.job_id, timeout=5.0)
    assert "Reading the source catalog: 2 of 2 tables" in seen
    # Recorded by the worker itself: the screen's poll is not needed for DONE.
    assert get_status(session.workflow, WorkflowStep.EVALUATION) is StepStatus.DONE


def test_a_cancelled_evaluation_ends_cancelled_and_keeps_the_previous_report() -> None:
    import threading

    from dsql_migrator.ui.evaluation import _settle_unfinished_run

    gate = threading.Event()
    release = threading.Event()

    class _Slow:
        def introspect(self, conn, *, progress=None):
            gate.set()
            release.wait(5)
            progress(1, 2)  # the next table boundary: the cancel lands here
            raise AssertionError("not reached")

    tgt_factory, _ = _target_factory(_empty_target())
    runner, manager, state, _session = _screen_runner(lambda _p: _Slow(), tgt_factory)
    previous = object()
    state._result = previous  # a report from an earlier run
    runner()
    assert gate.wait(5)
    manager.request_cancel(state.job_id)
    release.set()
    assert manager.wait(state.job_id, timeout=5.0)
    assert manager.get_status(state.job_id).status == "CANCELLED"
    assert state.result is previous  # begin_run kept it; nothing replaced it
    assert _settle_unfinished_run(state, cancelled=True, error="") is StepStatus.DONE
    assert state.run_notice[1] == "Evaluation cancelled"


def test_a_second_run_is_refused_while_one_is_running() -> None:
    import threading

    release = threading.Event()

    class _Blocking:
        def introspect(self, conn, *, progress=None):
            release.wait(5)
            return SourceInventory()

    tgt_factory, _ = _target_factory(_empty_target())
    runner, manager, state, _session = _screen_runner(lambda _p: _Blocking(), tgt_factory)
    runner()
    first = state.job_id
    runner()  # a second click / another tab
    assert state.job_id == first
    release.set()
    assert manager.wait(first, timeout=5.0)


def test_a_superseded_or_retired_run_cannot_write_its_result() -> None:
    state = EvaluationState()
    old = state.begin_run()
    new = state.begin_run()
    assert state.set_result(object(), token=old) is False
    assert state.result is None
    kept = object()
    assert state.set_result(kept, token=new) is True
    state.retire_run()  # cancelled or reaped: a late return must not land
    assert state.set_result(object(), token=new) is False
    assert state.result is kept
    # Start over re-initialises the state in place: an old token never matches again.
    state.__init__()
    assert state.set_result(object(), token=new) is False


def test_a_failed_rerun_keeps_the_previous_report_and_the_step_done() -> None:
    from dsql_migrator.ui.evaluation import _settle_unfinished_run

    first_run = EvaluationState()
    assert _settle_unfinished_run(first_run, cancelled=False, error="boom") is StepStatus.FAILED
    assert first_run.error == "boom"
    rerun = EvaluationState()
    rerun._result = object()
    assert _settle_unfinished_run(rerun, cancelled=False, error="boom") is StepStatus.DONE
    assert rerun.run_notice == ("warning", "The re-run failed; the previous assessment is kept", "boom")
    assert rerun.error is None


def test_the_watchdog_reaps_an_evaluation_with_its_own_message() -> None:
    from dsql_migrator.ui.evaluation import EVALUATION_STALL_MESSAGE

    now = [0.0]
    manager = JobManager(stall_timeout_seconds=10, clock=lambda: now[0])
    manager.shutdown()  # this test drives the reap itself
    import threading

    release = threading.Event()
    job_id = manager.submit(lambda _h: release.wait(5), stall_message=EVALUATION_STALL_MESSAGE)
    import time

    deadline = time.monotonic() + 5
    while manager.get_status(job_id).status != "RUNNING" and time.monotonic() < deadline:
        time.sleep(0.01)
    now[0] = 60.0
    assert manager.reap_stalled_jobs() == [job_id]
    error = manager.get_error(job_id) or ""
    assert error.startswith("No progress for 10s while reading the source or target catalog")
    assert "Retry the failed tables" not in error
    release.set()


def test_a_leftover_poll_never_settles_or_retires_the_current_run() -> None:
    """Two tabs share a session: Tab 2's poll on the old job A retired the CURRENT run B,
    whose finished result was then dropped (step DONE with no report)."""
    from dsql_migrator.ui.evaluation import _install_poll_timer

    state = EvaluationState()
    old_token = state.begin_run()
    state.job_id, state.job_token = "job-a", old_token

    timers = []

    class _Ui:
        def timer(self, _interval, callback):
            timers.append(callback)

    class _Manager:
        def get_status(self, job_id):
            from types import SimpleNamespace

            return SimpleNamespace(status="CANCELLED")

        def get_error(self, _job_id):
            return None

    class _Session:
        workflow = None

        def set_workflow(self, workflow):
            raise AssertionError("a leftover poll must not settle the step")

    _install_poll_timer(_Ui(), _Manager(), _Session(), state, refresh=lambda: None)
    new_token = state.begin_run()  # Start over + a new run B
    state.job_id, state.job_token = "job-b", new_token
    timers[0]()  # Tab 2's poll on A ticks
    assert state.is_current(new_token)


def test_a_cancel_retires_the_run_so_a_late_result_is_not_recorded() -> None:
    state = EvaluationState()
    token = state.begin_run()
    state.job_token = token
    state.retire_run(state.job_token)  # what the Cancel button does
    assert state.set_result(object(), token=token) is False


# ---------------------------------------------------------------------------
# A large report: a bounded, lazily built object list (review scale-1 / ui-ux-6 / tests-9).
# Every row and finding card used to be built up front, collapsed ones included (Quasar
# keeps them in the DOM), and each filter click rebuilt the whole page: about 98,000
# elements and five seconds of the shared event loop for 1,000 tables.
# ---------------------------------------------------------------------------


def _bulk_items(
    count: int,
    *,
    kind: str = "TABLE",
    prefix: str = "app.t",
    classification: Classification = Classification.MANUAL,
    findings: int = 5,
) -> list:
    """``count`` assessed objects of one kind, each with ``findings`` findings (none for
    AUTO), named ``<prefix>00000`` upwards so name order is creation order."""
    from dsql_migrator.core.models import AssessmentConcern, AssessmentItem

    auto = classification is Classification.AUTO
    items = []
    for i in range(count):
        name = f"{prefix}{i:05d}"
        concerns = [] if auto else [
            AssessmentConcern(
                rule_id=f"RULE_{k}",
                classification=classification,
                risk=f"risk {k} of {name}",
                recommendation=f"fix {k} of {name}",
                effort=EffortLevel.MEDIUM,
            )
            for k in range(findings)
        ]
        items.append(
            AssessmentItem(
                object_name=name,
                rule_id="RULE_0" if concerns else "COMPATIBLE",
                classification=classification,
                kind=kind,
                effort=None if auto else EffortLevel.MEDIUM,
                concerns=concerns,
            )
        )
    return items


def _bulk_result(items: list) -> EvaluationResult:
    from dsql_migrator.core.models import AssessmentReport

    return EvaluationResult(
        inventory=SourceInventory(
            tables=[
                TableDef(
                    name="app.t",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
            ]
        ),
        assessment=AssessmentReport.from_items(items),
        target_inventory=TargetInventory(),
        target_conflicts=[],
    )


def _rows(texts: list) -> list:
    """The object rows a render emitted (their names), in order."""
    return [t for t in texts if t.startswith("app.")]


def _mixed(tables: int) -> list:
    """``tables`` MANUAL tables, 60 MANUAL views and 40 UNSUPPORTED triggers, each object
    with five findings: three open sections, two of them longer than a page."""
    return (
        _bulk_items(tables)
        + _bulk_items(60, kind="VIEW", prefix="app.v")
        + _bulk_items(
            40, kind="TRIGGER", prefix="app.trg", classification=Classification.UNSUPPORTED
        )
    )


def test_the_result_screen_builds_a_bounded_number_of_elements_at_5000_objects() -> None:
    """What the screen builds must not grow with the source: 5,000 objects cost exactly
    what 1,000 do, no finding card is built before its row is opened, and each open
    section shows one page."""
    from dsql_migrator.ui.evaluation import OBJECTS_PER_PAGE, _render_result

    def render(tables: int) -> _ItemUi:
        ui = _ItemUi()
        _render_result(ui, _bulk_result(_mixed(tables)), eval_state=EvaluationState())
        return ui

    small, large = render(900), render(4_900)
    assert large.created == small.created, (small.created, large.created)
    # The eager list built ~460,000 elements here. One page per open section (140 header-
    # only rows), plus the score card, chart, filters and target analysis, is ~800; cards
    # built up front for those rows alone would add ~10,000, unpaged rows ~30,000.
    assert large.created < 2_000, large.created
    rows = _rows(large.texts)
    assert len([r for r in rows if r.startswith("app.t0")]) == OBJECTS_PER_PAGE
    assert len([r for r in rows if r.startswith("app.v")]) == OBJECTS_PER_PAGE
    assert len([r for r in rows if r.startswith("app.trg")]) == 40
    assert not [t for t in large.texts if t.startswith("risk ")], "cards built up front"
    # A section with more than one page says where the rest is.
    assert "Showing 1 to 50 of 4,900. The exports list every object." in large.texts
    assert len(large.paginations) == 2  # Tables and Views; 40 triggers fit one page


def test_the_exports_list_every_object_while_the_screen_shows_one_page() -> None:
    from dsql_migrator.ui.evaluation import OBJECTS_PER_PAGE, _render_result

    result = _bulk_result(_bulk_items(120))
    ui = _ItemUi()
    _render_result(ui, result, eval_state=EvaluationState())
    assert len(_rows(ui.texts)) == OBJECTS_PER_PAGE
    for fmt in ("json", "text", "html"):
        content = assessment_download(result, fmt).content
        missing = [
            i.object_name for i in result.assessment.items if i.object_name not in content
        ]
        assert not missing, (fmt, missing[:3])


def test_a_row_builds_its_finding_cards_once_on_its_first_expand() -> None:
    from dsql_migrator.ui.evaluation import _render_assessment_item

    item = _five_rule_item()
    ui = _ItemUi()
    _render_assessment_item(ui, item)
    assert not [c.rule_id for c in item.concerns if c.rule_id in ui.texts], ui.texts
    collapsed = ui.created
    ui.open(item.object_name)
    assert all(c.rule_id in ui.texts for c in item.concerns)
    opened = ui.created
    assert opened > collapsed
    # The value change fires on every open; what was built is kept, not built again.
    _element, on_change, _start = ui.expansions[0]
    on_change(_Changed(False))
    on_change(_Changed(True))
    assert ui.created == opened


def test_a_closed_section_builds_no_rows_until_it_is_opened() -> None:
    """An all-AUTO section starts closed -- and empty, since Quasar would ship hidden rows."""
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import OBJECTS_PER_PAGE, _render_assessment

    report = AssessmentReport.from_items(
        _bulk_items(500, classification=Classification.AUTO)
    )
    ui = _ItemUi()
    _render_assessment(ui, report, eval_state=EvaluationState())
    assert _rows(ui.texts) == []
    ui.open("Tables")
    assert len(_rows(ui.texts)) == OBJECTS_PER_PAGE


def test_a_long_section_shows_one_page_at_a_time_most_critical_first() -> None:
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import _render_assessment

    report = AssessmentReport.from_items(
        _bulk_items(110)
        + _bulk_items(10, prefix="app.u", classification=Classification.UNSUPPORTED)
    )
    state = EvaluationState()
    ui = _ItemUi()
    _render_assessment(ui, report, eval_state=state)
    first_page = _rows(ui.texts)
    assert first_page == (
        [f"app.u{i:05d}" for i in range(10)] + [f"app.t{i:05d}" for i in range(40)]
    )
    ((_element, turn, bounds, value),) = ui.paginations
    assert (bounds, value) == ((1, 3), 1)
    assert "Showing 1 to 50 of 120. The exports list every object." in ui.texts

    mark = len(ui.texts)
    turn(_Changed(2))
    assert _rows(ui.texts[mark:]) == [f"app.t{i:05d}" for i in range(40, 90)]
    assert "Showing 51 to 100 of 120. The exports list every object." in ui.texts[mark:]
    assert state.list_pages["TABLE"] == 2
    # Turning a page redraws that section only, not the list (nor the page).
    assert ui.refreshes == 0
    mark = len(ui.texts)
    turn(_Changed(2))  # the page it already shows
    assert ui.texts[mark:] == []

    # A redraw of the screen keeps the page (it lives on the state).
    again = _ItemUi()
    _render_assessment(again, report, eval_state=state)
    assert _rows(again.texts) == [f"app.t{i:05d}" for i in range(40, 90)]


def test_filters_and_the_name_search_redraw_only_the_object_list() -> None:
    """They used to call the workflow shell's refresh, which rebuilt the whole page --
    sidebar, diagram, score, chart and every row -- on each click."""
    import inspect

    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import _render_assessment, _render_result

    assert "refresh" not in inspect.signature(_render_assessment).parameters
    assert "refresh" not in inspect.signature(_render_result).parameters
    report = AssessmentReport.from_items(
        _bulk_items(300, classification=Classification.AUTO)
        + _bulk_items(
            20, kind="TRIGGER", prefix="app.trg", classification=Classification.UNSUPPORTED
        )
    )
    state = EvaluationState()
    state.list_pages["TABLE"] = 3
    ui = _ItemUi()
    _render_assessment(ui, report, eval_state=state)
    assert "320 objects" in ui.texts
    ((_box, on_name, placeholder),) = ui.inputs
    assert placeholder == "Filter objects by name"  # Schema Conversion's wording

    rendered = ui.created
    ui.selects["Classification"](_Changed("UNSUPPORTED"))
    assert (state.classification_filter, ui.refreshes) == ("UNSUPPORTED", 1)
    assert "20 of 320 objects match" in ui.texts
    assert ui.created - rendered < 300, ui.created - rendered  # the list, not the page
    assert "TABLE" not in state.list_pages  # a different set: pages start again

    # The search ignores case and surrounding spaces, and opens the section it matches in
    # (the all-AUTO Tables section starts closed).
    ui.selects["Classification"](_Changed("ALL"))
    mark = len(ui.texts)
    on_name(_Changed("  APP.T00042 "))
    assert state.name_filter == "APP.T00042"
    assert _rows(ui.texts[mark:]) == ["app.t00042"]
    assert "1 of 320 objects match" in ui.texts[mark:]
    # The filter bar is outside the redrawn region: the box under the cursor survives.
    assert len(ui.inputs) == 1

    on_name(_Changed("no such object"))
    assert ui.texts[-1] == "No objects match these filters."
    refreshes, mark = ui.refreshes, len(ui.texts)
    ui.buttons["Clear filters"][-1]()
    assert (state.classification_filter, state.effort_filter, state.name_filter) == (
        "ALL",
        "ALL",
        "",
    )
    assert ui.refreshes == refreshes + 1
    redrawn = ui.texts[mark:]
    assert redrawn[0] == "320 objects" and "No objects match these filters." not in redrawn


def test_a_second_browser_tab_redraws_for_a_filter_or_page_the_first_already_set() -> None:
    """The filters and pages live on the session's state, which all its browser tabs share,
    so a tab must redraw for its own control's change even when another tab already put
    the state at that value -- or its control shows the new value over the old list."""
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import _render_assessment

    report = AssessmentReport.from_items(
        _bulk_items(120)
        + _bulk_items(
            20, kind="TRIGGER", prefix="app.trg", classification=Classification.UNSUPPORTED
        )
    )
    state = EvaluationState()
    tab_a, tab_b = _ItemUi(), _ItemUi()
    _render_assessment(tab_a, report, eval_state=state)
    _render_assessment(tab_b, report, eval_state=state)

    ((_a, turn_a, _bounds, _value),) = tab_a.paginations
    ((_b, turn_b, _bounds, _value),) = tab_b.paginations
    turn_a(_Changed(2))
    mark = len(tab_b.texts)
    turn_b(_Changed(2))
    assert _rows(tab_b.texts[mark:])[:1] == ["app.t00050"]

    tab_a.selects["Classification"](_Changed("UNSUPPORTED"))
    mark = len(tab_b.texts)
    tab_b.selects["Classification"](_Changed("UNSUPPORTED"))
    assert tab_b.refreshes == 1
    assert "20 of 140 objects match" in tab_b.texts[mark:]


def test_a_tab_shows_the_filters_another_tab_set_once_it_redraws() -> None:
    """Tab A narrowed to Unsupported; tab B's search then redrew its list filtered by that
    too, while its own dropdown still read All -- and picking All fired no change."""
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import _render_assessment

    report = AssessmentReport.from_items(
        _bulk_items(30)
        + _bulk_items(5, kind="TRIGGER", prefix="app.trg", classification=Classification.UNSUPPORTED)
    )
    state = EvaluationState()
    tab_a, tab_b = _ItemUi(), _ItemUi()
    _render_assessment(tab_a, report, eval_state=state)
    _render_assessment(tab_b, report, eval_state=state)
    tab_a.selects["Classification"](_Changed("UNSUPPORTED"))
    ((_box, on_name, _placeholder),) = tab_b.inputs
    on_name(_Changed("trg"))
    assert tab_b.select_elements["Classification"].value == "UNSUPPORTED"
    assert state.name_filter == "trg"


def test_counts_beside_each_other_share_one_number_format() -> None:
    from dsql_migrator.ui.evaluation import assessment_kind_summary

    items = _bulk_items(5000) + _bulk_items(
        1200, kind="TABLE", prefix="app.x", classification=Classification.UNSUPPORTED
    )
    assert assessment_kind_summary(items) == "1,200 Unsupported · 5,000 Review needed"


def test_a_new_result_opens_every_group_on_its_first_page() -> None:
    state = EvaluationState()
    token = state.begin_run()
    state.list_pages["TABLE"] = 3
    assert state.set_result(object(), token=token)
    assert state.list_pages == {}


def test_assessment_page_slices_and_clamps() -> None:
    from dsql_migrator.ui.evaluation import assessment_page

    items = list(range(120))
    assert assessment_page(items, 1, size=50) == (list(range(50)), 1, 3)
    assert assessment_page(items, 3, size=50) == (list(range(100, 120)), 3, 3)
    # Out of range (a section that shrank, a stale page) is clamped, never empty.
    assert assessment_page(items, 9, size=50)[1:] == (3, 3)
    assert assessment_page(items, 0, size=50)[1:] == (1, 3)
    assert assessment_page([], 2, size=50) == ([], 1, 1)


def test_filter_assessment_items_by_name_ignores_case_and_spaces() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _bulk_items(3) + _bulk_items(
        2, prefix="shop.orders_", classification=Classification.UNSUPPORTED
    )
    kept = filter_assessment_items(items, name=" ORDERS ")
    assert [i.object_name for i in kept] == ["shop.orders_00000", "shop.orders_00001"]
    assert filter_assessment_items(items, name="") == items
    # Combined with the other filters (AND).
    assert filter_assessment_items(items, name="app.", classification="UNSUPPORTED") == []


def test_every_export_says_what_was_assessed_against_what_and_when() -> None:
    """No export said which source or target it described, when it was made or by which
    version, or what its effort figures meant; JSON and text also dropped the target's
    existing names the screen shows."""
    from dsql_migrator import __version__

    result = replace(_result(), target_conflicts=["app.orders"])
    assert result.source_label and result.source_label.endswith(":3306/app")
    assert result.assessed_at and result.assessed_at.endswith(" UTC")

    data = json.loads(assessment_download(result, "json", exported_at=_EXPORTED_AT).content)
    meta = data["metadata"]
    assert meta["source"] == result.source_label
    assert meta["exported"] == _EXPORTED_AT and meta["assessed"] == result.assessed_at
    assert meta["tool_version"] == __version__
    assert meta["readiness"].endswith("/ 100 readiness)") and "SIMPLE under 2 hours" in meta["effort_scale"]
    assert data["target"]["conflicts"] == ["app.orders"]
    assert data["target"]["endpoint"] == result.target_endpoint
    # The model's own keys stay where they were.
    assert {"items", "summary", "effort_summary"} <= set(data)

    text = assessment_download(result, "text", exported_at=_EXPORTED_AT).content
    assert f"Source:" in text and result.source_label in text
    assert f"Tool version: {__version__}" in text
    assert "Already on the target (1):\n- app.orders" in text

    markup = assessment_download(result, "html", exported_at=_EXPORTED_AT).content
    assert '<table class="meta">' in markup and "<th>Exported</th><td>2026-10-17 01:05:09 UTC</td>" in markup


def test_an_export_never_lists_a_report_marker() -> None:
    from dsql_migrator.core.assessor_postgres import with_pg_catalog_facts
    from dsql_migrator.core.catalog_postgres import PgCatalogFacts, RowSecurityFacts

    result = _result()
    marked = replace(
        result,
        assessment=with_pg_catalog_facts(
            result.assessment, PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=())
        ),
    )
    assert any(i.rule_id == "PG_CATALOG_CHECKED" for i in marked.assessment.items)
    data = json.loads(assessment_download(marked, "json", exported_at=_EXPORTED_AT).content)
    assert all(item["rule_id"] != "PG_CATALOG_CHECKED" for item in data["items"])
    assert "PG_CATALOG_CHECKED" not in assessment_download(marked, "text").content
