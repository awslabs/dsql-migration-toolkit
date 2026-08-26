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

    def introspect(self, conn: SourceConnectionConfig) -> SourceInventory:
        self.calls.append(conn)
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
    # A table with a foreign key is classified MANUAL by the assessor, and a
    # table without a primary key is UNSUPPORTED, giving a non-trivial report.
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
    assert Classification.MANUAL in classifications
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

    # The FK table is SIMPLE; the no-PK table is MEDIUM. Both are counted in the
    # effort summary, and every non-AUTO item carries an effort.
    efforts = {item.object_name: item.effort for item in result.assessment.items}
    assert efforts["orders"] is EffortLevel.SIMPLE
    assert efforts["audit_log"] is EffortLevel.MEDIUM
    assert result.assessment.effort_summary[EffortLevel.SIMPLE] == 1
    assert result.assessment.effort_summary[EffortLevel.MEDIUM] == 1
    assert result.assessment.effort_summary[EffortLevel.SIGNIFICANT] == 0


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
        def introspect(self, conn: SourceConnectionConfig) -> SourceInventory:
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
                rule_id="FK_UNSUPPORTED",
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
            rule_id="FK_UNSUPPORTED",
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
            rule_id="FK_UNSUPPORTED",
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
    # A finished run (result present) is never reset, even if the job is gone.
    assert (
        reconcile_evaluation_step(
            StepStatus.IN_PROGRESS, job_alive=False, has_result=True
        )
        is None
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


def test_assessment_download_json_is_valid_and_named() -> None:
    download = assessment_download(_result(), "json")
    assert download.filename == "compatibility_assessment.json"
    assert download.media_type == "application/json"
    parsed = json.loads(download.content)
    assert "items" in parsed and "summary" in parsed
    assert "effort_summary" in parsed


def test_assessment_download_text_is_human_readable() -> None:
    download = assessment_download(_result(), "text")
    assert download.filename == "compatibility_assessment.txt"
    assert download.media_type == "text/plain"
    assert "Compatibility Assessment Report" in download.content
    assert "Estimated manual effort" in download.content


def test_assessment_download_html_is_named_and_typed() -> None:
    download = assessment_download(_result(), "html")
    assert download.filename == "compatibility_assessment.html"
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
    # Unknown kinds fall back to a title-cased label.
    assert kind_section_label("SEQUENCE") == "Sequence"


# ---------------------------------------------------------------------------
# The Evaluation row renders ONE block per matched rule. The joined
# risk/recommendation strings made a five-rule table unreadable: two run-on
# paragraphs, with the Nth risk in one and its fix in the other.
# ---------------------------------------------------------------------------


class _ItemUi:
    """A NiceGUI stand-in that records emitted text, badges, and separators."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.badges: list[str] = []
        self.separators = 0
        self.icons: list[str] = []
        self.classes: list[str] = []

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

    body = "\n".join(ui.texts)
    # Every rule id is shown, so the reader can see WHICH rules fired.
    for rule_id in ("FK_UNSUPPORTED", "AUTO_INCREMENT", "CI_COLLATION",
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
        rule_id="FK_UNSUPPORTED",
        classification=Classification.MANUAL,
        risk="a; b",
        recommendation="fix a; fix b",
        kind="TABLE",
    )
    ui = _ItemUi()
    _render_assessment_item(ui, legacy)
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
                        ColumnDef(name="amt", mysql_type="decimal(65,30)"),
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

    report = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name=f"{schema}.t",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
                for schema in ("ecommerce", "ecommerce_demo")
            ]
        )
    )
    cluster = next(i for i in report.items if i.kind == "DATABASE")

    ui = _ItemUi()
    _render_assessment_item(ui, cluster)
    # The category badge treatment, same as a table row.
    assert "1 Review needed" in ui.badges, ui.badges
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
    # Tier-3 #25: a PostgreSQL array column is UNSUPPORTED under PG assessor rules but would
    # be assessed differently under MySQL rules, so this proves run_evaluation built the
    # assessor with the SESSION's source_type (POSTGRES), not the MySQL default.
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
    assert item.classification is Classification.UNSUPPORTED
