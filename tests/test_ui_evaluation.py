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


def test_build_assessment_chart_data_groups_by_kind_and_bucket() -> None:
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
    # Kinds are ordered most-impactful first (highest manual-work share):
    # TRIGGER is 100% manual (1/1) vs TABLE 50% (1/2), so TRIGGER comes first.
    assert data.kinds == ["TRIGGER", "TABLE"]
    assert data.auto == [0, 1]
    assert data.simple == [0, 1]
    assert data.medium == [0, 0]
    assert data.significant == [1, 0]


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
    assert filter_assessment_items(items, mode="ALL") == items
    # An unknown mode falls back to ALL.
    assert filter_assessment_items(items, mode="bogus") == items


def test_filter_assessment_items_attention_excludes_auto() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    names = [
        item.object_name
        for item in filter_assessment_items(_filter_items(), mode="ATTENTION")
    ]
    assert names == ["fk_table", "no_pk"]  # AUTO ('clean') excluded; order kept


def test_filter_assessment_items_specific_classification() -> None:
    from dsql_migrator.ui.evaluation import filter_assessment_items

    items = _filter_items()
    assert [i.object_name for i in filter_assessment_items(items, mode="AUTO")] == [
        "clean"
    ]
    assert [
        i.object_name for i in filter_assessment_items(items, mode="UNSUPPORTED")
    ] == ["no_pk"]
    assert [
        i.object_name for i in filter_assessment_items(items, mode="MANUAL")
    ] == ["fk_table"]


def test_evaluation_state_assessment_filter_defaults_to_all() -> None:
    assert EvaluationState().assessment_filter == "ALL"


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
