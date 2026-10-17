"""G81: the review of G68-G78 -- the catalog refreshes take everything they stamp before
they read, and step aside for the whole read."""

from __future__ import annotations

import asyncio
import sys
import types
from types import SimpleNamespace


def _run_sc_refresh(monkeypatch, during_read):
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.models import (
        AssessmentReport,
        ColumnDef,
        SourceConnectionConfig,
        SourceInventory,
        SourceType,
        TableDef,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import evaluation as ev
    from dsql_migrator.ui import schema_conversion as sc
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_data_migration import _ScreenUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    def table(db: str, name: str) -> TableDef:
        return TableDef(
            name=f"{db}.{name}",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        )

    store = SessionStore()
    eval_store = EvaluationStore()
    conv_store = sc.SchemaConversionStore()
    session = store.get_or_create("s1")
    source_a = SourceConnectionConfig(host="big-a.example", port=3306, database="shop")
    session.set_source(source_a, None)
    session.set_source_verified(True)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")
    )
    session.set_target_verified(True)
    eval_state = eval_store.get_or_create("s1")
    eval_state.set_result(
        EvaluationResult(
            inventory=SourceInventory(tables=[table("shop", "orders")]),
            assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(schemas=[]),
            target_conflicts=[],
            source_label="MySQL 8.0.39 at big-a.example:3306/shop",
        )
    )

    captured: dict = {}
    real = sc._render_browser_and_preview

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(sc, "_render_browser_and_preview", _spy)

    # The Evaluation re-run (of source B, chosen on Connect meanwhile) that finishes while
    # the refresh is still reading A's catalog.
    newer_run = EvaluationResult(
        inventory=SourceInventory(tables=[table("billing", "invoices")]),
        assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[],
        source_label="MySQL 8.0.39 at small-b.example:3306/billing",
        assessed_at="2026-10-17 06:00:00 UTC",
    )

    class _Introspector:
        last_vanished: list = []

        def introspect(self, config):
            assert config is source_a
            during_read(eval_state, newer_run)
            return SourceInventory(tables=[table("shop", "orders"), table("shop", "items")])

    monkeypatch.setattr(ev, "_default_introspector_factory", lambda _pw: _Introspector())

    content, _runner = sc.build_schema_conversion_screen(
        store, "s1", job_manager=JobManager(), eval_store=eval_store, conv_store=conv_store,
    )
    content(lambda: None)
    assert captured.get("on_refresh_source") is not None

    asyncio.run(captured["on_refresh_source"]())
    return eval_state, newer_run, [m for m, _ in ui.notified]



def test_a_schema_conversion_refresh_steps_aside_for_a_run_that_finished_during_the_read(
    monkeypatch,
) -> None:
    """old_result was read AFTER the catalog read: a re-run that finished meanwhile was
    replaced by the refresh, and the toast said 'Source browser refreshed.'"""

    def finish_a_run(eval_state, newer_run):
        assert eval_state.set_result(newer_run)

    eval_state, newer_run, notified = _run_sc_refresh(monkeypatch, finish_a_run)
    assert eval_state.result is newer_run
    assert "Source browser refreshed." not in notified
    assert any("changed while this refresh read the catalogs" in m for m in notified)


def test_a_schema_conversion_refresh_says_nothing_after_start_over(monkeypatch) -> None:
    def start_over(eval_state, _newer_run):
        eval_state.__init__()  # EvaluationStore.reset_in_place

    eval_state, _newer, notified = _run_sc_refresh(monkeypatch, start_over)
    assert eval_state.result is None
    assert "Source browser refreshed." not in notified


def test_a_data_migration_refresh_keeps_the_engine_it_read_with(monkeypatch) -> None:
    """The label was taken before the catalog read but the engine after it: another
    source tested on Connect meanwhile graded and titled A's inventory as PostgreSQL."""
    from dsql_migrator.core.assessor import SOURCE_VISIBILITY_RULE_ID
    from dsql_migrator.core.assessor_postgres import PG_CATALOG_UNREAD_RULE_ID
    from dsql_migrator.core.models import (
        AssessmentItem,
        AssessmentReport,
        Classification,
        ColumnDef,
        SourceConnectionConfig,
        SourceInventory,
        SourceType,
        TableDef,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui import evaluation as ev
    from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.workflow import StepStatus, WorkflowStep, with_status

    from tests.test_ui_data_migration import _ScreenUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    source_a = SourceConnectionConfig(host="mysql-a.example", port=3306, database="app")
    session.set_source(source_a, None)
    session.set_source_verified(True)
    session.set_source_version("8.0.39", "8.0.39", None)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")
    )
    session.set_target_verified(True)
    session.set_migration_type(MigrationType.FULL_LOAD_ONLY)
    session.set_workflow(
        with_status(session.workflow, WorkflowStep.SCHEMA_CONVERSION, StepStatus.DONE)
    )

    tables = [
        TableDef(
            name="app.orders",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        )
    ]
    gap = AssessmentItem(
        object_name="source listing", kind="DATABASE", rule_id=SOURCE_VISIBILITY_RULE_ID,
        classification=Classification.MANUAL, risk="the source user cannot list routines",
        recommendation="grant SHOW_ROUTINE",
    )
    evals = EvaluationStore()
    eval_state = evals.get_or_create("s1")
    eval_state.set_result(
        EvaluationResult(
            inventory=SourceInventory(tables=tables),
            assessment=AssessmentReport.from_items([gap]),
            target_inventory=TargetInventory(schemas=[]),
            target_conflicts=[],
            source_type=SourceType.MYSQL,
            target_endpoint="c.dsql.us-east-1.on.aws",
            source_label="MySQL 8.0.39 at mysql-a.example:3306/app",
        )
    )
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = ["table:app.orders"]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)

    captured: dict = {}
    real = dm._render_table_selection

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(dm, "_render_table_selection", _spy)

    read_configs: list = []

    class _Introspector:
        last_vanished: list = []

        def introspect(self, config):
            read_configs.append(config)
            # While the (long) catalog read of A runs, the operator tests PostgreSQL
            # source B on Connect (what on_test_source does: set_source, verified, version).
            session.set_source(
                SourceConnectionConfig(
                    source_type=SourceType.POSTGRES, host="pg-b.example", port=5432,
                    database="billing",
                ),
                None,
            )
            session.set_source_verified(True)
            session.set_source_version("16.4", "16.4", None)
            return SourceInventory(tables=tables)

    class _Browser:
        def browse(self, _config):
            return TargetInventory(schemas=[])

    monkeypatch.setattr(ev, "_default_introspector_factory", lambda _pw: _Introspector())
    monkeypatch.setattr(ev, "_default_target_browser_factory", lambda _p: _Browser())

    jobs = SimpleNamespace(get=lambda _id: None, snapshot=lambda _id: None)
    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs,
    )
    content(lambda: None)
    assert captured.get("on_refresh") is not None, "Refresh objects not offered"

    asyncio.run(captured["on_refresh"]())

    result = eval_state.result
    rules = {i.rule_id for i in result.assessment.items}
    assert read_configs[0] is source_a
    assert result.source_label == "MySQL 8.0.39 at mysql-a.example:3306/app"
    assert result.source_type is SourceType.MYSQL
    assert SOURCE_VISIBILITY_RULE_ID in rules  # A was re-read: its own probe is kept
    assert PG_CATALOG_UNREAD_RULE_ID not in rules
