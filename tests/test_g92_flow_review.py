"""G92: the review of G81/G85 -- overlapping source and target refreshes in Schema
Conversion, and the Start CDC / combined-panel notices about generated columns and
CDC-only blockers."""

from __future__ import annotations

import asyncio
import sys
import types
from types import SimpleNamespace

from dsql_migrator.core.models import ColumnDef, TableDef
from tests.test_g66_cdc_generated_time_review import _GENERATED_DDL, _catalog_factory, _orders


def _overlapping_refreshes(monkeypatch, slow: str):
    """Run 'Refresh source' and Generate's silent target re-browse together; ``slow`` names
    the read (``introspect`` / ``browse``) that is held until the other has landed."""
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.models import (
        AssessmentReport,
        SourceConnectionConfig,
        SourceInventory,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import evaluation as ev
    from dsql_migrator.ui import schema_conversion as sc
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_data_migration import _ScreenUi

    gate = asyncio.Event()

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            if getattr(fn, "__name__", "") == slow:
                await gate.wait()
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    def table(name: str) -> TableDef:
        return TableDef(name=f"shop.{name}",
                        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                        primary_key=["id"])

    store, eval_store, conv_store = SessionStore(), EvaluationStore(), sc.SchemaConversionStore()
    session = store.get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="a.example", port=3306, database="shop"), None)
    session.set_source_verified(True)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")
    )
    session.set_target_verified(True)
    eval_state = eval_store.get_or_create("s1")
    eval_state.set_result(EvaluationResult(
        inventory=SourceInventory(tables=[table("orders")]),
        assessment=AssessmentReport.from_items([]), target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[], source_label="MySQL 8.0.39 at a.example:3306/shop",
        target_endpoint="c.dsql.us-east-1.on.aws",
    ))
    fresh_target = TargetInventory.model_validate({"schemas": [{"name": "shop", "tables": [
        {"name": "orders", "schema_name": "shop", "kind": "TABLE"}]}]})

    class _Introspector:
        last_vanished: list = []

        def introspect(self, _config):
            return SourceInventory(tables=[table("orders"), table("items")])

    class _Browser:
        def browse(self, _config):
            return fresh_target

    monkeypatch.setattr(ev, "_default_introspector_factory", lambda _pw: _Introspector())
    monkeypatch.setattr(ev, "_default_target_browser_factory", lambda _p: _Browser())
    captured: dict = {}
    real = sc._render_browser_and_preview

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(sc, "_render_browser_and_preview", _spy)
    content, _runner = sc.build_schema_conversion_screen(
        store, "s1", job_manager=JobManager(), eval_store=eval_store, conv_store=conv_store,
    )
    content(lambda: None)
    first, second = (
        ("on_refresh_source", "on_sync_target") if slow == "introspect"
        else ("on_sync_target", "on_refresh_source")
    )

    async def main() -> None:
        held = asyncio.create_task(captured[first]())
        await asyncio.sleep(0)
        await captured[second]()
        gate.set()
        await held

    asyncio.run(main())
    return eval_state.result, fresh_target, [m for m, _ in ui.notified]


def test_a_long_source_refresh_keeps_its_read_over_generates_re_browse(monkeypatch) -> None:
    """Generate's target re-browse landed during a minutes-long source read, which was then
    thrown away: 'The Evaluation result changed ... Refresh again'."""
    result, fresh_target, notified = _overlapping_refreshes(monkeypatch, slow="introspect")
    assert [t.name for t in result.inventory.tables] == ["shop.orders", "shop.items"]
    assert result.target_inventory is fresh_target
    assert "Source browser refreshed." in notified
    assert not any("changed while this refresh" in m for m in notified)


def test_generates_re_browse_is_kept_over_a_source_refresh_that_landed_first(monkeypatch) -> None:
    """A source refresh that landed during Generate's re-browse silently dropped the fresh
    target catalog: the diffs went back to the older 'exists on target' verdicts."""
    result, fresh_target, _notified = _overlapping_refreshes(monkeypatch, slow="browse")
    assert [t.name for t in result.inventory.tables] == ["shop.orders", "shop.items"]
    assert result.target_inventory is fresh_target


def test_a_refresh_still_steps_aside_for_a_newer_evaluation() -> None:
    from dsql_migrator.ui.evaluation import refresh_rebase

    old = SimpleNamespace(inventory=object(), target_inventory=object())
    target_refreshed = SimpleNamespace(inventory=old.inventory, target_inventory=object())
    rerun = SimpleNamespace(inventory=object(), target_inventory=object())
    state = SimpleNamespace(result=target_refreshed)
    assert refresh_rebase(state, old, "inventory") is target_refreshed
    assert refresh_rebase(state, old, "target_inventory") is None
    state.result = rerun
    assert refresh_rebase(state, old, "inventory") is None
    state.result = None
    assert refresh_rebase(state, old, "inventory") is None


def _pg_probe(monkeypatch, tables, catalog, applied):
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.core.models import SourceType

    state = SimpleNamespace(applied_conversions=lambda: (applied, True))
    monkeypatch.setattr(cdc_ui, "_cdc_tables_for_config", lambda *_a: tables)
    monkeypatch.setattr(cdc_ui, "_cdc_fk_connection_factory",
                        lambda _s: _catalog_factory(catalog, ()))
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES))
    return cdc_ui._probe_cdc_generated_mismatch(state, session, None, None)


def test_a_target_computing_a_pg_source_generated_column_is_not_refused(monkeypatch) -> None:
    """A PostgreSQL publication never carries a generated column, yet the target computing
    one Schema Conversion made ordinary was refused as '428C9' with a DROP EXPRESSION fix
    that would leave it stale under CDC."""
    orders = _orders()
    orders.columns[3].generated = True
    orders.columns[3].generated_kind = "STORED"
    ordinary = _GENERATED_DDL.replace(" GENERATED ALWAYS AS ((price * qty)) STORED", "")
    computes = {"shop.orders": {"id": "", "price": "", "qty": "", "total": "s"}}
    applied = {"shop.orders": SimpleNamespace(target_ddl=ordinary)}
    assert _pg_probe(monkeypatch, [orders], computes, applied) == []
    # Still held where CDC would leave it stale: GENERATED in Schema Conversion, ordinary
    # on the target.
    plain = {"shop.orders": {"id": "", "price": "", "qty": "", "total": ""}}
    generated = {"shop.orders": SimpleNamespace(target_ddl=_GENERATED_DDL)}
    assert _pg_probe(monkeypatch, [orders], plain, generated) == [("shop.orders", ["total"], [])]


def test_the_start_cdc_refusal_lists_are_bounded_and_read_right() -> None:
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    differing = [(f"shop.t{i:04d}", ["total"], []) for i in range(2000)]
    pinned = {(t, "total") for t, _e, _l in differing}
    _t, _i, _h, body = cdc_ui._cdc_generated_mismatch_notice(
        differing, is_pg=True, source_generated=pinned
    )
    assert "and 1995 more, generated on the PostgreSQL source" in body
    assert body.count(".total") <= 10 and len(body) < 4000

    missing = cdc_ui._TARGET_TABLE_MISSING
    _t, _i, _h, one = cdc_ui._cdc_generated_mismatch_notice(
        [("shop.items", ["total"], missing)], cdc_only=True
    )
    assert "load it again the way it was first loaded" in one
    _t, _i, _h, two = cdc_ui._cdc_generated_mismatch_notice(
        [("shop.items", ["total"], missing), ("shop.orders", ["total"], missing)], cdc_only=True
    )
    assert "load them again the way they were first loaded" in two


def test_the_combined_panel_names_each_blocked_table_once(monkeypatch) -> None:
    """The 'CDC stream blocked' notice repeated one bare title per failing table."""
    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteCheckId,
        PrerequisiteReport,
        PrerequisiteResult,
        PrerequisiteStatus,
    )

    class _Ui:
        def row(self):
            return self

        def classes(self, *_a):
            return self

        def props(self, *_a):
            return self

        def badge(self, *_a):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    title = "Every column's type can be carried by CDC"
    results = [
        PrerequisiteResult(check_id=PrerequisiteCheckId.COLUMN_REPLICABLE, title=title,
                           status=PrerequisiteStatus.FAIL, required=True,
                           target=f"app.devices_{i}", detail="x")
        for i in range(8)
    ]
    results.append(results[0].model_copy())
    shown: list = []
    monkeypatch.setattr(dm, "render_notice", lambda _ui, **kw: shown.append(kw.get("body", "")))
    dm._render_prereq_phase_verdicts(_Ui(), PrerequisiteReport.build(MigrationMode.CDC, results))
    (text,) = shown
    assert f"{title} (app.devices_0); {title} (app.devices_1)" in text
    assert text.count("app.devices_0") == 1 and "app.devices_5" not in text
    assert "and 3 more" in text
