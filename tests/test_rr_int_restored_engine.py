# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A RESTORED PostgreSQL session keeps its engine past the Data Migration screen and Cut over.

An older snapshot with no source host restores the engine WITHOUT a ``source_config``
(``session_persistence.apply_session_snapshot`` -> ``set_restored_source_type``).
tests/test_rr_a2_restored_engine.py pins the Cut over and Data Migration reads; these pin
the rest of the app, each of which still read the engine off ``source_config`` and so took
the restored PostgreSQL session for MySQL.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

from dsql_migrator.core.models import SourceConnectionConfig, SourceType


def _fresh_states():
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    return (
        SessionStore().get_or_create("s1"),
        EvaluationStore().get_or_create("s1"),
        SchemaConversionStore().get_or_create("s1"),
        DataMigrationStore().get_or_create("s1"),
    )


# -- The next save keeps the restored engine --------------------------------------------------


def test_a_restored_postgresql_engine_survives_the_next_save_and_restore() -> None:
    # The first persist poll after a restore re-captures the session. Capture read the
    # engine from a live source_config only, so it saved source_type=None over the old
    # snapshot and the restart after that brought the session back as MySQL.
    from dsql_migrator.ui import session_persistence as sp
    from dsql_migrator.ui.data_migration import session_source_type

    session, ev, cv, dm = _fresh_states()
    sp.apply_session_snapshot(sp.SessionSnapshot(session_id="s1", source_type="postgres"),
                              session, ev, cv, dm)
    assert session.source_config is None  # the premise: a host-less (older) snapshot
    assert session_source_type(session) is SourceType.POSTGRES

    saved = sp.capture_session_snapshot("s1", session, ev, cv, dm)
    assert saved.source_type == "postgres"
    again, ev2, cv2, dm2 = _fresh_states()
    sp.apply_session_snapshot(saved, again, ev2, cv2, dm2)
    assert session_source_type(again) is SourceType.POSTGRES


def test_a_live_source_still_decides_the_saved_engine_and_no_source_saves_none() -> None:
    from dsql_migrator.ui import session_persistence as sp

    session, ev, cv, dm = _fresh_states()
    assert sp.capture_session_snapshot("s1", session, ev, cv, dm).source_type is None
    session.set_source(SourceConnectionConfig(source_type=SourceType.MYSQL, host="h",
                                              database="d", username="u"), None)
    session.set_restored_source_type(SourceType.POSTGRES)  # a stale hint: the live one wins
    assert sp.capture_session_snapshot("s1", session, ev, cv, dm).source_type == "mysql"


# -- The AI DBA chats ---------------------------------------------------------------------------


def _restored_pg(store, *, ai: bool = True):
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.ai_assist import build_ai_assist_config

    session = store.get_or_create("s1")
    session.set_restored_source_type(SourceType.POSTGRES)
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                              region="us-east-1"))
    if ai:
        session.set_ai_assist(build_ai_assist_config(enabled=True))
    assert session.source_config is None
    return session


def _fake_nicegui(monkeypatch):
    import sys
    import types

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
    return ui


def _pg_evaluation(eval_store, *, source_type=SourceType.POSTGRES):
    from dsql_migrator.core.models import AssessmentReport, TargetInventory
    from dsql_migrator.ui.evaluation import EvaluationResult
    from tests.test_rr_a2_restored_engine import _pg_inventory

    inventory = _pg_inventory()
    eval_store.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[],
        source_label="PostgreSQL 16 at a.example:5432/shop",
        target_endpoint="c.dsql.us-east-1.on.aws", source_type=source_type,
    ))
    return inventory


def test_schema_conversions_ai_dba_chats_ground_a_restored_session_as_postgresql(monkeypatch):
    # The object browser on the same render already gets POSTGRES (session_source_type);
    # the per-object conversion chat and the reimplementation chat were told "MySQL".
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui import schema_conversion as sc
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.session import SessionStore

    _fake_nicegui(monkeypatch)
    store, eval_store, conv_store = SessionStore(), EvaluationStore(), sc.SchemaConversionStore()
    _restored_pg(store)
    inventory = _pg_evaluation(eval_store)
    browser: dict = {}
    real_browser = sc._render_browser_and_preview
    monkeypatch.setattr(sc, "_render_browser_and_preview",
                        lambda *a, **k: browser.update(k) or real_browser(*a, **k))
    engines: list = []
    for name in ("build_conversion_chat_system", "build_reimplementation_chat_system"):
        real = getattr(sc, name)

        def _spy(*a, source_engine=None, _real=real, _name=name, **k):
            engines.append((_name, source_engine))
            return _real(*a, source_engine=source_engine, **k)

        monkeypatch.setattr(sc, name, _spy)
    opened: list = []
    content, _runner = sc.build_schema_conversion_screen(
        store, "s1", job_manager=JobManager(), eval_store=eval_store, conv_store=conv_store,
        open_ai_scope=lambda **kw: opened.append(kw))
    content(lambda: None)
    assert browser["source_type"] is SourceType.POSTGRES

    browser["on_ai_chat"](inventory.tables[0].name, "SRC", "DDL")
    browser["on_reimplement_chat"]()
    assert len(opened) == 2
    assert engines == [("build_conversion_chat_system", "PostgreSQL"),
                       ("build_reimplementation_chat_system", "PostgreSQL")], engines
