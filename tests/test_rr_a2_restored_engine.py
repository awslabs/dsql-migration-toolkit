# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A RESTORED PostgreSQL session keeps its engine at Cut over and on the foreign-key card.

An older snapshot with no source host restores the engine WITHOUT a ``source_config``
(``session_persistence.apply_session_snapshot`` -> ``set_restored_source_type``), so every
``session.source_config.source_type if ... else MYSQL`` read -- and every
``getattr(source_config, "source_type", MYSQL)`` -- took that PostgreSQL session for MySQL.
Measured consequences, each pinned below:

* Cut over's identity / foreign-key inputs (``_identity_sync_context``, which
  ``_cutover_fk_targets`` / ``_cutover_identity_tables`` read) and the cut-over foreign-key
  pass converted a PostgreSQL inventory with the MySQL converter (a PG ``timestamp`` became
  TIMESTAMPTZ, a timezone-semantics change).
* ``_cutover_fk_keys`` held Finish on the "MySQL CDC can break this key" set (a cascade
  InnoDB never logs) for a PostgreSQL source.
* The runbook copy named MySQL.
* The Data Migration foreign-key card read ``_src.source_type`` off a ``None`` config; the
  advisory count swallowed the AttributeError, so its denominator read 0 and the card
  hid every pending key.
* The AI DBA on the Full Load failure / quarantine chat and on the CDC dead-letter /
  schema-drift chat read ``getattr(session.source_config, "source_type", None)``;
  ``source_engine_word(None)`` is "MySQL", so it was told the source is MySQL.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
)

PARENT = "public.customers"
CHILD = "public.orders"
FK = "fk_orders_customer"


def _pg_inventory() -> SourceInventory:
    """A PostgreSQL schema whose conversion differs by engine (``timestamp``) and whose one
    foreign key cascades (a key a MySQL CDC stream can break, never a PostgreSQL one)."""
    return SourceInventory(tables=[
        TableDef(
            name=PARENT,
            columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)],
            primary_key=["id"],
        ),
        TableDef(
            name=CHILD,
            columns=[
                ColumnDef(name="id", mysql_type="integer", nullable=False),
                ColumnDef(name="customer_id", mysql_type="integer", nullable=True),
                ColumnDef(name="placed_at", mysql_type="timestamp", nullable=True),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKeyDef(
                name=FK, columns=["customer_id"], referenced_table=PARENT,
                referenced_columns=["id"], on_delete="CASCADE",
            )],
        ),
    ])


def _restored_pg_session():
    """What a restore of an older, host-less PostgreSQL snapshot leaves behind."""
    from dsql_migrator.ui.session import SessionConnectionState

    session = SessionConnectionState()
    session.set_restored_source_type(SourceType.POSTGRES)
    session.set_target(TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    assert session.source_config is None  # the premise: no live config to read
    return session


def _stores(inventory, edited=None):
    conv_state = SimpleNamespace(edited_target_ddls=dict(edited or {}),
                                 preserve_foreign_keys=True)
    eval_store = SimpleNamespace(get_or_create=lambda _sid: SimpleNamespace(
        result=SimpleNamespace(inventory=inventory)))
    conv_store = SimpleNamespace(get=lambda _sid: conv_state)
    return eval_store, conv_store, conv_state


def _placed_at_type(conversion) -> str:
    ddl = conversion.target_ddl
    return ddl[ddl.index('"placed_at"'):].split(",")[0]


def test_cut_over_identity_and_foreign_key_inputs_convert_a_restored_session_as_postgresql():
    from dsql_migrator.ui.validation import _identity_sync_context

    eval_store, conv_store, _conv = _stores(_pg_inventory())
    _tables, applied = _identity_sync_context(
        _restored_pg_session(), eval_store, conv_store, "s1", migration_state=None)
    assert _placed_at_type(applied[CHILD]) == '"placed_at" TIMESTAMP', (
        "a restored PostgreSQL session's cut-over inputs were converted as MySQL "
        f"({_placed_at_type(applied[CHILD])})"
    )


def test_cut_over_holds_no_mysql_cdc_key_for_a_restored_postgresql_session():
    from dsql_migrator.ui.validation import _cutover_fk_keys

    eval_store, conv_store, _conv = _stores(_pg_inventory())
    keys, breakable, _defs = _cutover_fk_keys(
        _restored_pg_session(), eval_store, conv_store, "s1",
        migration_state=None, cdc_in_use=True)
    assert keys == [(CHILD, FK)]
    assert breakable == frozenset(), (
        "Finish waited for a key only a MySQL CDC stream can break, on a PostgreSQL source"
    )
    # The control: a live MySQL source with the same key IS held (the check discriminates).
    mysql = SimpleNamespace(source_config=SourceConnectionConfig(
        host="h", database="d", source_type=SourceType.MYSQL))
    _keys, mysql_breakable, _defs = _cutover_fk_keys(
        mysql, eval_store, conv_store, "s1", migration_state=None, cdc_in_use=True)
    assert mysql_breakable == frozenset({(CHILD, FK)})


def test_the_cut_over_foreign_key_pass_converts_a_restored_session_as_postgresql(monkeypatch):
    import dsql_migrator.ui.data_migration._full_load_engine as engine
    import dsql_migrator.ui.schema_conversion as schema_conversion
    from dsql_migrator.ui.validation import ValidationState, _run_cutover_foreign_keys

    # One edit, for a table the inventory does not hold (applied_table_conversions only
    # reads the edits of the tables it converts, so the conversion is unchanged): an EMPTY
    # dict is falsy, so ``edits or {}`` hands out a new {} with or without the copy, and
    # the identity check below could not tell them apart.
    eval_store, conv_store, conv_state = _stores(
        _pg_inventory(), {"public.not_migrated": "CREATE TABLE not_migrated (id integer)"})
    assert conv_state.edited_target_ddls
    captured: dict = {}
    real = schema_conversion.applied_table_conversions

    def _spy(result, edited, **kw):
        captured.setdefault("edits", []).append(edited)
        return real(result, edited, **kw)

    def _fake_apply(conversions, _connect, **_kw):
        captured["conversions"] = dict(conversions)
        return (1, 0, 0)

    monkeypatch.setattr(schema_conversion, "applied_table_conversions", _spy)
    monkeypatch.setattr(engine, "apply_preserved_foreign_keys", _fake_apply)
    monkeypatch.setattr("dsql_migrator.core.target_connection.DsqlConnector",
                        lambda *_a, **_k: SimpleNamespace(connect=lambda: object()))
    state = ValidationState()
    _run_cutover_foreign_keys(
        _restored_pg_session(), state, eval_store=eval_store, conversion_store=conv_store,
        session_id="s1", job_manager=None, refresh=lambda: None,
    )
    assert state.cutover_fk_apply == (1, 0, 0)
    assert _placed_at_type(captured["conversions"][CHILD]) == '"placed_at" TIMESTAMP', (
        "the cut-over pass converted a restored PostgreSQL session as MySQL"
    )
    # The pass runs on a job thread: it converts with a COPY of the Schema Conversion
    # edits, never the live dict another thread may be editing.
    assert captured["edits"] and all(
        edits is not conv_state.edited_target_ddls for edits in captured["edits"]
    ), "the job-thread pass read the live edits dict instead of a copy"
    assert all(edits == conv_state.edited_target_ddls for edits in captured["edits"])


def test_the_runbook_names_postgresql_for_a_restored_session():
    from dsql_migrator.ui.validation import _source_engine_word

    assert _source_engine_word(_restored_pg_session()) == "PostgreSQL"
    assert _source_engine_word(SimpleNamespace(source_config=None)) == "MySQL"


def _full_load_fk_card(monkeypatch, *, migration_type=None, open_ai_scope=None):
    """The Data Migration step of a finished Full load only of both tables for a RESTORED
    PostgreSQL session. Returns the FK card's providers (``_render_full_load_step`` records
    them), every input the screen built a migrator from, and the rendered texts.

    ``migration_type`` overrides Full load only; ``open_ai_scope`` turns AI Assist on (the
    session's opt-in) and is the screen's AI-panel seam."""
    from datetime import datetime, timezone

    from dsql_migrator.core.models import (
        AiAssistConfig,
        AssessmentReport,
        ChunkState,
        MigrationJob,
        StepStatus,
        TableSelection,
        TargetInventory,
        Watermark,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
    from dsql_migrator.ui.data_migration._full_load_engine import default_migrator_factory
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.workflow import WorkflowStep, with_status
    from tests.test_g142_review_fixes import _Jobs
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
    card: dict = {}
    monkeypatch.setattr(dm, "_render_full_load_step", lambda *_a, **kw: card.update(kw))
    inventory = _pg_inventory()
    names = [t.name for t in inventory.tables]
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_restored_source_type(SourceType.POSTGRES)
    session.set_target(TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    assert session.source_config is None
    session.set_migration_type(migration_type or MigrationType.FULL_LOAD_ONLY)
    if open_ai_scope is not None:
        session.set_ai_assist(AiAssistConfig(enabled=True))
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.DONE))
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = [f"table:{n}" for n in names]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "load"
    state.set_selection(TableSelection(selected_tables=names))
    jobs = _Jobs()
    load = MigrationJob(
        job_id="load", status="DONE",
        chunks=[ChunkState(chunk_id=n, status="DONE", attempts=1) for n in names],
        watermark=Watermark(snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
                            table_row_counts={n: 1 for n in names}))
    status_of = jobs.get_status
    jobs.get_status = lambda job_id: load if job_id == "load" else status_of(job_id)
    jobs.request_cancel = lambda _job_id: None
    built: list = []

    def _factory(inputs):
        built.append(inputs)
        return default_migrator_factory(inputs)

    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs, migrator_factory=_factory, open_ai_scope=open_ai_scope,
    )
    content(lambda: None)
    return card, built, " ".join(ui.texts)


def test_the_full_load_fk_card_counts_a_restored_postgresql_sessions_keys(monkeypatch):
    card, built, _texts = _full_load_fk_card(monkeypatch)
    assert card["foreign_keys_pending"]() == 1, (
        "the foreign-key card's denominator read 0 for a restored session: the count's "
        "inputs read the engine off a source_config that a restore leaves None"
    )
    # ...and those inputs are the PostgreSQL conversion, not a MySQL one.
    assert built, "the card's count built no migrator inputs"
    assert _placed_at_type(built[-1].table_conversions[CHILD]) == '"placed_at" TIMESTAMP'


def test_the_full_load_only_next_step_gives_a_restored_postgresql_session_the_slot_wording(
    monkeypatch,
):
    # A finished Full load only run names the way on to CDC. On PostgreSQL that run created
    # no replication slot, so the MySQL wording ("CDC streams from this Full Load's
    # watermark ... no re-snapshot") hands a restored PostgreSQL operator a dead start.
    _card, _built, texts = _full_load_fk_card(monkeypatch)
    assert "This run created no replication slot" in texts
    assert "streams from this Full Load's watermark" not in texts


def test_the_ai_dba_reasons_about_a_restored_postgresql_source(monkeypatch):
    # The Full Load failure / quarantine chat and the CDC dead-letter / schema-drift chat
    # each build an AI DBA strategist per send, grounded in the source engine. They read
    # getattr(session.source_config, "source_type", None), and source_engine_word(None) is
    # "MySQL": a restored PostgreSQL operator's AI DBA was told the source is MySQL.
    import dsql_migrator.core.assessment_strategist as strategist
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import MigrationType

    engines: list = []

    class _Strategist:
        def __init__(self, *_a, source_engine="MySQL", **_k):
            engines.append(source_engine)

        def stream_full_load_error_chat(self, *_a, **_k):
            return ""

        def stream_cdc_chat(self, *_a, **_k):
            return ""

    monkeypatch.setattr(strategist, "AssessmentStrategist", _Strategist)
    cdc: dict = {}
    monkeypatch.setattr(dm, "_render_cdc_step", lambda *_a, **kw: cdc.update(kw))
    scopes: list = []
    card, _built, _texts = _full_load_fk_card(
        monkeypatch, migration_type=MigrationType.FULL_LOAD_AND_CDC,
        open_ai_scope=lambda **kw: scopes.append(kw),
    )
    assert card["ai_error_opener"] is not None and cdc["cdc_ai_opener"] is not None
    card["ai_error_opener"](CHILD, "duplicate key value violates unique constraint")
    cdc["cdc_ai_opener"]("dlq", "1 record in the dead-letter queue", "Why?")
    assert [s["scope_id"] for s in scopes] == [f"full_load:failure:{CHILD}", "cdc:dlq"]
    for scope in scopes:
        scope["streamer"]([], lambda _delta: None)
    assert engines == ["PostgreSQL", "PostgreSQL"], (
        f"a restored PostgreSQL session's AI DBA was grounded as {engines}"
    )
