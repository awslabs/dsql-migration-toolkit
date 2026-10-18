"""G127: the identity sync reaches every migrated identity table with the source floor and a
heartbeat, and the cut-over runbook moves standalone PostgreSQL sequences past the source.

CUT-1: the cut-over "Sync identity sequences" (and the Validation-time sync) took only the
tables the last Validation compared. An identity table left out of it (its chip off under
Objects to validate) kept the sequence its Full Load sync left, below every id CDC delivered
since, so the application's first insert after the repoint collided (23505) under a green
"Advanced 1 sequence(s)" and an ack line reading "identity sequences synced (1 advanced)".
CUT-2: Evaluation defers each sequence no column owns (PG_STANDALONE_SEQUENCE) to cut over,
but the runbook never named one, and its CDC step 4 said every key sequence was advanced.
Extras: the accept-gap / scope-DONE syncs take the source floor (G42), and every identity-sync
tail heartbeats per table, so the 900 s stall watchdog cannot reap a job syncing many tables.
"""

from __future__ import annotations

import re
import sys
import types
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import (
    AssessmentReport,
    ColumnDef,
    ReconcileResult,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    StepStatus,
    TableDef,
    TableSelection,
    TableValidationResult,
    TargetConnectionConfig,
    TargetInventory,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.ui.data_migration import MigrationType

ORDERS, EVENTS = "shop.orders", "shop.events"
_TARGET = TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")


# ---------------------------------------------------------------------------
# An in-memory Aurora DSQL double: identity sequences (last_value / is_called), the rows'
# ids as ranges, RESTART WITH, and an application's id-less insert.
# ---------------------------------------------------------------------------


class _Target:
    def __init__(self) -> None:
        self.t: dict[str, dict] = {}

    def identity_table(self, name: str) -> None:
        self.t[name] = {"ranges": [], "last": 1, "called": False}

    def insert_explicit(self, name: str, lo: int, hi: int) -> None:
        self.t[name]["ranges"].append((lo, hi))  # Full Load / CDC: the sequence stays put

    def max_id(self, name: str):
        ranges = self.t[name]["ranges"]
        return max(hi for _lo, hi in ranges) if ranges else None

    def app_insert(self, name: str) -> "tuple[bool, int]":
        """The application's first id-less INSERT after the repoint: (no collision, id)."""
        seq = self.t[name]
        value = seq["last"] + 1 if seq["called"] else seq["last"]
        seq["last"], seq["called"] = value, True
        if any(lo <= value <= hi for lo, hi in seq["ranges"]):
            return False, value  # 23505
        seq["ranges"].append((value, value))
        return True, value

    def connect(self):
        return _Conn(self)


class _Cur:
    def __init__(self, target: _Target) -> None:
        self.target = target
        self.rows: list = []

    def execute(self, statement, params=None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        if "is_identity" in text:
            self.rows = [("id",)] if f"{params[0]}.{params[1]}" in self.target.t else []
        elif text.startswith("SELECT MAX("):
            schema, table = re.search(r'FROM "([^"]+)"\."([^"]+)"', text).groups()
            self.rows = [(self.target.max_id(f"{schema}.{table}"),)]
        elif "pg_get_serial_sequence" in text:
            schema, table = re.findall(r'"([^"]+)"', params[0])
            self.rows = [(f'{schema}."{table}_id_seq"',)]
        elif "last_value, is_called" in text:
            schema, table = re.search(r'FROM "([^"]+)"\."([^"]+)_id_seq"', text).groups()
            seq = self.target.t[f"{schema}.{table}"]
            self.rows = [(seq["last"], seq["called"])]
        elif "RESTART WITH" in text:
            schema, table, value = re.search(
                r'ALTER TABLE "([^"]+)"\."([^"]+)" ALTER COLUMN "id" RESTART WITH (\d+)', text
            ).groups()
            seq = self.target.t[f"{schema}.{table}"]
            seq["last"], seq["called"] = int(value), False
        else:
            raise AssertionError(f"unexpected SQL: {text}")

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        pass


class _Conn:
    def __init__(self, target: _Target) -> None:
        self.target = target

    def cursor(self):
        return _Cur(self.target)

    def close(self) -> None:
        pass


def _inventory() -> SourceInventory:
    # Two MySQL AUTO_INCREMENT keys: both become bigint identities by default (G37).
    return SourceInventory(tables=[
        TableDef(name=ORDERS, columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                                       ColumnDef(name="note", mysql_type="varchar(20)")],
                 primary_key=["id"], auto_increment_column="id"),
        TableDef(name=EVENTS, columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                                       ColumnDef(name="payload", mysql_type="text")],
                 primary_key=["id"], auto_increment_column="id"),
    ])


def _item(name: str, rows: int) -> TableValidationResult:
    return TableValidationResult(
        table=name, source_row_count=rows, target_row_count=rows, row_count_match=True,
        checksum_match=True, matched=True,
        reconcile=ReconcileResult(pk_column="id", source_count=rows, target_count=rows,
                                  missing_on_target=0, extra_on_target=0, consistent=True),
    )


def _conversion_store():
    return SimpleNamespace(
        get=lambda _sid: SimpleNamespace(preserve_foreign_keys=True, edited_target_ddls={})
    )


def _floors(monkeypatch, values):
    """The source's own counters (a stub of the autocommit catalog read); records the asks."""
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    asked: list = []

    def _read(_config, _password, tables):
        asked.append(sorted(t.name for t in tables))
        return {t.name: values[t.name] for t in tables if t.name in values}

    monkeypatch.setattr(engine, "source_next_key_values", _read)
    return asked


def _cutover_screen(monkeypatch, *, report, sync, logged, mtype=MigrationType.FULL_LOAD_AND_CDC):
    from dsql_migrator.ui import validation as uiv
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_validation import _ScreenUi

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "log_activity", lambda _c, action, **kw: logged.append((action, kw)))
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_target(_TARGET)
    session.set_migration_type(mtype)
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=_inventory(), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    vstore, mstore = uiv.ValidationStore(), DataMigrationStore()
    mstate = mstore.get_or_create("s1")
    mstate.set_migration_type(mtype)
    mstate.selection = TableSelection(selected_tables=[ORDERS, EVENTS])
    content, runner = uiv.build_cutover_screen(
        sessions, "s1", validation_store=vstore, migration_store=mstore, eval_store=evals,
        conversion_store=_conversion_store(), sync_sequences=sync,
    )
    vstate = vstore.get_or_create("s1")
    vstate.set_result(report)
    vstate.table_exclude = {EVENTS}  # the chip turned off under Objects to validate
    return SimpleNamespace(ui=ui, content=content, runner=runner, vstate=vstate)


def _click(ui, label: str) -> None:
    (handler,) = [cb for text, cb in ui.clicks if text == label]
    handler()


# ---------------------------------------------------------------------------
# CUT-1: the cut-over sync reaches every migrated identity table
# ---------------------------------------------------------------------------


def test_the_cut_over_sync_reaches_an_identity_table_left_out_of_the_validation(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the sync was handed ["shop.orders"] only, the screen showed a
    # green "Advanced 1 sequence(s)", the ack line read "identity sequences synced (1
    # advanced)", and the application's first insert into shop.events drew 5,000,001 (23505).
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    target = _Target()
    target.identity_table(ORDERS)
    target.identity_table(EVENTS)
    target.insert_explicit(ORDERS, 1, 1000)
    target.insert_explicit(EVENTS, 1, 5_000_000)
    # The Full Load's own sync (source counters as the floor), then CDC's explicit ids.
    assert sync_identity_sequences(
        [ORDERS, EVENTS], connection_factory=target.connect,
        source_next_values={ORDERS: 1001, EVENTS: 5_000_001},
    ) == {ORDERS: 1001, EVENTS: 5_000_001}
    target.insert_explicit(ORDERS, 1001, 1200)
    target.insert_explicit(EVENTS, 5_000_001, 5_800_000)
    asked = _floors(monkeypatch, {ORDERS: 1201, EVENTS: 5_800_101})  # deleted ids stay issued
    calls: list = []

    def _sync(table_names, *, connection_factory, **extras):
        calls.append((sorted(table_names), extras))
        return sync_identity_sequences(table_names, connection_factory=target.connect, **extras)

    logged: list = []
    report = ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[_item(ORDERS, 1200)])
    s = _cutover_screen(monkeypatch, report=report, sync=_sync, logged=logged)
    s.content(lambda: None)
    assert "2 tables use a server-generated (AUTO_INCREMENT / IDENTITY) key" in s.ui.body()
    _click(s.ui, "Sync identity sequences")

    ((names, extras),) = calls
    assert names == [EVENTS, ORDERS]
    # The source floor and the expected identities cover the left-out table too.
    assert asked == [[EVENTS, ORDERS]]
    assert extras["source_next_values"] == {ORDERS: 1201, EVENTS: 5_800_101}
    assert set(extras["expected_identity"]) == {ORDERS, EVENTS}
    assert s.vstate.cutover_identity_sync == {ORDERS: 1201, EVENTS: 5_800_101}
    assert s.vstate.cutover_identity_sync_covered == (ORDERS, EVENTS)

    s.ui.texts.clear()
    s.ui.clicks.clear()
    s.content(lambda: None)
    body = s.ui.body()
    assert f"Advanced 2 sequence(s): {EVENTS} → 5800101, {ORDERS} → 1201." in body
    assert "Identity sequences not synced" not in body
    s.runner()
    (ack,) = [kw["detail"] for action, kw in logged if action == "cut over acknowledged"]
    assert "identity sequences synced (2 advanced)" in ack
    # The application's first insert into either table draws a free id.
    assert target.app_insert(EVENTS) == (True, 5_800_101)
    assert target.app_insert(ORDERS) == (True, 1201)


def test_a_migrated_identity_table_the_sync_did_not_reach_is_named_never_read_as_synced(
        monkeypatch) -> None:
    # The schema the sync is scoped from could not be read in the job: the report's tables
    # are still synced, and the screen and the ack line name the one left out.
    from dsql_migrator.ui.validation import (
        ValidationState,
        _cutover_integrity_detail,
        _identity_sync_missed,
        _render_cutover_section,
        _run_cutover_identity_sync,
    )

    from tests.test_ui_validation import _CutoverUi, _cutover_summary, _no_drift

    def _unreadable():
        raise RuntimeError("the inventory could not be converted")

    calls: list = []

    def _sync(table_names, *, connection_factory, **_kw):
        calls.append(list(table_names))
        return {}

    state = ValidationState()
    report = ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[_item(ORDERS, 5)])
    _run_cutover_identity_sync(
        SimpleNamespace(target_config=_TARGET, aws_profile=None), state, report,
        sync=_sync, identity_tables=_unreadable,
    )
    assert calls == [[ORDERS]]
    missed = _identity_sync_missed(state, [ORDERS, EVENTS])
    assert missed == [EVENTS]
    assert _identity_sync_missed(ValidationState(), [ORDERS, EVENTS]) == []  # not run yet

    ack = _cutover_integrity_detail(state, 0, identity_missed=missed)
    assert "identity sequences: 1 NOT synced (shop.events)" in ack
    assert "identity sequences synced" not in ack

    ui = _CutoverUi()
    _render_cutover_section(
        ui, _cutover_summary(), _no_drift(), cdc_in_use=True,
        identity_sync_provider=lambda: None,
        identity_sync_result=state.cutover_identity_sync,
        identity_sync_failed=state.cutover_identity_sync_failed,
        identity_sync_missed=missed,
        identity_pending_count=2,
    )
    blob = " ".join(ui.texts)
    assert "Identity sequences not synced — do not cut over yet" in blob
    assert "shop.events" in blob
    assert "Done — no server-generated key needed advancing." not in blob


def test_an_unselected_identity_table_pointing_into_the_set_is_neither_counted_nor_synced(
) -> None:
    # The foreign-key scope keeps an unselected child whose key points into the migrated
    # set (its key is re-created by Apply foreign keys); it was never loaded, so it has no
    # sequence to move, and syncing it would report "no identity on the target" for it.
    from dsql_migrator.core.models import ForeignKeyDef
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.validation import (
        _cutover_identity_tables,
        _cutover_pending_identity_tables,
        _in_migration_scope,
        _identity_sync_context,
    )

    child = TableDef(
        name="shop.order_notes",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="order_id", mysql_type="int")],
        primary_key=["id"], auto_increment_column="id",
        foreign_keys=[ForeignKeyDef(name="fk_notes_order", columns=["order_id"],
                                    referenced_table=ORDERS, referenced_columns=["id"])],
    )
    inventory = SourceInventory(tables=[*_inventory().tables, child])
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    session = SimpleNamespace(source_config=None)
    migration = SimpleNamespace(selection=TableSelection(selected_tables=[ORDERS, EVENTS]))
    _tables, applied = _identity_sync_context(
        session, evals, _conversion_store(), "s1", migration_state=migration)
    assert "shop.order_notes" in applied  # kept by the foreign-key scope...
    assert "shop.order_notes" in _in_migration_scope(applied, migration)
    # ...but no identity table of the migration.
    assert _cutover_identity_tables(
        session, evals, _conversion_store(), "s1", migration_state=migration
    ) == [ORDERS, EVENTS]
    assert _cutover_pending_identity_tables(
        session, evals, _conversion_store(), "s1", migration_state=migration
    ) == 2
    # No recorded selection: every table, as before.
    assert _cutover_identity_tables(
        session, evals, _conversion_store(), "s1",
        migration_state=SimpleNamespace(selection=TableSelection()),
    ) == [ORDERS, EVENTS, "shop.order_notes"]


def test_the_validation_run_syncs_every_migrated_identity_table_with_the_floor_and_a_heartbeat(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the Validation-time sync took the run's scope (the selection
    # minus the chips), passed no heartbeat, and so left shop.events out.
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.job_manager import JobHandle, JobManager
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_validation_screen

    from tests.test_ui_validation import _FakeValidator

    asked = _floors(monkeypatch, {ORDERS: 11, EVENTS: 21})
    calls: list = []

    def _sync(table_names, *, connection_factory, **kw):
        calls.append((list(table_names), kw))
        return {}

    store, evals, mstore, vstore = (
        SessionStore(), EvaluationStore(), DataMigrationStore(), ValidationStore())
    manager = JobManager()
    session = store.get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="db", database="shop"), SecretValue("pw"))
    session.set_target(_TARGET)
    session.set_source_verified(True)
    session.set_target_verified(True)
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=_inventory(), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(), target_conflicts=[]))
    mstore.get_or_create("s1").selection = TableSelection(selected_tables=[ORDERS, EVENTS])
    _content, runner = build_validation_screen(
        store, "s1", job_manager=manager, eval_store=evals, migration_store=mstore,
        validation_store=vstore, conversion_store=_conversion_store(), sync_sequences=_sync,
        validator_factory=lambda _i: _FakeValidator(ValidationReport.build(
            mode=ValidationMode.CHECKSUM, items=[_item(ORDERS, 10)])),
    )
    vstate = vstore.get_or_create("s1")
    vstate.table_exclude = {EVENTS}
    runner()
    assert manager.wait(vstate.job_id, timeout=5.0) is True
    ((names, kw),) = calls
    assert names == [ORDERS, EVENTS]
    assert asked == [[EVENTS, ORDERS]]
    assert kw["source_next_values"] == {ORDERS: 11, EVENTS: 21}
    assert set(kw["expected_identity"]) == {ORDERS, EVENTS}
    assert isinstance(getattr(kw["heartbeat"], "__self__", None), JobHandle)


# ---------------------------------------------------------------------------
# Extras: heartbeats, and the source floor on the accept-gap / scope-DONE syncs
# ---------------------------------------------------------------------------


def test_the_identity_sync_heartbeats_once_per_table() -> None:
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    target = _Target()  # no identity table: each table is one catalog read
    beats: list = []
    names = ["app.a", "app.b", "app.c"]
    out = sync_identity_sequences(
        names, connection_factory=target.connect, heartbeat=lambda: beats.append(1)
    )
    assert out == dict.fromkeys(names)
    assert len(beats) == 3


def test_a_cut_over_sync_over_slow_tables_is_not_reaped_by_the_stall_watchdog() -> None:
    # FAILS WITHOUT THE FIX: no beat while the tail ran, so ten minutes per table on the
    # watchdog's clock reaped the sync job (FAILED) at its second table.
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.target_introspector import sync_identity_sequences
    from dsql_migrator.ui.validation import ValidationState, _run_cutover_identity_sync

    now = [0.0]
    manager = JobManager(stall_timeout_seconds=900.0, clock=lambda: now[0])
    reaped: list = []

    class _SlowCur(_Cur):
        def execute(self, statement, params=None) -> None:
            now[0] += 600.0  # each table's catalog read takes ten minutes
            reaped.extend(manager.reap_stalled_jobs())
            super().execute(statement, params)

    class _SlowConn(_Conn):
        def cursor(self):
            return _SlowCur(self.target)

    def _sync(table_names, *, connection_factory, **kw):
        return sync_identity_sequences(
            table_names, connection_factory=lambda: _SlowConn(_Target()), **kw
        )

    state = ValidationState()
    report = ValidationReport.build(
        mode=ValidationMode.CHECKSUM, items=[_item(f"shop.t{i}", 1) for i in range(4)]
    )
    _run_cutover_identity_sync(
        SimpleNamespace(target_config=_TARGET, aws_profile=None), state, report,
        job_manager=manager, sync=_sync,
    )
    (job,) = manager.list_jobs()
    assert manager.wait(job.job_id, timeout=5.0) is True
    assert reaped == []
    assert manager.get_status(job.job_id).status == "DONE"
    assert state.cutover_identity_sync == {}


def test_the_full_load_finalize_sync_takes_the_load_jobs_heartbeat() -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration._full_load_engine import _finalize_run, _RunCounts

    seen: list = []

    class _Handle:
        cancelled = False

        def heartbeat(self) -> None:
            pass

    handle = _Handle()
    _finalize_run(
        handle, "j1", ["t"], _RunCounts(real_failed=0, quarantined=0), ErrorLogStore(),
        accept_quarantined_rows=False, inputs=object(),
        sync_sequences=lambda names, *, connection_factory, **kw: seen.append(kw) or {},
    )
    (kw,) = seen
    assert kw["heartbeat"] == handle.heartbeat


def test_the_post_load_syncs_take_the_source_floor_and_a_heartbeat(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: sync_identity_sequences_for_tables passed neither, so the sync
    # after an accepted quarantine gap (or a scope turning DONE) restarted an identity at
    # MAX + 1 and re-issued ids the source had issued and deleted (G42).
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from dsql_migrator.ui.data_migration._full_load_engine import (
        sync_identity_sequences_for_tables,
    )

    seen: list = []

    def _sync(names, *, connection_factory, **kw):
        seen.append((list(names), kw))
        return {}

    beat = lambda: None  # noqa: E731
    sync_identity_sequences_for_tables(
        _TARGET, ["app.a"], sync=_sync,
        extras={"source_next_values": {"app.a": 42}, "expected_identity": {"app.a"}},
        heartbeat=beat,
    )
    assert seen == [(["app.a"], {
        "source_next_values": {"app.a": 42}, "expected_identity": {"app.a"}, "heartbeat": beat,
    })]

    forwarded: list = []
    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables",
                        lambda _cfg, names, **kw: forwarded.append((list(names), kw)) or {})
    jobs = SimpleNamespace(submitted=[], submit=lambda fn: jobs.submitted.append(fn))
    view = SimpleNamespace(chunks=[SimpleNamespace(chunk_id="app.a", status="DONE"),
                                   SimpleNamespace(chunk_id="app.b", status="FAILED")])
    assert fl.submit_scope_identity_sync(
        jobs, SimpleNamespace(target_config=_TARGET, aws_profile=None), view,
        extras=lambda names: {"source_next_values": {n: 7 for n in names}},
    )
    (work,) = jobs.submitted
    handle = SimpleNamespace(heartbeat=lambda: None)
    work(handle)
    ((names, kw),) = forwarded
    assert names == ["app.a"]
    assert kw["extras"] == {"source_next_values": {"app.a": 7}}
    assert kw["heartbeat"] is handle.heartbeat


def test_a_narrowed_set_turning_done_syncs_its_loaded_tables_above_the_source_counter(
        monkeypatch) -> None:
    # The builder's floor reaches the scope sync: the loaded AUTO_INCREMENT table is synced
    # with its source counter and as an expected identity.
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore, _full_load_ui as fl
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

    from tests.test_g12_table_set_after_load import _Jobs, _c, _job
    from tests.test_ui_data_migration import _ScreenUi

    asked = _floors(monkeypatch, {"app.a": 501})
    synced: list = []
    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables",
                        lambda _cfg, names, **kw: synced.append((list(names), kw)) or {})

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.ui = _ScreenUi()  # type: ignore[attr-defined]
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(
        source_type=SourceType.MYSQL, host="h", port=3306, database="app", username="u"), None)
    session.set_target(_TARGET)
    session.set_migration_type(MigrationType.FULL_LOAD_ONLY)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    tables = [TableDef(name=f"app.{n}", columns=[ColumnDef(name="id", mysql_type="int",
                                                            nullable=False)],
                       primary_key=["id"], auto_increment_column="id") for n in ("a", "b")]
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=tables), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = ["table:app.a", "table:app.b"]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "job-1"
    state.set_selection(TableSelection(selected_tables=["app.a"]))
    jobs = _Jobs({"job-1": _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")],
                                counts={"app.a": 1, "app.b": 1})})
    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs,
    )
    content(lambda: None)
    assert get_status(session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    (work,) = jobs.submitted
    work(SimpleNamespace())
    ((names, kw),) = synced
    assert names == ["app.a"]
    assert asked == [["app.a"]]
    assert kw["extras"]["source_next_values"] == {"app.a": 501}
    assert set(kw["extras"]["expected_identity"]) == {"app.a"}


# ---------------------------------------------------------------------------
# CUT-2: the runbook moves standalone PostgreSQL sequences past the source
# ---------------------------------------------------------------------------


def _standalone_screen(monkeypatch, mtype, *, postgres=True, logged=None):
    from dsql_migrator.core.assessor_postgres import standalone_sequence_items
    from dsql_migrator.core.catalog_postgres import StandaloneSequence
    from dsql_migrator.ui import validation as uiv
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.workflow import WorkflowStep, with_status

    from tests.test_ui_validation import _ScreenUi

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    if logged is not None:
        monkeypatch.setattr(uiv, "log_activity", lambda *a, **k: logged.append((a, k)))
    # The entity table has a plain bigint key the application fills from orders_seq, a
    # Hibernate-style sequence (INCREMENT BY 50) that no column owns.
    inventory = SourceInventory(tables=[TableDef(
        name="public.orders", columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"])])
    items = standalone_sequence_items([StandaloneSequence(
        name="public.orders_seq", data_type="bigint", start_value=1, min_value=1,
        max_value=9223372036854775807, increment=50, cycle=False, cache_size=1,
        last_value=1001, readable=True, is_called=True, callers=())], inventory) if postgres else []
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(
        host="h", database="d", source_type=SourceType.POSTGRES if postgres else SourceType.MYSQL
    ), None)
    session.set_migration_type(mtype)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.DONE))
    session.set_workflow(with_status(session.workflow, WorkflowStep.VALIDATION, StepStatus.DONE))
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=AssessmentReport.from_items(items),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[],
        source_type=SourceType.POSTGRES if postgres else SourceType.MYSQL))
    conversions = SchemaConversionStore()
    conversions.get_or_create("s1")
    vstore = uiv.ValidationStore()
    content, runner = uiv.build_cutover_screen(
        sessions, "s1", validation_store=vstore, eval_store=evals, conversion_store=conversions,
    )
    vstore.get_or_create("s1").set_result(ValidationReport.build(
        mode=ValidationMode.CHECKSUM, items=[_item("public.orders", 1)]))
    content(lambda: None)
    return ui.body(), runner


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_ONLY, MigrationType.FULL_LOAD_AND_CDC])
def test_the_runbook_names_each_standalone_sequence_with_its_source_read_and_setval(
        monkeypatch, mtype) -> None:
    # FAILS WITHOUT THE FIX: neither runbook named orders_seq nor any setval, so the
    # application's nextval() on Aurora DSQL handed out ids the source had already used.
    logged: list = []
    body, runner = _standalone_screen(monkeypatch, mtype, logged=logged)
    header = "Before you repoint: move standalone sequences past the source"
    assert header in body
    assert 'SELECT last_value, is_called FROM "public"."orders_seq";' in body
    assert "SELECT setval('\"public\".\"orders_seq\"', <last_value>, <is_called>);" in body
    # After the numbered steps (the final drain / reload), before the rollback caveat.
    assert body.index("Steps to cut over") < body.index(header)
    assert body.index(header) < body.index("Keep the source as your rollback anchor")
    runner()
    acks = [k["detail"] for a, k in logged if a[1] == "cut over acknowledged"]
    assert acks and "standalone sequences left to the operator: 1" in acks[0]


def test_the_cdc_step_four_says_it_advances_identity_sequences_only(monkeypatch) -> None:
    body, _runner = _standalone_screen(monkeypatch, MigrationType.FULL_LOAD_AND_CDC)
    assert "advances any auto-generated / identity key sequence" not in body
    assert (
        "This final run also advances the sequence of each identity key (only sequences a "
        "column owns) past the rows CDC delivered" in body
    )


def test_a_session_without_standalone_sequences_has_no_such_block(monkeypatch) -> None:
    logged: list = []
    body, runner = _standalone_screen(
        monkeypatch, MigrationType.FULL_LOAD_AND_CDC, postgres=False, logged=logged
    )
    assert "move standalone sequences" not in body and "setval" not in body
    runner()
    acks = [k["detail"] for a, k in logged if a[1] == "cut over acknowledged"]
    assert acks and "standalone sequences" not in acks[0]
