# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Full Load engine: the column-exclusion overlay (P2).

P2-05 pins the behaviour-neutral foundations: the job's ``recreated_tables`` record and
its downgrade-safe serialization, the overlay input fields slimmed out of the worker
payload (still picklable with a live ledger view in the parent), the migrator seams and
``bind_run_inputs``, the extracted promotion predicate, and the ``OverlayRunError`` family
that no source-retry loop mistakes for a source drop.
"""

import dataclasses
import json
import pickle
import threading

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.job_store import SqliteJobStore, S3JobStore
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType
from dsql_migrator.core.overlay_ledger import (
    InMemoryOverlayLedger,
    OverlayLedgerView,
    SqliteOverlayLedger,
)
from dsql_migrator.ui.data_migration import recreated_unfinished
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    FullLoadIncompleteError,
    OverlayCatalogError,
    OverlayLedgerReadError,
    OverlayLedgerWriteError,
    OverlayNeedsReloadError,
    OverlayNotInPlaceError,
    OverlayRecordMismatchError,
    OverlayRunError,
    _migrate_table_with_source_retry,
    _needs_empty_target_promotion,
    _record_recreated,
    _retry_source_drops_in_process,
    _ShardWorkerArgs,
    _slim_worker_inputs,
    _TableWorkerArgs,
)

from tests.test_job_store import _FakeS3
from tests.test_ui_data_migration import _RetryHandle, _inputs, _tables


# ---------------------------------------------------------------------------
# Job model
# ---------------------------------------------------------------------------


def _job(**chunks):
    return MigrationJob(job_id="j1", chunks=[ChunkState(chunk_id=name, status=status)
                                             for name, status in chunks.items()])


def test_recreated_tables_defaults_empty_and_is_left_out_of_the_payload_while_empty():
    legacy = _job(**{"db.t": "DONE"}).model_dump_json()
    assert "recreated_tables" not in legacy  # a 0.1.561 build (extra="forbid") still loads it
    assert MigrationJob.model_validate_json(legacy).recreated_tables == []
    job = _job(**{"db.t": "FAILED"})
    _record_recreated(job, "db.t")
    _record_recreated(job, "db.t")  # a thread-path re-entry recreates it again: idempotent
    assert job.recreated_tables == ["db.t"]
    assert json.loads(job.model_dump_json())["recreated_tables"] == ["db.t"]


def test_recreated_tables_round_trip_through_both_job_stores(tmp_path):
    job = _job(**{"db.t": "FAILED"})
    job.recreated_tables = ["db.t"]
    lite = SqliteJobStore(str(tmp_path / "jobs.sqlite"))
    lite.save(job, None)
    assert [p.job.recreated_tables for p in lite.load_all()] == [["db.t"]]
    s3 = S3JobStore("b", s3_client=_FakeS3(buckets=("b",)))
    s3.save(job, None)
    assert [p.job.recreated_tables for p in s3.load_all()] == [["db.t"]]


def test_recreated_unfinished():
    job = _job(done="DONE", failed="FAILED", pending="PENDING", running="IN_PROGRESS")
    job.recreated_tables = ["done", "failed", "pending", "failed", "running", "no_chunk"]
    assert recreated_unfinished(job) == ["failed", "pending", "running"]

    class _Double:
        chunks = []

    assert recreated_unfinished(_Double()) == []


# ---------------------------------------------------------------------------
# Inputs: slimmed worker payload, picklable with a live view in the parent
# ---------------------------------------------------------------------------


def _bound_inputs(tmp_path):
    store = SqliteOverlayLedger(str(tmp_path / "ledger.sqlite"))
    view = OverlayLedgerView(store, "k", store.read("k"))
    view._lock = threading.Lock()  # the view is parent-only; a lock can never be pickled
    seen = []
    return dataclasses.replace(
        _inputs(),
        excluded_lob_columns={"orders": frozenset({"body"}), "customers": frozenset({"bio"})},
        overlay_columns={"orders": frozenset({"body"}), "customers": frozenset({"bio"})},
        legacy_exclusions={"customers": frozenset({"old"})},
        overlay_recorded_defaults={"orders": {"body": "'x'::text"}, "customers": {"bio": "''::text"}},
        overlay_ledger=store,
        overlay_record=view,
        overlay_ledger_in_use=True,
        overlay_ledger_status="absent",
        on_table_recreated=lambda name: seen.append(name),
    )


def test_slimmed_worker_inputs_keep_only_this_table_and_drop_the_parent_only_fields(tmp_path):
    slim = _slim_worker_inputs(_bound_inputs(tmp_path), "orders")
    assert dict(slim.excluded_lob_columns) == {"orders": frozenset({"body"})}
    assert dict(slim.overlay_columns) == {"orders": frozenset({"body"})}
    assert dict(slim.legacy_exclusions) == {}
    assert dict(slim.overlay_recorded_defaults) == {"orders": {"body": "'x'::text"}}
    assert (slim.overlay_ledger, slim.overlay_record, slim.overlay_ledger_status,
            slim.on_table_recreated) == (None, None, None, None)
    assert slim.overlay_ledger_in_use is True  # inert in a worker


def test_the_bound_inputs_cannot_be_pickled_but_every_worker_payload_can(tmp_path):
    bound = _bound_inputs(tmp_path)
    with pytest.raises(Exception):
        pickle.dumps(bound)  # the guard below is meaningful only if this fails
    table = _tables()[0]
    pickle.dumps(_TableWorkerArgs(job_id="j", table=table, inputs=_slim_worker_inputs(bound, table.name)))
    pickle.dumps(_ShardWorkerArgs(job_id="j", table=table, inputs=_slim_worker_inputs(bound, table.name),
                                  pk_lower=1, pk_upper=10, shard_index=0))


# ---------------------------------------------------------------------------
# Migrator seams, bind_run_inputs
# ---------------------------------------------------------------------------


def test_bind_run_inputs_swaps_only_the_inputs():
    base = _inputs()
    migrator = BatchedTableMigrator(base, exporter=object(), watermark_capturer=object())
    recreator, importer_factory, exporter = (
        migrator._table_recreator, migrator._importer_factory, migrator._exporter)
    view = OverlayLedgerView(InMemoryOverlayLedger(), "k", InMemoryOverlayLedger().read("k"))
    bound = dataclasses.replace(base, overlay_record=view)
    migrator.bind_run_inputs(bound)
    assert migrator._inputs is bound
    assert (migrator._table_recreator, migrator._importer_factory, migrator._exporter) == \
        (recreator, importer_factory, exporter)


def test_overlay_seams_resolve_the_module_defaults_at_call_time(monkeypatch):
    calls = []
    migrator = BatchedTableMigrator(_inputs(), exporter=object(), watermark_capturer=object())
    monkeypatch.setattr(engine, "_default_overlay_facts_reader",
                        lambda inputs, names, cols, **kw: calls.append(("facts", names, kw)) or {})
    monkeypatch.setattr(engine, "_default_overlay_applier",
                        lambda inputs, name, planned, **kw: calls.append(("apply", name, kw)))
    migrator.overlay_facts(["orders"], {"orders": ["body"]}, probe_rows=False)
    migrator.apply_overlay("orders", {}, expected_defaults={"body": None})
    assert calls == [("facts", ["orders"], {"probe_rows": False, "primary_key_tables": frozenset()}),
                     ("apply", "orders", {"expected_defaults": {"body": None}})]
    injected = BatchedTableMigrator(_inputs(), exporter=object(), watermark_capturer=object(),
                                    overlay_facts_reader=lambda *a, **k: "mine")
    assert injected.overlay_facts(["orders"], {}, probe_rows=True) == "mine"


# ---------------------------------------------------------------------------
# The extracted promotion predicate
# ---------------------------------------------------------------------------


def _never():
    raise AssertionError("probe must not run")


@pytest.mark.parametrize("kw", [
    {"is_replace": True}, {"cdc_coexisting": True}, {"target_key_columns": None},
    {"target_key_columns": ["id"]}, {"pre_recreated": True},
])
def test_promotion_short_circuits_before_any_probe(kw):
    args = dict(target_key_columns=["tenant", "id"], source_pk=["id"], is_replace=False,
                cdc_coexisting=False, pre_recreated=False, target_is_empty=_never, live_key=_never)
    args.update(kw)
    assert _needs_empty_target_promotion(**args) is False


def test_promotion_probes_in_order_and_only_an_equal_live_key_skips_it():
    order = []

    def empty(value):
        return lambda: order.append("count") or value

    def key(value):
        return lambda: order.append("key") or value

    base = dict(target_key_columns=["tenant", "id"], source_pk=["id"], is_replace=False,
                cdc_coexisting=False, pre_recreated=False)
    assert _needs_empty_target_promotion(**base, target_is_empty=empty(False), live_key=_never) is False
    assert _needs_empty_target_promotion(**base, target_is_empty=empty(True), live_key=key(["id"])) is True
    assert _needs_empty_target_promotion(**base, target_is_empty=empty(True), live_key=key(None)) is True
    assert _needs_empty_target_promotion(**base, target_is_empty=empty(True),
                                         live_key=key(["tenant", "id"])) is False
    assert order == ["count", "count", "key", "count", "key", "count", "key"]


# ---------------------------------------------------------------------------
# The OverlayRunError family is never a source drop
# ---------------------------------------------------------------------------


class _Conn08(Exception):
    sqlstate = "08006"


def test_overlay_errors_carry_the_sqlstate_and_are_incomplete_load_errors():
    for cls in (OverlayCatalogError, OverlayLedgerReadError, OverlayLedgerWriteError,
                OverlayRecordMismatchError, OverlayNotInPlaceError, OverlayNeedsReloadError):
        err = cls("x", cause=_Conn08())
        assert isinstance(err, OverlayRunError) and isinstance(err, FullLoadIncompleteError)
        assert err.sqlstate == "08006" and engine._error_code(err) == "08006"
    assert OverlayRunError("x").sqlstate is None


def test_a_class_08_overlay_error_is_not_retried_as_a_source_drop(monkeypatch):
    assert engine.is_source_transient_error(OverlayCatalogError("x", cause=_Conn08()), SourceType.POSTGRES)
    sleeps = []
    monkeypatch.setattr(engine._time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS", "3")

    class _Migrator:
        source_type = SourceType.POSTGRES
        attempts = 0

        def migrate_table(self, table, **_kw):
            self.attempts += 1
            raise OverlayCatalogError("cannot read the overlay columns", cause=_Conn08())

    migrator = _Migrator()
    with pytest.raises(OverlayCatalogError):
        _migrate_table_with_source_retry(migrator, _tables()[0], on_rows=lambda *a, **k: None,
                                         handle=_RetryHandle())
    assert migrator.attempts == 1 and sleeps == []

    calls = []

    def work():
        calls.append(1)
        raise OverlayCatalogError("cannot read the overlay columns", cause=_Conn08())

    with pytest.raises(OverlayCatalogError):
        _retry_source_drops_in_process(work, cancelled=lambda: False, table_name="orders",
                                       source_type=SourceType.POSTGRES)
    assert calls == [1] and sleeps == []


# ---------------------------------------------------------------------------
# P2-06: step 0, the overlaying recreator, thread path B, _assert_overlay_in_place
# ---------------------------------------------------------------------------

import copy  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from dsql_migrator.core.error_log import ErrorLogStore  # noqa: E402
from dsql_migrator.core.job_manager import JobManager  # noqa: E402
from dsql_migrator.core.models import (  # noqa: E402
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.core.batched_import import BatchedImportResult, OnConflictMode  # noqa: E402
from dsql_migrator.core.overlay_ledger import LedgerRead, OverlayLedgerError, overlay_ledger_key  # noqa: E402
from dsql_migrator.core.schema_applier import apply_exclusion_overlay  # noqa: E402
from dsql_migrator.core.target_introspector import ColumnNullFacts as F  # noqa: E402
from dsql_migrator.core.target_introspector import target_overlay_facts  # noqa: E402
from dsql_migrator.ui.data_migration._full_load_engine import (  # noqa: E402
    DataMigrationInputs,
    _assert_overlay_in_place,
    _log_excluded_lob_columns,
    _lob_excluded_note,
    run_full_load,
    run_full_load_retry,
)

from tests.test_overlay_core import FakeDb  # noqa: E402
from tests.test_ui_data_migration import _FakeExporter, _FakeWatermarkCapturer, _watermark  # noqa: E402

DOCS = TableDef(name="docs", primary_key=["id"], columns=[
    ColumnDef(name="id", mysql_type="int", nullable=False),
    ColumnDef(name="body", mysql_type="text", nullable=False, default="x"),
    ColumnDef(name="note", mysql_type="text")])
PLAIN = TableDef(name="plain", primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="int", nullable=False)])

# The target as the applied DDL creates it (a recreate restores exactly this).
FRESH = {
    "docs": {"id": {"notnull": True, "default": None}, "body": {"notnull": True, "default": "'x'::text"},
             "note": {"notnull": False, "default": None}},
    "plain": {"id": {"notnull": True, "default": None}},
}


class _Importer:
    def __init__(self, db):
        self.db, self.calls = db, []

    def import_rows(self, rows, table, *, index_ddls=None, job=None, on_batch_loaded=None,
                    should_cancel=None, on_conflict=None, shard_sources=None, key_columns=None):
        rows = list(rows)
        self.calls.append((table.name, on_conflict, [c.name for c in table.columns]))
        self.index_ddls = getattr(self, "index_ddls", {})
        self.index_ddls[table.name] = index_ddls
        self.db.rows[table.name] = True
        return BatchedImportResult(rows_loaded=len(rows))


class _Rig:
    """A BatchedTableMigrator on the thread path over a fake DSQL catalog (FakeDb) with the
    REAL facts reader and overlay unit behind the migrator seams."""

    def __init__(self, *, tables=(DOCS, PLAIN), rows=None, overlay=None, excluded=None,
                 replace=frozenset(), ledger="memory", in_use=False, legacy=None, cdc=False):
        self.db = FakeDb(copy.deepcopy(FRESH), rows=dict(rows or {}))
        self.recreated, self.facts_calls, self.applied = [], [], []
        excluded = {"docs": frozenset({"body"})} if excluded is None else excluded
        overlay = excluded if overlay is None else overlay
        self.store = InMemoryOverlayLedger(sleep=lambda _s: None) if ledger == "memory" else ledger
        self.inputs = DataMigrationInputs(
            source_config=SourceConnectionConfig(host="db", database="app"),
            source_password=None,
            target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
            inventory=SourceInventory(tables=list(tables)),
            replace_tables=frozenset(replace),
            excluded_lob_columns=excluded,
            overlay_columns=overlay,
            legacy_exclusions=legacy or {},
            overlay_ledger=self.store,
            overlay_ledger_in_use=in_use,
            cdc_coexisting=cdc,
        )
        self.importer = _Importer(self.db)
        self.migrator = BatchedTableMigrator(
            self.inputs,
            exporter=_FakeExporter({"docs": [{"id": 1, "body": "b", "note": "n"}], "plain": [{"id": 1}]}),
            watermark_capturer=_FakeWatermarkCapturer(_watermark()),
            importer_factory=lambda _i: self.importer,
            table_recreator=self._recreate,
            target_counter=lambda t: None,
            target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            overlay_facts_reader=self._facts,
            overlay_applier=self._apply,
        )

    def _recreate(self, table):
        self.recreated.append(table.name)
        self.db.tables[table.name] = {c: dict({"identity": "", "generated": ""}, **v)
                                      for c, v in copy.deepcopy(FRESH[table.name]).items()}
        self.db.rows[table.name] = False
        return []

    def _facts(self, inputs, names, cols, **kw):
        self.facts_calls.append((list(names), kw.get("probe_rows")))
        return target_overlay_facts(names, cols, connection_factory=self.db.connect, sleep=lambda _s: None, **kw)

    def _apply(self, inputs, name, planned, **kw):
        self.applied.append((name, dict(kw.get("expected_defaults") or {})))
        return apply_exclusion_overlay(name, planned, connection_factory=self.db.connect,
                                       sleep=lambda _s: None, jitter=lambda: 0.0, **kw)

    def alters(self):
        return [t.split(" ALTER COLUMN ")[1] for _c, t in self.db.log if t.startswith("ALTER TABLE")]

    def run(self, tables=None, *, retry_prior=None):
        manager, error_log = JobManager(), ErrorLogStore()
        tables = list(tables or self.inputs.inventory.tables)
        if retry_prior is None:
            job_id = manager.submit(lambda h: run_full_load(h, tables, migrator=self.migrator,
                                                            error_log=error_log, inputs=self.inputs))
        else:
            job_id = manager.submit(lambda h: run_full_load_retry(h, retry_prior, tables, migrator=self.migrator,
                                                                  error_log=error_log, inputs=self.inputs))
        assert manager.wait(job_id, timeout=60.0)  # an upper bound: a loaded CI host was slower than 10 s
        self.job, self.error, self.error_log = manager.get_status(job_id), manager.get_error(job_id), error_log
        return {c.chunk_id: c.status for c in self.job.chunks}

    def record(self, table="docs"):
        return self.store.read(overlay_ledger_key(self.inputs.target_config)).tables.get(table)

    def messages(self):
        return " | ".join(r.message for r in self.error_log.records(self.job.job_id))


@pytest.fixture
def events(monkeypatch):
    got = []
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")  # the thread path
    monkeypatch.setattr(engine, "log_activity",
                        lambda category, action, **kw: got.append((action, getattr(kw.get("status"), "value", None),
                                                                   kw.get("target"), kw.get("detail") or "")))
    return got


class _RaisingStore:
    def __getattr__(self, name):
        raise AssertionError(f"the ledger must not be touched ({name})")


def test_a_run_without_exclusions_reads_the_ledger_once_and_is_otherwise_unchanged(events):
    # One read (an earlier session may have left records for this target); with nothing
    # recorded -- or an unreadable / raising store -- the run is exactly as before.
    for store in ("empty", "raising"):
        rig = _Rig(excluded={}, overlay={}, ledger=_RaisingStore() if store == "raising" else "memory")
        reads = []
        if store == "empty":
            real = rig.store.read
            rig.store.read = lambda key: reads.append(key) or real(key)
        bound = []
        rig.migrator.bind_run_inputs = lambda i: bound.append(i)
        assert rig.run() == {"docs": "DONE", "plain": "DONE"}, store
        assert bound == [] and rig.facts_calls == [] and rig.applied == [], store
        assert rig.job.recreated_tables == []
        if store == "empty":
            assert len(reads) == 1 and rig.store.blobs == {}


def test_a_record_left_by_an_earlier_session_is_checked_even_without_exclusions(events):
    # Review fix: the page-header Run of a fresh session (no exclusions, never used the
    # ledger) must not append values into rows an earlier exclusion left NULL.
    rig = _Rig(excluded={}, overlay={}, rows={"docs": True, "plain": True})
    _seed_record(rig)
    marked = []
    rig.migrator.bind_run_inputs(dataclasses.replace(rig.inputs, on_overlay_ledger_used=lambda: marked.append(1)))
    rig.inputs = rig.migrator._inputs
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert "hold NULL from an earlier exclusion set" in rig.messages() and marked == [1]


def test_records_in_use_without_overlay_columns_read_once_and_write_nothing(events):
    rig = _Rig(excluded={}, overlay={}, in_use=True, rows={"docs": True, "plain": True})
    reads = []
    real_read = rig.store.read
    rig.store.read = lambda key: reads.append(key) or real_read(key)
    assert rig.run() == {"docs": "DONE", "plain": "DONE"}
    assert len(reads) == 1 and rig.facts_calls == [] and rig.store.blobs == {}


def test_overlay_columns_without_a_ledger_fail_before_the_watermark(events):
    for retry in (False, True):
        rig = _Rig(ledger=None)
        hooks = []
        rig.migrator.predrop_blocking_foreign_keys = lambda: hooks.append("fk")
        rig.migrator.predrop_dependent_views = lambda: hooks.append("views")
        rig.migrator._watermark_capturer.capture = lambda *a: hooks.append("watermark")
        events.clear()
        rig.run(retry_prior=[] if retry else None)
        assert rig.job.status == "FAILED" and "no overlay ledger configured" in rig.error
        assert hooks == [] and rig.applied == [] and rig.importer.calls == []
        terminal = [e for e in events if e[0] == ("retry failed" if retry else "run failed")]
        assert len(terminal) == 1 and "BEFORE any table was" in terminal[0][3]


def test_the_ledger_is_read_before_the_pre_drops_in_both_run_paths(events):
    for retry in (False, True):
        rig = _Rig(rows={"docs": False})
        order = []
        real_read = rig.store.read
        rig.store.read = lambda key: order.append("read") or real_read(key)
        rig.migrator.predrop_blocking_foreign_keys = lambda: order.append("fk")
        rig.migrator.predrop_dependent_views = lambda: order.append("views")
        rig.run(retry_prior=[] if retry else None)
        assert order[:3] == ["read", "fk", "views"], (retry, order)


def test_an_unreadable_ledger_fails_only_the_overlay_tables_and_keeps_them_out_of_replace(events):
    rig = _Rig(replace={"docs", "plain"})
    rig.store.read = lambda key: LedgerRead("unreadable", {}, "AccessDenied")
    seen_replace = []
    rig.migrator.predrop_blocking_foreign_keys = lambda: seen_replace.append(set(rig.migrator._inputs.replace_tables))
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert seen_replace == [{"plain"}]  # nothing of docs is dropped
    assert rig.recreated == ["plain"] and rig.applied == []
    assert [c[0] for c in rig.importer.calls] == ["plain"]
    assert "cannot read the record of dropped DEFAULTs; nothing was changed on this table" in rig.messages()
    warnings = [e for e in events if e[0] == "overlay ledger unreadable"]
    assert len(warnings) == 1 and "fail: docs." in warnings[0][3]
    assert not [e for e in events if e[0] == "run failed"]


def test_thread_path_recreate_records_then_overlays_then_loads(events):
    rig = _Rig(replace={"docs"})
    assert rig.run([DOCS]) == {"docs": "DONE"}
    assert rig.recreated == ["docs"]
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert rig.applied == [("docs", {"body": "'x'::text"})]  # only the recorded DEFAULT may be dropped
    record = rig.record()
    assert record.set_columns == ["body"] and record.empty_when_applied
    assert record.column("body").original_default == "'x'::text" and record.column("body").dropped_not_null
    assert rig.job.recreated_tables == ["docs"]
    assert rig.importer.calls == [("docs", OnConflictMode.NONE, ["id", "note"])]
    actions = [e[0] for e in events]
    assert actions.index("overlay planned") < actions.index("overlay applied")
    planned = [e for e in events if e[0] == "overlay planned"][0][3]
    assert "body: DROP NOT NULL, DROP DEFAULT (was 'x'::text)" in planned


def test_thread_path_appends_in_place_into_an_empty_table(events):
    rig = _Rig(rows={"docs": False})
    assert rig.run([DOCS]) == {"docs": "DONE"}
    assert rig.recreated == [] and rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert rig.record().empty_when_applied is True
    assert rig.importer.calls == [("docs", OnConflictMode.SKIP_EXISTING, ["id", "note"])]


def test_an_append_into_rows_written_without_the_set_needs_drop_and_reload(events):
    rig = _Rig(rows={"docs": True, "plain": True})
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert "needs Drop & reload" in rig.messages() and "nothing was changed on it" in rig.messages()
    assert rig.alters() == [] and rig.store.blobs == {} and [c[0] for c in rig.importer.calls] == ["plain"]
    # Exempt while CDC coexists: appended with a WARNING, the overlay still applied.
    rig = _Rig(rows={"docs": True}, cdc=True)
    assert rig.run([DOCS]) == {"docs": "DONE"}
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert [e for e in events if e[0] == "overlay append" and e[1] == "warning"]


def test_a_failing_record_write_fails_only_that_table(events):
    rig = _Rig(rows={"docs": False, "plain": False})
    rig.store.merge_tables = lambda key, merges: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert rig.alters() == []  # nothing ALTERed without its record
    assert "cannot record the DEFAULTs of docs before dropping them" in rig.messages()
    # A recreated table: the text says it was recreated empty and how to finish it.
    rig = _Rig(replace={"docs"})
    rig.store.merge_tables = lambda key, merges: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    assert rig.run([DOCS]) == {"docs": "FAILED"}
    assert rig.alters() == [] and "recreated empty; Retry unfinished recreates and loads it" in rig.messages()
    assert rig.job.recreated_tables == ["docs"]


def test_a_source_drop_re_entry_appends_under_the_same_record(events, monkeypatch):
    class _Lost(Exception):
        pass

    lost = _Lost(2013, "Lost connection to MySQL server during query")
    rig = _Rig(rows={"docs": False})
    reads = []
    real_read = rig.store.read
    rig.store.read = lambda key: reads.append(key) or real_read(key)
    first = {"done": False}
    real_import = rig.importer.import_rows

    def flaky(rows, table, **kw):
        out = real_import(rows, table, **kw)
        if not first["done"]:
            first["done"] = True
            raise lost
        return out

    rig.importer.import_rows = flaky
    monkeypatch.setattr(engine._time, "sleep", lambda _s: None)
    assert rig.run([DOCS]) == {"docs": "DONE"}
    assert len(reads) == 1  # ONE ledger read per run; the view carries attempt 1's write
    assert [c[1] for c in rig.importer.calls] == [OnConflictMode.SKIP_EXISTING, OnConflictMode.SKIP_EXISTING]
    assert "needs Drop & reload" not in (rig.error or "")


def test_a_worker_recreate_verifies_the_recorded_defaults_before_any_alter(events):
    rig = _Rig(replace={"docs"})
    worker = BatchedTableMigrator(
        _slim_worker_inputs(dataclasses.replace(rig.inputs, overlay_recorded_defaults={"docs": {"body": "'y'::text"}}),
                            "docs"),
        exporter=object(), watermark_capturer=object(), table_recreator=rig._recreate,
        overlay_facts_reader=rig._facts, overlay_applier=rig._apply)
    with pytest.raises(OverlayRecordMismatchError, match="not the 'y'::text recorded before the load"):
        worker._overlaying_recreator(DOCS)
    assert rig.applied == [] and rig.alters() == []  # not even DROP NOT NULL
    worker.bind_run_inputs(dataclasses.replace(worker._inputs, overlay_recorded_defaults={"docs": {"body": "'x'::text"}}))
    worker._overlaying_recreator(DOCS)
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT'] and rig.store.blobs == {}


def test_the_in_place_assertion_fails_a_worker_closed_before_any_write(events):
    rig = _Rig(replace={"docs"})
    worker = BatchedTableMigrator(
        _slim_worker_inputs(rig.inputs, "docs"), exporter=_FakeExporter({"docs": [{"id": 1}]}),
        watermark_capturer=object(), importer_factory=lambda _i: rig.importer,
        table_recreator=rig._recreate, target_counter=lambda t: None, target_generated_reader=lambda t: set(),
        overlay_facts_reader=rig._facts, overlay_applier=rig._apply)
    # The process path's parent recreated it but has not overlaid it (interim, pinned).
    with pytest.raises(OverlayNotInPlaceError, match="still NOT NULL or with a DEFAULT"):
        worker.migrate_table(DOCS, pre_recreated=True)
    assert rig.importer.calls == []
    with pytest.raises(OverlayNotInPlaceError, match="could not confirm"):
        _assert_overlay_in_place("docs", {"body"}, facts_reader=lambda *a, **k: {"docs": None})
    # Live computed columns are omit-only and never block.
    rig.db.tables["docs"]["body"]["generated"] = "s"
    _assert_overlay_in_place("docs", {"body"}, facts_reader=lambda n, c, **k: rig._facts(None, n, c, **k))


def test_the_shard_worker_asserts_the_overlay_before_its_first_write(monkeypatch):
    rig = _Rig(replace={"docs"})
    monkeypatch.setattr(engine, "_default_overlay_facts_reader", rig._facts)
    monkeypatch.setattr(BatchedTableMigrator, "_default_target_generated", lambda self, t: set())
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    args = _ShardWorkerArgs(job_id="j", table=DOCS, inputs=_slim_worker_inputs(rig.inputs, "docs"),
                            pk_lower=1, pk_upper=10, shard_index=0)
    result = engine._migrate_shard_in_process(args)
    assert result.status == "FAILED" and "still NOT NULL or with a DEFAULT" in result.error_message


def test_migrate_table_without_exclusions_reads_no_overlay_facts():
    rig = _Rig(excluded={}, overlay={})

    def never(*a, **k):
        raise AssertionError("no overlay facts read without exclusions")

    rig.migrator._overlay_facts_reader = never
    rig.migrator.migrate_table(PLAIN)
    rig.migrator.migrate_table(DOCS)


def test_the_activity_copy_names_null_legacy_and_plain_exclusions(events):
    inputs = dataclasses.replace(
        _inputs(), excluded_lob_columns={"t": frozenset({"a", "b", "c"})},
        overlay_columns={"t": frozenset({"a"})}, legacy_exclusions={"t": frozenset({"b"})})
    _log_excluded_lob_columns(inputs)
    details = {e[2]: e[3] for e in events if e[0] == "column excluded"}
    assert "every row stores NULL" in details["t.a"]
    assert "earlier version" in details["t.b"] and "not altered" in details["t.b"]
    assert "left NULL on the target" in details["t.c"]  # unchanged when not an overlay entry
    migrator = BatchedTableMigrator(inputs, exporter=object(), watermark_capturer=object())
    assert _lob_excluded_note(migrator, "t") == " (3 column(s) excluded: a, b, c; loaded as NULL: a)"
    only = BatchedTableMigrator(dataclasses.replace(inputs, overlay_columns={"t": frozenset({"a", "b", "c"})}),
                                exporter=object(), watermark_capturer=object())
    assert _lob_excluded_note(only, "t") == " (3 column(s) excluded: a, b, c; loaded as NULL)"


def test_an_overlay_failure_never_earns_a_source_connection_hint(events):
    class _PgMigrator:
        source_type = SourceType.POSTGRES

        def capture_watermark(self, tables):
            return _watermark()

        def migrate_table(self, table, **_kw):
            raise OverlayCatalogError("cannot read the overlay columns", cause=_Conn08())

    manager, error_log = JobManager(), ErrorLogStore()
    job_id = manager.submit(lambda h: run_full_load(h, [PLAIN], migrator=_PgMigrator(), error_log=error_log))
    assert manager.wait(job_id, timeout=5.0)
    (record,) = error_log.records(job_id)
    assert record.error_code == "08006" and "cannot read the overlay columns" in record.message
    assert "connection dropped" not in record.message and "failover" not in record.message


# ---------------------------------------------------------------------------
# P2-07/08: the process-path pre-pass (driven through run_full_load, workers inline)
# ---------------------------------------------------------------------------

import concurrent.futures  # noqa: E402

from dsql_migrator.core.column_exclusion import set_digest  # noqa: E402
from dsql_migrator.core.converter import TableConversion  # noqa: E402
from dsql_migrator.core.overlay_ledger import OverlayLedgerColumn, OverlayLedgerTable  # noqa: E402
from dsql_migrator.ui.data_migration._full_load_engine import _seed_retry_chunks  # noqa: E402

from tests.test_overlay_core import Denied  # noqa: E402


class _InlineExecutor:
    """ProcessPoolExecutor stand-in: runs each worker function inline, in submission order."""

    submitted: list = []

    def __init__(self, max_workers=None, mp_context=None, initializer=None, initargs=()):
        assert max_workers and max_workers >= 1
        if initializer is not None:
            initializer(*initargs)

    def submit(self, fn, *args):
        _InlineExecutor.submitted.append(args[0].table.name)
        future = concurrent.futures.Future()
        try:
            future.set_result(fn(*args))
        except BaseException as exc:  # noqa: BLE001
            future.set_exception(exc)
        return future

    def shutdown(self, wait=True, cancel_futures=False):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _ProcRig(_Rig):
    """The rig on the PROCESS path: a BatchedTableMigrator subclass the workers rebuild
    from their slimmed inputs (so they get the same fake catalog and seams)."""

    def __init__(self, monkeypatch, **kw):
        super().__init__(**kw)
        rig = self
        self.worker_inputs = []

        class RigMigrator(BatchedTableMigrator):
            def __init__(self, inputs, **_ignored):
                rig.worker_inputs.append(inputs)
                super().__init__(
                    inputs,
                    exporter=_FakeExporter({"docs": [{"id": 1, "body": "b", "note": "n"}], "plain": [{"id": 1}],
                                            **{f"t{i}": [{"id": 1}] for i in range(60)}}),
                    watermark_capturer=_FakeWatermarkCapturer(_watermark()),
                    importer_factory=lambda _i: rig.importer,
                    table_recreator=rig._recreate,
                    target_counter=lambda t: None,
                    target_pk_reader=lambda t: list(t.primary_key),
                    target_generated_reader=lambda t: set(),
                    overlay_facts_reader=rig._facts,
                    overlay_applier=rig._apply,
                )

        monkeypatch.setattr(engine, "BatchedTableMigrator", RigMigrator)
        monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
        monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
        monkeypatch.setattr(engine, "_worker_progress_queue", None)
        monkeypatch.setattr(engine, "_worker_cancel_event", None)
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
        _InlineExecutor.submitted = []
        self.worker_inputs.clear()
        self.migrator = RigMigrator(self.inputs)
        self.worker_inputs.clear()


def _failures_for(events, name):
    return [e for e in events if e[0] == "load table" and e[1] == "failure" and e[2] == name]


def _faulty_facts(rig, *, on_call):
    calls = {"n": 0}
    real = rig._facts

    def facts(inputs, names, cols, **kw):
        calls["n"] += 1
        if calls["n"] == on_call:
            return {n: None for n in names} if on_call != "raise" else None
        return real(inputs, names, cols, **kw)

    return facts


@pytest.mark.parametrize("where", ["step1-unknown", "step1-raise", "step2-undecidable", "step3-rows",
                                   "step4-recreate", "step5-unknown", "step6-alter"])
def test_process_prepass_fails_only_the_failing_table_counted_once(monkeypatch, events, where):
    replace = {"docs"} if where in ("step4-recreate", "step5-unknown", "step6-alter") else set()
    rig = _ProcRig(monkeypatch, replace=replace, rows={"docs": where == "step3-rows"})
    if where in ("step1-unknown", "step5-unknown"):
        # A replace table is first read after its recreate (step 5), so that is call 1 too.
        rig.migrator._overlay_facts_reader = _faulty_facts(rig, on_call=1)
    elif where == "step1-raise":
        rig.migrator._overlay_facts_reader = lambda *a, **k: (_ for _ in ()).throw(Conn08e())
    elif where == "step2-undecidable":
        conv = TableConversion(table="docs", target_ddl='CREATE TABLE "docs" ("id" INT, "note" TEXT, "body" TEXT, PRIMARY KEY ("note", "id"))')
        rig.migrator.bind_run_inputs(dataclasses.replace(rig.inputs, table_conversions={"docs": conv}))
        rig.inputs = rig.migrator._inputs
        rig.migrator._overlay_facts_reader = _faulty_facts(rig, on_call=1)
    elif where == "step4-recreate":
        def boom(table):
            raise RuntimeError("cannot drop table docs because other objects depend on it")
        rig.migrator._table_recreator = boom
    elif where == "step6-alter":
        rig.db.fault(r"DROP NOT NULL", Denied())
    states = rig.run()
    assert states == {"docs": "FAILED", "plain": "DONE"}, (where, rig.messages())
    records = [r for r in rig.error_log.records(rig.job.job_id) if r.table == "docs"]
    assert len(records) == 1 and records[0].occurred_at is not None, where
    assert len(_failures_for(events, "docs")) == 1, where
    assert "1 of 2 table(s) did not fully load" in rig.error
    assert "docs" not in _InlineExecutor.submitted and "plain" in _InlineExecutor.submitted
    if where == "step2-undecidable":
        assert rig.recreated == []
    if where == "step5-unknown":
        assert rig.record() is None and rig.alters() == [] and rig.job.recreated_tables == ["docs"]
    if where == "step6-alter":
        assert records[0].error_code == "42501" and rig.job.recreated_tables == ["docs"]


class Conn08e(Exception):
    sqlstate = "08006"


def test_when_every_table_fails_in_the_prepass_the_pool_is_still_well_formed(monkeypatch, events):
    rig = _ProcRig(monkeypatch, tables=(DOCS,), rows={"docs": True})  # needs Drop & reload
    assert rig.run() == {"docs": "FAILED"}
    assert "1 of 1 table(s) did not fully load" in rig.error and _InlineExecutor.submitted == []


def test_process_path_records_overlays_serially_then_submits(monkeypatch, events):
    rig = _ProcRig(monkeypatch, replace={"docs"})
    merges = []
    real_merge = rig.store.merge_tables
    rig.store.merge_tables = lambda key, m: merges.append(sorted(m)) or real_merge(key, m)
    order = []
    real_apply = rig._apply

    def apply(inputs, name, planned, **kw):
        order.append(("alter", name))
        return real_apply(inputs, name, planned, **kw)

    rig.migrator._overlay_applier = apply
    real_submit = _InlineExecutor.submit
    monkeypatch.setattr(_InlineExecutor, "submit", lambda self, fn, *a: order.append(("submit", a[0].table.name)) or real_submit(self, fn, *a))
    states = rig.run()
    assert states == {"docs": "DONE", "plain": "DONE"}, rig.error
    assert merges == [["docs"]]  # ONE batch, before any ALTER
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert order.index(("alter", "docs")) < min(i for i, e in enumerate(order) if e[0] == "submit")
    assert rig.record().column("body").original_default == "'x'::text"
    assert rig.job.recreated_tables == ["docs"]
    docs_worker = [i for i in rig.worker_inputs if "docs" in i.excluded_lob_columns][0]
    assert dict(docs_worker.overlay_recorded_defaults) == {"docs": {"body": "'x'::text"}}
    assert docs_worker.overlay_record is None  # a worker never writes the ledger
    assert ("docs", OnConflictMode.NONE, ["id", "note"]) in rig.importer.calls


def test_the_serial_pin_holds_across_tables(monkeypatch, events):
    tables = [TableDef(name=f"t{i}", primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                                                                  ColumnDef(name="c", mysql_type="text", nullable=False)])
              for i in range(5)]
    for t in tables:
        FRESH[t.name] = {"id": {"notnull": True, "default": None}, "c": {"notnull": True, "default": "'v'::text"}}
    try:
        overlay = {t.name: frozenset({"c"}) for t in tables}
        rig = _ProcRig(monkeypatch, tables=tables, excluded=overlay, replace={t.name for t in tables})
        rig.db.fault(r'"t1" ALTER COLUMN "c" DROP NOT NULL', Occ())
        inflight = {"now": 0, "max": 0}
        real_apply = rig._apply

        def apply(inputs, name, planned, **kw):
            inflight["now"] += 1
            inflight["max"] = max(inflight["max"], inflight["now"])
            try:
                return real_apply(inputs, name, planned, **kw)
            finally:
                inflight["now"] -= 1

        rig.migrator._overlay_applier = apply
        assert set(rig.run().values()) == {"DONE"}
        assert inflight["max"] == 1
        per_table = [t.split(" ALTER COLUMN ")[0] + " " + t.split(" ALTER COLUMN ")[1]
                     for _c, t in rig.db.log if t.startswith("ALTER TABLE")]
        assert per_table == [f'ALTER TABLE "t{i}" "c" {a}' for i in range(5)
                             for a in (["DROP NOT NULL", "DROP NOT NULL", "DROP DEFAULT"] if i == 1 else ["DROP NOT NULL", "DROP DEFAULT"])]
    finally:
        for t in tables:
            FRESH.pop(t.name, None)


from tests.test_overlay_core import Occ  # noqa: E402


def test_a_failing_batch_alters_nothing_and_fails_what_it_carried(monkeypatch, events):
    key = overlay_ledger_key(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    rig = _ProcRig(monkeypatch, replace={"docs", "plain"}, excluded={"docs": frozenset({"body"})})
    owed = OverlayLedgerTable(table="plain", set_digest="d", set_columns=["gone"], empty_when_applied=True,
                              applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                              columns=[OverlayLedgerColumn(column="gone", original_default="'g'::text")])
    rig.store.put_tables(key, {"plain": owed})
    rig.store.merge_tables = lambda k, m: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    assert rig.run() == {"docs": "FAILED", "plain": "FAILED"}
    assert rig.alters() == [] and rig.applied == [] and rig.importer.calls == []
    text = rig.messages()
    assert "cannot record the DEFAULTs of docs before dropping them; nothing was altered" in text
    assert "cannot record the change to the exclusion record of plain; nothing was loaded" in text
    assert "recreated empty; Retry unfinished recreates and loads it" in text


def test_a_recreated_record_only_table_is_deleted_before_its_first_write(monkeypatch, events):
    key = overlay_ledger_key(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    rig = _ProcRig(monkeypatch, replace={"plain"}, excluded={}, in_use=True)
    owed = OverlayLedgerTable(table="plain", set_digest="d", set_columns=["gone"], empty_when_applied=True,
                              applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                              columns=[OverlayLedgerColumn(column="gone", original_default="'g'::text")])
    rig.store.put_tables(key, {"plain": owed})
    order = []
    real_merge = rig.store.merge_tables
    rig.store.merge_tables = lambda k, m: order.append("ledger") or real_merge(k, m)
    real_import = rig.importer.import_rows
    rig.importer.import_rows = lambda rows, table, **kw: order.append("import") or real_import(rows, table, **kw)
    assert rig.run([PLAIN]) == {"plain": "DONE"}
    assert rig.record("plain") is None and order.index("ledger") < order.index("import")


def test_an_unreadable_ledger_on_the_process_path_never_recreates_the_overlay_table(monkeypatch, events):
    rig = _ProcRig(monkeypatch, replace={"docs", "plain"})
    rig.store.read = lambda key: LedgerRead("unreadable", {}, "AccessDenied")
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert rig.recreated == ["plain"] and rig.alters() == []
    assert "cannot read the record of dropped DEFAULTs; nothing was changed on this table" in rig.messages()
    assert len(_failures_for(events, "docs")) == 1


def test_the_prepass_budget_is_constant_in_connections(monkeypatch, events):
    tables = [TableDef(name=f"t{i}", primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                                                                  ColumnDef(name="c", mysql_type="text")])
              for i in range(50)]
    for t in tables:
        FRESH[t.name] = {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}
    try:
        rig = _ProcRig(monkeypatch, tables=tables, excluded={t.name: frozenset({"c"}) for t in tables})
        at_submit = {}
        real_submit = _InlineExecutor.submit
        monkeypatch.setattr(_InlineExecutor, "submit", lambda self, fn, *a: at_submit.setdefault("connects", rig.db.connects) and real_submit(self, fn, *a) or real_submit(self, fn, *a))
        merges = []
        real_merge = rig.store.merge_tables
        rig.store.merge_tables = lambda k, m: merges.append(len(m)) or real_merge(k, m)
        assert set(rig.run().values()) == {"DONE"}
        assert at_submit["connects"] == 1  # the one step-1 read for 50 tables
        assert merges == [50] and rig.applied == []
    finally:
        for t in tables:
            FRESH.pop(t.name, None)


def test_retry_seeding_keeps_every_unfinished_recreate():
    prior = [ChunkState(chunk_id="a", status="FAILED"), ChunkState(chunk_id="b", status="PENDING"),
             ChunkState(chunk_id="c", status="DONE")]
    job = MigrationJob(job_id="r1")
    _seed_retry_chunks(job, prior, {"a"}, prior_recreated_tables=["a", "b", "c", "b", "zz"])
    assert job.recreated_tables == ["a", "b"]  # b is withheld from this retry but kept
    job2 = MigrationJob(job_id="r0")
    _seed_retry_chunks(job2, prior, {"a"})
    assert job2.recreated_tables == []


def test_the_parent_promotes_an_empty_overlay_table_whose_key_changed(monkeypatch, events):
    conv = TableConversion(
        table="docs",
        target_ddl='CREATE TABLE "docs" ("id" INT NOT NULL, "body" TEXT NOT NULL DEFAULT \'x\', "note" TEXT NOT NULL, PRIMARY KEY ("note", "id"))',
        index_ddls=['CREATE UNIQUE INDEX ASYNC docs_u ON "docs" ("body")'])
    rig = _ProcRig(monkeypatch, rows={"docs": False})
    rig.migrator.bind_run_inputs(dataclasses.replace(rig.inputs, table_conversions={"docs": conv}))
    rig.inputs = rig.migrator._inputs
    rig.db.pks["docs"] = ["id"]  # the live key is still the source key
    states = rig.run()
    assert states == {"docs": "DONE", "plain": "DONE"}, rig.messages()
    assert "docs" in rig.migrator._inputs.replace_tables and rig.recreated == ["docs"]
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert ("docs", OnConflictMode.NONE, ["id", "note"]) in rig.importer.calls
    assert rig.importer.index_ddls["docs"] == ['CREATE UNIQUE INDEX ASYNC docs_u ON "docs" ("body")']
    assert [e for e in events if e[2] == "docs" and "applied key differs" in e[3]]


def test_a_run_without_exclusions_keeps_the_recreate_abort(monkeypatch, events):
    # Decision 1: per-table isolation only with the overlay pre-pass; otherwise 0.1.561.
    rig = _ProcRig(monkeypatch, replace={"plain"}, excluded={}, overlay={}, ledger=_RaisingStore())

    def boom(table):
        raise RuntimeError("cannot drop table plain because other objects depend on it")

    rig.migrator._table_recreator = boom
    states = rig.run([DOCS, PLAIN])
    assert rig.job.status == "FAILED" and "cannot drop table plain" in rig.error
    assert _InlineExecutor.submitted == [] and set(states.values()) <= {"PENDING", "FAILED"}
    assert [e for e in events if e[0] == "run failed"]


def test_a_recorded_only_append_with_rows_is_never_batched_and_loads_when_the_batch_fails(monkeypatch, events):
    key = overlay_ledger_key(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    rig = _ProcRig(monkeypatch, rows={"docs": False, "plain": True})
    owed = OverlayLedgerTable(table="plain", set_digest="d", set_columns=["gone"], empty_when_applied=True,
                              applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                              columns=[OverlayLedgerColumn(column="gone", original_default="'g'::text")])
    rig.store.put_tables(key, {"plain": owed})
    batches = []
    rig.store.merge_tables = lambda k, m: batches.append(sorted(m)) or (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    assert rig.run() == {"docs": "FAILED", "plain": "DONE"}
    assert batches == [["docs"]] and rig.applied == []


def test_a_worker_source_drop_recreates_verifies_the_recorded_default_and_loads(monkeypatch, events):
    class _Lost(Exception):
        pass

    rig = _ProcRig(monkeypatch, replace={"docs"})
    monkeypatch.setattr(engine._time, "sleep", lambda _s: None)
    real_import = rig.importer.import_rows
    state = {"first": True}

    def flaky(rows, table, **kw):
        out = real_import(rows, table, **kw)
        if table.name == "docs" and state["first"]:
            state["first"] = False
            raise _Lost(2013, "Lost connection to MySQL server during query")
        return out

    rig.importer.import_rows = flaky
    assert rig.run([DOCS]) == {"docs": "DONE"}, rig.messages()
    assert rig.recreated == ["docs", "docs"]  # the parent, then the worker's own retry
    # Parent overlay, then the worker's VERIFY-RECORDED overlay after its own recreate.
    assert rig.alters() == ['"body" DROP NOT NULL', '"body" DROP DEFAULT'] * 2
    assert [a[1] for a in rig.applied] == [{"body": "'x'::text"}, {"body": "'x'::text"}]


def test_a_cdc_coexisting_run_drops_a_restored_default_before_its_first_write(monkeypatch, events):
    key = overlay_ledger_key(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    rig = _ProcRig(monkeypatch, rows={"docs": True}, cdc=True, tables=(DOCS,))
    rig.db.tables["docs"]["body"]["notnull"] = False  # an earlier overlay dropped NOT NULL
    record = OverlayLedgerTable(
        table="docs", set_digest=set_digest(["body"]),
        set_columns=["body"], empty_when_applied=True, applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        columns=[OverlayLedgerColumn(column="body", original_default="'x'::text", dropped_not_null=True,
                                     restored_at=datetime(2026, 9, 2, tzinfo=timezone.utc))])
    rig.store.put_tables(key, {"docs": record})
    order = []
    real_import = rig.importer.import_rows
    rig.importer.import_rows = lambda rows, table, **kw: order.append("import") or real_import(rows, table, **kw)
    real_apply = rig._apply
    rig.migrator._overlay_applier = lambda *a, **k: order.append("alter") or real_apply(*a, **k)
    assert rig.run() == {"docs": "DONE"}, rig.messages()
    assert rig.alters() == ['"body" DROP DEFAULT'] and order == ["alter", "import"]
    after = rig.record().column("body")
    assert after.original_default == "'x'::text" and after.restored_at is None  # owed again



# ---------------------------------------------------------------------------
# Review fixes (P2-05/06 verification)
# ---------------------------------------------------------------------------


def _seed_record(rig, table="docs", columns=("body",), empty=True):
    key = overlay_ledger_key(rig.inputs.target_config)
    rig.store.put_tables(key, {table: OverlayLedgerTable(
        table=table, set_digest=set_digest(list(columns)), set_columns=list(columns), empty_when_applied=empty,
        applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        columns=[OverlayLedgerColumn(column=c, original_default="'x'::text", dropped_not_null=True) for c in columns])})


@pytest.mark.parametrize("path", ["thread", "process"])
def test_an_unreadable_ledger_blocks_every_table_of_a_session_that_used_it(monkeypatch, events, path):
    kw = dict(excluded={}, overlay={}, in_use=True, rows={"docs": True, "plain": True}, replace={"plain"})
    rig = _ProcRig(monkeypatch, **kw) if path == "process" else _Rig(**kw)
    rig.store.read = lambda key: LedgerRead("unreadable", {}, "AccessDenied")
    assert rig.run() == {"docs": "FAILED", "plain": "FAILED"}
    assert rig.importer.calls == [] and rig.recreated == []  # docs may hold NULL rows; nothing is touched
    (warning,) = [e for e in events if e[0] == "overlay ledger unreadable"]
    assert "fail: docs, plain." in warning[3]


def test_a_recorded_column_the_load_cannot_write_is_not_re_included(events):
    # (a) the source no longer has it; (b) the target computes it. New rows land NULL like the old.
    source_dropped = TableDef(name="docs", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="note", mysql_type="text")])
    rig = _Rig(tables=(source_dropped,), excluded={}, overlay={}, in_use=True, rows={"docs": True})
    _seed_record(rig)
    assert rig.run() == {"docs": "DONE"}, rig.messages()
    rig = _Rig(tables=(DOCS,), excluded={}, overlay={}, in_use=True, rows={"docs": True})
    _seed_record(rig)
    rig.db.tables["docs"]["body"]["generated"] = "s"
    rig.migrator._target_generated_reader = lambda t: {"body"}
    assert rig.run() == {"docs": "DONE"}, rig.messages()
    # Control: truly re-included (the source has it, the target does not compute it) -> refused.
    rig = _Rig(tables=(DOCS,), excluded={}, overlay={}, in_use=True, rows={"docs": True})
    _seed_record(rig)
    assert rig.run() == {"docs": "FAILED"} and "hold NULL from an earlier exclusion set" in rig.messages()


@pytest.mark.parametrize("path", ["thread", "process"])
def test_a_legacy_entry_does_not_exempt_a_new_overlay_column_from_the_reload_verdict(monkeypatch, events, path):
    kw = dict(tables=(DOCS,), rows={"docs": True}, excluded={"docs": frozenset({"body", "note"})},
              overlay={"docs": frozenset({"body"})}, legacy={"docs": frozenset({"note"})})
    rig = _ProcRig(monkeypatch, **kw) if path == "process" else _Rig(**kw)
    assert rig.run() == {"docs": "FAILED"}
    assert "needs Drop & reload" in rig.messages() and rig.alters() == [] and rig.importer.calls == []
    assert rig.record() is None


def test_a_failed_record_update_on_a_recreated_table_without_exclusions_says_what_failed(events):
    rig = _Rig(tables=(DOCS,), excluded={}, overlay={}, in_use=True, replace={"docs"})
    _seed_record(rig)
    rig.store.merge_tables = lambda key, merges: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    assert rig.run() == {"docs": "FAILED"}
    text = rig.messages()
    assert "cannot update the exclusion record of docs after recreating it" in text
    assert "before dropping them" not in text


def test_a_computed_overlay_column_is_not_reported_loaded_as_null(events):
    rig = _Rig(tables=(DOCS,), rows={"docs": False})
    rig.db.tables["docs"]["body"]["generated"] = "s"
    assert rig.run() == {"docs": "DONE"} and rig.alters() == []
    (line,) = [e for e in events if e[0] == "load table" and e[2] == "docs"]
    assert "loaded as NULL" not in line[3] and "excluded: body" in line[3]


# ---------------------------------------------------------------------------
# P2-10: activation (the builders pass the overlay inputs; the app builds the ledger)
# ---------------------------------------------------------------------------

import inspect  # noqa: E402

import dsql_migrator.ui.data_migration as dm  # noqa: E402


def _squash(src):
    return " ".join(src.split())


def test_the_start_and_retry_builders_pass_the_overlay_inputs_and_consume_the_forced_set():
    src = _squash(inspect.getsource(dm.build_data_migration_screen))
    assert src.count("_overlay_inputs(") == 2  # the Start and retry builders
    assert "_run_replace, _disclosed = migration_state.take_run_choice()" in src
    assert "replace_tables=_run_replace," in src
    assert "conv_state.preserve_foreign_keys, tables=names, )" in src  # scoped to the run's tables
    assert "taken_replace, retry_disclosed = migration_state.take_run_choice()" in src
    assert "frozenset(taken_replace | set(recreated_unfinished(current))) & set(names)" in src
    assert "prior_recreated_tables=prior_recreated," in src
    # Marked just before each submit, not while the inputs are built.
    assert src.count("_mark_overlay_use(migration_state, inputs.overlay_columns) migration_state.job_id = job_manager.submit(work)") == 1
    assert src.count("_mark_overlay_use(migration_state, retry_inputs.overlay_columns) migration_state.job_id = job_manager.submit(work)") == 1
    fk_builder = src[src.index("def _fk_apply_inputs"):src.index("fk_inputs = _fk_apply_inputs()")]
    assert "_overlay_inputs" not in fk_builder  # loads nothing, no ledger


def test_overlay_inputs_are_the_overlay_legacy_sets_and_the_ledger_handle():
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import apply_session_snapshot

    state = dm.DataMigrationState()
    apply_session_snapshot(SessionSnapshot(session_id="s", cdc_lob_exclusions=["docs:body", "docs:note"],
                                           exclusion_semantics=1, legacy_column_exclusions=["docs:note"]),
                           SessionConnectionState(), EvaluationState(), SchemaConversionState(), state)
    state.overlay_ledger = handle = object()
    got = dm._overlay_inputs(state, SourceInventory(tables=[DOCS]), None, True)
    assert got["overlay_columns"] == {"docs": frozenset({"body"})}
    assert got["legacy_exclusions"] == {"docs": frozenset({"note"})}
    assert got["overlay_ledger"] is handle and got["overlay_ledger_in_use"] is True  # this run overlays
    assert got["on_overlay_ledger_used"] == state.mark_overlay_ledger_in_use
    assert state.overlay_ledger_in_use is False  # building inputs marks nothing; the submit does
    assert dm._overlay_inputs(state, SourceInventory(tables=[DOCS]), None, True, tables=["other"])["overlay_columns"] == {}


def test_the_app_builds_the_ledger_beside_the_session_state_without_io(tmp_path):
    import types

    from dsql_migrator.core.overlay_ledger import S3OverlayLedger
    from dsql_migrator.ui import app

    s3 = app._build_overlay_ledger(types.SimpleNamespace(session_state_bucket="b", aws_region="us-east-1",
                                                         aws_profile=None, session_state_path=None),
                                   S3OverlayLedger, SqliteOverlayLedger)
    assert isinstance(s3, S3OverlayLedger) and s3._client is None  # lazy: no S3 call at construction
    lite = app._build_overlay_ledger(types.SimpleNamespace(session_state_bucket=None, aws_region=None,
                                                           aws_profile=None, session_state_path=str(tmp_path / "s.db")),
                                     S3OverlayLedger, SqliteOverlayLedger)
    assert isinstance(lite, SqliteOverlayLedger)
    reset = inspect.getsource(app.build_page) if hasattr(app, "build_page") else inspect.getsource(app)
    body = reset[reset.index("def _reset_session"):]
    body = body[:body.index("\n    def ", 1)] if "\n    def " in body[1:] else body
    assert "OVERLAY_LEDGER" not in body and "ledger" not in body.lower()  # Start over never touches it


@pytest.mark.parametrize("path", ["thread", "process"])
def test_the_bundled_exclude_and_reload_lands_null(monkeypatch, events, path):
    # exclude_lob_and_reload -> _run_retry_for -> run_full_load_retry on a NOT NULL DEFAULT column.
    kw = dict(replace={"docs"}, rows={"docs": True}, tables=(DOCS,))
    rig = _ProcRig(monkeypatch, **kw) if path == "process" else _Rig(**kw)
    order = []
    real_import = rig.importer.import_rows
    rig.importer.import_rows = lambda rows, table, **k: order.append(("import", rig.alters())) or real_import(rows, table, **k)
    prior = [ChunkState(chunk_id="docs", status="FAILED")]
    assert rig.run([DOCS], retry_prior=prior) == {"docs": "DONE"}, rig.messages()
    assert rig.recreated == ["docs"]
    assert order == [("import", ['"body" DROP NOT NULL', '"body" DROP DEFAULT'])]  # overlaid BEFORE the write


def test_an_excluded_not_null_column_without_a_default_loads(events):
    FRESH["docs"]["note"] = {"notnull": True, "default": None}
    try:
        rig = _Rig(tables=(DOCS,), rows={"docs": False}, excluded={"docs": frozenset({"note"})})
        assert rig.run() == {"docs": "DONE"}
        assert rig.alters() == ['"note" DROP NOT NULL']  # the P1 "moot gate": no 23502 possible
    finally:
        FRESH["docs"]["note"] = {"notnull": False, "default": None}


def test_a_legacy_session_makes_no_alter_and_no_ledger_write(events):
    rig = _Rig(tables=(DOCS,), rows={"docs": True}, excluded={"docs": frozenset({"body"})}, overlay={},
               legacy={"docs": frozenset({"body"})}, ledger=_RaisingStore())
    assert rig.run() == {"docs": "DONE"} and rig.alters() == []
    assert rig.importer.calls == [("docs", OnConflictMode.SKIP_EXISTING, ["id", "note"])]  # still omitted



def test_a_stop_before_a_replace_table_is_recreated_records_nothing_for_it(monkeypatch, events):
    FRESH["t2"] = {"id": {"notnull": True, "default": None}, "c": {"notnull": True, "default": "'v'::text"}}
    try:
        t2 = TableDef(name="t2", primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                                                               ColumnDef(name="c", mysql_type="text")])
        rig = _ProcRig(monkeypatch, tables=(DOCS, t2), replace={"docs", "t2"}, rows={"docs": True, "t2": True},
                       excluded={"docs": frozenset({"body"}), "t2": frozenset({"c"})})
        stopped = {"v": False}
        real_recreate = rig._recreate

        def recreate(table):
            out = real_recreate(table)
            stopped["v"] = True  # the operator presses Stop right after the first recreate
            return out

        rig.migrator._table_recreator = recreate
        real_update = None

        from dsql_migrator.core.job_manager import JobHandle
        monkeypatch.setattr(JobHandle, "cancelled", property(lambda self: stopped["v"]))
        rig.run()
        assert rig.recreated == ["docs"] and rig.record("t2") is None and rig.alters() == []
    finally:
        FRESH.pop("t2", None)


def test_a_replace_overlay_table_is_not_read_before_its_recreate(monkeypatch, events):
    rig = _ProcRig(monkeypatch, tables=(DOCS,), replace={"docs"}, rows={"docs": True})
    assert rig.run() == {"docs": "DONE"}
    assert rig.facts_calls[0] == (["docs"], False)  # the first read is step 5's, after the recreate


def test_overlay_planned_names_no_default_restore_for_a_not_null_only_column(events):
    """LIVE 4/5/8: a column with no DEFAULT (only NOT NULL dropped) no longer logs 'the dropped
    DEFAULTs are recorded first and restored at cut over'; one that has a DEFAULT still does."""
    from types import SimpleNamespace

    engine._log_overlay_planned("app.t", {"note": SimpleNamespace(not_null=True, default_expr=None)})
    engine._log_overlay_planned("app.u", {"body": SimpleNamespace(not_null=True, default_expr="'x'::text")})
    planned = {e[2]: e[3] for e in events if e[0] == "overlay planned"}
    assert planned["app.t"] == "note: DROP NOT NULL"
    assert planned["app.u"].endswith("; the dropped DEFAULTs are recorded first and restored at cut over")


def test_the_column_excluded_line_does_not_assume_a_default(events):
    inputs = dataclasses.replace(
        _inputs(), excluded_lob_columns={"t": frozenset({"a"})}, overlay_columns={"t": frozenset({"a"})})
    _log_excluded_lob_columns(inputs)
    details = {e[2]: e[3] for e in events if e[0] == "column excluded"}
    assert "the DEFAULT is restored" not in details["t.a"]
    assert "any DEFAULT it had is restored at cut over" in details["t.a"]
