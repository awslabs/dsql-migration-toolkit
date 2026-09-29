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
        assert manager.wait(job_id, timeout=10.0)
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


def test_a_run_without_exclusions_never_touches_the_ledger(events):
    rig = _Rig(excluded={}, overlay={}, ledger=_RaisingStore())
    bound = []
    rig.migrator.bind_run_inputs = lambda i: bound.append(i)
    assert rig.run() == {"docs": "DONE", "plain": "DONE"}
    assert bound == [] and rig.facts_calls == [] and rig.applied == []


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
        if not retry:
            assert [e for e in events if e[0] == "run failed"] and \
                len([e for e in events if e[0] == "run failed"]) == 1
            assert "BEFORE any table was loaded" in [e for e in events if e[0] == "run failed"][0][3]


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
    assert len(warnings) == 1 and "docs" in warnings[0][3] and "Still reloaded: plain" in warnings[0][3]
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
