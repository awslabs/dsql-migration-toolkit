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
