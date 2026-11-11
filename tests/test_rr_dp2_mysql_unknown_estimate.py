# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DP-2: a MySQL start point's UNKNOWN row estimate, consumer by consumer.

The MySQL watermark records an unknown estimate as 0 (``watermark._approximate_row_counts``):
``information_schema.TABLES`` has no row for the table, its ``TABLE_ROWS`` is NULL, or the
estimate read failed. A PostgreSQL start point keeps ``None`` (0.1.499: collapsed to 0, the
panel read "5 / 0"). The 2026-11-06 refactoring review asked whether MySQL should keep
``None`` too, once every consumer was audited. This file is that audit, run on a MySQL
watermark that holds ``None`` beside the same watermark holding 0 (doubles only: no MySQL,
Aurora DSQL or AWS).

A None changes three decisions, so the MySQL fold stays (the review's one-line comment marks
it):

* the process pool's start order (``_largest_first``): a 0 starts last, a None first -- a
  MySQL table gets no size-on-disk estimate (PostgreSQL's fallback), so it stays unknown;
* the index pass's order: a 0 goes among the empty tables by name, a None after all of them;
* the Full Load verdict (``full_load_completeness``): a 0 is complete, a None is a table
  "without an estimate to compare", so the green "Full Load complete" turns into the info note.

Alike for both: an automatic split and the Reader shards minimum (neither spares the planning
read), CDC partition sizing (both dropped), Validation (an approximate figure is never an exact
count), and every reader of the KEYS only (CDC coverage, Validation's scope gap, the table-set
lock, the activity line).

Displays that already show a None as unknown, on either engine: the Rows cell and its tooltip,
the watermark panel, the CDC per-table status and Change & reload's estimate. A None
round-trips through the persisted job record and the E2E harness's state.
"""

from __future__ import annotations

import gc
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.job_store import SqliteJobStore
from dsql_migrator.core.models import (
    ChunkState,
    MigrationJob,
    SourceType,
    TableDef,
    Watermark,
)
from dsql_migrator.core.validator import _exact_watermark_count
from dsql_migrator.core.watermark import capture_watermark, estimate_source_rows
from dsql_migrator.ui.data_migration import _table_set_lock
from dsql_migrator.ui.data_migration._cdc_ui import _cdc_row_counts_from_watermark
from dsql_migrator.ui.data_migration._exclusion_dialog import (
    ExclusionDialogContext,
    _render_drop_card,
)
from dsql_migrator.ui.data_migration._full_load_ui import (
    _render_completeness_banner,
    _render_watermark,
    _rows_breakdown_tooltip,
    _rows_target_source_cell,
)
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    build_full_load_table_rows,
    build_migration_table_status,
    cdc_covered_names,
    full_load_completeness,
)
from dsql_migrator.ui.validation import full_load_covered_tables

import tests.test_g137_mysql_reader_shards as g137
from tests.test_full_load_start_order import _Exporter, _migrator
from tests.test_g137_mysql_reader_shards import rig  # noqa: F401 - the process-path rig
from tests.test_p4a_deferred_indexes import prig  # noqa: F401 - the process-path rig
from tests.test_ui_data_migration import _RecordingUi, _WatermarkUi
from tests.test_watermark import FIXED_NOW, _FakeConnection

# UNKNOWN sorts before EMPTY by name, so an order that puts a None behind the real zeros shows.
UNKNOWN, EMPTY, BIG = "app.a_unknown", "app.b_empty", "app.c_big"
NAMES = (UNKNOWN, EMPTY, BIG)
_TS = datetime(2026, 11, 6, tzinfo=timezone.utc)


def _counts(unknown) -> dict:
    return {UNKNOWN: unknown, EMPTY: 0, BIG: 50_000}


def _mysql_wm(unknown, *, counts=None) -> Watermark:
    """A MySQL start point as ``capture_watermark`` records it -- binlog coordinates, approximate
    estimates -- with ``unknown`` as UNKNOWN's figure: 0 (what MySQL records today) or None
    (what a PostgreSQL start point keeps)."""
    return Watermark(
        binlog_file="mysql-bin.000009",
        binlog_position=4,
        gtid_mode="OFF",
        server_uuid="3e11fa47-71ca-11e1-9e33-c80aa9429562",
        snapshot_timestamp=_TS,
        table_row_counts=_counts(unknown) if counts is None else dict(counts),
        row_counts_approximate=True,
    )


def _pg_wm(counts) -> Watermark:
    return Watermark(
        wal_lsn="3/AF012B8",
        snapshot_timestamp=_TS,
        table_row_counts=dict(counts),
        row_counts_approximate=True,
    )


def _tables() -> "list[TableDef]":
    return [TableDef(name=name, primary_key=["id"]) for name in NAMES]


def _done_job(unknown) -> MigrationJob:
    """A finished Full Load of the three tables, under the MySQL start point."""
    return MigrationJob(
        job_id="j",
        status="DONE",
        watermark=_mysql_wm(unknown),
        chunks=[
            ChunkState(chunk_id=UNKNOWN, status="DONE", rows_loaded=42),
            ChunkState(chunk_id=EMPTY, status="DONE"),
            ChunkState(chunk_id=BIG, status="DONE", rows_loaded=50_000),
        ],
    )


def _manager(job) -> SimpleNamespace:
    """The JobManager slice the screens read a record through (``_current_job``)."""
    return SimpleNamespace(get_status=lambda _job_id: job)


@pytest.fixture(autouse=True)
def _free_the_runs_queues():
    """A process-path run leaves its spawn-context Queue and Event (semaphores) to the cyclic
    garbage collector; collected later, inside the resource tracker's own call in another
    module's test, they warn that it was called reentrantly. Freed after each test here."""
    yield
    gc.collect()


# ---------------------------------------------------------------------------
# 1. What the MySQL start point records (kept)
# ---------------------------------------------------------------------------


class _EstimateReadFails(_FakeConnection):
    """A source that answers everything but the estimate read (the connection dropped)."""

    def execute(self, statement, _parameters=None):  # noqa: ANN001, ANN201
        if "INFORMATION_SCHEMA.TABLES" in str(statement).upper():
            raise RuntimeError("Lost connection to MySQL server during query")
        return super().execute(statement, _parameters)


def test_the_mysql_start_point_records_an_unknown_estimate_as_0() -> None:
    # "nulled": TABLE_ROWS is NULL; "ghost": information_schema has no row for it at all;
    # "empty": a real 0.
    connection = _FakeConnection(
        master_status={"File": "mysql-bin.000009", "Position": 4, "Executed_Gtid_Set": ""},
        counts={"orders": 5, "empty": 0, "nulled": None},
    )
    tables = ["orders", "empty", "nulled", "ghost"]
    watermark = capture_watermark(connection, tables, now=lambda: FIXED_NOW)
    assert watermark.table_row_counts == {"orders": 5, "empty": 0, "nulled": 0, "ghost": 0}
    assert watermark.row_counts_approximate is True
    # The same read outside the start point keeps the distinction (the CDC counts view, the
    # Reader shards minimum's read): only the start point folds an unknown into 0.
    assert estimate_source_rows(connection, tables) == {
        "orders": 5, "empty": 0, "nulled": None, "ghost": None,
    }


def test_a_failed_estimate_read_records_every_table_as_0() -> None:
    watermark = capture_watermark(
        _EstimateReadFails(counts={"orders": 5}), ["orders", "items"], now=lambda: FIXED_NOW
    )
    # Every requested table keeps its key, so the readers of the keys (section 3) see the
    # run's tables whatever the figures are.
    assert watermark.table_row_counts == {"orders": 0, "items": 0}


# ---------------------------------------------------------------------------
# 2. The decisions a None would change (why the fold stays)
# ---------------------------------------------------------------------------


def _ddl(name: str) -> str:
    short = name.split(".", 1)[1]
    return f'CREATE INDEX ASYNC "ix_{short}" ON "app"."{short}" ("email")'


def _load_twice(prig) -> dict:  # noqa: F811 - the fixture
    """One process-path run (Reader shards 4, workers inline) per figure of UNKNOWN: the order
    the pool started the tables in, and the order the index pass after it submitted their
    indexes in (each whole-table unit hands its DDL back, as one that recreated its table)."""
    table_of = {_ddl(name): name for name in NAMES}
    out = {}
    for unknown in (0, None):
        prig.events.clear()
        prig.whole.clear()
        prig.whole.update({name: {"ddls": [_ddl(name)]} for name in NAMES})
        r = prig([g137._table(name) for name in NAMES], counts=_counts(unknown))
        assert all(r.chunks[name].status == "DONE" for name in NAMES)
        out[unknown] = SimpleNamespace(
            started=[e[1] for e in prig.events if e[0] == "unit"],
            indexed=[table_of[e[1][0]] for e in prig.events if e[0] == "index"],
        )
    return out


def test_a_none_would_start_first_in_the_pool_where_a_mysql_0_starts_last(prig) -> None:  # noqa: F811
    runs = _load_twice(prig)
    # Today: the largest first, the zeros last, in the order they came in.
    assert runs[0].started == [BIG, UNKNOWN, EMPTY]
    # A None is no estimate, and MySQL reads no size for it, so it would start first.
    assert runs[None].started == [UNKNOWN, BIG, EMPTY]


def test_a_none_would_follow_the_empty_tables_in_the_index_pass(prig) -> None:  # noqa: F811
    runs = _load_twice(prig)
    assert runs[0].indexed == [BIG, UNKNOWN, EMPTY]       # the zeros among themselves, by name
    assert runs[None].indexed == [BIG, EMPTY, UNKNOWN]    # unknown last, after every 0


def test_mysql_reads_no_size_for_a_none_where_postgresql_sizes_it() -> None:
    """The one engine-dependent reading of a None: PostgreSQL estimates a table without one
    from its size on disk, in one read; MySQL reads nothing, so the table stays unknown."""
    exporter = _Exporter({UNKNOWN: 7_000})
    assert engine._load_order_estimates(
        _migrator(exporter=exporter), _tables(), _mysql_wm(None)
    ) == {EMPTY: 0, BIG: 50_000}
    assert engine._load_order_estimates(
        _migrator(exporter=exporter), _tables(), _mysql_wm(0)
    ) == {UNKNOWN: 0, EMPTY: 0, BIG: 50_000}
    assert exporter.size_reads == []
    assert engine._load_order_estimates(
        _migrator(source=SourceType.POSTGRES, exporter=exporter), _tables(),
        _pg_wm(_counts(None)),
    ) == {UNKNOWN: 7_000, EMPTY: 0, BIG: 50_000}
    assert exporter.size_reads == [(SourceType.POSTGRES, [UNKNOWN])]
    # The index pass of a retry with nothing to load reads the start point alone, alike.
    assert engine._watermark_estimates(_mysql_wm(None)) == {EMPTY: 0, BIG: 50_000}
    assert engine._watermark_estimates(_mysql_wm(0)) == _counts(0)


def test_a_none_would_turn_full_load_complete_into_the_estimate_note() -> None:
    zero = full_load_completeness(build_full_load_table_rows(_done_job(0)))
    none = full_load_completeness(build_full_load_table_rows(_done_job(None)))
    # Today the 0 is "complete": the run reads as every row loaded.
    assert zero.all_complete and zero.unknown == 0 and zero.complete == 3
    # A None is a finished table without an estimate: the run is no longer all_complete.
    assert not none.all_complete and none.unknown == 1 and none.mismatched == []
    texts = {}
    for unknown, completeness in ((0, zero), (None, none)):
        ui = _RecordingUi()
        _render_completeness_banner(ui, completeness, approximate=True)
        texts[unknown] = "\n".join(ui.texts)
    assert "Full Load complete" in texts[0]
    assert "loaded rows match the source snapshot count" in texts[0]
    assert "Full Load complete" not in texts[None]
    assert "counts differ from the pre-load estimate" in texts[None]
    assert "1 without an estimate to compare" in texts[None]


# ---------------------------------------------------------------------------
# 3. The decisions a None leaves as they are
# ---------------------------------------------------------------------------


def test_an_automatic_split_reads_a_none_like_a_0(rig, monkeypatch) -> None:  # noqa: F811
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "0")     # automatic
    assert engine._auto_reader_ranges(None, 1000) == engine._auto_reader_ranges(0, 1000) == 1
    for unknown in (0, None):
        rig.reset()
        r = rig([g137._table()], watermark=g137._wm(counts={g137.EVENTS: unknown}))
        # One reader: no planning read, no range, no part.
        assert r.planned == [] and r.shards == [] and len(r.wholes) == 1
        assert r.wholes[0].read_ranges == ()
    # The rig can plan: a 12M-row estimate is read in its ranges by one session.
    rig.reset()
    big = rig([g137._table()], watermark=g137._wm(counts={g137.EVENTS: 12_000_000}))
    assert big.planned == [g137.EVENTS] and len(big.wholes[0].read_ranges) == 4


def test_the_reader_shards_minimum_reads_a_none_like_a_mysql_0(rig) -> None:  # noqa: F811
    seen = {}
    for unknown in (0, None):
        rig.reset()
        r = rig([g137._table()], watermark=g137._wm(counts={g137.EVENTS: unknown}))
        seen[unknown] = (r.planned, len(r.shards), r.parts)
    # Neither is a positive figure below the minimum, so neither spares the planning read:
    # the table is planned and read in its four ranges either way.
    assert seen[0] == seen[None] == ([g137.EVENTS], 4, {g137.EVENTS: 4})
    rig.reset()
    small = rig([g137._table()], watermark=g137._wm(counts={g137.EVENTS: 10}))
    assert small.planned == [] and small.shards == []     # a positive one below it does


def test_cdc_partition_sizing_drops_a_none_like_a_0() -> None:
    for unknown in (0, None):
        assert _cdc_row_counts_from_watermark(_mysql_wm(unknown), _tables()) == {BIG: 50_000}
        # No positive figure at all: None, so the deploy reads the source's at the click.
        only_unknown = _mysql_wm(unknown, counts={UNKNOWN: unknown, EMPTY: 0})
        assert _cdc_row_counts_from_watermark(only_unknown, _tables()) is None


def test_validation_takes_neither_as_an_exact_count() -> None:
    for unknown in (0, None):
        watermark = _mysql_wm(unknown)
        # An approximate start point never stands in for COUNT(*): Validation counts.
        assert _exact_watermark_count(watermark, UNKNOWN) is None
        assert _exact_watermark_count(watermark, BIG) is None


def test_the_readers_of_the_keys_see_the_same_tables(monkeypatch) -> None:
    details: list = []
    monkeypatch.setattr(
        engine, "log_activity", lambda _cat, _action, **kw: details.append(kw.get("detail"))
    )
    monkeypatch.setattr(_table_set_lock, "_cdc_started_for_migration", lambda *_a: False)
    for unknown in (0, None):
        watermark = _mysql_wm(unknown)
        job = MigrationJob(job_id="j", status="DONE", watermark=watermark)
        state = SimpleNamespace(
            job_id="j", migration_type=MigrationType.FULL_LOAD_AND_CDC, cdc_stack_phase=None
        )
        assert cdc_covered_names(watermark) == frozenset(NAMES)              # CDC capture
        assert engine._covered_by(watermark) == set(NAMES)                    # guarded additions
        assert full_load_covered_tables(state, _manager(job)) == frozenset(NAMES)  # scope gap
        # Full load + CDC's narrowing stays open on the record's per-table counts (G16).
        assert _table_set_lock._fl_cdc_narrowing_job(state, _manager(job), job=job) is job
        engine._log_captured_watermark(watermark)
    assert len(details) == 2 and all("; 3 table(s) counted" in d for d in details)


# ---------------------------------------------------------------------------
# 4. The displays (already a None as unknown, on either engine)
# ---------------------------------------------------------------------------


def _row(unknown, *, state="DONE"):
    job = MigrationJob(
        job_id="j",
        watermark=_mysql_wm(unknown),
        chunks=[ChunkState(chunk_id=UNKNOWN, status=state, rows_loaded=42)],
    )
    (row,) = build_full_load_table_rows(job)
    return row


def test_the_rows_cell_shows_an_em_dash_for_a_none() -> None:
    assert _rows_target_source_cell(_row(0)) == "42 / 0"
    assert _rows_target_source_cell(_row(None)) == "42 / —"
    assert "0 source rows (estimate)" in _rows_breakdown_tooltip(_row(0))
    assert "source rows" not in _rows_breakdown_tooltip(_row(None))
    # Progress, completeness and the "above the estimate" note read both the same.
    for state in ("IN_PROGRESS", "DONE"):
        zero, none = _row(0, state=state), _row(None, state=state)
        assert (zero.progress_pct, zero.complete, zero.expected_exceeded_pct) == (
            none.progress_pct, none.complete, none.expected_exceeded_pct
        )


def test_the_watermark_panel_shows_an_em_dash_for_a_none() -> None:
    shown = {}
    for unknown in (0, None):
        ui = _WatermarkUi()
        _render_watermark(ui, MigrationJob(job_id="j", watermark=_mysql_wm(unknown)))
        assert any("Per-table snapshot rows (3, estimated)" in e for e in ui.expansions)
        assert "mysql-bin.000009:4" in ui.labels        # rendered as the MySQL start point
        shown[unknown] = ui.labels[ui.labels.index(UNKNOWN) + 1]     # the count beside it
    assert shown == {0: "0", None: "—"}


def test_the_cdc_status_row_has_no_source_figure_for_a_none() -> None:
    def _status(unknown, **live):
        job = MigrationJob(
            job_id="j",
            watermark=_mysql_wm(unknown),
            chunks=[ChunkState(chunk_id=UNKNOWN, status="DONE", rows_loaded=42)],
        )
        (row,) = build_migration_table_status(
            [UNKNOWN], full_load_job=job, target_counts={UNKNOWN: 42}, **live
        )
        return row.source_rows, row.source_estimate, row.consistency

    assert _status(0) == (0, True, "consistent")      # "0" (an estimate) beside 42 on target
    assert _status(None) == (None, False, "unknown")  # no figure: "refresh to check"
    # A refreshed live estimate wins over the start point's figure either way.
    for unknown in (0, None):
        assert _status(unknown, source_counts={UNKNOWN: 40})[:2] == (40, True)


def test_change_and_reload_names_no_estimate_for_a_none() -> None:
    lines = {}
    for unknown in (0, None):
        job = MigrationJob(job_id="j", watermark=_mysql_wm(unknown))
        ctx = ExclusionDialogContext(
            migration_state=SimpleNamespace(job_id="j"),
            job_manager=_manager(job),
            session=None,
            inventory_fn=lambda: None,
            inputs_fn=lambda: None,
            refresh=lambda: None,
        )
        resolution = SimpleNamespace(diff=None, retained_legacy=(), selected_after=None)
        ui = _RecordingUi()
        _render_drop_card(ui, ctx, resolution, [UNKNOWN, BIG])
        lines[unknown] = {
            name: next(t for t in ui.texts if t.startswith(name)) for name in (UNKNOWN, BIG)
        }
    assert lines[0][UNKNOWN].endswith("~0 rows re-read (estimate)")
    assert "rows re-read" not in lines[None][UNKNOWN]
    assert lines[None][BIG].endswith("~50,000 rows re-read (estimate)")


# ---------------------------------------------------------------------------
# 5. The persisted record
# ---------------------------------------------------------------------------


def test_a_none_estimate_round_trips_through_the_persisted_record(tmp_path) -> None:
    job = _done_job(None)
    # The job record's JSON, as both job stores write it: null in, None out.
    payload = job.model_dump_json()
    assert json.loads(payload)["watermark"]["table_row_counts"][UNKNOWN] is None
    assert MigrationJob.model_validate_json(payload).watermark.table_row_counts == _counts(None)
    # Through the SQLite job store, as a restart reloads it.
    store = SqliteJobStore(str(tmp_path / "jobs.db"))
    try:
        store.save(job, None)
        (persisted,) = store.load_all()
    finally:
        store.close()
    assert persisted.job.watermark == job.watermark
    assert persisted.job.watermark.table_row_counts[UNKNOWN] is None
    # The E2E harness's state: model_dump(mode="json") through JSON, then model_validate.
    state = json.loads(json.dumps({"watermark": job.watermark.model_dump(mode="json")}))
    assert Watermark.model_validate(state["watermark"]).table_row_counts == _counts(None)
    # A real 0 stays 0 through the same record.
    again = MigrationJob.model_validate_json(_done_job(0).model_dump_json())
    assert again.watermark.table_row_counts == _counts(0)
