# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The Full Load write-path counters (core.load_stats, R0).

The perf harness counted OCC retries on the PARENT's logger, which the spawned table
workers never write to, so every report said "OCC: 0 batches". The workers now keep these
counters themselves and the parent merges them. What is pinned here:

* a 40001 is counted under Aurora DSQL's cause (OC000 / OC001) when its message names one,
  as bare ``40001`` when it does not -- and an error is reduced to its code, never its text;
* the latency histograms are bounded and read back within their bucket error;
* snapshots merge exactly (shards -> table -> run) and subtract into windows;
* the parent's board keeps a unit's final snapshot over a late live one.
"""

from __future__ import annotations

import json
import math

import pytest

from dsql_migrator.core.load_stats import (
    LoadStats,
    LoadStatsBoard,
    diff_load_stats,
    histogram_percentile,
    histogram_summary,
    merge_load_stats,
    retry_code,
    summarize_load_stats,
)


class _Err(Exception):
    def __init__(self, message: str, sqlstate=None, diag=None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        if diag is not None:
            self.diag = diag


class _Diag:
    def __init__(self, **fields) -> None:
        self.__dict__.update(fields)


# --------------------------------------------------------------------------- codes


@pytest.mark.parametrize("message, code", [
    ("change conflicts with another transaction, please retry: (OC000)", "40001/OC000"),
    ("schema has been updated by another transaction, please retry: (OC001)", "40001/OC001"),
    # The code wins wherever it sits; the phrases are only a fallback.
    ("conflicts with another transaction OC001", "40001/OC001"),
    ("schema has been updated by another transaction, retry as needed", "40001/OC001"),
    ("mutation conflicts with another transaction, retry as needed", "40001/OC000"),
    # A 40001 that names no cause is counted as such, never guessed.
    ("could not serialize access due to concurrent update", "40001"),
])
def test_a_40001_is_counted_under_the_cause_its_message_names(message, code) -> None:
    assert retry_code(_Err(message, "40001")) == code


def test_the_cause_is_also_read_from_the_driver_diagnostics() -> None:
    exc = _Err("serialization failure", "40001",
               diag=_Diag(message_primary="x", message_detail="(OC001)", message_hint=None))
    assert retry_code(exc) == "40001/OC001"


@pytest.mark.parametrize("sqlstate, code", [
    ("XX000", "XX000"), ("08006", "08006"), ("23505", "23505"), (None, "conn"), ("", "conn"),
])
def test_other_errors_are_counted_by_their_sqlstate(sqlstate, code) -> None:
    assert retry_code(_Err("server closed the connection unexpectedly", sqlstate)) == code


def test_counting_never_raises_over_the_error_being_handled() -> None:
    # record_retry runs inside the loader's except block: a raise there would replace the
    # very error the retry wrapper is classifying.
    class _Unprintable(Exception):
        sqlstate = "40001"

        def __str__(self) -> str:
            raise RuntimeError("no text")

    assert retry_code(_Unprintable()) == "40001"
    stats = LoadStats()
    stats.add_phase("not-a-phase", 1.0)  # ignored, not a KeyError
    assert "not-a-phase_s" not in stats.snapshot()


def test_an_error_is_reduced_to_its_code_never_its_text() -> None:
    # Property 7: the driver text can carry a row's values (DETAIL: Key (email)=(...)).
    stats = LoadStats()
    secret = "alice@example.com"
    stats.record_retry(_Err(f"duplicate key value; DETAIL: Key (email)=({secret})", "40001"),
                       exhausted=False)
    stats.record_index_retry(_Err(f"boom {secret}", None))
    snapshot = stats.snapshot()
    assert secret not in json.dumps(snapshot, default=str)
    assert snapshot["retries"] == {"40001": 1}
    assert snapshot["index_retries"] == {"conn": 1}


# --------------------------------------------------------------------------- histograms


def _hist(values) -> dict:
    stats = LoadStats()
    for ms in values:
        stats.record_batch(ms / 1000.0, failed=False)
    return stats.snapshot()["batch_ms"]


def test_percentiles_come_back_within_the_bucket_error() -> None:
    values = [float(v) for v in range(1, 1001)]  # 1..1000 ms
    histogram = _hist(values)
    for q, exact in ((50, 500.0), (95, 950.0), (99, 990.0)):
        got = histogram_percentile(histogram, q)
        assert abs(got - exact) / exact < 0.05, (q, got)
    # Never above the exact maximum, and the summary keeps that maximum.
    assert histogram_percentile(histogram, 100) <= 1000.0
    summary = histogram_summary(histogram)
    assert summary["samples"] == 1000 and summary["max"] == 1000.0
    assert summary["mean"] == pytest.approx(500.5, abs=0.1)


def test_an_empty_histogram_reads_as_unknown() -> None:
    assert histogram_percentile({}, 50) is None
    assert histogram_percentile(None, 50) is None
    assert histogram_summary(None) == {
        "samples": 0, "p50": None, "p95": None, "max": None, "mean": None}


def test_a_histogram_stays_bounded_however_many_samples_it_takes() -> None:
    # One bucket per ~9% of latency, so a million batches cost no more than ten.
    histogram = _hist([1.0 + (i % 5000) * 3.7 for i in range(20_000)])
    assert len(histogram["buckets"]) < 120
    huge = _hist([10.0 ** 9])  # ~11.6 days: lands in the last bucket, max still exact
    assert histogram_summary(huge)["max"] == 10.0 ** 9


# --------------------------------------------------------------------------- collector


def test_a_collector_counts_every_kind_of_event() -> None:
    stats = LoadStats()
    stats.record_execute(0.5)
    stats.record_execute(0.7)
    stats.record_batch(1.2, failed=False)
    stats.record_batch(3.0, failed=True)
    stats.record_statement(1)
    stats.record_statement(3)
    stats.record_retry(_Err("(OC000)", "40001"), exhausted=False)
    stats.record_retry(_Err("x", "XX000"), exhausted=True)
    stats.record_unique_recovery()
    stats.record_connect(reconnect=False)
    stats.record_connect(reconnect=True)
    stats.record_connect_failure()
    stats.record_connection_kept()
    stats.add_reader_blocked(1.5)
    stats.add_writer_wait(0.25)
    stats.add_phase("load", 10.0)
    stats.add_phase("index", 0.5)
    stats.add_phase("count", 0.25)
    stats.mark_load(100.0, 110.0)
    stats.mark_load(95.0, 105.0)
    s = stats.snapshot()
    assert (s["units"], s["batches"], s["batches_failed"]) == (1, 2, 1)
    assert (s["statements"], s["attempts"]) == (2, 4)
    assert s["attempts_per_statement"] == {1: 1, 3: 1}
    assert s["retries"] == {"40001/OC000": 1, "XX000": 1} and s["retries_exhausted"] == 1
    assert s["unique_recoveries"] == 1
    assert (s["connects"], s["reconnects"], s["connect_failures"]) == (2, 1, 1)
    assert s["connections_kept"] == 1
    assert (s["reader_blocked_s"], s["writer_wait_s"]) == (1.5, 0.25)
    assert (s["load_s"], s["index_s"], s["count_s"]) == (10.0, 0.5, 0.25)
    assert (s["load_started_at"], s["load_finished_at"]) == (95.0, 110.0)
    assert s["execute_ms"]["n"] == 2 and s["batch_ms"]["n"] == 2
    # No process clock: no process figures.
    assert "cpu_s" not in s and "peak_rss_bytes" not in s


def test_a_snapshot_is_a_copy() -> None:
    stats = LoadStats()
    stats.record_batch(0.1, failed=False)
    first = stats.snapshot()
    stats.record_batch(0.1, failed=False)
    assert first["batches"] == 1 and first["batch_ms"]["n"] == 1
    assert stats.snapshot()["batches"] == 2


def test_a_process_clock_measures_the_process() -> None:
    stats = LoadStats(process_clock=True)
    sum(i * i for i in range(200_000))  # burn a little CPU
    s = stats.snapshot()
    assert s["cpu_s"] is not None and s["cpu_s"] >= 0.0
    assert s["wall_s"] >= 0.0
    assert s["peak_rss_bytes"] is None or s["peak_rss_bytes"] > 1024 * 1024


def test_a_collector_that_loads_nothing_adds_no_unit() -> None:
    assert LoadStats(units=0).snapshot()["units"] == 0
    assert merge_load_stats([LoadStats().snapshot(), LoadStats(units=0).snapshot()])["units"] == 1


# --------------------------------------------------------------------------- merge / diff


def _unit(batches: int, ms: float, *, oc001=0, cpu=None, rss=None, window=(0.0, 1.0)):
    stats = LoadStats()
    for _ in range(batches):
        stats.record_batch(ms / 1000.0, failed=False)
        stats.record_statement(1)
    for _ in range(oc001):
        stats.record_retry(_Err("(OC001)", "40001"), exhausted=False)
    stats.mark_load(*window)
    snapshot = stats.snapshot()
    if cpu is not None:
        snapshot.update(cpu_s=cpu, wall_s=10.0, peak_rss_bytes=rss)
    return snapshot


def test_shards_merge_into_one_table_exactly() -> None:
    a = _unit(10, 100.0, oc001=2, cpu=8.0, rss=500, window=(10.0, 20.0))
    b = _unit(30, 300.0, oc001=1, cpu=9.0, rss=900, window=(5.0, 15.0))
    merged = merge_load_stats([a, b, None, {}])
    assert merged["units"] == 2 and merged["batches"] == 40 and merged["statements"] == 40
    assert merged["retries"] == {"40001/OC001": 3}
    assert merged["batch_ms"]["n"] == 40 and merged["batch_ms"]["max"] == 300.0
    assert sum(merged["batch_ms"]["buckets"].values()) == 40
    assert (merged["load_started_at"], merged["load_finished_at"]) == (5.0, 20.0)
    assert merged["cpu_s"] == 17.0 and merged["wall_s"] == 20.0
    assert merged["peak_rss_bytes"] == 900
    assert merge_load_stats([]) == {}


def test_a_merge_survives_a_json_round_trip() -> None:
    a = _unit(5, 50.0)
    restored = json.loads(json.dumps(a))  # bucket and attempt keys become strings
    merged = merge_load_stats([restored, a])
    assert merged["batch_ms"]["n"] == 10
    assert merged["attempts_per_statement"] == {1: 10}
    assert histogram_percentile(merged["batch_ms"], 50) == pytest.approx(50.0, rel=0.05)


def test_a_window_is_the_difference_of_two_snapshots() -> None:
    stats = LoadStats()
    for _ in range(4):
        stats.record_batch(0.1, failed=False)
    stats.record_retry(_Err("(OC000)", "40001"), exhausted=False)
    before = stats.snapshot()
    for _ in range(6):
        stats.record_batch(1.0, failed=False)
    stats.record_retry(_Err("(OC001)", "40001"), exhausted=False)
    stats.add_reader_blocked(2.0)
    window = diff_load_stats(stats.snapshot(), before)
    assert window["batches"] == 6
    assert window["retries"] == {"40001/OC001": 1}
    assert window["reader_blocked_s"] == 2.0
    # The window's own latencies: all six took ~1 s.
    assert histogram_percentile(window["batch_ms"], 50) == pytest.approx(1000.0, rel=0.05)
    # Nothing goes below zero, and with nothing before the window is everything.
    assert diff_load_stats(before, stats.snapshot())["batches"] == 0
    assert diff_load_stats(before, None)["batches"] == 4
    assert diff_load_stats(None, before) == {}


def test_a_summary_splits_the_occ_causes_and_dates_the_load_window() -> None:
    stats = LoadStats()
    for code in ("(OC000)", "(OC001)", "(OC001)", "serialization failure"):
        stats.record_retry(_Err(code, "40001"), exhausted=False)
    stats.record_retry(_Err("x", "XX000"), exhausted=True)
    stats.record_statement(1)
    stats.record_statement(1)
    stats.record_statement(4)
    stats.mark_load(0.0, 90.5)
    snapshot = stats.snapshot()
    snapshot.update(cpu_s=45.0, wall_s=90.0, peak_rss_bytes=2 ** 30)
    s = summarize_load_stats(snapshot)
    assert (s["oc000"], s["oc001"], s["oc_unparsed"]) == (1, 2, 1)
    assert s["retries_total"] == 5 and s["retries_exhausted"] == 1
    assert s["statements_retried"] == 1 and s["pct_statements_retried"] == pytest.approx(33.33)
    assert s["attempts_per_statement"] == {"1": 2, "4": 1}
    assert s["load_started_at"] == "1970-01-01T00:00:00.000+00:00"
    assert s["load_finished_at"] == "1970-01-01T00:01:30.500+00:00"
    assert s["load_window_s"] == 90.5
    assert s["worker_cores"] == 0.5 and s["peak_rss_bytes"] == 2 ** 30
    assert summarize_load_stats(None) == {}
    json.dumps(s)  # report-ready


def test_connections_kept_merge_diff_and_summarize() -> None:
    # P4c: a connection kept after a statement-level error is counted next to reconnects.
    def _kept(n: int) -> dict:
        stats = LoadStats()
        stats.record_connect(reconnect=False)
        for _ in range(n):
            stats.record_connection_kept()
        return stats.snapshot()

    older = _kept(0)
    del older["connections_kept"]  # a snapshot from before the counter existed
    merged = merge_load_stats([_kept(2), _kept(3), older])
    assert merged["connections_kept"] == 5 and merged["connects"] == 3
    assert merge_load_stats([older])["connections_kept"] == 0
    assert diff_load_stats(_kept(7), _kept(4))["connections_kept"] == 3
    assert diff_load_stats(_kept(4), older)["connections_kept"] == 4
    summary = summarize_load_stats(merged)
    assert (summary["connects"], summary["reconnects"], summary["connections_kept"]) == (3, 0, 5)
    assert summarize_load_stats(older)["connections_kept"] == 0


# --------------------------------------------------------------------------- the board


def test_the_board_merges_per_table_and_per_run() -> None:
    board = LoadStatsBoard()
    board.record("app.big", 0, _unit(10, 100.0))
    board.record("app.big", 1, _unit(20, 100.0))
    board.record("app.small", None, _unit(3, 10.0))
    board.record("app.small", None, {})  # nothing to record
    assert board.table("app.big")["batches"] == 30
    assert board.table("app.big")["units"] == 2
    assert board.table("app.none") == {}
    assert set(board.tables()) == {"app.big", "app.small"}
    assert board.run()["batches"] == 33 and board.run()["units"] == 3


def test_a_late_live_snapshot_never_replaces_a_units_final_one() -> None:
    # The live snapshot and the result travel on different channels, so the drain can
    # still be holding a stale one when the result loop records the final.
    board = LoadStatsBoard()
    board.record("t", 0, _unit(5, 1.0))
    board.record("t", 0, _unit(50, 1.0), final=True)
    board.record("t", 0, _unit(7, 1.0))  # stale, arrives after the result
    assert board.table("t")["batches"] == 50
    # Live snapshots replace each other (cumulative), so a dropped one costs nothing.
    board.record("u", None, _unit(1, 1.0))
    board.record("u", None, _unit(9, 1.0))
    assert board.table("u")["batches"] == 9


def test_counts_only_every_leaf_is_a_number() -> None:
    stats = LoadStats(process_clock=True)
    stats.record_batch(0.2, failed=False)
    stats.record_retry(_Err("(OC001)", "40001"), exhausted=False)

    def _leaves(value):
        if isinstance(value, dict):
            for key, item in value.items():
                assert isinstance(key, (str, int)), key
                yield from _leaves(item)
        else:
            yield value

    for leaf in _leaves(stats.snapshot()):
        assert leaf is None or (isinstance(leaf, (int, float)) and not isinstance(leaf, bool)
                                and math.isfinite(leaf)), leaf
