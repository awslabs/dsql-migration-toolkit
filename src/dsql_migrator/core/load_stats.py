# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Full Load write-path counters: kept by each load unit, merged by the parent.

Why this exists: the Full Load runs each table (or reader shard) in its own spawned
process, and those processes set up no logging. A counter attached to the parent's
logger -- what the perf harness used -- therefore saw no batch at all, so every "OCC: 0
batches" it reported measured nothing. This module is the replacement: each load unit
keeps a :class:`LoadStats` collector, sends cumulative :meth:`LoadStats.snapshot` dicts
to the parent over the existing progress queue and returns the last one with its result,
and the parent merges them per table and per run (:class:`LoadStatsBoard`).

Counts and timings only (Property 7): a snapshot holds integers, seconds, epoch
timestamps and SQLSTATE codes -- never a row value, a key, a statement or a credential.
An error is reduced to its code (:func:`retry_code`) the moment it is counted; its
message is never kept.

Bounded memory at any table size: latency distributions are sparse log histograms
(:data:`_STEPS_PER_OCTAVE` buckets per doubling, at most :data:`_MAX_BUCKET` + 1 of them),
not sample lists, so a billion-row table's collector stays a few hundred integers, and two
histograms merge exactly by adding their buckets. A percentile read from one is within
about 4.5% of the true value.
"""

from __future__ import annotations

import math
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

# Buckets per doubling of the latency: bucket ``i`` covers (2^((i-1)/8), 2^(i/8)] ms, so
# reading its geometric middle is at most ~4.5% from any value in it. Bucket 0 holds every
# sample up to 1 ms.
_STEPS_PER_OCTAVE = 8
# 2^24 ms is about 4.7 hours; anything slower is counted in this last bucket (the exact
# maximum is kept apart, so it is still reported).
_MAX_BUCKET = 24 * _STEPS_PER_OCTAVE

# Aurora DSQL names its two optimistic-concurrency causes in the 40001 message: OC000 a
# data (row) conflict, OC001 a schema (catalog) change. Matched as a token anywhere in the
# message; the phrases are a fallback for a message that leaves the code out.
_OC_CODE = re.compile(r"\bOC\d{3}\b")
_OC_PHRASES = (
    ("schema has been updated", "OC001"),
    ("conflicts with another transaction", "OC000"),
)

# The phases a table's time is split into: writing the rows, submitting its post-load
# CREATE INDEX ASYNC statements, and the post-load target COUNT(*) of a replace load.
PHASES = ("load", "index", "count")

_COUNTERS = (
    "units", "batches", "batches_failed", "statements", "attempts", "retries_exhausted",
    "unique_recoveries", "connects", "reconnects", "connect_failures", "connections_kept",
)
_SECONDS = ("reader_blocked_s", "writer_wait_s") + tuple(f"{p}_s" for p in PHASES)
_CODE_COUNTS = ("retries", "index_retries", "attempts_per_statement")
_HISTOGRAMS = ("batch_ms", "execute_ms")
# Present only for a unit that measured its own process (``process_clock=True``).
_PROCESS_SUMS = ("cpu_s", "wall_s")


def retry_code(exc: BaseException) -> str:
    """The code a failed attempt is counted under -- never its message (Property 7).

    ``40001/OC000`` or ``40001/OC001`` when Aurora DSQL's message names the cause, bare
    ``40001`` when it does not (counted, never guessed); any other SQLSTATE as itself
    (``XX000``, ``08006``, ...); ``conn`` for a connection that dropped before the server
    sent a SQLSTATE.
    """
    state = getattr(exc, "sqlstate", None)
    if not state:
        return "conn"
    state = str(state)
    if state != "40001":
        return state
    try:
        parts = [str(exc)]
    except Exception:  # noqa: BLE001 - counting must never replace the error being handled
        parts = []
    diag = getattr(exc, "diag", None)
    for name in ("message_primary", "message_detail", "message_hint"):
        try:
            value = getattr(diag, name, None) if diag is not None else None
        except Exception:  # noqa: BLE001 - a driver's diagnostics must never break a load
            value = None
        if value:
            parts.append(str(value))
    text = " ".join(parts)
    match = _OC_CODE.search(text)
    if match:
        return f"40001/{match.group(0)}"
    lowered = text.lower()
    for phrase, code in _OC_PHRASES:
        if phrase in lowered:
            return f"40001/{code}"
    return "40001"


# --------------------------------------------------------------------------- histograms


def _new_histogram() -> dict:
    return {"n": 0, "sum": 0.0, "max": 0.0, "buckets": {}}


def _bucket(ms: float) -> int:
    if ms <= 1.0:
        return 0
    return min(_MAX_BUCKET, math.ceil(math.log2(ms) * _STEPS_PER_OCTAVE))


def _bucket_value(index: int) -> float:
    """The geometric middle of bucket ``index`` (ms)."""
    if index <= 0:
        return 1.0
    return 2 ** ((index - 0.5) / _STEPS_PER_OCTAVE)


def _histogram_add(histogram: dict, ms: float) -> None:
    ms = max(0.0, float(ms))
    histogram["n"] += 1
    histogram["sum"] += ms
    if ms > histogram["max"]:
        histogram["max"] = ms
    index = _bucket(ms)
    buckets = histogram["buckets"]
    buckets[index] = buckets.get(index, 0) + 1


def _int_keys(counts: Mapping) -> dict:
    """``counts`` with its bucket keys as ints (a JSON round trip makes them strings)."""
    return {int(key): int(value) for key, value in (counts or {}).items()}


def _histogram_merge(into: dict, other: Optional[Mapping]) -> None:
    if not other:
        return
    into["n"] += int(other.get("n") or 0)
    into["sum"] += float(other.get("sum") or 0.0)
    into["max"] = max(float(into["max"]), float(other.get("max") or 0.0))
    buckets = into["buckets"]
    for index, count in _int_keys(other.get("buckets") or {}).items():
        buckets[index] = buckets.get(index, 0) + count


def histogram_percentile(histogram: Optional[Mapping], q: float) -> Optional[float]:
    """The ``q``-th percentile (0 < q <= 100, nearest rank) in ms; ``None`` when empty.

    Read from the bucket the rank falls in (its geometric middle), never above the exact
    maximum when one is known.
    """
    if not histogram:
        return None
    buckets = _int_keys(histogram.get("buckets") or {})
    total = sum(buckets.values())
    if total <= 0:
        return None
    rank = max(1, math.ceil(total * min(100.0, max(0.0, q)) / 100.0))
    seen = 0
    for index in sorted(buckets):
        seen += buckets[index]
        if seen >= rank:
            value = _bucket_value(index)
            peak = histogram.get("max")
            return min(value, float(peak)) if peak is not None else value
    return None  # unreachable: the ranks add up to total


def histogram_summary(histogram: Optional[Mapping]) -> dict:
    """``samples`` / ``p50`` / ``p95`` / ``max`` / ``mean`` (ms, one decimal) of a histogram."""
    n = int((histogram or {}).get("n") or 0)
    if not n:
        return {"samples": 0, "p50": None, "p95": None, "max": None, "mean": None}
    p50 = histogram_percentile(histogram, 50)
    p95 = histogram_percentile(histogram, 95)
    peak = (histogram or {}).get("max")
    return {
        "samples": n,
        "p50": round(p50, 1) if p50 is not None else None,
        "p95": round(p95, 1) if p95 is not None else None,
        "max": round(float(peak), 1) if peak is not None else None,
        "mean": round(float(histogram["sum"]) / n, 1),
    }


# --------------------------------------------------------------------------- process


def _process_cpu_seconds() -> Optional[float]:
    """User + system CPU seconds of THIS process, all its threads (``getrusage``)."""
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        return float(usage.ru_utime + usage.ru_stime)
    except Exception:  # noqa: BLE001 - no resource module (Windows): unknown
        return None


def _peak_rss_bytes() -> Optional[int]:
    """This process's resident-set high-water mark in bytes (``getrusage``).

    The kernel keeps it for the life of the process, so a pool worker that loaded an
    earlier unit reports the larger of the two. Linux reports KiB, macOS bytes.
    """
    try:
        import resource

        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:  # noqa: BLE001 - unknown
        return None
    return peak if sys.platform == "darwin" else peak * 1024


# --------------------------------------------------------------------------- collector


class LoadStats:
    """Thread-safe counters of one load unit: a table, or one reader shard of it.

    Shared by the unit's reader thread(s), its submit loop and its writer threads, and
    cumulative across the unit's attempts (a source-drop re-read keeps counting into the
    same collector), so every :meth:`snapshot` supersedes the one before it.

    ``process_clock=True`` (a unit that has its process to itself) also records the
    process's CPU seconds and wall seconds since this collector was created, and its
    peak RSS. ``units=0`` for a collector that reads and writes no rows itself (the
    parent's own index pass of a sharded table), so it adds no unit to a merge.
    """

    def __init__(self, *, process_clock: bool = False, units: int = 1) -> None:
        self._lock = threading.Lock()
        self._counts = {name: 0 for name in _COUNTERS}
        self._counts["units"] = max(0, int(units))
        self._seconds = {name: 0.0 for name in _SECONDS}
        self._codes: dict[str, dict] = {name: {} for name in _CODE_COUNTS}
        self._histograms = {name: _new_histogram() for name in _HISTOGRAMS}
        self._load_started_at: Optional[float] = None
        self._load_finished_at: Optional[float] = None
        self._cpu_start = _process_cpu_seconds() if process_clock else None
        self._wall_start = time.monotonic() if process_clock else None

    # -- data batches -------------------------------------------------------------

    def record_execute(self, seconds: float) -> None:
        """One INSERT's ``execute``: a write round trip, failed or not (incl. driver work)."""
        with self._lock:
            _histogram_add(self._histograms["execute_ms"], seconds * 1000.0)

    def record_batch(self, seconds: float, *, failed: bool) -> None:
        """One data batch resolved, ``seconds`` from its first attempt to its outcome."""
        with self._lock:
            self._counts["batches"] += 1
            if failed:
                self._counts["batches_failed"] += 1
            _histogram_add(self._histograms["batch_ms"], seconds * 1000.0)

    def record_statement(self, attempts: int) -> None:
        """One statement run through the retry budget, taking ``attempts`` tries.

        A batch is one statement, unless a permanently failing row splits it.
        """
        attempts = max(0, int(attempts))
        with self._lock:
            self._counts["statements"] += 1
            self._counts["attempts"] += attempts
            per = self._codes["attempts_per_statement"]
            per[attempts] = per.get(attempts, 0) + 1

    def record_retry(self, exc: BaseException, *, exhausted: bool) -> None:
        """A data attempt failed with a retryable error (by :func:`retry_code`).

        Each one aborted a transaction on the target; it is retried unless it used the
        last attempt of its budget (``exhausted``).
        """
        code = retry_code(exc)
        with self._lock:
            retries = self._codes["retries"]
            retries[code] = retries.get(code, 0) + 1
            if exhausted:
                self._counts["retries_exhausted"] += 1

    def record_index_retry(self, exc: BaseException) -> None:
        """A post-load ``CREATE INDEX ASYNC`` attempt failed with a retryable error."""
        code = retry_code(exc)
        with self._lock:
            retries = self._codes["index_retries"]
            retries[code] = retries.get(code, 0) + 1

    def record_unique_recovery(self) -> None:
        """A 23505 recovered by reading the keys already present and inserting the rest."""
        with self._lock:
            self._counts["unique_recoveries"] += 1

    # -- connections ----------------------------------------------------------------

    def record_connect(self, *, reconnect: bool) -> None:
        """A target connection opened; ``reconnect`` when it replaced a discarded one."""
        with self._lock:
            self._counts["connects"] += 1
            if reconnect:
                self._counts["reconnects"] += 1

    def record_connect_failure(self) -> None:
        with self._lock:
            self._counts["connect_failures"] += 1

    def record_connection_kept(self) -> None:
        """A target connection that raised while in use was kept for the next lease: the
        server rejected only the statement (an OCC 40001, a data error) and the session
        stayed usable, so no reconnect followed."""
        with self._lock:
            self._counts["connections_kept"] += 1

    # -- read-ahead queue ---------------------------------------------------------

    def add_reader_blocked(self, seconds: float) -> None:
        """A reader waited this long to put a batch into the full read-ahead queue."""
        with self._lock:
            self._seconds["reader_blocked_s"] += max(0.0, seconds)

    def add_writer_wait(self, seconds: float) -> None:
        """The write pool had a free slot and waited this long on an empty queue."""
        with self._lock:
            self._seconds["writer_wait_s"] += max(0.0, seconds)

    # -- phases ---------------------------------------------------------------------

    def add_phase(self, phase: str, seconds: float) -> None:
        """Time spent in ``phase`` (one of :data:`PHASES`; any other name is ignored)."""
        key = f"{phase}_s"
        with self._lock:
            if key in self._seconds:
                self._seconds[key] += max(0.0, seconds)

    def mark_load(self, started_at: float, finished_at: float) -> None:
        """One data pass ran from ``started_at`` to ``finished_at`` (epoch seconds)."""
        with self._lock:
            if self._load_started_at is None or started_at < self._load_started_at:
                self._load_started_at = started_at
            if self._load_finished_at is None or finished_at > self._load_finished_at:
                self._load_finished_at = finished_at

    # -- reading ----------------------------------------------------------------------

    def snapshot(self) -> dict:
        """A plain, picklable copy of every count so far (cumulative)."""
        with self._lock:
            out: dict[str, Any] = dict(self._counts)
            out.update(self._seconds)
            for name in _CODE_COUNTS:
                out[name] = dict(self._codes[name])
            for name in _HISTOGRAMS:
                histogram = self._histograms[name]
                out[name] = {
                    "n": histogram["n"], "sum": histogram["sum"], "max": histogram["max"],
                    "buckets": dict(histogram["buckets"]),
                }
            out["load_started_at"] = self._load_started_at
            out["load_finished_at"] = self._load_finished_at
        if self._wall_start is not None:
            cpu = _process_cpu_seconds()
            out["cpu_s"] = (
                max(0.0, cpu - self._cpu_start)
                if cpu is not None and self._cpu_start is not None
                else None
            )
            out["wall_s"] = max(0.0, time.monotonic() - self._wall_start)
            out["peak_rss_bytes"] = _peak_rss_bytes()
        return out


# --------------------------------------------------------------------------- merging


def merge_load_stats(snapshots: Iterable[Optional[Mapping]]) -> dict:
    """One snapshot for several units: counts, seconds and buckets add up, the load
    window spans them all, and the peak RSS is the largest. ``{}`` for none."""
    merged: dict[str, Any] = {}
    for snapshot in snapshots:
        if not snapshot:
            continue
        if not merged:
            merged = {name: 0 for name in _COUNTERS}
            merged.update({name: 0.0 for name in _SECONDS})
            merged.update({name: {} for name in _CODE_COUNTS})
            merged.update({name: _new_histogram() for name in _HISTOGRAMS})
            merged.update(load_started_at=None, load_finished_at=None)
        for name in _COUNTERS:
            merged[name] += int(snapshot.get(name) or 0)
        for name in _SECONDS:
            merged[name] += float(snapshot.get(name) or 0.0)
        for name in _CODE_COUNTS:
            target = merged[name]
            for code, count in (snapshot.get(name) or {}).items():
                key = int(code) if name == "attempts_per_statement" else code
                target[key] = target.get(key, 0) + int(count)
        for name in _HISTOGRAMS:
            _histogram_merge(merged[name], snapshot.get(name))
        started, finished = snapshot.get("load_started_at"), snapshot.get("load_finished_at")
        if started is not None and (
            merged["load_started_at"] is None or started < merged["load_started_at"]
        ):
            merged["load_started_at"] = started
        if finished is not None and (
            merged["load_finished_at"] is None or finished > merged["load_finished_at"]
        ):
            merged["load_finished_at"] = finished
        for name in _PROCESS_SUMS:
            if snapshot.get(name) is not None:
                merged[name] = float(merged.get(name) or 0.0) + float(snapshot[name])
        rss = snapshot.get("peak_rss_bytes")
        if rss is not None:
            merged["peak_rss_bytes"] = max(int(merged.get("peak_rss_bytes") or 0), int(rss))
    return merged


def diff_load_stats(current: Optional[Mapping], previous: Optional[Mapping]) -> dict:
    """What happened between two cumulative snapshots (a progress window).

    Counts, seconds and buckets are subtracted (never below zero); the load window and
    the peak RSS are ``current``'s, since neither can be taken apart.
    """
    current = merge_load_stats([current])
    if not current:
        return {}
    previous = merge_load_stats([previous])
    if not previous:
        return current
    out = dict(current)
    for name in _COUNTERS:
        out[name] = max(0, current[name] - previous[name])
    for name in _SECONDS + tuple(n for n in _PROCESS_SUMS if n in current):
        out[name] = max(0.0, float(current.get(name) or 0.0) - float(previous.get(name) or 0.0))
    for name in _CODE_COUNTS:
        before = previous[name]
        out[name] = {
            code: count - before.get(code, 0)
            for code, count in current[name].items()
            if count - before.get(code, 0) > 0
        }
    for name in _HISTOGRAMS:
        now, before = current[name], previous[name]
        buckets = {
            index: count - before["buckets"].get(index, 0)
            for index, count in now["buckets"].items()
            if count - before["buckets"].get(index, 0) > 0
        }
        out[name] = {
            "n": max(0, now["n"] - before["n"]),
            "sum": max(0.0, now["sum"] - before["sum"]),
            # Not subtractable: the percentile read clamps to it, so keep the current one.
            "max": now["max"],
            "buckets": buckets,
        }
    return out


def _iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat(
        timespec="milliseconds"
    )


def summarize_load_stats(snapshot: Optional[Mapping]) -> dict:
    """A report-ready reading of a (merged) snapshot: percentiles instead of buckets, the
    OC000 / OC001 split, the load window as UTC timestamps and the worker cores."""
    stats = merge_load_stats([snapshot])
    if not stats:
        return {}
    retries = dict(stats["retries"])
    statements = stats["statements"]
    retried = sum(
        count for attempts, count in stats["attempts_per_statement"].items() if attempts > 1
    )
    started, finished = stats["load_started_at"], stats["load_finished_at"]
    out: dict[str, Any] = {
        "units": stats["units"],
        "batches": stats["batches"],
        "batches_failed": stats["batches_failed"],
        "statements": statements,
        "attempts": stats["attempts"],
        "statements_retried": retried,
        "pct_statements_retried": round(100.0 * retried / statements, 2) if statements else 0.0,
        "attempts_per_statement": {
            str(attempts): count
            for attempts, count in sorted(stats["attempts_per_statement"].items())
        },
        "retries": dict(sorted(retries.items())),
        "retries_total": sum(retries.values()),
        "oc000": retries.get("40001/OC000", 0),
        "oc001": retries.get("40001/OC001", 0),
        # A 40001 whose message named no cause: counted apart, never guessed.
        "oc_unparsed": retries.get("40001", 0),
        "retries_exhausted": stats["retries_exhausted"],
        "index_retries": dict(sorted(stats["index_retries"].items())),
        "unique_recoveries": stats["unique_recoveries"],
        "connects": stats["connects"],
        "reconnects": stats["reconnects"],
        "connect_failures": stats["connect_failures"],
        "connections_kept": stats["connections_kept"],
        "batch_ms": histogram_summary(stats["batch_ms"]),
        "execute_ms": histogram_summary(stats["execute_ms"]),
        "reader_blocked_s": round(stats["reader_blocked_s"], 3),
        "writer_wait_s": round(stats["writer_wait_s"], 3),
        "load_s": round(stats["load_s"], 3),
        "index_s": round(stats["index_s"], 3),
        "count_s": round(stats["count_s"], 3),
        "load_started_at": _iso(started),
        "load_finished_at": _iso(finished),
        "load_window_s": (
            round(finished - started, 3) if started is not None and finished is not None else None
        ),
    }
    cpu, wall = stats.get("cpu_s"), stats.get("wall_s")
    if cpu is not None:
        out["cpu_s"] = round(cpu, 3)
    if wall is not None:
        out["wall_s"] = round(wall, 3)
        # Mean cores per worker process: CPU seconds over unit-seconds (each unit has its
        # process to itself), the client-bound vs target-bound signal.
        out["worker_cores"] = round(cpu / wall, 3) if cpu is not None and wall > 0 else None
    if stats.get("peak_rss_bytes") is not None:
        out["peak_rss_bytes"] = int(stats["peak_rss_bytes"])
    return out


# --------------------------------------------------------------------------- the parent


class LoadStatsBoard:
    """The parent's view of a run: each load unit's latest snapshot, merged on read.

    A unit is ``(table, unit)``: ``None`` for a table loaded whole, a shard index for one
    reader shard, ``"index"`` for the parent's own post-load index pass of a sharded
    table. Snapshots are cumulative, so the latest one replaces the one before (a dropped
    progress message costs nothing), and the result's final snapshot wins over a live one
    that arrives after it on the other channel. Thread-safe: the progress drain and the
    result loop both record.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._units: dict[tuple[str, object], dict] = {}
        self._final: set[tuple[str, object]] = set()

    def record(
        self, table: str, unit: object, snapshot: Optional[Mapping], *, final: bool = False
    ) -> None:
        if not snapshot:
            return
        key = (str(table), unit)
        with self._lock:
            if key in self._final and not final:
                return
            self._units[key] = dict(snapshot)
            if final:
                self._final.add(key)

    def table(self, name: str) -> dict:
        """One table's units merged (``{}`` before any of them reported)."""
        with self._lock:
            units = [s for (table, _unit), s in self._units.items() if table == name]
        return merge_load_stats(units)

    def tables(self) -> dict[str, dict]:
        """Every reported table, merged per table."""
        with self._lock:
            items = list(self._units.items())
        grouped: dict[str, list] = {}
        for (table, _unit), snapshot in items:
            grouped.setdefault(table, []).append(snapshot)
        return {table: merge_load_stats(units) for table, units in grouped.items()}

    def run(self) -> dict:
        """Every unit of the run merged."""
        with self._lock:
            units = list(self._units.values())
        return merge_load_stats(units)


__all__ = [
    "PHASES",
    "LoadStats",
    "LoadStatsBoard",
    "diff_load_stats",
    "histogram_percentile",
    "histogram_summary",
    "merge_load_stats",
    "retry_code",
    "summarize_load_stats",
]
