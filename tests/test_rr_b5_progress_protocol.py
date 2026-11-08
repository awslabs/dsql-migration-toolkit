# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The worker -> parent progress protocol has one dispatch and one row accumulator.

The drain thread's live loop and its final drain after stop each carried their own copy of
the message dispatch, and the copies had drifted: only the live loop knew the heartbeat, so
a heartbeat still queued at teardown fell through to the ``(table, loaded, skipped)``
unpack, raised ValueError and logged "Full Load final progress drain could not apply a
message" with a traceback. Both loops now call ``_apply_progress_message``.

On the sending side the time-or-rows throttled row flush was written three times (the
process table worker, the shard worker and the thread path); all three use
``_RowFlusher`` now. The wire format is unchanged.

Doubles only: no source, no target, no worker processes.
"""

from __future__ import annotations

import ast
import queue as queue_module
from pathlib import Path

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine

_ENGINE = Path(engine.__file__)


class _Handle:
    cancelled = False

    def __init__(self) -> None:
        self.updates: list = []

    def update(self, fn):  # noqa: ANN001, ANN201 - a job write
        self.updates.append(fn)


class _Event:
    def __init__(self, is_set: bool = False) -> None:
        self._set = is_set

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True


@pytest.mark.parametrize("stopped_first", [False, True])
def test_a_heartbeat_is_a_liveness_stamp_in_both_drain_loops(stopped_first, caplog) -> None:
    """``stopped_first`` puts the heartbeat in the final drain (the stop is already set),
    the loop that used to log a spurious warning for it."""
    q: "queue_module.Queue[object]" = queue_module.Queue()
    q.put(engine._HEARTBEAT)
    q.put(engine._PROGRESS_SENTINEL)
    handle = _Handle()
    with caplog.at_level("WARNING", logger=engine.__name__):
        engine._drain_progress_queue(q, handle, _Event(), _Event(stopped_first))
    assert "could not apply a message" not in caplog.text
    assert not [r for r in caplog.records if r.exc_info]
    # One job write, which changes nothing: the watchdog's liveness stamp.
    assert len(handle.updates) == 1

    class _Job:
        pass

    job = _Job()
    before = dict(vars(job))
    assert handle.updates[0](job) is None and vars(job) == before


@pytest.mark.parametrize("stopped_first", [False, True])
def test_both_drain_loops_apply_the_same_messages(stopped_first, monkeypatch, caplog) -> None:
    applied: list = []
    monkeypatch.setattr(
        engine, "_start_then_advance", lambda job, n, dl, ds: applied.append((n, dl, ds))
    )
    q: "queue_module.Queue[object]" = queue_module.Queue()
    q.put(("app.orders", 10, 2))
    q.put(engine._HEARTBEAT)
    q.put(("app.orders", 5, 0))
    q.put(engine._PROGRESS_SENTINEL)
    handle = _Handle()
    engine._drain_progress_queue(q, handle, _Event(), _Event(stopped_first))
    for fn in handle.updates:
        fn(object())
    assert applied == [("app.orders", 10, 2), ("app.orders", 5, 0)]
    assert "could not apply a message" not in caplog.text


def test_a_malformed_message_is_still_logged_and_skipped(caplog) -> None:
    q: "queue_module.Queue[object]" = queue_module.Queue()
    q.put(("too", "many", "fields", "here", "!"))
    q.put(engine._PROGRESS_SENTINEL)
    with caplog.at_level("WARNING", logger=engine.__name__):
        engine._drain_progress_queue(q, _Handle(), _Event(), _Event(True))
    assert "final progress drain could not apply a message" in caplog.text


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def test_the_row_flusher_sends_on_rows_or_on_time_and_flushes_the_rest(monkeypatch) -> None:
    clock = _Clock()
    monkeypatch.setattr(engine, "_time", clock)
    sent: list = []
    rows = engine._RowFlusher(lambda loaded, skipped: sent.append((loaded, skipped)))
    rows.add(engine.PROGRESS_FLUSH_ROWS - 2, 1)
    assert sent == []  # below both thresholds
    rows.add(0, 1)  # loaded + skipped reaches the row threshold
    assert sent == [(engine.PROGRESS_FLUSH_ROWS - 2, 2)]
    rows.add(3)
    clock.now += engine.PROGRESS_FLUSH_SECONDS - 0.5
    rows.add(1)
    assert sent[1:] == []
    clock.now += 0.5  # PROGRESS_FLUSH_SECONDS since the last flush
    rows.add(1, 4)
    assert sent[1:] == [(5, 4)]
    rows.add(7)
    rows.flush()
    assert sent[2:] == [(7, 0)]
    rows.flush()  # nothing pending: nothing sent
    assert sent[3:] == []


def _function(tree: ast.Module, name: str) -> ast.AST:
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == name
    )


def test_the_dispatch_and_the_flush_cadence_have_one_home() -> None:
    tree = ast.parse(_ENGINE.read_text(encoding="utf-8"))
    tags = {"_HEARTBEAT", "_LOAD_STATS", "_SOURCE_REREAD", "_CHUNK_STARTED",
            "_CHUNK_THROTTLED"}
    drain = _function(tree, "_drain_progress_queue")
    # The drain reads no tag itself: every kind is _apply_progress_message's.
    assert not {n.id for n in ast.walk(drain) if isinstance(n, ast.Name)} & tags
    calls = [
        n for n in ast.walk(drain)
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_apply_progress_message"
    ]
    assert len(calls) == 2  # the live loop and the final drain
    # The flush thresholds are read only by _RowFlusher.
    flusher = _function(tree, "_RowFlusher")
    inside = {id(n) for n in ast.walk(flusher)}
    readers = [
        n.lineno for n in ast.walk(tree)
        if isinstance(n, ast.Name)
        and n.id in {"PROGRESS_FLUSH_ROWS", "PROGRESS_FLUSH_SECONDS"}
        and isinstance(n.ctx, ast.Load)
        and id(n) not in inside
    ]
    assert readers == []
