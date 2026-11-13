# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""JS-6: every in-process retry of the CDC sink runs through ``DsqlSinkTask.retried``.

Five sites spelled ``OccRetry.withRetry(work, this::beforeRetry, config.maxRetries(),
config.retryBackoffMs())`` by hand: the chunk transaction, the one-row transaction of the
record-by-record replay, a held change's try, and the two catalog reads. A sixth site that
left out ``this::beforeRetry`` would retry without the rollback before the next attempt (after a
failed statement, every statement of the next attempt fails with 25P02) and without counting
the retry in the summary line. Now one ``retried`` holds the call, and the one-row
transaction is one ``commitOne``. This pins that shape; the Java tests pin what it does.
"""

from __future__ import annotations

import pathlib
import re

_SINK_TASK = (
    pathlib.Path(__file__).resolve().parents[1]
    / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
)


def _source() -> str:
    """The sink task's source without its comments."""
    text = _SINK_TASK.read_text(encoding="utf-8")
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return re.sub(r"//[^\n]*", "", text)


def _body(src: str, signature: str) -> str:
    """The text of the method that starts with ``signature``, up to its closing brace."""
    start = src.index(signature)
    return src[start : src.index("\n  }\n", start)]


def test_the_only_occ_retry_call_is_retried_with_the_tasks_budget_and_step() -> None:
    src = _source()
    assert src.count("OccRetry.withRetry(") == 1
    body = re.sub(r"\s+", " ", _body(src, "private <T> T retried(OccRetry.SqlWork<T> work)"))
    assert (
        "return OccRetry.withRetry( work, this::beforeRetry, config.maxRetries(), "
        "config.retryBackoffMs());" in body
    ), body


def test_the_one_row_transaction_is_commit_one_at_both_of_its_sites() -> None:
    src = _source()
    body = re.sub(r"\s+", " ", _body(src, "private void commitOne(ChangeEvent event)"))
    assert (
        "retried( () -> { Connection conn = connection(); executeOne(conn, event); "
        "conn.commit(); return null; });" in body
    ), body
    assert "commitOne(a.event());" in _body(src, "private void applyRecordByRecord(")
    assert "commitOne(event);" in _body(src, "private void resolveDueHolds()")
    chunk = re.sub(r"\s+", " ", _body(src, "private void applyChunk(List<Applicable> chunk)"))
    assert "retried( () -> { Connection conn = connection(); applyChunkBatched(conn, chunk);" in (
        chunk
    ), chunk
    for reader, load in (
        ("Map<String, ValueFit.TargetType> readTargetTypes(", "loadTargetTypes(key[0], key[1])"),
        ("Boolean readSourceKeyIndex(", "loadSourceKeyIndex(key[0], key[1], columns)"),
    ):
        assert f"return retried(() -> {load});" in _body(src, reader), reader
