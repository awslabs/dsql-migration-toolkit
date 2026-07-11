# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the durable job stores (resumability, Property 4).

Focus on :class:`S3JobStore`: round-trip persistence, self-provisioning, the
write-coalescing that keeps S3 PUTs bounded to status transitions (so a large
Full Load does not cause a PUT storm), terminal pruning, and the best-effort
contract (a persistence error never propagates). A fake in-memory S3 client is
injected so no AWS is reached.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from dsql_migrator.core.job_store import PersistedJob, S3JobStore
from dsql_migrator.core.models import ChunkState, MigrationJob


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------


class _S3Error(Exception):
    def __init__(self, code: str, msg: str = "") -> None:
        super().__init__(f"{code}: {msg}")
        self.response = {"Error": {"Code": code}}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakePaginator:
    def __init__(self, objects: dict) -> None:
        self._objects = objects

    def paginate(self, *, Bucket, Prefix=""):  # noqa: N803
        contents = [
            {"Key": key, "LastModified": meta[1]}
            for key, meta in self._objects.items()
            if key.startswith(Prefix)
        ]
        yield {"Contents": contents}


class _FakeS3:
    """In-memory S3 double supporting object Metadata + head_object (for prune)."""

    def __init__(self, *, buckets=("b",), put_error=None) -> None:
        self._objects: dict = {}  # key -> (body, LastModified, metadata)
        self._buckets = set(buckets)
        self._put_error = put_error
        self._clock = 0
        self.created_buckets: list[str] = []
        self.put_count = 0

    def head_bucket(self, *, Bucket):  # noqa: N803
        if Bucket not in self._buckets:
            raise _S3Error("404", "Not Found")

    def create_bucket(self, *, Bucket, **_kw):  # noqa: N803
        self._buckets.add(Bucket)
        self.created_buckets.append(Bucket)

    def put_object(self, *, Bucket, Key, Body, Metadata=None, **_kw):  # noqa: N803
        if self._put_error is not None:
            raise self._put_error
        self._clock += 1
        self.put_count += 1
        lm = datetime(2026, 7, 11, 0, 0, 0, self._clock, tzinfo=timezone.utc)
        self._objects[Key] = (bytes(Body), lm, dict(Metadata or {}))

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key not in self._objects:
            raise _S3Error("NoSuchKey", "Not Found")
        body, _, _ = self._objects[Key]
        return {"Body": _Body(body)}

    def head_object(self, *, Bucket, Key):  # noqa: N803
        if Key not in self._objects:
            raise _S3Error("NoSuchKey", "Not Found")
        _, _, meta = self._objects[Key]
        return {"Metadata": meta}

    def delete_object(self, *, Bucket, Key):  # noqa: N803
        self._objects.pop(Key, None)

    def get_paginator(self, _name):
        return _FakePaginator(self._objects)


class _Clock:
    """A manually-advanced monotonic clock for deterministic throttle tests."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _job(job_id: str, status: str, chunks: list[tuple]) -> MigrationJob:
    """Build a MigrationJob. chunks = list of (chunk_id, status, rows_loaded)."""
    return MigrationJob(
        job_id=job_id,
        status=status,  # type: ignore[arg-type]
        chunks=[
            ChunkState(chunk_id=cid, status=st, rows_loaded=rl)  # type: ignore[arg-type]
            for cid, st, rl in chunks
        ],
    )


def _store(fake: _FakeS3, clock=None) -> S3JobStore:
    return S3JobStore("b", s3_client=fake, clock=clock, min_write_interval_seconds=5.0)


# ---------------------------------------------------------------------------
# round-trip / self-provisioning
# ---------------------------------------------------------------------------


def test_s3_job_store_round_trips_job_and_error() -> None:
    fake = _FakeS3(buckets=("b",))
    store = _store(fake)
    store.save(_job("j1", "RUNNING", [("orders", "DONE", 100)]), error="boom")

    loaded = store.load_all()
    assert len(loaded) == 1
    assert isinstance(loaded[0], PersistedJob)
    assert loaded[0].job.job_id == "j1"
    assert loaded[0].job.chunks[0].chunk_id == "orders"
    assert loaded[0].job.chunks[0].rows_loaded == 100
    assert loaded[0].error == "boom"


def test_s3_job_store_creates_bucket_when_absent() -> None:
    fake = _FakeS3(buckets=())
    store = S3JobStore("b", region="ap-northeast-2", s3_client=fake)
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 0)]), None)
    assert "b" in fake.created_buckets
    assert store.load_all()[0].job.job_id == "j1"


# ---------------------------------------------------------------------------
# write coalescing (the reason this store exists rather than PUT-per-save)
# ---------------------------------------------------------------------------


def test_s3_job_store_coalesces_progress_but_flushes_transitions() -> None:
    fake = _FakeS3(buckets=("b",))
    clock = _Clock()
    store = _store(fake, clock=clock)

    # First save always writes (new job).
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 0)]), None)
    assert fake.put_count == 1

    # Pure progress (rows_loaded changes, status signature unchanged) within the
    # throttle window -> NO new PUT (coalesced).
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 500)]), None)
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 900)]), None)
    assert fake.put_count == 1

    # A STATUS TRANSITION (chunk DONE) flushes immediately, even inside the window.
    store.save(_job("j1", "RUNNING", [("orders", "DONE", 1000)]), None)
    assert fake.put_count == 2

    # The persisted snapshot reflects the transition (DONE / 1000).
    assert store.load_all()[0].job.chunks[0].status == "DONE"


def test_s3_job_store_progress_flushes_after_throttle_window() -> None:
    fake = _FakeS3(buckets=("b",))
    clock = _Clock()
    store = _store(fake, clock=clock)

    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 0)]), None)
    assert fake.put_count == 1
    # Same signature, within window -> skipped.
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 10)]), None)
    assert fake.put_count == 1
    # Advance past the 5s window -> the next progress save is due and PUTs.
    clock.t = 6.0
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 20)]), None)
    assert fake.put_count == 2


def test_s3_job_store_failed_put_is_retried_next_save() -> None:
    # On a PUT failure the coalescing state is NOT advanced, so the next save with
    # the same signature retries rather than being throttled away.
    fake = _FakeS3(buckets=("b",), put_error=_S3Error("SlowDown", "throttled"))
    clock = _Clock()
    store = _store(fake, clock=clock)
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 0)]), None)  # fails, swallowed
    fake._put_error = None  # S3 recovers
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 1)]), None)  # retries now
    assert fake.put_count == 1
    assert store.load_all()[0].job.job_id == "j1"


# ---------------------------------------------------------------------------
# delete / prune / best-effort
# ---------------------------------------------------------------------------


def test_s3_job_store_delete_removes_object() -> None:
    fake = _FakeS3(buckets=("b",))
    store = _store(fake)
    store.save(_job("j1", "DONE", [("orders", "DONE", 5)]), None)
    store.delete("j1")
    assert store.load_all() == []


def test_s3_job_store_prune_keeps_newest_done_never_active() -> None:
    fake = _FakeS3(buckets=("b",))
    store = _store(fake)
    # Three DONE (oldest -> newest) + one active RUNNING that must never be pruned.
    store.save(_job("old1", "DONE", [("t", "DONE", 1)]), None)
    store.save(_job("old2", "DONE", [("t", "DONE", 1)]), None)
    store.save(_job("new1", "DONE", [("t", "DONE", 1)]), None)
    store.save(_job("active", "RUNNING", [("t", "IN_PROGRESS", 1)]), None)

    deleted = store.prune_terminal(1)  # keep only the single newest DONE
    assert set(deleted) == {"old1", "old2"}
    remaining = {p.job.job_id for p in store.load_all()}
    assert remaining == {"new1", "active"}  # active RUNNING survives


def test_s3_job_store_save_best_effort_on_error() -> None:
    fake = _FakeS3(buckets=("b",), put_error=_S3Error("AccessDenied", "no perms"))
    store = _store(fake)
    store.save(_job("j1", "RUNNING", [("orders", "IN_PROGRESS", 0)]), None)  # must NOT raise
    assert store.load_all() == []


def test_s3_job_store_load_all_ignores_corrupt_snapshot() -> None:
    fake = _FakeS3(buckets=("b",))
    store = _store(fake)
    store.save(_job("good", "DONE", [("t", "DONE", 1)]), None)
    # Inject a corrupt object under the jobs/ prefix.
    fake._objects["jobs/bad.json"] = (
        b"{not valid json",
        datetime(2026, 7, 11, tzinfo=timezone.utc),
        {"status": "DONE"},
    )
    loaded = store.load_all()
    assert {p.job.job_id for p in loaded} == {"good"}  # corrupt one skipped
