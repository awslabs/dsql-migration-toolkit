# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the built-in batched ``INSERT`` import fallback (task 8.4).

Covers (Requirements 5.2/5.3/5.6, Properties 2, 3, 4, 5, 9.4):

- batching respects the default size and the hard cap (<= 3,000 rows/batch),
- the multi-row ``INSERT ... ON CONFLICT`` is parameterized (values bound, never
  interpolated) with safely double-quoted identifiers (Requirement 9.4),
- a ``SQLSTATE 40001`` conflict on a batch is retried via ``with_occ_retry`` and
  eventually succeeds, idempotently (no duplicate rows) -- Property 5,
- re-applying a batch does not duplicate rows (Property 3),
- a resumed run skips already-``DONE`` batches and converges to the same target
  state as an uninterrupted run (Property 4),
- DDL/DML separation: no ``CREATE INDEX`` appears inside a data-batch transaction
  and the index DDLs run after all data, each as its own statement (Property 2),
- ``CREATE INDEX ASYNC`` statements are issued post-load,
- parallel execution is bounded by the configured parallelism,
- a clear structured :class:`BatchedImportResult` is returned.

All tests use an injected fake connection/cursor with an in-memory store that
simulates ``ON CONFLICT`` semantics; no real DSQL connection is opened.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest
from pydantic import ValidationError

from dsql_migrator.core.batched_import import (
    DEFAULT_BATCH_ROWS,
    MAX_BATCH_ROWS,
    BatchedImporter,
    BatchedImportError,
    BatchedImportOptions,
    BatchedImportResult,
    OnConflictMode,
    _prefetch_enabled,
    batch_chunk_id,
    build_insert_statement,
)
from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    MigrationJob,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.core.occ import OCC_SQLSTATE


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeSerializationFailure(Exception):
    """A fake psycopg-like error exposing ``sqlstate`` (simulates 40001)."""

    def __init__(self, sqlstate: str = OCC_SQLSTATE) -> None:
        super().__init__("serialization failure")
        self.sqlstate = sqlstate


@dataclass
class _ExecutedInsert:
    """A recorded ``INSERT`` execution (rendered SQL + bound parameters)."""

    sql_text: str
    num_rows: int
    params: list[object]


@dataclass
class _FakeStore:
    """Shared in-memory target table simulating ``ON CONFLICT`` semantics.

    ``rows`` is keyed by the conflict-key tuple, so re-inserting an existing key
    is a no-op under DO NOTHING (idempotent). ``executed_inserts`` and
    ``executed_ddls`` record the ordered statement history for assertions, and
    ``connections_created`` proves connection creation is bounded.
    """

    rows: dict[tuple, dict[str, object]] = field(default_factory=dict)
    history: list[str] = field(default_factory=list)
    executed_inserts: list[_ExecutedInsert] = field(default_factory=list)
    executed_ddls: list[str] = field(default_factory=list)
    insert_failures: list[str] = field(default_factory=list)
    poison_keys: set = field(default_factory=set)
    connections_created: int = 0
    select_calls: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def seed(self, rows: list[dict[str, object]], key_columns: list[str]) -> None:
        """Pre-populate the store as if a prior run had loaded ``rows``."""
        for row in rows:
            self.rows[tuple(row[name] for name in key_columns)] = dict(row)


class _FakeCursor:
    """A minimal psycopg-like cursor that defers work to its connection."""

    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection
        self.rowcount = -1
        self.closed = False
        self._result: list[tuple] = []

    def execute(self, query: Any, params: Optional[list[object]] = None) -> None:
        self._connection.handle_execute(query, params, self)

    def fetchall(self) -> list[tuple]:
        return self._result

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    """A fake autocommit connection backed by a shared :class:`_FakeStore`."""

    def __init__(
        self, store: _FakeStore, columns: list[str], key_columns: list[str]
    ) -> None:
        self._store = store
        self._columns = columns
        self._key_columns = key_columns
        self.autocommit = True
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def handle_execute(
        self, query: Any, params: Optional[list[object]], cursor: _FakeCursor
    ) -> None:
        text = query if isinstance(query, str) else query.as_string(None)
        if "CREATE" in text and "INDEX" in text:
            with self._store.lock:
                self._store.history.append(f"DDL:{text}")
                self._store.executed_ddls.append(text)
            cursor.rowcount = -1
            return
        if text.lstrip().upper().startswith("SELECT"):
            # SKIP_EXISTING existence probe: params are the batch's key values
            # flattened in row order; regroup into key tuples and return those
            # already present in the store.
            values = list(params or [])
            width = max(1, len(self._key_columns))
            tuples = [
                tuple(values[i : i + width]) for i in range(0, len(values), width)
            ]
            with self._store.lock:
                present = [key for key in tuples if key in self._store.rows]
                self._store.select_calls += 1
            cursor._result = present
            cursor.rowcount = len(present)
            return
        self._apply_insert(text, list(params or []), cursor)

    def _apply_insert(
        self, text: str, params: list[object], cursor: _FakeCursor
    ) -> None:
        with self._store.lock:
            if self._store.insert_failures:
                state = self._store.insert_failures.pop(0)
                raise _FakeSerializationFailure(sqlstate=state)
            rows = _chunk_rows(params, self._columns)
            has_on_conflict = "ON CONFLICT" in text
            do_update = "DO UPDATE" in text
            keys = [
                tuple(row[name] for name in self._key_columns) for row in rows
            ]
            if not has_on_conflict:
                # A persistent poison row: a bare INSERT containing it always
                # fails with a permanent (non-retryable) data error (atomic).
                if any(key in self._store.poison_keys for key in keys):
                    raise _FakeSerializationFailure(sqlstate="42804")
                # Bare INSERT: any existing PK is a unique violation, and the
                # multi-row statement is atomic (nothing inserted) -- like DSQL.
                if any(key in self._store.rows for key in keys):
                    raise _FakeSerializationFailure(sqlstate="23505")
                for key, row in zip(keys, rows):
                    self._store.rows[key] = row
                affected = len(rows)
            else:
                affected = 0
                for key, row in zip(keys, rows):
                    if key in self._store.rows:
                        if do_update:
                            self._store.rows[key] = row
                            affected += 1
                    else:
                        self._store.rows[key] = row
                        affected += 1
            self._store.history.append("INSERT")
            self._store.executed_inserts.append(
                _ExecutedInsert(sql_text=text, num_rows=len(rows), params=list(params))
            )
        cursor.rowcount = affected

    def close(self) -> None:
        self.closed = True


def _chunk_rows(params: list[object], columns: list[str]) -> list[dict[str, object]]:
    """Reconstruct row dicts from a row-major flat parameter list."""
    width = len(columns)
    return [
        dict(zip(columns, params[offset : offset + width]))
        for offset in range(0, len(params), width)
    ]


class _SleepRecorder:
    """An injectable sleep function recording the delays it was asked for."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def _no_sleep(_seconds: float) -> None:
    return None


def _zero_jitter() -> float:
    return 0.0


def _connection_factory(store: _FakeStore, columns: list[str], key_columns: list[str]):
    def factory() -> _FakeConnection:
        with store.lock:
            store.connections_created += 1
        return _FakeConnection(store, columns, key_columns)

    return factory


def _table(
    name: str = "customers",
    columns: tuple[str, ...] = ("id", "name"),
    primary_key: tuple[str, ...] = ("id",),
) -> TableDef:
    column_defs = [
        ColumnDef(
            name=column,
            mysql_type="int" if column in primary_key else "varchar(50)",
        )
        for column in columns
    ]
    return TableDef(name=name, columns=column_defs, primary_key=list(primary_key))


def _rows(count: int) -> list[dict[str, object]]:
    return [{"id": index, "name": f"name-{index}"} for index in range(count)]


def _importer(
    store: _FakeStore,
    columns: list[str],
    key_columns: list[str],
    *,
    options: Optional[BatchedImportOptions] = None,
    sleep=_no_sleep,
    jitter=_zero_jitter,
    occ_max_attempts: int = 5,
) -> BatchedImporter:
    return BatchedImporter(
        options or BatchedImportOptions(),
        connection_factory=_connection_factory(store, columns, key_columns),
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=0.0,
        sleep=sleep,
        jitter=jitter,
    )


# ---------------------------------------------------------------------------
# Options: defaults and the hard cap (Property 2)
# ---------------------------------------------------------------------------


def test_default_on_conflict_is_idempotent_do_nothing() -> None:
    assert BatchedImportOptions().on_conflict is OnConflictMode.DO_NOTHING


def test_default_batch_size_is_a_few_hundred() -> None:
    assert BatchedImportOptions().batch_size == DEFAULT_BATCH_ROWS
    assert DEFAULT_BATCH_ROWS <= MAX_BATCH_ROWS


def test_batch_size_at_the_hard_cap_is_accepted() -> None:
    assert BatchedImportOptions(batch_size=MAX_BATCH_ROWS).batch_size == MAX_BATCH_ROWS


def test_batch_size_above_the_hard_cap_is_rejected() -> None:
    with pytest.raises(ValidationError):
        BatchedImportOptions(batch_size=MAX_BATCH_ROWS + 1)


def test_batch_size_below_one_is_rejected() -> None:
    with pytest.raises(ValidationError):
        BatchedImportOptions(batch_size=0)


# ---------------------------------------------------------------------------
# Statement building: parameter binding + safe identifiers (Requirement 9.4)
# ---------------------------------------------------------------------------


def test_build_insert_binds_values_and_quotes_identifiers() -> None:
    statement = build_insert_statement(
        "my table", ["id", "full name"], 2, OnConflictMode.DO_NOTHING, ["id"]
    )
    text = statement.as_string(None)
    # Values are placeholders, never interpolated.
    assert text.count("%s") == 4
    # Identifiers are safely double-quoted (table and columns).
    assert '"my table"' in text
    assert '"full name"' in text
    # Idempotent conflict handling with an explicit conflict target.
    assert 'ON CONFLICT ("id") DO NOTHING' in text


def test_build_insert_do_nothing_without_keys_omits_conflict_target() -> None:
    text = build_insert_statement(
        "t", ["id", "name"], 1, OnConflictMode.DO_NOTHING, []
    ).as_string(None)
    assert "ON CONFLICT DO NOTHING" in text
    assert "ON CONFLICT (" not in text


def test_build_insert_none_emits_plain_insert_without_on_conflict() -> None:
    # NONE -> plain INSERT, no ON CONFLICT clause (clean/replace load into an
    # empty target; avoids the DSQL multi-row ON CONFLICT silent row-drop).
    text = build_insert_statement(
        "t", ["id", "name"], 2, OnConflictMode.NONE, ["id"]
    ).as_string(None)
    assert "ON CONFLICT" not in text
    assert text.count("%s") == 4  # values still parameterized


def test_build_insert_do_update_sets_non_key_columns() -> None:
    text = build_insert_statement(
        "t", ["id", "name", "email"], 1, OnConflictMode.DO_UPDATE, ["id"]
    ).as_string(None)
    assert 'ON CONFLICT ("id") DO UPDATE SET' in text
    assert '"name" = EXCLUDED."name"' in text
    assert '"email" = EXCLUDED."email"' in text


def test_build_insert_do_update_all_keys_degrades_to_do_nothing() -> None:
    text = build_insert_statement(
        "t", ["id", "tenant"], 1, OnConflictMode.DO_UPDATE, ["id", "tenant"]
    ).as_string(None)
    assert 'ON CONFLICT ("id", "tenant") DO NOTHING' in text


def test_build_insert_rejects_zero_rows() -> None:
    with pytest.raises(ValueError):
        build_insert_statement("t", ["id"], 0, OnConflictMode.DO_NOTHING, ["id"])


def test_build_insert_qualifies_schema_table_target() -> None:
    # A schema-qualified target must compose to "schema"."table", not a single
    # "schema.table" identifier (which would not exist on the target).
    text = build_insert_statement(
        "customers_sample.categories",
        ["id"],
        1,
        OnConflictMode.DO_NOTHING,
        ["id"],
    ).as_string(None)
    assert 'INSERT INTO "customers_sample"."categories"' in text
    # Unqualified names are unaffected.
    plain = build_insert_statement(
        "categories", ["id"], 1, OnConflictMode.DO_NOTHING, ["id"]
    ).as_string(None)
    assert 'INSERT INTO "categories"' in plain


def test_batch_chunk_id_is_deterministic_and_ordered() -> None:
    assert batch_chunk_id("customers", 0) == "customers#batch-000000"
    assert batch_chunk_id("customers", 1) == "customers#batch-000001"
    assert batch_chunk_id("customers", 0) == batch_chunk_id("customers", 0)


# ---------------------------------------------------------------------------
# Batching + parameter binding through the importer
# ---------------------------------------------------------------------------


def test_import_splits_rows_into_capped_batches() -> None:
    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )

    result = importer.import_rows(_rows(5), _table())

    # 5 rows / batch_size 2 -> 3 batches, none exceeding the batch size.
    assert len(store.executed_inserts) == 3
    assert [insert.num_rows for insert in store.executed_inserts].count(2) == 2
    assert max(insert.num_rows for insert in store.executed_inserts) <= 2
    assert result.batches_completed == 3
    assert result.rows_loaded == 5
    assert len(store.rows) == 5


def test_import_key_columns_override_drives_on_conflict_target() -> None:
    # Phase 0: when the TARGET PK differs from the source PK (a composite key),
    # the engine passes key_columns= to import_rows and that must become the
    # ON CONFLICT target -- otherwise the idempotent upsert references a
    # constraint the target does not have (SQLSTATE 42P10).
    store = _FakeStore()
    columns = ["id", "customer_id", "name"]
    override = ["customer_id", "id"]
    importer = _importer(store, columns, override)
    table = _table(columns=("id", "customer_id", "name"), primary_key=("id",))

    rows = [
        {"id": i, "customer_id": i % 3, "name": f"n{i}"} for i in range(4)
    ]
    importer.import_rows(iter(rows), table, key_columns=override)

    sql = store.executed_inserts[0].sql_text
    assert 'ON CONFLICT ("customer_id", "id")' in sql


def test_import_without_key_columns_falls_back_to_source_pk() -> None:
    # No override -> today's behavior: ON CONFLICT targets the source PK. This is
    # the no-op guarantee for every table whose target PK == source PK.
    store = _FakeStore()
    importer = _importer(store, ["id", "name"], ["id"])

    importer.import_rows(_rows(3), _table())

    sql = store.executed_inserts[0].sql_text
    assert 'ON CONFLICT ("id")' in sql


def test_import_reports_each_batch_to_on_batch_loaded() -> None:
    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )
    progress: list[tuple[int, int]] = []

    result = importer.import_rows(
        _rows(5), _table(), on_batch_loaded=lambda loaded, skipped: progress.append(
            (loaded, skipped)
        )
    )

    # 5 rows / batch_size 2 -> batches of 2, 2, 1; the callback fires once per
    # successful batch with (rows_inserted, rows_skipped), summing to the total.
    # This is what lets the UI show a live cumulative count instead of only a
    # final one. Nothing pre-existed here, so every reported skip count is 0.
    assert sorted(loaded for loaded, _ in progress) == [1, 2, 2]
    assert sum(loaded for loaded, _ in progress) == result.rows_loaded == 5
    assert all(skipped == 0 for _, skipped in progress)


def test_on_batch_loaded_reports_skipped_rows_for_live_progress() -> None:
    # A re-load over rows that already exist on the target must still report
    # progress: the callback fires with the skipped (conflict) count so the UI
    # advances instead of looking stuck at zero (rows are present, just not new).
    store = _FakeStore()
    store.seed([{"id": i, "name": f"v{i}"} for i in range(5)], ["id"])  # all present
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(
            on_conflict=OnConflictMode.SKIP_EXISTING, batch_size=2
        ),
    )
    progress: list[tuple[int, int]] = []

    result = importer.import_rows(
        _rows(5),
        _table(),
        on_batch_loaded=lambda loaded, skipped: progress.append((loaded, skipped)),
    )

    # Nothing newly inserted, but every batch is reported as skipped progress so
    # the UI shows movement rather than a stuck-at-zero count.
    assert result.rows_loaded == 0
    assert sum(skipped for _, skipped in progress) == 5
    assert all(loaded == 0 for loaded, _ in progress)


def test_load_batch_logs_pk_range_and_counts_at_debug(caplog) -> None:
    import logging

    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )
    with caplog.at_level(logging.DEBUG, logger="dsql_migrator.core.batched_import"):
        importer.import_rows(_rows(5), _table())

    batch_logs = [r for r in caplog.records if "import batch" in r.getMessage()]
    # 5 rows at 2/batch -> 3 batches -> one DEBUG line per batch (never per row).
    assert len(batch_logs) == 3
    blob = " ".join(r.getMessage() for r in batch_logs)
    # PK range derived from the id column, attempted/inserted/conflict counts present.
    assert "pk_range=[0..1]" in blob and "pk_range=[4..4]" in blob
    assert "attempted=" in blob and "inserted=" in blob and "occ_retries=" in blob
    # PII guard: PK + counts only, never row VALUES.
    assert not any(f"name-{i}" in blob for i in range(5))


def test_load_batch_logs_occ_retry_count_at_debug(caplog) -> None:
    import logging

    store = _FakeStore()
    store.insert_failures = [OCC_SQLSTATE]  # one conflict before success
    importer = _importer(
        store, ["id", "name"], ["id"],
        options=BatchedImportOptions(batch_size=10), occ_max_attempts=5,
    )
    with caplog.at_level(logging.DEBUG, logger="dsql_migrator.core.batched_import"):
        importer.import_rows(_rows(3), _table())

    batch_logs = [r for r in caplog.records if "import batch" in r.getMessage()]
    assert len(batch_logs) == 1
    assert "occ_retries=1" in batch_logs[0].getMessage()


def test_no_batch_logs_when_debug_disabled(caplog) -> None:
    import logging

    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )
    with caplog.at_level(logging.INFO, logger="dsql_migrator.core.batched_import"):
        importer.import_rows(_rows(5), _table())
    # Off by default (INFO): no per-batch trace, no hot-path cost.
    assert [r for r in caplog.records if "import batch" in r.getMessage()] == []


def test_import_stops_early_when_should_cancel_becomes_true() -> None:
    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )
    calls = {"n": 0}

    def should_cancel() -> bool:
        # Allow the first batch to submit, then request a stop.
        calls["n"] += 1
        return calls["n"] > 1

    result = importer.import_rows(
        _rows(10), _table(), should_cancel=should_cancel
    )

    # Stopped before loading all 10 rows; the result flags the incomplete load so
    # the caller can mark the table retryable (the re-load is idempotent).
    assert result.cancelled is True
    assert 0 < result.rows_loaded < 10
    assert len(store.rows) == result.rows_loaded


def test_prefetch_preserves_order_and_drains_all() -> None:
    # The bounded prefetch wrapper must yield every item in the SAME order the
    # underlying reader produced them (batches map to fixed PK ranges -- order is
    # load correctness, not cosmetics).
    src = iter(range(50))
    out = list(BatchedImporter._prefetch(src, depth=4))
    assert out == list(range(50))


def test_prefetch_runs_reader_ahead_of_consumer() -> None:
    # The whole point: the reader keeps producing while the consumer is slow, so
    # by the time the consumer takes item 0 the reader has already produced more
    # than one item (overlap), bounded by `depth`. We record production timing.
    produced: list[int] = []

    def _slow_reader():
        for i in range(10):
            produced.append(i)
            yield i

    gen = BatchedImporter._prefetch(_slow_reader(), depth=4)
    first = next(gen)  # consume just one
    assert first == 0
    # Give the background reader a moment to fill the bounded queue.
    import time as _t
    _t.sleep(0.1)
    # It should have read ahead (more than the 1 we consumed), but not the whole
    # stream unboundedly -- capped near depth + the in-flight item.
    assert len(produced) > 1
    assert len(produced) <= 4 + 2  # depth buffered + one in `put` + margin
    gen.close()  # stop + join the reader thread (no leak)


def test_prefetch_reraises_reader_exception() -> None:
    # A source-read error must surface on the consumer, not vanish on the reader
    # thread, so the batch failure is reported exactly as before.
    def _boom():
        yield 1
        yield 2
        raise RuntimeError("source read failed")

    gen = BatchedImporter._prefetch(_boom(), depth=4)
    got = []
    with pytest.raises(RuntimeError, match="source read failed"):
        for item in gen:
            got.append(item)
    assert got == [1, 2]  # items before the error are still delivered in order


def test_prefetch_close_joins_reader_without_leak() -> None:
    # Closing the generator early (e.g. on cancel) must stop and join the reader
    # thread so no fullload-prefetch thread lingers.
    before = {t.name for t in threading.enumerate()}

    def _endless():
        i = 0
        while True:
            yield i
            i += 1

    gen = BatchedImporter._prefetch(_endless(), depth=2)
    assert next(gen) == 0
    gen.close()
    import time as _t
    _t.sleep(0.2)
    after = [t for t in threading.enumerate() if t.name == "fullload-prefetch"]
    assert after == [], "prefetch reader thread should be joined after close()"


def test_batch_chunk_id_namespaces_by_shard() -> None:
    # Unsharded id is unchanged (byte-for-byte resume compatibility); a sharded id
    # is namespaced so two shards' index-0 batches never collide.
    assert batch_chunk_id("orders", 0) == "orders#batch-000000"
    assert batch_chunk_id("orders", 5) == "orders#batch-000005"
    assert batch_chunk_id("orders", 0, shard_id=0) == "orders#s00-batch-000000"
    assert batch_chunk_id("orders", 0, shard_id=1) == "orders#s01-batch-000000"
    assert (
        batch_chunk_id("orders", 0, shard_id=0)
        != batch_chunk_id("orders", 0, shard_id=1)
    )


def test_prefetch_many_merges_all_shards_and_drains_everything() -> None:
    # K shard readers -> one queue -> the consumer sees every item exactly once.
    # Order across shards is not guaranteed, so compare as a set.
    def _shard(values):  # noqa: ANN001
        return iter(values)

    shards = [_shard(range(0, 10)), _shard(range(10, 20)), _shard(range(20, 30))]
    out = list(BatchedImporter._prefetch_many(shards, depth=4))
    assert sorted(out) == list(range(30))
    assert len(out) == 30  # no drops, no duplicates


def test_prefetch_many_empty_shard_list_returns_empty_not_deadlock() -> None:
    # Guard against the "no producer ever posts _END" hang: an empty work_iters
    # list must yield nothing and return promptly, not block on the queue forever.
    out = list(BatchedImporter._prefetch_many([], depth=4))
    assert out == []


def test_prefetch_many_single_shard_still_drains() -> None:
    # A one-element list is a degenerate merge but must still deliver every item.
    out = list(BatchedImporter._prefetch_many([iter(range(5))], depth=4))
    assert out == [0, 1, 2, 3, 4]


def test_prefetch_many_reraises_a_shard_exception() -> None:
    # A failing shard surfaces its error on the consumer (not swallowed on a
    # worker thread), so the batch failure is reported like the single-reader path.
    def _ok():
        yield 1
        yield 2

    def _boom():
        yield 3
        raise RuntimeError("shard read failed")

    gen = BatchedImporter._prefetch_many([_ok(), _boom()], depth=4)
    with pytest.raises(RuntimeError, match="shard read failed"):
        list(gen)


def test_prefetch_many_joins_all_reader_threads_on_close() -> None:
    # Closing early (cancel) must stop + join every shard producer -- no leak.
    def _endless(start):  # noqa: ANN001
        i = start
        while True:
            yield i
            i += 1

    gen = BatchedImporter._prefetch_many(
        [_endless(0), _endless(1000), _endless(2000)], depth=2
    )
    assert next(gen) is not None
    gen.close()
    import time as _t
    _t.sleep(0.2)
    leaked = [
        t for t in threading.enumerate()
        if t.name.startswith("fullload-prefetch-s")
    ]
    assert leaked == [], "all shard prefetch threads should be joined after close()"


def test_prefetch_enabled_default_on(monkeypatch: pytest.MonkeyPatch) -> None:
    # Prefetch is ON by default (unset env) so production behavior is unchanged.
    monkeypatch.delenv("DSQL_MIGRATOR_FULL_LOAD_PREFETCH", raising=False)
    assert _prefetch_enabled() is True


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1", True), ("true", True), ("TRUE", True), ("yes", True), ("on", True),
        ("0", False), ("false", False), ("False", False), ("no", False),
        ("off", False), ("", False), ("  off  ", False),
    ],
)
def test_prefetch_enabled_env_toggle(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: bool
) -> None:
    # The measurement seam: falsey values disable the read-ahead queue so a single
    # deployed image can be A/B'd (prefetch on vs off) in-VPC via an ECS env var.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_PREFETCH", value)
    assert _prefetch_enabled() is expected


def test_import_binds_values_and_does_not_interpolate() -> None:
    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=10)
    )

    importer.import_rows(_rows(3), _table())

    insert = store.executed_inserts[0]
    # Values arrive as bound parameters, not embedded in the SQL text.
    assert insert.params == [0, "name-0", 1, "name-1", 2, "name-2"]
    assert "name-0" not in insert.sql_text
    assert insert.sql_text.count("%s") == 6


def test_result_is_structured() -> None:
    store = _FakeStore()
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )

    result = importer.import_rows(_rows(4), _table(), index_ddls=[])

    assert isinstance(result, BatchedImportResult)
    assert result.rows_loaded == 4
    assert result.conflicts == 0
    assert result.batches_completed == 2
    assert result.batches_skipped == 0
    assert result.failures == 0
    assert result.indexes_created == 0


# ---------------------------------------------------------------------------
# Idempotency (Property 3)
# ---------------------------------------------------------------------------


def test_reapplying_batches_does_not_duplicate_rows() -> None:
    store = _FakeStore()
    table = _table()
    options = BatchedImportOptions(batch_size=2)

    first = _importer(store, ["id", "name"], ["id"], options=options).import_rows(
        _rows(4), table
    )
    second = _importer(store, ["id", "name"], ["id"], options=options).import_rows(
        _rows(4), table
    )

    assert first.rows_loaded == 4
    # Re-running loads nothing new; every row is an idempotent conflict.
    assert second.rows_loaded == 0
    assert second.conflicts == 4
    assert len(store.rows) == 4


# ---------------------------------------------------------------------------
# OCC safety (Property 5)
# ---------------------------------------------------------------------------


def test_occ_conflict_on_batch_is_retried_then_succeeds() -> None:
    store = _FakeStore()
    store.insert_failures = [OCC_SQLSTATE]  # first INSERT attempt conflicts once
    sleeper = _SleepRecorder()
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(batch_size=10),
        sleep=sleeper,
        occ_max_attempts=5,
    )

    result = importer.import_rows(_rows(3), _table())

    assert result.rows_loaded == 3
    assert result.failures == 0
    assert len(store.rows) == 3  # retry did not duplicate rows
    assert len(sleeper.delays) == 1  # exactly one backoff before the retry
    # Only the successful attempt mutated the store / was recorded.
    assert len(store.executed_inserts) == 1


def test_exhausted_occ_conflict_is_recorded_as_failure() -> None:
    store = _FakeStore()
    store.insert_failures = [OCC_SQLSTATE]
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(batch_size=10),
        occ_max_attempts=1,  # no retry budget -> the single conflict fails the batch
    )

    result = importer.import_rows(_rows(3), _table())

    assert result.failures == 1
    assert result.batches_completed == 0
    assert store.rows == {}  # no partial state left behind


# ---------------------------------------------------------------------------
# Resumability and convergence (Property 4)
# ---------------------------------------------------------------------------


def test_resume_skips_done_batches_and_converges_to_uninterrupted_state() -> None:
    table = _table()
    all_rows = _rows(6)
    options = BatchedImportOptions(batch_size=2)  # 3 deterministic batches

    # Uninterrupted run loads everything.
    uninterrupted_store = _FakeStore()
    uninterrupted_job = MigrationJob(job_id="job-full")
    _importer(uninterrupted_store, ["id", "name"], ["id"], options=options).import_rows(
        all_rows, table, job=uninterrupted_job
    )

    # Resumed run: the first two batches were already DONE in a prior run, and
    # the target already holds their rows. Only the third batch should load.
    resumed_store = _FakeStore()
    resumed_store.seed(all_rows[:4], ["id"])
    resumed_job = MigrationJob(
        job_id="job-resume",
        chunks=[
            ChunkState(chunk_id=batch_chunk_id(table.name, 0), status="DONE", rows_loaded=2),
            ChunkState(chunk_id=batch_chunk_id(table.name, 1), status="DONE", rows_loaded=2),
        ],
    )
    result = _importer(
        resumed_store, ["id", "name"], ["id"], options=options
    ).import_rows(all_rows, table, job=resumed_job)

    # Only the not-yet-done batch executed this run.
    assert result.batches_skipped == 2
    assert result.batches_completed == 1
    assert result.rows_loaded == 2
    assert len(resumed_store.executed_inserts) == 1
    # Final target state equals the uninterrupted run (Property 4 equivalence).
    assert resumed_store.rows == uninterrupted_store.rows
    # The job converges to all chunks DONE at 100%.
    assert {chunk.status for chunk in resumed_job.chunks} == {"DONE"}
    assert resumed_job.progress_pct == 100.0


def test_job_progress_and_chunk_state_updated() -> None:
    store = _FakeStore()
    job = MigrationJob(job_id="job-1")
    importer = _importer(
        store, ["id", "name"], ["id"], options=BatchedImportOptions(batch_size=2)
    )

    importer.import_rows(_rows(4), _table(), job=job)

    assert len(job.chunks) == 2
    assert {chunk.status for chunk in job.chunks} == {"DONE"}
    assert all(chunk.attempts == 1 for chunk in job.chunks)
    assert job.progress_pct == 100.0
    assert job.error_count == 0


# ---------------------------------------------------------------------------
# DDL/DML separation + post-load CREATE INDEX ASYNC (Property 2)
# ---------------------------------------------------------------------------


def _index_ddls() -> list[str]:
    return [
        'CREATE INDEX ASYNC "ix_name" ON "customers" ("name")',
        'CREATE UNIQUE INDEX ASYNC "ix_email" ON "customers" ("email")',
    ]


def test_indexes_created_after_all_data_each_its_own_statement() -> None:
    store = _FakeStore()
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(batch_size=2, parallelism=1),
    )

    result = importer.import_rows(_rows(4), _table(), index_ddls=_index_ddls())

    # Every data INSERT happens before any DDL: no CREATE INDEX inside a data
    # transaction, and DDL strictly follows the data load (Property 2).
    last_insert = max(i for i, kind in enumerate(store.history) if kind == "INSERT")
    first_ddl = min(i for i, kind in enumerate(store.history) if kind.startswith("DDL"))
    assert last_insert < first_ddl
    # No data-batch INSERT statement carries DDL.
    assert all("CREATE INDEX" not in insert.sql_text for insert in store.executed_inserts)
    # Each index DDL ran as its own separate statement.
    assert store.executed_ddls == _index_ddls()
    assert result.indexes_created == 2


def test_indexes_are_skipped_when_a_data_batch_fails() -> None:
    store = _FakeStore()
    store.insert_failures = [OCC_SQLSTATE]
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(batch_size=10, parallelism=1),
        occ_max_attempts=1,
    )

    result = importer.import_rows(_rows(3), _table(), index_ddls=_index_ddls())

    assert result.failures == 1
    assert result.indexes_created == 0
    assert store.executed_ddls == []  # no DDL issued after a failed load


# ---------------------------------------------------------------------------
# Bounded parallelism (Property 2)
# ---------------------------------------------------------------------------


def test_parallel_connection_use_is_bounded() -> None:
    store = _FakeStore()
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(batch_size=1, parallelism=2),
    )

    result = importer.import_rows(_rows(10), _table())

    # 10 single-row batches, but never more than `parallelism` connections.
    assert result.batches_completed == 10
    assert len(store.rows) == 10
    assert store.connections_created <= 2


# ---------------------------------------------------------------------------
# Configuration guards
# ---------------------------------------------------------------------------


def test_requires_connection_factory_or_target() -> None:
    with pytest.raises(BatchedImportError):
        BatchedImporter(BatchedImportOptions())


def test_target_builds_a_default_connection_factory() -> None:
    target = TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )
    # Construction must succeed without opening a connection or reaching AWS.
    importer = BatchedImporter(BatchedImportOptions(), target=target)
    assert isinstance(importer, BatchedImporter)


def test_do_update_without_key_columns_raises() -> None:
    store = _FakeStore()
    table = _table(columns=("id", "name"), primary_key=())  # no primary key
    importer = _importer(
        store,
        ["id", "name"],
        [],
        options=BatchedImportOptions(on_conflict=OnConflictMode.DO_UPDATE),
    )
    with pytest.raises(BatchedImportError, match="DO_UPDATE"):
        importer.import_rows(_rows(2), table)


def test_table_without_columns_raises() -> None:
    store = _FakeStore()
    importer = _importer(store, ["id"], ["id"])
    empty_table = TableDef(name="empty", columns=[], primary_key=["id"])
    with pytest.raises(BatchedImportError, match="no columns"):
        importer.import_rows(_rows(1), empty_table)


def test_do_update_upserts_existing_rows() -> None:
    store = _FakeStore()
    table = _table()
    options = BatchedImportOptions(
        on_conflict=OnConflictMode.DO_UPDATE, batch_size=10
    )
    # Seed an existing row with a stale value, then upsert a new value.
    store.seed([{"id": 0, "name": "stale"}], ["id"])

    _importer(store, ["id", "name"], ["id"], options=options).import_rows(
        [{"id": 0, "name": "fresh"}], table
    )

    assert store.rows[(0,)]["name"] == "fresh"


# ---------------------------------------------------------------------------
# SKIP_EXISTING: DSQL-safe idempotent load (filter-then-plain-insert)
# ---------------------------------------------------------------------------


def test_skip_existing_inserts_only_missing_and_preserves_existing() -> None:
    store = _FakeStore()
    # Pre-existing rows, as if a concurrent CDC sink already wrote newer versions.
    store.seed([{"id": 1, "name": "cdc-1"}, {"id": 2, "name": "cdc-2"}], ["id"])
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    # A Full Load batch over ids 0..3: 1 and 2 already present (skip, keep CDC's
    # value), 0 and 3 missing (plain-INSERT). No ON CONFLICT is ever issued.
    rows = [{"id": index, "name": f"name-{index}"} for index in range(4)]
    result = importer.import_rows(rows, _table())

    assert result.failures == 0
    assert result.rows_loaded == 2          # only the missing rows inserted
    assert result.conflicts == 2            # existing rows reported as skipped
    # CDC's newer values are preserved (never overwritten by the snapshot).
    assert store.rows[(1,)]["name"] == "cdc-1"
    assert store.rows[(2,)]["name"] == "cdc-2"
    # Missing rows are loaded.
    assert store.rows[(0,)]["name"] == "name-0"
    assert store.rows[(3,)]["name"] == "name-3"
    # No statement carried an ON CONFLICT clause (the silent-drop is impossible).
    assert all(
        "ON CONFLICT" not in ins.sql_text for ins in store.executed_inserts
    )


def test_skip_existing_all_present_inserts_nothing() -> None:
    store = _FakeStore()
    store.seed([{"id": i, "name": f"v{i}"} for i in range(3)], ["id"])
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    result = importer.import_rows(_rows(3), _table())
    assert result.rows_loaded == 0          # idempotent: everything already there
    assert result.conflicts == 3
    assert result.failures == 0


def test_skip_existing_supports_composite_key() -> None:
    store = _FakeStore()
    store.seed([{"a": 1, "b": 2, "v": "old"}], ["a", "b"])
    importer = _importer(
        store,
        ["a", "b", "v"],
        ["a", "b"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    table = _table(columns=("a", "b", "v"), primary_key=("a", "b"))
    rows = [{"a": 1, "b": 2, "v": "new"}, {"a": 1, "b": 3, "v": "x"}]
    result = importer.import_rows(rows, table)
    # (1,2) already present -> skipped (kept); (1,3) missing -> inserted.
    assert result.failures == 0
    assert result.rows_loaded == 1
    assert result.conflicts == 1
    assert store.rows[(1, 2)]["v"] == "old"   # existing composite row preserved
    assert store.rows[(1, 3)]["v"] == "x"


def test_skip_existing_requires_a_primary_key() -> None:
    store = _FakeStore()
    importer = _importer(
        store,
        ["id", "name"],
        [],  # no key
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    result = importer.import_rows(_rows(2), _table(primary_key=()))
    # No primary key surfaces as a failure -- never a silent drop.
    assert result.failures == 1


def test_skip_existing_retries_on_concurrent_unique_violation() -> None:
    store = _FakeStore()
    # The first INSERT loses a race to a concurrent CDC sink insert (SQLSTATE
    # 23505). SKIP_EXISTING must re-derive the existing set and retry the
    # remaining rows rather than failing the whole batch.
    store.insert_failures = ["23505"]
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    result = importer.import_rows(_rows(3), _table())
    assert result.failures == 0
    assert result.rows_loaded == 3


def test_skip_existing_no_overlap_is_single_insert_without_select() -> None:
    store = _FakeStore()  # empty target: the dominant initial-load case
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    result = importer.import_rows(_rows(5), _table())
    # Optimistic fast path: one plain INSERT, NO pre-SELECT round-trip.
    assert result.rows_loaded == 5
    assert result.failures == 0
    assert store.select_calls == 0


def test_skip_existing_overlap_falls_back_to_select_filter() -> None:
    store = _FakeStore()
    store.seed([{"id": 1, "name": "cdc-1"}], ["id"])
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.SKIP_EXISTING),
    )
    result = importer.import_rows(_rows(3), _table())  # ids 0,1,2 -- id 1 exists
    # Optimistic INSERT hits 23505 -> falls back to the SELECT-filter path.
    assert result.rows_loaded == 2  # 0 and 2 inserted
    assert result.failures == 0
    assert store.select_calls >= 1
    assert store.rows[(1,)]["name"] == "cdc-1"  # existing row preserved


# ---------------------------------------------------------------------------
# Byte-aware batch cap (DSQL 10 MiB per-write-transaction limit)
# ---------------------------------------------------------------------------


def test_iter_batches_splits_on_byte_budget_before_row_count() -> None:
    from dsql_migrator.core.batched_import import _iter_batches

    rows = [{"v": "x" * 1000} for _ in range(5)]  # ~1000 bytes each
    batches = list(_iter_batches(rows, batch_size=100, max_bytes=2500))
    # ~2 rows fit under 2500 bytes; the row-count cap (100) is never reached.
    assert [len(b) for b in batches] == [2, 2, 1]


def test_iter_batches_single_oversized_row_yields_alone() -> None:
    from dsql_migrator.core.batched_import import _iter_batches

    rows = [{"v": "x" * 5000}, {"v": "y" * 5000}]
    batches = list(_iter_batches(rows, batch_size=100, max_bytes=1000))
    # A single row cannot be split, so each oversized row is its own batch.
    assert [len(b) for b in batches] == [1, 1]


def test_iter_batches_without_byte_cap_uses_row_count_only() -> None:
    from dsql_migrator.core.batched_import import _iter_batches

    rows = [{"v": i} for i in range(5)]
    batches = list(_iter_batches(rows, batch_size=2))
    assert [len(b) for b in batches] == [2, 2, 1]


# ---------------------------------------------------------------------------
# Failure handling: transient retry / reconnect, permanent error, pool eviction
# ---------------------------------------------------------------------------


def test_transient_connection_error_is_retried_and_recovers() -> None:
    store = _FakeStore()
    store.insert_failures = ["08006"]  # first INSERT: connection drop / token expiry
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.NONE),
    )
    result = importer.import_rows(_rows(3), _table())
    # Class-08 is transient: the loader reconnects (fresh token) and retries.
    assert result.failures == 0
    assert result.rows_loaded == 3


def test_ssl_eof_drop_without_sqlstate_is_transient_and_retryable() -> None:
    # The exact production failure: a mid-query TLS teardown surfaces as a psycopg
    # OperationalError with NO sqlstate. It MUST be classified transient (retryable)
    # so the loader reconnects + replays the idempotent batch, instead of failing
    # the whole table. Regression guard for the class-08-only under-classification.
    from dsql_migrator.core.batched_import import (
        _is_retryable_load_error,
        _is_transient_connection_error,
    )

    class _ConnDrop(Exception):
        sqlstate = None

    exc = _ConnDrop(
        "sending prepared query failed: SSL error: unexpected eof while reading"
    )
    assert _is_transient_connection_error(exc) is True
    assert _is_retryable_load_error(exc) is True


def test_transient_classifier_positives_and_negatives() -> None:
    from dsql_migrator.core.batched_import import _is_transient_connection_error

    class _E(Exception):
        def __init__(self, msg: str, state):
            super().__init__(msg)
            self.sqlstate = state

    # class-08 (server-reported connection exception) stays transient
    assert _is_transient_connection_error(_E("connection failure", "08006")) is True
    # other no-sqlstate connection-lost signatures
    assert _is_transient_connection_error(
        _E("server closed the connection unexpectedly", None)) is True
    assert _is_transient_connection_error(_E("connection reset by peer", None)) is True
    # a real DATA error carries a (non-08) sqlstate -> NOT transient
    assert _is_transient_connection_error(_E("invalid input syntax", "22P02")) is False
    # a structural error with NO sqlstate but NOT a connection drop -> NOT transient
    # (must not be retried forever / must surface as a real failure)
    assert _is_transient_connection_error(_E("table has no primary key", None)) is False


def test_poison_row_is_isolated_and_the_rest_loads() -> None:
    store = _FakeStore()
    store.poison_keys = {(1,)}  # id=1 permanently fails to insert (data error)
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.NONE),
    )
    result = importer.import_rows(_rows(3), _table())  # ids 0, 1, 2
    # The poison row is isolated (quarantined); the rest of the batch still loads.
    assert result.rows_loaded == 2
    assert result.quarantined == 1
    assert result.failures == 0
    assert (0,) in store.rows and (2,) in store.rows
    assert (1,) not in store.rows
    record = result.quarantine_records[0]
    assert record.table == "customers"
    assert "id=1" in record.primary_key
    assert record.error_code == "42804"


def test_retryable_error_exhausted_fails_batch_not_quarantined() -> None:
    store = _FakeStore()
    # A transient connection error that never recovers within the retry budget.
    store.insert_failures = ["08006"] * 10
    importer = _importer(
        store,
        ["id", "name"],
        ["id"],
        options=BatchedImportOptions(on_conflict=OnConflictMode.NONE),
        occ_max_attempts=3,
    )
    result = importer.import_rows(_rows(2), _table())
    # Retryable budget exhausted -> a real batch failure (retried later), NOT a
    # poison quarantine.
    assert result.failures == 1
    assert result.quarantined == 0


def test_pool_discards_connection_after_in_use_error() -> None:
    from dsql_migrator.core.batched_import import _ConnectionPool

    created: list = []

    class _RecordingConn:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def factory():
        conn = _RecordingConn()
        created.append(conn)
        return conn

    pool = _ConnectionPool(factory, size=1)
    with pytest.raises(RuntimeError):
        with pool.lease():
            raise RuntimeError("boom")  # connection errored while in use
    assert created[0].closed is True  # broken connection discarded
    # The next lease creates a FRESH connection (slot refilled with None).
    with pool.lease() as conn2:
        assert conn2 is not created[0]
    assert len(created) == 2
