# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P4c: the loader keeps its Aurora DSQL connection after a statement-level rejection.

``_ConnectionPool.lease`` closed the connection and refilled its slot empty on ANY error
raised while it was in use, so every OCC retry opened a new connection (a TLS handshake and
an IAM token): live, perf_ecom counted as many reconnects as OC001 retries, and the
poison-row split reconnected once per failing half. Pinned here:

* the keep / discard matrix: kept after a 40001 (OC000 / OC001), 23505, 22001, 54000 or
  42804 on a session that reports itself open, unbroken and idle; discarded after a class
  08 / 53 / 57 / 58 / XX error, a class-40 code other than 40001, an error with no SQLSTATE,
  a non-Exception, and after a kept code when the session is not idle, is closed or broken,
  or cannot be read (a double without psycopg's attributes is discarded, as before);
* the error is re-raised unchanged either way, and the retry decisions do not move;
* the importer end to end: a 40001 then success loads every row on one connection, a
  class-08 drop still reconnects, the P0 F6 OC001 after an overlay ALTER is retried on the
  session that met it, a poison-row split and a 23505 recovery stay on theirs;
* the post-load index pass retries a 40001 on the same connection.
"""

from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest
from psycopg import pq
from psycopg.pq import TransactionStatus

from dsql_migrator.core.batched_import import (
    OnConflictMode,
    _ConnectionPool,
    _is_retryable_load_error,
    _session_survives,
)
from dsql_migrator.core.load_stats import LoadStats

from tests.test_batched_import import (
    _FakeStore,
    _StaleAfterAlterConnection,
    _connection_factory,
    _index_ddls,
    _rows,
    _table,
)
from tests.test_full_load_counters import (
    _OC000,
    _OC001,
    _DsqlError,
    _failing_factory,
    _importer,
)


class _Session:
    """A psycopg-like connection reporting ``closed`` / ``broken`` /
    ``info.transaction_status`` as configured (``info=False``: no ``info`` at all), and
    recording ``close``."""

    def __init__(
        self, *, closed=False, broken=False, status=TransactionStatus.IDLE, info=True
    ) -> None:
        self.closed = closed
        self.broken = broken
        if info:
            self.info = SimpleNamespace(transaction_status=status)
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _Interrupt(BaseException):
    """Not an Exception (a KeyboardInterrupt-like), even with a kept code on it."""

    sqlstate = "40001"


def _pool(**session):
    made: list[_Session] = []

    def factory() -> _Session:
        made.append(_Session(**session))
        return made[-1]

    stats = LoadStats()
    return _ConnectionPool(factory, 1, stats=stats), made, stats


def _fail_in_use(pool: _ConnectionPool, error: BaseException) -> None:
    """Lease, raise ``error`` while the connection is in use, and check it is re-raised
    unchanged."""
    with pytest.raises(type(error)) as raised:
        with pool.lease():
            raise error
    assert raised.value is error


# --------------------------------------------------------------------------- the matrix


_KEPT = [
    ("40001", _OC000),
    ("40001", _OC001),
    ("23505", "duplicate key value violates unique constraint"),
    ("22001", "value too long for type character varying(5)"),
    ("54000", "transaction age limit of 300s exceeded"),
    ("42804", "column is of type integer but expression is of type text"),
]


@pytest.mark.parametrize("sqlstate, message", _KEPT)
def test_a_statement_level_rejection_keeps_the_connection(sqlstate, message) -> None:
    pool, made, stats = _pool()
    _fail_in_use(pool, _DsqlError(message, sqlstate))
    with pool.lease() as again:
        assert again is made[0]
    assert len(made) == 1 and made[0].close_calls == 0
    s = stats.snapshot()
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (1, 0, 1)


_DISCARDED = [
    _DsqlError("server closed the connection unexpectedly", "08006"),
    _DsqlError("message too large", "08P01"),
    _DsqlError("server unavailable", "XX000"),
    _DsqlError("server unavailable", "xx000"),  # a code is read case-blind
    _DsqlError("terminating connection due to administrator command", "57P01"),
    _DsqlError("sorry, too many clients already", "53300"),
    _DsqlError("could not read block", "58030"),
    _DsqlError("statement completion unknown", "40003"),
    _DsqlError("deadlock detected", "40P01"),
    _DsqlError("no code", ""),
    RuntimeError("boom"),
    # A socket that died before the server sent a SQLSTATE: psycopg's own error, no code.
    psycopg.OperationalError("consuming input failed: server closed the connection"),
]


@pytest.mark.parametrize("error", _DISCARDED, ids=lambda e: getattr(e, "sqlstate", None) or
                         type(e).__name__)
def test_an_error_that_may_have_ended_the_session_discards_it(error) -> None:
    pool, made, stats = _pool()
    _fail_in_use(pool, error)
    assert made[0].close_calls == 1
    with pool.lease() as again:
        assert again is made[1]
    s = stats.snapshot()
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (2, 1, 0)


@pytest.mark.parametrize("session", [
    {"status": TransactionStatus.INTRANS},
    {"status": TransactionStatus.INERROR},
    {"status": TransactionStatus.UNKNOWN},
    {"status": TransactionStatus.ACTIVE},
    {"closed": True},
    {"broken": True},
    {"info": False},
], ids=lambda s: "-".join(f"{k}={getattr(v, 'name', v)}" for k, v in s.items()))
def test_a_kept_code_on_a_session_that_is_not_idle_discards_it(session) -> None:
    pool, made, stats = _pool(**session)
    _fail_in_use(pool, _DsqlError(_OC001, "40001"))
    assert made[0].close_calls == 1
    with pool.lease() as again:
        assert again is made[1]
    s = stats.snapshot()
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (2, 1, 0)


def test_a_non_exception_discards_the_connection_and_propagates() -> None:
    pool, made, stats = _pool()
    _fail_in_use(pool, _Interrupt())
    assert made[0].close_calls == 1
    assert stats.snapshot()["connections_kept"] == 0


def test_a_clean_lease_and_a_kept_one_go_back_to_the_pool_alike() -> None:
    pool, made, stats = _pool()
    with pool.lease():
        pass
    _fail_in_use(pool, _DsqlError(_OC000, "40001"))
    with pool.lease():
        pass
    pool.close_all()  # a kept connection is still the pool's to close
    assert len(made) == 1 and made[0].close_calls == 1
    s = stats.snapshot()
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (1, 0, 1)


def test_the_check_never_raises() -> None:
    class _BadCode(Exception):
        @property
        def sqlstate(self):
            raise RuntimeError("no diagnostics")

    class _BadInfo(_Session):
        @property
        def info(self):
            raise RuntimeError("no pgconn")

    class _BadClosed:
        @property
        def closed(self):
            raise RuntimeError("no pgconn")

    occ = _DsqlError(_OC001, "40001")
    assert _session_survives(_BadCode("x"), _Session()) is False
    assert _session_survives(occ, _BadInfo(info=False)) is False  # the property raises
    assert _session_survives(occ, _BadClosed()) is False
    assert _session_survives(occ, None) is False
    assert _session_survives(occ, object()) is False
    # An idle session that does not say whether it is open is not kept either.
    idle_only = SimpleNamespace(info=SimpleNamespace(transaction_status=TransactionStatus.IDLE))
    assert _session_survives(occ, idle_only) is False
    assert _session_survives(_DsqlError("x", 40001), _Session()) is False  # not a string
    assert _session_survives(occ, _Session()) is True


def test_a_real_psycopg_connection_that_is_not_open_is_discarded() -> None:
    # The check reads psycopg's own attributes: an unconnected Connection reports closed,
    # broken and an UNKNOWN transaction status.
    connection = psycopg.Connection(pq.PGconn.connect_start(b"host=/nonexistent-p4c port=1"))
    try:
        assert connection.closed and connection.info.transaction_status is not None
        assert _session_survives(_DsqlError(_OC001, "40001"), connection) is False
    finally:
        connection.close()


@pytest.mark.parametrize("sqlstate, retried", [
    ("40001", True), ("08006", True), ("08P01", True), ("XX000", True),
    ("23505", False), ("22001", False), ("54000", False), ("42804", False),
    ("57P01", False), ("53300", False), ("58030", False), ("40003", False),
])
def test_keeping_a_connection_moves_no_retry_decision(sqlstate, retried) -> None:
    # Where a retry runs changes, never whether it runs (_is_retryable_load_error as before).
    assert _is_retryable_load_error(_DsqlError("x", sqlstate)) is retried


# --------------------------------------------------------------------------- the importer


def _idle_sessions(factory, made: "list | None" = None):
    """``factory``'s connections, each also reporting an open, unbroken, idle psycopg
    session -- what a real autocommit connection reports after a rejected statement --
    and appended to ``made``."""

    def make():
        connection = factory()
        connection.broken = False
        connection.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        if made is not None:
            made.append(connection)
        return connection

    return make


def _ddl_failing(factory, errors: list):
    """``factory``'s connections, raising ``errors`` in order on CREATE INDEX first."""

    def make():
        connection = factory()
        original = connection.handle_execute

        def handle_execute(query, params, cursor):
            text = query if isinstance(query, str) else query.as_string(None)
            if "CREATE" in text and "INDEX" in text and errors:
                raise errors.pop(0)
            return original(query, params, cursor)

        connection.handle_execute = handle_execute
        return connection

    return make


def test_an_occ_retry_runs_on_the_connection_that_met_the_conflict() -> None:
    store, made = _FakeStore(), []
    errors = [_DsqlError(_OC001, "40001")]
    factory = _idle_sessions(_failing_factory(store, ["id", "name"], ["id"], errors), made)

    importer = _importer(factory)
    result = importer.import_rows(_rows(6), _table(), on_conflict=OnConflictMode.NONE)

    assert (result.rows_loaded, result.failures) == (6, 0) and len(store.rows) == 6
    s = importer.load_stats.snapshot()
    assert s["retries"] == {"40001/OC001": 1}
    assert s["attempts_per_statement"] == {2: 1, 1: 2}
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (1, 0, 1)
    assert store.connections_created == 1 and len(made) == 1


def test_a_dropped_connection_is_still_replaced() -> None:
    store, made = _FakeStore(), []
    errors = [_DsqlError("server closed the connection unexpectedly", "08006")]
    factory = _idle_sessions(_failing_factory(store, ["id", "name"], ["id"], errors), made)

    importer = _importer(factory)
    result = importer.import_rows(_rows(6), _table(), on_conflict=OnConflictMode.NONE)

    assert (result.rows_loaded, result.failures) == (6, 0) and len(store.rows) == 6
    s = importer.load_stats.snapshot()
    assert s["retries"] == {"08006": 1}
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (2, 1, 0)
    assert store.connections_created == 2 and len(made) == 2


@pytest.mark.parametrize(
    "mode", [OnConflictMode.DO_NOTHING, OnConflictMode.NONE, OnConflictMode.SKIP_EXISTING]
)
def test_the_f6_oc001_is_retried_on_the_session_that_met_it(mode) -> None:
    # test_batched_import.py's P0 F6 scenario on sessions that report themselves idle: a
    # connection used before an overlay ALTER meets one OC001 on its next statement, then
    # holds the new catalog (as an Aurora DSQL session is expected to: the live check), so
    # it is kept and serves the retry and later batches -- nothing lost or doubled, and no
    # connection replaced.
    store, made = _FakeStore(), []
    store.epoch, store.oc001, store.statements, store.alter_after = 0, 0, 0, 2
    store.reused_stale = 0

    def stale_after_alter() -> _StaleAfterAlterConnection:
        with store.lock:
            store.connections_created += 1
        return _StaleAfterAlterConnection(store, ["id", "name"], ["id"])

    importer = _importer(_idle_sessions(stale_after_alter, made), batch_size=5, parallelism=2)
    result = importer.import_rows(_rows(40), _table(), on_conflict=mode)

    assert result.failures == 0 and result.rows_loaded == 40, mode
    assert sorted(store.rows) == [(i,) for i in range(40)]
    assert 1 <= store.oc001 <= 2, mode
    assert len(made) == 2 and store.reused_stale >= 1  # kept, and used again
    s = importer.load_stats.snapshot()
    assert (s["reconnects"], s["connections_kept"]) == (0, store.oc001)


@pytest.mark.parametrize("idle_session, reconnects, kept", [(True, 0, 4), (False, 4, 0)])
def test_a_poison_row_split_stays_on_its_connection(idle_session, reconnects, kept) -> None:
    # 8 rows, id 5 always fails (42804): the batch, then 4..7, then 4..5, then 5 alone fail.
    # A double without psycopg's session attributes is discarded after each, as before.
    store = _FakeStore()
    store.poison_keys = {(5,)}
    factory = _connection_factory(store, ["id", "name"], ["id"])
    if idle_session:
        factory = _idle_sessions(factory)

    importer = _importer(factory, batch_size=8)
    result = importer.import_rows(_rows(8), _table(), on_conflict=OnConflictMode.NONE)

    assert (result.rows_loaded, result.quarantined, result.failures) == (7, 1, 0)
    s = importer.load_stats.snapshot()
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (
        1 + reconnects, reconnects, kept)
    assert s["retries"] == {}  # a permanent error is split, never retried


def test_an_uncertain_commit_reconnects_and_its_23505_recovery_does_not() -> None:
    # Aurora DSQL's XX000 at commit (the rows landed): XX is discarded; the retry's plain
    # INSERT then meets 23505, which keeps the connection for the select-filter recovery.
    store = _FakeStore()
    store.landed_failures.append("XX000")
    factory = _idle_sessions(_connection_factory(store, ["id", "name"], ["id"]))

    importer = _importer(factory, batch_size=10)
    result = importer.import_rows(_rows(4), _table(), on_conflict=OnConflictMode.NONE)

    assert result.failures == 0 and len(store.rows) == 4
    s = importer.load_stats.snapshot()
    assert s["retries"] == {"XX000": 1} and s["unique_recoveries"] == 1
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (2, 1, 1)


@pytest.mark.parametrize("path", ["create_indexes", "import_rows"])
def test_the_index_pass_retries_a_40001_on_the_same_connection(path) -> None:
    # The sharded table's own pass (create_indexes) and a table's post-load pass alike.
    store = _FakeStore()
    errors = [_DsqlError(_OC001, "40001")]
    factory = _idle_sessions(
        _ddl_failing(_connection_factory(store, ["id", "name"], ["id"]), errors)
    )

    importer = _importer(factory)
    if path == "create_indexes":
        created, failures = importer.create_indexes(_index_ddls())
    else:
        result = importer.import_rows(_rows(3), _table(), index_ddls=_index_ddls())
        assert result.rows_loaded == 3
        created, failures = result.indexes_created, result.index_failures

    assert (created, failures) == (2, [])
    assert len(store.executed_ddls) == 2
    s = importer.load_stats.snapshot()
    assert s["index_retries"] == {"40001/OC001": 1}
    assert (s["connects"], s["reconnects"], s["connections_kept"]) == (1, 0, 1)
    assert store.connections_created == 1
