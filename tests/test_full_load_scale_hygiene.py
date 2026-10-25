# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scale hygiene for the Full Load: four small fixes that matter on hours-long, many-table runs.

* **The MySQL reader's server-side wait.** A stream that opts into per-socket timeouts (the
  Full Load reader) runs the session pins in the same SET plus ``net_write_timeout = 600``:
  the reader takes a keyset page row by row and stops while Aurora DSQL is slow, and after
  MySQL's default 60 s the source dropped the connection and the table was read again from
  the start. The shared pins, and every other source session, are unchanged.
* **One IAM token mint per process at a time.** Every ``DsqlConnector`` of one identity and
  cluster shares one token; concurrent connects wait for the one mint in flight (or get its
  error) instead of minting their own; each mint builds one session and one client; a token
  Aurora DSQL refused is not handed out again.
* **One connection for the recreate pre-pass.** The serial pre-pass runs every replace
  table's column read and DROP + CREATE on one target connection (one token) instead of two
  connections per table, still one table at a time, every statement unprepared as before; a
  connection that broke, failed a table or is 30 minutes old is replaced.
* **The launch stagger waits only before a process starts.** The pool starts one process per
  submission up to its size and queues the rest, so the 0.15 s wait now comes only before
  the submissions that start one; the others are submitted at once.

Doubles only, but for one real two-process spawn pool: the premise the stagger rests on.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from types import SimpleNamespace

import psycopg
import pytest
from psycopg import pq
from sqlalchemy import text

from dsql_migrator.core import introspector, target_connection
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.introspector import is_write_or_ddl, source_engine_kwargs
from dsql_migrator.core.models import SourceType, TargetConnectionConfig
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.ui import connect
from dsql_migrator.ui.connect import make_source_engine_factory
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._full_load_engine import (
    FULL_LOAD_SOURCE_READ_TIMEOUT_SECONDS,
    BatchedTableMigrator,
    FullLoadIncompleteError,
    _ReusedTargetConnection,
    _TableWorkerResult,
)

from tests.test_g137_mysql_reader_shards import _Handle, _inputs, _table, _wm
from tests.test_g148_source_server_settings import (
    ANSI_PARAMETER_GROUP,
    PIN,
    _MySqlServer,
    _through,
)

STREAM_PIN = f"{PIN}, net_write_timeout = 600"


# ---------------------------------------------------------------------------
# 1. The MySQL reader's net_write_timeout
# ---------------------------------------------------------------------------


def test_a_stream_lets_the_server_wait_600_s_and_keeps_the_pins_exact() -> None:
    assert introspector._MYSQL_STREAM_NET_WRITE_TIMEOUT_SECONDS == 600
    stream = source_engine_kwargs(read_timeout_seconds=FULL_LOAD_SOURCE_READ_TIMEOUT_SECONDS)
    assert stream["connect_args"]["init_command"] == STREAM_PIN
    assert stream["connect_args"]["read_timeout"] == FULL_LOAD_SOURCE_READ_TIMEOUT_SECONDS
    # One statement, the pins first and exactly as before, and the read-only guard lets it by.
    assert STREAM_PIN.count("SET") == 1 and STREAM_PIN.startswith(PIN + ", ")
    assert not is_write_or_ddl(STREAM_PIN)
    # A session without per-socket timeouts (introspection, Validation, the probes) keeps the
    # shared pins alone; PostgreSQL has no init command at all.
    assert source_engine_kwargs()["connect_args"]["init_command"] == PIN
    pg = dialect_for(SourceType.POSTGRES).engine_kwargs(read_timeout_seconds=300)
    assert "init_command" not in pg["connect_args"]


def test_the_full_load_streams_session_runs_it_and_its_other_sessions_do_not(monkeypatch) -> None:
    from tests.test_ui_data_migration import _inputs as _load_inputs

    server = _MySqlServer()
    _through(monkeypatch, server, connect)
    worker = BatchedTableMigrator(_load_inputs())  # what a worker builds for its table
    source = worker._inputs.source_config
    builders = (
        worker._exporter._engine_factory,           # the Full Load stream (and its shards)
        worker._watermark_capturer._engine_factory,  # the start point's reads
        make_source_engine_factory(None),            # connection test, Evaluation, probes
    )
    for build in builders:
        engine_ = build(source)
        try:
            with engine_.connect() as connection:
                connection.execute(text("SELECT 1"))
        finally:
            engine_.dispose()
    assert [kw["init_command"] for kw in server.connect_kwargs] == [STREAM_PIN, PIN, PIN]
    # The stream's first statement is the one SET, under the parameter group's sql_mode, as
    # the pin always was.
    assert server.log[0] == (STREAM_PIN, ANSI_PARAMETER_GROUP)


# ---------------------------------------------------------------------------
# 2. One token mint per process at a time
# ---------------------------------------------------------------------------

_TARGET = TargetConnectionConfig(cluster_endpoint="a.dsql.us-east-1.on.aws", region="us-east-1")


class _Aws:
    """``build_session`` double: counts the sessions, clients and token mints it serves.

    ``hold`` makes a mint wait until that many other callers are waiting for it (counted by
    :class:`_CountingFuture`), so a test sees them all arrive during the one mint.
    """

    def __init__(self, *, hold: int = 0, error: "BaseException | None" = None) -> None:
        self.sessions = self.clients = self.mints = 0
        self.hold = hold
        self.error = error
        self.profiles: list = []
        self._lock = threading.Lock()

    def build_session(self, aws_profile):  # noqa: ANN001, ANN201 - the build_session seam
        aws = self
        with self._lock:
            self.sessions += 1
            self.profiles.append(aws_profile)

        class _Session:
            def client(self, service_name, **kwargs):  # noqa: ANN001, ANN201
                assert service_name == "dsql" and set(kwargs) == {"region_name"}
                with aws._lock:
                    aws.clients += 1
                return aws

        return _Session()

    def generate_db_connect_admin_auth_token(  # noqa: ANN201
        self, hostname: str, region: str, expires_in: int
    ):
        deadline = time.monotonic() + 10
        while _CountingFuture.waiting < self.hold and time.monotonic() < deadline:
            time.sleep(0.001)
        with self._lock:
            self.mints += 1
            number = self.mints
        if self.error is not None:
            raise self.error
        return f"token-{number}"

    generate_db_connect_auth_token = generate_db_connect_admin_auth_token


class _CountingFuture(concurrent.futures.Future):
    """The slot's Future, counting the callers that wait on it."""

    waiting = 0
    _lock = threading.Lock()

    def result(self, timeout=None):  # noqa: ANN001, ANN201
        with _CountingFuture._lock:
            _CountingFuture.waiting += 1
        return super().result(timeout)


@pytest.fixture
def aws(monkeypatch) -> _Aws:
    aws = _Aws()
    monkeypatch.setattr(target_connection, "build_session", aws.build_session)
    monkeypatch.setattr(target_connection, "Future", _CountingFuture)
    _CountingFuture.waiting = 0
    return aws


def _connector(config: TargetConnectionConfig = _TARGET, *, passwords=None, connect=None, **kw):
    """A connector with the DEFAULT token generator and clock (the production path)."""

    def _connect(**kwargs):  # noqa: ANN202
        if passwords is not None:
            passwords.append(kwargs["password"])
        return connect(**kwargs) if connect is not None else SimpleNamespace(autocommit=True)

    return DsqlConnector(config, connect_factory=_connect, ipv4_resolver=lambda _h: None, **kw)


def _together(n: int, work) -> list:  # noqa: ANN001
    """Run ``work(i)`` on ``n`` threads released at once; their results (or errors) by i."""
    barrier = threading.Barrier(n)
    out: list = [None] * n

    def _run(i: int) -> None:
        barrier.wait()
        try:
            out[i] = work(i)
        except BaseException as exc:  # noqa: BLE001 - returned to the test
            out[i] = exc

    threads = [threading.Thread(target=_run, args=(i,)) for i in range(n)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    return out


def test_concurrent_connects_of_many_connectors_mint_one_token(aws) -> None:
    aws.hold = 7
    passwords: list = []
    _together(8, lambda _i: _connector(passwords=passwords).connect())
    # Before: 8 mints, each with a session and a client of its own.
    assert (aws.mints, aws.sessions, aws.clients) == (1, 1, 1)
    assert passwords == ["token-1"] * 8
    # A later connector, and a later connect, reuse it while it is fresh.
    _connector(passwords=passwords).connect()
    assert aws.mints == 1 and passwords[-1] == "token-1"


def test_a_loaders_writer_threads_on_one_connector_mint_one_token(aws) -> None:
    aws.hold = 7
    passwords: list = []
    shared = _connector(passwords=passwords)
    _together(8, lambda _i: shared.connect())
    assert aws.mints == 1 and passwords == ["token-1"] * 8


def test_the_callers_waiting_on_a_failed_mint_get_its_error_and_the_next_call_mints_again(
    aws,
) -> None:
    aws.hold = 3
    aws.error = RuntimeError("Unable to locate credentials")
    results = _together(4, lambda _i: _connector().connect())
    assert all(isinstance(r, RuntimeError) for r in results)
    assert aws.mints == 1  # one attempt for the four callers, not four
    aws.error, aws.hold = None, 0
    passwords: list = []
    _connector(passwords=passwords).connect()
    assert aws.mints == 2 and passwords == ["token-2"]


def test_a_token_due_for_refresh_is_minted_again_once(aws) -> None:
    first = _connector()
    first.connect()
    first._token_slot.cached.refresh_at = 0.0  # due now
    aws.hold = 7
    passwords: list = []
    _together(8, lambda _i: _connector(passwords=passwords).connect())
    assert aws.mints == 2 and passwords == ["token-2"] * 8


def test_a_token_minted_before_the_host_slept_is_minted_again(aws, monkeypatch) -> None:
    # The connector's monotonic clock stops while the host sleeps, but Aurora DSQL judges a
    # token by the wall clock. Shared, a token minted before a laptop's suspend went to every
    # connector after it -- a refused connect each -- until the monotonic deadline passed.
    wall = [1_000_000.0]
    monkeypatch.setattr(target_connection, "_wall_clock", lambda: wall[0])
    passwords: list = []
    _connector(passwords=passwords).connect()
    lifetime = (target_connection.DEFAULT_TOKEN_EXPIRY_SECONDS
                - target_connection.DEFAULT_REFRESH_MARGIN_SECONDS)
    wall[0] += lifetime - 1  # still fresh by both clocks
    _connector(passwords=passwords).connect()
    wall[0] += 1 + 3600  # an hour of sleep: only the wall clock moved
    _connector(passwords=passwords).connect()
    assert passwords == ["token-1", "token-1", "token-2"] and aws.mints == 2


def test_connectors_share_a_token_per_identity_and_cluster_only(aws) -> None:
    def token(config: TargetConnectionConfig = _TARGET, **kw) -> str:
        return _connector(config, **kw)._current_token().reveal()

    assert token() == token() == "token-1"
    assert token(aws_profile="other") == "token-2"
    assert token(aws_profile="other") == "token-2"
    assert token(_TARGET.model_copy(update={"cluster_endpoint": "b.dsql.us-east-1.on.aws"})) \
        == "token-3"
    assert token(_TARGET.model_copy(update={"username": "app_user"})) == "token-4"
    assert token(token_expiry_seconds=600) == "token-5"
    assert aws.profiles == [None, "other", None, None, None]
    # An injected generator or clock (a unit test's seam) keeps a token of its own.
    calls: list = []

    def own(hostname, region, expires_in):  # noqa: ANN001, ANN202
        calls.append(hostname)
        return f"own-{len(calls)}"

    assert token(token_generator=own) == "own-1" and token(token_generator=own) == "own-2"
    assert token(clock=lambda: 5.0) == "token-6" and aws.mints == 6


class OperationalError(Exception):
    """A psycopg connect failure: no SQLSTATE, the server's message in the text."""

    sqlstate = None


def test_a_refused_token_is_never_handed_out_again_and_a_network_failure_keeps_it(aws) -> None:
    def refuse(**_kw):  # noqa: ANN202
        raise OperationalError(
            'connection failed: connection to server at "a.dsql.us-east-1.on.aws" '
            '(10.0.0.1), port 5432 failed: FATAL:  password authentication failed for user '
            '"admin"'
        )

    def unreachable(**_kw):  # noqa: ANN202
        raise OperationalError("connection failed: connection timeout expired")

    with pytest.raises(OperationalError):
        _connector(connect=refuse).connect()
    passwords: list = []
    _connector(passwords=passwords).connect()
    assert aws.mints == 2 and passwords == ["token-2"]   # minted again, from fresh credentials
    with pytest.raises(OperationalError):
        _connector(connect=unreachable).connect()
    _connector(passwords=passwords).connect()
    assert aws.mints == 2 and passwords == ["token-2", "token-2"]  # the network said nothing
    # SQLSTATE class 28 (invalid authorization) counts as a refusal too.
    failure = OperationalError("connection failed")
    failure.sqlstate = "28000"
    assert target_connection._token_refused(failure)
    assert not target_connection._token_refused(OperationalError("could not receive data"))


def test_forgetting_a_token_never_drops_the_one_that_replaced_it(aws) -> None:
    old = _connector()
    used = old._current_token()
    old._token_slot.cached.refresh_at = 0.0
    newer = _connector()._current_token()
    assert newer.reveal() == "token-2"
    old._forget_token(used)  # a late failure of a connect that used the old token
    assert old._token_slot.cached.secret is newer
    old._forget_token(newer)
    assert old._token_slot.cached is None


# ---------------------------------------------------------------------------
# 3. The recreate pre-pass on one connection
# ---------------------------------------------------------------------------


class _DroppedConnection(Exception):
    """A dropped DSQL connection, as psycopg raises it (no SQLSTATE)."""

    sqlstate = None


class _Conn:
    """A DSQL connection double. Records every statement and close into ``log`` as
    ``(connection number, text)``; ``drop_on`` makes the statement containing it -- or the
    catalog read logged as it, e.g. ``"columns t2"`` -- fail as a dropped connection (once),
    and ``columns`` is what the catalog read returns per table."""

    def __init__(self, number: int, log: list, *, drop_on=None, columns=None) -> None:
        self.number, self.log, self.drop_on = number, log, drop_on
        self.columns = dict(columns or {})
        self.closed = False

    def cursor(self) -> "_Cursor":
        return _Cursor(self)

    def close(self) -> None:
        self.closed = True
        self.log.append((self.number, "close"))


class _Cursor:
    def __init__(self, connection: _Conn) -> None:
        self._connection, self._rows = connection, []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc) -> bool:
        return False

    def execute(self, statement, params=None) -> None:  # noqa: ANN001
        conn = self._connection
        sql = statement.as_string(None) if hasattr(statement, "as_string") else str(statement)
        if conn.closed:
            raise _DroppedConnection("the connection is closed")
        if "information_schema.columns" in sql:
            entry = f"columns {params[1]}"
            conn.log.append((conn.number, entry))
            if conn.drop_on == entry:
                conn.drop_on, conn.closed = None, True
                raise _DroppedConnection(
                    "consuming input failed: server closed the connection unexpectedly"
                )
            self._rows = [(name,) for name in conn.columns.get(params[1], [])]
            return
        conn.log.append((conn.number, " ".join(sql.split()).split(" (", 1)[0]))
        if conn.drop_on and conn.drop_on in sql:
            conn.drop_on, conn.closed = None, True
            raise _DroppedConnection(
                "consuming input failed: server closed the connection unexpectedly"
            )

    def fetchall(self) -> list:
        return list(self._rows)

    def close(self) -> None:
        pass


class _Dsql:
    """The engine's ``DsqlConnector`` double: every connection it opens, numbered from 1."""

    def __init__(self, *, drops=(), columns=None) -> None:
        self.log: list = []
        self.connections: list = []
        self.connectors = 0
        self._drops = list(drops)
        self._columns = columns

    def __call__(self, config, aws_profile=None):  # noqa: ANN001, ANN204 - the class
        self.connectors += 1
        return SimpleNamespace(connect=self._connect)

    def _connect(self) -> _Conn:
        conn = _Conn(len(self.connections) + 1, self.log,
                     drop_on=self._drops.pop(0) if self._drops else None,
                     columns=self._columns)
        self.connections.append(conn)
        return conn


def _ddl(name: str) -> str:
    return (f'CREATE TABLE "app"."{name.split(".")[1]}" ("id" BIGINT NOT NULL, '
            '"email" VARCHAR(64) NOT NULL, PRIMARY KEY ("id"))')


def _conversion(name: str) -> TableConversion:
    return TableConversion(table=name, target_ddl=_ddl(name),
                           schema_ddls=['CREATE SCHEMA IF NOT EXISTS "app"'])


def _recreated(table: str, conn: int) -> list:
    """What one table's recreate runs on connection ``conn``: the column read, then the
    schema ensure, the DROP and the CREATE (each its own autocommit statement)."""
    short = table.split(".")[1]
    return [(conn, f"columns {short}"), (conn, 'CREATE SCHEMA IF NOT EXISTS "app"'),
            (conn, f'DROP TABLE IF EXISTS "app"."{short}"'),
            (conn, f'CREATE TABLE "app"."{short}"')]


class _RecordingPool:
    """ProcessPoolExecutor stand-in: runs each unit inline at submission, recording it."""

    log: list = []

    def __init__(  # noqa: ANN001
        self, max_workers=None, mp_context=None, initializer=None, initargs=()
    ) -> None:
        _RecordingPool.log.append(("pool", max_workers))
        if initializer is not None:
            initializer(*initargs)

    def submit(self, fn, *args):  # noqa: ANN001, ANN201
        _RecordingPool.log.append(("submit", args[0].table.name))
        future = concurrent.futures.Future()
        try:
            future.set_result(fn(*args))
        except BaseException as exc:  # noqa: BLE001
            future.set_exception(exc)
        return future

    def shutdown(self, wait=True, cancel_futures=False):  # noqa: ANN001
        pass

    def __enter__(self) -> "_RecordingPool":
        return self

    def __exit__(self, *_exc) -> bool:
        return False


class _Time:
    """The engine's ``time`` module with ``sleep`` recorded, not slept."""

    def __init__(self, log: list) -> None:
        self._log = log

    def sleep(self, seconds: float) -> None:
        self._log.append(("sleep", seconds))

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(time, name)


@pytest.fixture
def process_run(monkeypatch):
    """Run ``_migrate_tables_in_parallel`` on the process path, the workers inline; the
    recreator is the DEFAULT one, over the :class:`_Dsql` double."""
    log: list = []
    _RecordingPool.log = log

    def _whole(args):  # noqa: ANN001, ANN202
        log.append(("start", args.table.name))
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=1)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _RecordingPool)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda *_a, **_k: None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")

    def _run(names, *, replace=None, dsql=None, parallelism=2, withheld=()):  # noqa: ANN001, ANN202
        log.clear()
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", str(parallelism))
        dsql = dsql or _Dsql()
        dsql.log = log  # its statements and the pool's submissions, in one order
        monkeypatch.setattr(engine, "DsqlConnector", dsql)
        monkeypatch.setattr(engine, "_time", _Time(log))
        tables = [_table(n) for n in names]
        inputs = _inputs(tables, cdc=False, replace=names if replace is None else replace,
                         conversions={n: _conversion(n) for n in names})
        migrator = BatchedTableMigrator(
            inputs, exporter=object(), watermark_capturer=object(),
            importer_factory=lambda _i: SimpleNamespace(create_indexes=lambda d: (0, [])),
            target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            target_unique_keys_reader=lambda t: [list(t.primary_key)],
        )
        if withheld:
            migrator.bind_run_inputs(dataclasses.replace(
                migrator._inputs, name_clash_tables=frozenset(withheld)))
        handle = _Handle(names)
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, ErrorLogStore(), watermark=_wm(), fresh_watermark=True
        )
        statements = [entry for entry in log if isinstance(entry[0], int)]
        return SimpleNamespace(log=list(log), dsql=dsql, statements=statements, counts=counts,
                               status={c.chunk_id: c.status for c in handle.job.chunks})

    return _run


NAMES = ["app.t1", "app.t2", "app.t3"]


def test_the_pre_pass_recreates_every_table_on_one_connection_then_starts_the_units(
    process_run,
) -> None:
    r = process_run(NAMES)
    # Before: two connections (two connectors, two token mints) per table, six here.
    assert len(r.dsql.connections) == 1 and r.dsql.connectors == 1
    assert r.statements == [
        *_recreated("app.t1", 1), *_recreated("app.t2", 1), *_recreated("app.t3", 1),
        (1, "close"),
    ]
    # Serial and before any unit: the one connection is closed, then the units start.
    assert r.log.index((1, "close")) < r.log.index(("submit", "app.t1"))
    starts = [entry for entry in r.log if entry[0] == "start"]
    assert starts == [("start", n) for n in NAMES]
    assert set(r.status.values()) == {"DONE"} and r.counts == (0, 0)


def test_a_unit_that_lost_the_connection_replays_on_a_new_one_which_the_rest_reuse(
    process_run,
) -> None:
    r = process_run(NAMES, dsql=_Dsql(drops=['CREATE TABLE "app"."t2"']))
    assert len(r.dsql.connections) == 2
    replay = _recreated("app.t2", 2)[1:]  # the unit replays from its schema ensure
    assert r.statements == [
        *_recreated("app.t1", 1), *_recreated("app.t2", 1),  # its CREATE lost connection 1
        (1, "close"), *replay, *_recreated("app.t3", 2), (2, "close"),
    ]
    assert set(r.status.values()) == {"DONE"}


def test_a_connection_dropped_between_tables_is_replaced_at_the_next_tables_column_read(
    process_run,
) -> None:
    # A table's column read is the first statement on the reused connection, so a connection
    # Aurora DSQL dropped after the table before fails there first. It had no retry: the
    # whole run aborted (no overlay pre-pass) on a connection a fresh one per table never
    # had. It reads again on a new connection, which the rest reuse.
    r = process_run(NAMES, dsql=_Dsql(drops=["columns t2"]))
    assert len(r.dsql.connections) == 2
    assert r.statements == [
        *_recreated("app.t1", 1), (1, "columns t2"), (1, "close"),
        *_recreated("app.t2", 2), *_recreated("app.t3", 2), (2, "close"),
    ]
    assert set(r.status.values()) == {"DONE"}


class _CatalogError(Exception):
    def __init__(self, sqlstate) -> None:  # noqa: ANN001
        super().__init__(f"catalog read failed ({sqlstate})")
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    ("sqlstate", "attempts"),
    [("XX000", 2), ("40001", 2), ("57P01", 2), ("08006", 2), ("42501", 1), ("28000", 1)],
)
def test_the_column_read_runs_again_only_on_a_retryable_failure(
    monkeypatch, sqlstate, attempts
) -> None:
    # Aurora DSQL's server unavailable, an OCC conflict and a lost session (a class-08 drop, or
    # one the server ended, as Validation's reads count it) read again; a real refusal fails
    # the recreate at once, as before.
    monkeypatch.setattr(engine, "_time", _Time([]))
    calls: list = []

    class _Once:
        def cursor(self):  # noqa: ANN202
            return self

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_exc) -> bool:
            return False

        def execute(self, *_a) -> None:
            if len(calls) == 1:
                raise _CatalogError(sqlstate)

        def fetchall(self) -> list:
            return [("id",), ("email",)]

        def close(self) -> None:
            pass

    def connect() -> _Once:
        calls.append(1)
        return _Once()

    def read():  # noqa: ANN202
        return engine._target_columns_the_conversion_would_drop(
            None, "app.t1", _conversion("app.t1"), connection_factory=connect
        )

    if attempts == 1:
        with pytest.raises(_CatalogError):
            read()
    else:
        assert read() == []
    assert len(calls) == attempts


def test_a_table_that_fails_its_recreate_drops_the_connection_and_the_run_still_closes_it(
    process_run,
) -> None:
    # Connection 1 reports a column the conversion does not have on t2: the recreate refuses
    # (the run aborts, as before -- there is no overlay pre-pass here).
    dsql = _Dsql(columns={"t2": ["id", "email", "added_by_hand"]})
    with pytest.raises(FullLoadIncompleteError, match="added_by_hand"):
        process_run(NAMES, dsql=dsql)
    assert dsql.log == [*_recreated("app.t1", 1), (1, "columns t2"), (1, "close")]
    assert len(dsql.connections) == 1


def test_an_append_only_run_opens_no_connection(process_run) -> None:
    r = process_run(NAMES, replace=[])
    assert r.dsql.connections == [] and set(r.status.values()) == {"DONE"}


def test_the_reused_connection_is_replaced_when_closed_discarded_or_30_minutes_old() -> None:
    opened: list = []

    def _open() -> SimpleNamespace:
        conn = SimpleNamespace(closed=False, number=len(opened) + 1, closes=0)

        def close() -> None:
            conn.closes += 1
            conn.closed = True

        conn.close = close
        opened.append(conn)
        return conn

    now = [1000.0]
    reused = _ReusedTargetConnection(_open, clock=lambda: now[0])
    first = reused()
    first.close()                      # a unit ends: the connection stays open
    assert reused().number == 1 and opened[0].closes == 0
    now[0] += engine._REUSED_CONNECTION_MAX_AGE_SECONDS - 1
    assert reused().number == 1
    now[0] += 1                        # 30 minutes old: replaced before its next use
    assert reused().number == 2 and opened[0].closes == 1
    opened[1].closed = True            # it broke (a unit saw the drop)
    assert reused().number == 3 and opened[1].closes == 1  # released all the same
    reused.discard()                   # a table failed on it
    assert opened[2].closes == 1 and reused().number == 4
    reused.close()
    reused.close()                     # idempotent
    assert [c.closes for c in opened] == [1, 1, 1, 1]  # each closed exactly once
    assert engine._REUSED_CONNECTION_MAX_AGE_SECONDS == 30 * 60


def test_the_reused_connection_sends_every_statement_unprepared() -> None:
    # Every table runs the same schema ensure on it: psycopg would prepare that DDL from its
    # sixth run on one connection, which a fresh connection per table never reached. A real
    # psycopg connection whose connect has already failed (no server anywhere).
    connection = psycopg.Connection(pq.PGconn.connect_start(b"host=/nonexistent-hyg port=1"))
    try:
        assert connection.prepare_threshold == 5  # psycopg's default
        reused = _ReusedTargetConnection(lambda: connection)
        reused()
        assert connection.prepare_threshold is None
        reused.close()
    finally:
        connection.close()


def test_outside_the_pre_pass_a_recreate_opens_and_closes_its_own_connections(
    monkeypatch,
) -> None:
    # The thread path and a worker's own recreate (a retry) call it without a connection:
    # one connector, a connection for the column read and one for the DROP + CREATE.
    dsql = _Dsql()
    monkeypatch.setattr(engine, "DsqlConnector", dsql)
    inputs = _inputs([_table("app.t1")], cdc=False, replace=["app.t1"],
                     conversions={"app.t1": _conversion("app.t1")})
    assert engine._default_table_recreator(inputs)(_table("app.t1")) == []
    assert dsql.connectors == 1 and len(dsql.connections) == 2
    assert dsql.log == [(1, "columns t1"), (1, "close"), *_recreated("app.t1", 2)[1:],
                        (2, "close")]
    # A dropped column read reads again there too, on a connection of its own.
    dsql = _Dsql(drops=["columns t1"])
    monkeypatch.setattr(engine, "DsqlConnector", dsql)
    monkeypatch.setattr(engine, "_time", _Time([]))
    assert engine._default_table_recreator(inputs)(_table("app.t1")) == []
    assert dsql.log == [(1, "columns t1"), (1, "close"), (2, "columns t1"), (2, "close"),
                        *_recreated("app.t1", 3)[1:], (3, "close")]


# ---------------------------------------------------------------------------
# 4. The launch stagger
# ---------------------------------------------------------------------------


def _submissions(log: list) -> list:
    return [entry for entry in log if entry[0] in ("submit", "sleep")]


def test_the_stagger_waits_only_before_the_submissions_that_start_a_process(
    process_run,
) -> None:
    names = [f"app.t{i}" for i in range(1, 7)]
    stagger = engine._PROCESS_LAUNCH_STAGGER_SECONDS
    r = process_run(names, replace=[], parallelism=3)
    # Three processes: a wait before the second and the third; the other three queue. Before,
    # a wait came before every submission but the first (1000 tables: 150 s of submit loop).
    assert ("pool", 3) in r.log
    assert _submissions(r.log) == [
        ("submit", "app.t1"), ("sleep", stagger), ("submit", "app.t2"), ("sleep", stagger),
        ("submit", "app.t3"), ("submit", "app.t4"), ("submit", "app.t5"),
        ("submit", "app.t6")]
    # Fewer units than the parallelism: every unit starts a process, as before.
    r = process_run(names[:2], replace=[], parallelism=3)
    assert ("pool", 2) in r.log
    assert _submissions(r.log) == [
        ("submit", "app.t1"), ("sleep", stagger), ("submit", "app.t2")]


def test_a_withheld_table_neither_counts_nor_waits(process_run) -> None:
    stagger = engine._PROCESS_LAUNCH_STAGGER_SECONDS
    r = process_run(["app.t1", "app.t2", "app.t3", "app.t4"], replace=[], parallelism=2,
                    withheld={"app.t1"})
    assert _submissions(r.log) == [
        ("submit", "app.t2"), ("sleep", stagger), ("submit", "app.t3"), ("submit", "app.t4")]


def test_premise_a_spawn_pool_starts_one_process_per_submission_up_to_its_size() -> None:
    """The stagger rests on how CPython's pool starts its processes: with ``spawn`` each
    ``submit`` starts one while there is no idle one and it has fewer than ``max_workers``;
    a later submission only queues. Asserted against a real pool, while its first tasks
    still run, so the stagger's count can never drift from the pool's without this failing.
    """
    pool = ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn"))
    try:
        started = []
        futures = []
        for _ in range(4):
            futures.append(pool.submit(time.sleep, 0.5))
            started.append(len(pool._processes))
        assert started == [1, 2, 2, 2]
        for future in futures:
            future.result(timeout=120)
    finally:
        processes = list((getattr(pool, "_processes", None) or {}).values())
        pool.shutdown(wait=True)
        for process in processes:
            process.join(60.0)
