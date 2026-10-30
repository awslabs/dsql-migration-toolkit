# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Automatic key-range splitting sees fresh MySQL row estimates.

MySQL 8.0 and later (Aurora MySQL 3) answer ``information_schema.TABLES.TABLE_ROWS`` from a
copy the data dictionary caches for ``information_schema_stats_expiry`` seconds, a day by
default. A table that grew since the copy was taken read as it was then: Reader shards 0
(automatic, from 5,000,000 estimated rows) read it whole, and the start order (R2) started it
late. The Full Load's estimate sessions -- the start point's and the shard planner's -- now send
``SET SESSION information_schema_stats_expiry = 0`` once, before their estimate reads, so
InnoDB answers from the statistics it keeps (still no row read). Pinned here:

* the gate: MySQL 8.0 and later (Aurora MySQL 3, 8.4, 9.x), told from the version SQLAlchemy's
  own MySQL dialect parsed at connect, real version strings included; nothing is sent to 5.7 or
  Aurora MySQL 2, to a connection that is not MySQL's, or when the version is unknown;
* a refused SET falls back to the cached estimates and the run goes on (one log line); a
  ``ReadOnlySourceError`` still propagates (Property 1);
* the read-only guard lets exactly this statement through, unchanged: it refuses writes, not a
  SET, so it needed no exception;
* the start point: the SET once, inside its snapshot, after the binlog reads and right before
  the estimate read, whose estimate is then the live one; nothing on 5.7;
* the shard planner (an explicit Reader shards value against ``SHARD_MIN_ROWS``): the SET once
  per planning session, before its estimate read;
* end to end, FAILING WITHOUT THE CHANGE: the start point captured from a session that caches
  statistics makes the automatic planner read a table that grew past 5M rows in key ranges, and
  start it first; the shard planner plans a table the cache puts under the minimum;
* PostgreSQL sends nothing, and the estimate read itself is unchanged, so the UI's own reads
  (Refresh counts, the CDC monitor, whose tooltip says the figure is cached) are as before;
* the manual says so in every language.

Doubles only: no MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.mysql import pymysql as _pymysql

from dsql_migrator.core.exporter import TableExporter
from dsql_migrator.core.introspector import (
    ReadOnlySourceError,
    install_read_only_guard,
    is_write_or_ddl,
)
from dsql_migrator.core.models import SourceConnectionConfig
from dsql_migrator.core.source_dialect.mysql import (
    MySQLSourceDialect,
    _LIVE_TABLE_STATISTICS,
    _caches_table_statistics,
)
from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect
from dsql_migrator.core.watermark import (
    COMMIT,
    SHOW_BINARY_LOG_STATUS,
    START_CONSISTENT_SNAPSHOT,
    capture_watermark,
    estimate_source_rows,
)

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g137_mysql_reader_shards import _table
from tests.test_g25_docs import LANGS, MANUAL, _flat, _has
from tests.test_p1_key_range_spread import spread  # noqa: F401 - the planner harness (fixture)

_M = 1_000_000
EVENTS = "app.events"
STEADY = "app.steady"
_MYSQL = SourceConnectionConfig(host="db", database="app")


@pytest.fixture(autouse=True)
def _automatic(monkeypatch):
    """Reader shards unset (automatic, the default), as the planner harness expects."""
    monkeypatch.delenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", raising=False)


def _mysql_dialect(version: str):
    """SQLAlchemy's own MySQL dialect with ``version`` parsed as it parses ``SELECT VERSION()``
    when it connects: what a real source connection carries as ``connection.dialect``."""
    dialect = _pymysql.dialect()
    dialect._parse_server_version(version)
    return dialect


class _Refused(Exception):
    """What the server answers a SET it refuses (here: MySQL error 1193)."""


class _Result:
    def __init__(self, *, rows=(), mapping=None, scalar=None) -> None:
        self._rows = list(rows)
        self._mapping = mapping
        self._scalar = scalar

    def mappings(self):
        return self

    def first(self):
        return self._mapping

    def scalar(self):
        return self._scalar

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


def _key(name: str) -> tuple:
    schema, _, table = name.partition(".")
    return schema, table


class _Session:
    """A MySQL source session. ``information_schema.TABLES`` answers TABLE_ROWS from
    ``cached`` until the session sets ``information_schema_stats_expiry`` to 0, then from
    ``live`` (the storage engine) -- as MySQL 8.0+ does; a 5.7 server has no cache (always
    ``live``) and refuses that SET, as it has no such variable. ``refuse``: what the SET raises
    on 8.0+. ``version=None``: no MySQL dialect on the connection. Every statement is in
    ``sql``."""

    def __init__(self, version="8.0.36", *, cached=None, live=None, refuse=None) -> None:
        self.dialect = _mysql_dialect(version) if version else None
        self.caches = bool(version) and int(version.split(".")[0]) >= 8
        self.cached = {_key(n): v for n, v in (cached or {}).items()}
        self.live = {_key(n): v for n, v in (live or {}).items()}
        self.refuse = refuse
        self.expiry = 86400
        self.sql: list = []

    def execution_options(self, **_kwargs):
        return self

    def execute(self, statement, params=None):
        sql = str(statement)
        self.sql.append(sql)
        upper = sql.upper()
        if upper.startswith("SET SESSION INFORMATION_SCHEMA_STATS_EXPIRY"):
            if not self.caches:
                raise _Refused("(1193, 'Unknown system variable')")
            if self.refuse is not None:
                raise self.refuse
            self.expiry = int(sql.rsplit("=", 1)[1])
            return _Result()
        if "FROM INFORMATION_SCHEMA.TABLES" in upper:
            source = self.live if (self.expiry == 0 or not self.caches) else self.cached
            pairs = [(params[f"s{i}"], params[f"t{i}"]) for i in range(len(params) // 2)]
            return _Result(rows=[(s, t, source[(s, t)]) for s, t in pairs if (s, t) in source])
        if upper.startswith("SELECT DATABASE()"):
            return _Result(scalar="app")
        if upper.startswith(SHOW_BINARY_LOG_STATUS):
            return _Result(mapping={"File": "mysql-bin.000009", "Position": 4,
                                    "Executed_Gtid_Set": ""})
        if upper.startswith("SELECT MIN("):
            return _Result(mapping={"lo": 1, "hi": 12 * _M})
        return _Result()  # START TRANSACTION, COMMIT, @@GLOBAL reads, the fingerprint read

    def index(self, prefix: str) -> int:
        (found,) = [i for i, sql in enumerate(self.sql) if sql.upper().startswith(prefix.upper())]
        return found


class _Engine:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def connect(self):
        session = self.session

        class _Context:
            def __enter__(self):
                return session

            def __exit__(self, *_exc):
                return False

        return _Context()

    def dispose(self) -> None:
        pass


_ESTIMATE = "SELECT table_schema, table_name, table_rows FROM information_schema.tables"


# ---------------------------------------------------------------------------
# 1. The gate, the fallback and the guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("version", [
    "8.0.36", "8.0.23-mysql_aurora.3.02.0", "8.0.mysql_aurora.3.04.0", "8.4.3", "9.4.0",
])
def test_mysql_8_and_later_reads_live_statistics_with_one_session_set(version) -> None:
    session = _Session(version)
    assert _caches_table_statistics(session) is True
    MySQLSourceDialect().prepare_estimate_session(session)
    assert session.sql == [_LIVE_TABLE_STATISTICS] and session.expiry == 0


@pytest.mark.parametrize("version", ["5.7.44-log", "5.7.12-mysql_aurora.2.11.2", None])
def test_mysql_5_7_or_a_connection_that_is_not_mysqls_is_sent_nothing(version) -> None:
    session = _Session(version)
    assert _caches_table_statistics(session) is False
    MySQLSourceDialect().prepare_estimate_session(session)
    assert session.sql == []


def test_an_unknown_version_or_another_engine_is_sent_nothing() -> None:
    from types import SimpleNamespace

    from sqlalchemy.dialects.postgresql import psycopg
    from sqlalchemy.dialects.sqlite import pysqlite

    unknown = _Session()
    unknown.dialect = _pymysql.dialect()           # never connected: no version read yet
    assert unknown.dialect.server_version_info is None
    others = [SimpleNamespace(dialect=d) for d in (pysqlite.dialect(), psycopg.dialect())]
    for connection in (unknown, *others):
        assert _caches_table_statistics(connection) is False
    MySQLSourceDialect().prepare_estimate_session(unknown)
    assert unknown.sql == []


def test_a_refused_set_falls_back_to_the_cached_estimates(caplog) -> None:
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M},
                       refuse=_Refused("(1227, 'Access denied')"))
    with caplog.at_level(logging.INFO, logger="dsql_migrator.core.source_dialect.mysql"):
        MySQLSourceDialect().prepare_estimate_session(session)  # never raises
    assert session.sql == [_LIVE_TABLE_STATISTICS] and session.expiry == 86400
    assert estimate_source_rows(session, [EVENTS]) == {EVENTS: 3 * _M}
    (record,) = caplog.records
    assert record.levelno == logging.INFO and "cached" in record.getMessage()
    assert "1227" in record.getMessage() and record.exc_info is None


def test_a_read_only_refusal_still_propagates() -> None:
    session = _Session(refuse=ReadOnlySourceError("refused non-read statement"))
    with pytest.raises(ReadOnlySourceError):
        MySQLSourceDialect().prepare_estimate_session(session)


class _Reached(Exception):
    """Raised by a listener after the guard's: the statement got past the guard."""


def test_the_read_only_guard_lets_exactly_this_statement_through_unchanged() -> None:
    # The guard refuses writes, DDL, locks and server administration, not a SET, so it needed
    # no exception: the statement passes it as written, a SESSION assignment of one variable,
    # and the guard refuses what it refused before.
    assert _LIVE_TABLE_STATISTICS == "SET SESSION information_schema_stats_expiry = 0"
    assert is_write_or_ddl(_LIVE_TABLE_STATISTICS) is False
    engine = create_engine("sqlite://")
    install_read_only_guard(engine)
    reached: list = []

    @event.listens_for(engine, "before_cursor_execute")
    def _after_the_guard(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        reached.append(statement)
        raise _Reached()

    with engine.connect() as connection:
        with pytest.raises(_Reached):
            connection.execute(text(_LIVE_TABLE_STATISTICS))
        for write in ("INSERT INTO t VALUES (1)", "DROP TABLE t", "FLUSH TABLES",
                      "CREATE TABLE t (id INT)", "LOCK TABLES t READ"):
            with pytest.raises(ReadOnlySourceError):
                connection.execute(text(write))
    assert reached == [_LIVE_TABLE_STATISTICS]
    engine.dispose()


# ---------------------------------------------------------------------------
# 2. The start point and the shard planner
# ---------------------------------------------------------------------------


def test_the_start_point_sets_it_once_in_its_snapshot_right_before_the_estimates() -> None:
    # FAILS WITHOUT THE CHANGE: the estimate was the cached 3M rows.
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M})
    watermark = capture_watermark(session, [EVENTS])
    assert watermark.table_row_counts == {EVENTS: 12 * _M}
    assert watermark.row_counts_approximate is True
    assert session.sql.count(_LIVE_TABLE_STATISTICS) == 1
    order = [session.index(START_CONSISTENT_SNAPSHOT), session.index(SHOW_BINARY_LOG_STATUS),
             session.index(_LIVE_TABLE_STATISTICS), session.index(_ESTIMATE),
             session.index(COMMIT)]
    assert order == sorted(order)
    assert session.index(_LIVE_TABLE_STATISTICS) == session.index("SELECT DATABASE()") - 1
    assert not [sql for sql in session.sql if is_write_or_ddl(sql)]
    assert not any("COUNT(*)" in sql.upper() for sql in session.sql)


def test_the_start_point_on_5_7_sends_no_set_and_reads_as_before() -> None:
    session = _Session("5.7.44-log", live={EVENTS: 12 * _M})
    watermark = capture_watermark(session, [EVENTS])
    assert watermark.table_row_counts == {EVENTS: 12 * _M}
    assert not any("INFORMATION_SCHEMA_STATS_EXPIRY" in sql.upper() for sql in session.sql)


def test_a_refused_set_never_fails_the_start_point() -> None:
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M},
                       refuse=_Refused("(1193, 'Unknown system variable')"))
    watermark = capture_watermark(session, [EVENTS])
    assert watermark.table_row_counts == {EVENTS: 3 * _M}      # the cached estimate
    assert watermark.binlog_file == "mysql-bin.000009" and session.sql[-1] == COMMIT


def test_the_shard_planner_reads_the_live_estimate_against_the_shard_minimum() -> None:
    # An explicit Reader shards value: the table must have SHARD_MIN_ROWS estimated rows.
    # FAILS WITHOUT THE CHANGE: the cache's 500K rows kept a 12M-row table whole.
    sessions = [_Session(cached={EVENTS: 500_000}, live={EVENTS: 12 * _M}) for _ in range(2)]
    for session in sessions:
        exporter = TableExporter(engine_factory=lambda _c, s=session: _Engine(s))
        ranges = exporter.plan_pk_shard_ranges(_MYSQL, _table(EVENTS), 4, min_rows=1 * _M)
        assert len(ranges) == 4
        assert session.sql.count(_LIVE_TABLE_STATISTICS) == 1     # once per planning session
        assert session.index(_LIVE_TABLE_STATISTICS) < session.index(_ESTIMATE)
    # Without a minimum (automatic: the start point's estimate decided) nothing is read but
    # the leading key's MIN and MAX.
    bare = _Session()
    TableExporter(engine_factory=lambda _c: _Engine(bare)).plan_pk_shard_ranges(
        _MYSQL, _table(EVENTS), 4)
    assert len(bare.sql) == 1 and bare.sql[0].startswith("SELECT MIN(")


def test_a_refused_set_leaves_the_planner_the_cached_estimate() -> None:
    session = _Session(cached={EVENTS: 500_000}, live={EVENTS: 12 * _M}, refuse=_Refused("no"))
    exporter = TableExporter(engine_factory=lambda _c: _Engine(session))
    assert exporter.plan_pk_shard_ranges(_MYSQL, _table(EVENTS), 4, min_rows=1 * _M) == [
        (None, None)]


def test_a_table_grown_past_5m_rows_since_the_cache_is_split_and_starts_first(
        spread) -> None:  # noqa: F811
    # FAILS WITHOUT THE CHANGE: the start point held the cached 3M rows, so the automatic
    # planner read app.events whole and started the 4M-row app.steady before it.
    session = _Session(cached={EVENTS: 3 * _M, STEADY: 4 * _M},
                       live={EVENTS: 12 * _M, STEADY: 4 * _M})
    watermark = capture_watermark(session, [STEADY, EVENTS])
    assert watermark.table_row_counts == {STEADY: 4 * _M, EVENTS: 12 * _M}
    r = spread([_table(STEADY, lead="varchar(10)"), _table(EVENTS)], watermark.table_row_counts)
    assert r.planned == [(EVENTS, 4, 0)]                  # 12M rows: 4 key ranges
    assert len(r.wholes[EVENTS].read_ranges) == 4 and r.wholes[STEADY].read_ranges == ()
    assert _InlineExecutor.submitted == [EVENTS, STEADY]  # R2: the largest first


# ---------------------------------------------------------------------------
# 3. What did not change
# ---------------------------------------------------------------------------


def test_postgresql_sends_nothing() -> None:
    class _Never:
        def execute(self, *_a, **_k):
            raise AssertionError("PostgreSQL's reltuples is read as it stands")

    assert PostgresSourceDialect().prepare_estimate_session(_Never()) is None


def test_the_estimate_read_itself_sends_no_set() -> None:
    # The SET belongs to the Full Load's estimate sessions; the read that the UI's Refresh
    # counts and the CDC monitor share is unchanged (the CDC monitor's tooltip says the MySQL
    # figure is cached).
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M})
    assert estimate_source_rows(session, [EVENTS]) == {EVENTS: 3 * _M}
    assert _LIVE_TABLE_STATISTICS not in session.sql


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_says_the_split_reads_mysql_estimates_from_innodb(lang) -> None:
    variable = _LIVE_TABLE_STATISTICS.split()[2]
    text_ = (MANUAL / lang / "03-full-load.md").read_text(encoding="utf-8")
    parallel = _flat(lang, text_.split("## 3.8", 1)[1])
    assert f"`{variable}`" in parallel and "InnoDB" in parallel, lang
    assert _has(lang, parallel, "MySQL 8.0") and _has(lang, parallel, "Aurora MySQL 3"), lang
