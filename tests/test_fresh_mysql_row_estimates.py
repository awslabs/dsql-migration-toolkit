# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Automatic key-range splitting, and the UI's own row estimates, see fresh MySQL estimates.

MySQL 8.0 and later (Aurora MySQL 3) answer ``information_schema.TABLES.TABLE_ROWS`` from a
copy the data dictionary caches for ``information_schema_stats_expiry`` seconds, a day by
default. A table that grew since the copy was taken read as it was then: Reader shards 0
(automatic, from 5,000,000 estimated rows) read it whole, and the start order (R2) started it
late. The Full Load's estimate sessions -- the start point's and the shard planner's -- now send
``SET SESSION information_schema_stats_expiry = 0`` once, before their estimate reads, so
InnoDB answers from the statistics it keeps (still no row read). So do the UI's own estimate
reads: Refresh source/target counts (the CDC monitor's Source rows (est.), which compared a
purged table's exact target count with the cache's old figure and read "rows missing") and
the CDC partition sizing of a deploy before any Full Load (which sized a table grown since
the cache by its old size). Pinned here:

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
* the UI's reads (the counts refresh, CDC partition sizing): the SET once per read, before it;
  nothing on 5.7; a refused SET leaves the cached figure and the read still lands; end to end, a
  purged table reads consistent and a grown one gets its partitions by its live size; the CDC
  monitor's tooltip says where the figure comes from;
* PostgreSQL sends nothing, and the shared estimate read sends no SET of its own;
* the manual says so in every language (§3.8 and §7.2).

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
from dsql_migrator.core.source_dialect.mysql import MySQLSourceDialect, _LIVE_TABLE_STATISTICS
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
    MySQLSourceDialect().prepare_estimate_session(session)
    assert session.sql == [_LIVE_TABLE_STATISTICS] and session.expiry == 0


@pytest.mark.parametrize("version", ["5.7.44-log", "5.7.12-mysql_aurora.2.11.2", None])
def test_mysql_5_7_or_a_connection_that_is_not_mysqls_is_sent_nothing(version) -> None:
    session = _Session(version)
    MySQLSourceDialect().prepare_estimate_session(session)
    assert session.sql == []


def test_an_unknown_version_or_another_engine_is_sent_nothing() -> None:
    from sqlalchemy.dialects.postgresql import psycopg
    from sqlalchemy.dialects.sqlite import pysqlite

    unknown = _Session()
    unknown.dialect = _pymysql.dialect()           # never connected: no version read yet
    assert unknown.dialect.server_version_info is None
    sessions = [unknown]
    # Another engine's connection, with the version its server reported (PostgreSQL 16 is past
    # MySQL's 8): the engine, not the number, keeps the statement away.
    for other, version in ((psycopg.dialect(), (16, 4)), (pysqlite.dialect(), (3, 45, 1))):
        other.server_version_info = version
        sessions.append(_Session())
        sessions[-1].dialect = other
    for session in sessions:
        MySQLSourceDialect().prepare_estimate_session(session)
        assert session.sql == [], session.dialect.name


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
# 3. The UI's own reads: Refresh source/target counts and CDC partition sizing
# ---------------------------------------------------------------------------


def _ui_session():
    """The browser session the UI reads the MySQL source through: the password in memory
    only (Property 7), no target."""
    from types import SimpleNamespace

    from dsql_migrator.config import SecretValue

    return SimpleNamespace(
        source_config=_MYSQL, source_password=SecretValue("pw"), has_source=lambda: True,
        has_target=lambda: False, target_config=None,
    )


def _refreshed_rows(monkeypatch, session: _Session, tables=(EVENTS,)):
    """Refresh source/target counts over ``session`` (the CDC monitor's Source rows (est.)),
    through the real MySQL dialect: the source estimates, which must have been read."""
    from dsql_migrator.ui.data_migration import DataMigrationState, _cdc_status

    monkeypatch.setattr(
        _cdc_status, "make_source_engine_factory", lambda _pw: lambda _c: _Engine(session))
    source_counts, *_rest, source_available = _cdc_status._fetch_migration_row_counts(
        DataMigrationState(), _ui_session(), list(tables))
    assert source_available is True
    return source_counts


def _cdc_sized_rows(monkeypatch, session: _Session, tables=(EVENTS,)):
    """The estimates a CDC deploy sizes its topic partitions from when no Full Load recorded
    any (CDC infrastructure deployed first), read over ``session`` through the real dialect."""
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.ui.data_migration._cdc_ui import _estimate_cdc_table_rows

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda _pw, **_k: lambda _c: _Engine(session))
    return _estimate_cdc_table_rows(_ui_session(), list(tables))


_UI_READS = pytest.mark.parametrize(
    "read", [_refreshed_rows, _cdc_sized_rows], ids=["counts-refresh", "cdc-sizing"])


@_UI_READS
def test_each_ui_read_sets_it_once_right_before_its_estimate(monkeypatch, read) -> None:
    # FAILS WITHOUT THE CHANGE: both read the cached 3M rows.
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M})
    assert read(monkeypatch, session) == {EVENTS: 12 * _M}
    assert session.sql.count(_LIVE_TABLE_STATISTICS) == 1
    assert session.index(_LIVE_TABLE_STATISTICS) == session.index("SELECT DATABASE()") - 1
    assert session.index(_LIVE_TABLE_STATISTICS) < session.index(_ESTIMATE)
    assert not [sql for sql in session.sql if is_write_or_ddl(sql)]
    assert not any("COUNT(*)" in sql.upper() for sql in session.sql)


@_UI_READS
def test_each_ui_read_on_5_7_sends_no_set(monkeypatch, read) -> None:
    session = _Session("5.7.44-log", live={EVENTS: 12 * _M})
    assert read(monkeypatch, session) == {EVENTS: 12 * _M}
    assert not any("INFORMATION_SCHEMA_STATS_EXPIRY" in sql.upper() for sql in session.sql)


@_UI_READS
def test_a_refused_set_leaves_each_ui_read_the_cached_estimate(monkeypatch, read) -> None:
    # The read still lands (the counts refresh still reports the source as read).
    session = _Session(cached={EVENTS: 3 * _M}, live={EVENTS: 12 * _M},
                       refuse=_Refused("(1227, 'Access denied')"))
    assert read(monkeypatch, session) == {EVENTS: 3 * _M}


def test_a_table_purged_since_the_cache_no_longer_reads_as_rows_missing(monkeypatch) -> None:
    # FAILS WITHOUT THE CHANGE: the cache still held the 12M rows of a table since purged to
    # 3M, so the monitor set the target's exact 3M against 12M -- far past the estimate's
    # tolerance, with the stream caught up -- and read "rows missing" (gap).
    from dsql_migrator.ui.data_migration import build_migration_table_status

    session = _Session(cached={EVENTS: 12 * _M}, live={EVENTS: 3 * _M})
    (row,) = build_migration_table_status(
        [EVENTS], source_counts=_refreshed_rows(monkeypatch, session),
        target_counts={EVENTS: 3 * _M}, source_max_pk={EVENTS: 9}, target_max_pk={EVENTS: 9})
    assert row.source_estimate is True and row.consistency == "consistent"


def test_a_table_grown_since_the_cache_gets_its_cdc_partitions_by_its_live_size(
        monkeypatch) -> None:
    # Eight captured tables the cache holds at 1M rows each (even: one partition per topic);
    # app.events has grown to 12M since. FAILS WITHOUT THE CHANGE: the deploy sized from the
    # cache left app.events on one partition, so one sink task streamed it.
    from dsql_migrator.core.cdc import CDC_DEFAULT_TOPIC_PREFIX as prefix, cdc_scaling_params

    names = [EVENTS] + [f"app.t{i}" for i in range(1, 8)]
    session = _Session(cached={name: 1 * _M for name in names},
                       live={**{name: 1 * _M for name in names}, EVENTS: 12 * _M})
    counts = _cdc_sized_rows(monkeypatch, session, names)
    params = dict(cdc_scaling_params(names, prefix, row_counts_by_table=counts, env={}))
    assert params["SinkTopicPartitions"], "uniform: every topic on one partition"
    partitions = dict(pair.rsplit(":", 1) for pair in params["SinkTopicPartitions"].split(","))
    assert partitions.pop(f"{prefix}.{EVENTS}") == "4"
    assert set(partitions.values()) == {"1"} and len(partitions) == 7


def test_the_cdc_monitor_tooltip_says_mysql_8_reads_innodb_not_the_cache() -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_monitoring import _source_estimate_header_tip

    tip = _source_estimate_header_tip(SourceType.MYSQL)
    variable = _LIVE_TABLE_STATISTICS.split()[2]
    assert "MySQL 8.0+ it is read from InnoDB, not from the copy MySQL caches" in tip
    assert variable in tip and "also caches it" not in tip


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_says_cdc_partition_sizing_reads_mysql_estimates_from_innodb(lang) -> None:
    variable = _LIVE_TABLE_STATISTICS.split()[2]
    text_ = (MANUAL / lang / "07-performance-and-tuning.md").read_text(encoding="utf-8")
    cdc = _flat(lang, text_.split("\n### CDC (", 1)[1].split("\n#### ", 1)[0])
    assert f"`{variable}`" in cdc and "InnoDB" in cdc, lang
    assert _has(lang, cdc, "MySQL 8.0") and _has(lang, cdc, "Aurora MySQL 3"), lang


# ---------------------------------------------------------------------------
# 4. What did not change
# ---------------------------------------------------------------------------


def test_postgresql_sends_nothing() -> None:
    class _Never:
        def execute(self, *_a, **_k):
            raise AssertionError("PostgreSQL's reltuples is read as it stands")

    assert PostgresSourceDialect().prepare_estimate_session(_Never()) is None


def test_the_estimate_read_itself_sends_no_set() -> None:
    # Each caller sends the SET once on its own session (the start point, the planner, the
    # counts refresh, CDC partition sizing); the estimate read they share sends none itself.
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
