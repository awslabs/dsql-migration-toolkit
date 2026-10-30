# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A table, column or key named with a ``:`` reaches the source as written, through every
statement the tool runs with SQLAlchemy ``text()``.

``text()`` reads a ``:`` that follows a space or other punctuation and precedes a word as a
bind parameter -- inside a quoted name too -- and turns ``\\:`` into ``:``. No server
anywhere. What each driver receives is read where it would be sent, by the harnesses of
``test_validation_percent_names``: a real SQLAlchemy ``mysql+pymysql`` engine over PyMySQL's
own cursor, and a real ``postgresql+psycopg`` engine over an offline psycopg connection,
then psycopg's own conversion. SQLite runs the Full Load reader's statements of both
dialects for real (it reads backticks and double quotes alike, and a ``:`` in a quoted name
is the name's).

Pinned here:

* :func:`~dsql_migrator.core.source_dialect.base.text_safe`: every string of up to five of
  the characters ``text()`` and the drivers read (``:``, a backslash, ``$``, ``%``, a
  letter, a space) comes back from SQLAlchemy's own compiler exactly, in the paramstyle of
  each of the tool's drivers, with no bind parameter read from it; a name without a ``:``
  is written for ``text()`` byte for byte as it was;
* every statement the tool runs through ``text()`` with a name in it -- Validation's
  MySQL-source reads, the Full Load reader's pages and key ranges, the row counts and
  high-water marks (MySQL and PostgreSQL), the CDC publication writes, the PostgreSQL
  sequence read, the MySQL role-grant read and the oversized-value probe -- sends each name
  as itself: byte for byte what the same statement sends for the name with each ``:`` as a
  byte no layer reads, put back (and the probe, now quoting as its engine does, measures a
  name with an embedded quote too);
* end to end on test_g138's doubles: Validator matches tables, keys and foreign keys named
  with a ``:`` on a MySQL source (and on a PostgreSQL source, whose reads never went through
  ``text()``) and counts their orphans, and the Full Load reader reads every row in key
  order -- a key led by an ENUM member by member -- on the doubles, and on SQLite for both
  dialects, sharded or not;
* the harnesses fail a name written into ``text()`` as before, as a source does.
"""

from __future__ import annotations

import contextlib
import itertools
import sqlite3
from typing import Any, Optional

import pytest
from psycopg import pq
from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects import mysql as sa_mysql
from sqlalchemy.dialects import postgresql as sa_postgresql
from sqlalchemy.dialects import sqlite as sa_sqlite
from sqlalchemy.engine import default as sa_default
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.pool import StaticPool

from dsql_migrator.core import validator as v
from dsql_migrator.core.cdc_pg_slot import (
    add_table_to_publication,
    create_publication,
    set_replica_identity_full,
)
from dsql_migrator.core.exporter import compute_pk_shard_ranges, keyset_stream
from dsql_migrator.core.introspector import install_read_only_guard
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
    ValidationMode,
)
from dsql_migrator.core.source_dialect import postgres as postgres_dialect
from dsql_migrator.core.source_dialect.base import text_safe
from dsql_migrator.core.source_dialect.mysql import MySQLSourceDialect
from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect, _sequence_row
from dsql_migrator.core.validation_sql import (
    build_mysql_checksum_sql,
    build_mysql_composite_boundary_sql,
    build_mysql_composite_page_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
    build_mysql_pk_first_page_sql,
    build_mysql_pk_next_page_sql,
    build_mysql_pk_token_sql,
)
from dsql_migrator.core.watermark import count_source_rows, max_pk_source
from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe
from tests.test_cdc_pg_slot import _FakeConn
from tests.test_g138_composite_validation import PgDouble, _Result, _Table
from tests.test_validation_percent_names import (
    _NOWHERE,
    _PG_SOURCE,
    _SOURCE,
    _TARGET,
    _composite_variants,
    _keyed_table,
    _MySqlSourceEngine,
    _OfflinePg,
    _single_key_variants,
    _SqlState,
    _target_sent,
    _WireMySqlSource,
    _WirePgSource,
    _WireTarget,
)

# Names whose ':' text() read as a bind parameter or an escape: after a space, at the start,
# after a backslash (which it dropped) or a '%', before unicode or '$', and the name of a
# parameter the statements bind (:last, which took the cursor's value).
_READ_NAMES = ("a :b", ":b", "a\\:b", "\\:", "a%:b", "ünï :ö", ":last", "a :$b")
# A ':' text() never misread: after a letter or a digit, '::', a lone ':'.
_UNREAD_NAMES = ("x::y", "k:1", ":")
# And names without a ':' -- a backslash, a '%', a space -- which the fix leaves as they were.
_OTHER_NAMES = ("plain", "back\\slash", "pct%off", "sp ace")
_NAMES = _READ_NAMES + _UNREAD_NAMES + _OTHER_NAMES
# The stand-in the expected bytes are built with: special to no layer (SQLAlchemy's text(),
# PyMySQL's formatting and escaping, psycopg's placeholder read, libpq, SQLite).
_COLON = "\x01"


def _twin(name: str) -> str:
    return name.replace(":", _COLON)


def _put_back(sent: Any) -> Any:
    if isinstance(sent, bytes):
        return sent.replace(_COLON.encode(), b":")
    return sent.replace(_COLON, ":")


# ---------------------------------------------------------------------------
# Where each driver would send
# ---------------------------------------------------------------------------


class _PgTextEngine:
    """A real SQLAlchemy ``postgresql+psycopg`` engine over an offline psycopg connection,
    stopped where it would call psycopg's ``cursor.execute``: :meth:`sent` is what psycopg
    sends for a statement run through ``connection.execute`` -- ``text()`` compiled by
    SQLAlchemy, handed to psycopg with the parameters SQLAlchemy hands it, converted by
    psycopg's own query class. With the tool's read-only guard, as every source engine has
    it, except for the CDC write engine's stand-in (``guarded=False``)."""

    def __init__(self, connection: _OfflinePg, *, guarded: bool = True) -> None:
        self.raw = connection
        self.engine = create_engine(
            "postgresql+psycopg://", creator=lambda: connection, _initialize=False,
            poolclass=StaticPool,
        )
        if guarded:
            install_read_only_guard(self.engine)
        self.handed: list[tuple[str, Any]] = []
        event.listen(self.engine, "do_execute", self._do_execute)
        event.listen(self.engine, "do_execute_no_params", self._do_execute_no_params)

    def _do_execute(self, cursor: Any, statement: str, parameters: Any, context: Any) -> bool:
        self.handed.append((statement, parameters))
        return True

    def _do_execute_no_params(self, cursor: Any, statement: str, context: Any) -> bool:
        self.handed.append((statement, None))
        return True

    def sent(self, statement: Any, params: Any = None) -> bytes:
        handed = len(self.handed)
        with self.engine.connect() as connection:
            if params:
                connection.execute(statement, params)
            else:
                connection.execute(statement)
        assert len(self.handed) == handed + 1
        return _target_sent(self.raw, *self.handed[-1])


@pytest.fixture(scope="module")
def offline():
    connection = _OfflinePg(pq.PGconn.connect_start(_NOWHERE))
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(scope="module")
def pg_source(offline):
    harness = _PgTextEngine(offline)
    try:
        yield harness
    finally:
        harness.engine.dispose()


@pytest.fixture(scope="module")
def pg_writes(offline):
    harness = _PgTextEngine(offline, guarded=False)
    try:
        yield harness
    finally:
        harness.engine.dispose()


@pytest.fixture(scope="module")
def mysql_source():
    harness = _MySqlSourceEngine()
    try:
        yield harness
    finally:
        harness.engine.dispose()


@pytest.fixture
def no_inheritance(monkeypatch):
    # SQLite has no pg_catalog: a PostgreSQL table read there is no inheritance parent.
    monkeypatch.setattr(postgres_dialect, "pg_reads_own_rows_only", lambda _c, _n: False)


class _Recorder:
    """A connection that keeps each statement it is given, with its parameters, then runs it
    on ``inner``."""

    def __init__(self, inner: Any, reads: Optional[list] = None) -> None:
        self._inner = inner
        self.reads: list = [] if reads is None else reads

    def execute(self, statement: Any, parameters: Any = None, **kwargs: Any) -> Any:
        self.reads.append((statement, dict(parameters or {})))
        if parameters is None:
            return self._inner.execute(statement, **kwargs)
        return self._inner.execute(statement, parameters, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _Answers:
    """A connection answering every statement with ``rows``."""

    def __init__(self, *rows: tuple) -> None:
        self._rows = list(rows)

    def execute(self, statement: Any, parameters: Any = None) -> _Result:
        return _Result(self._rows)


class _RecordingEngine:
    """An engine whose connections are :class:`_Recorder` s, keeping every read in ``reads``."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self.reads: list = []

    @contextlib.contextmanager
    def connect(self):
        with self.engine.connect() as connection:
            yield _Recorder(connection, self.reads)


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sqlite_source(table: TableDef, rows: "list[dict]") -> Any:
    """``table`` holding ``rows`` on SQLite, its schema an attached database, behind the
    tool's read-only guard. A double-quoted name no table has is an error, as on
    PostgreSQL (SQLite would read it as a string literal)."""
    engine = create_engine("sqlite://", poolclass=StaticPool)
    event.listen(
        engine, "connect",
        lambda dbapi, _record: dbapi.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False),
    )
    schema, _, bare = table.name.partition(".")
    target = f"{_quoted(schema)}.{_quoted(bare)}"
    columns = ", ".join(
        f"{_quoted(c.name)} {'INTEGER' if c.mysql_type == 'int' else 'TEXT'}"
        for c in table.columns
    )
    keys = ", ".join(_quoted(k) for k in table.primary_key)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ATTACH DATABASE ':memory:' AS {_quoted(schema)}")
        connection.exec_driver_sql(f"CREATE TABLE {target} ({columns}, PRIMARY KEY ({keys}))")
        connection.exec_driver_sql(
            f"INSERT INTO {target} VALUES ({', '.join('?' * len(table.columns))})",
            [tuple(row[c.name] for c in table.columns) for row in rows],
        )
    install_read_only_guard(engine)
    return engine


# ---------------------------------------------------------------------------
# text_safe and the dialects against SQLAlchemy's own compiler
# ---------------------------------------------------------------------------


_PARAMSTYLES = {
    "PyMySQL (format)": sa_mysql.pymysql.dialect(),
    "psycopg (pyformat)": sa_postgresql.psycopg.dialect(),
    "SQLite (qmark)": sa_sqlite.pysqlite.dialect(),
    "named": sa_default.DefaultDialect(),
}


def test_text_safe_comes_back_from_sqlalchemys_text_exactly() -> None:
    # Every string of up to five of the characters text() and the drivers read. Expected:
    # the same statement with each ':' as a byte no layer reads, compiled the same way, and
    # the ':' put back -- so a '%' is doubled for PyMySQL and psycopg as it always was.
    alphabet = (":", "\\", "$", "%", "a", " ")
    strings = ["".join(p) for n in range(1, 6) for p in itertools.product(alphabet, repeat=n)]
    misread: dict[str, set] = {}
    for label, dialect in _PARAMSTYLES.items():
        misread[label] = set()
        for string in strings:
            name = f'"{string}"'
            compiled = text(f"SELECT {text_safe(name)} FROM t WHERE k > :last").compile(
                dialect=dialect
            )
            assert set(compiled.params) == {"last"}, string  # no parameter from the name
            twin = text(f"SELECT {_twin(name)} FROM t WHERE k > :last").compile(dialect=dialect)
            assert str(compiled) == _put_back(str(twin)), (label, string)
            raw = text(f"SELECT {name} FROM t WHERE k > :last").compile(dialect=dialect)
            if set(raw.params) != {"last"} or str(raw) != str(compiled):
                misread[label].add(string)
    assert all(text_safe(s) == s for s in strings if ":" not in s)
    # As written before, text() misreads the same names for each of the drivers'
    # paramstyles: thousands of them, each with a ':'.
    drivers = {frozenset(names) for label, names in misread.items() if label != "named"}
    assert len(drivers) == 1
    (names,) = drivers
    assert len(names) > 1000 and all(":" in s for s in names)


def test_the_dialects_name_for_text_what_they_quote_for_everything_else() -> None:
    for dialect in (MySQLSourceDialect(), PostgresSourceDialect()):
        for name in (*_NAMES, "s.a :b", "a :b.t", "a`b", 'q":b'):
            # What text() makes of a name quoted for it: the name as the engine quotes it.
            assert str(text(dialect.text_identifier(name)).compile()) == (
                dialect.quote_identifier(name)
            ), name
            assert str(text(dialect.text_table(name)).compile()) == dialect.quote_table(name)
            if ":" not in name:  # byte for byte as before
                assert dialect.text_identifier(name) == dialect.quote_identifier(name)
                assert dialect.text_table(name) == dialect.quote_table(name)
    # A statement shown to the operator, or run without text(), keeps the engine's quoting.
    assert MySQLSourceDialect().quote_identifier("a :b") == "`a :b`"
    assert PostgresSourceDialect().quote_table("s.a :b") == '"s"."a :b"'


def test_the_harnesses_fail_a_name_written_into_text_as_before(mysql_source, pg_source) -> None:
    # Guard the guard: the checks below pass only over harnesses that read each statement as
    # SQLAlchemy and the drivers do. A name quoted as the tool quoted it before, its ':' left
    # for text() to read, fails there as on a source: a value is missing for the parameter
    # text() read from the name, or the name takes a parameter's value, or loses a backslash.
    mysql, pg = MySQLSourceDialect(), PostgresSourceDialect()
    with pytest.raises(StatementError, match="A value is required for bind parameter 'b'"):
        mysql_source.sent(text(f"SELECT COUNT(*) FROM {mysql.quote_table('s.a :b')}"))
    with pytest.raises(StatementError, match="A value is required for bind parameter 'b'"):
        pg_source.sent(text(f"SELECT COUNT(*) FROM {pg.quote_table('s.a :b')}"))
    last = f"SELECT {mysql.quote_identifier(':last')} FROM t WHERE k > :last"
    assert mysql_source.sent(text(last), {"last": 7}) == "SELECT `7` FROM t WHERE k > 7"
    backslash = f"SELECT {pg.quote_identifier('a' + chr(92) + ':b')} FROM t"  # "a\:b"
    assert pg_source.sent(text(backslash)) == b'SELECT "a:b" FROM t'
    # The wire double refuses the column such a name became, as a server does ...
    source = _WireMySqlSource(mysql_source, _Table("app.t", (":last",), [{":last": 1}]))
    with pytest.raises(_SqlState, match="`7` does not exist"):
        source.execute(text("SELECT COUNT(*) FROM `app`.`t` WHERE `:last` > :last"), {"last": 7})
    # ... and so does SQLite.
    table = TableDef(name="s.t", columns=[ColumnDef(name="a\\:b", mysql_type="int")],
                     primary_key=["a\\:b"])
    engine = _sqlite_source(table, [{"a\\:b": 1}])
    with engine.connect() as connection, pytest.raises(OperationalError, match="no such column"):
        connection.execute(text('SELECT "a\\:b" FROM "s"."t"'))
    engine.dispose()


# ---------------------------------------------------------------------------
# Every statement, every name: the bytes the driver sends
# ---------------------------------------------------------------------------


def _validation_reads(name: str) -> list:
    """Every statement Validation's builders write for a MySQL source with a name in it,
    ``name`` in each place a name can be, and the parameters its reader passes (``None``:
    none). (The one statement Validation writes itself, ``_source_count``'s, is read below;
    a walked member's page in the ENUM run.)"""
    dialect = MySQLSourceDialect()
    reads: list = []
    for table, key in _single_key_variants(name):
        schema, _, bare = table.name.partition(".")
        reads += [
            ("checksum", build_mysql_checksum_sql(table), None),
            ("pk tokens", build_mysql_pk_token_sql(table, key), {"sample_size": 5}),
            ("pk page", build_mysql_pk_first_page_sql(table, key), {"page": 5}),
            ("pk page after", build_mysql_pk_next_page_sql(table, key), {"last": 7, "page": 5}),
            ("checksum page", build_mysql_page_checksum_first_sql(table, key), {"page": 5}),
            ("checksum page after", build_mysql_page_checksum_next_sql(table, key),
             {"last": 7, "page": 5}),
            # The catalog reads bind the names as values: untouched, and pinned.
            ("primary index", v._MYSQL_PRIMARY_INDEX_SQL.format(schema=":schema"),
             {"schema": schema, "name": bare}),
            ("column types", v._MYSQL_COLUMN_TYPES_SQL, {"schema": schema, "name": bare}),
        ]
    for table in _composite_variants(name):
        where, params = dialect.keyset_after_sql(table, (7, 8), {})
        reads += [
            ("composite page", build_mysql_composite_page_sql(table), {"page": 5}),
            ("composite page after", build_mysql_composite_page_sql(table, where),
             {**params, "page": 5}),
            ("composite boundary", build_mysql_composite_boundary_sql(table), {"off": 4}),
            ("composite boundary after", build_mysql_composite_boundary_sql(table, where),
             {**params, "off": 4}),
        ]
    # An ENUM / SET key column: its cursor term is the members after the cursor's, or the
    # bitmask; a key LED by an ENUM is walked, its member named first.
    for key, kind, last in (((name, "k2"), "set", ("m1", 8)), (("k1", name), "enum", (7, "m1")),
                            ((name, "k2"), "enum", ("m1", 8))):
        table = _keyed_table("s.t", key, "c")
        table = table.model_copy(update={"columns": [
            c.model_copy(update={"mysql_type": f"{kind}('m1','m2')"}) if c.name == name else c
            for c in table.columns
        ]})
        members = {name: (kind, ("m1", "m2") if kind == "set" else ("", "m1", "m2"))}
        where, params = dialect.keyset_after_sql(table, last, members)
        reads.append((f"{kind} key {key.index(name)} page after",
                      build_mysql_composite_page_sql(table, where), {**params, "page": 5}))
    return reads


@pytest.mark.parametrize("name", _NAMES)
def test_every_mysql_validation_read_sends_each_name_as_itself(name, mysql_source) -> None:
    reads, twins = _validation_reads(name), _validation_reads(_twin(name))
    assert [r[0] for r in reads] == [t[0] for t in twins]
    quoted, named = MySQLSourceDialect().quote_identifier(name), 0
    for (label, statement, params), (_, twin, twin_params) in zip(reads, twins):
        expected = _put_back(mysql_source.sent(text(twin), twin_params))
        sent = mysql_source.sent(text(statement), params)
        assert sent == expected, label
        named += quoted in sent
    assert named >= len(reads) // 2
    # _source_count runs its own COUNT(*): the same.
    for table_name in (f"{name}.t", f"s.{name}"):
        with mysql_source.engine.connect() as connection:
            assert v._source_count(connection, table_name) == 0
        assert mysql_source.raw.sent[-1] == (
            f"SELECT COUNT(*) FROM {MySQLSourceDialect().quote_table(table_name)}"
        )


def _reader_tables(name: str) -> "list[TableDef]":
    """``name`` as the schema, the table, a one-column key, each column of a composite key,
    and another column -- integer keys, so each table is read in key ranges too."""

    def table(table_name: str, key: "tuple[str, ...]", other: str = "c") -> TableDef:
        columns = [ColumnDef(name=k, mysql_type="int") for k in key]
        return TableDef(
            name=table_name,
            columns=columns + [ColumnDef(name=other, mysql_type="varchar(20)")],
            primary_key=list(key),
        )

    return [
        table(f"{name}.t", ("id",)),
        table(f"s.{name}", ("id",)),
        table("s.t", (name,)),
        table("s.t", (name, "k2")),
        table("s.t", ("k1", name)),
        table("s.t", ("id",), other=name),
    ]


def _rows(table: TableDef) -> "list[dict]":
    """Five rows in key order: keys 1..5, or (0, 0), (0, 1), (1, 0), (1, 1), (2, 0)."""
    key, other = table.primary_key, table.columns[-1].name
    rows = []
    for i in range(5):
        row = dict(zip(key, (i + 1,) if len(key) == 1 else (i // 2, i % 2)))
        row[other] = f"v{i}"
        rows.append(row)
    return rows


def _reader_reads(table: TableDef, dialect: Any) -> tuple:
    """What the Full Load reads of ``table`` with ``dialect``'s SQL, run on SQLite: every
    statement with its parameters, and what came back -- the rows in pages of 2, the key
    ranges of 2 readers, each range's rows, the row count and the high-water mark."""
    engine = _sqlite_source(table, _rows(table))
    reads: list = []
    try:
        with engine.connect() as connection:
            source = _Recorder(connection, reads)
            rows = [dict(r) for r in keyset_stream(source, table, batch_size=2, dialect=dialect)]
            shards = compute_pk_shard_ranges(source, table, 2, dialect)
            sharded = [
                dict(r) for lo, hi in shards
                for r in keyset_stream(source, table, batch_size=2, pk_lower=lo, pk_upper=hi,
                                       dialect=dialect)
            ]
            counted = count_source_rows(source, [table.name], dialect)[table.name]
            high = max_pk_source(source, {table.name: table.primary_key[0]}, dialect)[table.name]
    finally:
        engine.dispose()
    return reads, (rows, shards, sharded, counted, high)


@pytest.mark.parametrize("engine", ["mysql", "postgresql"])
@pytest.mark.parametrize("name", _NAMES)
def test_every_full_load_read_sends_each_name_as_itself(
    name, engine, mysql_source, pg_source, no_inheritance
) -> None:
    dialect = MySQLSourceDialect() if engine == "mysql" else PostgresSourceDialect()
    send = mysql_source.sent if engine == "mysql" else pg_source.sent
    for table, twin in zip(_reader_tables(name), _reader_tables(_twin(name))):
        reads, (rows, shards, sharded, counted, high) = _reader_reads(table, dialect)
        # Run for real, on SQLite: every row in key order, read again across two key
        # ranges, counted, and the high-water mark of the leading key column.
        assert rows == _rows(table)
        assert len(shards) == 2 and sharded == rows
        assert counted == 5 and high == max(r[table.primary_key[0]] for r in rows)
        twin_reads, _ = _reader_reads(twin, dialect)
        assert len(reads) == len(twin_reads) >= 9, table.name
        for (statement, params), (twin_statement, twin_params) in zip(reads, twin_reads):
            assert send(statement, params) == _put_back(send(twin_statement, twin_params)), (
                table.name, str(statement)
            )


def _other_reads(name: str) -> "list[tuple[str, str, Any, dict]]":
    """The other statements run through ``text()`` with a name in them, ``name`` in each place:
    ``(label, engine, statement, parameters)``, each kept as the tool's code built it."""
    reads: list = []

    def keep(label: str, engine: str, recorder: _Recorder, wanted: "tuple[str, ...]") -> None:
        for statement, params in recorder.reads:
            if str(statement).startswith(wanted):
                reads.append((label, engine, statement, params))

    tables = [f"s.{name}", f"{name}.t"]
    # The CDC publication writes (cdc_pg_slot, the source-write engine).
    writes = _Recorder(_FakeConn(pubs={"dsqlmig_pub": []}))
    set_replica_identity_full(writes, tables)
    create = _Recorder(_FakeConn())
    create_publication(create, name="dsqlmig_pub", tables=tables)
    for table in tables:
        add_table_to_publication(writes, name="dsqlmig_pub", table=table)
    keep("replica identity / add table", "pg write", writes, ("ALTER",))
    keep("create publication", "pg write", create, ("CREATE",))
    # The PostgreSQL sequence read (Evaluation's standalone sequences).
    for schema, sequence in (("s", name), (name, "seq")):
        answers = _Recorder(_Answers((5, True)))
        assert _sequence_row(answers, schema, sequence) == (5, True)
        keep("sequence row", "pg read", answers, ("SELECT last_value",))
    # The MySQL grants read for an active role (Evaluation's listing gaps).
    role = f"`{name}`@`%`"

    class _Grants:
        def execute(self, statement: Any, params: Any = None) -> _Result:
            sql = str(statement)
            if sql == "SELECT CURRENT_ROLE()":
                return _Result([(role,)])
            if sql.startswith("SHOW GRANTS"):
                return _Result([("GRANT SELECT ON `app`.* TO `u`@`%`",)])
            return _Result([(0,)])

    grants = _Recorder(_Grants())
    MySQLSourceDialect().listing_gaps(grants, ["app"])
    keep("role grants", "mysql read", grants, ("SHOW GRANTS FOR",))
    return reads


@pytest.mark.parametrize("name", _NAMES)
def test_every_other_text_statement_sends_each_name_as_itself(
    name, mysql_source, pg_source, pg_writes
) -> None:
    reads, twins = _other_reads(name), _other_reads(_twin(name))
    assert [(r[0], r[1]) for r in reads] == [(t[0], t[1]) for t in twins]
    # Two REPLICA IDENTITY FULL and two ADD TABLE, the CREATE PUBLICATION, two sequence
    # reads and the grants read.
    assert len(reads) == 8
    senders = {"mysql read": mysql_source.sent, "pg read": pg_source.sent,
               "pg write": pg_writes.sent}
    for (label, engine, statement, params), (_, _, twin, twin_params) in zip(reads, twins):
        send = senders[engine]
        assert send(statement, params) == _put_back(send(twin, twin_params)), label


def _probe_reads(name: str, source_type: SourceType) -> tuple:
    """The oversized-value probe of ``name`` as the table, the schema and a measured column,
    run on SQLite: every statement, and the answers (a 9-byte value against a limit of 5)."""
    reads, answers = [], []
    for table_name, columns in ((f"s.{name}", ["c"]), (f"{name}.t", ["c"]), ("s.t", [name, "c"])):
        table = TableDef(
            name=table_name,
            columns=[ColumnDef(name="id", mysql_type="int")]
            + [ColumnDef(name=c, mysql_type="text") for c in columns],
            primary_key=["id"],
        )
        row = {"id": 1, **{c: ("123456789" if i == 0 else "12") for i, c in enumerate(columns)}}
        engine = _RecordingEngine(_sqlite_source(table, [row]))
        probe = SessionSourceProbe(
            SourceConnectionConfig(source_type=source_type, host="h", username="u"), None
        )
        answers.append(probe.oversized_values(
            table_name, columns, limit_bytes=5, timeout_seconds=1.0, engine=engine
        ))
        engine.engine.dispose()
        reads += [(s, p) for s, p in engine.reads if str(s).startswith("SELECT EXISTS")]
    return reads, answers


@pytest.mark.parametrize("source_type", [SourceType.MYSQL, SourceType.POSTGRES])
@pytest.mark.parametrize("name", _NAMES)
def test_the_oversized_value_probe_measures_each_column_as_named(
    name, source_type, mysql_source, pg_source, no_inheritance
) -> None:
    reads, answers = _probe_reads(name, source_type)
    twin_reads, _ = _probe_reads(_twin(name), source_type)
    # Measured on SQLite for real: the 9-byte value over the limit, the 2-byte one not.
    assert answers == [{"c": True}, {"c": True}, {name: True, "c": False}]
    send = mysql_source.sent if source_type is SourceType.MYSQL else pg_source.sent
    assert len(reads) == len(twin_reads) == 4
    for (statement, params), (twin, twin_params) in zip(reads, twin_reads):
        assert send(statement, params) == _put_back(send(twin, twin_params))


@pytest.mark.parametrize(
    "source_type, name", [(SourceType.MYSQL, "tick`name"), (SourceType.POSTGRES, 'q"uote')]
)
def test_the_oversized_value_probe_quotes_a_name_as_its_engine_does(
    source_type, name, no_inheritance
) -> None:
    # Its own quoting doubled no embedded quote, so such a name broke the statement and its
    # table or column was never measured; the dialect's quoting doubles it.
    _, answers = _probe_reads(name, source_type)
    assert answers == [{"c": True}, {"c": True}, {name: True, "c": False}]


# ---------------------------------------------------------------------------
# End to end: Validator and the Full Load reader on the doubles
# ---------------------------------------------------------------------------


def _colon_tables(*, pg_types: bool = False, orphans: int = 0) -> "list[_Table]":
    """A table keyed by an integer named ``:last`` (the cursor parameter's own name), one
    keyed by ``(k :1, :b)`` and a child of the first keyed by ``id :x`` -- in a schema, each
    with columns, named with a ':' -- the composite table a child of the first too.
    ``orphans`` of the child's rows point at no parent row."""
    integer, text_type = ("integer", "text") if pg_types else ("int", "varchar(20)")
    parent = _Table(
        "s :app.a :b", (":last",),
        [{":last": i, "x::y": f"v{i}", "a\\:b": i * 3} for i in range(1, 24)],
        types={":last": integer, "x::y": text_type, "a\\:b": integer},
    )
    composite = _Table(
        "s :app.x::y", ("k :1", ":b"),
        [{"k :1": i // 4, ":b": i % 4, "ünï :ö": f"n{i}", "p :id": 1 + i % 23}
         for i in range(23)],
        types={"k :1": integer, ":b": integer, "ünï :ö": text_type, "p :id": integer},
    )
    child = _Table(
        "s :app.a%:b", ("id :x",),
        [{"id :x": i, "p :id": 900 + i if i <= orphans else 1 + i % 23} for i in range(1, 31)],
        types={"id :x": integer, "p :id": integer},
    )
    return [parent, composite, child]


_FK = ForeignKeyDef(name="fk :parent", columns=["p :id"], referenced_table="s :app.a :b",
                    referenced_columns=[":last"])


def _definitions(tables: "list[_Table]") -> "list[TableDef]":
    parent, composite, child = tables
    return [
        parent.tabledef(),
        composite.tabledef().model_copy(update={"foreign_keys": [_FK]}),
        child.tabledef().model_copy(update={"foreign_keys": [_FK]}),
    ]


def _validate(source: Any, target: Any, tables: "list[TableDef]",
              config: SourceConnectionConfig) -> Any:
    return v.Validator(
        source_engine_factory=lambda _cfg: source,
        target_connection_factory=lambda _cfg: target,
        reconcile_page_size=4,
        null_proof_columns={"s :app.a :b": ["a\\:b"], "s :app.x::y": ["ünï :ö"]},
    ).validate(
        config, _TARGET, tables, ValidationMode.CHECKSUM, reconcile=True, check_orphans=True,
    )


def _assert_matched(report: Any) -> None:
    assert report.is_match is True, [(i.table, i.error, i.orphan_check_error)
                                     for i in report.items]
    assert all(i.matched and i.checksum_match for i in report.items)
    assert [i.target_row_count for i in report.items] == [23, 23, 30]
    assert [i.reconcile.consistent for i in report.items if i.reconcile] == [True, True]
    assert report.orphan_findings == [] and report.orphan_unchecked == []


def test_a_mysql_source_validates_tables_and_keys_named_with_a_colon(
    offline, mysql_source
) -> None:
    tables = _colon_tables()
    source = _WireMySqlSource(mysql_source, *tables)
    _assert_matched(_validate(source, _WireTarget(offline, *tables), _definitions(tables),
                              _SOURCE))
    # Every page, first and after a cursor, named the tables and keys as written.
    pages = [s for s in source.sent if "page_tok" in s or "ck_tok" in s or " AS pk " in s]
    assert pages and all("`s :app`." in s for s in pages)
    assert any("`:last` > 4" in s for s in pages)  # the cursor bound, the key named
    assert any("`k :1` = 1 AND `:b` > " in s for s in pages)


def test_a_postgresql_source_validates_them_as_it_always_did(offline) -> None:
    # Its reads run through psycopg without text() (exec_driver_sql): pinned.
    tables = _colon_tables(pg_types=True)
    source = _WirePgSource(offline, PgDouble(*tables))
    _assert_matched(_validate(source, _WireTarget(offline, *tables), _definitions(tables),
                              _PG_SOURCE))
    assert any(b'"s :app"."a :b"' in query for query, _ in source.sent)


def test_orphans_of_a_foreign_key_named_with_a_colon_are_counted(offline, mysql_source) -> None:
    tables = _colon_tables(orphans=2)
    source = _WireMySqlSource(mysql_source, *tables)
    report = _validate(source, _WireTarget(offline, *tables), _definitions(tables), _SOURCE)
    assert [(f.table, f.foreign_key, f.orphan_count) for f in report.orphan_findings] == [
        ("s :app.a%:b", "fk :parent", 2)
    ]
    assert all(i.matched for i in report.items)


def test_the_full_load_reader_reads_every_row_on_the_double(mysql_source) -> None:
    for table in _colon_tables():
        source = _WireMySqlSource(mysql_source, table)
        read = list(keyset_stream(source, table.tabledef(), batch_size=4,
                                  dialect=MySQLSourceDialect()))
        assert read == sorted(table.rows, key=lambda r: tuple(r[c] for c in table.key))
        assert len(source.sent) == len(table.rows) // 4 + 1
        assert all(f"`{table.key[-1]}`" in s for s in source.sent)


_MEMBERS = ("a :b", "b", "c")


def test_a_key_led_by_an_enum_named_with_a_colon_is_read_member_by_member(
    offline, mysql_source
) -> None:
    # Validation's checksum pages and the Full Load reader read a key led by an ENUM one
    # member at a time: ``<lead> = :last_0`` for a member's first page, the cursor within it.
    table = _Table(
        "s :app.e :num", ("st :1", "id :x"),
        [{"st :1": _MEMBERS[i % 3], "id :x": i, "v :1": f"v{i}"} for i in range(1, 20)],
        types={"st :1": "enum('a :b','b','c')", "id :x": "int", "v :1": "varchar(20)"},
    )
    source = _WireMySqlSource(mysql_source, table)
    report = v.Validator(
        source_engine_factory=lambda _cfg: source,
        target_connection_factory=lambda _cfg: _WireTarget(offline, table),
        reconcile_page_size=4,
    ).validate(_SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM)
    (item,) = report.items
    assert item.matched and item.checksum_match, item.error
    firsts = [s for s in source.sent if "ck_tok" in s and " WHERE `st :1` = 'b' ORDER BY " in s]
    assert firsts  # a member's first page: the ENUM column named, its member bound
    reader = _WireMySqlSource(mysql_source, table)
    read = list(keyset_stream(reader, table.tabledef(), batch_size=4,
                              dialect=MySQLSourceDialect()))
    assert read == sorted(table.rows, key=lambda r: (_MEMBERS.index(r["st :1"]), r["id :x"]))
    assert any(" WHERE `st :1` = 'c' ORDER BY " in s for s in reader.sent)
