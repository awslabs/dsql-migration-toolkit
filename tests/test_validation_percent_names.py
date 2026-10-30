# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validation names a table, column or key holding a ``%`` exactly, on every engine it reads.

No server anywhere. What each driver receives is read where it would be sent:

* Aurora DSQL: psycopg's own conversion (``Cursor._query_cls``, a ``PostgresQuery``) over a
  real :class:`psycopg.Connection` whose connect attempt has already failed -- libpq's own
  identifier quoting and the connection's adapters, while nothing can be sent;
* a PostgreSQL source: the real shim (and ``FROM ONLY`` cursor) over a real SQLAlchemy
  ``postgresql+psycopg`` engine on that connection, stopped at ``do_execute`` /
  ``do_execute_no_params`` with what SQLAlchemy hands psycopg, then psycopg's conversion;
* a MySQL source: a real SQLAlchemy ``mysql+pymysql`` engine over PyMySQL's own cursor,
  stopped at ``_query`` with the string ``execute`` sends.

Pinned here:

* every statement Validation and cut over run with names in it -- the count, reconcile,
  checksum and orphan pages (first, and after a keyset cursor), the row-diff sample, the
  single-statement fallbacks, a PostgreSQL inheritance parent's ``FROM ONLY`` reads, the
  MySQL source's reads and the identity re-sync -- sends each name as itself: for names
  with a ``%`` (``pct%off``, ``a%sb``, ``x%%y``, ...) and without (mixed case, quotes,
  backslash, unicode), byte for byte what the same statement sends for a name without one;
* Validator runs end to end on the in-memory doubles of ``test_g138_composite_validation``,
  behind cursors that convert each statement as the drivers do and a server that refuses a
  name no table holds -- doubles that fail a page named as Validation named it before, as
  Aurora DSQL did: tables, keys and foreign keys named with a ``%`` match on a MySQL and a
  PostgreSQL source (an inheritance parent too), orphans are counted, the cut-over orphan
  count agrees.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any, Optional

import psycopg
import pymysql
import pytest
from psycopg import adapt, pq, sql
from psycopg._queries import PostgresQuery
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from dsql_migrator.core import validation_sql
from dsql_migrator.core import validator as v
from dsql_migrator.core.introspector import (
    ReadOnlySourceError,
    _default_engine_factory,
    install_read_only_guard,
)
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    ValidationMode,
)
from dsql_migrator.core.source_dialect.mysql import MySQLSourceDialect
from dsql_migrator.core.target_introspector import sync_identity_sequences
from dsql_migrator.core.validation_sql import (
    _quote_mysql_identifier,
    _quote_mysql_table,
    build_mysql_checksum_sql,
    build_mysql_composite_boundary_sql,
    build_mysql_composite_page_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
    build_mysql_pk_first_page_sql,
    build_mysql_pk_next_page_sql,
    build_mysql_pk_token_sql,
    build_orphan_count_sql,
    build_orphan_listing_sql,
    build_pg_checksum_sql,
    build_pg_composite_orphan_page_sql,
    build_pg_composite_page_sql,
    build_pg_count_sql,
    build_pg_orphan_page_first_sql,
    build_pg_orphan_page_next_sql,
    build_pg_page_checksum_first_sql,
    build_pg_page_checksum_next_sql,
    build_pg_pk_first_page_sql,
    build_pg_pk_next_page_sql,
    build_pg_pk_token_sql,
)
from dsql_migrator.core.validator_postgres import PgSourceConnection
from dsql_migrator.ui.data_migration._full_load_engine import _count_orphans
from tests.test_g138_composite_validation import (
    MySqlDouble,
    PgDouble,
    _PgCursor,
    _PgSqla,
    _Table,
)

_NOWHERE = b"host=/nonexistent-dsql-migrator-percent-names-test port=1"
_SOURCE = SourceConnectionConfig(host="db.example.com", database="app")
_PG_SOURCE = SourceConnectionConfig(
    host="db.example.com", database="app", source_type=SourceType.POSTGRES
)
_TARGET = TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")

# A '%' where psycopg's placeholder read would see one: a stray '%o', a '%s' that is a
# placeholder, an escaped '%%', a lone '%' before the closing quote, a named placeholder,
# and the binary / text placeholders '%b' / '%t'.
_PERCENT_NAMES = ("pct%off", "a%sb", "x%%y", "%", "%(x)s", "100%", "%b%t")
# And names whose quoting is not the point, but must not move either.
_OTHER_NAMES = ("MixedCase", 'q"uote', "tick`name", "back\\slash", "ünï 日本語", "sp ace")
_NAMES = _PERCENT_NAMES + _OTHER_NAMES
# The stand-in the expected bytes are built with: special to no layer (psycopg's read,
# libpq's quoting, SQLAlchemy's text(), PyMySQL's formatting and escaping).
_PLAIN = "\x01"


class _SqlState(Exception):
    def __init__(self, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


# ---------------------------------------------------------------------------
# Where each driver would send
# ---------------------------------------------------------------------------


class _OfflinePg(psycopg.Connection):
    """A real psycopg connection whose connect attempt has already failed: its adapters and
    libpq's identifier quoting work, and nothing can be sent. Its cursors are made without
    the connection-state check, and the pool's rollback on return is nothing."""

    def cursor(self, *args: Any, **kwargs: Any) -> psycopg.Cursor:
        return psycopg.Cursor(self)

    def rollback(self) -> None:
        return None

    def commit(self) -> None:
        return None


@pytest.fixture(scope="module")
def offline():
    connection = _OfflinePg(pq.PGconn.connect_start(_NOWHERE))
    try:
        yield connection
    finally:
        connection.close()


def _converted(connection: Any, statement: Any, params: Any) -> PostgresQuery:
    """What ``cursor.execute(statement[, params])`` on a psycopg cursor of ``connection``
    hands libpq: ``_start_query`` makes a fresh ``Transformer`` of the cursor and
    ``_convert_query`` runs the cursor's query class -- which reads the text for
    placeholders only when it is given parameters (``params`` not ``None``)."""
    cursor = psycopg.Cursor(connection)
    pgq = type(cursor)._query_cls(adapt.Transformer(cursor))
    pgq.convert(statement, params)
    return pgq


def _target_sent(connection: Any, statement: Any, params: Any) -> bytes:
    return _converted(connection, statement, params).query


class _PgSourceEngine:
    """A real SQLAlchemy ``postgresql+psycopg`` engine over :class:`_OfflinePg`, with the
    tool's read-only guard, stopped where it would call psycopg's ``cursor.execute``: it
    records the statement and the parameters SQLAlchemy hands psycopg (``None``: none)."""

    def __init__(self, connection: _OfflinePg) -> None:
        self.raw = connection
        self.engine = create_engine(
            "postgresql+psycopg://", creator=lambda: connection, _initialize=False,
            poolclass=StaticPool,
        )
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

    def sent(self, statement: Any, params: Any, *, from_only: Optional[str] = None) -> bytes:
        """The bytes psycopg sends when the PostgreSQL source shim runs ``statement`` --
        through ``FROM ONLY`` reads of ``from_only`` when given, as for an inheritance
        parent."""
        handed = len(self.handed)
        with self.engine.connect() as connection:
            source: Any = PgSourceConnection(connection)
            if from_only is not None:
                source = v._OwnRowsSource(source, from_only)
            source.cursor().execute(statement, params)
        assert len(self.handed) == handed + 1
        handed_sql, handed_params = self.handed[-1]
        return _target_sent(self.raw, handed_sql, handed_params)


@pytest.fixture(scope="module")
def pg_source(offline):
    harness = _PgSourceEngine(offline)
    try:
        yield harness
    finally:
        harness.engine.dispose()


class _CapturingCursor(pymysql.cursors.Cursor):
    """PyMySQL's own cursor, stopped where it would write to the socket: ``_query`` gets
    the string ``execute`` sends (``mogrify``: the query %-formatted with the escaped
    arguments). Every statement answers one row, ``(0,)``."""

    def _query(self, q: str) -> int:
        self.connection.sent.append(q)
        self.description = (("c0", 8, None, None, None, None, None),)
        self._rows = ((0,),)
        self.rownumber = 0
        self.rowcount = 1
        return 1


class _OfflineMySql(pymysql.connections.Connection):
    """A PyMySQL connection that never connects (``defer_connect``), so nothing is sent.
    ``server_status`` 0 is a server's default (backslash escapes on), which the string
    escaping reads."""

    def __init__(self) -> None:
        super().__init__(defer_connect=True, cursorclass=_CapturingCursor)
        self.server_status = 0
        self.sent: list[str] = []

    def rollback(self) -> None:
        return None

    def commit(self) -> None:
        return None


class _MySqlSourceEngine:
    """A real SQLAlchemy ``mysql+pymysql`` engine over :class:`_OfflineMySql`, with the
    tool's read-only guard: ``sent`` is the string PyMySQL sends for a statement."""

    def __init__(self) -> None:
        self.raw = _OfflineMySql()
        self.engine = create_engine(
            "mysql+pymysql://", creator=lambda: self.raw, _initialize=False,
            poolclass=StaticPool,
        )
        install_read_only_guard(self.engine)

    def sent(self, statement: Any, params: Any = None) -> str:
        with self.engine.connect() as connection:
            if params is None:
                connection.execute(statement)
            else:
                connection.execute(statement, params)
        return self.raw.sent[-1]


@pytest.fixture(scope="module")
def mysql_source():
    harness = _MySqlSourceEngine()
    try:
        yield harness
    finally:
        harness.engine.dispose()


def test_the_harnesses_stop_where_the_drivers_send(offline, pg_source, mysql_source) -> None:
    # The model of execute(): if psycopg moved its query class, it would lie.
    assert psycopg.Cursor._query_cls is PostgresQuery
    # The harness engines are the tool's own dialects, configured as its engines are.
    for config, harness in ((_SOURCE, mysql_source), (_PG_SOURCE, pg_source)):
        tool = _default_engine_factory(config)
        try:
            assert type(tool.dialect) is type(harness.engine.dialect)
            assert tool.dialect.paramstyle == harness.engine.dialect.paramstyle
        finally:
            tool.dispose()
    # SQLAlchemy hands psycopg an EMPTY mapping for a statement without parameters -- which
    # psycopg reads for placeholders -- unless told there are none (no_parameters).
    with pg_source.engine.connect() as connection:
        connection.exec_driver_sql('SELECT 1 FROM "pct%off"')
        assert pg_source.handed[-1][1] == {} and pg_source.handed[-1][1] is not None
        connection.exec_driver_sql(
            'SELECT 1 FROM "pct%off"', execution_options={"no_parameters": True}
        )
        assert pg_source.handed[-1] == ('SELECT 1 FROM "pct%off"', None)
        connection.exec_driver_sql("SELECT %(last)s", {"last": "5"})
        assert pg_source.handed[-1] == ("SELECT %(last)s", {"last": "5"})
        # The shim sends a statement without parameters under no_parameters, and the
        # read-only guard (Property 1) still sees it first.
        handed = len(pg_source.handed)
        with pytest.raises(ReadOnlySourceError):
            PgSourceConnection(connection).cursor().execute(
                sql.SQL("DELETE FROM {}").format(sql.Identifier("s", "pct%off"))
            )
        assert len(pg_source.handed) == handed
    # PyMySQL sends what mogrify makes; SQLAlchemy's text() doubles each '%' for it, with
    # parameters or without, and PyMySQL's formatting halves them again.
    cursor = mysql_source.raw.cursor()
    cursor.execute("SELECT `x%%y` WHERE a = %(a)s", {"a": "it's"})
    assert mysql_source.raw.sent[-1] == cursor.mogrify("SELECT `x%%y` WHERE a = %(a)s",
                                                       {"a": "it's"})
    assert mysql_source.sent(text("SELECT COUNT(*) FROM `pct%off`")) == (
        "SELECT COUNT(*) FROM `pct%off`"
    )


# ---------------------------------------------------------------------------
# Every statement, every name, both engines: the bytes the driver sends
# ---------------------------------------------------------------------------


def _typed_columns(name: str) -> list[ColumnDef]:
    """``name`` as a column of each checksum render family (plain, numeric, temporal,
    binary), so each family's expression names it."""
    return [
        ColumnDef(name=name, mysql_type="varchar(20)"),
        ColumnDef(name=name + "#n", mysql_type="DECIMAL(10,2)"),
        ColumnDef(name=name + "#t", mysql_type="DATETIME(6)"),
        ColumnDef(name=name + "#b", mysql_type="BLOB"),
    ]


def _keyed_table(table_name: str, key: "tuple[str, ...]", other: str) -> TableDef:
    columns = [ColumnDef(name=k, mysql_type="int") for k in key] + _typed_columns(other)
    return TableDef(name=table_name, columns=columns, primary_key=list(key))


def _single_key_variants(name: str) -> "list[tuple[TableDef, str]]":
    """``name`` as the schema, the table, the key column and the other columns."""
    return [
        (_keyed_table(f"{name}.t", ("id",), "c"), "id"),
        (_keyed_table(f"s.{name}", ("id",), "c"), "id"),
        (_keyed_table("s.t", (name,), "c"), name),
        (_keyed_table("s.t", ("id",), name), "id"),
    ]


def _composite_variants(name: str) -> "list[TableDef]":
    return [
        _keyed_table(f"{name}.t", ("k1", "k2"), "c"),
        _keyed_table(f"s.{name}", ("k1", "k2"), "c"),
        _keyed_table("s.t", (name, "k2"), "c"),
        _keyed_table("s.t", ("k1", name), "c"),
        _keyed_table("s.t", ("k1", "k2"), name),
    ]


def _foreign_key_variants(name: str) -> "list[tuple[str, ForeignKeyDef]]":
    """``name`` as the child table, the foreign-key column, the parent table and its key."""

    def fk(columns, parent, referenced):
        return ForeignKeyDef(name="fk", columns=columns, referenced_table=parent,
                             referenced_columns=referenced)

    return [
        (f"s.{name}", fk(["p_id"], "s.p", ["id"])),
        ("s.c", fk([name], "s.p", ["id"])),
        ("s.c", fk(["p_id"], f"{name}.p", ["id"])),
        ("s.c", fk(["p_id"], f"s.{name}", ["id"])),
        ("s.c", fk(["p_id"], "s.p", [name])),
        ("s.c", fk([name, "q"], "s.p", ["id", name + "#2"])),
    ]


_T, _S, _O = "target", "source", "source FROM ONLY"


def _pg_reads(name: str) -> list:
    """Every PostgreSQL / Aurora DSQL statement Validation and cut over run, with ``name``
    in each place a name can be, and the parameters its reader passes (``None``: run
    without). ``sides``: where it runs -- the Aurora DSQL target, a PostgreSQL source, and a
    PostgreSQL inheritance parent's ``FROM ONLY`` reads (whose table is the statement's)."""
    reads: list = []

    def add(label, statement, params, *sides, table=None):
        reads.append((label, statement, params, sides, table))

    last, keys = {"last": "5"}, {"last_0": "5", "last_1": "x"}
    for table, key in _single_key_variants(name):
        other = table.columns[-1].name.rsplit("#", 1)[0]
        n = table.name
        add("pk page", build_pg_pk_first_page_sql(table, key, 5), None, _T, _S, _O, table=n)
        add("pk page after", build_pg_pk_next_page_sql(table, key, 5), last, _T, _S, _O, table=n)
        add("pk page + proof", build_pg_pk_first_page_sql(table, key, 5, (other,)), None, _T)
        add("pk page after + proof", build_pg_pk_next_page_sql(table, key, 5, (other,)),
            last, _T)
        add("count", build_pg_count_sql(n), None, _T, _S, _O, table=n)
        add("count + proof", build_pg_count_sql(n, (other,)), None, _T)
        for sip in (False, True):
            sides = (_T, _S, _O) if sip else (_T,)
            add("checksum", build_pg_checksum_sql(table, sip), None, *sides, table=n)
            add("pk tokens", build_pg_pk_token_sql(table, key, 5, sip), None, *sides, table=n)
            add("checksum page", build_pg_page_checksum_first_sql(table, key, 5, sip), None,
                *sides, table=n)
            add("checksum page after", build_pg_page_checksum_next_sql(table, key, 5, sip),
                last, *sides, table=n)
    for table in _composite_variants(name):
        other, key, n = table.columns[-4].name, tuple(table.primary_key), table.name
        for checksum in (False, True):
            for proof in ((), (other,)):
                for first, params in ((True, None), (False, keys)):
                    add(f"composite page first={first} checksum={checksum} proof={bool(proof)}",
                        build_pg_composite_page_sql(table, key, 5, checksum=checksum,
                                                    nonnull_columns=proof, first=first),
                        params, _T)
        for first, params in ((True, None), (False, keys)):
            add(f"source composite checksum page first={first}",
                build_pg_composite_page_sql(table, key, 5, True, checksum=True, first=first,
                                            row_value=True),
                params, _S, _O, table=n)
    for child, fk in _foreign_key_variants(name):
        add("orphan count", build_orphan_count_sql(child, fk), None, _T)
        add("orphan page", build_pg_orphan_page_first_sql(child, fk, "id", 5), None, _T)
        add("orphan page after", build_pg_orphan_page_next_sql(child, fk, "id", 5), last, _T)
        for first, params in ((True, None), (False, keys)):
            add(f"composite orphan page first={first}",
                build_pg_composite_orphan_page_sql(child, fk, ("id", "v"), 5, first=first),
                params, _T)
    return reads


def _pg_sent(side: str, statement: Any, params: Any, table: Optional[str], offline: Any,
             pg_source: _PgSourceEngine) -> bytes:
    if side == _T:
        return _target_sent(offline, statement, params)
    return pg_source.sent(statement, params, from_only=table if side == _O else None)


@pytest.mark.parametrize("name", _NAMES)
def test_every_postgresql_read_sends_each_name_as_itself(name, offline, pg_source) -> None:
    # The expected bytes: the same statement built with each '%' of the name as a byte no
    # layer reads (\x01), sent the same way, and the '%' put back -- the statement every
    # name without a '%' gets, every name quoted as libpq quotes it.
    reads, twins = _pg_reads(name), _pg_reads(name.replace("%", _PLAIN))
    assert [r[0] for r in reads] == [t[0] for t in twins]
    quoted, named, checked = sql.Identifier(name).as_bytes(offline), 0, 0
    for (label, statement, params, sides, table), (_, twin, _, _, twin_table) in zip(
        reads, twins
    ):
        for side in sides:
            expected = _pg_sent(side, twin, params, twin_table, offline, pg_source)
            assert b"%" not in expected  # so every '%' below is one a name put back
            sent = _pg_sent(side, statement, params, table, offline, pg_source)
            assert sent == expected.replace(_PLAIN.encode(), b"%"), (label, side)
            if side == _O:
                assert b" FROM ONLY " in sent, label
            named += quoted in sent
            checked += 1
    assert checked >= 200 and named >= checked * 3 // 4


def test_the_three_names_of_the_known_issue_fail_as_it_says(offline) -> None:
    # Before: a page after a cursor read the quoted names for placeholders.
    table = _keyed_table("app.t", ("id",), "c")
    for name, error in (("pct%off", "got '%o'"), ("a%sb", "cannot be mixed")):
        unsafe = sql.SQL("SELECT {} FROM t WHERE id > {}").format(
            sql.Identifier(name), sql.Placeholder("last"))
        with pytest.raises(psycopg.ProgrammingError, match=error):
            _target_sent(offline, unsafe, {"last": "1"})
    unsafe = sql.SQL("SELECT {} FROM t WHERE id > {}").format(
        sql.Identifier("x%%y"), sql.Placeholder("last"))
    assert b'"x%y"' in _target_sent(offline, unsafe, {"last": "1"})  # silently another
    # Now: the same names reach Aurora DSQL as themselves, on the page after the cursor.
    for name in ("pct%off", "a%sb", "x%%y"):
        statement = build_pg_page_checksum_next_sql(
            table.model_copy(update={"columns": [*table.columns,
                                                 ColumnDef(name=name, mysql_type="int")]}),
            "id", 5,
        )
        assert sql.Identifier(name).as_bytes(offline) in _target_sent(
            offline, statement, {"last": "1"})


def test_param_safe_doubles_only_the_names(offline) -> None:
    statement = sql.SQL("SELECT {}, {} FROM {} WHERE {} > {} LIMIT {}").format(
        sql.Identifier("a%sb"), sql.SQL("'100%'"), sql.Identifier("s", "x%%y"),
        sql.Identifier("plain"), sql.Placeholder("last"), sql.Literal(5),
    )
    safe = validation_sql._param_safe(statement)
    # Each name's '%' doubled, quoted as Identifier quotes it; SQL text, literals and
    # placeholders untouched, and a statement already param-safe stays as it is.
    assert safe.as_string(None) == (
        """SELECT "a%%sb", '100%' FROM "s"."x%%%%y" WHERE "plain" > %(last)s LIMIT 5"""
    )
    assert validation_sql._param_safe(safe).as_string(None) == safe.as_string(None)
    assert validation_sql._param_safe(sql.Identifier('q"%')).as_bytes(None) == b'"q""%%"'
    # A first page carries no placeholder and is run without parameters: names as they are.
    table = _keyed_table("s.pct%off", ("a%sb",), "c")
    assert '"s"."pct%off"' in build_pg_pk_first_page_sql(table, "a%sb", 5).as_string(None)
    assert '"s"."pct%%off"' in build_pg_pk_next_page_sql(table, "a%sb", 5).as_string(None)


def test_the_orphan_listing_shown_to_the_operator_names_each_table_as_itself() -> None:
    # Shown for psql, never run by the tool: not param-safe, even with its window's
    # placeholders.
    fk = ForeignKeyDef(name="fk", columns=["p%id"], referenced_table="app.pct%off",
                       referenced_columns=["a%sb"])
    for after in (None, [sql.Placeholder("after_0")], [sql.Literal(7)]):
        listing = build_orphan_listing_sql(
            "app.x%%y", fk, ["id%"], page_size=100, after=after
        ).as_string(None)
        assert '"app"."x%%y"' in listing and '"app"."pct%off"' in listing
        assert '"p%id"' in listing and '"x%%%%y"' not in listing


def _mysql_reads(name: str) -> list:
    """Every statement Validation runs on a MySQL source, with ``name`` in each place a name
    can be, and the parameters its reader passes (``None``: none)."""
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
            ("primary index", v._MYSQL_PRIMARY_INDEX_SQL.format(schema=":schema"),
             {"schema": schema, "name": bare}),
            ("column types", v._MYSQL_COLUMN_TYPES_SQL, {"schema": schema, "name": bare}),
        ]
    for table in _composite_variants(name):
        lead = dialect.quote_identifier(table.primary_key[0])
        where, params = dialect.keyset_after_sql(table, (7, 8), {})
        reads += [
            ("composite page", build_mysql_composite_page_sql(table), {"page": 5}),
            ("composite page after", build_mysql_composite_page_sql(table, where),
             {**params, "page": 5}),
            ("composite boundary", build_mysql_composite_boundary_sql(table), {"off": 4}),
            ("composite boundary after", build_mysql_composite_boundary_sql(table, where),
             {**params, "off": 4}),
            ("member page", build_mysql_composite_page_sql(table, f"{lead} = :last_0"),
             {"last_0": "new", "page": 5}),
        ]
    return reads


@pytest.mark.parametrize("name", _NAMES)
def test_every_mysql_source_read_sends_each_name_as_itself(name, mysql_source) -> None:
    # Already exact before the fix, and pinned: SQLAlchemy's text() doubles every '%' for
    # PyMySQL's pyformat, and PyMySQL's %-formatting -- which runs with an empty mapping too
    # -- halves them again; so a name, and DATE_FORMAT's '%Y-%m-%d', arrive as written.
    swapped = name.replace("%", _PLAIN)
    reads, twins = _mysql_reads(name), _mysql_reads(swapped)
    quoted, named = _quote_mysql_identifier(name), 0
    for (label, statement, params), (_, twin, twin_params) in zip(reads, twins):
        expected = mysql_source.sent(text(twin), twin_params)
        sent = mysql_source.sent(text(statement), params)
        assert sent == expected.replace(_PLAIN, "%"), label
        named += quoted in sent
        if "DATE_FORMAT(" in sent:
            assert "'%Y-%m-%d %H:%i:%s.%f'" in sent, label
    assert named >= len(reads) // 2
    # _source_count runs its own COUNT(*): the same.
    for table_name in (f"{name}.t", f"s.{name}"):
        with mysql_source.engine.connect() as connection:
            assert v._source_count(connection, table_name) == 0
        assert mysql_source.raw.sent[-1] == f"SELECT COUNT(*) FROM {_quote_mysql_table(table_name)}"


def test_the_identity_resync_names_each_table_and_column_as_itself(offline) -> None:
    # Already exact (every statement with a name in it runs without parameters, the catalog
    # reads bind the names), and pinned.
    class _IdentityTarget:
        def __init__(self) -> None:
            self.sent: list[bytes] = []
            self._rows: list = []

        def cursor(self) -> "_IdentityTarget":
            return self

        def execute(self, statement: Any, params: Any = None) -> None:
            query = _target_sent(offline, statement, params)
            self.sent.append(query)
            if b"information_schema.columns" in query:
                self._rows = [("a%sb",)]
            elif query.startswith(b"SELECT MAX("):
                self._rows = [(23,)]
            elif b"pg_get_serial_sequence" in query:
                self._rows = [('app."pct%off_a%sb_seq"',)]
            elif b"last_value, is_called" in query:
                self._rows = [(1, False)]
            else:
                self._rows = []

        def fetchall(self) -> list:
            return list(self._rows)

        def fetchone(self) -> Any:
            return self._rows[0] if self._rows else None

        def close(self) -> None:
            return None

    target = _IdentityTarget()
    assert sync_identity_sequences(["app.pct%off"], connection_factory=lambda: target) == {
        "app.pct%off": 24
    }
    assert b'SELECT MAX("a%sb") FROM "app"."pct%off"' in target.sent
    assert b'SELECT last_value, is_called FROM "app"."pct%off_a%sb_seq"' in target.sent
    assert b'ALTER TABLE "app"."pct%off" ALTER COLUMN "a%sb" RESTART WITH 24' in target.sent


# ---------------------------------------------------------------------------
# End to end: Validator.validate and the cut-over orphan count on the doubles
# ---------------------------------------------------------------------------


_QUOTED_PG = re.compile(r'"((?:[^"]|"")*)"')
_QUOTED_MYSQL = re.compile(r"`((?:[^`]|``)*)`")
_ALIAS = re.compile(r"ck_(?:k|nn)\d+")


def _known_names(double: Any) -> set[str]:
    names: set[str] = set()
    for table in double.tables.values():
        names.update(table.name.split(".", 1))
        names.update(table.columns)
        names.update(table.key)
    return names


def _refuse_unknown_names(double: Any, statement: str, quoted: "re.Pattern[str]",
                          quote: str) -> None:
    """Refuse a statement naming an object no table of ``double`` has, as a server does."""
    known = _known_names(double)
    for found in quoted.findall(statement):
        name = found.replace(quote * 2, quote)
        if name not in known and not _ALIAS.fullmatch(name):
            raise _SqlState("42703", f"column {quote}{name}{quote} does not exist")


def _server_reads(connection: Any, double: Any, statement: Any, params: Any,
                  sent: list) -> str:
    """What the server reads of ``statement``: converted by psycopg's own query class with
    ``params`` (raising as psycopg raises), recorded in ``sent`` with whether it ran with
    parameters, refused for a name no table has, and handed back as text with each ``$n``
    written as ``%(name)s`` again -- the form the double reads its bounds from."""
    pgq = _converted(connection, statement, params)
    sent.append((pgq.query, params is not None))
    query = pgq.query.decode()
    _refuse_unknown_names(double, query, _QUOTED_PG, '"')
    order = getattr(pgq, "_order", None)
    if not order:
        return query
    return re.sub(r"\$(\d+)", lambda m: f"%({order[int(m[1]) - 1]})s", query)


class _WireCursor(_PgCursor):
    def execute(self, statement: Any, params: Any = None) -> None:
        owner = self._owner
        self._result = owner.resolve(
            _server_reads(owner.offline, owner, statement, params, owner.sent), params
        )


class _WireTarget(PgDouble):
    """test_g138's Aurora DSQL double reached through psycopg cursors: each statement is
    converted as psycopg converts it, with the parameters its reader passed."""

    def __init__(self, connection: Any, *tables: _Table, **kwargs: Any) -> None:
        super().__init__(*tables, **kwargs)
        self.offline = connection
        self.sent: list[tuple[bytes, bool]] = []

    def cursor(self) -> _WireCursor:
        return _WireCursor(self)


class _WirePgSource(_PgSqla):
    """test_g138's PostgreSQL source double behind the real shim: ``exec_driver_sql`` hands
    psycopg what SQLAlchemy hands it (the parameters; none under ``no_parameters``; else an
    EMPTY mapping, pinned above), and the shim renders against the offline connection as
    against a live one."""

    def __init__(self, connection: Any, double: PgDouble, **kwargs: Any) -> None:
        super().__init__(double, **kwargs)
        self.connection = SimpleNamespace(driver_connection=connection)
        self.offline = connection
        self.sent: list[tuple[bytes, bool]] = []

    def exec_driver_sql(self, sql_text: str, params: Any = None,
                        execution_options: Any = None) -> Any:
        no_parameters = bool((execution_options or {}).get("no_parameters"))
        handed = params if params else (None if no_parameters else {})
        return super().exec_driver_sql(
            _server_reads(self.offline, self.double, sql_text, handed, self.sent), params
        )


class _WireMySqlSource(MySqlDouble):
    """test_g138's MySQL source double, every statement also sent through SQLAlchemy and
    PyMySQL (:class:`_MySqlSourceEngine`) and refused for a name no table has."""

    def __init__(self, harness: _MySqlSourceEngine, *tables: _Table) -> None:
        super().__init__(*tables)
        self._harness = harness
        self.sent: list[str] = []

    def execute(self, statement: object, parameters: object = None) -> Any:
        sent = self._harness.sent(statement, parameters)
        self.sent.append(sent)
        _refuse_unknown_names(self, sent, _QUOTED_MYSQL, "`")
        return super().execute(statement, parameters)


_FK = ForeignKeyDef(name="fk%parent", columns=["p%id"], referenced_table="app.pct%off",
                    referenced_columns=["a%sb"])


def _percent_tables(*, pg_types: bool = False, orphans: int = 0) -> "list[_Table]":
    """A table keyed by an integer named ``a%sb``, one keyed by ``(k%1, k%%2)`` and a child
    of the first keyed by ``id%`` -- every name, the tables' own and their schema-qualified
    ones, holding a ``%`` -- and the composite table a child of the first too. ``orphans``
    of the child's rows point at no parent row."""
    integer, text_type = ("integer", "text") if pg_types else ("int", "varchar(20)")
    parent = _Table(
        "app.pct%off", ("a%sb",),
        [{"a%sb": i, "x%%y": f"v{i}", "Mixed%Case": i * 3} for i in range(1, 24)],
        types={"a%sb": integer, "x%%y": text_type, "Mixed%Case": integer},
    )
    composite = _Table(
        "app.x%%y", ("k%1", "k%%2"),
        [{"k%1": i // 4, "k%%2": i % 4, "ünï%": f"n{i}", "p%id": 1 + i % 23}
         for i in range(23)],
        types={"k%1": integer, "k%%2": integer, "ünï%": text_type, "p%id": integer},
    )
    child = _Table(
        "app.a%sb", ("id%",),
        [{"id%": i, "p%id": 900 + i if i <= orphans else 1 + i % 23} for i in range(1, 31)],
        types={"id%": integer, "p%id": integer},
    )
    return [parent, composite, child]


def _definitions(tables: "list[_Table]") -> "list[TableDef]":
    parent, composite, child = tables
    return [
        parent.tabledef(),
        composite.tabledef().model_copy(update={"foreign_keys": [_FK]}),
        child.tabledef().model_copy(update={"foreign_keys": [_FK]}),
    ]


_PROOF = {"app.pct%off": ["Mixed%Case"], "app.x%%y": ["ünï%"]}


def _run(source: Any, target: Any, tables: "list[_Table]", config: SourceConnectionConfig):
    return v.Validator(
        source_engine_factory=lambda _cfg: source,
        target_connection_factory=lambda _cfg: target,
        reconcile_page_size=4,
        null_proof_columns=_PROOF,
    ).validate(
        config, _TARGET, _definitions(tables), ValidationMode.CHECKSUM, reconcile=True,
        check_orphans=True,
    )


def _assert_matched(report: Any, target: _WireTarget) -> None:
    assert report.is_match is True, [(i.table, i.error, i.orphan_check_error)
                                     for i in report.items]
    assert all(i.matched and i.checksum_match for i in report.items)
    assert [i.target_row_count for i in report.items] == [23, 23, 30]
    assert [i.reconcile.consistent for i in report.items if i.reconcile] == [True, True]
    assert [i.proof_state for i in report.items[:2]] == ["complete", "complete"]
    assert report.orphan_findings == [] and report.orphan_unchecked == []
    # Pages after a cursor ran, with their parameters, on every table.
    after = [query for query, with_params in target.sent if with_params]
    for table in (b'"app"."pct%off"', b'"app"."x%%y"', b'"app"."a%sb"'):
        assert any(table in query for query in after), table


def test_a_mysql_source_validates_tables_and_keys_named_with_a_percent(
    offline, mysql_source
) -> None:
    tables = _percent_tables()
    source, target = _WireMySqlSource(mysql_source, *tables), _WireTarget(offline, *tables)
    _assert_matched(_run(source, target, tables, _SOURCE), target)
    pages = [s for s in source.sent if "page_tok" in s or "ck_tok" in s]
    assert pages and all("`app`." in s for s in pages)
    assert any("`app`.`x%%y`" in s and "`k%%2` > " in s for s in source.sent)


@pytest.mark.parametrize("inheritance_parent", [False, True])
def test_a_postgresql_source_validates_tables_and_keys_named_with_a_percent(
    offline, inheritance_parent
) -> None:
    tables = _percent_tables(pg_types=True)
    source = _WirePgSource(offline, PgDouble(*tables), inheritance_parent=inheritance_parent)
    target = _WireTarget(offline, *tables)
    _assert_matched(_run(source, target, tables, _PG_SOURCE), target)
    first = [query for query, with_params in source.sent if not with_params]
    after = [query for query, with_params in source.sent if with_params]
    assert first and after  # first pages (no parameters) and pages after a cursor
    if inheritance_parent:
        assert all(b" FROM ONLY " in query for query, _ in source.sent)


def test_orphans_of_a_foreign_key_named_with_a_percent_are_counted(offline, mysql_source):
    tables = _percent_tables(orphans=2)
    source, target = _WireMySqlSource(mysql_source, *tables), _WireTarget(offline, *tables)
    report = _run(source, target, tables, _SOURCE)
    assert [(f.table, f.foreign_key, f.orphan_count) for f in report.orphan_findings] == [
        ("app.a%sb", "fk%parent", 2)
    ]
    assert report.is_match is False
    assert all(i.matched for i in report.items)  # the data itself is equal


@pytest.mark.parametrize("child, expected", [("app.a%sb", 3), ("app.x%%y", 0)])
def test_the_cut_over_orphan_count_reads_a_percent_name_page_after_page(
    offline, child, expected
) -> None:
    # A one-column key (id%) and a composite one (k%1, k%%2), each read by its live key.
    tables = _percent_tables(orphans=3)
    target = _WireTarget(offline, *tables)
    assert _count_orphans(lambda: target, child, _FK, page_size=4) == expected
    after = [query for query, with_params in target.sent
             if with_params and query.startswith(b"WITH pg AS MATERIALIZED")]
    assert len(after) >= 5 and all(b'LEFT JOIN "app"."pct%off" AS p' in q for q in after)


def test_the_wire_doubles_fail_a_page_named_as_before_as_aurora_dsql_did(offline) -> None:
    # Guard the guard: the end-to-end runs above pass only over doubles that read each
    # statement as psycopg does. Named as Validation named a page after a cursor before (plain
    # sql.Identifier, run with its parameters) and handed an EMPTY mapping as SQLAlchemy hands
    # a PostgreSQL source's statement without parameters (pinned above), a '%' fails the read
    # or names a column no table has -- as on Aurora DSQL -- and nothing reaches the source.
    def after_cursor(table: str, key: str) -> sql.Composed:
        return sql.SQL("SELECT COUNT(*) FROM {} WHERE {} > {}").format(
            sql.Identifier(*table.split(".", 1)), sql.Identifier(key), sql.Placeholder("last")
        )

    target = _WireTarget(offline, *_percent_tables())
    with pytest.raises(psycopg.ProgrammingError):  # '%o' is no placeholder
        target.cursor().execute(after_cursor("app.pct%off", "a%sb"), {"last": "1"})
    with pytest.raises(_SqlState, match='"x%y" does not exist'):  # x%%y read as x%y
        target.cursor().execute(after_cursor("app.x%%y", "k%%2"), {"last": "1"})
    source = _WirePgSource(offline, PgDouble(*_percent_tables(pg_types=True)))
    with pytest.raises(psycopg.ProgrammingError):
        source.exec_driver_sql('SELECT COUNT(*) FROM "app"."pct%off"')
    assert source.double.executed == []
