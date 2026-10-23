# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G148: the source's server settings are read once at Evaluation, and the tool's own MySQL
source sessions read with a neutral sql_mode.

Nothing read the source's server settings, so Evaluation could not say what the application
runs under, and every tool session inherited the parameter group's sql_mode: under
ANSI_QUOTES, SHOW CREATE re-printed a view's body with double-quoted identifiers, which the
converter (like MySQL's default mode) read as string literals -- a view returning constants,
with no warning.

* The pin: one SET in the shared init_command pins time_zone '+00:00' and sql_mode
  'ALLOW_INVALID_DATES' (G154; '' until then) on every MySQL source session, through every
  engine the tool builds; through the real
  SQLAlchemy MySQL dialect over a PyMySQL-like double whose parameter group sets ANSI_QUOTES,
  a view body is read backtick-quoted (and, as a control, double-quoted without the pin).
* The capture: MySQL's GLOBAL values in one SHOW GLOBAL VARIABLES; PostgreSQL's TimeZone over
  a probe connection without the UTC startup option, plus its ALTER DATABASE / ALTER ROLE
  defaults; carried on SourceInventory.server_settings (left out of the payload while empty),
  best effort.
* The findings: SQL_MODE, EXPLICIT_DEFAULTS_FOR_TIMESTAMP and AUTO_INCREMENT_STEP, database
  level, MANUAL; the case folding and collation settings are recorded only (the time zone is
  graded since G149: tests/test_g149_source_time_zone.py).
* The Evaluation screen lists the settings under the source counts; the manual (en / ko /
  ja) says what is read and why, and that the tool's sessions read with a neutral sql_mode.

Doubles only: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

import re
import types
from pathlib import Path
from typing import Optional

import pytest
from sqlalchemy import create_engine

from dsql_migrator.core import introspector
from dsql_migrator.core.assessor import (
    AUTO_INCREMENT_STEP_RULE_ID,
    IMPLICIT_TIMESTAMP_RULE_ID,
    SQL_MODE_RULE_ID,
    ZERO_DATES_RULE_ID,
    CompatibilityAssessor,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.introspector import (
    SourceIntrospector,
    _default_engine_factory,
    install_read_only_guard,
    is_write_or_ddl,
    source_engine_kwargs,
)
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect import postgres as pg_dialect_module
from dsql_migrator.core.source_dialect.mysql import MYSQL_SERVER_SETTINGS
from dsql_migrator.ui import connect
from dsql_migrator.ui.connect import make_source_engine_factory

PIN = "SET time_zone = '+00:00', sql_mode = 'ALLOW_INVALID_DATES'"
# The session sql_mode the pin leaves: no flag that changes how SQL parses (G154).
SESSION_MODE = "ALLOW_INVALID_DATES"
# What an RDS / Aurora parameter group might set: MySQL 8.0's default plus ANSI_QUOTES and
# PIPES_AS_CONCAT.
ANSI_PARAMETER_GROUP = (
    "ANSI_QUOTES,PIPES_AS_CONCAT,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,"
    "NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
MYSQL_80_DEFAULT = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)


# ---------------------------------------------------------------------------
# A MySQL server double (PyMySQL-like DBAPI) for the real SQLAlchemy MySQL dialect
# ---------------------------------------------------------------------------


def _view_body(quote: str) -> str:
    """SHOW CREATE of the view ``v_open``, its identifiers quoted the way the session's
    sql_mode makes MySQL print them (it prints the parsed view again for every SHOW)."""
    q = quote
    return (
        f"CREATE ALGORITHM=UNDEFINED DEFINER={q}admin{q}@{q}%{q} SQL SECURITY DEFINER "
        f"VIEW {q}v_open{q} AS select {q}orders{q}.{q}id{q} AS {q}id{q},"
        f"{q}orders{q}.{q}status{q} AS {q}status{q} from {q}orders{q} "
        f"where ({q}orders{q}.{q}status{q} = 'open')"
    )


class _MySqlServer:
    """A DBAPI module double: one MySQL server with the GLOBAL values of ``global_vars``.

    Like PyMySQL, ``connect()`` runs the ``init_command`` it is given before returning. A
    session starts from the GLOBAL time_zone and sql_mode, a ``SET`` changes the session's,
    ``SELECT @@x`` reads the session's and ``SHOW GLOBAL VARIABLES`` the GLOBAL ones. The
    database ``app`` holds one view, ``v_open``, whose SHOW CREATE quotes identifiers with
    double quotes while the SESSION's sql_mode has ANSI_QUOTES. Every statement is recorded
    with the session sql_mode it ran under."""

    def __init__(self, global_vars: Optional[dict] = None) -> None:
        self.global_vars = {
            "sql_mode": ANSI_PARAMETER_GROUP,
            "time_zone": "SYSTEM",
            "system_time_zone": "UTC",
            "explicit_defaults_for_timestamp": "ON",
            "auto_increment_increment": "1",
            "auto_increment_offset": "1",
            "lower_case_table_names": "0",
            "character_set_server": "utf8mb4",
            "collation_server": "utf8mb4_0900_ai_ci",
            "default_collation_for_utf8mb4": "utf8mb4_0900_ai_ci",
            "div_precision_increment": "4",
            "group_concat_max_len": "1024",
            "transaction_isolation": "REPEATABLE-READ",
            **(global_vars or {}),
        }
        self.log: list[tuple[str, str]] = []
        self.connect_kwargs: list[dict] = []
        server = self

        class Error(Exception):
            pass

        class Cursor:
            def __init__(self, session: "Session") -> None:
                self._session, self._rows = session, []
                self.description, self.rowcount = None, -1

            def _result(self, rows: list, width: int) -> None:
                self._rows = list(rows)
                self.description = [(f"c{i}", 253) + (None,) * 5 for i in range(width)]
                self.rowcount = len(self._rows)

            def execute(self, statement: str, parameters: object = None) -> None:
                session = self._session
                server.log.append((statement, session.vars["sql_mode"]))
                self.description, self._rows, self.rowcount = None, [], 0
                text_ = " ".join(statement.split())
                if text_.upper().startswith("SET "):
                    for name, value in re.findall(r"(\w+)\s*=\s*'([^']*)'", text_):
                        session.vars[name.lower()] = value
                    return
                read = re.fullmatch(r"SELECT @@(\w+)", text_)
                if read:
                    return self._result([(session.vars.get(read.group(1)),)], 1)
                if text_ == "SELECT VERSION()":
                    return self._result([("8.0.39",)], 1)
                if text_ == "SELECT DATABASE()":
                    return self._result([("app",)], 1)
                if re.fullmatch(r'SHOW FULL TABLES FROM [`"]app[`"]', text_):
                    return self._result([("v_open", "VIEW")], 2)
                if re.fullmatch(r'SHOW CREATE TABLE (?:[`"]app[`"]\.)?[`"]v_open[`"]', text_):
                    quote = '"' if "ANSI_QUOTES" in session.vars["sql_mode"] else "`"
                    body = _view_body(quote)
                    return self._result([("v_open", body, "utf8mb4", "utf8mb4_0900_ai_ci")], 4)
                if text_.startswith("SHOW GLOBAL VARIABLES WHERE Variable_name IN"):
                    wanted = set((parameters or {}).values()) if isinstance(parameters, dict) else set()
                    rows = sorted(
                        (name, value) for name, value in server.global_vars.items()
                        if name in wanted
                    )
                    return self._result(rows, 2)
                # Every other catalog read (information_schema, SHOW GRANTS ...) finds nothing.
                return self._result([], 1)

            def fetchone(self):  # noqa: ANN201
                return self._rows.pop(0) if self._rows else None

            def fetchall(self):  # noqa: ANN201
                rows, self._rows = self._rows, []
                return rows

            def fetchmany(self, size=None):  # noqa: ANN001, ANN201
                size = size or len(self._rows)
                rows, self._rows = self._rows[:size], self._rows[size:]
                return rows

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, **connect_kwargs: object) -> None:
                server.connect_kwargs.append(dict(connect_kwargs))
                self.vars = {
                    name: server.global_vars[name]
                    for name in ("sql_mode", "time_zone", "transaction_isolation",
                                 "lower_case_table_names")
                }
                init_command = connect_kwargs.get("init_command")
                if init_command:  # PyMySQL runs it inside connect()
                    Cursor(self).execute(str(init_command))

            def cursor(self, *_cursor_class: object) -> Cursor:
                return Cursor(self)

            def commit(self) -> None:
                pass

            def rollback(self) -> None:
                pass

            def autocommit(self, _on: bool) -> None:
                pass

            def character_set_name(self) -> str:
                return "utf8mb4"

            def ping(self, *_reconnect: object) -> bool:
                return True

            def close(self) -> None:
                pass

        self.dbapi = types.ModuleType("mysql_server_double")
        self.dbapi.paramstyle = "pyformat"
        self.dbapi.Error = Error
        for name in ("Warning", "InterfaceError", "DatabaseError", "OperationalError",
                     "ProgrammingError", "IntegrityError", "DataError", "InternalError",
                     "NotSupportedError"):
            setattr(self.dbapi, name, type(name, (Error,), {}))
        self.dbapi.connect = lambda *_a, **kw: Session(**kw)

    def modes_at(self, pattern: str) -> list[str]:
        """The session sql_mode each statement matching ``pattern`` ran under."""
        return [mode for sent, mode in self.log if re.search(pattern, sent)]


def _through(monkeypatch: pytest.MonkeyPatch, server: _MySqlServer, module) -> None:  # noqa: ANN001
    """Make ``module``'s create_engine build its engines over ``server``."""
    real = module.create_engine
    monkeypatch.setattr(
        module, "create_engine",
        lambda url, **kwargs: real(url, module=server.dbapi, **kwargs),
    )


def _config() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example", database="app", username="migrator")


# ---------------------------------------------------------------------------
# 1. The pin: one SET, on every MySQL source session
# ---------------------------------------------------------------------------


def test_the_shared_init_command_pins_utc_and_a_neutral_sql_mode_in_one_set() -> None:
    for kwargs in (source_engine_kwargs(), source_engine_kwargs(read_timeout_seconds=30)):
        assert kwargs["connect_args"]["init_command"] == PIN
    assert PIN.count("SET") == 1  # one statement, both assignments
    assert not is_write_or_ddl(PIN)
    # The dialect hands every engine the same kwargs; PostgreSQL is untouched.
    assert dialect_for(SourceType.MYSQL).engine_kwargs()["connect_args"]["init_command"] == PIN
    assert "init_command" not in dialect_for(SourceType.POSTGRES).engine_kwargs()["connect_args"]


@pytest.mark.parametrize("factory_name", ["ui", "core", "fence"])
def test_every_mysql_source_engine_runs_the_pin_on_connect(
    monkeypatch: pytest.MonkeyPatch, factory_name: str
) -> None:
    from dsql_migrator.core import mysql_start_point_fence
    from sqlalchemy import text

    server = _MySqlServer()
    module = {"ui": connect, "core": introspector}.get(factory_name)
    if module is None:
        import sqlalchemy

        _through(monkeypatch, server, sqlalchemy)
        engine = mysql_start_point_fence.build_fence_engine(_config(), None)
    else:
        _through(monkeypatch, server, module)
        factory = make_source_engine_factory(None) if factory_name == "ui" else _default_engine_factory
        engine = factory(_config())
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    finally:
        engine.dispose()
    assert server.connect_kwargs[0]["init_command"] == PIN
    # The first statement of the session is the pin, under the parameter group's mode, and
    # everything after it runs under the pinned one -- SQLAlchemy's own sql_mode read too.
    assert server.log[0] == (PIN, ANSI_PARAMETER_GROUP)
    assert {mode for _sent, mode in server.log[1:]} == {SESSION_MODE}
    assert server.modes_at(r"^SELECT @@sql_mode$") == [SESSION_MODE]
    assert engine.dialect._server_ansiquotes is False


@pytest.mark.parametrize("factory_name", ["ui", "core"])
def test_a_view_body_is_never_read_under_the_parameter_groups_ansi_quotes(
    monkeypatch: pytest.MonkeyPatch, factory_name: str
) -> None:
    """Through introspection itself: SHOW CREATE runs under the pinned sql_mode, so the body
    is backtick-quoted, and the converter reads its identifiers as identifiers."""
    server = _MySqlServer()
    module = connect if factory_name == "ui" else introspector
    _through(monkeypatch, server, module)
    factory = make_source_engine_factory(None) if factory_name == "ui" else _default_engine_factory
    inventory = SourceIntrospector(engine_factory=factory).introspect(_config())

    (view,) = inventory.views
    assert view.name == "app.v_open"
    assert view.definition == _view_body("`")
    assert '"' not in view.definition
    assert server.modes_at(r"^SHOW CREATE TABLE") == [SESSION_MODE]
    converted = SchemaConverter().convert_view(view)
    assert converted.auto_converted
    assert '"orders"."id" AS "id"' in converted.target_ddl
    assert "'orders'" not in converted.target_ddl
    # The same read captured the GLOBAL values, ANSI_QUOTES included: the application's
    # sessions do run under it, which Evaluation reports.
    assert inventory.server_settings["sql_mode"] == ANSI_PARAMETER_GROUP
    report = CompatibilityAssessor(source_type=SourceType.MYSQL).assess(inventory)
    (item,) = [i for i in report.items if i.rule_id == SQL_MODE_RULE_ID]
    assert "ANSI_QUOTES and PIPES_AS_CONCAT" in item.risk


def test_control_without_the_pin_the_body_comes_back_ansi_quoted(monkeypatch) -> None:  # noqa: ANN001
    """The double models the hazard: an engine without the tool's init_command reads the
    view under the parameter group's ANSI_QUOTES, and the converter then writes the
    identifiers as string literals (see the converter's own documentation test below)."""
    server = _MySqlServer()

    def bare_factory(conn: SourceConnectionConfig):  # noqa: ANN202
        engine = create_engine(
            "mysql+pymysql://migrator@db.example/app", module=server.dbapi, pool_pre_ping=True
        )
        install_read_only_guard(engine)
        return engine

    inventory = SourceIntrospector(engine_factory=bare_factory).introspect(_config())
    (view,) = inventory.views
    assert view.definition == _view_body('"')
    assert server.modes_at(r"^SHOW CREATE TABLE") == [ANSI_PARAMETER_GROUP]
    assert "'orders'.'id'" in SchemaConverter().convert_view(view).target_ddl


def test_the_converter_reads_an_ansi_quoted_body_as_string_literals() -> None:
    """Documentation, not a wish: the converter parses a MySQL view body in MySQL's default
    mode, where "x" is a string. It never sees an ANSI-quoted body any more (the pin above);
    one in a session an earlier version read under ANSI_QUOTES is converted like this until
    Evaluation reads the source again."""
    ansi = ViewDef(name="app.v_open", definition=_view_body('"'))
    converted = SchemaConverter().convert_view(ansi)
    assert "'orders'.'id' AS \"id\"" in converted.target_ddl
    backticks = SchemaConverter().convert_view(ViewDef(name="app.v_open", definition=_view_body("`")))
    assert '"orders"."id" AS "id"' in backticks.target_ddl


# ---------------------------------------------------------------------------
# 2. The capture: MySQL's GLOBAL values in one read
# ---------------------------------------------------------------------------


class _VariablesConnection:
    """A connection of the named dialect that answers SHOW GLOBAL VARIABLES as MySQL does:
    the bound names this version has, in name order."""

    def __init__(
        self, variables: Optional[dict] = None, *, dialect_name: str = "mysql", fail: bool = False
    ) -> None:
        self.dialect = types.SimpleNamespace(name=dialect_name)
        self.variables = dict(variables or {})
        self.fail = fail
        self.executed: list[tuple[str, dict]] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        self.executed.append((str(statement), dict(parameters or {})))
        if self.fail:
            raise RuntimeError("SHOW GLOBAL VARIABLES failed")
        names = set((parameters or {}).values())
        rows = sorted((n, v) for n, v in self.variables.items() if n in names)
        return types.SimpleNamespace(fetchall=lambda: rows)


def test_mysql_reads_the_global_values_once_with_the_names_bound() -> None:
    variables = {
        "sql_mode": "STRICT_TRANS_TABLES",
        "time_zone": "+09:00",
        "system_time_zone": "KST",
        "explicit_defaults_for_timestamp": "OFF",
        "auto_increment_increment": "2",
        "auto_increment_offset": "1",
        "lower_case_table_names": "1",
        "character_set_server": "latin1",
        "collation_server": "latin1_swedish_ci",
        # MySQL 5.7: no default_collation_for_utf8mb4.
        "div_precision_increment": "4",
        "group_concat_max_len": "1024",
        "max_connections": "100",  # not asked for
    }
    connection = _VariablesConnection(variables)
    settings = dialect_for(SourceType.MYSQL).server_settings(connection)
    expected = [name for name in MYSQL_SERVER_SETTINGS if name != "default_collation_for_utf8mb4"]
    assert list(settings) == expected  # the screen's order, not the server's
    assert settings["time_zone"] == "+09:00" and settings["sql_mode"] == "STRICT_TRANS_TABLES"
    assert "max_connections" not in settings
    ((statement, params),) = connection.executed
    assert statement.startswith("SHOW GLOBAL VARIABLES WHERE Variable_name IN (")
    assert sorted(params.values()) == sorted(MYSQL_SERVER_SETTINGS)
    assert not any(name in statement for name in MYSQL_SERVER_SETTINGS)  # bound, not formatted
    assert not is_write_or_ddl(statement)


def test_an_empty_global_value_is_kept_as_empty() -> None:
    settings = dialect_for(SourceType.MYSQL).server_settings(_VariablesConnection({"sql_mode": ""}))
    assert settings == {"sql_mode": ""}


def test_mysql_server_settings_are_best_effort() -> None:
    mysql = dialect_for(SourceType.MYSQL)
    assert mysql.server_settings(_VariablesConnection(fail=True)) == {}
    other = _VariablesConnection({"sql_mode": ""}, dialect_name="sqlite")
    assert mysql.server_settings(other) == {}
    assert other.executed == []  # not even tried on a connection that is not MySQL's


def test_the_inventory_carries_what_the_dialect_read() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory
    from dsql_migrator.core.source_dialect import MySQLSourceDialect
    from tests.test_introspector import _FakeInspector, _NonMysqlConnection

    class _Read(MySQLSourceDialect):
        def server_settings(self, connection):  # noqa: ANN001, ANN202
            return {"sql_mode": "", "time_zone": "SYSTEM"}

    catalog = {"shop": {"tables": {"orders": {}}}}
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), None, dialect=_Read()
    )
    assert inventory.server_settings == {"sql_mode": "", "time_zone": "SYSTEM"}
    # Nothing read (a connection that is not MySQL's, a failed read): empty, no failure.
    plain = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), None,
        dialect=dialect_for(SourceType.MYSQL),
    )
    assert plain.server_settings == {}
    assert [t.name for t in plain.tables] == ["shop.orders"]


# ---------------------------------------------------------------------------
# 3. The capture: PostgreSQL's TimeZone, past the UTC startup option
# ---------------------------------------------------------------------------


class _ProbeEngine:
    """A probe engine double: one connection answering the TimeZone read with ``row``."""

    def __init__(self, row: Optional[tuple], *, fail: bool = False) -> None:
        self.row, self.fail = row, fail
        self.executed: list[str] = []
        self.disposed = False

    def connect(self):  # noqa: ANN201
        engine = self

        class _Connection:
            def __enter__(self_):  # noqa: ANN001, ANN204, N805
                return self_

            def __exit__(self_, *_exc):  # noqa: ANN001, ANN204, N805
                return False

            def execute(self_, statement):  # noqa: ANN001, ANN201, N805
                engine.executed.append(str(statement))
                if engine.fail:
                    raise RuntimeError("connection refused")
                return types.SimpleNamespace(first=lambda: engine.row)

        return _Connection()

    def dispose(self) -> None:
        self.disposed = True


class _PgConnection:
    """The tool's pinned PostgreSQL connection: its engine's URL, and pg_db_role_setting."""

    def __init__(self, defaults: Optional[list] = None, *, dialect_name: str = "postgresql",
                 fail: bool = False) -> None:
        self.dialect = types.SimpleNamespace(name=dialect_name)
        self.engine = types.SimpleNamespace(url="postgresql+psycopg://migrator:secret@db/app")
        self.defaults, self.fail = list(defaults or []), fail
        self.executed: list[str] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        self.executed.append(str(statement))
        if self.fail:
            raise RuntimeError("permission denied")
        return types.SimpleNamespace(fetchall=lambda: list(self.defaults))


def _probe(monkeypatch: pytest.MonkeyPatch, engine: _ProbeEngine) -> list:
    built: list = []

    def build(url):  # noqa: ANN001, ANN202
        built.append(url)
        return engine

    monkeypatch.setattr(pg_dialect_module, "_settings_probe_engine", build)
    return built


def test_the_timezone_probe_connection_sends_no_startup_option() -> None:
    from sqlalchemy.engine import URL
    from sqlalchemy.pool import NullPool

    # The tool's own sessions pin UTC as a startup option, which outranks the parameter
    # group and every ALTER DATABASE / ALTER ROLE default; the probe sends none.
    assert "-c timezone=UTC" in dialect_for(SourceType.POSTGRES).engine_kwargs()["connect_args"]["options"]
    probe_args = pg_dialect_module._SETTINGS_PROBE_KWARGS["connect_args"]
    assert "options" not in probe_args and probe_args["connect_timeout"] > 0
    url = URL.create("postgresql+psycopg", username="u", password="pw", host="db", database="app")
    engine = pg_dialect_module._settings_probe_engine(url)  # creates no connection yet
    try:
        assert engine.url == url
        assert isinstance(engine.pool, NullPool)
        (guard,) = list(engine.dispatch.before_cursor_execute)  # the read-only guard
        with pytest.raises(introspector.ReadOnlySourceError):
            guard(None, None, "ALTER DATABASE app SET timezone = 'UTC'", {}, None, False)
    finally:
        engine.dispose()


def test_pg_default_timezone_is_read_over_its_own_connection(monkeypatch) -> None:  # noqa: ANN001
    engine = _ProbeEngine(("Asia/Seoul", "configuration file"))
    built = _probe(monkeypatch, engine)
    connection = _PgConnection()
    assert pg_dialect_module.pg_default_timezone(connection) == "Asia/Seoul"
    assert built == [connection.engine.url]  # the source's own URL, nothing else
    assert engine.executed == [pg_dialect_module._PG_SESSION_TIMEZONE_SQL]
    assert engine.disposed
    assert connection.executed == []  # the pinned session was not asked: it says UTC


@pytest.mark.parametrize(
    ("row", "fail"),
    [
        (("UTC", "client"), False),  # this process's own PGTZ / PGOPTIONS, not the server's
        (None, False),
        (("", "default"), False),
        (None, True),
    ],
)
def test_pg_default_timezone_is_best_effort(monkeypatch, row, fail) -> None:  # noqa: ANN001
    engine = _ProbeEngine(row, fail=fail)
    _probe(monkeypatch, engine)
    assert pg_dialect_module.pg_default_timezone(_PgConnection()) is None
    assert engine.disposed


def test_pg_default_timezone_needs_the_connections_url() -> None:
    connection = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))
    assert pg_dialect_module.pg_default_timezone(connection) is None


def test_pg_timezone_defaults_name_the_statement_that_set_each() -> None:
    connection = _PgConnection(
        [
            ("", True, "Asia/Seoul"),
            ("", False, "UTC"),
            ("app", False, "Asia/Tokyo"),
            ("app", True, "Europe/Paris"),
        ]
    )
    assert pg_dialect_module.pg_timezone_defaults(connection) == {
        "TimeZone (ALTER DATABASE)": "Asia/Seoul",
        "TimeZone (ALTER ROLE ALL)": "UTC",
        "TimeZone (ALTER ROLE app)": "Asia/Tokyo",
        "TimeZone (ALTER ROLE app IN DATABASE)": "Europe/Paris",
    }
    (statement,) = connection.executed
    # Only the TimeZone entries leave the server (a role's other settings can hold anything),
    # for this database or every database.
    assert "lower(split_part(c.entry, '=', 1)) = 'timezone'" in statement
    assert "datname = current_database()" in statement
    assert not is_write_or_ddl(statement)
    assert pg_dialect_module.pg_timezone_defaults(_PgConnection(fail=True)) == {}


def test_pg_server_settings_combine_both_reads_on_a_postgresql_connection(monkeypatch) -> None:  # noqa: ANN001
    engine = _ProbeEngine(("America/New_York", "database"))
    built = _probe(monkeypatch, engine)
    pg = dialect_for(SourceType.POSTGRES)
    settings = pg.server_settings(_PgConnection([("app", False, "Asia/Tokyo")]))
    assert settings == {
        "TimeZone": "America/New_York",
        "TimeZone (ALTER ROLE app)": "Asia/Tokyo",
    }
    # Not on a connection that is not PostgreSQL's (the SQLite double): no probe either.
    other = _PgConnection(dialect_name="sqlite")
    assert pg.server_settings(other) == {} and other.executed == [] and len(built) == 1
    # A failed probe still keeps what pg_db_role_setting said.
    _probe(monkeypatch, _ProbeEngine(None, fail=True))
    assert pg.server_settings(_PgConnection([("", True, "UTC")])) == {
        "TimeZone (ALTER DATABASE)": "UTC"
    }


# ---------------------------------------------------------------------------
# 4. The model: left out of the payload while empty
# ---------------------------------------------------------------------------


def test_server_settings_are_left_out_of_the_payload_while_empty() -> None:
    from dsql_migrator.core.session_state_store import SessionSnapshot

    empty = SourceInventory()
    assert "server_settings" not in empty.model_dump()
    assert "server_settings" not in empty.model_dump_json()
    # An inventory saved by an earlier version (no such key) loads; a read one round-trips.
    earlier = '{"tables": [], "views": [], "triggers": [], "routines": [], "events": []}'
    assert SourceInventory.model_validate_json(earlier).server_settings == {}
    read = SourceInventory(server_settings={"sql_mode": "", "time_zone": "SYSTEM"})
    again = SourceInventory.model_validate_json(read.model_dump_json())
    assert list(again.server_settings.items()) == [("sql_mode", ""), ("time_zone", "SYSTEM")]
    snapshot = SessionSnapshot(session_id="s", inventory=read)
    restored = SessionSnapshot.model_validate_json(snapshot.model_dump_json())
    assert restored.inventory.server_settings == read.server_settings


# ---------------------------------------------------------------------------
# 5. The findings
# ---------------------------------------------------------------------------


def _table(name: str = "app.orders", *columns: ColumnDef, auto_increment: bool = False) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False), *columns],
        primary_key=["id"],
        auto_increment_column="id" if auto_increment else None,
    )


def _database_items(inventory: SourceInventory, source_type: SourceType = SourceType.MYSQL) -> dict:
    report = CompatibilityAssessor(source_type=source_type).assess(inventory)
    return {
        item.rule_id: item
        for item in report.items
        if item.rule_id in (
            SQL_MODE_RULE_ID, ZERO_DATES_RULE_ID, IMPLICIT_TIMESTAMP_RULE_ID,
            AUTO_INCREMENT_STEP_RULE_ID,
        )
    }


_DATED = _table("app.orders", ColumnDef(name="placed", mysql_type="date"))


def _assert_database_gap(item, *, object_name: str, effort: str) -> None:  # noqa: ANN001
    assert item.classification is Classification.MANUAL  # "Review needed": the warning tone
    assert item.kind == "DATABASE"
    assert item.object_name == object_name
    assert item.effort is not None and item.effort.value == effort
    (concern,) = item.concerns
    assert concern.note_kind is ConversionNoteKind.LOSS and concern.risk == item.risk
    assert "DB cluster parameter group" in item.recommendation


def test_a_default_sql_mode_has_nothing_to_report() -> None:
    inventory = SourceInventory(tables=[_DATED], server_settings={"sql_mode": MYSQL_80_DEFAULT})
    assert _database_items(inventory) == {}


def test_an_empty_sql_mode_lists_the_group_by_and_date_flags() -> None:
    inventory = SourceInventory(tables=[_DATED], server_settings={"sql_mode": ""})
    items = _database_items(inventory)
    item = items[SQL_MODE_RULE_ID]
    _assert_database_gap(item, object_name="sql_mode", effort="MEDIUM")
    assert item.risk.startswith("The source's global sql_mode is ''.")
    assert "Without ONLY_FULL_GROUP_BY" in item.risk and "SQLSTATE 42803" in item.risk
    assert "ANSI_QUOTES" not in item.risk
    assert "reaches new connections without a reboot" in item.recommendation
    # G150: what the zero-date and strict flags let in is its own item, ZERO_DATES
    # (tests/test_g150_zero_dates.py), and such a row now loads -- written as CDC writes it --
    # so no item says Aurora DSQL rejects it on the way in.
    assert "0000-00-00" not in item.risk
    dates = items[ZERO_DATES_RULE_ID]
    for reason in ("no NO_ZERO_DATE", "no NO_ZERO_IN_DATE", "no strict mode"):
        assert reason in dates.risk
    assert "'0000-00-00'" in dates.risk and "SQLSTATE 22008" not in dates.risk
    assert "Without strict mode MySQL also stores an invalid date" in dates.risk
    assert "'2024-02-31'" not in dates.risk  # no ALLOW_INVALID_DATES


def test_the_date_flags_matter_only_where_a_table_holds_dates() -> None:
    undated = SourceInventory(tables=[_table()], server_settings={"sql_mode": ""})
    items = _database_items(undated)
    assert "ONLY_FULL_GROUP_BY" in items[SQL_MODE_RULE_ID].risk
    assert ZERO_DATES_RULE_ID not in items
    # Strict, but ALLOW_INVALID_DATES: the invalid day is named, the lenient write is not.
    invalid = SourceInventory(
        tables=[_table("app.events", ColumnDef(name="at", mysql_type="datetime(6)"))],
        server_settings={"sql_mode": MYSQL_80_DEFAULT + ",ALLOW_INVALID_DATES"},
    )
    items = _database_items(invalid)
    item = items[ZERO_DATES_RULE_ID]
    assert "(ALLOW_INVALID_DATES)" in item.risk and "'2024-02-31'" in item.risk
    # Strict mode with both zero-date flags still refuses zero dates: not claimed.
    assert "'0000-00-00'" not in item.risk and "'2024-00-15'" not in item.risk
    assert "Without strict mode" not in item.risk
    # The rest of the mode has nothing to report (ONLY_FULL_GROUP_BY is set).
    assert SQL_MODE_RULE_ID not in items


@pytest.mark.parametrize(
    ("mode", "named", "not_named"),
    [
        # Strict, NO_ZERO_DATE, no NO_ZERO_IN_DATE: only a zero month or day gets in.
        (
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_DATE",
            ["'2024-00-15'"],
            ["'0000-00-00'", "'2024-02-31'", "Without strict mode"],
        ),
        # Both zero-date flags, no strict mode: they only warn, so a zero date gets in.
        (
            "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE",
            ["'0000-00-00'", "Without strict mode"],
            ["'2024-00-15'", "'2024-02-31'"],
        ),
        # Nothing refuses anything: every kind, listed as one sentence.
        (
            "ONLY_FULL_GROUP_BY,ALLOW_INVALID_DATES",
            ["a zero date ('0000-00-00'), a zero month or day ('2024-00-15') or a day the "
             "month does not have ('2024-02-31')."],
            [],
        ),
    ],
)
def test_the_date_part_names_only_the_values_the_flags_let_in(
    mode: str, named: list, not_named: list
) -> None:
    inventory = SourceInventory(tables=[_DATED], server_settings={"sql_mode": mode})
    risk = _database_items(inventory)[ZERO_DATES_RULE_ID].risk  # G150: its own item
    for text_ in named:
        assert text_ in risk, text_
    for text_ in not_named:
        assert text_ not in risk, text_


def test_instance_level_settings_name_the_instance_parameter_group_too() -> None:
    """Aurora MySQL: sql_mode and explicit_defaults_for_timestamp are instance-level
    parameters (the DB cluster parameter group holds their default, an instance's DB parameter
    group overrides it); auto_increment_increment / _offset are cluster-level only."""
    tables = [
        _table("app.t", ColumnDef(name="ts", mysql_type="timestamp", nullable=False),
               auto_increment=True),
    ]
    settings = {
        "sql_mode": "",
        "explicit_defaults_for_timestamp": "OFF",
        "auto_increment_increment": "2",
    }
    items = _database_items(SourceInventory(tables=tables, server_settings=settings))
    override = "or the DB parameter group of an instance that overrides it"
    assert override in items[SQL_MODE_RULE_ID].recommendation
    assert override in items[IMPLICIT_TIMESTAMP_RULE_ID].recommendation
    step = items[AUTO_INCREMENT_STEP_RULE_ID].recommendation
    assert "(Aurora MySQL: its DB cluster parameter group)" in step and override not in step


def test_ansi_quotes_and_pipes_as_concat_point_at_the_query_converter() -> None:
    # The composite ANSI stands for them (and ONLY_FULL_GROUP_BY), whether or not listed.
    inventory = SourceInventory(
        tables=[_table()], server_settings={"sql_mode": "ANSI,STRICT_ALL_TABLES"}
    )
    item = _database_items(inventory)[SQL_MODE_RULE_ID]
    assert "With ANSI_QUOTES and PIPES_AS_CONCAT" in item.risk
    assert "Query Converter" in item.risk and "sql_mode 'ALLOW_INVALID_DATES'" in item.risk
    assert "ONLY_FULL_GROUP_BY" not in item.risk
    assert "CONCAT() for ||" in item.recommendation


@pytest.mark.parametrize("settings", [{}, {"sql_mode": "1073741824"}, {"time_zone": "SYSTEM"}])
def test_no_sql_mode_finding_when_the_mode_was_not_read(settings: dict) -> None:
    assert _database_items(SourceInventory(tables=[_DATED], server_settings=settings)) == {}


def test_explicit_defaults_for_timestamp_off_lists_the_timestamp_not_null_columns() -> None:
    tables = [
        _table(
            "app.orders",
            ColumnDef(name="created", mysql_type="timestamp", nullable=False),
            ColumnDef(name="updated", mysql_type="timestamp(6)", nullable=False),
            ColumnDef(name="seen", mysql_type="timestamp", nullable=True),  # declared NULL
            ColumnDef(name="placed", mysql_type="datetime", nullable=False),  # not a TIMESTAMP
        ),
    ]
    inventory = SourceInventory(
        tables=tables, server_settings={"explicit_defaults_for_timestamp": "OFF"}
    )
    item = _database_items(inventory)[IMPLICIT_TIMESTAMP_RULE_ID]
    _assert_database_gap(item, object_name="explicit_defaults_for_timestamp", effort="SIMPLE")
    assert "(2): app.orders.created, app.orders.updated." in item.risk
    assert "SQLSTATE 23502" in item.risk and "ON_UPDATE_TIMESTAMP" in item.risk
    assert "reboot where the parameter is static" in item.recommendation


def test_explicit_defaults_for_timestamp_lists_the_first_ten_and_counts_the_rest() -> None:
    columns = [ColumnDef(name=f"t{i:02d}", mysql_type="timestamp", nullable=False) for i in range(12)]
    inventory = SourceInventory(
        tables=[_table("app.audit", *columns)],
        server_settings={"explicit_defaults_for_timestamp": "0"},
    )
    risk = _database_items(inventory)[IMPLICIT_TIMESTAMP_RULE_ID].risk
    assert "(12): app.audit.t00" in risk and "app.audit.t09 and 2 more." in risk
    assert "app.audit.t10" not in risk


@pytest.mark.parametrize(
    "settings",
    [{"explicit_defaults_for_timestamp": "ON"}, {"explicit_defaults_for_timestamp": "1"}, {}],
)
def test_no_timestamp_finding_when_the_setting_is_on_or_unread(settings: dict) -> None:
    table = _table("app.t", ColumnDef(name="ts", mysql_type="timestamp", nullable=False))
    assert _database_items(SourceInventory(tables=[table], server_settings=settings)) == {}


def test_no_timestamp_finding_without_a_timestamp_not_null_column() -> None:
    table = _table("app.t", ColumnDef(name="ts", mysql_type="timestamp", nullable=True))
    inventory = SourceInventory(
        tables=[table], server_settings={"explicit_defaults_for_timestamp": "OFF"}
    )
    assert _database_items(inventory) == {}


def test_an_auto_increment_step_names_the_tables_whose_keys_it_shapes() -> None:
    inventory = SourceInventory(
        tables=[_table("app.orders", auto_increment=True), _table("app.notes")],
        server_settings={"auto_increment_increment": "2", "auto_increment_offset": "2"},
    )
    item = _database_items(inventory)[AUTO_INCREMENT_STEP_RULE_ID]
    _assert_database_gap(item, object_name="auto_increment_increment", effort="SIMPLE")
    assert "auto_increment_increment is 2, auto_increment_offset 2" in item.risk
    assert "2 apart" in item.risk and "steps by 1" in item.risk
    assert item.risk.endswith("AUTO_INCREMENT key (1): app.orders.")


@pytest.mark.parametrize(
    ("settings", "auto_increment"),
    [
        # An increment of 1 issues consecutive values whatever the offset.
        ({"auto_increment_increment": "1", "auto_increment_offset": "5"}, True),
        ({"auto_increment_increment": "2"}, False),  # no AUTO_INCREMENT key to shape
        ({"auto_increment_increment": "two"}, True),
        ({}, True),
    ],
)
def test_no_auto_increment_step_finding(settings: dict, auto_increment: bool) -> None:
    inventory = SourceInventory(
        tables=[_table("app.orders", auto_increment=auto_increment)], server_settings=settings
    )
    assert _database_items(inventory) == {}


def test_the_case_folding_and_collation_settings_are_recorded_only() -> None:
    # The time zone is UTC here: one that is not is graded since G149 (SOURCE_TIME_ZONE,
    # tests/test_g149_source_time_zone.py).
    settings = {
        "sql_mode": MYSQL_80_DEFAULT,
        "time_zone": "SYSTEM",
        "system_time_zone": "UTC",
        "explicit_defaults_for_timestamp": "ON",
        "auto_increment_increment": "1",
        "auto_increment_offset": "1",
        "lower_case_table_names": "1",
        "character_set_server": "latin1",
        "collation_server": "latin1_swedish_ci",
        "default_collation_for_utf8mb4": "utf8mb4_general_ci",
        "div_precision_increment": "8",
        "group_concat_max_len": "4096",
    }
    tables = [_DATED, _table("app.t", ColumnDef(name="ts", mysql_type="timestamp", nullable=False),
                             auto_increment=True)]
    with_settings = CompatibilityAssessor().assess(SourceInventory(tables=tables, server_settings=settings))
    without = CompatibilityAssessor().assess(SourceInventory(tables=tables))
    assert with_settings.items == without.items


def test_a_postgresql_utc_timezone_is_recorded_not_graded() -> None:
    # A TimeZone other than UTC is graded since G149 (SOURCE_TIME_ZONE,
    # tests/test_g149_source_time_zone.py).
    table = TableDef(
        name="public.t",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)],
        primary_key=["id"],
    )
    settings = {"TimeZone": "UTC", "TimeZone (ALTER ROLE app)": "UTC",
                "sql_mode": ""}  # a MySQL name on a PostgreSQL report grades nothing either
    graded = CompatibilityAssessor(source_type=SourceType.POSTGRES)
    assert (
        graded.assess(SourceInventory(tables=[table], server_settings=settings)).items
        == graded.assess(SourceInventory(tables=[table])).items
    )


# ---------------------------------------------------------------------------
# 6. Evaluation: the run grades them, the screen lists them
# ---------------------------------------------------------------------------


def test_run_evaluation_grades_the_settings_the_read_captured() -> None:
    from dsql_migrator.ui.evaluation import run_evaluation
    from tests.test_ui_evaluation import _empty_target, _inputs, _source_factory, _target_factory

    inventory = SourceInventory(
        tables=[_DATED], server_settings={"sql_mode": "NO_ENGINE_SUBSTITUTION"}
    )
    source, _ = _source_factory(inventory)
    target, _ = _target_factory(_empty_target())
    result = run_evaluation(_inputs(), introspector_factory=source, target_browser_factory=target)
    assert result.inventory.server_settings == {"sql_mode": "NO_ENGINE_SUBSTITUTION"}
    # The mode lacks ONLY_FULL_GROUP_BY and lets zero dates in, and the table has a DATE.
    assert [i.rule_id for i in result.assessment.items if i.kind == "DATABASE"] == [
        SQL_MODE_RULE_ID, ZERO_DATES_RULE_ID,
    ]
    # A read that captured nothing grades nothing, and the run completes as before.
    source, _ = _source_factory(SourceInventory(tables=[_DATED]))
    plain = run_evaluation(_inputs(), introspector_factory=source, target_browser_factory=target)
    assert not [i for i in plain.assessment.items if i.kind == "DATABASE"]


def test_the_settings_rows_are_in_read_order_with_an_empty_value_spelled_out() -> None:
    from dsql_migrator.ui.evaluation import source_settings_rows

    inventory = SourceInventory(server_settings={"sql_mode": "", "time_zone": "SYSTEM"})
    assert source_settings_rows(inventory) == [("sql_mode", "(empty)"), ("time_zone", "SYSTEM")]
    assert source_settings_rows(SourceInventory()) == []
    assert source_settings_rows(None) == []


@pytest.mark.parametrize("source_type", [SourceType.MYSQL, SourceType.POSTGRES])
def test_the_evaluation_screen_lists_the_settings_under_the_source_counts(
    source_type: SourceType,
) -> None:
    from dsql_migrator.core.models import AssessmentReport, TargetInventory
    from dsql_migrator.ui.evaluation import (
        EvaluationResult,
        EvaluationState,
        _render_result,
        source_settings_caption,
    )
    from tests.test_ui_evaluation import _ItemUi

    settings = (
        {"sql_mode": "", "time_zone": "SYSTEM"}
        if source_type is SourceType.MYSQL
        else {"TimeZone": "Asia/Seoul", "TimeZone (ALTER ROLE app)": "UTC"}
    )

    table = _table("app.t")
    # One report for both renders, so the settings panel is the only difference.
    report = CompatibilityAssessor(source_type=source_type).assess(SourceInventory(tables=[table]))

    def render(inventory: SourceInventory) -> _ItemUi:
        ui = _ItemUi()
        result = EvaluationResult(
            inventory=inventory, assessment=report, target_inventory=TargetInventory(),
            target_conflicts=[], source_type=source_type,
        )
        _render_result(ui, result, eval_state=EvaluationState())
        return ui

    shown = render(SourceInventory(tables=[table], server_settings=settings))
    caption = source_settings_caption(source_type)
    assert caption in shown.texts
    for name, value in settings.items():
        index = shown.texts.index(name)
        assert shown.texts[index + 1] == (value or "(empty)")
    if source_type is SourceType.MYSQL:
        assert "GLOBAL" in caption and "sql_mode 'ALLOW_INVALID_DATES'" in caption
        assert "nothing is written to the source" in caption
    else:
        assert "ALTER ROLE" in caption and "SET TIME ZONE" in caption
    # A report without settings (a failed read, or one made before they were read) shows none.
    hidden = render(SourceInventory(tables=[table]))
    assert caption not in hidden.texts
    assert len(hidden.expansions) == len(shown.expansions) - 1


# ---------------------------------------------------------------------------
# 7. The manual says the same in every language
# ---------------------------------------------------------------------------


_NOTHING_WRITTEN = {
    "en": "nothing is written to the source",
    "ko": "소스에는 아무것도 쓰지 않습니다",
    "ja": "ソースには何も書き込みません",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_which_settings_evaluation_reads_and_how_the_tool_reads(
    language: str,
) -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual" / language
    chapter = " ".join((manual / "02-evaluation-and-schema-conversion.md").read_text(
        encoding="utf-8").split())
    section = chapter.split("## 2.1", 1)[1].split("### ", 1)[0]
    for name in MYSQL_SERVER_SETTINGS:
        assert f"`{name}`" in section, name
    for rule in (SQL_MODE_RULE_ID, IMPLICIT_TIMESTAMP_RULE_ID, AUTO_INCREMENT_STEP_RULE_ID):
        assert f"`{rule}`" in section, rule
    assert "`TimeZone`" in section and "`ALTER DATABASE`" in section and "`ALTER ROLE`" in section
    assert "`SET TIME ZONE`" in section  # what the read cannot see
    assert f"`{PIN}`" in section
    assert "`ANSI_QUOTES`" in section
    assert _NOTHING_WRITTEN[language] in section
