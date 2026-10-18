# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G136: every MySQL source session reads at REPEATABLE READ, so a consistent snapshot is one
on a READ-COMMITTED source too.

The tool opens its MySQL snapshots with ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` and
never set the isolation level, and MySQL ignores that clause below REPEATABLE READ (warning
138). On a source configured READ-COMMITTED every Full Load read, the watermark capture and
Validation's per-table snapshot read each statement at its own point in time: through the
real single reader, a row whose key moved during the read was read twice and another was
missed. Every MySQL source session now runs ``SET SESSION TRANSACTION ISOLATION LEVEL
REPEATABLE READ`` first, through the dialect's ``session_setup_sql`` seam.

* The statement: MySQL's session setup is the pin, PostgreSQL's is unchanged, and both pass
  the read-only guard.
* Every MySQL source engine the tool builds carries it: both engine factories, a Full Load
  worker's stream and watermark capture, and Validation's runner (UI and library).
* It holds for the life of a pooled session, through the real SQLAlchemy MySQL dialect over
  a DBAPI double whose server default is READ-COMMITTED: the snapshot of the first checkout,
  and the checkout after that AUTOCOMMIT use, where the pool sets the session back to the
  level the dialect read at first connect (so the pin has to run before that read).
* The manual (en / ko / ja): 3.2 says MySQL reads run at REPEATABLE READ and that a
  READ-COMMITTED source then holds a read view per table read; 7.3's write-heavy source
  says it too.

Doubles only: no MySQL, no AWS.
"""

from __future__ import annotations

import re
import types
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from dsql_migrator.core import introspector
from dsql_migrator.core.introspector import (
    ReadOnlySourceError,
    _default_engine_factory,
    install_read_only_guard,
    is_write_or_ddl,
)
from dsql_migrator.core.models import SourceConnectionConfig, SourceType
from dsql_migrator.core.source_dialect import MySQLSourceDialect, dialect_for
from dsql_migrator.core.validator import Validator
from dsql_migrator.ui import connect
from dsql_migrator.ui.connect import make_source_engine_factory

PIN = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
SNAPSHOT = "START TRANSACTION WITH CONSISTENT SNAPSHOT"


# ---------------------------------------------------------------------------
# 1. The statement
# ---------------------------------------------------------------------------


def test_every_mysql_source_session_is_set_to_repeatable_read() -> None:
    assert dialect_for(SourceType.MYSQL).session_setup_sql == (PIN,)
    # Every instance, not only the registry's (the watermark module keeps one of its own).
    assert MySQLSourceDialect().session_setup_sql == (PIN,)
    assert dialect_for(SourceType.POSTGRES).session_setup_sql == ("SET row_security = off",)


@pytest.mark.parametrize("source_type", [SourceType.MYSQL, SourceType.POSTGRES])
def test_the_session_setup_passes_the_read_only_guard(source_type: SourceType) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    install_read_only_guard(engine)
    (guard,) = list(engine.dispatch.before_cursor_execute)
    try:
        for statement in dialect_for(source_type).session_setup_sql:
            assert not is_write_or_ddl(statement)
            guard(None, None, statement, {}, None, False)  # not refused
        with pytest.raises(ReadOnlySourceError):  # the control: the guard is the live one
            guard(None, None, "INSERT INTO t VALUES (1)", {}, None, False)
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# 2. Every MySQL source engine the tool builds
# ---------------------------------------------------------------------------


def _session_setup_of(engine) -> list:  # noqa: ANN001
    try:
        return [
            fn.session_setup_sql
            for fn in engine.pool.dispatch.connect
            if hasattr(fn, "session_setup_sql")
        ]
    finally:
        engine.dispose()


def test_every_mysql_source_engine_the_tool_builds_carries_the_pin() -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import BatchedTableMigrator
    from dsql_migrator.ui.validation import _default_validator_factory

    from tests.test_ui_data_migration import _inputs as _load_inputs
    from tests.test_ui_validation import _inputs as _validation_inputs

    # What a Full Load worker builds for its table (_migrate_*_in_process): the defaults.
    worker = BatchedTableMigrator(_load_inputs())
    source = worker._inputs.source_config
    assert source.source_type is SourceType.MYSQL
    builders = {
        "ui.connect (connection test, Evaluation, CDC, probes)": make_source_engine_factory(None),
        "core default (introspector, exporter, watermark)": _default_engine_factory,
        "Full Load stream and shards": worker._exporter._engine_factory,
        "Full Load watermark capture": worker._watermark_capturer._engine_factory,
        "Validation (UI)": _default_validator_factory(_validation_inputs())._source_engine_factory,
        "Validation (library)": Validator()._source_engine_factory,
    }
    for name, build in builders.items():
        assert _session_setup_of(build(source)) == [(PIN,)], name


# ---------------------------------------------------------------------------
# 3. A pooled session stays at REPEATABLE READ (real SQLAlchemy MySQL dialect)
# ---------------------------------------------------------------------------


class _ReadCommittedSource:
    """A DBAPI module double for the real SQLAlchemy MySQL dialect. Each session starts at the
    server's default isolation (READ-COMMITTED here), a ``SET SESSION TRANSACTION ISOLATION
    LEVEL`` changes it and ``SELECT @@transaction_isolation`` reads it, as on MySQL; every
    statement is recorded with the level its session was at."""

    def __init__(self) -> None:
        self.sessions = 0
        self.log: list[tuple[str, str]] = []
        source = self

        class Error(Exception):
            pass

        class Cursor:
            def __init__(self, session: "Session") -> None:
                self._session, self._rows = session, []
                self.description, self.rowcount = None, -1

            def execute(self, statement: str, parameters: object = None) -> None:
                source.log.append((statement, self._session.isolation))
                level = re.fullmatch(r"SET SESSION TRANSACTION ISOLATION LEVEL (.+)", statement)
                if level:
                    self._session.isolation = level.group(1).replace(" ", "-")
                variables = {
                    "transaction_isolation": self._session.isolation,
                    "sql_mode": "",
                    "lower_case_table_names": "0",
                }
                read = re.fullmatch(r"SELECT @@(\w+)", statement)
                values = {"SELECT VERSION()": "8.0.39", "SELECT DATABASE()": "d"}
                value = variables.get(read.group(1)) if read else values.get(statement)
                self._rows = [] if value is None else [(value,)]
                self.description = None if value is None else [("v", 253) + (None,) * 5]
                self.rowcount = len(self._rows)

            def fetchone(self):  # noqa: ANN201
                return self._rows.pop(0) if self._rows else None

            def fetchall(self):  # noqa: ANN201
                rows, self._rows = self._rows, []
                return rows

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, **_connect_args: object) -> None:
                source.sessions += 1
                self.isolation = "READ-COMMITTED"

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

        self.dbapi = types.ModuleType("read_committed_source")
        self.dbapi.paramstyle = "pyformat"
        self.dbapi.Error = Error
        for name in ("Warning", "InterfaceError", "DatabaseError", "OperationalError",
                     "ProgrammingError", "IntegrityError", "DataError", "InternalError",
                     "NotSupportedError"):
            setattr(self.dbapi, name, type(name, (Error,), {}))
        self.dbapi.connect = lambda *_a, **kw: Session(**kw)

    def isolation_at(self, statement: str) -> list[str]:
        return [level for sent, level in self.log if sent == statement]


@pytest.mark.parametrize("factory_name", ["ui", "core"])
def test_a_pooled_session_reads_at_repeatable_read_on_a_read_committed_source(
    monkeypatch: pytest.MonkeyPatch, factory_name: str
) -> None:
    source = _ReadCommittedSource()
    module = connect if factory_name == "ui" else introspector
    real_create_engine = module.create_engine
    monkeypatch.setattr(
        module, "create_engine",
        lambda url, **kwargs: real_create_engine(url, module=source.dbapi, **kwargs),
    )
    factory = make_source_engine_factory(None) if factory_name == "ui" else _default_engine_factory
    engine = factory(SourceConnectionConfig(host="h", database="d", username="u"))
    try:
        # As the Full Load stream, the watermark capture and Validation open a snapshot.
        with engine.connect() as connection:
            snapshot = connection.execution_options(isolation_level="AUTOCOMMIT")
            snapshot.execute(text(SNAPSHOT))
            snapshot.execute(text("COMMIT"))
        # The same pooled session, given back after that AUTOCOMMIT use and taken again.
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        default_level = engine.dialect.default_isolation_level
    finally:
        engine.dispose()
    assert source.sessions == 1
    # The pin is the session's first statement, sent at the source's own default.
    assert source.log[0] == (PIN, "READ-COMMITTED")
    assert source.isolation_at(SNAPSHOT) == ["REPEATABLE-READ"]
    # Given back after its AUTOCOMMIT use, the session is set to the level the dialect read
    # at first connect -- the pinned one, because the pin ran before that read -- so the
    # next checkout reads at it too.
    assert default_level == "REPEATABLE READ"
    assert source.isolation_at("SELECT 1") == ["REPEATABLE-READ"]


# ---------------------------------------------------------------------------
# 4. The manual says the same in every language
# ---------------------------------------------------------------------------


def _flat(text_: str) -> str:
    return " ".join(text_.split())


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_mysql_reads_at_repeatable_read_and_what_it_holds(language: str) -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual" / language
    export = (manual / "03-full-load.md").read_text(encoding="utf-8")
    export = _flat(export.split("## 3.2", 1)[1].split("## 3.3", 1)[0])
    assert PIN in export
    assert "`transaction_isolation`" in export  # the server's own setting is left alone
    assert "warning 138" in export and "`READ-COMMITTED`" in export
    assert "read view" in export and "History List Length" in export
    assert "07-performance-and-tuning.md#" in export
    tuning = (manual / "07-performance-and-tuning.md").read_text(encoding="utf-8")
    write_heavy = _flat(tuning.split("## 7.3", 1)[1].split("## 7.4", 1)[0])
    assert "`READ-COMMITTED`" in write_heavy and "Validation" in write_heavy
    assert "03-full-load.md#32-" in write_heavy
