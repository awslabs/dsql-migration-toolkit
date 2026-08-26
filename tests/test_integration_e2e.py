# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end integration tests wiring the migration engine together (Task 14).

This module exercises the full pipeline -- introspection -> compatibility
assessment -> schema conversion -> query conversion -> export -> import ->
validation -- across two tiers:

1. **In-process pipeline (runs by default).** It drives the *real* engine
   components and threads real data through them, using in-memory / SQLite
   substitutes at the database boundary instead of Docker. The source
   introspector runs against a real SQLite database (real SQLAlchemy
   reflection); export streams rows from a fake source connection and the
   exported rows are loaded by the real batched importer into a fake DSQL store
   that simulates ``ON CONFLICT`` semantics; validation runs through the real
   :class:`Validator` against fake source/target connections. The OCC retry path
   (Property 5) is covered by injecting a ``SQLSTATE 40001`` conflict into the
   import and asserting the pipeline still converges idempotently -- this is the
   design's "OCC path covered with mocking" because a PostgreSQL 16 container
   does not reproduce DSQL's optimistic-concurrency failure.

2. **Infrastructure-gated tests (skipped unless infra is present).** A real
   local MySQL end-to-end test and a PostgreSQL 16 "can the converted artifacts
   actually execute" test. These require external services that are typically
   unavailable here, so they are marked ``integration`` and skipped unless
   ``RUN_INTEGRATION_TESTS=1`` and the dependency is reachable. They never fail
   the default suite.

To run the infra-gated tests:

- MySQL E2E: ``RUN_INTEGRATION_TESTS=1
  DSQL_MIGRATOR_TEST_MYSQL_URL='mysql+pymysql://user:pass@127.0.0.1:3306/app'
  uv run pytest -m integration``
- PG16 apply: ``RUN_INTEGRATION_TESTS=1
  DSQL_MIGRATOR_TEST_PG_DSN='postgresql://user:pass@127.0.0.1:5432/app'
  uv run pytest -m integration``
"""

from __future__ import annotations

import io
import os
from typing import Any, Optional

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.exporter import CsvRowWriter, RowWriter, export_rows
from dsql_migrator.core.introspector import (
    SourceIntrospector,
    install_read_only_guard,
    is_write_or_ddl,
)
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    TableDef,
    TargetConnectionConfig,
    ValidationMode,
)
from dsql_migrator.core.occ import OCC_SQLSTATE
from dsql_migrator.core.query_converter import QueryConverter
from dsql_migrator.core.validator import Validator


# ---------------------------------------------------------------------------
# Infrastructure gating helpers
# ---------------------------------------------------------------------------


def _integration_enabled() -> bool:
    """Return ``True`` only when integration tests are explicitly opted into."""
    return os.environ.get("RUN_INTEGRATION_TESTS") == "1"


_REQUIRE_INTEGRATION = pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_INTEGRATION_TESTS=1 (and provide infra) to run integration tests",
)


# ---------------------------------------------------------------------------
# In-process fakes (database boundary substitutes)
# ---------------------------------------------------------------------------


class _FakeMappingResult:
    """Mirrors the ``mappings()`` slice of a SQLAlchemy result for export."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):  # noqa: ANN201 - mirrors SQLAlchemy
        return iter(self._rows)


class _FakeExportConnection:
    """Serves keyset pages from an in-memory dataset for the export leg.

    Interprets the ``last`` / ``batch_size`` binds exactly like a real keyset
    query and records every statement so the read-only guarantee can be checked.
    """

    def __init__(self, rows: list[dict], pk: str = "id") -> None:
        self._rows = sorted(rows, key=lambda row: row[pk])
        self._pk = pk
        self.executed: list[str] = []

    def execution_options(self, **_kwargs):  # noqa: ANN201 - mirrors SQLAlchemy
        return self

    def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        self.executed.append(sql)
        upper = sql.strip().upper()
        if upper.startswith("START TRANSACTION") or upper.startswith("COMMIT"):
            return _FakeMappingResult([])
        params = parameters or {}
        last = params.get("last")
        limit = params.get("batch_size")
        candidates = [r for r in self._rows if last is None or r[self._pk] > last]
        return _FakeMappingResult(candidates[:limit] if limit is not None else candidates)


class _CapturingWriter(RowWriter):
    """A row writer that captures converted rows so they can be re-imported."""

    def __init__(self) -> None:
        self.columns: list[str] = []
        self.rows: list[dict] = []

    def write_header(self, columns: list[str]) -> None:
        self.columns = list(columns)

    def write_row(self, row) -> None:  # noqa: ANN001
        self.rows.append(dict(row))

    def close(self) -> None:  # noqa: D401 - nothing to release
        return None


class _ImportConflict(Exception):
    """A psycopg-like serialization failure carrying ``sqlstate`` (40001)."""

    def __init__(self) -> None:
        super().__init__("serialization failure")
        self.sqlstate = OCC_SQLSTATE


class _FakeDsqlStore:
    """In-memory DSQL target simulating idempotent ``ON CONFLICT`` inserts."""

    def __init__(self, *, fail_first: bool = False) -> None:
        self.rows: dict[tuple, dict] = {}
        self.inserts = 0
        self._pending_failures = [OCC_SQLSTATE] if fail_first else []


class _FakeDsqlCursor:
    def __init__(self, connection: "_FakeDsqlConnection") -> None:
        self._connection = connection
        self.rowcount = -1

    def execute(self, query: Any, params: Optional[list] = None) -> None:
        self.rowcount = self._connection.apply(query, params)

    def close(self) -> None:  # noqa: D401 - nothing to release
        return None


class _FakeDsqlConnection:
    """A fake autocommit DSQL connection backed by a shared store."""

    def __init__(self, store: _FakeDsqlStore, columns: list[str], keys: list[str]) -> None:
        self._store = store
        self._columns = columns
        self._keys = keys
        self.autocommit = True

    def cursor(self) -> _FakeDsqlCursor:
        return _FakeDsqlCursor(self)

    def apply(self, query: Any, params: Optional[list]) -> int:
        text_sql = query if isinstance(query, str) else query.as_string(None)
        if self._store._pending_failures:
            self._store._pending_failures.pop(0)
            raise _ImportConflict()
        width = len(self._columns)
        flat = list(params or [])
        affected = 0
        for offset in range(0, len(flat), width):
            row = dict(zip(self._columns, flat[offset : offset + width]))
            key = tuple(row[name] for name in self._keys)
            if key not in self._store.rows:
                self._store.rows[key] = row
                affected += 1
        self._store.inserts += 1
        return affected

    def close(self) -> None:  # noqa: D401 - nothing to release
        return None


# Validation-leg fakes (mirror the SQLAlchemy source / psycopg target APIs).


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


class _FakeValidationSource:
    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = counts
        self.executed: list[str] = []

    def execution_options(self, **_kwargs):  # noqa: ANN201
        return self

    def execute(self, statement, _parameters=None, execution_options=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        self.executed.append(sql)
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            import re

            match = re.findall(r"FROM `([^`]+)`", sql)
            return _ScalarResult(self._counts.get(match[-1] if match else "", 0))
        return _ScalarResult(None)


class _FakeValidationSourceEngine:
    def __init__(self, connection: _FakeValidationSource) -> None:
        self._connection = connection
        self.disposed = False

    def connect(self):  # noqa: ANN201
        connection = self._connection

        class _Ctx:
            def __enter__(self):  # noqa: ANN204
                return connection

            def __exit__(self, *_a):  # noqa: ANN204
                return False

        return _Ctx()

    def dispose(self) -> None:
        self.disposed = True


class _FakeValidationTargetCursor:
    def __init__(self, connection: "_FakeValidationTarget") -> None:
        self._connection = connection
        self._result: object = None
        self._rows: Optional[list] = None

    def execute(self, statement: Any, parameters: Any = None) -> None:
        self._result = self._connection.resolve(statement, parameters)
        self._rows = self._result if isinstance(self._result, list) else None

    def fetchone(self):  # noqa: ANN201
        if self._result is None or isinstance(self._result, list):
            return None
        return (self._result,)

    def fetchall(self) -> list:
        return list(self._rows or [])

    def close(self) -> None:  # noqa: D401 - nothing to release
        return None


class _FakeValidationTarget:
    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = counts

    def cursor(self) -> _FakeValidationTargetCursor:
        return _FakeValidationTargetCursor(self)

    def resolve(self, statement: Any, parameters: Any = None) -> object:
        import re

        text_sql = statement if isinstance(statement, str) else statement.as_string(None)
        params = parameters or {}
        match = re.findall(r'FROM "([^"]+)"', text_sql)
        table = match[-1] if match else ""
        if "COUNT(*)" in text_sql:
            return self._counts.get(table, 0)
        # Bounded keyset PK page (the target row-count path replaced the unbounded
        # COUNT(*) for single-column-PK tables): synthesize ascending PKs 1..count and
        # page them by the `last` keyset value + LIMIT.
        if "AS pk" in text_sql and "md5(" not in text_sql:
            n = self._counts.get(table, 0)
            last = params.get("last")
            start = 0 if last is None else int(last)
            limit_match = re.search(r"LIMIT (\d+)", text_sql)
            limit = int(limit_match.group(1)) if limit_match else n
            return [(pk,) for pk in range(start + 1, n + 1)][:limit]
        return None

    def close(self) -> None:  # noqa: D401 - nothing to release
        return None


# ---------------------------------------------------------------------------
# In-process source (SQLite) helpers
# ---------------------------------------------------------------------------


def _build_sqlite_source(engine: Engine) -> None:
    """Create a small real source schema for introspection."""
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE customers ("
                "id INTEGER PRIMARY KEY, "
                "name VARCHAR(100) NOT NULL"
                ")"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "id INTEGER PRIMARY KEY, "
                "customer_id INTEGER NOT NULL, "
                "total NUMERIC, "
                "FOREIGN KEY (customer_id) REFERENCES customers (id)"
                ")"
            )
        )


def _sqlite_introspector() -> SourceIntrospector:
    """Return an introspector backed by a fresh, read-only-guarded SQLite DB."""
    engine = create_engine("sqlite://")
    _build_sqlite_source(engine)
    install_read_only_guard(engine)
    return SourceIntrospector(engine_factory=lambda _conn: engine)


def _customers_table() -> TableDef:
    """A controlled customers table used for the export/import legs."""
    return TableDef(
        name="customers",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="name", mysql_type="VARCHAR(100)", nullable=False),
            ColumnDef(name="is_active", mysql_type="TINYINT(1)", nullable=False),
        ],
        primary_key=["id"],
    )


def _customer_rows() -> list[dict]:
    return [
        {"id": 1, "name": "alice", "is_active": 1},
        {"id": 2, "name": "bob", "is_active": 0},
        {"id": 3, "name": "carol", "is_active": 1},
    ]


def _source_config() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example.com", database="app")


def _target_config() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )


def _run_pipeline(*, fail_first_import: bool) -> tuple[_FakeDsqlStore, int]:
    """Run introspect -> assess -> convert -> export -> import end-to-end.

    Returns the populated DSQL store and the exported row count. ``fail_first_import``
    injects a single ``SQLSTATE 40001`` conflict on the first import batch to
    exercise the OCC retry path within the full pipeline (Property 5).
    """
    # 1. Introspection (real SQLAlchemy reflection on SQLite).
    introspector = _sqlite_introspector()
    inventory = introspector.introspect(_source_config())
    assert {t.name for t in inventory.tables} >= {"customers", "orders"}

    # 2. Compatibility assessment (real rule engine).
    report = CompatibilityAssessor().assess(inventory)
    assert report.items  # every object classified (Property 8)

    # 3. Schema conversion (real sqlglot transpile + DSQL constraints).
    conversion = SchemaConverter().convert(inventory)
    assert conversion.execution_units()

    # 4. Query conversion (real transpile + lock checks).
    query_result = QueryConverter().convert(
        "INSERT INTO `customers` (`id`) VALUES (1) "
        "ON DUPLICATE KEY UPDATE `id` = VALUES(`id`)"
    )
    assert query_result.converted_sql is not None

    # 5. Export (real keyset stream + value conversion) through a fake source.
    table = _customers_table()
    export_conn = _FakeExportConnection(_customer_rows())
    writer = _CapturingWriter()
    exported = export_rows(export_conn, table, writer, batch_size=2)
    assert exported == 3
    # The exporter must never write to the source.
    assert [s for s in export_conn.executed if is_write_or_ddl(s)] == []
    # TINYINT(1) values were converted to booleans on the way out.
    assert writer.rows[0]["is_active"] is True

    # 6. Import (real batched INSERT + OCC retry) into a fake DSQL store.
    store = _FakeDsqlStore(fail_first=fail_first_import)
    columns = [c.name for c in table.columns]
    importer = BatchedImporter(
        BatchedImportOptions(on_conflict=OnConflictMode.DO_NOTHING, batch_size=2),
        connection_factory=lambda: _FakeDsqlConnection(store, columns, ["id"]),
        occ_base_delay=0.0,
        sleep=lambda _s: None,
        jitter=lambda: 0.0,
    )
    result = importer.import_rows(writer.rows, table)
    assert result.failures == 0
    return store, exported


# ---------------------------------------------------------------------------
# In-process end-to-end (runs by default)
# ---------------------------------------------------------------------------


def test_end_to_end_in_process_pipeline() -> None:
    """introspect -> assess -> convert -> export -> import -> validate, in process."""
    store, exported = _run_pipeline(fail_first_import=False)

    # Every exported row landed in the target exactly once.
    assert exported == 3
    assert len(store.rows) == 3

    # Validation leg: source/target row counts agree -> overall match.
    source = _FakeValidationSource(counts={"customers": 3})
    target = _FakeValidationTarget(counts={"customers": 3})
    validator = Validator(
        source_engine_factory=lambda _c: _FakeValidationSourceEngine(source),
        target_connection_factory=lambda _c: target,
    )
    report = validator.validate(
        _source_config(),
        _target_config(),
        [_customers_table()],
        mode=ValidationMode.ROW_COUNT,
    )
    assert report.is_match is True
    assert report.items[0].source_row_count == 3
    assert report.items[0].target_row_count == 3
    # The validator never wrote to the source.
    assert [s for s in source.executed if is_write_or_ddl(s)] == []


def test_end_to_end_pipeline_retries_occ_conflict() -> None:
    """A 40001 conflict during import is retried and the load still converges."""
    store, exported = _run_pipeline(fail_first_import=True)

    # Despite the injected conflict, every row loaded exactly once (idempotent).
    assert exported == 3
    assert len(store.rows) == 3


def test_end_to_end_import_is_idempotent_on_rerun() -> None:
    """Re-running the import loads nothing new (Property 3 within the pipeline)."""
    store, _ = _run_pipeline(fail_first_import=False)
    before = dict(store.rows)

    table = _customers_table()
    columns = [c.name for c in table.columns]
    importer = BatchedImporter(
        BatchedImportOptions(on_conflict=OnConflictMode.DO_NOTHING, batch_size=2),
        connection_factory=lambda: _FakeDsqlConnection(store, columns, ["id"]),
        occ_base_delay=0.0,
        sleep=lambda _s: None,
        jitter=lambda: 0.0,
    )
    rerun = importer.import_rows(
        [
            {"id": 1, "name": "alice", "is_active": True},
            {"id": 2, "name": "bob", "is_active": False},
            {"id": 3, "name": "carol", "is_active": True},
        ],
        table,
    )

    assert rerun.rows_loaded == 0
    assert rerun.conflicts == 3
    assert store.rows == before


# ---------------------------------------------------------------------------
# Infrastructure-gated: real local MySQL end-to-end (skipped without infra)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@_REQUIRE_INTEGRATION
def test_real_mysql_introspection_and_export() -> None:
    """E2E against a real local MySQL: introspect a table, then export its rows.

    Requires ``RUN_INTEGRATION_TESTS=1`` and a reachable MySQL pointed to by
    ``DSQL_MIGRATOR_TEST_MYSQL_URL``. Skipped (not failed) when unavailable.
    """
    url = os.environ.get("DSQL_MIGRATOR_TEST_MYSQL_URL")
    if not url:
        pytest.skip("set DSQL_MIGRATOR_TEST_MYSQL_URL to a reachable MySQL URL")

    try:
        engine = create_engine(url)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - environment not ready -> skip
        pytest.skip(f"MySQL not reachable: {exc}")

    # Provision a tiny disposable schema, then run the engine against it.
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS it_customers"))
        connection.execute(
            text(
                "CREATE TABLE it_customers ("
                "id INT PRIMARY KEY, name VARCHAR(100) NOT NULL"
                ")"
            )
        )
        connection.execute(
            text("INSERT INTO it_customers (id, name) VALUES (1, 'a'), (2, 'b')")
        )

    try:
        introspector = SourceIntrospector(engine_factory=lambda _c: engine)
        inventory = introspector.introspect(_source_config())
        table = next(t for t in inventory.tables if t.name == "it_customers")

        conversion = SchemaConverter().convert_table(table)
        assert 'CREATE TABLE "it_customers"' in conversion.target_ddl

        from dsql_migrator.core.exporter import TableExporter

        buffer = io.StringIO()
        rows = TableExporter(engine_factory=lambda _c: engine).export_table(
            _source_config(), table, CsvRowWriter(buffer)
        )
        assert rows == 2
    finally:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS it_customers"))
        engine.dispose()


# ---------------------------------------------------------------------------
# Infrastructure-gated: PostgreSQL 16 artifact execution (skipped without infra)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@_REQUIRE_INTEGRATION
def test_converted_artifacts_execute_on_postgres16() -> None:
    """The converted DDL + a parameterized load execute on a real PostgreSQL 16.

    Validates that conversion artifacts are runnable PostgreSQL (DSQL is PG16
    compatible). DSQL-only constructs are adjusted for vanilla PG: ``CREATE INDEX
    ASYNC`` has no ``ASYNC`` keyword in PG16, so it is stripped here. OCC
    behaviour is NOT exercised here -- PG16 does not reproduce DSQL's SQLSTATE
    40001 -- it is covered by the in-process OCC test above.

    Requires ``RUN_INTEGRATION_TESTS=1`` and ``DSQL_MIGRATOR_TEST_PG_DSN``.
    Skipped (not failed) when unavailable.
    """
    dsn = os.environ.get("DSQL_MIGRATOR_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set DSQL_MIGRATOR_TEST_PG_DSN to a reachable PostgreSQL 16 DSN")

    try:
        import psycopg
    except ImportError:  # pragma: no cover - psycopg is a project dependency
        pytest.skip("psycopg is not available")

    try:
        connection = psycopg.connect(dsn, autocommit=True)
    except Exception as exc:  # noqa: BLE001 - environment not ready -> skip
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    table = _customers_table()
    conversion = SchemaConverter().convert_table(table)
    columns = [c.name for c in table.columns]
    from dsql_migrator.core.batched_import import build_insert_statement

    try:
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS "customers"')
            # Apply the converted CREATE TABLE (its own single-DDL transaction).
            cursor.execute(conversion.target_ddl)
            # Apply index DDLs, stripping the DSQL-only ASYNC keyword for PG16.
            for index_ddl in conversion.index_ddls:
                cursor.execute(index_ddl.replace(" ASYNC", ""))

        # Load rows with the same parameterized INSERT the importer builds.
        statement = build_insert_statement(
            table.name, columns, 3, OnConflictMode.DO_NOTHING, ["id"]
        )
        params: list[object] = []
        for row in [
            {"id": 1, "name": "alice", "is_active": True},
            {"id": 2, "name": "bob", "is_active": False},
            {"id": 3, "name": "carol", "is_active": True},
        ]:
            params.extend(row[name] for name in columns)
        with connection.cursor() as cursor:
            cursor.execute(statement, params)
            cursor.execute('SELECT COUNT(*) FROM "customers"')
            count = cursor.fetchone()[0]
        assert count == 3
    finally:
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS "customers"')
        connection.close()
