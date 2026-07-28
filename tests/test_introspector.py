# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit and property tests for the read-only source introspector.

Covers:
- ``test_connection`` success, failure-reason, and credential non-exposure
  (Requirements 1.1, 1.4, 9.2 / Property 7).
- ``introspect`` reflection assembly and ``information_schema`` enrichment
  (Requirements 1.2, 1.3).
- Read-only guarantee: no write/DDL statement reaches the source (Property 1 /
  Requirement 1.5).
"""

from __future__ import annotations

from typing import Optional

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from dsql_migrator.config import SecretRef, SecretSource
from dsql_migrator.core.introspector import (
    ReadOnlySourceError,
    SourceIntrospector,
    collect_routines,
    collect_triggers,
    collect_events,
    enrich_columns,
    enrich_index_types,
    enrich_partitions,
    install_read_only_guard,
    is_write_or_ddl,
)
from dsql_migrator.core.models import (
    ColumnDef,
    IndexDef,
    ObjectType,
    SourceConnectionConfig,
    TableDef,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _build_sqlite_schema(engine: Engine) -> None:
    """Create a small schema used to exercise dialect-agnostic reflection."""
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
        connection.execute(text("CREATE INDEX idx_orders_total ON orders (total)"))
        connection.execute(
            text("CREATE VIEW active_orders AS SELECT id FROM orders")
        )


def _sqlite_factory(record: Optional[list[str]] = None):
    """Return an engine factory backed by a fresh in-memory SQLite database.

    The same engine instance is reused across calls so a schema created on first
    use remains visible. When ``record`` is provided, every executed statement is
    appended to it for read-only assertions.
    """
    engine = create_engine("sqlite://")
    _build_sqlite_schema(engine)
    install_read_only_guard(engine)

    if record is not None:

        @event.listens_for(engine, "before_cursor_execute")
        def _capture(  # noqa: ANN001 - SQLAlchemy event signature
            conn, cursor, statement, parameters, context, executemany
        ) -> None:
            record.append(statement)

    def factory(_conn: SourceConnectionConfig) -> Engine:
        return engine

    return factory


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeConnection:
    """A minimal connection that returns canned rows for enrichment queries."""

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.executed: list[str] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        self.executed.append(str(statement))
        return _FakeResult(self._rows)


def _source_config() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example.com", database="app")


# ---------------------------------------------------------------------------
# Statement classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "  select * from t",
        "SHOW TRIGGERS",
        "DESCRIBE orders",
        "EXPLAIN SELECT 1",
        "SET SESSION TRANSACTION READ ONLY",
        "PRAGMA table_info('orders')",
        "/* comment */ SELECT 1",
        "-- lead comment\nSELECT 1",
        "(SELECT 1)",
        "COMMIT",
        "ROLLBACK",
    ],
)
def test_read_statements_are_not_flagged(sql: str) -> None:
    assert is_write_or_ddl(sql) is False


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "REPLACE INTO t VALUES (1)",
        "CREATE TABLE t (id INT)",
        "ALTER TABLE t ADD COLUMN c INT",
        "DROP TABLE t",
        "TRUNCATE TABLE t",
        "RENAME TABLE a TO b",
        "GRANT SELECT ON t TO u",
        "REVOKE SELECT ON t FROM u",
        "LOAD DATA INFILE 'x' INTO TABLE t",
        "CALL my_proc()",
        "LOCK TABLES t WRITE",
        "/* c */ DROP TABLE t",
    ],
)
def test_write_and_ddl_statements_are_flagged(sql: str) -> None:
    assert is_write_or_ddl(sql) is True


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    result = introspector.test_connection(_source_config())
    assert result.success is True
    assert "successful" in result.detail.lower()


def test_test_connection_failure_returns_reason() -> None:
    def failing_factory(_conn: SourceConnectionConfig) -> Engine:
        raise RuntimeError("could not reach host db.example.com:3306")

    introspector = SourceIntrospector(engine_factory=failing_factory)
    result = introspector.test_connection(_source_config())
    assert result.success is False
    assert "Connection failed" in result.detail
    assert "could not reach host" in result.detail


def test_test_connection_does_not_expose_credentials() -> None:
    secret_password = "super-secret-password"

    def leaky_factory(_conn: SourceConnectionConfig) -> Engine:
        # Simulate a driver error that echoes the password in its message.
        raise RuntimeError(
            f"access denied using password {secret_password} for db.example.com"
        )

    config = SourceConnectionConfig(
        host="db.example.com",
        database="app",
        username="app_user",
        secret=SecretRef(source=SecretSource.ENVIRONMENT, locator="DB_PASSWORD"),
    )
    introspector = SourceIntrospector(engine_factory=leaky_factory)

    import os

    os.environ["DB_PASSWORD"] = secret_password
    try:
        result = introspector.test_connection(config)
    finally:
        del os.environ["DB_PASSWORD"]

    assert result.success is False
    assert secret_password not in result.detail
    assert "***" in result.detail


# ---------------------------------------------------------------------------
# introspect — reflection assembly
# ---------------------------------------------------------------------------


def test_introspect_collects_tables_columns_pk_and_fk() -> None:
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    inventory = introspector.introspect(_source_config())

    table_names = {table.name for table in inventory.tables}
    assert {"customers", "orders"} <= table_names

    orders = next(table for table in inventory.tables if table.name == "orders")
    assert orders.primary_key == ["id"]
    column_names = {column.name for column in orders.columns}
    assert {"id", "customer_id", "total"} == column_names

    assert len(orders.foreign_keys) == 1
    fk = orders.foreign_keys[0]
    assert fk.columns == ["customer_id"]
    assert fk.referenced_table == "customers"
    assert fk.referenced_columns == ["id"]
    assert fk.name  # a name is always present (synthesized when missing)


def test_introspect_collects_indexes_and_views() -> None:
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    inventory = introspector.introspect(_source_config())

    orders = next(table for table in inventory.tables if table.name == "orders")
    assert any(index.name == "idx_orders_total" for index in orders.indexes)

    view_names = {view.name for view in inventory.views}
    assert "active_orders" in view_names


def test_introspect_result_is_serializable() -> None:
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    inventory = introspector.introspect(_source_config())
    # Round-trips through the Pydantic model without loss.
    assert inventory == type(inventory).model_validate(inventory.model_dump())


# ---------------------------------------------------------------------------
# information_schema enrichment (mocked MySQL metadata)
# ---------------------------------------------------------------------------


def test_collect_triggers_maps_rows_to_object_refs() -> None:
    connection = _FakeConnection([("trg_audit",), ("trg_updated_at",)])
    triggers = collect_triggers(connection, "app")
    assert [t.name for t in triggers] == ["trg_audit", "trg_updated_at"]
    assert all(t.object_type is ObjectType.TRIGGER for t in triggers)


def test_collect_routines_distinguishes_procedures_and_functions() -> None:
    connection = _FakeConnection(
        [("sp_recalc", "PROCEDURE"), ("fn_total", "FUNCTION")]
    )
    routines = collect_routines(connection, "app")
    by_name = {r.name: r.object_type for r in routines}
    assert by_name["sp_recalc"] is ObjectType.PROCEDURE
    assert by_name["fn_total"] is ObjectType.FUNCTION


def test_collect_events_maps_rows_to_object_refs() -> None:
    connection = _FakeConnection([("evt_nightly",), ("evt_cleanup",)])
    events = collect_events(connection, "app")
    assert [e.name for e in events] == ["evt_nightly", "evt_cleanup"]
    assert all(e.object_type is ObjectType.EVENT for e in events)


def test_enrich_columns_sets_collation_and_auto_increment() -> None:
    tables = [
        TableDef(
            name="orders",
            columns=[
                ColumnDef(name="id", mysql_type="INT"),
                ColumnDef(name="code", mysql_type="VARCHAR(20)"),
            ],
        )
    ]
    rows = [
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE)
        ("orders", "id", None, "auto_increment", "int unsigned"),
        ("orders", "code", "utf8mb4_general_ci", "", "varchar(20)"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)

    table = tables[0]
    assert table.auto_increment_column == "id"
    code_column = next(c for c in table.columns if c.name == "code")
    assert code_column.collation == "utf8mb4_general_ci"
    # COLUMN_TYPE overrides the (lossy) reflected mysql_type, preserving unsigned.
    id_column = next(c for c in table.columns if c.name == "id")
    assert id_column.mysql_type == "int unsigned"


def test_enrich_columns_restores_unsigned_and_tinyint1_from_column_type() -> None:
    # SQLAlchemy's reflected str(type) is lossy: it drops ``unsigned`` and the
    # ``tinyint(1)`` display width, which under-sizes the DSQL target (an unsigned
    # column overflows the signed mapping) and loses the boolean convention. The
    # enrichment must overwrite mysql_type with information_schema.COLUMN_TYPE.
    tables = [
        TableDef(
            name="t",
            columns=[
                ColumnDef(name="flag", mysql_type="TINYINT"),       # really tinyint(1)
                ColumnDef(name="qty", mysql_type="SMALLINT"),       # really smallint unsigned
                ColumnDef(name="big", mysql_type="INTEGER"),        # really int unsigned
            ],
        )
    ]
    rows = [
        ("t", "flag", None, "", "tinyint(1)"),
        ("t", "qty", None, "", "smallint unsigned"),
        ("t", "big", None, "", "int unsigned"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)
    by_name = {c.name: c for c in tables[0].columns}
    assert by_name["flag"].mysql_type == "tinyint(1)"
    assert by_name["qty"].mysql_type == "smallint unsigned"
    assert by_name["big"].mysql_type == "int unsigned"


def test_enrich_columns_marks_generated_and_on_update_columns() -> None:
    tables = [
        TableDef(
            name="orders",
            columns=[
                ColumnDef(name="id", mysql_type="INT"),
                ColumnDef(name="total", mysql_type="DECIMAL(10,2)"),
                ColumnDef(name="updated_at", mysql_type="TIMESTAMP"),
                ColumnDef(name="created_at", mysql_type="TIMESTAMP"),
            ],
        )
    ]
    rows = [
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE)
        ("orders", "id", None, "auto_increment", "int"),
        ("orders", "total", None, "STORED GENERATED", "decimal(10,2)"),
        ("orders", "updated_at", None,
         "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "timestamp"),
        # DEFAULT_GENERATED alone (expression default) must NOT be a generated col.
        ("orders", "created_at", None, "DEFAULT_GENERATED", "timestamp"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)

    by_name = {c.name: c for c in tables[0].columns}
    assert by_name["total"].generated is True
    assert by_name["updated_at"].auto_update_timestamp is True
    assert by_name["created_at"].generated is False
    assert by_name["created_at"].auto_update_timestamp is False


def test_enrich_index_types_records_index_type() -> None:
    tables = [
        TableDef(
            name="articles",
            columns=[ColumnDef(name="id", mysql_type="INT")],
            indexes=[
                IndexDef(name="ft_body", columns=["body"]),
                IndexDef(name="ix_name", columns=["name"]),
            ],
        )
    ]
    rows = [
        ("articles", "ft_body", "FULLTEXT"),
        ("articles", "ix_name", "BTREE"),
    ]
    enrich_index_types(_FakeConnection(rows), "app", tables)

    by_name = {i.name: i for i in tables[0].indexes}
    assert by_name["ft_body"].index_type == "FULLTEXT"
    assert by_name["ix_name"].index_type == "BTREE"


def test_enrich_partitions_marks_partitioned_tables() -> None:
    tables = [
        TableDef(name="metrics", columns=[ColumnDef(name="id", mysql_type="INT")]),
        TableDef(name="settings", columns=[ColumnDef(name="id", mysql_type="INT")]),
    ]
    enrich_partitions(_FakeConnection([("metrics",)]), "app", tables)

    metrics = next(t for t in tables if t.name == "metrics")
    settings = next(t for t in tables if t.name == "settings")
    assert metrics.partitioned is True
    assert settings.partitioned is False


# ---------------------------------------------------------------------------
# Cluster-wide introspection (no database selected) — multi-schema assembly
# ---------------------------------------------------------------------------


class _FakeInspector:
    """A minimal inspector over an in-memory multi-schema catalog for tests.

    ``catalog`` maps schema name -> {"tables": {name: {...}}, "views": {name: def}}.
    Only the methods :func:`_assemble_inventory` uses are implemented, each
    accepting the ``schema`` keyword like a real SQLAlchemy inspector.
    """

    def __init__(self, catalog: dict) -> None:
        self._catalog = catalog

    def get_schema_names(self):
        return list(self._catalog.keys())

    def get_table_names(self, schema=None):
        return list(self._catalog.get(schema, {}).get("tables", {}).keys())

    def get_columns(self, table_name, schema=None):
        table = self._catalog[schema]["tables"][table_name]
        return table.get("columns", [{"name": "id", "type": "INTEGER", "nullable": False}])

    def get_pk_constraint(self, table_name, schema=None):
        table = self._catalog[schema]["tables"][table_name]
        return {"constrained_columns": table.get("primary_key", ["id"])}

    def get_indexes(self, table_name, schema=None):
        return []

    def get_foreign_keys(self, table_name, schema=None):
        table = self._catalog[schema]["tables"][table_name]
        return list(table.get("foreign_keys", []))

    def get_view_names(self, schema=None):
        return list(self._catalog.get(schema, {}).get("views", {}).keys())

    def get_view_definition(self, view_name, schema=None):
        return self._catalog[schema]["views"][view_name]


class _NonMysqlConnection:
    """A connection whose dialect is not MySQL (skips information_schema enrich)."""

    class _Dialect:
        name = "sqlite"

    dialect = _Dialect()


def test_cluster_wide_introspection_qualifies_names_across_schemas() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory

    catalog = {
        "information_schema": {"tables": {"FILES": {}}},  # system schema -> skipped
        "mysql": {"tables": {"user": {}}},  # system schema -> skipped
        "shop": {
            "tables": {"orders": {}, "customers": {}},
            "views": {"active": "SELECT 1"},
        },
        "billing": {"tables": {"invoices": {}}},
    }
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), None, is_mysql=False
    )

    table_names = {table.name for table in inventory.tables}
    # Names are qualified with their schema and system schemas are excluded.
    assert table_names == {"shop.orders", "shop.customers", "billing.invoices"}
    assert {view.name for view in inventory.views} == {"shop.active"}
    assert "mysql.user" not in table_names


def test_cluster_wide_introspection_qualifies_cross_schema_fk_target() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory

    catalog = {
        "shop": {
            "tables": {
                "orders": {
                    "foreign_keys": [
                        {
                            "name": "fk_cust",
                            "constrained_columns": ["customer_id"],
                            "referred_schema": "billing",
                            "referred_table": "customers",
                            "referred_columns": ["id"],
                        }
                    ]
                },
            },
        },
        "billing": {"tables": {"customers": {}}},
    }
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), None, is_mysql=False
    )
    orders = next(t for t in inventory.tables if t.name == "shop.orders")
    # Cross-schema FK target is qualified with the FK's referred_schema, matching
    # how the child table name is qualified in cluster-wide mode (was unqualified
    # "customers", which resolved against the search_path / a wrong same-named table).
    assert orders.foreign_keys[0].referenced_table == "billing.customers"


def test_cluster_wide_same_schema_fk_qualified_with_reflected_schema() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory

    catalog = {
        "shop": {
            "tables": {
                "orders": {
                    "foreign_keys": [
                        {
                            "name": "fk_self",
                            "constrained_columns": ["parent_id"],
                            "referred_table": "orders",
                            "referred_columns": ["id"],
                            # No referred_schema -> same schema as the child table.
                        }
                    ]
                },
            },
        },
    }
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), None, is_mysql=False
    )
    orders = next(t for t in inventory.tables if t.name == "shop.orders")
    assert orders.foreign_keys[0].referenced_table == "shop.orders"


def test_single_database_introspection_keeps_unqualified_names() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory

    # In single-database mode the connection's default schema is reflected
    # (schema=None) and names stay unqualified.
    catalog = {None: {"tables": {"orders": {}, "customers": {}}, "views": {}}}
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), "shop", is_mysql=False
    )

    assert {table.name for table in inventory.tables} == {"orders", "customers"}


# ---------------------------------------------------------------------------
# Property 1 — read-only source
# ---------------------------------------------------------------------------


def test_introspect_issues_no_write_or_ddl_statements() -> None:
    """Property 1: introspection must never issue a write/DDL to the source."""
    executed: list[str] = []
    introspector = SourceIntrospector(engine_factory=_sqlite_factory(record=executed))

    introspector.introspect(_source_config())

    assert executed  # statements were actually run
    offending = [sql for sql in executed if is_write_or_ddl(sql)]
    assert offending == [], f"read-only source issued write/DDL: {offending}"


def test_test_connection_issues_no_write_or_ddl_statements() -> None:
    """Property 1: the connection check must never issue a write/DDL."""
    executed: list[str] = []
    introspector = SourceIntrospector(engine_factory=_sqlite_factory(record=executed))

    introspector.test_connection(_source_config())

    offending = [sql for sql in executed if is_write_or_ddl(sql)]
    assert offending == []


def test_read_only_guard_blocks_write_attempt() -> None:
    """The guard refuses any write at the cursor level, enforcing Property 1."""
    engine = create_engine("sqlite://")
    install_read_only_guard(engine)
    with engine.connect() as connection:
        with pytest.raises(ReadOnlySourceError):
            connection.execute(text("CREATE TABLE evil (id INT)"))
        with pytest.raises(ReadOnlySourceError):
            connection.execute(text("INSERT INTO evil VALUES (1)"))


# ---------------------------------------------------------------------------
# test_connection captures the source server version (for the overview diagram)
# ---------------------------------------------------------------------------


class _VersionConn:
    """A fake connection returning a server version for ``SELECT VERSION()``."""

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN204
        return False

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement).strip().upper()

        class _R:
            def __init__(self, rows):  # noqa: ANN001
                self._rows = rows

            def first(self):  # noqa: ANN201
                return self._rows[0] if self._rows else None

        if "VERSION()" in sql:
            return _R([("8.0.mysql_aurora.3.04.0",)])
        if "AURORA_VERSION" in sql:
            return _R([("3.04.0",)])
        if "INNODB_VERSION" in sql:
            return _R([("8.0.42",)])
        return _R([(1,)])


class _VersionEngine:
    def connect(self):  # noqa: ANN201
        return _VersionConn()

    def dispose(self) -> None:
        pass


def test_test_connection_captures_server_version() -> None:
    introspector = SourceIntrospector(engine_factory=lambda _conn: _VersionEngine())
    result = introspector.test_connection(_source_config())
    assert result.success is True
    assert result.server_version == "8.0.mysql_aurora.3.04.0"
    assert result.mysql_version == "8.0.42"  # community engine version
    assert result.aurora_version == "3.04.0"  # from @@aurora_version


def test_test_connection_version_is_optional_when_unavailable() -> None:
    # The real sqlite test engine has no VERSION() function; the probe is best
    # effort, so the test still succeeds with no version captured.
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    result = introspector.test_connection(_source_config())
    assert result.success is True
    assert result.server_version is None
    assert result.mysql_version is None


# ---------------------------------------------------------------------------
# Source transient-error classification (Aurora failover mid-Full-Load)
# ---------------------------------------------------------------------------


def _pymysql_operational(code: int, message: str) -> Exception:
    """A stand-in for PyMySQL's OperationalError(code, message) shape."""

    class OperationalError(Exception):
        pass

    return OperationalError(code, message)


def test_source_transient_error_recognizes_failover_error_codes() -> None:
    # An Aurora failover closes every open connection; the driver reports one of
    # these codes. Each must be recognized so the table is RE-READ, not failed.
    from dsql_migrator.core.introspector import is_source_transient_error

    assert is_source_transient_error(
        _pymysql_operational(2013, "Lost connection to MySQL server during query")
    )
    assert is_source_transient_error(
        _pymysql_operational(2006, "MySQL server has gone away")
    )
    assert is_source_transient_error(
        _pymysql_operational(2003, "Can't connect to MySQL server on 'host'")
    )
    assert is_source_transient_error(
        _pymysql_operational(1053, "Server shutdown in progress")
    )
    assert is_source_transient_error(
        _pymysql_operational(1927, "Connection was killed")
    )


def test_source_transient_error_unwraps_sqlalchemy_and_cause() -> None:
    # SQLAlchemy wraps the driver error; the code lives on .orig. A re-raised error
    # may only carry __cause__. Both must still classify.
    from dsql_migrator.core.introspector import is_source_transient_error

    class DBAPIError(Exception):
        def __init__(self, orig):
            super().__init__("(pymysql.err.OperationalError) wrapped")
            self.orig = orig

    wrapped = DBAPIError(_pymysql_operational(2013, "Lost connection"))
    assert is_source_transient_error(wrapped) is True

    chained = RuntimeError("export failed")
    chained.__cause__ = _pymysql_operational(2006, "gone away")
    assert is_source_transient_error(chained) is True


def test_source_transient_error_recognizes_socket_timeout() -> None:
    # A failover can stall the socket rather than reset it; the bounded read_timeout
    # then raises a timeout, which carries no MySQL error code.
    import socket

    from dsql_migrator.core.introspector import is_source_transient_error

    assert is_source_transient_error(socket.timeout("timed out")) is True
    assert is_source_transient_error(TimeoutError("read timed out")) is True


def test_source_transient_error_rejects_data_and_schema_errors() -> None:
    # A data/schema error fails identically forever, so retrying it would only add
    # delay before the same failure. These must NOT be classified as transient.
    from dsql_migrator.core.introspector import is_source_transient_error

    assert is_source_transient_error(
        _pymysql_operational(1054, "Unknown column 'x' in 'field list'")
    ) is False
    assert is_source_transient_error(
        _pymysql_operational(1146, "Table 'db.t' doesn't exist")
    ) is False
    assert is_source_transient_error(ValueError("bad value for column id")) is False
    assert is_source_transient_error(KeyError("missing_key")) is False
    # A permission error is a real configuration problem, not a blip.
    assert is_source_transient_error(
        _pymysql_operational(1045, "Access denied for user")
    ) is False


def test_source_error_hint_explains_failover_and_reassures() -> None:
    # Part A: the raw driver text tells the operator nothing, so a dropped source
    # connection gets a what-happened / what-to-do-next explanation.
    from dsql_migrator.core.introspector import is_source_transient_error, source_error_hint

    hint = source_error_hint(
        _pymysql_operational(2013, "Lost connection to MySQL server during query")
    )
    assert hint is not None
    assert "failover" in hint.lower()
    # It must state the two things the user needs to trust a re-run.
    assert "idempotent" in hint.lower()
    assert "never duplicates" in hint.lower() or "no duplicates" in hint.lower()
    # And that the source was not modified (the load only reads it).
    assert "only reads" in hint.lower() or "was changed" in hint.lower()

    # No hint invented for an unrelated error (it would be misleading).
    assert source_error_hint(_pymysql_operational(1054, "Unknown column")) is None
    assert is_source_transient_error(_pymysql_operational(1054, "Unknown column")) is False
