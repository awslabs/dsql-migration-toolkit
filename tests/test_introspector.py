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
    SourceType,
    TableDef,
)
from dsql_migrator.core.source_dialect import dialect_for


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


class _DefEngine:
    """Fake engine whose connection returns a canned definition-query result.

    ``mysql``: ``.execute(text).mappings().first()`` -> a column mapping.
    ``pg``:    ``.execute(text, params).first()`` -> a 1-tuple row.
    """

    def __init__(self, *, mysql=None, pg=None):
        self._mysql, self._pg = mysql, pg

    def connect(self):
        engine = self

        class _Res:
            def __init__(self, mapping=None, row=None):
                self._mapping, self._row = mapping, row

            def mappings(self):
                return self

            def first(self):
                return self._mapping if self._mapping is not None else self._row

        class _Conn:
            def __enter__(self_):
                return self_

            def __exit__(self_, *a):
                return False

            def execute(self_, statement, parameters=None):
                sql = str(statement)
                if engine._mysql and sql.startswith("SHOW CREATE") and any(
                    k in sql for k in engine._mysql
                ):
                    col, val = next(
                        (c, v) for k, (c, v) in engine._mysql.items() if k in sql
                    )
                    return _Res(mapping={col: val})
                if engine._pg and any(fn in sql for fn in engine._pg):
                    fn = next(f for f in engine._pg if f in sql)
                    return _Res(row=(engine._pg[fn],))
                return _Res(mapping=None, row=None)

        return _Conn()

    def dispose(self):
        pass


def test_fetch_object_definition_mysql_show_create_procedure() -> None:
    from dsql_migrator.core.models import ObjectType, SourceType

    body = "CREATE DEFINER=`root`@`%` PROCEDURE `app`.`sp_x`() BEGIN SELECT 1; END"
    intro = SourceIntrospector(
        engine_factory=lambda _c: _DefEngine(
            mysql={"PROCEDURE": ("Create Procedure", body)}
        )
    )
    conn = SourceConnectionConfig(host="h", database="app", source_type=SourceType.MYSQL)
    assert intro.fetch_object_definition(conn, "app.sp_x", ObjectType.PROCEDURE) == body


def test_fetch_object_definition_postgres_functiondef() -> None:
    from dsql_migrator.core.models import ObjectType, SourceType

    body = "CREATE OR REPLACE FUNCTION app.fn_x() RETURNS integer AS $$ ... $$;"
    intro = SourceIntrospector(
        engine_factory=lambda _c: _DefEngine(pg={"pg_get_functiondef": body})
    )
    conn = SourceConnectionConfig(
        host="h", database="app", source_type=SourceType.POSTGRES
    )
    assert intro.fetch_object_definition(conn, "app.fn_x", ObjectType.FUNCTION) == body


def test_fetch_object_definition_is_best_effort_and_identifier_safe() -> None:
    from dsql_migrator.core.introspector import _quote_mysql_identifier
    from dsql_migrator.core.models import ObjectType, SourceType

    # Injection-proof identifier quoting.
    assert _quote_mysql_identifier("sp_x") == "`sp_x`"
    assert _quote_mysql_identifier("app.sp_x") == "`app`.`sp_x`"
    assert _quote_mysql_identifier("bad`name") is None
    assert _quote_mysql_identifier("x; DROP TABLE y") is None
    assert _quote_mysql_identifier("a.b.c") is None

    # Missing object / empty result -> None (caller falls back to name-only).
    intro = SourceIntrospector(engine_factory=lambda _c: _DefEngine(mysql={}))
    conn = SourceConnectionConfig(host="h", database="app", source_type=SourceType.MYSQL)
    assert intro.fetch_object_definition(conn, "app.gone", ObjectType.PROCEDURE) is None


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
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE, COLUMN_DEFAULT)
        ("orders", "id", None, "auto_increment", "int unsigned", None),
        ("orders", "code", "utf8mb4_general_ci", "", "varchar(20)", "AB"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)

    table = tables[0]
    assert table.auto_increment_column == "id"
    code_column = next(c for c in table.columns if c.name == "code")
    assert code_column.collation == "utf8mb4_general_ci"
    # COLUMN_TYPE overrides the (lossy) reflected mysql_type, preserving unsigned.
    id_column = next(c for c in table.columns if c.name == "id")
    assert id_column.mysql_type == "int unsigned"
    # COLUMN_DEFAULT is the authoritative default source (see the test below for why),
    # and it arrives UNQUOTED -- "AB", not "'AB'".
    assert code_column.default == "AB"
    assert id_column.default is None


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
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE, COLUMN_DEFAULT)
        ("t", "flag", None, "", "tinyint(1)", "1"),
        ("t", "qty", None, "", "smallint unsigned", "0"),
        ("t", "big", None, "", "int unsigned", None),
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
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE, COLUMN_DEFAULT)
        ("orders", "id", None, "auto_increment", "int", None),
        ("orders", "total", None, "STORED GENERATED", "decimal(10,2)", None),
        # Note what information_schema gives us here that SHOW CREATE TABLE does not:
        # the ON UPDATE clause stays in EXTRA, so COLUMN_DEFAULT is just the default.
        ("orders", "updated_at", None,
         "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "timestamp",
         "CURRENT_TIMESTAMP"),
        # DEFAULT_GENERATED alone (expression default) must NOT be a generated col.
        ("orders", "created_at", None, "DEFAULT_GENERATED", "timestamp",
         "CURRENT_TIMESTAMP"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)

    by_name = {c.name: c for c in tables[0].columns}
    assert by_name["total"].generated is True
    assert by_name["updated_at"].auto_update_timestamp is True
    assert by_name["created_at"].generated is False
    assert by_name["created_at"].auto_update_timestamp is False
    # DEFAULT_GENERATED marks the default as an EXPRESSION, which is what tells a bare
    # CURRENT_TIMESTAMP apart from the literal string "CURRENT_TIMESTAMP" now that
    # COLUMN_DEFAULT arrives unquoted.
    assert by_name["created_at"].default_is_expression is True
    assert by_name["created_at"].default == "CURRENT_TIMESTAMP"
    assert by_name["total"].default_is_expression is False
    # And the ON UPDATE half is NOT folded into the default -- emitting it verbatim
    # would be a target syntax error.
    assert by_name["updated_at"].default == "CURRENT_TIMESTAMP"
    assert "ON UPDATE" not in (by_name["updated_at"].default or "")


def test_enrich_columns_classifies_temporal_function_default_on_mysql_57() -> None:
    """MySQL < 8.0.13 (e.g. 5.7) has no ``DEFAULT_GENERATED`` flag in EXTRA.

    A temporal FUNCTION default must still be classified as an expression -- otherwise
    the converter quotes it and the target CREATE fails ("invalid input syntax for type
    timestamp: 'CURRENT_TIMESTAMP(6)'"). The classification is scoped to temporal columns
    so a genuine VARCHAR literal that merely reads like a function is NOT misclassified.
    """
    tables = [
        TableDef(
            name="t",
            columns=[
                ColumnDef(name="created", mysql_type="datetime(6)"),
                ColumnDef(name="ts", mysql_type="timestamp"),
                ColumnDef(name="label", mysql_type="varchar(50)"),
            ],
        )
    ]
    rows = [
        # (TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE, COLUMN_DEFAULT)
        # 5.7: EXTRA is empty (the 8.0.13+ DEFAULT_GENERATED flag does not exist).
        ("t", "created", None, "", "datetime(6)", "CURRENT_TIMESTAMP(6)"),
        ("t", "ts", None, "", "timestamp", "CURRENT_TIMESTAMP"),
        # A VARCHAR literal that happens to read like a function must stay a literal.
        ("t", "label", "utf8mb4_general_ci", "", "varchar(50)", "NOW()"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)

    by = {c.name: c for c in tables[0].columns}
    assert by["created"].default_is_expression is True
    assert by["created"].default == "CURRENT_TIMESTAMP(6)"
    assert by["ts"].default_is_expression is True
    assert by["label"].default_is_expression is False  # temporal-scoped: literal untouched


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


def test_enrich_index_types_flags_mixed_expression_index() -> None:
    """A composite index mixing a plain column with an EXPRESSION key-part reflects
    with only its plain column(s) (SQLAlchemy drops the expression part). Emitting that
    narrower index would be WRONG -- for a UNIQUE index it changes uniqueness semantics.
    ``enrich_index_types`` catches it by comparing the reflected column count to the true
    key-part count from information_schema.STATISTICS and flags it like an all-expression
    index. A normal single-/multi-column index must NOT be falsely flagged.
    """
    tables = [
        TableDef(
            name="accounts",
            columns=[ColumnDef(name="id", mysql_type="INT")],
            indexes=[
                # KEY (tenant_id, (lower(email))) reflects with just ['tenant_id'];
                # STATISTICS still has 2 key-part rows for it.
                IndexDef(
                    name="uq_tenant_email", columns=["tenant_id"], unique=True
                ),
                # A plain single-column index: 1 reflected column, 1 STATISTICS row.
                IndexDef(name="ix_name", columns=["name"]),
            ],
        )
    ]
    rows = [
        # (TABLE_NAME, INDEX_NAME, INDEX_TYPE) -- one row per key-part on 8.0.13+.
        ("accounts", "uq_tenant_email", "BTREE"),  # key-part 1: tenant_id
        ("accounts", "uq_tenant_email", "BTREE"),  # key-part 2: (lower(email)) expr
        ("accounts", "ix_name", "BTREE"),
    ]
    enrich_index_types(_FakeConnection(rows), "app", tables)

    table = tables[0]
    # The mixed index is flagged (treated like an all-expression index)...
    assert "uq_tenant_email" in table.expression_indexes
    # ...and the plain index is NOT falsely flagged (its counts match).
    assert "ix_name" not in table.expression_indexes


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
    """A connection whose dialect is not MySQL (skips information_schema enrich).

    ``schemas`` (optional) is what the PostgreSQL dialect's ``list_schemas`` catalog
    query returns via ``execute(...).mappings()`` -- so the PG reflection path (which no
    longer trusts SQLAlchemy's over-filtering ``get_schema_names()``) can be exercised.
    """

    class _Dialect:
        name = "sqlite"

    dialect = _Dialect()

    def __init__(self, schemas=None) -> None:  # noqa: ANN001
        self._schemas = schemas

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        rows = [{"nspname": s} for s in (self._schemas or [])]

        class _R:
            def mappings(self_):  # noqa: ANN001, ANN202, N805
                return rows

        return _R()


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
        _FakeInspector(catalog), _NonMysqlConnection(), None, dialect=dialect_for(SourceType.MYSQL)
    )

    table_names = {table.name for table in inventory.tables}
    # Names are qualified with their schema and system schemas are excluded.
    assert table_names == {"shop.orders", "shop.customers", "billing.invoices"}
    assert {view.name for view in inventory.views} == {"shop.active"}
    assert "mysql.user" not in table_names


def test_postgres_reflects_all_schemas_even_with_database_set() -> None:
    # A PostgreSQL source's `database` is the connection, not a schema (database_is_schema
    # is False), so _assemble_inventory must reflect ALL non-system schemas, qualified --
    # never just `public`. Regression: a set database used to reflect only the default
    # schema, silently dropping a non-public schema (e.g. `app`).
    from dsql_migrator.core.introspector import _assemble_inventory

    catalog = {
        "pg_catalog": {"tables": {"pg_class": {}}},  # system -> skipped
        "information_schema": {"tables": {"tables": {}}},  # system -> skipped
        "public": {"tables": {"docs": {}}},
        "app": {"tables": {"orders": {}, "items": {}}},
    }
    inventory = _assemble_inventory(
        _FakeInspector(catalog),
        # PG lists schemas from the connection (pg_namespace), not the inspector.
        _NonMysqlConnection(schemas=list(catalog.keys())),
        "mydb",  # database SET -- for PG this is the connection, not a single schema
        dialect=dialect_for(SourceType.POSTGRES),
    )
    table_names = {table.name for table in inventory.tables}
    # Every non-system schema reflected AND schema-qualified (app not dropped).
    assert table_names == {"public.docs", "app.orders", "app.items"}


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
        _FakeInspector(catalog), _NonMysqlConnection(), None, dialect=dialect_for(SourceType.MYSQL)
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
        _FakeInspector(catalog), _NonMysqlConnection(), None, dialect=dialect_for(SourceType.MYSQL)
    )
    orders = next(t for t in inventory.tables if t.name == "shop.orders")
    assert orders.foreign_keys[0].referenced_table == "shop.orders"


def test_single_database_introspection_keeps_unqualified_names() -> None:
    from dsql_migrator.core.introspector import _assemble_inventory

    # In single-database mode the connection's default schema is reflected
    # (schema=None) and names stay unqualified.
    catalog = {None: {"tables": {"orders": {}, "customers": {}}, "views": {}}}
    inventory = _assemble_inventory(
        _FakeInspector(catalog), _NonMysqlConnection(), "shop", dialect=dialect_for(SourceType.MYSQL)
    )

    assert {table.name for table in inventory.tables} == {"orders", "customers"}


# ---------------------------------------------------------------------------
# View reflection is best-effort (one broken view must not abort the inventory)
# ---------------------------------------------------------------------------


class _BrokenViewInspector:
    """Inspector where one view's ``get_view_definition`` raises (a broken view)."""

    def get_view_names(self, schema=None):  # noqa: ANN001, ANN201
        return ["good_view", "broken_view"]

    def get_view_definition(self, view_name, schema=None):  # noqa: ANN001, ANN201
        if view_name == "broken_view":
            # A view whose underlying table was dropped, or a per-view privilege error.
            raise RuntimeError("View 'app.broken_view' references an invalid table")
        return "SELECT id FROM orders"


def test_reflect_views_skips_a_view_that_cannot_be_shown() -> None:
    # One un-SHOW-CREATE-able view must be SKIPPED (empty definition, name kept), not
    # abort reflection of the other views.
    from dsql_migrator.core.introspector import _reflect_views

    views = _reflect_views(_BrokenViewInspector())
    by_name = {v.name: v.definition for v in views}
    assert by_name["good_view"] == "SELECT id FROM orders"
    assert by_name["broken_view"] == ""  # skipped, not fatal


def test_reflect_views_does_not_swallow_read_only_guard() -> None:
    # The read-only guard sentinel must never be masked as a "bad view" (Property 1).
    from dsql_migrator.core.introspector import _reflect_views

    class _GuardTrippingInspector:
        def get_view_names(self, schema=None):  # noqa: ANN001, ANN201
            return ["v"]

        def get_view_definition(self, view_name, schema=None):  # noqa: ANN001, ANN201
            raise ReadOnlySourceError("refused non-read statement on read-only source")

    with pytest.raises(ReadOnlySourceError):
        _reflect_views(_GuardTrippingInspector())


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
    assert result.engine_version == "8.0.42"  # community engine version
    assert result.aurora_version == "3.04.0"  # from @@aurora_version


def test_test_connection_version_is_optional_when_unavailable() -> None:
    # The real sqlite test engine has no VERSION() function; the probe is best
    # effort, so the test still succeeds with no version captured.
    introspector = SourceIntrospector(engine_factory=_sqlite_factory())
    result = introspector.test_connection(_source_config())
    assert result.success is True
    assert result.server_version is None
    assert result.engine_version is None


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


class _PgErr(Exception):
    """A psycopg-shaped error carrying a string ``.sqlstate``."""

    def __init__(self, sqlstate: str, message: str = "") -> None:
        super().__init__(message or sqlstate)
        self.sqlstate = sqlstate


def test_is_source_transient_error_dispatches_by_source_type() -> None:
    from dsql_migrator.core.introspector import is_source_transient_error
    from dsql_migrator.core.models import SourceType

    failover = _PgErr("57P03", "cannot connect now, the DB is starting up")
    # Under PostgreSQL dispatch a PG failover SQLSTATE is transient (auto-retried)...
    assert is_source_transient_error(failover, SourceType.POSTGRES) is True
    # ...but the default (MySQL) classifier never fires for a string SQLSTATE -- which is
    # exactly the bug this dispatch fixes (a PG failover would otherwise not auto-retry).
    assert is_source_transient_error(failover) is False
    assert is_source_transient_error(failover, SourceType.MYSQL) is False
    # A PG data error is NOT transient under PG dispatch.
    assert is_source_transient_error(_PgErr("23505", "dup key"), SourceType.POSTGRES) is False


def test_source_error_hint_is_worded_for_the_source_engine() -> None:
    from dsql_migrator.core.introspector import (
        SOURCE_CONNECTION_LOST_HINT,
        source_error_hint,
    )
    from dsql_migrator.core.models import SourceType

    # PostgreSQL: the dropped-connection hint names PostgreSQL, not MySQL.
    pg_hint = source_error_hint(_PgErr("08006", "connection failure"), SourceType.POSTGRES)
    assert pg_hint is not None
    assert "PostgreSQL" in pg_hint and "MySQL" not in pg_hint
    assert "idempotent" in pg_hint.lower()
    # PostgreSQL too-many-connections (53300) gets the connection-limit hint, PG-worded.
    pg_toomany = source_error_hint(
        _PgErr("53300", "too many clients already"), SourceType.POSTGRES
    )
    assert pg_toomany is not None
    assert "PostgreSQL" in pg_toomany and "connection" in pg_toomany.lower()
    # Back-compat: the MySQL-rendered constant is unchanged and the default dispatch
    # still words the hint for MySQL.
    assert SOURCE_CONNECTION_LOST_HINT.startswith("The source MySQL connection dropped")
    mysql_hint = source_error_hint(_pymysql_operational(2013, "Lost connection"))
    assert mysql_hint is not None and "MySQL" in mysql_hint and "PostgreSQL" not in mysql_hint
