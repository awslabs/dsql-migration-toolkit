# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ``SourceDialect`` adapter and its ``dialect_for`` registry (Phase 0).

Phase 0 introduces the seam with MySQL as the sole, byte-identical dialect: it must
report the same driver scheme / system schemas / engine kwargs the introspector used
inline before, so routing the engine factories through it changes nothing.
"""

import pytest

from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect import (
    MySQLSourceDialect,
    PostgresSourceDialect,
    dialect_for,
)


def test_dialect_for_mysql_returns_mysql_dialect() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert isinstance(d, MySQLSourceDialect)
    assert d.source_type is SourceType.MYSQL


def test_dialect_for_is_a_singleton() -> None:
    assert dialect_for(SourceType.MYSQL) is dialect_for(SourceType.MYSQL)


def test_mysql_dialect_connection_constants_match_introspector() -> None:
    from dsql_migrator.core.introspector import MYSQL_DRIVER, MYSQL_SYSTEM_SCHEMAS

    d = dialect_for(SourceType.MYSQL)
    assert d.driver_scheme == MYSQL_DRIVER == "mysql+pymysql"
    assert d.default_port == 3306
    assert d.system_schemas == MYSQL_SYSTEM_SCHEMAS
    assert "information_schema" in d.system_schemas


def test_mysql_dialect_engine_kwargs_match_source_engine_kwargs() -> None:
    from dsql_migrator.core.introspector import source_engine_kwargs

    d = dialect_for(SourceType.MYSQL)
    assert d.engine_kwargs() == source_engine_kwargs()
    assert d.engine_kwargs(read_timeout_seconds=30) == source_engine_kwargs(
        read_timeout_seconds=30
    )


def test_dialect_for_postgres_returns_postgres_dialect() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert isinstance(d, PostgresSourceDialect)
    assert d.source_type is SourceType.POSTGRES
    assert dialect_for(SourceType.POSTGRES) is d  # singleton


def test_postgres_dialect_connection_constants() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert d.driver_scheme == "postgresql+psycopg"
    assert d.default_port == 5432
    assert d.system_schemas == frozenset(
        {"pg_catalog", "information_schema", "pg_toast"}
    )
    assert {"integer", "bigint", "smallint"} <= d.integer_pk_types
    assert "varchar" not in d.integer_pk_types


def test_postgres_dialect_quoting_is_double_quote() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert d.quote_identifier("id") == '"id"'
    assert d.quote_identifier('we"ird') == '"we""ird"'  # embedded double-quote doubled
    assert d.quote_table("app.orders") == '"app"."orders"'
    assert d.quote_table("orders") == '"orders"'
    assert d.quote_table("app.a.b") == '"app"."a.b"'  # split on first dot only


def test_postgres_dialect_snapshot_and_select_column() -> None:
    from dsql_migrator.core.models import ColumnDef

    d = dialect_for(SourceType.POSTGRES)
    assert d.snapshot_start_sql == "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    # v1: plain quoted column (no spatial ST_AsBinary special-case).
    assert d.select_column_sql(ColumnDef(name="geom", mysql_type="geometry")) == '"geom"'


def test_postgres_dialect_engine_kwargs_pin_utc_and_optional_statement_timeout() -> None:
    d = dialect_for(SourceType.POSTGRES)
    base = d.engine_kwargs()
    assert base["pool_pre_ping"] is True
    assert base["connect_args"]["options"] == "-c timezone=UTC"
    assert "connect_timeout" in base["connect_args"]
    timed = d.engine_kwargs(read_timeout_seconds=30)
    assert "statement_timeout=30000" in timed["connect_args"]["options"]


def test_postgres_dialect_enrich_is_v1_noop() -> None:
    # v1: structure comes from SQLAlchemy reflection; no pg_catalog enrichment yet.
    assert dialect_for(SourceType.POSTGRES).enrich(object(), "app", []) == ([], [], [])


def test_postgres_dialect_value_converter_is_a_phase2_stub() -> None:
    # Full Load value conversion for PG is Phase 2; it must fail loudly (never silently
    # mis-convert), so it raises rather than returning a half-baked converter.
    with pytest.raises(NotImplementedError):
        dialect_for(SourceType.POSTGRES).value_converter(object())


def test_mysql_dialect_quoting() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert d.quote_identifier("id") == "`id`"
    assert d.quote_identifier("a`b") == "`a``b`"  # embedded backtick doubled
    assert d.quote_table("db.tbl") == "`db`.`tbl`"
    assert d.quote_table("plain") == "`plain`"
    assert d.quote_table("db.tbl.extra") == "`db`.`tbl.extra`"  # split on first dot


def test_mysql_dialect_integer_pk_types() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert {"int", "bigint", "tinyint"} <= d.integer_pk_types
    assert "varchar" not in d.integer_pk_types


def test_exporter_quote_helpers_delegate_to_the_mysql_dialect() -> None:
    # The exporter's module helpers now delegate to the dialect (single source of
    # truth), so they must produce identical output.
    from dsql_migrator.core.exporter import (
        _quote_mysql_identifier,
        _quote_mysql_table,
    )

    d = dialect_for(SourceType.MYSQL)
    assert _quote_mysql_identifier("a`b") == d.quote_identifier("a`b")
    assert _quote_mysql_table("db.t") == d.quote_table("db.t")


def test_mysql_dialect_snapshot_sql_matches_watermark_constant() -> None:
    from dsql_migrator.core.watermark import START_CONSISTENT_SNAPSHOT

    assert dialect_for(SourceType.MYSQL).snapshot_start_sql == START_CONSISTENT_SNAPSHOT


def test_mysql_dialect_value_converter_returns_mysql_value_converter() -> None:
    from dsql_migrator.core.exporter import ValueConverter
    from dsql_migrator.core.models import ColumnDef, TableDef

    table = TableDef(
        name="t", columns=[ColumnDef(name="id", mysql_type="int")], primary_key=["id"]
    )
    vc = dialect_for(SourceType.MYSQL).value_converter(table)
    assert isinstance(vc, ValueConverter)


class _FakeDialectName:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEnrichConnection:
    """Minimal connection whose ``dialect.name`` decides the enrich branch."""

    def __init__(self, dialect_name: str) -> None:
        self.dialect = _FakeDialectName(dialect_name)


def test_mysql_dialect_enrich_orchestration_calls_and_order(monkeypatch) -> None:
    # Guards MySQLSourceDialect.enrich's ACTIVE branch: it must run all three in-place
    # enrichers and return (triggers, routines, events) IN THAT ORDER. A reorder, a
    # dropped call, or a missing collect_* would otherwise pass silently (the enrich_* /
    # collect_* helpers are tested directly elsewhere, but this orchestration was not).
    import dsql_migrator.core.introspector as intro
    from dsql_migrator.core.source_dialect import MySQLSourceDialect

    calls: list[tuple[str, object]] = []

    def _spy(name):
        def fn(connection, enrich_db, tables):
            calls.append((name, enrich_db))
        return fn

    monkeypatch.setattr(intro, "enrich_columns", _spy("columns"))
    monkeypatch.setattr(intro, "enrich_index_types", _spy("indexes"))
    monkeypatch.setattr(intro, "enrich_partitions", _spy("partitions"))
    monkeypatch.setattr(intro, "collect_triggers", lambda c, db: ["TRG"])
    monkeypatch.setattr(intro, "collect_routines", lambda c, db: ["ROUT"])
    monkeypatch.setattr(intro, "collect_events", lambda c, db: ["EVT"])

    triggers, routines, events = MySQLSourceDialect().enrich(
        _FakeEnrichConnection("mysql"), "app", []
    )
    # Return-tuple order (a swap would mis-file routines as triggers, etc.).
    assert triggers == ["TRG"]
    assert routines == ["ROUT"]
    assert events == ["EVT"]
    # All three in-place enrichers ran, against the right schema (a dropped call fails).
    assert [name for name, _ in calls] == ["columns", "indexes", "partitions"]
    assert all(db == "app" for _, db in calls)


def test_mysql_dialect_enrich_no_ops_on_non_mysql_connection(monkeypatch) -> None:
    # A non-MySQL connection (e.g. the SQLite test double) must skip enrichment entirely.
    import dsql_migrator.core.introspector as intro
    from dsql_migrator.core.source_dialect import MySQLSourceDialect

    def _boom(*_a, **_k):
        raise AssertionError("enrich ran information_schema queries on a non-MySQL conn")

    for fn in (
        "enrich_columns",
        "enrich_index_types",
        "enrich_partitions",
        "collect_triggers",
        "collect_routines",
        "collect_events",
    ):
        monkeypatch.setattr(intro, fn, _boom)

    assert MySQLSourceDialect().enrich(
        _FakeEnrichConnection("sqlite"), "app", []
    ) == ([], [], [])
