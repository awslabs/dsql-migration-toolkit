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
from dsql_migrator.core.source_dialect.base import SourceVersions


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
    opts = base["connect_args"]["options"]
    # Pin the locale/format GUCs so the source renders checksum text identically to the
    # DSQL target (whose defaults are exactly these); a non-default source locale would
    # otherwise cause a false checksum MISMATCH on byte-identical data.
    assert "-c timezone=UTC" in opts
    assert "-c datestyle=ISO" in opts
    assert "-c intervalstyle=postgres" in opts
    assert "-c lc_numeric=C" in opts
    assert "connect_timeout" in base["connect_args"]
    timed = d.engine_kwargs(read_timeout_seconds=30)
    assert "statement_timeout=30000" in timed["connect_args"]["options"]


def test_postgres_dialect_enrich_noops_on_non_pg_connection() -> None:
    # A non-PostgreSQL connection (no .dialect / not "postgresql") must skip the
    # pg_catalog query entirely -- e.g. the SQLite test double or a bare object.
    assert dialect_for(SourceType.POSTGRES).enrich(object(), "app", []) == ([], [], [])


class _FakePgMappings:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _FakePgConnection:
    """A connection whose dialect is PostgreSQL, returning canned format_type rows."""

    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        return _FakePgMappings(self._rows)


def test_postgres_dialect_enrich_captures_exact_pg_types() -> None:
    # enrich overwrites each reflected column type with format_type's exact string, so a
    # text[] shows as "text[]" (not the lossy "ARRAY") and numeric keeps its precision;
    # a column absent from the catalog result is left unchanged.
    from dsql_migrator.core.models import ColumnDef, TableDef

    conn = _FakePgConnection(
        [{"col": "tags", "typ": "text[]"}, {"col": "total", "typ": "numeric(12,2)"}]
    )
    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="tags", mysql_type="ARRAY"),
            ColumnDef(name="total", mysql_type="NUMERIC"),
            ColumnDef(name="note", mysql_type="TEXT"),
        ],
        primary_key=[],
    )
    result = dialect_for(SourceType.POSTGRES).enrich(conn, "shop", [table])
    assert result == ([], [], [])
    assert table.columns[0].mysql_type == "text[]"
    assert table.columns[1].mysql_type == "numeric(12,2)"
    assert table.columns[2].mysql_type == "TEXT"  # not in catalog rows -> unchanged


def test_postgres_dialect_value_converter_returns_pg_converter() -> None:
    # Phase 2: PG Full Load value conversion is implemented -- the dialect returns a
    # PostgresValueConverter (own module) exposing the convert_row/convert_value contract.
    from dsql_migrator.core.exporter_postgres import PostgresValueConverter
    from dsql_migrator.core.models import ColumnDef, TableDef

    table = TableDef(
        name="t",
        columns=[ColumnDef(name="id", mysql_type="integer")],
        primary_key=["id"],
    )
    vc = dialect_for(SourceType.POSTGRES).value_converter(table)
    assert isinstance(vc, PostgresValueConverter)
    assert vc.convert_row({"id": 1}) == {"id": 1}


# ---------------------------------------------------------------------------
# estimate_row_counts -- scan-free per-table row estimate (engine-specific catalog)
# ---------------------------------------------------------------------------


class _EstScalar:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


class _FakeEstimateConnection:
    """Answers the default-schema scalar and returns canned estimate rows.

    Records the estimate SQL so tests can assert the engine-specific catalog query.
    Rows are (schema, table, estimate) tuples as the real catalog would return.
    """

    def __init__(self, current_schema: object, rows: list) -> None:
        self._current_schema = current_schema
        self._rows = rows
        self.estimate_sql: str = ""

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        low = sql.lower()
        if "database()" in low or "current_schema()" in low:
            return _EstScalar(self._current_schema)
        self.estimate_sql = sql
        return list(self._rows)


def test_mysql_estimate_row_counts_uses_information_schema() -> None:
    conn = _FakeEstimateConnection("shop", [("shop", "orders", 100), ("shop", "items", 5)])
    out = dialect_for(SourceType.MYSQL).estimate_row_counts(conn, ["orders", "items"])
    assert out == {"orders": 100, "items": 5}
    assert "information_schema.tables" in conn.estimate_sql
    assert "table_rows" in conn.estimate_sql


def test_postgres_estimate_row_counts_uses_pg_class_reltuples() -> None:
    # reltuples estimates: positive -> int, -1 (never analyzed, PG14+) -> None,
    # NULL -> None, and a table missing from the result -> None.
    conn = _FakeEstimateConnection(
        "public",
        [("public", "orders", 100), ("public", "fresh", -1), ("public", "nully", None)],
    )
    out = dialect_for(SourceType.POSTGRES).estimate_row_counts(
        conn, ["orders", "fresh", "nully", "absent"]
    )
    assert out == {"orders": 100, "fresh": None, "nully": None, "absent": None}
    assert "pg_class" in conn.estimate_sql and "reltuples" in conn.estimate_sql
    assert "pg_namespace" in conn.estimate_sql
    assert "relkind" in conn.estimate_sql


def test_estimate_row_counts_empty_tables_short_circuits() -> None:
    conn = _FakeEstimateConnection("public", [])
    assert dialect_for(SourceType.POSTGRES).estimate_row_counts(conn, []) == {}
    assert conn.estimate_sql == ""  # no query issued


@pytest.mark.parametrize(
    "pg_type",
    ["json", "jsonb", "interval", "interval day to second", "interval second(3)"],
)
def test_postgres_select_column_casts_json_and_interval_to_text(pg_type: str) -> None:
    # json/jsonb/interval read via CAST(col AS text) so Full Load streams their exact
    # text (faithful + fast), aliased back to the column name so the row key is unchanged.
    from dsql_migrator.core.models import ColumnDef

    d = dialect_for(SourceType.POSTGRES)
    sql = d.select_column_sql(ColumnDef(name="c", mysql_type=pg_type))
    assert sql == 'CAST("c" AS text) AS "c"'


@pytest.mark.parametrize(
    "pg_type",
    ["integer", "bigint", "text", "numeric(12,2)", "uuid", "timestamp with time zone",
     "bytea", "boolean"],
)
def test_postgres_select_column_reads_scalars_as_is(pg_type: str) -> None:
    # Everything that binds natively is read as-is (just quoted) -- no needless cast.
    from dsql_migrator.core.models import ColumnDef

    d = dialect_for(SourceType.POSTGRES)
    assert d.select_column_sql(ColumnDef(name="c", mysql_type=pg_type)) == '"c"'


# ---------------------------------------------------------------------------
# probe_versions -- best-effort source version metadata for the overview diagram
# ---------------------------------------------------------------------------


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def first(self):  # noqa: ANN201
        return (self._value,) if self._value is not None else None


class _FakeVersionConnection:
    """Dispatches a version probe to the first matching canned scalar.

    ``responses`` is a list of (uppercased-substring, value): the first token found
    in the (upper-cased) SQL wins. The plain-version probe is keyed on ``" VERSION()"``
    (leading space) so it does NOT spuriously match ``AURORA_VERSION()``. A probe with
    no canned match raises -- proving each probe is isolated (a failing one must not
    sink the others).
    """

    def __init__(self, responses: list[tuple[str, object]]) -> None:
        self._responses = responses

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement).strip().upper()
        for token, value in self._responses:
            if token in sql:
                return _ScalarResult(value)
        raise RuntimeError(f"no canned response for {sql!r}")


def test_mysql_dialect_probe_versions() -> None:
    # MySQL reads VERSION() / @@innodb_version / @@aurora_version, each mapped to the
    # matching SourceVersions field.
    conn = _FakeVersionConnection(
        [
            ("INNODB_VERSION", "8.0.42"),
            ("AURORA_VERSION", "3.07.1"),
            (" VERSION()", "8.0.mysql_aurora.3.07.1"),
        ]
    )
    versions = dialect_for(SourceType.MYSQL).probe_versions(conn)
    assert versions == SourceVersions(
        server_version="8.0.mysql_aurora.3.07.1",
        engine_version="8.0.42",
        aurora_version="3.07.1",
    )


def test_postgres_dialect_probe_versions() -> None:
    # PostgreSQL reads version() (verbose) / SHOW server_version / aurora_version()
    # (Aurora only). The SHOW server_version packaging suffix ("16.10 (Homebrew)") is
    # stripped to a clean "16.10". AURORA_VERSION is matched before VERSION() so
    # aurora_version() is not mis-read as the verbose banner.
    conn = _FakeVersionConnection(
        [
            ("AURORA_VERSION", "16.4"),
            ("SERVER_VERSION", "16.10 (Homebrew)"),
            (" VERSION()", "PostgreSQL 16.10 on aarch64-apple-darwin"),
        ]
    )
    versions = dialect_for(SourceType.POSTGRES).probe_versions(conn)
    assert versions == SourceVersions(
        server_version="PostgreSQL 16.10 on aarch64-apple-darwin",
        engine_version="16.10",  # packaging suffix "(Homebrew)" stripped
        aurora_version="16.4",
    )


def test_probe_versions_are_best_effort_and_isolated() -> None:
    # A community/RDS PostgreSQL source has no aurora_version() function (that probe
    # raises); it must best-effort to None WITHOUT losing the versions that succeeded.
    conn = _FakeVersionConnection(
        [("SERVER_VERSION", "16.4"), (" VERSION()", "PostgreSQL 16.4")]
    )
    versions = dialect_for(SourceType.POSTGRES).probe_versions(conn)
    assert versions.engine_version == "16.4"
    assert versions.server_version == "PostgreSQL 16.4"
    assert versions.aurora_version is None  # aurora_version() probe raised -> None


def test_probe_versions_all_none_when_every_probe_fails() -> None:
    # An engine/connection that answers nothing (every probe raises) yields all-None
    # -- the connection test itself must still succeed (metadata is optional).
    conn = _FakeVersionConnection([])
    assert dialect_for(SourceType.MYSQL).probe_versions(conn) == SourceVersions()


# ---------------------------------------------------------------------------
# probe_grants -- engine-specific source privilege probe (feeds the "required
# privileges" Full Load prerequisite). PostgreSQL has no SHOW GRANTS, so running
# MySQL's statement against it would empty-out to a FALSE "SELECT missing" FAIL.
# ---------------------------------------------------------------------------


class _GrantsResult:
    def __init__(self, scalar_value=None, rows=None) -> None:
        self._scalar = scalar_value
        self._rows = rows or []

    def scalar(self):  # noqa: ANN201
        return self._scalar

    def fetchall(self):  # noqa: ANN201
        return self._rows


class _FakeGrantsConnection:
    """Dispatches a grant probe by SQL substring; an unmatched query raises.

    ``super`` answers ``current_setting('is_superuser')``; ``pg_rows`` answers the
    ``role_table_grants`` query; ``mysql_rows`` answers ``SHOW GRANTS``. A query with
    no configured answer raises, proving nothing else is issued.
    """

    def __init__(self, *, super=None, pg_rows=None, mysql_rows=None) -> None:
        self._super = super
        self._pg_rows = pg_rows
        self._mysql_rows = mysql_rows

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement).upper()
        if "IS_SUPERUSER" in sql:
            return _GrantsResult(scalar_value=self._super)
        if "ROLE_TABLE_GRANTS" in sql:
            return _GrantsResult(rows=self._pg_rows or [])
        if "SHOW GRANTS" in sql:
            return _GrantsResult(rows=self._mysql_rows or [])
        raise RuntimeError(f"unexpected grant probe SQL: {sql!r}")


def test_mysql_probe_grants_reads_show_grants() -> None:
    conn = _FakeGrantsConnection(
        mysql_rows=[("GRANT SELECT, INSERT ON `shop`.* TO `migrator`@`%`",)]
    )
    grants = dialect_for(SourceType.MYSQL).probe_grants(conn)
    assert grants == ["GRANT SELECT, INSERT ON `shop`.* TO `migrator`@`%`"]


def test_mysql_probe_grants_empty_on_error() -> None:
    class _Boom:
        def execute(self, *_a, **_k):  # noqa: ANN002, ANN003, ANN201
            raise RuntimeError("SHOW GRANTS failed")

    assert dialect_for(SourceType.MYSQL).probe_grants(_Boom()) == []


def test_postgres_probe_grants_superuser_reports_all_privileges() -> None:
    # A PG superuser bypasses every privilege check -> ALL PRIVILEGES (which satisfies
    # any requirement), WITHOUT ever querying role_table_grants (pg_rows unset -> would
    # raise if reached).
    conn = _FakeGrantsConnection(super="on")
    assert dialect_for(SourceType.POSTGRES).probe_grants(conn) == ["ALL PRIVILEGES"]


def test_postgres_probe_grants_non_superuser_lists_table_privileges() -> None:
    # A non-superuser's grants come from role_table_grants; SELECT being present makes
    # the Full Load privilege check pass.
    conn = _FakeGrantsConnection(super="off", pg_rows=[("SELECT",), ("INSERT",)])
    grants = dialect_for(SourceType.POSTGRES).probe_grants(conn)
    assert grants == ["SELECT", "INSERT"]
    assert " ".join(grants).upper().find("SELECT") != -1


def test_postgres_probe_grants_empty_when_no_grants_and_not_super() -> None:
    conn = _FakeGrantsConnection(super="off", pg_rows=[])
    assert dialect_for(SourceType.POSTGRES).probe_grants(conn) == []


def test_database_is_schema_flag_per_engine() -> None:
    # MySQL: a database IS a schema, so a set `database` reflects that one schema. A
    # PostgreSQL "database" is the connection whose schemas (public, app, ...) must ALL be
    # reflected -- so its flag is False (else a non-public schema is silently dropped).
    assert dialect_for(SourceType.MYSQL).database_is_schema is True
    assert dialect_for(SourceType.POSTGRES).database_is_schema is False


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
