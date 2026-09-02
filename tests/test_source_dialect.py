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
    # No read timeout -> no per-statement cap and no keepalive tuning (zero overhead).
    assert "statement_timeout" not in base["connect_args"]["options"]
    assert "keepalives" not in base["connect_args"]
    assert "tcp_user_timeout" not in base["connect_args"]
    timed = d.engine_kwargs(read_timeout_seconds=30)
    ca = timed["connect_args"]
    # statement_timeout is the hung-but-alive-query backstop (-> 57014, transient) ...
    assert "statement_timeout=30000" in ca["options"]
    # ... and TCP keepalives + tcp_user_timeout detect a dead/stalled/failed-over
    # connection (-> class 08, transient) WITHOUT capping a healthy streaming page --
    # the idle-timeout semantics MySQL's socket read_timeout has (tcp_user_timeout in ms).
    assert ca["keepalives"] == 1
    assert ca["keepalives_idle"] == 10 and ca["keepalives_interval"] == 5
    assert ca["keepalives_count"] == 3
    assert ca["tcp_user_timeout"] == 30000


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
    """A connection whose dialect is PostgreSQL, returning canned rows for ONE query kind.

    ``enrich`` now issues several catalog reads (columns, partitioning, FK schemas,
    triggers, routines) and ``list_schemas`` one more; this single-purpose double answers
    only the column (``pg_attribute``) enrich query and the ``list_schemas`` (``NOT LIKE``)
    query with its canned rows, and every OTHER catalog query with an empty result -- so a
    test that supplies only column rows (or only schema rows) is unaffected by the extra
    reads. Tests that need several catalog answers use :class:`_DispatchPgConnection`.
    """

    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        if "pg_attribute" in sql or "NOT LIKE" in sql:
            return _FakePgMappings(self._rows)
        return _FakePgMappings([])


class _DispatchPgConnection:
    """A PostgreSQL-typed connection that routes each catalog query to a canned answer.

    ``handlers`` maps an SQL substring -> ``callable(params) -> list[dict]`` (rows for
    ``.mappings()``); the first substring found in the statement wins and an unmatched
    query returns ``[]``. This lets one fake answer ``enrich``'s several reads (and
    ``extra_relations``) independently and honor the ``:nsp`` bind parameter.
    """

    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()

    def __init__(self, handlers: dict) -> None:
        self._handlers = handlers

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        for needle, fn in self._handlers.items():
            if needle in sql:
                return _FakePgMappings(fn(parameters or {}))
        return _FakePgMappings([])


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


def test_postgres_dialect_enrich_flags_stored_and_virtual_generated_columns() -> None:
    # T4-4: enrich reads pg_attribute.attgenerated -> both a STORED generated column ('s')
    # and a VIRTUAL generated column ('v', new in PG18 and its DEFAULT kind) set
    # ColumnDef.generated so the converter can warn (DSQL has no generated columns of either
    # kind). An ordinary column (attgenerated '') stays generated=False.
    from dsql_migrator.core.models import ColumnDef, TableDef

    conn = _FakePgConnection(
        [
            {"col": "id", "typ": "bigint", "gen": ""},
            {"col": "full_name", "typ": "text", "gen": "s"},
            {"col": "area", "typ": "numeric", "gen": "v"},
        ]
    )
    table = TableDef(
        name="people",
        columns=[
            ColumnDef(name="id", mysql_type="BIGINT"),
            ColumnDef(name="full_name", mysql_type="TEXT"),
            ColumnDef(name="area", mysql_type="NUMERIC"),
        ],
        primary_key=["id"],
    )
    dialect_for(SourceType.POSTGRES).enrich(conn, "shop", [table])
    assert table.columns[0].generated is False  # ordinary column
    assert table.columns[1].generated is True  # STORED generated column
    assert table.columns[1].mysql_type == "text"
    assert table.columns[2].generated is True  # VIRTUAL generated column (PG18+)


def test_postgres_enrich_scopes_columns_to_the_reflected_schema() -> None:
    # HIGH/silent: two schemas each hold a table named "orders" with DIFFERENT column
    # types/generated flags. enrich must scope its pg_attribute read to enrich_db (the
    # schema being reflected) via :nsp -- else it returns EVERY schema's columns and
    # last-wins bleeds another schema's types/flags in.
    from dsql_migrator.core.models import ColumnDef, TableDef

    by_schema = {
        "shop": [
            {"col": "amount", "typ": "numeric(12,2)", "gen": ""},
            {"col": "note", "typ": "text", "gen": "s"},
        ],
        "archive": [
            {"col": "amount", "typ": "bigint", "gen": ""},
            {"col": "note", "typ": "jsonb", "gen": ""},
        ],
    }
    conn = _DispatchPgConnection({"pg_attribute": lambda p: by_schema[p["nsp"]]})
    d = dialect_for(SourceType.POSTGRES)

    def _orders():
        return TableDef(
            name="orders",  # BARE at enrich time (caller qualifies afterwards)
            columns=[ColumnDef(name="amount", mysql_type="?"), ColumnDef(name="note", mysql_type="?")],
            primary_key=[],
        )

    shop = _orders()
    d.enrich(conn, "shop", [shop])
    assert [(c.mysql_type, c.generated) for c in shop.columns] == [
        ("numeric(12,2)", False),
        ("text", True),
    ]
    archive = _orders()
    d.enrich(conn, "archive", [archive])
    assert [(c.mysql_type, c.generated) for c in archive.columns] == [
        ("bigint", False),
        ("jsonb", False),
    ]


def test_postgres_enrich_collects_triggers_and_routines() -> None:
    # HIGH/silent: enrich must collect stored triggers (pg_trigger) and routines (pg_proc,
    # prokind -> FUNCTION/PROCEDURE) as ObjectRefs so TriggerRule/ProcedureRule flag them;
    # events stay empty (PostgreSQL has no scheduled events).
    from dsql_migrator.core.models import ObjectType

    conn = _DispatchPgConnection(
        {
            "pg_trigger": lambda p: [{"name": "audit_ins"}, {"name": "audit_upd"}],
            "pg_proc": lambda p: [
                {"name": "calc_total", "kind": "f"},
                {"name": "do_thing", "kind": "p"},
                {"name": "agg_stats", "kind": "a"},  # aggregate -> reported as a function
            ],
        }
    )
    triggers, routines, events = dialect_for(SourceType.POSTGRES).enrich(conn, "shop", [])
    assert [t.name for t in triggers] == ["audit_ins", "audit_upd"]
    assert all(t.object_type is ObjectType.TRIGGER for t in triggers)
    by = {r.name: r.object_type for r in routines}
    assert by["calc_total"] is ObjectType.FUNCTION
    assert by["do_thing"] is ObjectType.PROCEDURE
    assert by["agg_stats"] is ObjectType.FUNCTION
    assert events == []


def test_postgres_enrich_removes_partition_children_and_flags_parent() -> None:
    # HIGH/silent: get_table_names returns the partitioned PARENT (relkind 'p') AND each
    # declarative partition child (relispartition) -- migrating both double-represents the
    # data. enrich marks the parent partitioned (PartitionedTableRule) and REMOVES the
    # children from the tables list in place; an ordinary sibling is untouched.
    from dsql_migrator.core.models import ColumnDef, TableDef

    def _parts(_p):
        return [
            {"relname": "events", "relkind": "p", "is_partition": False},
            {"relname": "events_2023", "relkind": "r", "is_partition": True},
            {"relname": "events_2024", "relkind": "r", "is_partition": True},
            {"relname": "customers", "relkind": "r", "is_partition": False},
        ]

    conn = _DispatchPgConnection({"relispartition": _parts})
    tables = [
        TableDef(name=n, columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
        for n in ("events", "events_2023", "events_2024", "customers")
    ]
    dialect_for(SourceType.POSTGRES).enrich(conn, "shop", tables)
    assert [t.name for t in tables] == ["events", "customers"]  # children removed in place
    assert next(t for t in tables if t.name == "events").partitioned is True
    assert next(t for t in tables if t.name == "customers").partitioned is False


def test_postgres_enrich_corrects_cross_schema_fk_referenced_schema() -> None:
    # MEDIUM/silent: SQLAlchemy returns referred_schema=None when the parent is
    # search_path-visible, so _reflect_tables mis-qualifies the FK target to the CHILD's
    # schema (shop). enrich resolves the REAL parent schema (billing) from pg_constraint's
    # confrelid, search_path-independent.
    from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, TableDef

    def _fk_rows(_p):
        return [
            {"child": "orders", "conname": "fk_cust", "ref_schema": "billing", "ref_table": "customers"}
        ]

    conn = _DispatchPgConnection({"pg_constraint": _fk_rows})
    orders = TableDef(
        name="orders",  # BARE at enrich time
        columns=[ColumnDef(name="id", mysql_type="bigint")],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyDef(
                name="fk_cust",
                columns=["customer_id"],
                referenced_table="shop.customers",  # wrongly qualified to the CHILD schema
                referenced_columns=["id"],
            )
        ],
    )
    dialect_for(SourceType.POSTGRES).enrich(conn, "shop", [orders])
    assert orders.foreign_keys[0].referenced_table == "billing.customers"


def test_postgres_extra_relations_surfaces_matviews_and_foreign_tables() -> None:
    # HIGH/MEDIUM silent: matviews (relkind 'm') and foreign tables ('f') are returned by
    # neither get_view_names nor get_table_names, so the extra_relations seam enumerates
    # them and flags each ViewDef.unsupported_kind so Evaluation surfaces it.
    def _rels(_p):
        return [{"name": "daily_totals", "relkind": "m"}, {"name": "ext_orders", "relkind": "f"}]

    conn = _DispatchPgConnection({"relkind IN ('m', 'f')": _rels})
    relations = dialect_for(SourceType.POSTGRES).extra_relations(conn, "shop")
    assert {r.name: r.unsupported_kind for r in relations} == {
        "daily_totals": "materialized view",
        "ext_orders": "foreign table",
    }


def test_extra_relations_empty_for_mysql_and_non_pg_connections() -> None:
    # MySQL has neither matviews nor foreign tables (and Inspector.get_materialized_view_names
    # raises), so the seam is gated to the PG dialect on a genuine PG connection.
    assert dialect_for(SourceType.MYSQL).extra_relations(object(), "app") == []
    assert dialect_for(SourceType.POSTGRES).extra_relations(object(), "app") == []


def test_postgres_dialect_list_schemas_keeps_pg_prefixed_user_schemas() -> None:
    # SQLAlchemy's PG get_schema_names() filters `nspname NOT LIKE 'pg_%'` UNESCAPED, so a
    # user schema like `pgapp`/`pgdata` (pg + any char) is silently dropped. The dialect's
    # own list_schemas escapes the underscore, so such schemas are RETURNED (and only real
    # pg_* system/temp schemas are excluded). _user_schemas then removes system_schemas.
    from dsql_migrator.core.introspector import _user_schemas

    conn = _FakePgConnection(
        [
            {"nspname": "information_schema"},
            {"nspname": "pgapp"},  # pg + 'a' -> matches unescaped 'pg_%' but is a USER schema
            {"nspname": "public"},
        ]
    )
    dialect = dialect_for(SourceType.POSTGRES)
    # The dialect returns every non-pg_ schema, including the pg-prefixed user schema.
    assert dialect.list_schemas(conn) == ["information_schema", "pgapp", "public"]
    # _user_schemas then drops the engine system schemas (information_schema), keeping pgapp.
    kept = _user_schemas(None, dialect.system_schemas, connection=conn, dialect=dialect)
    assert "pgapp" in kept and "public" in kept
    assert "information_schema" not in kept


def test_postgres_dialect_supports_shared_snapshot_and_renders_export_import_sql() -> None:
    # PG shards can share ONE exported snapshot (pg_export_snapshot / SET TRANSACTION
    # SNAPSHOT, like pg_dump -j) -> a consistent range-sharded read even for a REPLACE
    # (no-CDC) load. The snapshot id can't be a bind param, so it is validated strictly.
    pg = dialect_for(SourceType.POSTGRES)
    assert pg.supports_shared_snapshot is True
    assert pg.export_snapshot_sql() == "SELECT pg_export_snapshot()"
    assert pg.set_transaction_snapshot_sql("00000003-0000001B-1") == (
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"
    )
    # an id with quotes/whitespace/; is rejected (injection guard), since it is interpolated
    for bad in ("x'; DROP TABLE t --", "a b", "a;b", "", "a'b"):
        with pytest.raises(ValueError):
            pg.set_transaction_snapshot_sql(bad)


def test_mysql_dialect_has_no_shared_snapshot() -> None:
    # InnoDB has no exported-snapshot equivalent -> shards fall back to their own snapshots
    # (safe only under a CDC handoff). The base defaults must raise, not silently no-op.
    my = dialect_for(SourceType.MYSQL)
    assert my.supports_shared_snapshot is False
    with pytest.raises(NotImplementedError):
        my.export_snapshot_sql()
    with pytest.raises(NotImplementedError):
        my.set_transaction_snapshot_sql("whatever")


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


def test_postgres_probe_grants_includes_role_inherited_select() -> None:
    # MEDIUM/loud-but-wrong: the migrator user holds SELECT only via membership in a group
    # role. probe_grants must check EFFECTIVE privilege (pg_has_role, honoring inheritance),
    # NOT grantee = current_user -- else it FALSELY reports "SELECT missing" and blocks Full
    # Load for a perfectly-privileged role-based grant.
    class _RoleGrantsConn:
        class _Dialect:
            name = "postgresql"

        dialect = _Dialect()

        def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
            sql = str(statement).upper()
            if "IS_SUPERUSER" in sql:
                return _GrantsResult(scalar_value="off")
            if "ROLE_TABLE_GRANTS" in sql:
                # The role-inherited grant is visible ONLY when the query accounts for role
                # membership; the old current_user-only filter (no pg_has_role) would miss it.
                assert "PG_HAS_ROLE" in sql
                return _GrantsResult(rows=[("SELECT",)])
            raise RuntimeError(f"unexpected grant probe SQL: {sql!r}")

    grants = dialect_for(SourceType.POSTGRES).probe_grants(_RoleGrantsConn())
    assert "SELECT" in grants


# ---------------------------------------------------------------------------
# Engine-specific source-error classification + hint wording + load-governor metric.
# A PostgreSQL source carries a STRING SQLSTATE (never MySQL int codes), so each
# dialect must classify its own transient/too-many/active-query surfaces -- a
# MySQL-only classifier silently never fires for psycopg.
# ---------------------------------------------------------------------------


class _PgErr(Exception):
    """A psycopg-shaped error: carries a string ``.sqlstate``."""

    def __init__(self, sqlstate=None, message: str = "") -> None:
        super().__init__(message or (sqlstate or ""))
        self.sqlstate = sqlstate


class _Wrapped(Exception):
    """A SQLAlchemy-shaped wrapper keeping the driver error on ``.orig``."""

    def __init__(self, orig: BaseException) -> None:
        super().__init__(str(orig))
        self.orig = orig
        self.sqlstate = None  # the wrapper itself has none; the orig carries it


def test_engine_display_names() -> None:
    assert dialect_for(SourceType.MYSQL).engine_display_name == "MySQL"
    assert dialect_for(SourceType.POSTGRES).engine_display_name == "PostgreSQL"


def test_mysql_is_transient_error_uses_mysql_codes() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert d.is_transient_error(Exception(2013, "Lost connection during query"))
    assert d.is_transient_error(Exception(1040, "Too many connections"))
    assert not d.is_transient_error(Exception(1064, "You have an error in your SQL"))
    # A PG-style string SQLSTATE means NOTHING to the MySQL classifier (proves why the
    # per-engine dispatch is needed): a PG failover would go un-retried under MySQL rules.
    assert not d.is_transient_error(_PgErr("57P03", "the database system is starting up"))


def test_mysql_is_too_many_connections() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert d.is_too_many_connections(Exception(1040, "Too many connections"))
    assert d.is_too_many_connections(Exception(0, "ERROR: too many connections"))
    assert not d.is_too_many_connections(Exception(2013, "Lost connection"))


def test_postgres_is_transient_error_by_sqlstate() -> None:
    import socket

    d = dialect_for(SourceType.POSTGRES)
    # Connection class 08 and the operator-intervention / insufficient-resource states.
    assert d.is_transient_error(_PgErr("08006", "connection failure"))
    assert d.is_transient_error(_PgErr("57P01", "admin shutdown"))
    assert d.is_transient_error(_PgErr("57P03", "cannot connect now (failover)"))
    assert d.is_transient_error(_PgErr("53300", "too many clients already"))
    # query_canceled (57014): the Full Load per-page read timeout (statement_timeout)
    # firing on a stalled/over-long page -- the analog of MySQL's socket read_timeout, so
    # it must auto-retry (parity fix; without it a timed-out PG page would fail unretried).
    assert d.is_transient_error(
        _PgErr("57014", "canceling statement due to statement timeout")
    )
    # Wrapped by SQLAlchemy: the real sqlstate is on .orig.
    assert d.is_transient_error(_Wrapped(_PgErr("08006")))
    # A socket timeout (stalled read) is transient.
    assert d.is_transient_error(socket.timeout())
    # A genuine data/constraint error carries a decisive non-transient SQLSTATE.
    assert not d.is_transient_error(_PgErr("23505", "duplicate key value"))
    assert not d.is_transient_error(_PgErr("42601", "syntax error at or near"))


def test_postgres_is_transient_error_no_sqlstate_paths() -> None:
    d = dialect_for(SourceType.POSTGRES)
    # A psycopg connection-level error type with NO sqlstate (server never answered).
    op = type("OperationalError", (Exception,), {})
    op.__module__ = "psycopg"
    assert d.is_transient_error(op("server closed the connection unexpectedly"))
    # Wrapped/re-raised losing the type+code: fall back to a known drop signature.
    assert d.is_transient_error(Exception("connection reset by peer"))
    # No sqlstate, no matching signature, not a psycopg type -> NOT transient.
    assert not d.is_transient_error(ValueError("some unrelated structural error"))


def test_postgres_is_too_many_connections() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert d.is_too_many_connections(_PgErr("53300", "sorry, too many clients already"))
    assert d.is_too_many_connections(_Wrapped(_PgErr("53300")))
    assert d.is_too_many_connections(Exception("remaining connection slots are reserved"))
    assert not d.is_too_many_connections(_PgErr("08006", "connection failure"))


class _MyStatusResult:
    def __init__(self, value) -> None:
        self._value = value

    def first(self):  # noqa: ANN201
        return ("Threads_running", str(self._value)) if self._value is not None else None


class _MyStatusConn:
    def __init__(self, value) -> None:
        self._value = value

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        assert "GLOBAL STATUS" in str(statement)  # MySQL metric, never on PG
        return _MyStatusResult(self._value)


class _PgScalar:
    def __init__(self, value) -> None:
        self._value = value

    def scalar(self):  # noqa: ANN201
        return self._value


class _PgActivityConn:
    def __init__(self, value, *, raise_error: bool = False) -> None:
        self._value = value
        self._raise = raise_error

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        if self._raise:
            raise RuntimeError("probe failed")
        assert "pg_stat_activity" in str(statement)  # PG metric, never a MySQL SHOW
        return _PgScalar(self._value)


def test_mysql_read_active_query_count_reads_threads_running() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert d.read_active_query_count(_MyStatusConn(7)) == 7
    assert d.read_active_query_count(_MyStatusConn(None)) is None


def test_postgres_read_active_query_count_reads_pg_stat_activity() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert d.read_active_query_count(_PgActivityConn(3)) == 3
    assert d.read_active_query_count(_PgActivityConn(None)) is None
    # Fail-open: any error -> None so the governor never stalls the load.
    assert d.read_active_query_count(_PgActivityConn(1, raise_error=True)) is None


class _LsnResult:
    def __init__(self, value) -> None:
        self._value = value

    def first(self):  # noqa: ANN201
        return (self._value,) if self._value is not None else None


class _LsnConn:
    def __init__(self, value, *, raise_error: bool = False) -> None:
        self._value = value
        self._raise = raise_error

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        if self._raise:
            raise RuntimeError("insufficient privilege")
        # Recovery-aware: primary -> pg_current_wal_lsn, standby -> pg_last_wal_replay_lsn.
        assert "pg_current_wal_lsn" in str(statement)
        assert "pg_is_in_recovery" in str(statement)
        return _LsnResult(self._value)


def test_mysql_capture_resume_lsn_is_none() -> None:
    # MySQL's resume coordinate is the binlog (WatermarkCapturer), not this seam.
    assert dialect_for(SourceType.MYSQL).capture_resume_lsn(object()) is None


def test_postgres_capture_resume_lsn_reads_wal_lsn() -> None:
    d = dialect_for(SourceType.POSTGRES)
    assert d.capture_resume_lsn(_LsnConn("3/AF012B8")) == "3/AF012B8"
    # Best effort: an error (e.g. insufficient privilege) -> None (still a valid watermark).
    assert d.capture_resume_lsn(_LsnConn(None, raise_error=True)) is None


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
