# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the target Aurora DSQL catalog introspector.

Covers:
- ``browse`` assembles the expected schema -> table/view -> column/index tree
  from canned ``information_schema`` / ``pg_catalog`` rows (Requirement 10.1).
- ``object_exists`` reports existence for qualified/unqualified names and is
  case-insensitive, supporting pre-apply conflict detection (Requirement 10.3).
- Catalog queries are parameterized (no value interpolation) and read-only, and
  ``object_exists`` issues no SQL (Requirement 9.4).
- The introspector only ever uses the injected target connector; it never opens
  a source connection and never reaches a real cluster (no live infrastructure).
"""

from __future__ import annotations

from typing import Optional

import pytest

from dsql_migrator.core.models import (
    TargetConnectionConfig,
    TargetInventory,
    TargetObjectKind,
)
from dsql_migrator.core.target_introspector import (
    COLUMNS_QUERY,
    INDEXES_QUERY,
    RELATIONS_QUERY,
    SYSTEM_SCHEMAS,
    TargetIntrospector,
    build_inventory,
    count_target_rows,
    tables_with_rows,
)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeCursor:
    """A cursor that returns canned rows selected by the executed statement."""

    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection
        self._rows: list[tuple] = []
        self.closed = False

    def execute(self, statement: str, parameters: object = None) -> None:
        self._connection.executed.append((statement, parameters))
        self._rows = self._connection.rows_for(statement)

    def fetchall(self) -> list[tuple]:
        return self._rows

    def close(self) -> None:
        self.closed = True
        self._connection.closed_cursors += 1


class _FakeConnection:
    """A minimal psycopg-like connection serving canned catalog rows."""

    def __init__(
        self,
        *,
        relations: list[tuple],
        columns: list[tuple],
        indexes: list[tuple],
    ) -> None:
        self._relations = relations
        self._columns = columns
        self._indexes = indexes
        self.executed: list[tuple[str, object]] = []
        self.closed = False
        self.closed_cursors = 0

    def rows_for(self, statement: str) -> list[tuple]:
        if "information_schema.tables" in statement:
            return self._relations
        if "information_schema.columns" in statement:
            return self._columns
        if "pg_catalog.pg_index" in statement:
            return self._indexes
        return []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class _FakeConnector:
    """A connector that hands out a single recording fake connection."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.connect_calls = 0

    def connect(self) -> _FakeConnection:
        self.connect_calls += 1
        return self._connection


def _target_config() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="my-cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
        database="postgres",
        username="admin",
    )


def _sample_connection() -> _FakeConnection:
    """A catalog with two schemas, two tables, one view, columns and an index."""
    relations = [
        ("public", "orders", "BASE TABLE"),
        ("public", "v_active_orders", "VIEW"),
        ("app", "customers", "BASE TABLE"),
    ]
    columns = [
        ("public", "orders", "id", "uuid", "NO"),
        ("public", "orders", "total", "numeric", "YES"),
        ("app", "customers", "id", "uuid", "NO"),
    ]
    indexes = [
        ("public", "orders", "orders_pkey", True),
        ("public", "orders", "orders_total_idx", False),
    ]
    return _FakeConnection(relations=relations, columns=columns, indexes=indexes)


def _introspector(
    connection: Optional[_FakeConnection] = None,
) -> tuple[TargetIntrospector, _FakeConnector]:
    connection = connection or _sample_connection()
    connector = _FakeConnector(connection)
    introspector = TargetIntrospector(connector_factory=lambda _conn: connector)
    return introspector, connector


# ---------------------------------------------------------------------------
# browse — object tree assembly
# ---------------------------------------------------------------------------


def test_browse_builds_expected_object_tree() -> None:
    introspector, connector = _introspector()

    inventory = introspector.browse(_target_config())

    assert connector.connect_calls == 1
    assert isinstance(inventory, TargetInventory)

    schema_names = {schema.name for schema in inventory.schemas}
    assert schema_names == {"public", "app"}

    public = next(s for s in inventory.schemas if s.name == "public")
    table_names = {t.name for t in public.tables}
    view_names = {v.name for v in public.views}
    assert table_names == {"orders"}
    assert view_names == {"v_active_orders"}

    orders = next(t for t in public.tables if t.name == "orders")
    assert orders.kind is TargetObjectKind.TABLE
    assert [c.name for c in orders.columns] == ["id", "total"]
    id_column = next(c for c in orders.columns if c.name == "id")
    total_column = next(c for c in orders.columns if c.name == "total")
    assert id_column.data_type == "uuid"
    assert id_column.nullable is False
    assert total_column.nullable is True
    assert {(i.name, i.unique) for i in orders.indexes} == {
        ("orders_pkey", True),
        ("orders_total_idx", False),
    }


def test_browse_separates_views_from_tables() -> None:
    introspector, _ = _introspector()

    inventory = introspector.browse(_target_config())

    public = next(s for s in inventory.schemas if s.name == "public")
    view = public.views[0]
    assert view.kind is TargetObjectKind.VIEW
    assert view.name == "v_active_orders"
    # The view must not also appear among the tables.
    assert all(t.name != "v_active_orders" for t in public.tables)


def test_browse_result_is_serializable() -> None:
    introspector, _ = _introspector()
    inventory = introspector.browse(_target_config())
    # Round-trips through the Pydantic model without loss.
    assert inventory == TargetInventory.model_validate(inventory.model_dump())


def test_browse_closes_connection_and_cursors() -> None:
    connection = _sample_connection()
    introspector, _ = _introspector(connection)

    introspector.browse(_target_config())

    assert connection.closed is True
    # One cursor opened and closed per catalog query (relations/columns/indexes).
    assert connection.closed_cursors == 3


def test_build_inventory_ignores_orphan_columns_and_indexes() -> None:
    inventory = build_inventory(
        relation_rows=[("public", "orders", "BASE TABLE")],
        column_rows=[
            ("public", "orders", "id", "uuid", "NO"),
            ("public", "ghost", "id", "uuid", "NO"),
        ],
        index_rows=[("public", "ghost", "ghost_idx", True)],
    )
    public = inventory.schemas[0]
    orders = public.tables[0]
    assert [c.name for c in orders.columns] == ["id"]
    assert orders.indexes == []


# ---------------------------------------------------------------------------
# object_exists — pre-apply conflict detection
# ---------------------------------------------------------------------------


def test_object_exists_qualified_and_unqualified() -> None:
    introspector, _ = _introspector()
    introspector.browse(_target_config())

    assert introspector.object_exists("public.orders") is True
    assert introspector.object_exists("orders") is True
    assert introspector.object_exists("app.customers") is True
    assert introspector.object_exists("v_active_orders") is True


def test_object_exists_returns_false_for_absent_objects() -> None:
    introspector, _ = _introspector()
    introspector.browse(_target_config())

    assert introspector.object_exists("public.missing") is False
    assert introspector.object_exists("missing") is False
    # Right name in the wrong schema is a non-match for a qualified lookup.
    assert introspector.object_exists("app.orders") is False


def test_object_exists_is_case_insensitive() -> None:
    introspector, _ = _introspector()
    introspector.browse(_target_config())

    assert introspector.object_exists("PUBLIC.ORDERS") is True
    assert introspector.object_exists("Orders") is True


def test_object_exists_covers_secondary_indexes() -> None:
    """An index already on the target is discoverable so it can be skipped."""
    introspector, _ = _introspector()
    introspector.browse(_target_config())

    # Qualified (schema.index_name), unqualified, and case-insensitive forms.
    assert introspector.object_exists("public.orders_total_idx") is True
    assert introspector.object_exists("orders_total_idx") is True
    assert introspector.object_exists("ORDERS_TOTAL_IDX") is True
    assert introspector.object_exists("orders_pkey") is True
    # An index that does not exist on the target is still reported absent.
    assert introspector.object_exists("orders_total_idx_absent") is False


def test_object_exists_before_browse_raises() -> None:
    introspector = TargetIntrospector(connector_factory=lambda _conn: None)
    with pytest.raises(RuntimeError):
        introspector.object_exists("public.orders")


def test_object_exists_consults_inventory_without_issuing_sql() -> None:
    connection = _sample_connection()
    introspector, _ = _introspector(connection)
    introspector.browse(_target_config())

    statements_after_browse = len(connection.executed)
    introspector.object_exists("public.orders")
    introspector.object_exists("missing")
    # No further statements were executed for existence checks.
    assert len(connection.executed) == statements_after_browse


def test_object_exists_covers_every_browsed_relation() -> None:
    """Property: every browsed relation is reported as existing (and absent ones not)."""
    introspector, _ = _introspector()
    inventory = introspector.browse(_target_config())

    for schema in inventory.schemas:
        for relation in (*schema.tables, *schema.views):
            assert introspector.object_exists(relation.qualified_name) is True
            assert introspector.object_exists(relation.name) is True
            assert introspector.object_exists(f"{relation.qualified_name}_absent") is False


# ---------------------------------------------------------------------------
# Requirement 9.4 — parameterized, read-only catalog access
# ---------------------------------------------------------------------------


def test_catalog_queries_are_parameterized_and_read_only() -> None:
    connection = _sample_connection()
    introspector, _ = _introspector(connection)
    introspector.browse(_target_config())

    assert len(connection.executed) == 3
    for statement, parameters in connection.executed:
        # Read-only: every catalog query is a SELECT.
        assert statement.lstrip().upper().startswith("SELECT")
        # The excluded schema names are bound as a parameter, never inlined.
        assert isinstance(parameters, dict)
        assert parameters["excluded_schemas"] == list(SYSTEM_SCHEMAS)
        for schema in SYSTEM_SCHEMAS:
            assert f"'{schema}'" not in statement
        assert "%(excluded_schemas)s" in statement


def test_browse_executes_only_the_known_catalog_queries() -> None:
    connection = _sample_connection()
    introspector, _ = _introspector(connection)
    introspector.browse(_target_config())

    executed_statements = [statement for statement, _ in connection.executed]
    assert RELATIONS_QUERY in executed_statements
    assert COLUMNS_QUERY in executed_statements
    assert INDEXES_QUERY in executed_statements


# ---------------------------------------------------------------------------
# tables_with_rows: detect non-empty target tables for the Full Load replace warning
# ---------------------------------------------------------------------------


class _RowProbeCursor:
    """A cursor that returns a row only for tables listed as non-empty."""

    def __init__(self, connection: "_RowProbeConnection") -> None:
        self._connection = connection
        self._row: object = None

    def execute(self, statement: object, parameters: object = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._connection.executed.append(text)
        # The statement is SELECT 1 FROM "<schema>"."<table>" LIMIT 1; raise for
        # tables the fake marks as missing, else return a row iff non-empty.
        for name in self._connection.missing:
            if _contains_table(text, name):
                raise RuntimeError("relation does not exist")
        self._row = (
            (1,)
            if any(_contains_table(text, n) for n in self._connection.nonempty)
            else None
        )

    def fetchone(self) -> object:
        return self._row

    def close(self) -> None:
        self._connection.closed_cursors += 1


def _contains_table(sql_text: str, qualified_name: str) -> bool:
    parts = qualified_name.split(".")
    return all(f'"{part}"' in sql_text for part in parts)


class _RowProbeConnection:
    def __init__(self, *, nonempty: set[str], missing: set[str] | None = None) -> None:
        self.nonempty = nonempty
        self.missing = missing or set()
        self.executed: list[str] = []
        self.closed = False
        self.closed_cursors = 0

    def cursor(self) -> _RowProbeCursor:
        return _RowProbeCursor(self)

    def close(self) -> None:
        self.closed = True


def test_tables_with_rows_returns_only_non_empty_tables() -> None:
    connection = _RowProbeConnection(nonempty={"app.orders"})

    found = tables_with_rows(
        ["app.orders", "app.customers"],
        connection_factory=lambda: connection,
    )

    assert found == {"app.orders"}
    assert connection.closed is True


def test_tables_with_rows_treats_missing_table_as_empty() -> None:
    # A table that does not exist (probe raises) is simply not flagged; the
    # connection is still closed and other tables are still checked.
    connection = _RowProbeConnection(nonempty={"app.orders"}, missing={"app.gone"})

    found = tables_with_rows(
        ["app.gone", "app.orders"],
        connection_factory=lambda: connection,
    )

    assert found == {"app.orders"}


# ---------------------------------------------------------------------------
# count_target_rows: exact per-table COUNT(*) for the migration-status view
# ---------------------------------------------------------------------------


class _CountCursor:
    """A cursor returning a per-table COUNT(*), or raising for missing tables."""

    def __init__(self, connection: "_CountConnection") -> None:
        self._connection = connection
        self._row: object = None

    def execute(self, statement: object, parameters: object = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._connection.executed.append(text)
        for name in self._connection.missing:
            if _contains_table(text, name):
                raise RuntimeError("relation does not exist")
        self._row = None
        for name, count in self._connection.counts.items():
            if _contains_table(text, name):
                self._row = (count,)
                break

    def fetchone(self) -> object:
        return self._row

    def close(self) -> None:
        self._connection.closed_cursors += 1


class _CountConnection:
    def __init__(self, *, counts: dict, missing: set[str] | None = None) -> None:
        self.counts = counts
        self.missing = missing or set()
        self.executed: list[str] = []
        self.closed = False
        self.closed_cursors = 0

    def cursor(self) -> _CountCursor:
        return _CountCursor(self)

    def close(self) -> None:
        self.closed = True


def test_count_target_rows_returns_exact_counts() -> None:
    connection = _CountConnection(counts={"app.orders": 100, "app.items": 0})

    counts = count_target_rows(
        ["app.orders", "app.items"], connection_factory=lambda: connection
    )

    assert counts == {"app.orders": 100, "app.items": 0}
    assert connection.closed is True


def test_count_target_rows_missing_table_is_none_not_zero() -> None:
    # A table that does not exist (count raises) maps to None (unknown), distinct
    # from a genuinely empty table (0).
    connection = _CountConnection(
        counts={"app.orders": 5}, missing={"app.gone"}
    )

    counts = count_target_rows(
        ["app.gone", "app.orders"], connection_factory=lambda: connection
    )

    assert counts == {"app.gone": None, "app.orders": 5}


# ---------------------------------------------------------------------------
# max_pk_target: high-water PK per target table (stream lag compare)
# ---------------------------------------------------------------------------


def test_max_pk_target_returns_high_water_and_skips_no_pk() -> None:
    from dsql_migrator.core.target_introspector import max_pk_target

    connection = _CountConnection(counts={"app.orders": 12759})
    out = max_pk_target(
        {"app.orders": "id", "app.nopk": ""},
        connection_factory=lambda: connection,
    )
    assert out["app.orders"] == 12759
    assert out["app.nopk"] is None  # empty pk -> skipped
    assert connection.closed is True


def test_max_pk_target_missing_table_is_none() -> None:
    from dsql_migrator.core.target_introspector import max_pk_target

    connection = _CountConnection(counts={"app.orders": 5}, missing={"app.gone"})
    out = max_pk_target(
        {"app.gone": "id", "app.orders": "id"},
        connection_factory=lambda: connection,
    )
    assert out == {"app.gone": None, "app.orders": 5}


# ---------------------------------------------------------------------------
# target_primary_key_columns: the target's ACTUAL primary key
#
# Full Load's append path uses this to decide against the live target instead of
# assuming its key -- assuming "the target still has its original key" is what
# blocked a correctly-applied composite PK from loading at all.
# ---------------------------------------------------------------------------


class _PkCursor:
    """A pg_index stand-in that actually HONORS the query's key-column bound.

    ``rows`` is the index's full ``indkey`` expansion -- key columns first, then the
    non-key "included"/stored columns -- paired with ``indnkeyatts``. The cursor returns
    the slice the statement asks for, so a query that forgets ``k.ord <= indnkeyatts``
    gets the stored columns too, exactly as a real cluster returns them.

    That fidelity matters: with a cursor that ignored the SQL and echoed a canned PK,
    the suite passed while the function returned every column of every table on a live
    Aurora DSQL cluster (every DSQL primary index carries the remaining columns as
    payload, so ``indnatts`` > ``indnkeyatts`` universally, not occasionally).
    """

    def __init__(self, connection: "_PkConnection") -> None:
        self._connection = connection
        self._rows: list = []

    def execute(self, statement: object, parameters: object = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._connection.executed.append((text, parameters))
        if self._connection.raises:
            raise RuntimeError("catalog unreadable")
        rows = list(self._connection.rows)
        # Emulate `WHERE ... k.ord <= ix.indnkeyatts`: without that predicate a real
        # index hands back its stored columns as well.
        if "indnkeyatts" in text:
            rows = rows[: self._connection.indnkeyatts]
        self._rows = rows

    def fetchall(self) -> list:
        return self._rows

    def close(self) -> None:
        self._connection.closed_cursors += 1


class _PkConnection:
    def __init__(
        self, *, rows: list, raises: bool = False, indnkeyatts: int | None = None
    ) -> None:
        self.rows = rows
        # How many leading entries of ``rows`` are actual KEY columns; the rest are the
        # index's stored/included payload. Defaults to "all of them" for tables whose
        # index carries no payload.
        self.indnkeyatts = len(rows) if indnkeyatts is None else indnkeyatts
        self.raises = raises
        self.executed: list[tuple] = []
        self.closed = False
        self.closed_cursors = 0

    def cursor(self) -> _PkCursor:
        return _PkCursor(self)

    def close(self) -> None:
        self.closed = True


def test_target_primary_key_columns_preserves_composite_key_order() -> None:
    # Key ORDER is the whole point of the composite strategy -- (user_id, id) must
    # not come back as (id, user_id) -- so the query orders by the indkey ordinal.
    from dsql_migrator.core.target_introspector import target_primary_key_columns

    connection = _PkConnection(rows=[("user_id",), ("id",)])

    columns = target_primary_key_columns(
        "ecommerce.orders", connection_factory=lambda: connection
    )

    assert columns == ["user_id", "id"]
    statement, params = connection.executed[0]
    assert "ORDER BY k.ord" in statement
    # schema/table travel as BOUND PARAMETERS, never interpolated (Requirement 9.4).
    assert params == {"schema": "ecommerce", "table": "orders"}
    assert "ecommerce" not in statement
    assert connection.closed is True


def test_target_primary_key_columns_excludes_the_indexes_stored_columns() -> None:
    """The primary key is the first ``indnkeyatts`` entries of ``indkey`` -- the rest are
    the index's non-key stored/included columns.

    Measured on a live Aurora DSQL cluster: EVERY primary index carries the table's
    remaining columns as payload (e.g. a 2-column table reports indnatts=2 with
    indnkeyatts=1; an 11-table schema reported indnatts up to 14 with indnkeyatts=1
    throughout). Omitting the bound made this function return every column of every
    table -- 11/11 tables disagreed with information_schema.key_column_usage -- which in
    turn would refuse every append with a changed key while naming an absurd "actual"
    key. This shape reproduces that: 'order_id' is the key, the rest is payload.
    """
    from dsql_migrator.core.target_introspector import target_primary_key_columns

    connection = _PkConnection(
        rows=[("order_id",), ("customer_id",), ("total_amount",), ("order_ts",)],
        indnkeyatts=1,
    )

    columns = target_primary_key_columns(
        "ecommerce_demo.orders", connection_factory=lambda: connection
    )

    assert columns == ["order_id"]
    statement, _params = connection.executed[0]
    assert "indnkeyatts" in statement, (
        "the query must bound indkey to the index's KEY columns"
    )


def test_target_primary_key_columns_keeps_a_composite_key_and_drops_payload() -> None:
    # The case the whole feature exists for: a composite (leading, id) key must come back
    # in index order AND without the stored columns that follow it in indkey.
    from dsql_migrator.core.target_introspector import target_primary_key_columns

    connection = _PkConnection(
        rows=[("user_id",), ("id",), ("amount",), ("created_at",)],
        indnkeyatts=2,
    )

    columns = target_primary_key_columns(
        "ecommerce.orders", connection_factory=lambda: connection
    )

    assert columns == ["user_id", "id"]


def test_target_primary_key_columns_resolves_a_bare_name_via_search_path() -> None:
    from dsql_migrator.core.target_introspector import target_primary_key_columns

    connection = _PkConnection(rows=[("id",)])

    columns = target_primary_key_columns(
        "orders", connection_factory=lambda: connection
    )

    assert columns == ["id"]
    statement, params = connection.executed[0]
    assert "pg_table_is_visible" in statement
    assert params == {"table": "orders"}


def test_target_primary_key_columns_returns_none_when_undeterminable() -> None:
    # Every "cannot determine" route must yield None (unknown) -- the caller treats
    # that as unsafe, so it must never be confused with a definite answer.
    from dsql_migrator.core.target_introspector import target_primary_key_columns

    # No primary key / table absent -> no rows.
    empty = _PkConnection(rows=[])
    assert (
        target_primary_key_columns("app.t", connection_factory=lambda: empty) is None
    )
    assert empty.closed is True

    # Catalog query raises.
    broken = _PkConnection(rows=[], raises=True)
    assert (
        target_primary_key_columns("app.t", connection_factory=lambda: broken) is None
    )
    assert broken.closed is True  # still closed on the error path

    # Cannot even connect.
    def _no_connection():
        raise RuntimeError("connect failed")

    assert (
        target_primary_key_columns("app.t", connection_factory=_no_connection) is None
    )


# ---------------------------------------------------------------------------
# sync_identity_sequences: advance an identity PK past the loaded rows
# ---------------------------------------------------------------------------


class _SeqCursor:
    """A catalog stand-in that HONORS the statement, like ``_PkCursor``.

    It answers the identity-column lookup only when the query actually filters on
    ``is_identity``, and records every DDL it is handed. A cursor that ignored the SQL
    would let "drop the identity filter" or "skip the no-identity guard" pass while the
    real function reached for a sequence that plain integer keys do not have.
    """

    def __init__(self, connection: "_SeqConnection") -> None:
        self._c = connection
        self._rows: list = []

    def execute(self, statement: object, parameters: object = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._c.executed.append(text)
        self._rows = []
        if "is_identity" in text:
            # Only an identity column is reported -- a plain integer PK yields nothing.
            table = (parameters or ("",))[-1]
            column = self._c.identity_by_table.get(table)
            self._rows = [(column,)] if column else []
        elif "MAX(" in text:
            self._rows = [(self._c.max_pk,)]
        elif "RESTART WITH" in text:
            self._c.restarts.append(text)

    def fetchall(self) -> list:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        return None


class _SeqConnection:
    def __init__(self, *, identity_by_table: dict, max_pk) -> None:
        self.identity_by_table = identity_by_table
        self.max_pk = max_pk
        self.executed: list[str] = []
        self.restarts: list[str] = []
        self.closed = False

    def cursor(self) -> _SeqCursor:
        return _SeqCursor(self)

    def close(self) -> None:
        self.closed = True


def test_sync_identity_sequences_restarts_past_the_loaded_max() -> None:
    """RESTART WITH must be max+1, never max.

    Full Load writes the source's own key values, and an explicitly-supplied value does
    NOT advance a ``GENERATED BY DEFAULT AS IDENTITY`` sequence -- so the sequence sits at
    its start while those values are taken, and the application's first insert after
    cut-over dies on a duplicate key (reproduced on a live ap-northeast-2 cluster).
    Restarting AT max would collide on that very first insert instead.
    """
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    conn = _SeqConnection(identity_by_table={"orders": "id"}, max_pk=742)
    out = sync_identity_sequences(
        ["ecommerce.orders"], connection_factory=lambda: conn
    )
    assert out == {"ecommerce.orders": 743}
    assert len(conn.restarts) == 1
    assert "RESTART WITH 743" in conn.restarts[0]
    assert conn.closed


def test_sync_identity_sequences_skips_tables_without_an_identity_column() -> None:
    """The KEEP_INTEGER default has a plain integer PK and no sequence at all.

    Issuing RESTART against it would error (or, worse, invent state), so the identity
    filter is what keeps the default strategy untouched.
    """
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    conn = _SeqConnection(identity_by_table={}, max_pk=99)
    out = sync_identity_sequences(
        ["ecommerce.plain"], connection_factory=lambda: conn
    )
    assert out == {"ecommerce.plain": None}
    assert conn.restarts == [], "a plain integer key must not be altered"
    # It must not even ask for MAX(pk) once it knows there is no identity column.
    assert not any("MAX(" in stmt for stmt in conn.executed)


def test_sync_identity_sequences_leaves_an_empty_table_alone() -> None:
    """An empty table's sequence is already correct at its start.

    ``MAX(pk)`` is NULL there, and "restarting" from it would move the sequence
    BACKWARDS (or fail), so the guard skips it.
    """
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    conn = _SeqConnection(identity_by_table={"empty": "id"}, max_pk=None)
    out = sync_identity_sequences(
        ["ecommerce.empty"], connection_factory=lambda: conn
    )
    assert out == {"ecommerce.empty": None}
    assert conn.restarts == []


def test_sync_identity_sequences_reports_per_table_and_is_injection_safe() -> None:
    """Mixed batch: one synced, one plain, one empty -- and identifiers are quoted."""
    from dsql_migrator.core.target_introspector import sync_identity_sequences

    conn = _SeqConnection(
        identity_by_table={"orders": "id"}, max_pk=5
    )
    out = sync_identity_sequences(
        ["ecommerce.orders", "ecommerce.plain"], connection_factory=lambda: conn
    )
    assert out["ecommerce.orders"] == 6
    assert out["ecommerce.plain"] is None
    # Identifiers go through psycopg.sql.Identifier, so they are quoted -- never
    # interpolated (Requirement 9.4).
    assert '"ecommerce"."orders"' in conn.restarts[0]
    assert '"id"' in conn.restarts[0]


# ---------------------------------------------------------------------------
# target_required_columns_without_default: value-required target columns
# ---------------------------------------------------------------------------


class _RequiredColsCursor:
    """Answers the existence probe, then the value-required-columns query.

    The function issues two statements: `SELECT 1 ...` to confirm the relation
    exists, then the `attnotnull AND NOT atthasdef AND attidentity = ''` list. This
    cursor distinguishes them by SQL text so a test can model a missing table
    (existence returns nothing) independently of the column list.
    """

    def __init__(self, connection: "_RequiredColsConnection") -> None:
        self._connection = connection
        self._rows: list = []
        self._one: object = None

    def execute(self, statement: object, parameters: object = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._connection.executed.append((text, parameters))
        if self._connection.raises:
            raise RuntimeError("catalog unreadable")
        if text.strip().startswith("SELECT 1"):
            self._one = (1,) if self._connection.exists else None
        else:
            self._rows = list(self._connection.columns)

    def fetchone(self) -> object:
        return self._one

    def fetchall(self) -> list:
        return self._rows

    def close(self) -> None:
        self._connection.closed_cursors += 1


class _RequiredColsConnection:
    def __init__(
        self, *, columns: list, exists: bool = True, raises: bool = False
    ) -> None:
        self.columns = columns
        self.exists = exists
        self.raises = raises
        self.executed: list[tuple] = []
        self.closed = False
        self.closed_cursors = 0

    def cursor(self) -> _RequiredColsCursor:
        return _RequiredColsCursor(self)

    def close(self) -> None:
        self.closed = True


def test_required_columns_without_default_lists_value_required_columns() -> None:
    from dsql_migrator.core.target_introspector import (
        target_required_columns_without_default,
    )

    conn = _RequiredColsConnection(columns=[("id",), ("added_notnull",)])

    result = target_required_columns_without_default(
        "ecommerce.orders", connection_factory=lambda: conn
    )

    assert result == ["id", "added_notnull"]
    # The list query filters on all three conditions -- nullable, defaulted, and
    # identity columns are excluded at the SQL level, not client-side.
    list_stmt = conn.executed[-1][0]
    assert "a.attnotnull" in list_stmt
    assert "NOT a.atthasdef" in list_stmt
    assert "a.attidentity = ''" in list_stmt
    # schema/table travel as bound parameters (Requirement 9.4).
    assert conn.executed[0][1] == {"schema": "ecommerce", "table": "orders"}
    assert conn.closed is True


def test_required_columns_without_default_returns_empty_for_a_clean_table() -> None:
    from dsql_migrator.core.target_introspector import (
        target_required_columns_without_default,
    )

    # The table exists and every value-required column is coverable -> [], NOT None.
    conn = _RequiredColsConnection(columns=[])

    result = target_required_columns_without_default(
        "t", connection_factory=lambda: conn
    )

    assert result == []


def test_required_columns_without_default_returns_none_for_a_missing_table() -> None:
    from dsql_migrator.core.target_introspector import (
        target_required_columns_without_default,
    )

    # Existence probe returns nothing -> None (unknown), NOT [] -- a missing table
    # must never look like "exists with no required columns".
    conn = _RequiredColsConnection(columns=[("x",)], exists=False)

    result = target_required_columns_without_default(
        "t", connection_factory=lambda: conn
    )

    assert result is None
    # The column list query is not even run once existence fails.
    assert len(conn.executed) == 1


def test_required_columns_without_default_returns_none_on_catalog_error() -> None:
    from dsql_migrator.core.target_introspector import (
        target_required_columns_without_default,
    )

    conn = _RequiredColsConnection(columns=[], raises=True)

    result = target_required_columns_without_default(
        "t", connection_factory=lambda: conn
    )

    assert result is None
    assert conn.closed is True
