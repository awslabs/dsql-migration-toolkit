# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Target Aurora DSQL catalog introspection (browsing + conflict detection).

The :class:`TargetIntrospector` queries the target's PostgreSQL system catalog
(``information_schema`` / ``pg_catalog``) over a ``psycopg`` connection and
assembles a :class:`~dsql_migrator.core.models.TargetInventory` object tree
(schemas -> tables/views -> columns/indexes). The tree powers the browsable
object view (Requirement 10.1) and the pre-apply existence/conflict check
(Requirement 10.3) used by the Schema Applier.

Scope: this component is read-only and only ever touches the *target*. It does
not access the source database and it does not execute any DDL/DML against the
target -- applying converted DDL is the Schema Applier's responsibility.

Design seams:

- The connection is obtained through an injectable ``connector_factory`` that
  defaults to building a :class:`~dsql_migrator.core.target_connection.DsqlConnector`
  for the supplied config. Tests inject a fake connector that returns canned
  catalog rows, so no live DSQL cluster (and no AWS call) is required.
- All catalog queries use static SQL with parameterized predicates (no string
  interpolation of values), in line with the untrusted-input handling rule
  (Requirement 9.4). ``object_exists`` resolves purely against the in-memory
  inventory and issues no SQL, so the looked-up name never reaches the database.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol, Sequence

from dsql_migrator.core.models import (
    TargetColumnDef,
    TargetConnectionConfig,
    TargetIndexDef,
    TargetInventory,
    TargetObjectKind,
    TargetRelation,
    TargetSchemaNode,
)

# System schemas that are never part of the user object tree.
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")

# Tables and views, with their type so views can be separated from tables.
RELATIONS_QUERY = (
    "SELECT table_schema, table_name, table_type "
    "FROM information_schema.tables "
    "WHERE table_schema <> ALL(%(excluded_schemas)s) "
    "ORDER BY table_schema, table_name"
)

# Columns of every user relation, ordered by their position within the table.
COLUMNS_QUERY = (
    "SELECT table_schema, table_name, column_name, data_type, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema <> ALL(%(excluded_schemas)s) "
    "ORDER BY table_schema, table_name, ordinal_position"
)

# Indexes of every user relation, with their uniqueness flag.
INDEXES_QUERY = (
    "SELECT n.nspname AS schema_name, t.relname AS table_name, "
    "i.relname AS index_name, ix.indisunique AS is_unique "
    "FROM pg_catalog.pg_index ix "
    "JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid "
    "JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid "
    "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "
    "WHERE n.nspname <> ALL(%(excluded_schemas)s) "
    "ORDER BY n.nspname, t.relname, i.relname"
)


class _Cursor(Protocol):
    """Minimal psycopg cursor contract used by the catalog queries."""

    def execute(self, statement: str, parameters: object = ...) -> object: ...

    def fetchall(self) -> list: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    """Minimal connection contract: yields cursors and can be closed."""

    def cursor(self) -> _Cursor: ...

    def close(self) -> None: ...


class _Connector(Protocol):
    """A factory of target connections (e.g. :class:`DsqlConnector`)."""

    def connect(self) -> _Connection: ...


# A connector factory builds a connector from a target connection config. It is
# injectable so unit tests can supply a fake that never reaches a real cluster.
ConnectorFactory = Callable[[TargetConnectionConfig], _Connector]


def _default_connector_factory(conn: TargetConnectionConfig) -> _Connector:
    """Build the default DSQL-backed connector for ``conn``.

    Imported lazily so importing this module does not pull in the target
    connection layer (and its ``boto3``/``psycopg`` dependencies) until used.
    """
    from dsql_migrator.core.target_connection import DsqlConnector

    return DsqlConnector(conn)


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise  # pylint: disable=broad-except
        pass


def _query(connection: _Connection, statement: str) -> list[tuple]:
    """Run a catalog query with the parameterized system-schema exclusion.

    The excluded schema names are passed as a bound parameter (never inlined),
    keeping the query injection-safe (Requirement 9.4).
    """
    cursor = connection.cursor()
    try:
        cursor.execute(statement, {"excluded_schemas": list(SYSTEM_SCHEMAS)})
        return list(cursor.fetchall())
    finally:
        _safe_close(cursor)


def _is_view(table_type: object) -> bool:
    """Return ``True`` when an ``information_schema`` table_type denotes a view."""
    return str(table_type or "").upper() == "VIEW"


def _to_nullable(is_nullable: object) -> bool:
    """Normalize an ``information_schema`` ``is_nullable`` value to a bool."""
    return str(is_nullable or "").upper() == "YES"


def build_inventory(
    relation_rows: list[tuple],
    column_rows: list[tuple],
    index_rows: list[tuple],
) -> TargetInventory:
    """Assemble a :class:`TargetInventory` tree from raw catalog rows.

    ``relation_rows`` are ``(schema, name, table_type)``; ``column_rows`` are
    ``(schema, table, column, data_type, is_nullable)``; ``index_rows`` are
    ``(schema, table, index_name, is_unique)``. Columns/indexes that reference an
    unknown relation are ignored so a partial catalog read still yields a valid
    tree.
    """
    relations: dict[tuple[str, str], TargetRelation] = {}
    schema_order: list[str] = []
    relation_order: list[tuple[str, str]] = []

    for schema_name, table_name, table_type in relation_rows:
        key = (schema_name, table_name)
        if key in relations:
            continue
        if schema_name not in schema_order:
            schema_order.append(schema_name)
        relation_order.append(key)
        relations[key] = TargetRelation(
            schema_name=schema_name,
            name=table_name,
            kind=TargetObjectKind.VIEW if _is_view(table_type) else TargetObjectKind.TABLE,
        )

    for schema_name, table_name, column_name, data_type, is_nullable in column_rows:
        relation = relations.get((schema_name, table_name))
        if relation is None:
            continue
        relation.columns.append(
            TargetColumnDef(
                name=column_name,
                data_type=str(data_type),
                nullable=_to_nullable(is_nullable),
            )
        )

    for schema_name, table_name, index_name, is_unique in index_rows:
        relation = relations.get((schema_name, table_name))
        if relation is None:
            continue
        relation.indexes.append(
            TargetIndexDef(name=index_name, unique=bool(is_unique))
        )

    schema_nodes: dict[str, TargetSchemaNode] = {
        name: TargetSchemaNode(name=name) for name in schema_order
    }
    for key in relation_order:
        relation = relations[key]
        node = schema_nodes[relation.schema_name]
        if relation.kind is TargetObjectKind.VIEW:
            node.views.append(relation)
        else:
            node.tables.append(relation)

    return TargetInventory(schemas=[schema_nodes[name] for name in schema_order])


class TargetIntrospector:
    """Browses the target DSQL catalog and answers object-existence queries.

    ``browse`` reads the catalog and returns the object tree, also caching a name
    index so that ``object_exists`` can answer pre-apply conflict checks without
    issuing any further SQL. The introspector is target-only and read-only.
    """

    def __init__(
        self, connector_factory: Optional[ConnectorFactory] = None
    ) -> None:
        """Create an introspector.

        ``connector_factory`` builds a target connector from a connection config;
        the default uses :class:`DsqlConnector`. Tests inject a factory that
        returns a fake connection so no real cluster or AWS call is needed.
        """
        self._connector_factory = connector_factory or _default_connector_factory
        self._qualified_names: Optional[set[str]] = None
        self._unqualified_names: Optional[set[str]] = None

    def browse(self, conn: TargetConnectionConfig) -> TargetInventory:
        """Read the target catalog and return the object tree (Requirement 10.1).

        Opens a connection via the connector, runs the three read-only catalog
        queries, assembles the tree, and caches a name index for
        :meth:`object_exists`. The connection is always closed before returning.
        """
        connector = self._connector_factory(conn)
        connection = connector.connect()
        try:
            relation_rows = _query(connection, RELATIONS_QUERY)
            column_rows = _query(connection, COLUMNS_QUERY)
            index_rows = _query(connection, INDEXES_QUERY)
        finally:
            _safe_close(connection)

        inventory = build_inventory(relation_rows, column_rows, index_rows)
        self._index_inventory(inventory)
        return inventory

    def object_exists(self, name: str) -> bool:
        """Report whether a target object already exists (Requirement 10.3).

        ``name`` may be qualified (``schema.name``) or unqualified (``name``);
        an unqualified name matches a relation in any schema. Tables, views, and
        secondary indexes are all covered, so a ``CREATE INDEX`` whose index
        already exists on the target is detected and can be skipped. Matching is
        case-insensitive to mirror PostgreSQL identifier folding. This consults
        the inventory cached by the most recent :meth:`browse` and issues no SQL,
        so the looked-up name never reaches the database. :meth:`browse` must be
        called first; otherwise a clear error is raised rather than returning a
        misleading ``False``.
        """
        if self._qualified_names is None or self._unqualified_names is None:
            raise RuntimeError("browse() must be called before object_exists().")

        normalized = name.strip().lower()
        if "." in normalized:
            return normalized in self._qualified_names
        return normalized in self._unqualified_names

    def _index_inventory(self, inventory: TargetInventory) -> None:
        """Build the case-insensitive name index used by :meth:`object_exists`.

        Tables, views, and their secondary indexes are all registered. In
        PostgreSQL/DSQL an index is itself a relation sharing the table/view
        namespace, so an index that already exists on the target must be
        discoverable here; otherwise the Schema Applier cannot honor
        ``SKIP_IF_EXISTS`` for a ``CREATE INDEX`` statement and the apply fails
        with ``relation "..." already exists`` (Requirement 10.3). An index's
        qualified name is ``schema.index_name`` (indexes live in the schema, not
        under the table); the unqualified form is the bare index name.
        """
        qualified: set[str] = set()
        unqualified: set[str] = set()
        for schema in inventory.schemas:
            for relation in (*schema.tables, *schema.views):
                qualified.add(relation.qualified_name.lower())
                unqualified.add(relation.name.lower())
                for index in relation.indexes:
                    qualified.add(f"{schema.name}.{index.name}".lower())
                    unqualified.add(index.name.lower())
        self._qualified_names = qualified
        self._unqualified_names = unqualified


def tables_with_rows(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> set[str]:
    """Return which of ``table_names`` currently hold at least one row on target.

    Read-only: runs a cheap ``SELECT 1 FROM <table> LIMIT 1`` per table over a
    single connection. Used by the Full Load "replace existing data" warning to
    tell the user exactly which selected target tables already contain data and
    would be dropped and recreated. A schema-qualified name (``schema.table``) is
    quoted with :class:`psycopg.sql.Identifier` (never interpolated). A table that
    does not exist or errors is treated as empty (it just will not be flagged).
    """
    from psycopg import sql

    present: set[str] = set()
    connection = connection_factory()
    try:
        for name in table_names:
            identifier = sql.Identifier(*name.split("."))
            statement = sql.SQL("SELECT 1 FROM {table} LIMIT 1").format(
                table=identifier
            )
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
                if cursor.fetchone() is not None:
                    present.add(name)
            except Exception:  # noqa: BLE001 - missing table/error means "no rows"
                pass
            finally:
                _safe_close(cursor)
    finally:
        _safe_close(connection)
    return present


def count_target_rows(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> dict[str, Optional[int]]:
    """Return an exact ``COUNT(*)`` per target table (read-only).

    Used by the CDC/Full Load per-table progress view to show how many rows have
    actually landed on the target, so the operator can see Full Load completion
    and CDC replication converge table by table (MSK Connect does not publish a
    per-table replicated-row metric, so the only honest source is a direct count).
    A schema-qualified ``schema.table`` is quoted with :class:`psycopg.sql.
    Identifier` (never interpolated). A table that does not exist or errors maps to
    ``None`` (unknown) rather than 0, so "not created yet" is distinguishable from
    "empty". All counts run over one connection.
    """
    from psycopg import sql

    counts: dict[str, Optional[int]] = {}
    connection = connection_factory()
    try:
        for name in table_names:
            identifier = sql.Identifier(*name.split("."))
            statement = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=identifier
            )
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
                row = cursor.fetchone()
                counts[name] = int(row[0]) if row is not None else None
            except Exception:  # noqa: BLE001 - missing table/error -> unknown
                counts[name] = None
            finally:
                _safe_close(cursor)
    finally:
        _safe_close(connection)
    return counts


def target_primary_key_columns(
    table_name: str,
    *,
    connection_factory: Callable[[], Any],
) -> Optional[list[str]]:
    """Return the target table's ACTUAL primary-key columns, in key order (read-only).

    The ground truth for "what key does the live target really have", read from
    ``pg_index``/``pg_attribute`` rather than inferred from the DDL the tool believes
    it applied. Full Load needs this on the append path: the applied conversion may
    ask for a composite ``(leading, id)`` key, and whether the target already has it
    decides between keying the idempotent load on those columns and refusing the load.
    Guessing either way is wrong -- assuming the target still has the source key
    blocks a correctly-migrated table, and assuming it has the new key could
    skip-wrong.

    ``table_name`` is ``schema.table`` (or bare, resolved against the search path) and
    is quoted via :class:`psycopg.sql.Identifier` -- never interpolated. Returns
    ``None`` when the key cannot be determined (table missing, no primary key, or any
    error), which callers MUST treat as "unknown" and not as "no key": an inability to
    read the catalog must never be mistaken for a definite answer.
    """
    # Resolve schema.table into the catalog's own terms. Both halves are passed as
    # BOUND PARAMETERS (never interpolated), so no identifier quoting is needed and
    # the query stays injection-safe (Requirement 9.4). A bare name is matched
    # against the current search path via pg_table_is_visible, mirroring how an
    # unqualified INSERT would resolve.
    #
    # ``indkey`` is ordered by key position, so unnesting WITH ORDINALITY and ordering
    # by it preserves the real key order -- (user_id, id) must not come back as
    # (id, user_id), since the leading column is the whole point of the composite-key
    # strategy.
    #
    # ``k.ord <= ix.indnkeyatts`` is REQUIRED, not a refinement. ``indkey`` lists the
    # index's KEY columns followed by its non-key "included"/stored columns, and only
    # the first ``indnkeyatts`` of them form the constraint. Aurora DSQL's primary
    # indexes routinely carry the remaining columns as stored payload (verified on a
    # live cluster: a 2-column table reported indnatts=2 with indnkeyatts=1, and an
    # 11-table schema reported EVERY column for EVERY table). Without this bound the
    # function returned the full column list, which then never equals the applied
    # composite key -- so every append with a changed key would be refused with a
    # message naming an absurd "actual" key. Cross-checked against
    # information_schema.key_column_usage on the same cluster: 11/11 tables now agree.
    parts = table_name.split(".", 1)
    if len(parts) == 2:
        schema, relname = parts
        where_relation = "n.nspname = %(schema)s AND c.relname = %(table)s"
        params: dict[str, object] = {"schema": schema, "table": relname}
    else:
        where_relation = (
            "c.relname = %(table)s AND pg_catalog.pg_table_is_visible(c.oid)"
        )
        params = {"table": parts[0]}
    statement = (
        "SELECT a.attname "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_index ix ON ix.indrelid = c.oid AND ix.indisprimary "
        "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE "
        "JOIN pg_catalog.pg_attribute a "
        "  ON a.attrelid = c.oid AND a.attnum = k.attnum "
        f"WHERE {where_relation} AND k.ord <= ix.indnkeyatts "
        "ORDER BY k.ord"
    )
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> unknown
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(statement, params)
        columns = [str(row[0]) for row in cursor.fetchall() if row and row[0]]
        return columns or None
    except Exception:  # noqa: BLE001 - unreadable catalog -> unknown, never a guess
        return None
    finally:
        if cursor is not None:
            _safe_close(cursor)
        _safe_close(connection)


def max_pk_target(
    pk_by_table: dict[str, str],
    *,
    connection_factory: Callable[[], Any],
) -> dict[str, Optional[int]]:
    """Return ``MAX(pk)`` per target table for a single-column integer PK (read-only).

    ``pk_by_table`` maps a qualified ``schema.table`` to its single PK column name.
    Used by the CDC consistency view to compare the target's high-water PK against
    the source's: an equal max means the stream's leading edge has caught up (no
    lag) even if the row COUNTs differ (a mid-stream gap). Returns ``None`` for a
    table whose PK is not a single integer column, is missing/empty, or errors.
    All reads run over one connection.
    """
    from psycopg import sql

    out: dict[str, Optional[int]] = {}
    connection = connection_factory()
    try:
        for name, pk in pk_by_table.items():
            if not pk:
                out[name] = None
                continue
            table_id = sql.Identifier(*name.split("."))
            col_id = sql.Identifier(pk)
            statement = sql.SQL("SELECT MAX({col}) FROM {table}").format(
                col=col_id, table=table_id
            )
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
                row = cursor.fetchone()
                val = row[0] if row is not None else None
                out[name] = int(val) if isinstance(val, int) else None
            except Exception:  # noqa: BLE001 - missing/non-integer/error -> unknown
                out[name] = None
            finally:
                _safe_close(cursor)
    finally:
        _safe_close(connection)
    return out


def sync_identity_sequences(
    table_names: "Sequence[str]",
    *,
    connection_factory: Callable[[], Any],
) -> dict[str, Optional[int]]:
    """Advance each target identity sequence past the loaded rows. Returns per-table
    ``RESTART WITH`` value, or ``None`` when the table has no identity column / nothing
    to do / could not be read.

    WHY THIS IS REQUIRED, not a nicety. The converter's ``IDENTITY_WITH_CACHE`` strategy
    emits ``GENERATED BY DEFAULT AS IDENTITY``, and ``BY DEFAULT`` is what lets Full Load
    write the source's own key values. But an explicitly-supplied value does NOT advance
    the underlying sequence -- so after a load the sequence still sits at its start while
    those values are already taken, and the application's FIRST insert after cut-over
    fails with a duplicate-key error. Verified on a live ap-northeast-2 cluster: load ids
    1..3, then an id-less insert raised
    ``duplicate key value violates unique constraint``.

    That is the worst shape a migration failure can take: row counts and checksums MATCH,
    so Validation passes clean, and it only surfaces after cut-over -- once the source has
    been frozen. Leaving it to a manual runbook step would make a silent, post-cut-over
    outage depend on someone remembering; the tool can read ``MAX(pk)`` and fix it.

    ``ALTER TABLE ... ALTER COLUMN ... RESTART WITH`` is used (also verified live: the next
    generated id became exactly the requested value, and subsequent ids continued from it
    despite ``CACHE 65536``). Every statement is built with ``psycopg.sql.Identifier`` --
    never interpolated (Requirement 9.4).

    Best-effort per table: a table with no identity column is skipped (``None``), and any
    error leaves that table untouched rather than failing the load -- but the caller MUST
    surface the outcome, because an unrepaired sequence is a post-cut-over failure.
    """
    from psycopg import sql

    out: dict[str, Optional[int]] = {}
    connection = connection_factory()
    try:
        for name in table_names:
            out[name] = None
            parts = name.split(".", 1)
            if len(parts) == 2:
                schema_name, bare = parts
            else:
                schema_name, bare = None, parts[0]
            cursor = connection.cursor()
            try:
                # Identity columns only: ``is_identity`` is the catalog's own answer, so a
                # plain integer key (the KEEP_INTEGER default) is correctly skipped rather
                # than having a sequence invented for it. Bound parameters throughout.
                if schema_name is not None:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "AND is_identity = 'YES'",
                        (schema_name, bare),
                    )
                else:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = %s AND is_identity = 'YES' "
                        "AND table_schema = ANY (current_schemas(false))",
                        (bare,),
                    )
                rows = cursor.fetchall() or []
            except Exception:  # noqa: BLE001 - unreadable catalog -> leave untouched
                _safe_close(cursor)
                continue
            _safe_close(cursor)
            if not rows:
                continue  # no identity column: nothing to sync
            column = str(rows[0][0])

            table_id = sql.Identifier(*name.split("."))
            col_id = sql.Identifier(column)
            cursor = connection.cursor()
            try:
                cursor.execute(
                    sql.SQL("SELECT MAX({col}) FROM {table}").format(
                        col=col_id, table=table_id
                    )
                )
                row = cursor.fetchone()
                current_max = row[0] if row is not None else None
            except Exception:  # noqa: BLE001
                _safe_close(cursor)
                continue
            _safe_close(cursor)
            if not isinstance(current_max, int):
                # Empty table (or a non-integer identity): the sequence's own start is
                # already correct, so touching it would only risk moving it backwards.
                continue

            restart_at = current_max + 1
            cursor = connection.cursor()
            try:
                cursor.execute(
                    sql.SQL(
                        "ALTER TABLE {table} ALTER COLUMN {col} RESTART WITH {value}"
                    ).format(
                        table=table_id, col=col_id, value=sql.Literal(restart_at)
                    )
                )
                out[name] = restart_at
            except Exception:  # noqa: BLE001 - leave it; the caller reports the gap
                pass
            finally:
                _safe_close(cursor)
    finally:
        _safe_close(connection)
    return out


__all__ = [
    "TargetIntrospector",
    "ConnectorFactory",
    "build_inventory",
    "count_target_rows",
    "max_pk_target",
    "sync_identity_sequences",
    "tables_with_rows",
    "target_primary_key_columns",
    "SYSTEM_SCHEMAS",
    "RELATIONS_QUERY",
    "COLUMNS_QUERY",
    "INDEXES_QUERY",
]
