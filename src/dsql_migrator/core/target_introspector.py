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

import re
import time as _time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Collection, Mapping, Optional, Protocol, Sequence, Union

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
#
# ``sys`` is Aurora DSQL's OWN catalog (iam_identity, job, address_map, the jobs/roles
# views, ...), not a user schema -- and it is not in PostgreSQL's system set, so a
# completely EMPTY DSQL cluster was reported as "1 schemas, 3 tables, 4 views". Verified
# against a live cluster: the only non-PostgreSQL schemas are the empty ``public`` and
# ``sys``, whose three tables and four views are exactly the counts the Evaluation report
# was showing. That made an empty target look occupied on the Evaluation summary and in
# Schema Conversion's object browser, and fed DSQL internals into
# ``target_existing_table_names`` (which treats a table's presence on the target as making
# the source table migratable). DSQL owns the name -- ``sys`` already exists on every
# cluster, so no user schema can take it -- and the MySQL source introspector likewise
# excludes its own ``sys`` (``MYSQL_SYSTEM_SCHEMAS``), so this is the symmetric rule.
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast", "sys")

# Every user schema, so an EMPTY one is seen: Aurora DSQL counts it toward its 10-schema
# limit, and a schema holding no table or view never appears in RELATIONS_QUERY.
SCHEMAS_QUERY = (
    "SELECT nspname FROM pg_catalog.pg_namespace "
    "WHERE nspname <> ALL(%(excluded_schemas)s) AND nspname NOT LIKE 'pg\\_%%' "
    "ORDER BY nspname"
)

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


def _table_identifier(name: str):
    """A psycopg identifier for a ``schema.table`` name, splitting on the FIRST dot only.

    ``sql.Identifier(*name.split("."))`` splits on EVERY dot, so a table whose bare name
    contains one (``app."orders.2024"``) became a three-part identifier and every statement
    built from it addressed a relation that does not exist. The catalog reads in this module
    already use ``split(".", 1)``, so the two disagreed -- and the callers that swallowed the
    resulting error reported a silent no-op. One helper keeps every call site consistent.
    """
    from psycopg import sql

    schema_name, _, bare = name.partition(".")
    return sql.Identifier(schema_name, bare) if bare else sql.Identifier(name)


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
    schema_rows: "Sequence[tuple]" = (),
) -> TargetInventory:
    """Assemble a :class:`TargetInventory` tree from raw catalog rows.

    ``relation_rows`` are ``(schema, name, table_type)``; ``column_rows`` are
    ``(schema, table, column, data_type, is_nullable)``; ``index_rows`` are
    ``(schema, table, index_name, is_unique)``; ``schema_rows`` are ``(schema,)`` for
    every user schema, so one holding no relation is still in the tree (after the others).
    Columns/indexes that reference an unknown relation are ignored so a partial catalog
    read still yields a valid tree.
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

    for row in schema_rows:
        if row and row[0] not in schema_order:
            schema_order.append(row[0])
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
        self._holders: dict[str, tuple[str, Optional[str]]] = {}

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
            try:
                schema_rows = _query(connection, SCHEMAS_QUERY)
            except Exception:  # noqa: BLE001 - extra, best effort: the tree stands without it
                schema_rows = []
        finally:
            _safe_close(connection)

        inventory = build_inventory(relation_rows, column_rows, index_rows, schema_rows)
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

    def relation_holder(self, name: str) -> "Optional[tuple[str, Optional[str]]]":
        """What holds the schema-qualified ``name`` on the target, from the last browse.

        ``("TABLE" | "VIEW", None)`` for a table or view, ``("INDEX", "<schema>.<table>")``
        for an index (with the table it is on), or ``None`` when unknown -- an unqualified
        name, or one the browse did not see. Lets the applier tell "this object is already
        there" apart from "another relation holds this name": Aurora DSQL keeps a schema's
        tables, views and indexes in one namespace. EXACT, unlike :meth:`object_exists`:
        ``name`` is matched as the catalog stores it (case included, see
        :func:`pg_stored_name`), so ``"Currency"`` and ``currency`` are two names, as they
        are on the target. Issues no SQL.
        """
        return self._holders.get(name)

    def _index_inventory(self, inventory: TargetInventory) -> None:
        """Build the case-insensitive name index used by :meth:`object_exists`.

        Tables, views, and their secondary indexes are all registered. In
        PostgreSQL/DSQL an index is itself a relation sharing the table/view
        namespace, so an index that already exists on the target must be
        discoverable here; otherwise the Schema Applier cannot honor
        ``SKIP_IF_EXISTS`` for a ``CREATE INDEX`` statement and the apply fails
        with ``relation "..." already exists`` (Requirement 10.3). An index's
        qualified name is ``schema.index_name`` (indexes live in the schema, not
        under the table); the unqualified form is the bare index name. The holder map
        behind :meth:`relation_holder` is keyed by the exact stored names instead.
        """
        qualified: set[str] = set()
        unqualified: set[str] = set()
        holders: dict[str, tuple[str, Optional[str]]] = {}
        for schema in inventory.schemas:
            for relation in (*schema.tables, *schema.views):
                qualified.add(relation.qualified_name.lower())
                unqualified.add(relation.name.lower())
                holders.setdefault(relation.qualified_name, (relation.kind.value, None))
                for index in relation.indexes:
                    qualified.add(f"{schema.name}.{index.name}".lower())
                    unqualified.add(index.name.lower())
                    holders.setdefault(
                        f"{schema.name}.{index.name}", ("INDEX", relation.qualified_name)
                    )
        self._qualified_names = qualified
        self._unqualified_names = unqualified
        self._holders = holders


# What holds one stored name in one schema now: its kind and, for an index, the table it is
# on. Bound parameters only (no identifier in the SQL text).
_HOLDER_QUERY = (
    "SELECT c.relkind, tn.nspname, t.relname "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "LEFT JOIN pg_catalog.pg_index ix ON ix.indexrelid = c.oid "
    "LEFT JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid "
    "LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace "
    "WHERE n.nspname = %s AND c.relname = %s"
)
_HOLDER_KINDS = {
    "r": "TABLE",
    "p": "TABLE",
    "v": "VIEW",
    "m": "MATERIALIZED VIEW",
    "i": "INDEX",
    "I": "INDEX",
    "S": "SEQUENCE",
}


def live_relation_holder(
    connection: Any, schema: str, name: str
) -> "Optional[tuple[str, Optional[str]]]":
    """What holds ``schema.name`` on the target NOW, shaped like
    :meth:`TargetIntrospector.relation_holder`; ``None`` when the name is free. RAISES.

    Both names are matched as the catalog stores them (:func:`pg_stored_name`). The browse
    behind ``relation_holder`` is a snapshot: an earlier REPLACE of the same run can have
    dropped the holder, or the user removed a leftover since. A holder that is neither a
    table, a view nor an index (a sequence, say) has the kind ``SEQUENCE`` or ``RELATION``.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(_HOLDER_QUERY, (pg_stored_name(schema), pg_stored_name(name)))
        rows = cursor.fetchall()
    finally:
        _safe_close(cursor)
    if not rows:
        return None
    relkind, on_schema, on_table = rows[0][0], rows[0][1], rows[0][2]
    kind = _HOLDER_KINDS.get(str(relkind), "RELATION")
    if kind == "INDEX" and on_schema and on_table:
        return kind, f"{on_schema}.{on_table}"
    return kind, None


def _target_holders(target: TargetInventory) -> "dict[tuple[str, str], tuple[str, Optional[str]]]":
    """(schema, stored name) -> what holds it on the target, shaped like
    :meth:`TargetIntrospector.relation_holder`: tables, views AND indexes (one namespace)."""
    holders: dict[tuple[str, str], tuple[str, Optional[str]]] = {}
    for schema in target.schemas:
        for relation in (*schema.tables, *schema.views):
            holders.setdefault((schema.name, relation.name), (relation.kind.value, None))
            for index in relation.indexes:
                holders.setdefault(
                    (schema.name, index.name), ("INDEX", relation.qualified_name)
                )
    return holders


def _stored_qualified(name: str) -> "Optional[tuple[str, str]]":
    schema, _, rest = name.partition(".")
    return (pg_stored_name(schema), pg_stored_name(rest)) if schema and rest else None


# What to do about the held names Evaluation lists (on screen and in the HTML export):
# the summary of :meth:`TargetNames.held_advice`. The copy an earlier version made of an
# index the conversion renames goes only after its table is applied -- dropping it first
# leaves the table without that index.
BLOCKED_NAMES_ADVICE = (
    "Drop or rename that object on the target, then apply -- except the copy an earlier "
    "version made of an index: apply that index's table first (it creates the index under "
    "its new name; a Replace of it also drops the copy), then drop the copy if it is still "
    "there."
)


class TargetNames:
    """The browsed target's names, matched the way Schema Apply matches them -- one rule
    for Evaluation (conflicts, limit counts, blocked names) and Schema Conversion (labels,
    the Replace/Skip prompt, the REPLACE confirmation), so the two steps cannot disagree.

    A qualified source name (``database.table`` / ``schema.table``, split on the first dot)
    is matched EXACTLY on stored names (the converter quotes every name, each part cut to
    63 bytes: :func:`pg_stored_name`), and only an object of the SAME kind counts
    (``views``: the source's view names); an unqualified one is matched case-insensitively
    against bare names in any schema. ``legacy_indexes`` (:class:`ConvertedNames`) names
    the copies an earlier version made of the indexes the conversion renames."""

    def __init__(
        self,
        target: TargetInventory,
        *,
        views: "Collection[str]" = (),
        legacy_indexes: "Optional[Mapping]" = None,
    ) -> None:
        self._holders = _target_holders(target)
        self._legacy_indexes = dict(legacy_indexes or {})
        self._unqualified = {
            relation.name.lower()
            for schema in target.schemas
            for relation in (*schema.tables, *schema.views)
        }
        self._views = set(views)
        self._own_views = {_stored_qualified(name) for name in self._views} - {None}

    def _kind(self, name: str) -> str:
        return "VIEW" if name in self._views else "TABLE"

    def exists(self, name: str) -> bool:
        """Whether the source relation ``name`` is already on the target."""
        stored = _stored_qualified(name)
        if stored is None:
            return name.strip().lower() in self._unqualified
        return self._holders.get(stored) == (self._kind(name), None)

    def _blocker(self, name: str) -> "Optional[tuple[tuple[str, str], str, Optional[str]]]":
        stored = _stored_qualified(name)
        holder = self._holders.get(stored) if stored is not None else None
        if holder is None or holder == (self._kind(name), None):
            return None
        return stored, holder[0], holder[1]  # type: ignore[return-value]

    def _index_copy(self, stored: "tuple[str, str]", owner: "Optional[str]") -> "Optional[str]":
        """The index the conversion now creates (on ``owner``) of which the index holding
        ``stored`` is an earlier version's copy, else ``None``."""
        copy_of = self._legacy_indexes.get(stored)
        return copy_of[1] if copy_of is not None and copy_of[0] == owner else None

    def blocked_by(self, name: str) -> "Optional[str]":
        """What ANOTHER kind of object holding ``name``'s stored name is (``"a view"``,
        ``"an index on s.t"``): Schema Apply refuses it in both modes. ``None`` otherwise."""
        blocker = self._blocker(name)
        if blocker is None:
            return None
        stored, kind, owner = blocker
        if kind != "INDEX":
            return f"a {kind.lower()}"
        renamed = self._index_copy(stored, owner)
        if renamed is not None:
            return f"the copy an earlier version made of index {renamed} on {owner}"
        return f"an index on {owner}"

    def held_advice(self, name: str) -> "Optional[str]":
        """What to do about the object :meth:`blocked_by` names (``None``: there is none).
        Drop or rename it -- unless it is an earlier version's copy of an index the
        conversion renames, which goes only once the renamed index is there: dropping it
        first leaves the table without that index (a UNIQUE one unenforced) until the
        table is applied again. The order Schema Apply itself gives."""
        blocker = self._blocker(name)
        if blocker is None:
            return None
        stored, kind, owner = blocker
        renamed = self._index_copy(stored, owner) if kind == "INDEX" else None
        if renamed is None:
            return "Drop or rename that object on the target, then apply."
        from dsql_migrator.core.schema_applier import _quoted_name

        drop = f"DROP INDEX {_quoted_name(*stored)}"
        if self._holders.get((stored[0], pg_stored_name(renamed))) == ("INDEX", owner):
            return f"{renamed} is already there, so drop the copy ({drop}), then apply."
        return (
            f"Apply {owner} first (it creates {renamed}; a Replace of it also drops the "
            f"copy), then drop the copy ({drop}) if it is still there, and apply."
        )

    def old_view_copy(self, name: str) -> "Optional[str]":
        """For a source view: the lower-case copy an earlier version created (unquoted)
        that a REPLACE drops with it (``schema.name``), else ``None``."""
        if name not in self._views:
            return None
        stored = _stored_qualified(name)
        if stored is None:
            return None
        folded = (pg_fold_unquoted(stored[0]), pg_fold_unquoted(stored[1]))
        if folded == stored or folded in self._own_views:
            return None
        if self._holders.get(folded) != ("VIEW", None):
            return None
        return f"{folded[0]}.{folded[1]}"


def relations_existing_on_target(
    names: "Sequence[str]",
    target: TargetInventory,
    *,
    views: "Collection[str]" = (),
) -> list[str]:
    """Return the ``names`` (source tables/views) that already exist on the target. Pure.

    The one matching rule for "this source relation is already there" (:class:`TargetNames`),
    shared by the Evaluation name-conflict list, the per-database limit counts (a relation
    that is already on the target adds nothing to them) and Schema Conversion. A name
    another kind of object holds is :func:`relations_blocked_on_target` instead. Input
    order is preserved.
    """
    matcher = TargetNames(target, views=views)
    return [name for name in names if matcher.exists(name)]


def relations_blocked_on_target(
    names: "Sequence[str]",
    target: TargetInventory,
    *,
    views: "Collection[str]" = (),
) -> "list[tuple[str, str]]":
    """``(name, what holds it)`` for each source table/view whose stored name ANOTHER kind
    of object holds on the target -- a view where a table goes, or an index (an earlier
    apply may have left one under a table's name). Schema Apply cannot create those in
    either mode until that object is dropped or renamed. Pure; input order is preserved."""
    matcher = TargetNames(target, views=views)
    blocked: list[tuple[str, str]] = []
    for name in names:
        holder = matcher.blocked_by(name)
        if holder is not None:
            blocked.append((name, holder))
    return blocked


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
            identifier = _table_identifier(name)
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


def tables_present(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> set[str]:
    """Return which of ``table_names`` EXIST on the target (regardless of row count).

    Distinct from :func:`tables_with_rows`, which treats a missing table as "empty"
    (it only cares whether a table HAS rows): this tells "missing" apart from "empty",
    so the Full Load confirm can warn that an **append** into a non-existent target
    table will fail (a table deleted / never created by Schema Conversion). Read-only:
    ``SELECT 1 FROM <table> LIMIT 0`` per table over a single connection, using the SAME
    identifier quoting the load uses (so it resolves the table exactly as the load
    would). A table that errors (does not exist) is simply absent from the result; the
    transaction is rolled back after each check so one missing table cannot abort the
    checks for the rest.
    """
    from psycopg import sql

    present: set[str] = set()
    connection = connection_factory()
    try:
        for name in table_names:
            identifier = _table_identifier(name)
            statement = sql.SQL("SELECT 1 FROM {table} LIMIT 0").format(
                table=identifier
            )
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
                present.add(name)  # the statement succeeded -> the table exists
            except Exception:  # noqa: BLE001 - a missing table just is not "present"
                # A failed statement can leave the transaction aborted (PostgreSQL),
                # which would make every subsequent check fail; reset it so each table
                # is tested independently. Harmless under an autocommit connection.
                try:
                    connection.rollback()
                except Exception:  # noqa: BLE001 - best effort
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
            identifier = _table_identifier(name)
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


def target_unique_keys(
    table_name: str,
    *,
    connection_factory: Callable[[], Any],
) -> "Optional[list[list[str]]]":
    """Every unique index of the LIVE target table, as its key columns (read-only), or None.

    One ``pg_index`` read (G137): the Full Load's reader-shard guard needs the unique keys
    an APPEND target really enforces, which the applied DDL cannot be trusted to describe
    (an index added by hand, an edit applied since). The primary key's index is one of
    them; a partial, an invalid or a still-building unique index counts too -- each checks
    new writes. ``None`` (UNKNOWN, which the caller treats as a refusal) when the catalog
    cannot be read, the table has no unique index (it is missing: Aurora DSQL requires a
    primary key) or an index holds an expression (fewer named key columns than
    ``indnkeyatts``).
    """
    parts = table_name.split(".", 1)
    if len(parts) == 2:
        where_relation = "n.nspname = %(schema)s AND c.relname = %(table)s"
        params: dict[str, object] = {"schema": parts[0], "table": parts[1]}
    else:
        where_relation = (
            "c.relname = %(table)s AND pg_catalog.pg_table_is_visible(c.oid)"
        )
        params = {"table": parts[0]}
    statement = (
        "SELECT ix.indnkeyatts, "
        "  ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a "
        "        JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) "
        "          ON a.attnum = k.attnum "
        "        WHERE a.attrelid = c.oid AND k.ord <= ix.indnkeyatts "
        "        ORDER BY k.ord) AS key_columns "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_index ix ON ix.indrelid = c.oid AND ix.indisunique "
        f"WHERE {where_relation}"
    )
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> unknown
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(statement, params)
        rows = cursor.fetchall()
    except Exception:  # noqa: BLE001 - unreadable catalog -> unknown, never a guess
        return None
    finally:
        if cursor is not None:
            _safe_close(cursor)
        _safe_close(connection)
    keys: list[list[str]] = []
    for row in rows or ():
        columns = [str(c) for c in (row[1] or ()) if c]
        if not columns or len(columns) != int(row[0] or 0):
            return None  # an expression key column (attnum 0) names no attribute
        keys.append(columns)
    return keys or None


def target_generated_columns(
    table_name: str,
    *,
    connection_factory: Callable[[], Any],
) -> "Optional[set[str]]":
    """Return the target table's ACTUAL generated columns (read-only), or ``None``.

    The ground truth for "does the live target compute this column", read from
    ``pg_attribute.attgenerated`` rather than inferred from the DDL the tool believes it
    applied -- the same two-signal split Full Load already uses for the primary key
    (:func:`target_primary_key_columns` on the append path vs
    :func:`~dsql_migrator.core.converter.parse_target_primary_key` on the recreate path).

    It matters because the applied DDL is NOT authoritative on the append path: a target
    created by an older build (which stripped the clause), by the customer's own DDL, or by
    an edited-then-reverted script can have an ORDINARY column where this run's conversion
    says GENERATED. Trusting the DDL there would drop the column from the INSERT and write
    NULL into every row -- silently, with no error and no quarantine.

    ``None`` means UNKNOWN (table missing, unreadable catalog, any error) and callers MUST
    NOT read it as "no generated columns": sending a value to a generated column fails the
    whole batch (428C9), and omitting one from an ordinary column nulls the data, so the
    only safe response to unknown is to refuse.

    Verified against a live Aurora DSQL cluster: a ``GENERATED ALWAYS AS (...) STORED``
    column reports ``attgenerated = 's'``.
    """
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
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        f"WHERE {where_relation} "
        "  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''"
    )
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> unknown
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        # Confirm the table EXISTS before reading "no generated columns" off an empty
        # result: a missing table would otherwise look identical to a plain one.
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE {where_relation}",
            params,
        )
        if cursor.fetchone() is None:
            return None
        cursor.execute(statement, params)
        return {str(row[0]) for row in cursor.fetchall() if row and row[0]}
    except Exception:  # noqa: BLE001 - unreadable catalog -> unknown, never a guess
        return None
    finally:
        if cursor is not None:
            _safe_close(cursor)
        _safe_close(connection)


@dataclass(frozen=True)
class GeneratedColumnsRead:
    """What :func:`target_generated_columns_of` read: each existing table's generated
    columns, the tables that do not exist, and those whose catalog could not be read."""

    columns: "dict[str, set[str]]"
    missing: "tuple[str, ...]" = ()
    unreadable: "tuple[str, ...]" = ()
    # Every column of each existing table, generated or not.
    names: "dict[str, set[str]]" = field(default_factory=dict)


def target_generated_columns_of(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> GeneratedColumnsRead:
    """The bulk twin of :func:`target_generated_columns`, over ONE connection: a DSQL
    connect mints an IAM token and does a TLS handshake, and Start CDC read every captured
    table this way. A missing table is told apart from an unreadable one (a connection
    failure makes every table unreadable)."""
    columns: dict[str, set[str]] = {}
    names: dict[str, set[str]] = {}
    missing: list[str] = []
    unreadable: list[str] = []
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> every table unreadable
        return GeneratedColumnsRead(columns={}, unreadable=tuple(table_names))
    try:
        for table_name in table_names:
            parts = table_name.split(".", 1)
            if len(parts) == 2:
                where_relation = "n.nspname = %(schema)s AND c.relname = %(table)s"
                params: dict[str, object] = {"schema": parts[0], "table": parts[1]}
            else:
                where_relation = "c.relname = %(table)s AND pg_catalog.pg_table_is_visible(c.oid)"
                params = {"table": parts[0]}
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT a.attname, a.attgenerated "
                    "FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                    "  AND a.attnum > 0 AND NOT a.attisdropped "
                    f"WHERE {where_relation}",
                    params,
                )
                rows = cursor.fetchall()
                if not rows:
                    missing.append(table_name)
                    continue
                columns[table_name] = {
                    str(row[0]) for row in rows if row and row[0] and row[1] not in (None, "")
                }
                names[table_name] = {str(row[0]) for row in rows if row and row[0]}
            except Exception:  # noqa: BLE001 - unreadable catalog -> unknown for this one
                unreadable.append(table_name)
            finally:
                if cursor is not None:
                    _safe_close(cursor)
    finally:
        _safe_close(connection)
    return GeneratedColumnsRead(
        columns=columns, missing=tuple(missing), unreadable=tuple(unreadable), names=names
    )


def target_primary_keys(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> dict[str, Optional[list[str]]]:
    """Return each table's ACTUAL primary-key columns, over ONE connection.

    The bulk twin of :func:`target_primary_key_columns`: same per-table catalog
    query and identical semantics (``None`` == unknown / unreadable, never a
    guess), but every table is resolved on a SINGLE reused connection instead of
    one connect per table. Opening a DSQL connection mints an IAM token and does a
    full TLS handshake to a distributed endpoint (~0.5-1s each, cross-region), so
    the per-table-connect version made the pre-Full-Load probe cost N+1 handshakes
    -- seconds of latency before the confirm dialog could open. Mirrors how
    :func:`count_target_rows` / :func:`tables_with_rows` already loop over one
    connection.

    A connection failure maps every table to ``None`` (all unknown); a per-table
    query error maps just that table to ``None``, so one bad relation never hides
    the rest.
    """
    result: dict[str, Optional[list[str]]] = {}
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> every table unknown
        return {name: None for name in table_names}
    try:
        for table_name in table_names:
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
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute(statement, params)
                columns = [str(row[0]) for row in cursor.fetchall() if row and row[0]]
                result[table_name] = columns or None
            except Exception:  # noqa: BLE001 - unreadable catalog -> unknown for this one
                result[table_name] = None
            finally:
                if cursor is not None:
                    _safe_close(cursor)
    finally:
        _safe_close(connection)
    return result


@dataclass(frozen=True)
class ColumnNullFacts:
    """One live target column as the exclusion overlay sees it.

    ``default_expr`` is ``pg_get_expr`` of its DEFAULT, verbatim (P0 F4: it round-trips
    through DROP/SET DEFAULT), and always ``None`` for a GENERATED column, whose
    ``pg_attrdef`` entry is the generation expression, not a DEFAULT.
    """

    not_null: bool
    default_expr: Optional[str]
    identity: bool = False
    generated: bool = False

    @property
    def computed(self) -> bool:
        """Aurora DSQL computes the value (identity / generated): never overlaid."""
        return self.identity or self.generated


@dataclass(frozen=True)
class TableOverlayFacts:
    """A live target table's overlay facts.

    ``has_rows`` is ``None`` when the row probe was not requested; ``primary_key`` is
    ``None`` when not requested and ``()`` when requested for a table without one. A
    column mapped to ``None`` is absent live.
    """

    exists: bool
    has_rows: Optional[bool]
    primary_key: "Optional[tuple[str, ...]]"
    columns: "Mapping[str, Optional[ColumnNullFacts]]"


def _relation_predicate(table_name: str) -> "tuple[str, dict[str, object]]":
    """The WHERE clause that resolves ``table_name`` exactly as the loader's INSERT does.

    ``schema.table`` (split on the FIRST dot, as :func:`_table_identifier`) names that
    relation; a bare name resolves through the search_path (``pg_table_is_visible``) --
    the same relation an unqualified INSERT or ALTER addresses. NOT the all-user-schemas
    rule of :func:`target_foreign_keys`, which could describe a different relation.
    """
    schema, _, relname = table_name.partition(".")
    if relname:
        return "n.nspname = %(schema)s AND c.relname = %(table)s", {
            "schema": schema,
            "table": relname,
        }
    return "c.relname = %(table)s AND pg_catalog.pg_table_is_visible(c.oid)", {
        "table": table_name
    }


# PostgreSQL (and Aurora DSQL) stores an identifier cut to NAMEDATALEN-1 bytes.
_PG_NAME_MAX_BYTES = 63


def pg_fold_unquoted(name: str) -> str:
    """``name`` as PostgreSQL folds an UNQUOTED identifier: ASCII letters to lower case."""
    return "".join(ch.lower() if "A" <= ch <= "Z" else ch for ch in name)


def pg_stored_name(name: str) -> str:
    """``name`` as the catalog stores it: UTF-8 cut to 63 bytes on a character boundary
    (PostgreSQL's pg_mbcliplen). A DDL or DML naming the long form reaches this column."""
    raw = name.encode("utf-8")
    if len(raw) <= _PG_NAME_MAX_BYTES:
        return name
    return raw[:_PG_NAME_MAX_BYTES].decode("utf-8", errors="ignore")


def read_overlay_column_facts(
    connection: Any, table_name: str, columns: Sequence[str]
) -> "tuple[bool, dict[str, Optional[ColumnNullFacts]]]":
    """``(table exists, {column: facts or None})`` in ONE catalog statement. RAISES.

    Bound parameters only (no identifier in the SQL text). The result is keyed by the
    names ASKED for, each matched EXACTLY against its stored form (:func:`pg_stored_name`:
    a name over 63 bytes is stored cut, and the loader's INSERT and the ALTER reach it
    that way); two asked names that the catalog would store as one raise. It raises on
    any error on purpose: the overlay unit needs the exception to retry a 40001 and to
    replay a lost connection (``core.schema_applier.apply_exclusion_overlay``).
    """
    by_stored: dict[str, str] = {}
    for column in columns:
        stored = pg_stored_name(column)
        if by_stored.setdefault(stored, column) != column:
            raise ValueError(
                f"{table_name}: columns {by_stored[stored]!r} and {column!r} are the same "
                "column on Aurora DSQL (identifiers are cut to 63 bytes)"
            )
    where, params = _relation_predicate(table_name)
    params = {**params, "columns": list(by_stored)}
    statement = (
        "SELECT a.attname, a.attnotnull, a.attidentity, a.attgenerated, "
        "pg_catalog.pg_get_expr(d.adbin, d.adrelid) "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 "
        "AND NOT a.attisdropped AND a.attname = ANY(%(columns)s) "
        "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum "
        f"WHERE {where}"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(statement, params)
        rows = cursor.fetchall()
    finally:
        _safe_close(cursor)
    facts: dict[str, Optional[ColumnNullFacts]] = {column: None for column in columns}
    if not rows:
        return False, facts
    for attname, notnull, identity, generated, default_expr in rows:
        asked = None if attname is None else by_stored.get(str(attname))
        if asked is None:
            continue  # the table exists; none of the asked columns does
        is_generated = bool(generated) and str(generated) != ""
        facts[asked] = ColumnNullFacts(
            not_null=bool(notnull),
            default_expr=None if is_generated else (
                None if default_expr is None else str(default_expr)
            ),
            identity=str(identity or "") in ("a", "d"),
            generated=is_generated,
        )
    return True, facts


# One OC001 on a connection used before a DDL (P0 F6) or a dropped socket must not make a
# table "unknown" (its load would then fail closed for nothing): each table's reads retry a
# 40001 or an XX000 (Aurora DSQL's temporary server unavailable) on the same connection, and
# a lost connection is replaced a bounded number of times.
_FACTS_OCC_ATTEMPTS = 3
_FACTS_RECONNECTS = 2


def target_overlay_facts(
    table_names: Sequence[str],
    columns_by_table: "Mapping[str, Sequence[str]]",
    *,
    connection_factory: Callable[[], Any],
    probe_rows: bool = True,
    primary_key_tables: "Collection[str]" = frozenset(),
    sleep: Callable[[float], None] = _time.sleep,
) -> "dict[str, Optional[TableOverlayFacts]]":
    """Each table's overlay facts over ONE connection; ``None`` = unknown.

    Every read is idempotent: a 40001 or an XX000 is retried on the same connection, and a
    transient connection loss reconnects (at most twice per call) and re-reads that table. A
    table whose reads still fail -- or any non-transient error, e.g. 42501 -- is unknown
    (best-effort rollback, then the next table); a connection that cannot be restored makes that
    table and every later one unknown, as a connect failure makes every table unknown. An
    absent table is ``exists=False`` with every column ``None``. ``probe_rows`` adds one
    ``SELECT 1 ... LIMIT 1`` per existing table; ``primary_key_tables`` adds one key read
    each. The connection is closed in a ``finally`` and never reused after an ALTER (P0 F6).
    """
    from psycopg import sql

    from dsql_migrator.core.occ import is_occ_conflict, with_occ_retry
    from dsql_migrator.core.target_connection import (
        is_server_unavailable,
        is_transient_connection_error,
    )

    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> every table unknown
        return {name: None for name in table_names}

    wanted_pk = set(primary_key_tables)

    def _read_table(conn: Any, name: str) -> TableOverlayFacts:
        exists, columns = read_overlay_column_facts(
            conn, name, list(columns_by_table.get(name, ()))
        )
        if not exists:
            return TableOverlayFacts(False, False, None, columns)
        has_rows: Optional[bool] = None
        if probe_rows:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql.SQL("SELECT 1 FROM {table} LIMIT 1").format(
                        table=_table_identifier(name)
                    )
                )
                has_rows = cursor.fetchone() is not None
            finally:
                _safe_close(cursor)
        primary_key: Optional[tuple[str, ...]] = None
        if name in wanted_pk:
            where, params = _relation_predicate(name)
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT a.attname FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN pg_catalog.pg_index ix ON ix.indrelid = c.oid "
                    "AND ix.indisprimary "
                    "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) "
                    "ON TRUE JOIN pg_catalog.pg_attribute a "
                    "ON a.attrelid = c.oid AND a.attnum = k.attnum "
                    f"WHERE {where} AND k.ord <= ix.indnkeyatts ORDER BY k.ord",
                    params,
                )
                primary_key = tuple(
                    str(row[0]) for row in cursor.fetchall() if row and row[0]
                )
            finally:
                _safe_close(cursor)
        return TableOverlayFacts(True, has_rows, primary_key, columns)

    def _rollback(conn: Any) -> None:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best effort
            pass

    def _attempt(conn: Any, name: str) -> TableOverlayFacts:
        try:
            return _read_table(conn, name)
        except Exception:
            _rollback(conn)
            raise

    retrying = with_occ_retry(
        max_attempts=_FACTS_OCC_ATTEMPTS,
        base_delay=0.05,
        sleep=sleep,
        retryable=lambda exc: is_occ_conflict(exc) or is_server_unavailable(exc),
    )(_attempt)
    result: dict[str, Optional[TableOverlayFacts]] = {}
    reconnects = 0
    try:
        for name in table_names:
            while True:
                if connection is None:  # lost for good: every later table is unknown
                    result[name] = None
                    break
                try:
                    result[name] = retrying(connection, name)
                    break
                except Exception as exc:  # noqa: BLE001 - classify, then this table unknown
                    if not is_transient_connection_error(exc):
                        result[name] = None
                        break
                    _safe_close(connection)
                    connection = None
                    if reconnects >= _FACTS_RECONNECTS:
                        continue
                    reconnects += 1
                    try:
                        connection = connection_factory()
                    except Exception:  # noqa: BLE001 - cannot reconnect
                        connection = None
    finally:
        if connection is not None:
            _safe_close(connection)
    return result


def target_foreign_keys(
    table_names: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> dict[str, Optional[list[str]]]:
    """Return each table's ENFORCED foreign-key constraint names, over ONE connection.

    Aurora DSQL enforces foreign keys, and an enforced FK is incompatible with CDC: the
    sink applies change records across several tasks with no parent-before-child
    ordering, so a child row can arrive first and be rejected with SQLSTATE 23503 --
    which the sink classifies as a poison row and dead-letters permanently (no retry).
    The CDC-start precondition uses this to detect FKs left on the target (e.g. by a
    preceding ``Full load only`` run, whose post-load pass applies and validates them)
    BEFORE any streaming begins.

    Mirrors :func:`target_primary_keys`: one reused connection (a DSQL connect mints an
    IAM token + full TLS handshake, so N+1 handshakes would stall the confirm dialog),
    and the SAME "``None`` == unknown / unreadable, never a guess" contract. That
    contract is load-bearing here and the detection must FAIL CLOSED: reporting an
    unreadable catalog as "no foreign keys" would open the gate on exactly the case the
    gate exists for, so a connection failure maps EVERY table to ``None`` and a
    per-table query error maps just that table to ``None`` (never to ``[]``).

    ``[]`` therefore means "read the catalog, this table genuinely has none".

    Accepts qualified (``schema.table``) and unqualified (``table``) names, because the
    tool spells target tables both ways -- a single-database MySQL source yields
    unqualified names, while a cluster-wide MySQL or any PostgreSQL source yields
    ``schema.table`` (see ``core/introspector.py``). A bare name is resolved across every
    USER schema, deliberately NOT via ``pg_table_is_visible`` (which the sibling readers
    use): that depends on the connection's search_path -- the default
    ``"$user", public`` for the tool's DSQL connections -- so a target table in any other
    schema read back as ``[]`` and this gate opened with foreign keys still enforced.
    Those siblings are unaffected because a miss maps them to ``None`` (unknown); only
    here did it become a positive "there are none". If a bare name is constrained in
    SEVERAL user schemas the answer is ``None`` (ambiguous), never a guess.
    """
    result: dict[str, Optional[list[str]]] = {}
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> every table UNKNOWN (never [])
        return {name: None for name in table_names}
    try:
        for table_name in table_names:
            parts = table_name.split(".", 1)
            if len(parts) == 2:
                schema, relname = parts
                where_relation = "n.nspname = %(schema)s AND c.relname = %(table)s"
                params: dict[str, object] = {"schema": schema, "table": relname}
            else:
                # Deliberately NOT pg_table_is_visible(): that depends on the
                # connection's search_path, which for the tool's DSQL connections is the
                # default '"$user", public'. A target table in any other schema (the
                # normal case for a schema-qualified conversion) then read back as [] ==
                # "genuinely none", so the CDC gate opened with enforced foreign keys
                # still in place -- fail OPEN, the exact opposite of its contract
                # (live-confirmed). Resolve the bare relname across every USER schema
                # instead, so the answer never depends on session state.
                where_relation = (
                    "c.relname = %(table)s AND n.nspname <> ALL(%(system)s)"
                )
                params = {"table": parts[0], "system": list(SYSTEM_SCHEMAS)}
            # contype='f' == foreign key. conrelid is the CHILD (referencing) table, so
            # this lists the constraints that would reject an out-of-order child row.
            # NB convalidated is deliberately NOT read: it is False right after
            # ADD ... NOT VALID and True after the async VALIDATE, so it says nothing
            # about who created the constraint -- name+table matching is the only safe
            # ownership test.
            # n.nspname is selected so an AMBIGUOUS bare name (same relname in several
            # user schemas) can be detected rather than silently merged.
            statement = (
                "SELECT n.nspname, con.conname "
                "FROM pg_catalog.pg_constraint con "
                "JOIN pg_catalog.pg_class c ON c.oid = con.conrelid "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE con.contype = 'f' AND {where_relation} "
                "ORDER BY n.nspname, con.conname"
            )
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute(statement, params)
                rows = [
                    (str(row[0]), str(row[1]))
                    for row in cursor.fetchall()
                    if row and row[1]
                ]
                schemas = {schema for schema, _name in rows}
                if len(schemas) > 1:
                    # The bare name matched constrained tables in more than one schema,
                    # so we cannot tell WHICH one is the migration target. Report unknown
                    # (blocking) rather than guessing or merging: over-reporting here
                    # costs a re-check, under-reporting costs silently dropped rows.
                    result[table_name] = None
                else:
                    result[table_name] = [name for _schema, name in rows]
            except Exception:  # noqa: BLE001 - unreadable catalog -> UNKNOWN, not "none"
                result[table_name] = None
            finally:
                if cursor is not None:
                    _safe_close(cursor)
    finally:
        _safe_close(connection)
    return result


def unique_index_state(
    table_name: str,
    columns: Sequence[str],
    *,
    connection_factory: Callable[[], Any],
) -> Optional[str]:
    """Return ``"valid"`` / ``"building"`` / ``"absent"`` for a unique index on ``columns``.

    Why this exists: an ``ADD CONSTRAINT ... FOREIGN KEY`` needs a UNIQUE index covering
    exactly the referenced columns, and DSQL raises the SAME error (SQLSTATE 42830,
    ``there is no unique constraint matching given keys for referenced table``) whether
    that index is MISSING or merely still BUILDING. The distinction decides what to do:

    * ``building`` -- the post-load ``CREATE INDEX ASYNC`` has not finished, so the ADD
      will succeed shortly. WAIT. (Live-verified on DSQL: right after
      ``CREATE UNIQUE INDEX ASYNC`` the index row exists with ``indisvalid = false`` and
      the ADD fails 42830; once ``indisvalid`` flips true the same ADD succeeds.)
    * ``absent`` -- no such index at all, so waiting is pointless. Fail fast with an
      actionable message.
    * ``valid`` -- ready; a 42830 then means something else is wrong.

    ``None`` means the catalog could not be read (unknown) -- callers must not treat that
    as ``absent``. Column ORDER need not match: a unique index on the same SET of key
    columns satisfies the foreign key.
    """
    wanted = [str(c) for c in columns]
    if not wanted:
        return None
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> unknown, never "absent"
        return None
    parts = table_name.split(".", 1)
    if len(parts) == 2:
        where_relation = "n.nspname = %(schema)s AND t.relname = %(table)s"
        params: dict[str, object] = {"schema": parts[0], "table": parts[1]}
    else:
        # Bare name: resolve across every USER schema rather than via the connection's
        # search_path (the default '"$user", public', which would miss the target).
        where_relation = "t.relname = %(table)s AND n.nspname <> ALL(%(system)s)"
        params = {"table": parts[0], "system": list(SYSTEM_SCHEMAS)}
    statement = (
        "SELECT i.relname, ix.indisvalid, "
        "  ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a "
        "        JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) "
        "          ON a.attnum = k.attnum "
        "        WHERE a.attrelid = t.oid AND k.ord <= ix.indnkeyatts "
        "        ORDER BY k.ord) AS key_columns "
        "FROM pg_catalog.pg_index ix "
        "JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid "
        "JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "
        # A partial unique index (Schema Conversion carries PostgreSQL's) is unique only
        # within its predicate, so it is no key for the stream.
        f"WHERE ix.indisunique AND ix.indpred IS NULL AND {where_relation}"
    )
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(statement, params)
        rows = cursor.fetchall()
    except Exception:  # noqa: BLE001 - unreadable catalog -> unknown
        return None
    finally:
        if cursor is not None:
            _safe_close(cursor)
        _safe_close(connection)
    building = False
    for row in rows or ():
        key_columns = [str(c) for c in (row[2] or ())]
        if set(key_columns) != set(wanted):
            continue
        if row[1]:
            return "valid"
        building = True
    return "building" if building else "absent"


def target_required_columns_without_default(
    table_name: str,
    *,
    connection_factory: Callable[[], Any],
) -> Optional[list[str]]:
    """Return the target columns that MUST be given a value on INSERT (read-only).

    Full Load builds its INSERT column list from the SOURCE table, so any column
    present only on the target is left out of every INSERT. That is fine when the
    column can take an absent value -- it is nullable, or has a DEFAULT (including a
    generated/identity column, which supplies its own value). It is NOT fine for a
    ``NOT NULL`` column with no default: the row has nothing to put there and the
    load fails with a not-null violation partway through (verified on a live DSQL
    cluster). This lists exactly those columns so the prerequisite check can compare
    them against the source and fail before the load starts rather than mid-run.

    A column is EXCLUDED (can take an absent value) when any of these holds:
    ``NOT attnotnull`` (nullable), ``atthasdef`` (an explicit ``DEFAULT``), or
    ``attidentity <> ''`` (a ``GENERATED``/identity column). The last is not
    redundant: verified on a live DSQL cluster, an identity column reports
    ``atthasdef = false`` -- DSQL records identity in ``attidentity``, not
    ``pg_attrdef`` -- so an ``atthasdef``-only test wrongly flags every identity
    column as value-required. ``attnum > 0`` skips system columns and
    ``NOT attisdropped`` skips dropped ones.

    ``table_name`` is ``schema.table`` (or bare, resolved against the search path)
    and both halves are passed as BOUND PARAMETERS -- never interpolated
    (Requirement 9.4). Returns ``None`` when the set cannot be determined -- the
    table does not exist, or any error -- which callers MUST treat as "unknown",
    distinct from ``[]`` ("the table exists and every extra column can take an
    absent value"). Existence is checked separately so a genuinely empty result on
    a real table is never conflated with a missing table.
    """
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
    # Two queries over one connection: first confirm the relation exists (so a
    # missing table is `None`, not a misleading empty list), then list the
    # value-required columns.
    exists_statement = (
        "SELECT 1 FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE {where_relation}"
    )
    statement = (
        "SELECT a.attname "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        f"WHERE {where_relation} "
        "AND a.attnum > 0 AND NOT a.attisdropped "
        "AND a.attnotnull AND NOT a.atthasdef AND a.attidentity = '' "
        "ORDER BY a.attnum"
    )
    try:
        connection = connection_factory()
    except Exception:  # noqa: BLE001 - cannot connect -> unknown
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(exists_statement, params)
        if cursor.fetchone() is None:
            return None  # relation not found -> unknown, not "no required columns"
        cursor.execute(statement, params)
        return [str(row[0]) for row in cursor.fetchall() if row and row[0]]
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
    lag) even if the row COUNTs differ (a mid-stream gap). A numeric(20,0) key -- a
    MySQL BIGINT UNSIGNED the conversion keeps whole -- reads back as a Decimal and is
    that integer. Returns ``None`` for a table whose PK is not a single integer
    column, is missing/empty, or errors. All reads run over one connection.
    """
    from psycopg import sql

    out: dict[str, Optional[int]] = {}
    connection = connection_factory()
    try:
        for name, pk in pk_by_table.items():
            if not pk:
                out[name] = None
                continue
            table_id = _table_identifier(name)
            col_id = sql.Identifier(pk)
            statement = sql.SQL("SELECT MAX({col}) FROM {table}").format(
                col=col_id, table=table_id
            )
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
                row = cursor.fetchone()
                val = row[0] if row is not None else None
                if isinstance(val, Decimal) and val == val.to_integral_value():
                    val = int(val)
                out[name] = int(val) if isinstance(val, int) else None
            except Exception:  # noqa: BLE001 - missing/non-integer/error -> unknown
                out[name] = None
            finally:
                _safe_close(cursor)
    finally:
        _safe_close(connection)
    return out


# The reason a table the conversion makes an identity has none on the target (applied as a
# plain integer -- with Keep source PK, or by a version that defaulted to it): reported as a
# FAILED sync so cut over says so instead of "Done -- nothing needed advancing".
NOT_AN_IDENTITY_REASON = (
    "the target key is not an identity column (it was applied as a plain integer, with "
    "Keep source PK or by an earlier version that defaulted to it), so nothing generates "
    "it: the application must supply ids, or re-apply the table with Replace and reload it"
)


def split_not_identity(failed: "Mapping[str, str]") -> "tuple[dict[str, str], list[str]]":
    """``(real failures, tables whose target key is no identity)``: the second is no failed
    RESTART -- there is no sequence to retry or advance -- and needs other advice."""
    real = {name: reason for name, reason in failed.items() if reason != NOT_AN_IDENTITY_REASON}
    plain = sorted(name for name, reason in failed.items() if reason == NOT_AN_IDENTITY_REASON)
    return real, plain


def not_identity_detail(tables: "Sequence[str]") -> str:
    """The activity-log detail for :func:`split_not_identity`'s second list."""
    return (
        f"{len(tables)} table(s) whose conversion makes the key an identity hold it as a "
        f"plain integer on the target ({', '.join(tables)}), so nothing generates it and an "
        "INSERT that omits the key fails after cut over. If the application supplies the "
        "ids, choose 'Keep source PK' for them in Schema Conversion; otherwise re-apply "
        "each table with Replace and reload it, so it gets the identity."
    )


def sync_identity_sequences(
    table_names: "Sequence[str]",
    *,
    connection_factory: Callable[[], Any],
    source_next_values: "Optional[Mapping[str, int]]" = None,
    expected_identity: "Collection[str]" = (),
    heartbeat: "Optional[Callable[[], None]]" = None,
) -> dict[str, "Union[int, str, None]"]:
    """Advance each target identity sequence past the loaded rows.

    ``source_next_values`` (table -> the next value the SOURCE would generate: MySQL's
    AUTO_INCREMENT counter, a PostgreSQL sequence's next value) is a floor: ids the source
    issued and then deleted stay issued, so a sequence restarted at the target's
    ``MAX + 1`` alone handed them out again after cut over (an empty table restarted at 1),
    and anything still holding such an id -- an audit row, an outside system -- silently
    pointed at the new row. ``expected_identity`` names the tables whose conversion makes
    the key an identity: one whose target key is NOT an identity is reported as a FAILED
    sync (:data:`NOT_AN_IDENTITY_REASON`) instead of a quiet no-op. ``heartbeat`` (a job's
    liveness reporter) is called once per table: the pass is the tail of a job, four
    statements and a DDL per table, and hundreds of tables were one silence the job
    manager's stall watchdog reaps.

    Per-table result value:
    - ``int`` -- the ``RESTART WITH`` value actually applied (the sequence advanced);
    - ``None`` -- nothing to do (no identity column / empty table);
    - ``str`` -- the sync **FAILED** for this table, carrying a short value-free reason.
      Either the identity-column catalog read errored (an unreadable table is NOT the
      same as "no identity column" -- treating it as the latter silently skips a table
      that may need advancing) or the ``RESTART WITH`` itself failed.

    The ``str`` case is why the value is not just ``Optional[int]``: a failed ALTER MUST
    be distinguishable from a no-op, or the caller paints a swallowed failure green.
    :func:`partition_identity_sync` splits the result into advanced / failed for callers.

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
    floors = dict(source_next_values or {})
    expected = set(expected_identity or ())
    # RECONNECT per table, and never let one table's connection loss discard the work
    # already recorded. This pass runs at the END of a load that can have taken hours, so
    # its connection is the most likely in the tool to be dead: a DSQL IAM token is
    # short-lived and a dropped backend used to make the whole function RAISE -- losing
    # every table after the failure AND the successes already in ``out``. Live-reproduced:
    # killing the backend after the first table left 5 of 6 tables at nextval=1 with
    # max(id)=11 and reported nothing, which is the silent post-cut-over duplicate-key
    # outage this function's own docstring calls the worst shape a migration failure can
    # take. Every other post-load pass already reconnects; this was the one that did not.
    connection = None
    try:
        for name in table_names:
            if heartbeat is not None:
                heartbeat()
            out[name] = None
            if connection is None:
                try:
                    connection = connection_factory()
                except Exception as exc:  # noqa: BLE001
                    # Cannot even connect: record it for THIS table and try again for the
                    # next one, so a transient outage does not blank the whole pass.
                    out[name] = (
                        f"{type(exc).__name__}: "
                        f"{(str(exc).strip().splitlines() or [''])[0]}"
                    )
                    continue
            parts = name.split(".", 1)
            if len(parts) == 2:
                schema_name, bare = parts
            else:
                schema_name, bare = None, parts[0]
            cursor = connection.cursor()
            try:
                # Identity columns only: ``is_identity`` is the catalog's own answer, so a
                # plain integer key (a Keep source PK table) is correctly skipped rather
                # than having a sequence invented for it. Bound parameters throughout.
                # The catalog holds a name cut to 63 bytes (a 64-character MySQL name
                # migrates shortened), so it is matched that way; the statements below
                # name the full form, which PostgreSQL cuts itself.
                if schema_name is not None:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "AND is_identity = 'YES'",
                        (pg_stored_name(schema_name), pg_stored_name(bare)),
                    )
                else:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = %s AND is_identity = 'YES' "
                        "AND table_schema = ANY (current_schemas(false))",
                        (pg_stored_name(bare),),
                    )
                rows = cursor.fetchall() or []
            except Exception as exc:  # noqa: BLE001 - unreadable catalog is a FAILURE
                _safe_close(cursor)
                # A catalog read that ERRORED is not "no identity column" (a silent
                # skip): surface it as a FAILED entry (value-free reason, Property 7)
                # so the caller never treats an unreadable table as safely synced
                # (audit finding D2). Distinct from the None no-op below.
                _head = (str(exc).strip().splitlines() or [""])[0].strip()
                out[name] = (
                    f"{type(exc).__name__}: {_head}" if _head else type(exc).__name__
                )
                # Drop the connection here too: the catalog read is the FIRST statement per
                # table, so it is where a dead backend / expired token surfaces, and keeping
                # the corpse made every remaining table fail for the same reason.
                _safe_close(connection)
                connection = None
                continue
            _safe_close(cursor)
            if not rows:
                if name in expected:
                    out[name] = NOT_AN_IDENTITY_REASON
                continue  # no identity column: nothing to sync
            column = str(rows[0][0])

            # Split the SAME way the catalog read above did (``split(".", 1)`` -> parts),
            # not on every dot: a table whose bare name CONTAINS a dot produced a 3-part
            # identifier here while the catalog read produced the right 2-part one, so the
            # MAX was sent to a relation that does not exist -- and the bare `continue`
            # below then buried the failure.
            table_id = sql.Identifier(*([schema_name, bare] if schema_name else [bare]))
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
            except Exception as exc:  # noqa: BLE001
                # Record the FAILURE, never a silent no-op. This function's own docstring
                # states the invariant -- "a failed ALTER MUST be distinguishable from a
                # no-op, or the caller paints a swallowed failure green" -- and the catalog
                # read and the ALTER both honour it; only this step did not. ``None`` is
                # classified as neither advanced nor failed, so the table was OMITTED from
                # the activity log while the same entry told the operator that "the
                # application's first insert after cut-over cannot collide". Any cause lands
                # here: an expired DSQL IAM token, an OCC 40001, a reset connection. The
                # result is the worst shape a migration failure can take -- counts and
                # checksums MATCH, Validation passes, and it only surfaces after cut-over,
                # with the source already frozen.
                _safe_close(cursor)
                out[name] = f"{type(exc).__name__}: {str(exc).strip().splitlines()[0]}"
                # The connection may be the casualty (a dead backend, an expired token), so
                # discard it: the next table reconnects instead of inheriting the corpse.
                _safe_close(connection)
                connection = None
                continue
            _safe_close(cursor)
            floor = floors.get(name)
            if not isinstance(current_max, int):
                if not isinstance(floor, int) or floor <= 1:
                    # Empty table (or a non-integer identity) with no source counter past
                    # its start: the sequence's own start is already correct.
                    continue
                restart_at = floor
            else:
                restart_at = max(current_max + 1, floor if isinstance(floor, int) else 0)
            # NEVER BACKWARDS. A sync can run again after cut over (the runbook says it is
            # safe to repeat), when the application has drawn ids the table no longer holds
            # (an outbox emptied by its consumer), or without the source counter (a restart
            # dropped the password, the source is stopped): restarting at MAX + 1 or at the
            # floor then reissued ids already handed out -- below a restart this tool made
            # itself. The sequence's own position is the bound; unreadable, the sync does
            # what it always did.
            position = _identity_next_value(connection, schema_name, bare, column)
            if isinstance(position, int) and restart_at <= position:
                continue  # already at or past it: nothing to advance
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
            except Exception as exc:  # noqa: BLE001
                # Record the FAILURE as a string, distinct from a None no-op, so the
                # caller can surface it instead of reporting the sync as clean. The
                # RESTART WITH is a DDL under DSQL OCC (can raise 40001 /
                # concurrent-DDL) and the IAM token can expire mid-session -- a
                # swallowed one is a silent post-cut-over duplicate-key outage. The
                # reason is first-line-only + type name (no row values; Property 7).
                first = str(exc).strip().splitlines()
                head = first[0].strip() if first else ""
                out[name] = f"{type(exc).__name__}: {head}" if head else type(exc).__name__
                _safe_close(connection)
                connection = None
            finally:
                _safe_close(cursor)
    finally:
        if connection is not None:
            _safe_close(connection)
    return out


def _identity_next_value(
    connection: Any, schema_name: "Optional[str]", bare: str, column: str
) -> Optional[int]:
    """The value the identity sequence of ``column`` hands out next (``last_value`` +1
    once drawn from; with CACHE it counts every block a session reserved), or ``None``
    when it cannot be read. Read-only; a failed read leaves no aborted transaction behind
    (the sync's connection is in autocommit, as every target connection is)."""
    from psycopg import sql

    qualified = ".".join(
        '"' + part.replace('"', '""') + '"' for part in ((schema_name, bare) if schema_name else (bare,))
    )
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (qualified, column))
        row = cursor.fetchone()
        sequence = row[0] if row is not None else None
        if not sequence:
            return None
        cursor.execute(
            sql.SQL("SELECT last_value, is_called FROM {}").format(
                sql.Identifier(*_split_regclass(str(sequence)))
            )
        )
        row = cursor.fetchone()
    except Exception:  # noqa: BLE001 - unknown position: the caller restarts as before
        return None
    finally:
        _safe_close(cursor)
    if row is None or len(row) < 2 or not isinstance(row[0], int):
        return None
    return row[0] + 1 if row[1] else row[0]


def _split_regclass(text: str) -> "list[str]":
    """The parts of a regclass's text form (``app."Odd.Name_seq"``), quotes removed."""
    return [
        quoted.replace('""', '"') if quoted else bare
        for quoted, bare in re.findall(r'"((?:[^"]|"")*)"|([^."]+)', text)
    ]


def partition_identity_sync(
    result: "Mapping[str, Union[int, str, None]]",
) -> "tuple[dict[str, int], dict[str, str]]":
    """Split a :func:`sync_identity_sequences` result into ``(advanced, failed)``.

    ``advanced`` maps table -> the ``int`` RESTART value that was applied; ``failed``
    maps table -> the ``str`` failure reason. ``None`` no-op entries (no identity
    column / empty / unreadable) belong to neither and are dropped. This is the one
    place callers should classify the result, so a failed ALTER can never be mistaken
    for "nothing to advance".
    """
    advanced: dict[str, int] = {}
    failed: dict[str, str] = {}
    for name, value in result.items():
        if isinstance(value, bool):  # guard: bool is an int subclass, never a value here
            continue
        if isinstance(value, int):
            advanced[name] = value
        elif isinstance(value, str):
            failed[name] = value
    return advanced, failed


__all__ = [
    "ColumnNullFacts",
    "TableOverlayFacts",
    "pg_stored_name",
    "read_overlay_column_facts",
    "target_overlay_facts",
    "TargetIntrospector",
    "ConnectorFactory",
    "build_inventory",
    "count_target_rows",
    "max_pk_target",
    "sync_identity_sequences",
    "partition_identity_sync",
    "TargetNames",
    "relations_blocked_on_target",
    "relations_existing_on_target",
    "tables_with_rows",
    "target_primary_key_columns",
    "target_primary_keys",
    "target_unique_keys",
    "target_foreign_keys",
    "unique_index_state",
    "SYSTEM_SCHEMAS",
    "RELATIONS_QUERY",
    "COLUMNS_QUERY",
    "INDEXES_QUERY",
]
