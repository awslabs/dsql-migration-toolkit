# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only source introspection for MySQL (RDS/Aurora).

The :class:`SourceIntrospector` connects to a source MySQL database using
SQLAlchemy + PyMySQL and extracts schema/object metadata into a
:class:`~dsql_migrator.core.models.SourceInventory`. It implements two
guarantees from the design:

- Read-only source (Property 1 / Requirement 1.5): a SQLAlchemy guard rejects
  any write or DDL statement before it reaches the database, so introspection
  and connection checks can never mutate the source.
- Credential confidentiality (Property 7 / Requirement 1.4 / 9.2): passwords are
  resolved from a :class:`~dsql_migrator.config.SecretRef` only at connect time,
  are never stored on this class, and are redacted from any failure message.

The MySQL-specific enrichment (triggers, routines, AUTO_INCREMENT, collation)
runs against ``information_schema`` only when connected to a MySQL dialect; the
table/column/PK/index/FK structure is collected via dialect-agnostic SQLAlchemy
reflection.
"""

from __future__ import annotations

from typing import Callable, Optional, Protocol

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from dsql_migrator.config import SecretValue, resolve_secret
from dsql_migrator.core.models import (
    ColumnDef,
    ConnectionResult,
    ForeignKeyDef,
    IndexDef,
    ObjectRef,
    ObjectType,
    SourceConnectionConfig,
    SourceInventory,
    TableDef,
    ViewDef,
)

MYSQL_DRIVER = "mysql+pymysql"

# Bounded TCP connect timeout (seconds) for the source MySQL engine. Without it,
# an unreachable host makes the driver's connect block for the OS default (tens
# of seconds to minutes), which hangs introspection/validation and makes the UI
# spin with no way to interrupt the blocked connect. A few seconds fails fast and
# surfaces a clear error instead. PyMySQL's connect kwarg is ``connect_timeout``.
SOURCE_CONNECT_TIMEOUT_SECONDS = 10

# Bounded per-socket read/write timeout (seconds) for a Full Load source stream.
# ``connect_timeout`` only bounds the initial TCP connect; once connected, a
# server/network stall mid-read (e.g. a large table's keyset page that never
# returns) leaves PyMySQL blocked in ``recv`` forever, so the table never
# fails OR completes and the job hangs in RUNNING. A read/write timeout makes a
# stalled read raise instead, so the table is marked FAILED and becomes
# retryable. It is NOT applied to the shared default (introspection/validation),
# whose legitimately long single queries -- exact ``COUNT(*)`` / checksums over a
# whole table -- must not be killed by a per-socket read timeout; it is opt-in
# for the Full Load keyset stream, which returns a bounded page well within it.
SOURCE_READ_TIMEOUT_SECONDS = 300


def source_engine_kwargs(
    *, read_timeout_seconds: Optional[int] = None
) -> dict[str, object]:
    """Return the ``create_engine`` kwargs shared by every source MySQL engine.

    Centralizes the engine settings so the connection test, introspection, and
    validation all build the source engine identically: ``pool_pre_ping`` plus a
    bounded ``connect_timeout`` (so an unreachable host fails fast instead of
    hanging). Exposed (and pure) so it can be asserted in a unit test without
    opening a connection.

    ``read_timeout_seconds`` (opt-in) additionally bounds each socket read/write
    so a connected-but-stalled stream raises instead of blocking forever. It is
    used only by the Full Load source stream (whose reads return a bounded keyset
    page); the default ``None`` keeps introspection/validation free to run a
    single long query (exact counts/checksums) without a per-read deadline.
    """
    connect_args: dict[str, object] = {
        "connect_timeout": SOURCE_CONNECT_TIMEOUT_SECONDS,
        # Pin the session to UTC. MySQL TIMESTAMP is stored in UTC but read/rendered
        # in the session's time_zone, so a non-UTC server/client default would make
        # TIMESTAMP columns drift versus the target's UTC rendering -- both in the
        # Full Load loader's reads and the validation checksum (which renders
        # TIMESTAMP via DATE_FORMAT while the PG side uses AT TIME ZONE 'UTC').
        # DATETIME is a wall-clock and unaffected; this makes TIMESTAMP deterministic
        # regardless of the server/client zone. Applied to every source engine (test,
        # introspection, validation, Full Load stream) since they share these kwargs.
        "init_command": "SET time_zone = '+00:00'",
    }
    if read_timeout_seconds is not None:
        # PyMySQL applies these per socket operation, not to the whole query.
        connect_args["read_timeout"] = read_timeout_seconds
        connect_args["write_timeout"] = read_timeout_seconds
    return {
        "pool_pre_ping": True,
        "connect_args": connect_args,
    }


# MySQL server schemas that are never part of a user's migratable inventory.
# When no specific database is selected, introspection assesses every other
# (user) schema on the cluster.
MYSQL_SYSTEM_SCHEMAS = frozenset(
    {"information_schema", "mysql", "performance_schema", "sys"}
)

# Leading keywords that indicate a write (DML), DDL, DCL, or locking statement.
# Any statement starting with one of these is refused on the read-only source.
_WRITE_OR_DDL_KEYWORDS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "MERGE",
        "LOAD",
        "IMPORT",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "RENAME",
        "GRANT",
        "REVOKE",
        "CALL",
        "DO",
        "LOCK",
        "UNLOCK",
    }
)


class ReadOnlySourceError(RuntimeError):
    """Raised when a write or DDL statement is attempted on the read-only source."""


class _Connection(Protocol):
    """Minimal connection contract used by the enrichment helpers.

    Only ``execute`` is required, which keeps the enrichment functions easy to
    unit test with a lightweight fake connection.
    """

    def execute(self, statement: object, parameters: object = ...) -> object: ...


def _strip_leading_noise(sql: str) -> str:
    """Remove leading whitespace, comments, and opening parentheses from ``sql``."""
    text_value = sql.lstrip()
    changed = True
    while changed and text_value:
        changed = False
        if text_value.startswith("/*"):
            end = text_value.find("*/")
            text_value = "" if end == -1 else text_value[end + 2 :].lstrip()
            changed = True
        elif text_value.startswith("--") or text_value.startswith("#"):
            newline = text_value.find("\n")
            text_value = "" if newline == -1 else text_value[newline + 1 :].lstrip()
            changed = True
        elif text_value.startswith("("):
            text_value = text_value[1:].lstrip()
            changed = True
    return text_value


def _first_keyword(sql: str) -> str:
    """Return the upper-cased first keyword of a SQL statement, or ``""``."""
    cleaned = _strip_leading_noise(sql)
    if not cleaned:
        return ""
    token = cleaned.split(None, 1)[0]
    return token.strip("(`\"';").upper()


def is_write_or_ddl(sql: str) -> bool:
    """Return ``True`` if ``sql`` is a write, DDL, DCL, or locking statement.

    Read statements (``SELECT``, ``SHOW``, ``DESCRIBE``, ``EXPLAIN``, ``SET``,
    transaction control, ``PRAGMA``, etc.) return ``False``.
    """
    return _first_keyword(sql) in _WRITE_OR_DDL_KEYWORDS


def install_read_only_guard(engine: Engine) -> None:
    """Attach a guard that refuses any write/DDL statement on ``engine``."""

    @event.listens_for(engine, "before_cursor_execute")
    def _guard(  # noqa: ANN001 - SQLAlchemy event signature
        conn, cursor, statement, parameters, context, executemany
    ) -> None:
        if is_write_or_ddl(statement):
            raise ReadOnlySourceError(
                f"refused non-read statement on read-only source: "
                f"{_first_keyword(statement)}"
            )


def _default_engine_factory(conn: SourceConnectionConfig) -> Engine:
    """Build a read-only-guarded MySQL engine from a connection config."""
    password: Optional[str] = None
    if conn.secret is not None:
        password = resolve_secret(conn.secret).reveal()
    url = URL.create(
        MYSQL_DRIVER,
        username=conn.username,
        password=password,
        host=conn.host,
        port=conn.port,
        database=conn.database,
    )
    engine = create_engine(url, **source_engine_kwargs())
    install_read_only_guard(engine)
    return engine


def _revealed_secret(conn: SourceConnectionConfig) -> Optional[str]:
    """Return the plaintext password for redaction, or ``None`` if unavailable."""
    if conn.secret is None:
        return None
    try:
        value = resolve_secret(conn.secret)
    except Exception:
        return None
    return value.reveal() if isinstance(value, SecretValue) else None


def _sanitize_message(message: str, secret: Optional[str]) -> str:
    """Redact a plaintext secret from an error message (Property 7)."""
    if secret:
        message = message.replace(secret, "***")
    return message


def _default_to_str(default: object) -> Optional[str]:
    """Normalize a reflected column default into a string or ``None``."""
    if default is None:
        return None
    return str(default)


def _reflect_tables(inspector: object, schema: Optional[str] = None) -> list[TableDef]:
    """Collect table/column/PK/index/FK structure via SQLAlchemy reflection.

    ``schema`` selects the database/schema to reflect; ``None`` uses the
    connection's default schema (single-database mode). Names are returned
    unqualified; the caller qualifies them with the schema in cluster-wide mode.
    """
    tables: list[TableDef] = []
    for table_name in inspector.get_table_names(schema=schema):  # type: ignore[attr-defined]
        columns = [
            ColumnDef(
                name=column["name"],
                mysql_type=str(column["type"]),
                nullable=bool(column.get("nullable", True)),
                default=_default_to_str(column.get("default")),
                collation=None,
            )
            for column in inspector.get_columns(table_name, schema=schema)  # type: ignore[attr-defined]
        ]

        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)  # type: ignore[attr-defined]
        primary_key = list(pk_constraint.get("constrained_columns") or [])

        indexes: list[IndexDef] = []
        for index in inspector.get_indexes(table_name, schema=schema):  # type: ignore[attr-defined]
            index_columns = [c for c in index.get("column_names", []) if c]
            index_name = index.get("name")
            if index_name and index_columns:
                indexes.append(
                    IndexDef(
                        name=index_name,
                        columns=index_columns,
                        unique=bool(index.get("unique")),
                    )
                )

        foreign_keys: list[ForeignKeyDef] = []
        for fk in inspector.get_foreign_keys(table_name, schema=schema):  # type: ignore[attr-defined]
            constrained = list(fk.get("constrained_columns") or [])
            referred_table = fk.get("referred_table")
            referred_columns = list(fk.get("referred_columns") or [])
            if not (constrained and referred_table and referred_columns):
                continue
            fk_name = fk.get("name") or f"{table_name}_{'_'.join(constrained)}_fkey"
            # Qualify the referenced table the same way the caller qualifies table
            # names in cluster-wide mode (``schema.table``): use the FK's own
            # referred_schema for a cross-schema FK, else the schema being
            # reflected (a same-schema FK). In single-database mode (``schema`` is
            # None) names stay unqualified, matching the child table name -- so a
            # downstream orphan-check/DDL query resolves the parent correctly
            # instead of hitting the search_path (or a wrong same-named table).
            referred_schema = fk.get("referred_schema") or schema
            referenced_table = (
                f"{referred_schema}.{referred_table}"
                if referred_schema
                else referred_table
            )
            foreign_keys.append(
                ForeignKeyDef(
                    name=fk_name,
                    columns=constrained,
                    referenced_table=referenced_table,
                    referenced_columns=referred_columns,
                )
            )

        tables.append(
            TableDef(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                indexes=indexes,
                foreign_keys=foreign_keys,
                auto_increment_column=None,
            )
        )
    return tables


def _reflect_views(inspector: object, schema: Optional[str] = None) -> list[ViewDef]:
    """Collect view names and definitions via SQLAlchemy reflection."""
    views: list[ViewDef] = []
    for view_name in inspector.get_view_names(schema=schema):  # type: ignore[attr-defined]
        try:
            definition = inspector.get_view_definition(view_name, schema=schema) or ""  # type: ignore[attr-defined]
        except NotImplementedError:
            definition = ""
        views.append(ViewDef(name=view_name, definition=definition))
    return views


def collect_triggers(connection: _Connection, database: str) -> list[ObjectRef]:
    """Read trigger names from ``information_schema`` (MySQL, read-only)."""
    rows = connection.execute(
        text(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA = :db ORDER BY TRIGGER_NAME"
        ),
        {"db": database},
    )
    return [ObjectRef(name=row[0], object_type=ObjectType.TRIGGER) for row in rows]


def collect_routines(connection: _Connection, database: str) -> list[ObjectRef]:
    """Read stored procedure/function names from ``information_schema``.

    Distinguishes ``PROCEDURE`` and ``FUNCTION`` via ``ROUTINE_TYPE`` so the
    assessment can categorize them separately (an unknown type falls back to the
    generic ``ROUTINE``).
    """
    rows = connection.execute(
        text(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA = :db ORDER BY ROUTINE_NAME"
        ),
        {"db": database},
    )
    type_by_value = {
        "PROCEDURE": ObjectType.PROCEDURE,
        "FUNCTION": ObjectType.FUNCTION,
    }
    return [
        ObjectRef(
            name=row[0],
            object_type=type_by_value.get(
                str(row[1]).upper() if row[1] is not None else "", ObjectType.ROUTINE
            ),
        )
        for row in rows
    ]


def collect_events(connection: _Connection, database: str) -> list[ObjectRef]:
    """Read scheduled EVENT names from ``information_schema`` (MySQL, read-only).

    Aurora DSQL has no event scheduler, so events have no migration target and
    are surfaced for reimplementation (e.g. EventBridge Scheduler + Lambda).
    """
    rows = connection.execute(
        text(
            "SELECT EVENT_NAME FROM information_schema.EVENTS "
            "WHERE EVENT_SCHEMA = :db ORDER BY EVENT_NAME"
        ),
        {"db": database},
    )
    return [ObjectRef(name=row[0], object_type=ObjectType.EVENT) for row in rows]


def enrich_columns(
    connection: _Connection, database: str, tables: list[TableDef]
) -> None:
    """Enrich tables in place with collation and AUTO_INCREMENT metadata.

    Uses ``information_schema.COLUMNS`` (MySQL, read-only) to fill each column's
    ``collation``, mark generated (``VIRTUAL/STORED GENERATED``) and
    ``ON UPDATE CURRENT_TIMESTAMP`` columns from ``EXTRA``, and set the table's
    ``auto_increment_column``.
    """
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = :db"
        ),
        {"db": database},
    )

    collation_by_column: dict[tuple[str, str], Optional[str]] = {}
    extra_by_column: dict[tuple[str, str], str] = {}
    column_type_by_column: dict[tuple[str, str], str] = {}
    auto_increment_by_table: dict[str, str] = {}
    for table_name, column_name, collation_name, extra, column_type in rows:
        collation_by_column[(table_name, column_name)] = collation_name
        extra_text = str(extra).lower() if extra else ""
        extra_by_column[(table_name, column_name)] = extra_text
        if column_type:
            column_type_by_column[(table_name, column_name)] = str(column_type)
        if "auto_increment" in extra_text:
            auto_increment_by_table[table_name] = column_name

    for table in tables:
        for column in table.columns:
            # Prefer MySQL's COLUMN_TYPE over SQLAlchemy's str(type): the latter is
            # LOSSY -- it drops the ``unsigned`` flag and the display width, so an
            # ``int unsigned`` reflects as ``INTEGER`` and ``tinyint(1)`` as
            # ``TINYINT``. That under-sizes the target type (an unsigned column
            # overflows the signed mapping: "smallint out of range") and loses the
            # TINYINT(1)->boolean convention. COLUMN_TYPE preserves both.
            column_type = column_type_by_column.get((table.name, column.name))
            if column_type:
                column.mysql_type = column_type
            collation = collation_by_column.get((table.name, column.name))
            if collation is not None:
                column.collation = collation
            extra_text = extra_by_column.get((table.name, column.name), "")
            # "VIRTUAL GENERATED"/"STORED GENERATED" mark a computed column;
            # "DEFAULT_GENERATED" (expression default) is intentionally excluded.
            if "virtual generated" in extra_text or "stored generated" in extra_text:
                column.generated = True
            if "on update" in extra_text:
                column.auto_update_timestamp = True
        if table.name in auto_increment_by_table:
            table.auto_increment_column = auto_increment_by_table[table.name]


def enrich_index_types(
    connection: _Connection, database: str, tables: list[TableDef]
) -> None:
    """Enrich each index in place with its MySQL ``INDEX_TYPE`` (read-only).

    Uses ``information_schema.STATISTICS`` to record an index's type (e.g.
    ``BTREE``, ``FULLTEXT``, ``SPATIAL``) so the assessor can flag index types
    that Aurora DSQL does not support. ``STATISTICS`` has one row per indexed
    column; the index type is constant per index, so the last value wins.
    """
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, INDEX_TYPE "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = :db"
        ),
        {"db": database},
    )
    type_by_index: dict[tuple[str, str], Optional[str]] = {}
    for table_name, index_name, index_type in rows:
        type_by_index[(table_name, index_name)] = index_type

    for table in tables:
        for index in table.indexes:
            index_type = type_by_index.get((table.name, index.name))
            if index_type is not None:
                index.index_type = str(index_type)


def enrich_partitions(
    connection: _Connection, database: str, tables: list[TableDef]
) -> None:
    """Mark tables that use MySQL native partitioning (read-only).

    Uses ``information_schema.PARTITIONS`` (MySQL, read-only): a table is
    partitioned when it has at least one row with a non-null ``PARTITION_NAME``.
    """
    rows = connection.execute(
        text(
            "SELECT DISTINCT TABLE_NAME FROM information_schema.PARTITIONS "
            "WHERE TABLE_SCHEMA = :db AND PARTITION_NAME IS NOT NULL"
        ),
        {"db": database},
    )
    partitioned_tables = {row[0] for row in rows}
    for table in tables:
        if table.name in partitioned_tables:
            table.partitioned = True


def _user_schemas(inspector: object) -> list[str]:
    """Return non-system schema names on the cluster, in catalog order."""
    names = inspector.get_schema_names()  # type: ignore[attr-defined]
    return [name for name in names if name not in MYSQL_SYSTEM_SCHEMAS]


def _qualify(schema: str, *object_lists: list) -> None:
    """Prefix each object's ``name`` with ``schema.`` in place (cluster mode)."""
    for objects in object_lists:
        for obj in objects:
            obj.name = f"{schema}.{obj.name}"


def _assemble_inventory(
    inspector: object,
    connection: _Connection,
    database: Optional[str],
    *,
    is_mysql: bool,
) -> SourceInventory:
    """Assemble a :class:`SourceInventory` from one or all schemas.

    When ``database`` is set, a single schema is reflected with unqualified
    names (single-database mode). When it is empty/``None``, every non-system
    schema is reflected and names are qualified ``schema.object`` (cluster-wide
    mode). On MySQL, each reflected schema is enriched via ``information_schema``.
    """
    if database:
        # Single-database mode: reflect the connection's default schema and keep
        # names unqualified. ``enrich_db`` is the selected database.
        plans: list[tuple[Optional[str], str, bool]] = [(None, database, False)]
    else:
        # Cluster-wide mode: reflect every user schema and qualify names.
        plans = [(schema, schema, True) for schema in _user_schemas(inspector)]

    all_tables: list[TableDef] = []
    all_views: list[ViewDef] = []
    all_triggers: list[ObjectRef] = []
    all_routines: list[ObjectRef] = []
    all_events: list[ObjectRef] = []

    for reflect_schema, enrich_db, qualify in plans:
        tables = _reflect_tables(inspector, schema=reflect_schema)
        views = _reflect_views(inspector, schema=reflect_schema)
        triggers: list[ObjectRef] = []
        routines: list[ObjectRef] = []
        events: list[ObjectRef] = []

        if is_mysql:
            enrich_columns(connection, enrich_db, tables)
            enrich_index_types(connection, enrich_db, tables)
            enrich_partitions(connection, enrich_db, tables)
            triggers = collect_triggers(connection, enrich_db)
            routines = collect_routines(connection, enrich_db)
            events = collect_events(connection, enrich_db)

        if qualify:
            _qualify(enrich_db, tables, views, triggers, routines, events)

        all_tables.extend(tables)
        all_views.extend(views)
        all_triggers.extend(triggers)
        all_routines.extend(routines)
        all_events.extend(events)

    return SourceInventory(
        tables=all_tables,
        views=all_views,
        triggers=all_triggers,
        routines=all_routines,
        events=all_events,
    )


class SourceIntrospector:
    """Connects to a source MySQL database and extracts its inventory (read-only)."""

    def __init__(
        self,
        engine_factory: Optional[Callable[[SourceConnectionConfig], Engine]] = None,
    ) -> None:
        """Create an introspector.

        ``engine_factory`` builds a read-only-guarded SQLAlchemy engine for a
        connection config. The default targets MySQL via PyMySQL; tests may
        inject a factory to supply an alternative engine.
        """
        self._engine_factory = engine_factory or _default_engine_factory

    def test_connection(self, conn: SourceConnectionConfig) -> ConnectionResult:
        """Validate connectivity and return a success/failure result.

        Implements Requirement 1.1. On failure the reason is returned with any
        plaintext credential redacted (Requirement 1.4 / 9.2).
        """
        engine: Optional[Engine] = None
        try:
            engine = self._engine_factory(conn)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                # Read the server version read-only so the UI can show the source
                # engine (e.g. Aurora MySQL version) on the overview diagram. A
                # failure here must not fail the connection test, so it is best
                # effort.
                version: Optional[str] = None
                try:
                    row = connection.execute(text("SELECT VERSION()")).first()
                    version = str(row[0]) if row and row[0] is not None else None
                except Exception:  # noqa: BLE001 - version is optional metadata
                    version = None
                # The community MySQL engine version behind an Aurora build is not
                # in VERSION() (which carries only major.minor before the Aurora
                # tag); @@innodb_version usually exposes the full patch (e.g.
                # 8.0.42). Best effort, optional.
                mysql_version: Optional[str] = None
                try:
                    row = connection.execute(
                        text("SELECT @@innodb_version")
                    ).first()
                    mysql_version = (
                        str(row[0]) if row and row[0] is not None else None
                    )
                except Exception:  # noqa: BLE001 - optional metadata
                    mysql_version = None
                # Aurora MySQL exposes its engine version (e.g. 3.07.1) via
                # @@aurora_version even when VERSION() reports only the
                # MySQL-compatible patch. Present only on Aurora; best effort.
                aurora_version: Optional[str] = None
                try:
                    row = connection.execute(
                        text("SELECT @@aurora_version")
                    ).first()
                    aurora_version = (
                        str(row[0]) if row and row[0] is not None else None
                    )
                except Exception:  # noqa: BLE001 - non-Aurora has no such var
                    aurora_version = None
            return ConnectionResult(
                success=True,
                detail="Connection successful.",
                server_version=version,
                mysql_version=mysql_version,
                aurora_version=aurora_version,
            )
        except Exception as exc:  # noqa: BLE001 - surfaced as a failure reason
            message = _sanitize_message(str(exc), _revealed_secret(conn))
            reason = message or exc.__class__.__name__
            return ConnectionResult(
                success=False,
                detail=f"Connection failed: {reason}",
            )
        finally:
            if engine is not None:
                engine.dispose()

    def introspect(self, conn: SourceConnectionConfig) -> SourceInventory:
        """Extract the source inventory (Requirements 1.2, 1.3).

        When ``conn.database`` is set, only that database/schema is reflected and
        object names are unqualified. When it is empty/``None``, the entire
        cluster is assessed: every non-system schema is reflected and each object
        name is qualified as ``schema.object`` so names stay unique across
        databases. Tables/columns/PK/indexes/FK are collected via SQLAlchemy
        reflection; on MySQL, ``information_schema`` is queried per schema to
        enrich triggers, routines, AUTO_INCREMENT, and collation. All access is
        read-only (Property 1).
        """
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                inspector = inspect(connection)
                is_mysql = connection.dialect.name == "mysql"
                return _assemble_inventory(
                    inspector, connection, conn.database, is_mysql=is_mysql
                )
        finally:
            engine.dispose()


__all__ = [
    "SourceIntrospector",
    "ReadOnlySourceError",
    "is_write_or_ddl",
    "install_read_only_guard",
    "collect_triggers",
    "collect_routines",
    "collect_events",
    "enrich_columns",
    "enrich_index_types",
    "enrich_partitions",
]
