# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only source introspection for MySQL and PostgreSQL (RDS/Aurora).

The :class:`SourceIntrospector` connects to a source database (MySQL or
PostgreSQL) via SQLAlchemy and extracts schema/object metadata into a
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

from typing import TYPE_CHECKING, Callable, Optional, Protocol

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from dsql_migrator.config import SecretValue, resolve_secret
from dsql_migrator.core.models import (
    CheckConstraintDef,
    ColumnDef,
    ConnectionResult,
    ForeignKeyDef,
    IndexDef,
    ObjectRef,
    ObjectType,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)

if TYPE_CHECKING:
    from dsql_migrator.core.source_dialect import SourceDialect

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

# MySQL client/server error codes that mean "this connection is gone", not "your
# query was wrong". An Aurora MySQL failover (writer promotion during a patch,
# instance replacement, or an AZ event) closes every open connection, so a Full
# Load reading a large table mid-stream sees one of these:
#   2013 CR_SERVER_LOST            -- lost connection during the query
#   2006 CR_SERVER_GONE_ERROR      -- server closed the connection before the query
#   2003 CR_CONN_HOST_ERROR        -- can't connect (endpoint still re-pointing)
#   2002 CR_CONNECTION_ERROR       -- socket-level connect failure
#   2055 CR_SERVER_LOST_EXTENDED   -- lost connection, with a system error detail
#   1053 ER_SERVER_SHUTDOWN        -- server shutting down (promotion in progress)
#   1077/1079                      -- normal/aborted shutdown in progress
#   1927 ER_CONNECTION_KILLED      -- the connection was killed (failover fencing)
# These are all recoverable by RE-READING the table from a fresh connection.
#   1040 ER_CON_COUNT_ERROR       -- too many connections (see below)
#   1203 ER_TOO_MANY_USER_CONNECTIONS
# 1040/1203 are included because they are also SELF-INFLICTED and self-clearing: a
# failover makes every reader reconnect at once, and a high table x shard fan-out can
# briefly exceed the source's max_connections. Backing off and re-reading is exactly
# the right response -- the connections drain as other readers finish.
MYSQL_TRANSIENT_ERROR_CODES = frozenset(
    {2013, 2006, 2003, 2002, 2055, 1053, 1077, 1079, 1927, 1040, 1203}
)

# Lowercase message substrings for the same conditions, as a fallback when the
# numeric code was lost (SQLAlchemy/PyMySQL wrapping, or a socket timeout raised as
# a plain OSError). Deliberately NOT generic words like "error" -- each of these is
# specific to a connection that died, never to a bad query or a data problem.
MYSQL_TRANSIENT_SIGNATURES = (
    "lost connection",
    "server has gone away",
    "server closed the connection",
    "connection was killed",
    "can't connect to mysql server",
    "broken pipe",
    "connection reset",
    "connection aborted",
    "server shutdown in progress",
    "too many connections",
    "read timed out",
    "timed out",
)


def _mysql_source_transient(exc: BaseException) -> bool:
    """True for a source-MySQL failure that a fresh connection can recover from.

    MySQL-specific classifier behind ``MySQLSourceDialect.is_transient_error``; the
    engine-dispatching public entry point is :func:`is_source_transient_error` below.

    The Full Load's source read is the one place this matters: an Aurora failover
    (or any connection drop / stall) kills the in-flight read of a large table.
    Such a table is NOT broken -- re-reading it from a new connection succeeds -- so
    it is worth an automatic retry, whereas a genuine data/schema error (a bad type,
    a missing column) would fail identically forever and must surface immediately.

    Classification, in order: the MySQL error CODE carried by the driver exception
    (checked first because it is unambiguous), then a socket timeout type, then a
    message-signature fallback for a wrapped error whose code was lost. Anything
    unrecognized is treated as NON-transient, so a real error is never retried into
    a delay loop.
    """
    import socket

    # A DBAPI error wrapped by SQLAlchemy keeps the driver exception on .orig.
    candidates = [exc]
    orig = getattr(exc, "orig", None)
    if orig is not None and orig is not exc:
        candidates.append(orig)
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        candidates.append(cause)

    for candidate in candidates:
        # PyMySQL raises OperationalError(code, message) -- the code is args[0].
        args = getattr(candidate, "args", ()) or ()
        if args and isinstance(args[0], int) and args[0] in MYSQL_TRANSIENT_ERROR_CODES:
            return True
        # A stalled read hits the socket read_timeout (or the OS), which surfaces as
        # a timeout rather than a MySQL error code.
        if isinstance(candidate, (socket.timeout, TimeoutError)):
            return True

    message = str(exc).lower()
    return any(sig in message for sig in MYSQL_TRANSIENT_SIGNATURES)


# Operator-facing explanation for a source connection that dropped mid-load. The
# raw driver text ("OperationalError: (2013, 'Lost connection to MySQL server
# during query')") tells the user nothing about what to do, and this case is
# EXPECTED on Aurora (failover during a multi-hour load), so it gets a concrete
# next step and an explicit safety reassurance instead. ``{engine}`` is filled with
# the source dialect's display name so a PostgreSQL migration never reads "MySQL".
SOURCE_CONNECTION_LOST_HINT_TEMPLATE = (
    "The source {engine} connection dropped mid-read. On Aurora this is usually a "
    "failover (writer promotion during patching, an instance replacement, or an AZ "
    "event) — the database itself is fine. Nothing on the source was changed (the "
    "load only reads it), and re-running is safe: the load is idempotent and "
    "resumes by primary key, so it fills only what is missing and never duplicates "
    "rows."
)
# MySQL-rendered constant kept for backward compatibility (and existing callers/tests).
SOURCE_CONNECTION_LOST_HINT = SOURCE_CONNECTION_LOST_HINT_TEMPLATE.format(engine="MySQL")


# Codes that specifically mean the SOURCE ran out of connection slots, which needs
# different advice from a failover: fewer concurrent readers, not "wait and re-run".
_MYSQL_TOO_MANY_CONNECTIONS_CODES = frozenset({1040, 1203})

SOURCE_TOO_MANY_CONNECTIONS_HINT_TEMPLATE = (
    "The source {engine} refused a new connection because it is at its connection "
    "limit. Full Load opens one source reader per table (times the reader shards "
    "per table), so a high parallelism can exhaust a small instance's "
    "max_connections. Lower DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM (and/or "
    "FULL_LOAD_READER_SHARDS), or raise the source's max_connections, then re-run — "
    "the load is idempotent and fills only what is missing."
)
SOURCE_TOO_MANY_CONNECTIONS_HINT = SOURCE_TOO_MANY_CONNECTIONS_HINT_TEMPLATE.format(
    engine="MySQL"
)


def _mysql_too_many_connections(exc: BaseException) -> bool:
    """True when ``exc`` is the source MySQL refusing a connection for lack of slots.

    MySQL-specific predicate behind ``MySQLSourceDialect.is_too_many_connections``.
    """
    candidates = [exc]
    for attr in ("orig", "__cause__"):
        nested = getattr(exc, attr, None)
        if nested is not None and nested is not exc:
            candidates.append(nested)
    for candidate in candidates:
        args = getattr(candidate, "args", ()) or ()
        if (
            args
            and isinstance(args[0], int)
            and args[0] in _MYSQL_TOO_MANY_CONNECTIONS_CODES
        ):
            return True
    return "too many connections" in str(exc).lower()


# Backward-compatible alias (the historical private name).
_is_too_many_connections = _mysql_too_many_connections


def is_source_transient_error(
    exc: BaseException, source_type: SourceType = SourceType.MYSQL
) -> bool:
    """True for a source failure a fresh connection can recover from, per engine.

    Dispatches to the source dialect's classifier so the recoverable shapes are the
    right ones for the engine: MySQL numeric driver codes, PostgreSQL SQLSTATE classes
    (on ``.sqlstate``). A MySQL-only classifier silently never fires for psycopg, so a
    PostgreSQL failover would go un-retried without this dispatch. Defaults to MySQL so
    existing callers are unchanged. Anything unrecognized is NON-transient.
    """
    from dsql_migrator.core.source_dialect import dialect_for

    return dialect_for(source_type).is_transient_error(exc)


def source_error_hint(
    exc: BaseException, source_type: SourceType = SourceType.MYSQL
) -> Optional[str]:
    """Return an actionable operator hint for ``exc``, or ``None`` if there is none.

    Keeps the "what happened / what to do next" phrasing next to the classifier that
    recognizes the condition, so every surface (per-table error log, activity log,
    the UI notice) explains a source failure the same way, worded for the SOURCE engine
    (the dialect's display name fills the template — a PostgreSQL migration never reads
    "MySQL"). Connection EXHAUSTION gets its own hint: it is also transient, but waiting
    is not the fix -- the operator needs to reduce reader concurrency or raise the limit.
    """
    from dsql_migrator.core.source_dialect import dialect_for

    dialect = dialect_for(source_type)
    engine = dialect.engine_display_name
    if dialect.is_too_many_connections(exc):
        return SOURCE_TOO_MANY_CONNECTIONS_HINT_TEMPLATE.format(engine=engine)
    if dialect.is_transient_error(exc):
        return SOURCE_CONNECTION_LOST_HINT_TEMPLATE.format(engine=engine)
    return None


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
    """Build a read-only-guarded source engine from a connection config.

    The driver scheme and engine kwargs come from the source dialect selected by
    ``conn.source_type`` (default MySQL). ``dialect_for`` is imported lazily to avoid
    an import cycle (``source_dialect`` imports this module's MySQL helpers).
    """
    from dsql_migrator.core.source_dialect import dialect_for

    dialect = dialect_for(conn.source_type)
    password: Optional[str] = None
    if conn.secret is not None:
        password = resolve_secret(conn.secret).reveal()
    url = URL.create(
        dialect.driver_scheme,
        username=conn.username,
        password=password,
        host=conn.host,
        port=conn.port,
        database=conn.database,
    )
    engine = create_engine(url, **dialect.engine_kwargs())
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
                # MySQL column COMMENT (dropped on conversion; captured to warn).
                comment=(column.get("comment") or None),
            )
            for column in inspector.get_columns(table_name, schema=schema)  # type: ignore[attr-defined]
        ]

        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)  # type: ignore[attr-defined]
        primary_key = list(pk_constraint.get("constrained_columns") or [])

        indexes: list[IndexDef] = []
        expression_indexes: list[str] = []
        for index in inspector.get_indexes(table_name, schema=schema):  # type: ignore[attr-defined]
            raw_columns = index.get("column_names", [])
            index_columns = [c for c in raw_columns if c]
            index_name = index.get("name")
            # A MySQL 8 functional/expression index (``KEY ((LOWER(email)))``) reflects
            # its expression key-part(s) as None, and an ALL-expression index reflects
            # with an EMPTY column_names list (SQLAlchemy drops the parts it cannot name).
            # Either way it loses key columns; note it so the converter can warn it was
            # not carried over rather than let it vanish silently below.
            if index_name and (not index_columns or any(c is None for c in raw_columns)):
                expression_indexes.append(str(index_name))
            # MySQL prefix-index lengths (``KEY (col(N))``) live under the reflected
            # index's dialect_options["mysql_length"] = {column: N}. Carry them so the
            # converter can warn that DSQL indexes the FULL column (no prefix support).
            dialect_options = index.get("dialect_options") or {}
            prefix_lengths = {
                str(col): int(length)
                for col, length in (dialect_options.get("mysql_length") or {}).items()
                if col in index_columns
            }
            # PostgreSQL partial-index predicate and access method (SQLAlchemy exposes
            # both under dialect_options): a PARTIAL index (postgresql_where) and a
            # non-btree method (postgresql_using) have no Aurora DSQL equivalent, so they
            # must be carried so the converter can warn rather than emit a plain
            # btree/full index silently. Absent (None) on a MySQL index / a normal index.
            pg_where = dialect_options.get("postgresql_where")
            pg_using = dialect_options.get("postgresql_using")
            if index_name and index_columns:
                indexes.append(
                    IndexDef(
                        name=index_name,
                        columns=index_columns,
                        unique=bool(index.get("unique")),
                        prefix_lengths=prefix_lengths,
                        where=str(pg_where) if pg_where else None,
                        method=str(pg_using) if pg_using else None,
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
            # Referential actions (ON DELETE / ON UPDATE). SQLAlchemy's MySQL
            # dialect parses these out of SHOW CREATE TABLE into ``options`` and
            # OMITS the key when the action is the default NO ACTION, so a missing
            # key simply means "no automatic child-row change". Captured because a
            # CASCADE / SET NULL / SET DEFAULT is performed inside InnoDB and is
            # therefore absent from the binary log -- so CDC cannot replicate it
            # (see ForeignKeyDef.has_cascade_action).
            options = fk.get("options") or {}
            foreign_keys.append(
                ForeignKeyDef(
                    name=fk_name,
                    columns=constrained,
                    referenced_table=referenced_table,
                    referenced_columns=referred_columns,
                    on_delete=(options.get("ondelete") or None),
                    on_update=(options.get("onupdate") or None),
                )
            )

        # CHECK constraints (MySQL 8.0.16+). DSQL supports CHECK, but the converter does
        # not re-emit an arbitrary source expression (it may use MySQL-only functions);
        # reflecting them lets the assessor SURFACE the table (MANUAL) rather than
        # silently dropping a source-enforced constraint (Property 8 completeness).
        check_constraints: list[CheckConstraintDef] = []
        try:
            for ck in inspector.get_check_constraints(table_name, schema=schema):  # type: ignore[attr-defined]
                ck_name = ck.get("name")
                if not ck_name:
                    continue
                check_constraints.append(
                    CheckConstraintDef(
                        name=str(ck_name),
                        expression=str(ck.get("sqltext") or ""),
                    )
                )
        except Exception:  # noqa: BLE001 - reflection of checks is best-effort
            # An older MySQL (< 8.0.16) or a dialect quirk: absence of CHECK reflection
            # must never fail the whole introspection.
            check_constraints = []

        # MySQL table COMMENT (dropped on conversion; captured to warn). Best-effort:
        # a dialect without table-comment reflection must not fail introspection.
        table_comment: Optional[str] = None
        try:
            table_comment = (
                inspector.get_table_comment(table_name, schema=schema).get("text")  # type: ignore[attr-defined]
                or None
            )
        except Exception:  # noqa: BLE001 - table-comment reflection is best-effort
            table_comment = None

        tables.append(
            TableDef(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                indexes=indexes,
                foreign_keys=foreign_keys,
                check_constraints=check_constraints,
                auto_increment_column=None,
                expression_indexes=expression_indexes,
                comment=table_comment,
            )
        )
    return tables


def _reflect_views(inspector: object, schema: Optional[str] = None) -> list[ViewDef]:
    """Collect view names and definitions via SQLAlchemy reflection.

    The per-view ``get_view_definition`` is best-effort: a single view that cannot
    be ``SHOW CREATE``'d (an invalid/broken view whose underlying table was dropped,
    or a privilege error on that one view) must NOT abort the entire inventory. Such
    a view is SKIPPED with an empty definition -- its name is still carried so the
    operator can see it exists -- mirroring the best-effort try/except used for
    CHECK-constraint and table-comment reflection above. The read-only guard sentinel
    is never masked (Property 1): a ``ReadOnlySourceError`` is re-raised.
    """
    views: list[ViewDef] = []
    for view_name in inspector.get_view_names(schema=schema):  # type: ignore[attr-defined]
        try:
            definition = inspector.get_view_definition(view_name, schema=schema) or ""  # type: ignore[attr-defined]
        except ReadOnlySourceError:
            # Never swallow the read-only guard sentinel -- it means a write/DDL was
            # attempted on the source, which must surface, not be silently skipped.
            raise
        except Exception:  # noqa: BLE001 - one bad/broken view must not abort the inventory
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
    ``collation`` and ``default``, mark generated (``VIRTUAL/STORED GENERATED``) and
    ``ON UPDATE CURRENT_TIMESTAMP`` columns from ``EXTRA``, and set the table's
    ``auto_increment_column``.
    """
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME, EXTRA, COLUMN_TYPE, "
            "COLUMN_DEFAULT "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = :db"
        ),
        {"db": database},
    )

    collation_by_column: dict[tuple[str, str], Optional[str]] = {}
    extra_by_column: dict[tuple[str, str], str] = {}
    column_type_by_column: dict[tuple[str, str], str] = {}
    default_by_column: dict[tuple[str, str], Optional[str]] = {}
    auto_increment_by_table: dict[str, str] = {}
    for (
        table_name,
        column_name,
        collation_name,
        extra,
        column_type,
        column_default,
    ) in rows:
        collation_by_column[(table_name, column_name)] = collation_name
        extra_text = str(extra).lower() if extra else ""
        extra_by_column[(table_name, column_name)] = extra_text
        if column_type:
            column_type_by_column[(table_name, column_name)] = str(column_type)
        default_by_column[(table_name, column_name)] = (
            None if column_default is None else str(column_default)
        )
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
            # Prefer MySQL's COLUMN_DEFAULT over SQLAlchemy's reflected default, for
            # the same reason COLUMN_TYPE is preferred above: the reflected value comes
            # from a regex over SHOW CREATE TABLE and is lossy in ways that matter.
            # Measured against a real MySQL 8:
            #   * `datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`
            #     reflects as ONE string, "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            #     -- emitting that verbatim is a target syntax error. COLUMN_DEFAULT
            #     holds just "CURRENT_TIMESTAMP" (the ON UPDATE half stays in EXTRA,
            #     which is where ``auto_update_timestamp`` already reads it).
            #   * `bit(1) DEFAULT b'1'` reflects as None -- SILENTLY LOST -- while
            #     COLUMN_DEFAULT holds "b'1'".
            #   * `varchar DEFAULT 'a\'b'` reflects TRUNCATED at the backslash;
            #     COLUMN_DEFAULT holds the real value.
            #   * a MySQL 8 expression default reflects parenthesized ("(uuid())");
            #     COLUMN_DEFAULT holds "uuid()".
            # COLUMN_DEFAULT is UNQUOTED (a literal 0 is "0", not "'0'"), so the
            # literal-vs-expression decision now uses EXTRA's DEFAULT_GENERATED flag
            # rather than the presence of quotes -- see ``default_is_expression``.
            if (table.name, column.name) in default_by_column:
                column.default = default_by_column[(table.name, column.name)]
            extra_text = extra_by_column.get((table.name, column.name), "")
            # "VIRTUAL GENERATED"/"STORED GENERATED" mark a computed column;
            # "DEFAULT_GENERATED" (expression default) is intentionally excluded.
            if "virtual generated" in extra_text or "stored generated" in extra_text:
                column.generated = True
            if "on update" in extra_text:
                column.auto_update_timestamp = True
            # MySQL flags an EXPRESSION default (as opposed to a literal) with
            # DEFAULT_GENERATED in EXTRA. That is the authoritative signal now that the
            # default value itself arrives unquoted from COLUMN_DEFAULT: without it a
            # literal string "uuid()" and the function call uuid() are indistinguishable.
            # CURRENT_TIMESTAMP on a datetime/timestamp column also carries this flag.
            if "default_generated" in extra_text:
                column.default_is_expression = True
            elif column.default and (column.mysql_type or "").strip().lower().startswith(
                ("timestamp", "datetime")
            ):
                # MySQL < 8.0.13 (e.g. 5.7) has NO DEFAULT_GENERATED flag in EXTRA, so a
                # temporal FUNCTION default arrives in COLUMN_DEFAULT looking like a bare
                # token (e.g. "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(6)"). On a
                # datetime/timestamp column such a value can ONLY be the function -- MySQL
                # rejects a same-text string literal there -- so classify it as an
                # expression default (else the converter quotes it and the target CREATE
                # fails with "invalid input syntax for type timestamp"). Scoped to temporal
                # columns so a genuine VARCHAR literal is never misclassified; 8.0+ keeps
                # using the authoritative DEFAULT_GENERATED flag above.
                _canon = column.default.strip().upper().replace(" ", "")
                if _canon in (
                    "CURRENT_TIMESTAMP", "NOW()", "LOCALTIME", "LOCALTIMESTAMP",
                    "UTC_TIMESTAMP", "UTC_TIMESTAMP()",
                ) or _canon.startswith((
                    "CURRENT_TIMESTAMP(", "NOW(", "LOCALTIME(", "LOCALTIMESTAMP(",
                    "UTC_TIMESTAMP(",
                )):
                    column.default_is_expression = True
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

    ``STATISTICS`` also has one row per *key-part*, and on MySQL 8.0.13+ a
    functional/expression key-part is its own row (``EXPRESSION`` set,
    ``COLUMN_NAME`` NULL). SQLAlchemy's ``get_indexes`` reflects only the plain
    column key-parts (an expression part parses to nothing and is DROPPED), so a
    MIXED index like ``KEY (tenant_id, (lower(email)))`` reflects with just
    ``columns=['tenant_id']`` -- narrower than reality. Emitting that as-is would
    produce a WRONG index (for a UNIQUE index it silently changes the uniqueness
    semantics). The all-expression case reflects with NO columns and is already
    flagged at reflection time; the mixed case is caught HERE by comparing the
    reflected column count against the true key-part count: when the reflected
    index has fewer columns than STATISTICS rows for that index, an expression
    key-part was dropped, so it is treated exactly like an all-expression index --
    added to ``expression_indexes`` (so ``_expression_index_warning`` fires) and
    thereby NOT emitted as a narrower constraint by the converter. On 5.7 there are
    no functional key-parts, so the counts always match and behavior is unchanged.
    """
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, INDEX_TYPE "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = :db"
        ),
        {"db": database},
    )
    type_by_index: dict[tuple[str, str], Optional[str]] = {}
    keypart_count_by_index: dict[tuple[str, str], int] = {}
    for table_name, index_name, index_type in rows:
        key = (table_name, index_name)
        type_by_index[key] = index_type
        # Each STATISTICS row is one key-part (a plain column OR a functional
        # expression part on 8.0.13+), so counting rows gives the true key-part count.
        keypart_count_by_index[key] = keypart_count_by_index.get(key, 0) + 1

    for table in tables:
        for index in table.indexes:
            key = (table.name, index.name)
            index_type = type_by_index.get(key)
            if index_type is not None:
                index.index_type = str(index_type)
            # A reflected index with fewer columns than its true key-part count lost
            # an expression key-part (SQLAlchemy dropped it). Treat it like an
            # all-expression index: flag it and let the converter skip emitting the
            # narrower (possibly UNIQUE) index that would change the semantics.
            true_keyparts = keypart_count_by_index.get(key)
            if (
                true_keyparts is not None
                and len(index.columns) < true_keyparts
                and index.name not in table.expression_indexes
            ):
                table.expression_indexes.append(index.name)


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


def _user_schemas(
    inspector: object,
    system_schemas: frozenset[str],
    *,
    connection: object = None,
    dialect: "Optional[SourceDialect]" = None,
) -> list[str]:
    """Return non-system schema names on the cluster, in catalog order.

    ``system_schemas`` (the source dialect's engine-internal schemas) are excluded.
    The dialect may supply its own listing via ``list_schemas`` (PostgreSQL does, because
    SQLAlchemy's ``get_schema_names()`` drops user schemas named like ``pgapp``); when it
    returns ``None`` we fall back to the SQLAlchemy inspector.
    """
    names = None
    if dialect is not None and connection is not None:
        names = dialect.list_schemas(connection)
    if names is None:
        names = inspector.get_schema_names()  # type: ignore[attr-defined]
    return [name for name in names if name not in system_schemas]


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
    dialect: "SourceDialect",
) -> SourceInventory:
    """Assemble a :class:`SourceInventory` from one or all schemas.

    For an engine whose ``database`` IS a schema (MySQL, ``dialect.database_is_schema``):
    when ``database`` is set, that single schema is reflected with unqualified names
    (single-database mode); when empty/``None``, every non-system schema is reflected,
    ``schema.object``-qualified (cluster-wide mode). For an engine whose schemas live
    INSIDE the connection database (PostgreSQL, ``database_is_schema`` False), every
    non-system schema of the connected database is reflected + qualified regardless (so a
    non-``public`` schema is never silently dropped). Structural reflection is
    dialect-agnostic; the ``dialect`` supplies the system schemas + engine enrichment.
    """
    if database and dialect.database_is_schema:
        # Single-database mode (MySQL): reflect the connection's default schema and keep
        # names unqualified. ``enrich_db`` is the selected database.
        plans: list[tuple[Optional[str], str, bool]] = [(None, database, False)]
    else:
        # Reflect every non-system schema and qualify names. This is MySQL's cluster-wide
        # mode (blank database) AND the only mode for PostgreSQL (whose one connected
        # database holds many schemas -- public, app, ... -- all of which must migrate).
        plans = [
            (schema, schema, True)
            for schema in _user_schemas(
                inspector, dialect.system_schemas, connection=connection, dialect=dialect
            )
        ]

    all_tables: list[TableDef] = []
    all_views: list[ViewDef] = []
    all_triggers: list[ObjectRef] = []
    all_routines: list[ObjectRef] = []
    all_events: list[ObjectRef] = []

    for reflect_schema, enrich_db, qualify in plans:
        tables = _reflect_tables(inspector, schema=reflect_schema)
        views = _reflect_views(inspector, schema=reflect_schema)
        # Engine-specific enrichment (columns/indexes/partitions in place + stored
        # triggers/routines/events). No-ops for a dialect/connection without it.
        triggers, routines, events = dialect.enrich(connection, enrich_db, tables)
        # Relations with no plain-table/plain-view migration target that structural
        # reflection misses entirely (PostgreSQL materialized views + foreign tables,
        # relkinds 'm'/'f'). Carried as flagged views so Evaluation surfaces them
        # instead of dropping them; empty for engines/connections without them.
        views.extend(dialect.extra_relations(connection, enrich_db))

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
    """Connects to a source database (MySQL or PostgreSQL) and extracts its inventory (read-only)."""

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
        from dsql_migrator.core.source_dialect import dialect_for

        dialect = dialect_for(conn.source_type)
        engine: Optional[Engine] = None
        try:
            engine = self._engine_factory(conn)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                # Read source version metadata read-only so the UI can show the
                # source engine (e.g. Aurora MySQL / PostgreSQL version) on the
                # overview diagram. The dialect probes each version best-effort, so
                # a failure here never fails the connection test.
                versions = dialect.probe_versions(connection)
            return ConnectionResult(
                success=True,
                detail="Connection successful.",
                server_version=versions.server_version,
                engine_version=versions.engine_version,
                aurora_version=versions.aurora_version,
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
        from dsql_migrator.core.source_dialect import dialect_for

        dialect = dialect_for(conn.source_type)
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                inspector = inspect(connection)
                return _assemble_inventory(
                    inspector, connection, conn.database, dialect=dialect
                )
        finally:
            engine.dispose()

    def fetch_object_definition(
        self,
        conn: SourceConnectionConfig,
        object_name: str,
        object_type: ObjectType,
    ) -> Optional[str]:
        """Fetch the CREATE definition (body) of a routine / trigger / event, read-only.

        Introspection captures only the NAME and kind of stored procedures, functions,
        triggers and events -- never their body (unlike views, whose definition IS
        collected) -- so a per-object AI question about one of these otherwise has no
        source code to reason over. This runs the engine's definition query ON DEMAND
        through the read-only-guarded engine and returns the raw definition text --
        MySQL ``SHOW CREATE {PROCEDURE|FUNCTION|TRIGGER|EVENT}``, or PostgreSQL
        ``pg_get_functiondef`` / ``pg_get_triggerdef`` from the catalog. Best-effort:
        returns ``None`` on any failure (missing privilege, dropped object, no core
        equivalent) so the caller falls back to name-only guidance. Read-only
        (Property 1); the source password is injected by the engine factory and never
        stored. The engine kind follows ``conn.source_type`` (the factory selects the
        MySQL vs PostgreSQL driver from it).
        """
        source_type = getattr(conn, "source_type", SourceType.MYSQL)
        ident = None
        if source_type is SourceType.MYSQL:
            ident = _quote_mysql_identifier(object_name)
            if ident is None:
                return None
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                if source_type is SourceType.POSTGRES:
                    return _pg_object_definition(connection, object_name, object_type)
                if ident is not None:
                    return _mysql_object_definition(connection, ident, object_type)
                return None
        except Exception:  # noqa: BLE001 - best-effort; caller falls back to name-only
            return None
        finally:
            engine.dispose()


def _quote_mysql_identifier(name: str) -> Optional[str]:
    """Backtick-quote a (optionally schema-qualified) identifier for ``SHOW CREATE``.

    Returns ``None`` for anything unsafe -- a backtick / quote / semicolon, or a part
    that is not a plain identifier ``[\\w$]+``. The name comes from introspection (not
    user input), so this is defense-in-depth, keeping the interpolated ``SHOW CREATE``
    statement injection-proof.
    """
    import re

    if not name or "`" in name or '"' in name or ";" in name:
        return None
    parts = name.split(".")
    if not 1 <= len(parts) <= 2 or any(not re.fullmatch(r"[\w$]+", p) for p in parts):
        return None
    return ".".join(f"`{p}`" for p in parts)


def _mysql_object_definition(connection, ident: str, object_type: ObjectType) -> Optional[str]:
    """Return the MySQL ``SHOW CREATE`` body for one routine / trigger / event.

    Each ``SHOW CREATE`` is wrapped so a wrong-kind guess (a ``ROUTINE`` that is really
    a FUNCTION, tried as a PROCEDURE first) yields ``None`` instead of raising.
    """
    def _show(kind: str, column: str) -> Optional[str]:
        try:
            row = connection.execute(text(f"SHOW CREATE {kind} {ident}")).mappings().first()
            return row.get(column) if row else None
        except Exception:  # noqa: BLE001 - wrong kind / missing object -> None
            return None

    if object_type is ObjectType.PROCEDURE:
        return _show("PROCEDURE", "Create Procedure")
    if object_type is ObjectType.FUNCTION:
        return _show("FUNCTION", "Create Function")
    if object_type is ObjectType.ROUTINE:
        return _show("PROCEDURE", "Create Procedure") or _show("FUNCTION", "Create Function")
    if object_type is ObjectType.TRIGGER:
        return _show("TRIGGER", "SQL Original Statement")
    if object_type is ObjectType.EVENT:
        return _show("EVENT", "Create Event")
    return None


def _pg_object_definition(connection, object_name: str, object_type: ObjectType) -> Optional[str]:
    """Return the PostgreSQL definition for one function / procedure / trigger.

    Uses parameterized catalog queries (``pg_get_functiondef`` / ``pg_get_triggerdef``)
    -- the identifier is BOUND, never interpolated, so this is injection-proof. The name
    may be schema-qualified (``schema.name``); unqualified matches by name across
    schemas (first match). ``EVENT`` has no core PostgreSQL equivalent (that is a MySQL
    scheduled event), so it returns ``None``.
    """
    parts = object_name.split(".")
    schema = parts[0] if len(parts) == 2 else None
    name = parts[-1]

    def _one(sql: str, **params) -> Optional[str]:
        try:
            row = connection.execute(text(sql), params).first()
            return row[0] if row and row[0] else None
        except Exception:  # noqa: BLE001 - missing object / privilege -> None
            return None

    if object_type in (ObjectType.PROCEDURE, ObjectType.FUNCTION, ObjectType.ROUTINE):
        # pg_get_functiondef covers both functions (prokind 'f') and procedures ('p').
        sql = (
            "SELECT pg_get_functiondef(p.oid) FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = :name"
            + (" AND n.nspname = :schema" if schema else "")
            + " LIMIT 1"
        )
        return _one(sql, name=name, **({"schema": schema} if schema else {}))
    if object_type is ObjectType.TRIGGER:
        sql = (
            "SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE t.tgname = :name AND NOT t.tgisinternal"
            + (" AND n.nspname = :schema" if schema else "")
            + " LIMIT 1"
        )
        return _one(sql, name=name, **({"schema": schema} if schema else {}))
    return None


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
