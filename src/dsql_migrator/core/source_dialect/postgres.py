# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``PostgresSourceDialect`` -- RDS/Aurora PostgreSQL source-reading behavior.

The migration TARGET is Aurora DSQL (PostgreSQL-16 wire), so a PostgreSQL source is
near-identity: psycopg driver, double-quote identifiers, a REPEATABLE READ snapshot,
psycopg-native values. Scope is Full Load + Validation; CDC is deferred. Some methods
are staged: ``enrich`` is a v1 no-op (SQLAlchemy reflection already yields the
structure; PG-catalog enrichment is a later refinement) and ``value_converter`` is a
Phase-2 item that fails loudly rather than risk a silent mis-conversion.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text

from dsql_migrator.core.introspector import SOURCE_CONNECT_TIMEOUT_SECONDS
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import (
    SourceDialect,
    SourceVersions,
    estimate_row_counts_query,
    probe_scalar,
)

# System schemas excluded when resolving a bare (unqualified) table name to its columns.
_PG_SYSTEM_SCHEMAS_SQL = "('pg_catalog', 'information_schema', 'pg_toast')"

# PostgreSQL SQLSTATEs (beyond connection class ``08``) that a fresh connection +
# idempotent re-read recovers from during Full Load: operator_intervention (57P0x --
# admin/crash shutdown and cannot_connect_now during a failover), insufficient_resources
# (53300 too_many_connections / 53400 configuration_limit), which drain as other readers
# finish, and query_canceled (57014). 57014 is how the Full Load per-page read timeout
# surfaces on PostgreSQL: ``read_timeout_seconds`` is applied as libpq ``statement_timeout``
# (see engine_kwargs), so a stalled or over-long page is canceled with 57014 -- the exact
# analog of MySQL's socket ``read_timeout`` (a stall -> socket.timeout, classified transient)
# -- and must likewise auto-retry the table from a fresh snapshot (the read path's only
# source of 57014; a cooperative Stop raises ExportCancelled, not a driver cancel). A
# genuine data/schema error carries a 22/23/42 SQLSTATE and is therefore NOT matched
# (never retried into a delay loop); bounded retry attempts stop a page that never completes.
_PG_TRANSIENT_SQLSTATES = frozenset(
    {"57P01", "57P02", "57P03", "53300", "53400", "57014"}
)


def _pg_error_candidates(exc: BaseException) -> list[BaseException]:
    """The exception plus its wrapped ``.orig`` / ``.__cause__`` (psycopg under
    SQLAlchemy keeps the real ``.sqlstate`` on ``.orig``)."""
    candidates: list[BaseException] = [exc]
    for attr in ("orig", "__cause__"):
        nested = getattr(exc, attr, None)
        if nested is not None and nested is not exc:
            candidates.append(nested)
    return candidates


def _reads_as_text(type_string: str) -> bool:
    """True for PG types Full Load must read via a text cast rather than natively.

    psycopg's native round trip is lossy or parse-heavy for these:
    - ``json`` / ``jsonb``: the default loader ``json.loads`` -> a Python dict/list, which
      the target dumper would ``json.dumps`` back (a ~10x round trip) and which collapses a
      JSON literal ``null`` to Python ``None`` (-> SQL NULL);
    - ``interval`` (incl. fields-qualified ``interval day to second``): psycopg loads it as
      a ``datetime.timedelta``, which CANNOT hold months/years -- it silently collapses
      ``1 mon`` -> 30 days / ``1 year`` -> 365 days, and raises under a non-default
      ``IntervalStyle``.
    Reading these as their exact source text (``CAST(col AS text)``) and binding that text
    to the identical target column as an unknown-typed literal (oid 0, which the server
    re-parses) is faithful for all of them -- the same path MySQL's JSON text uses.
    """
    base = type_string.split("(", 1)[0].strip().lower()
    return base in ("json", "jsonb") or base.startswith("interval")

# PostgreSQL integer base types (lower-cased, precision stripped). Same sharding
# rationale as MySQL: only a collation-free integer leading PK column is range-shardable.
# Includes the internal aliases (int2/int4/int8) and the serial pseudo-types, which
# reflect as their underlying integer type but are listed for robustness.
_PG_INTEGER_PK_TYPES = frozenset(
    {
        "smallint",
        "integer",
        "int",
        "bigint",
        "int2",
        "int4",
        "int8",
        "smallserial",
        "serial",
        "bigserial",
    }
)


class PostgresSourceDialect(SourceDialect):
    """RDS/Aurora PostgreSQL source dialect (read-only)."""

    source_type = SourceType.POSTGRES
    # A PostgreSQL "database" is the connection target; its user data lives in SCHEMAS
    # inside it. So a set ``database`` must reflect ALL non-system schemas (public, app,
    # ...), schema-qualified -- not just the default ``public`` (which would silently
    # drop every other schema). Contrast MySQL, where a database IS a schema.
    database_is_schema = False

    @property
    def driver_scheme(self) -> str:
        # psycopg 3 (already a project dependency); the SQLAlchemy 2.x psycopg dialect.
        return "postgresql+psycopg"

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def system_schemas(self) -> frozenset[str]:
        # Engine-internal schemas never part of a user's migratable inventory.
        return frozenset({"pg_catalog", "information_schema", "pg_toast"})

    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        # Pin locale/format GUCs so the source renders text IDENTICALLY to the Aurora
        # DSQL target (whose defaults are exactly these: timezone/DateStyle=ISO,
        # IntervalStyle=postgres, lc_numeric=C). Validation reuses the target's PG
        # checksum renderer, whose numeric to_char 'D' mask honors lc_numeric and whose
        # date/interval ::text honor DateStyle/IntervalStyle -- so a source DB with a
        # non-default locale (e.g. lc_numeric=de_DE -> '3,14') would otherwise produce a
        # FALSE checksum MISMATCH on byte-identical data. Pinning also makes the Full Load
        # interval text cast (see select_column_sql) style-consistent. UTC also keeps
        # timestamp/timestamptz deterministic. psycopg passes these via libpq ``options``;
        # a read timeout bounds a stalled stream via ``statement_timeout`` (milliseconds).
        options = (
            "-c timezone=UTC -c datestyle=ISO -c intervalstyle=postgres -c lc_numeric=C"
        )
        connect_args: dict[str, object] = {
            "connect_timeout": SOURCE_CONNECT_TIMEOUT_SECONDS,
            "options": options,
        }
        if read_timeout_seconds is not None:
            timeout = int(read_timeout_seconds)
            # MySQL's read timeout is a per-socket IDLE timeout: it fails a STALLED read
            # (a page that stops delivering rows / a dropped or failed-over connection)
            # WITHOUT capping a healthy page that keeps streaming. PostgreSQL has no
            # per-statement idle timeout, so match the intent with two libpq mechanisms:
            #   - TCP keepalives + tcp_user_timeout detect a dead/stalled/failed-over
            #     connection (unACKed data) within ~the budget -> a class-08 connection
            #     error the dialect classifies transient -> the table auto-retries. These
            #     do NOT fire while a page is actively streaming (data keeps getting
            #     ACKed), so a legitimately slow-but-progressing page is never killed --
            #     unlike a bare statement_timeout, which is a TOTAL per-statement cap.
            #   - statement_timeout stays as the backstop for a hung-but-alive query
            #     (server executing, delivering nothing): it fires SQLSTATE 57014, also
            #     classified transient (see _PG_TRANSIENT_SQLSTATES) so the table retries.
            options += f" -c statement_timeout={timeout * 1000}"
            connect_args["options"] = options
            connect_args["keepalives"] = 1
            connect_args["keepalives_idle"] = max(1, timeout // 3)
            connect_args["keepalives_interval"] = max(1, timeout // 6)
            connect_args["keepalives_count"] = 3
            # tcp_user_timeout is milliseconds; no-op on platforms without TCP_USER_TIMEOUT
            # (e.g. macOS) and on Unix-domain sockets, effective on the Linux deploy target.
            connect_args["tcp_user_timeout"] = timeout * 1000
        return {"pool_pre_ping": True, "connect_args": connect_args}

    def enrich(
        self, connection: object, enrich_db: str, tables: list
    ) -> tuple[list, list, list]:
        # Capture EXACT PostgreSQL type strings via format_type(atttypid, atttypmod):
        # generic SQLAlchemy reflection loses array element types (text[] -> "ARRAY"),
        # timestamptz -> "TIMESTAMP", precision, etc. This overwrites each column's
        # reflected type string in place so the converter/assessor see the true PG type.
        # A non-PostgreSQL connection (e.g. the SQLite test double) no-ops (mirrors the
        # MySQL dialect's runtime guard). Stored trigger/function/event collection from
        # pg_catalog is a later refinement, so triggers/routines/events stay empty.
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "postgresql":
            return ([], [], [])

        for table in tables:
            schema, _, bare = table.name.rpartition(".")
            params: dict[str, object] = {"rel": bare}
            if schema:
                schema_filter = "AND n.nspname = :nsp"
                params["nsp"] = schema
            else:
                # Bare name (single-schema reflection): restrict to a user schema.
                schema_filter = f"AND n.nspname NOT IN {_PG_SYSTEM_SCHEMAS_SQL}"
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT a.attname AS col, "
                    "format_type(a.atttypid, a.atttypmod) AS typ "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = :rel AND a.attnum > 0 "
                    f"AND NOT a.attisdropped {schema_filter}"
                ),
                params,
            ).mappings()
            exact = {row["col"]: row["typ"] for row in rows}
            for column in table.columns:
                resolved = exact.get(column.name)
                if resolved:
                    column.mysql_type = resolved
        return ([], [], [])

    def quote_identifier(self, name: str) -> str:
        # PostgreSQL: double quotes, embedded double-quotes doubled.
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def quote_table(self, name: str) -> str:
        # Split on the first dot so ``schema.table`` becomes "schema"."table"; each part
        # is quoted independently (a lone quoted "schema.table" would be one identifier).
        schema, separator, obj = name.partition(".")
        if separator and schema and obj:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(obj)}"
        return self.quote_identifier(name)

    @property
    def integer_pk_types(self) -> frozenset[str]:
        return _PG_INTEGER_PK_TYPES

    def select_column_sql(self, column: object) -> str:
        # Most columns read as-is (quoted). json/jsonb/interval are read via a text cast so
        # Full Load streams their EXACT text and binds it back to the identical target
        # column as an unknown-typed literal (oid 0), which the server re-parses --
        # faithful AND fast (see _reads_as_text for why the native psycopg round trip is
        # lossy/parse-heavy for these). json/jsonb can't be a PK and an interval PK still
        # paginates correctly (the text boundary is cast back to interval for `> :last`).
        # PostGIS geometry is out of scope (no ST_AsBinary-style case) for a first release.
        quoted = self.quote_identifier(column.name)  # type: ignore[attr-defined]
        if _reads_as_text(column.mysql_type):  # type: ignore[attr-defined]
            return f"CAST({quoted} AS text) AS {quoted}"
        return quoted

    @property
    def snapshot_start_sql(self) -> str:
        # PostgreSQL consistent read snapshot for the streaming read.
        return "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"

    def value_converter(self, table: object, *, target_types: object = None) -> object:
        # PG->DSQL is psycopg-native on both ends, so Full Load value conversion is pure
        # pass-through (json/jsonb/interval fidelity is handled by select_column_sql's text
        # cast on read, not per value). Kept in its own module (exporter_postgres) per the
        # per-engine separation rule.
        from dsql_migrator.core.exporter_postgres import PostgresValueConverter

        return PostgresValueConverter(table, target_types=target_types)

    def estimate_row_counts(
        self, connection: object, tables: list[str]
    ) -> "dict[str, Optional[int]]":
        # PostgreSQL: pg_class.reltuples is the planner's row estimate (maintained by
        # ANALYZE/autovacuum); join pg_namespace for the schema, and the default schema is
        # current_schema() (NOT current_database()). relkind IN ('r','p') covers ordinary
        # + partitioned tables. reltuples is -1 for a never-analyzed table in PG14+ (and
        # can be a stale float); map negative/NULL to None ("unknown", not a real 0).
        return estimate_row_counts_query(
            connection,
            tables,
            current_schema_sql="SELECT current_schema()",
            select_from="FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
            schema_column="n.nspname",
            table_column="c.relname",
            estimate_column="c.reltuples::bigint",
            extra_filter="c.relkind IN ('r', 'p')",
            parse_estimate=lambda value: (
                None if value is None or int(value) < 0 else int(value)
            ),
        )

    def probe_versions(self, connection: object) -> SourceVersions:
        # version() is the verbose banner ("PostgreSQL 16.4 ... on <arch>").
        # SHOW server_version is "<numeric>[ (<packaging>)]" -- "16.10 (Homebrew)",
        # "16.4 (Debian ...)", or a clean "16.4" on RDS/Aurora -- so keep only the
        # leading numeric token for a clean engine_version. aurora_version() gives the
        # Aurora PostgreSQL engine version (Aurora only; community/RDS lacks the
        # function, so it best-efforts to None).
        server_version = probe_scalar(connection, "SHOW server_version")
        return SourceVersions(
            server_version=probe_scalar(connection, "SELECT version()"),
            engine_version=server_version.split()[0] if server_version else None,
            aurora_version=probe_scalar(connection, "SELECT aurora_version()"),
        )

    def probe_grants(self, connection: object) -> list[str]:
        # PostgreSQL has NO ``SHOW GRANTS`` (running MySQL's statement here errors ->
        # empty -> a FALSE "SELECT missing" FAIL that blocks the Full Load). Instead:
        # a superuser bypasses every privilege check, so report ALL PRIVILEGES; a
        # non-superuser's table privileges come from information_schema.role_table_grants
        # (privileges granted to the current role or PUBLIC). If SELECT is among them the
        # Full Load privilege check passes. Coarse by design -- grant presence, not
        # per-migrated-table -- which matches MySQL's SHOW GRANTS. Best effort: any error
        # yields [] (the check FAILs with remediation).
        try:
            is_super = connection.execute(  # type: ignore[attr-defined]
                text("SELECT current_setting('is_superuser')")
            ).scalar()
        except Exception:  # noqa: BLE001 - unknown -> fall through to the grants query
            is_super = None
        if str(is_super).lower() == "on":
            return ["ALL PRIVILEGES"]
        try:
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT DISTINCT privilege_type "
                    "FROM information_schema.role_table_grants "
                    "WHERE grantee IN (current_user, 'PUBLIC')"
                )
            ).fetchall()
        except Exception:  # noqa: BLE001 - treated as "no grants visible"
            return []
        return [str(row[0]) for row in rows if row]

    @property
    def engine_display_name(self) -> str:
        return "PostgreSQL"

    def is_transient_error(self, exc: BaseException) -> bool:
        # psycopg carries a STRING SQLSTATE on .sqlstate (never an int code like MySQL),
        # so the MySQL classifier would never fire for a PG source. Classify by SQLSTATE:
        # connection class 08 or the operator-intervention/insufficient-resource states a
        # fresh connection recovers from. A decisive non-transient SQLSTATE (22/23/42 data
        # or schema error) means NOT transient -- never fall through to signatures. Only
        # when NO SQLSTATE is present anywhere (server never answered: a dropped socket /
        # TLS teardown / connect timeout the wrapper may have flattened) do we treat a
        # psycopg connection-level error type, or a known drop signature, as transient.
        import socket

        candidates = _pg_error_candidates(exc)
        saw_sqlstate = False
        for candidate in candidates:
            if isinstance(candidate, (socket.timeout, TimeoutError)):
                return True
            state = getattr(candidate, "sqlstate", None)
            if isinstance(state, str):
                saw_sqlstate = True
                if state.startswith("08") or state in _PG_TRANSIENT_SQLSTATES:
                    return True
        if saw_sqlstate:
            return False  # a real, non-transient SQLSTATE is authoritative
        for candidate in candidates:
            module = type(candidate).__module__ or ""
            name = type(candidate).__name__
            if module.startswith("psycopg") and name in (
                "OperationalError",
                "InterfaceError",
            ):
                return True
        from dsql_migrator.core.target_connection import TRANSIENT_CONN_SIGNATURES

        message = str(exc).lower()
        return any(sig in message for sig in TRANSIENT_CONN_SIGNATURES)

    def is_too_many_connections(self, exc: BaseException) -> bool:
        # PostgreSQL too_many_connections is SQLSTATE 53300 (its message is
        # "sorry, too many clients already" / "remaining connection slots are reserved").
        for candidate in _pg_error_candidates(exc):
            state = getattr(candidate, "sqlstate", None)
            if isinstance(state, str) and state == "53300":
                return True
        low = str(exc).lower()
        return (
            "too many clients" in low
            or "too many connections" in low
            or "remaining connection slots" in low
        )

    def capture_resume_lsn(self, connection: object) -> Optional[str]:
        # The WAL LSN a PostgreSQL CDC catch-up resumes from (the gapless handoff point,
        # PG's analog of MySQL binlog:pos). pg_current_wal_lsn() is the primary's current
        # insert position; on a standby/read-replica it errors, so branch on
        # pg_is_in_recovery() to pg_last_wal_replay_lsn(). Cast to text ('3/AF012B8').
        # Best effort via probe_scalar: any failure (insufficient privilege) -> None.
        return probe_scalar(
            connection,
            "SELECT (CASE WHEN pg_is_in_recovery() "
            "THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END)::text",
        )

    def read_active_query_count(self, connection: object) -> Optional[int]:
        # PostgreSQL live active-query concurrency = backends currently executing a query
        # in pg_stat_activity (state='active'). This is a plain SELECT that SUCCEEDS
        # inside the export's REPEATABLE READ snapshot (so it never aborts the txn the way
        # a MySQL SHOW would) and reads live shared-memory state (not the MVCC snapshot),
        # so the governor sees current load. pg_stat_activity.state exists on every
        # supported PostgreSQL (9.2+). Fail-open: None on any error -> governor won't
        # throttle (and never stalls the load).
        try:
            value = connection.execute(  # type: ignore[attr-defined]
                text("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
            ).scalar()
        except Exception:  # noqa: BLE001 - best-effort; never fail the load on a probe
            return None
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None


__all__ = ["PostgresSourceDialect"]
