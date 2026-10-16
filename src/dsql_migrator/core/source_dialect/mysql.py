# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``MySQLSourceDialect`` -- RDS/Aurora MySQL source-reading behavior.

The original, default engine. Delegates to the existing introspector/exporter/watermark
helpers (lazy-imported inside methods where needed to avoid an import cycle) so the
MySQL path is byte-identical to before the dialect seam was introduced.
"""

from __future__ import annotations

import re

from typing import Optional

from dsql_migrator.core.introspector import (
    MYSQL_DRIVER,
    MYSQL_SYSTEM_SCHEMAS,
    _mysql_source_transient,
    _mysql_too_many_connections,
    source_engine_kwargs,
)
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import (
    SourceDialect,
    SourceVersions,
    estimate_row_counts_query,
    probe_scalar,
)

# MySQL integer base types (lower-cased, display width / UNSIGNED / ZEROFILL stripped
# before matching). Reader range sharding bands the LEADING PK column, which is only
# safe for a collation-free integer column; a non-integer leading column falls back to
# a single reader.
_MYSQL_INTEGER_PK_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)


class MySQLSourceDialect(SourceDialect):
    """RDS/Aurora MySQL source dialect -- the original, default engine."""

    source_type = SourceType.MYSQL

    @property
    def driver_scheme(self) -> str:
        return MYSQL_DRIVER

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def system_schemas(self) -> frozenset[str]:
        return MYSQL_SYSTEM_SCHEMAS

    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        return source_engine_kwargs(read_timeout_seconds=read_timeout_seconds)

    def enrich(
        self,
        connection: object,
        enrich_db: str,
        tables: list,
        views: "Optional[list]" = None,
    ) -> tuple[list, list, list]:
        # ``views`` is unused: MySQL has no extension-owned objects to prune, and its
        # reflection returns exactly the user's views.
        # MySQL enrichment reads information_schema; run it only against a genuine
        # MySQL connection. A non-MySQL engine (e.g. the SQLite double used in tests)
        # safely no-ops, preserving the prior runtime gate
        # (``connection.dialect.name == "mysql"``).
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "mysql":
            return ([], [], [])
        from dsql_migrator.core.introspector import (
            collect_events,
            collect_routines,
            collect_triggers,
            enrich_columns,
            enrich_index_types,
            enrich_partitions,
        )

        enrich_columns(connection, enrich_db, tables)
        enrich_index_types(connection, enrich_db, tables)
        enrich_partitions(connection, enrich_db, tables)
        return (
            collect_triggers(connection, enrich_db),
            collect_routines(connection, enrich_db),
            collect_events(connection, enrich_db),
        )

    def quote_identifier(self, name: str) -> str:
        # MySQL: backticks, embedded backticks doubled.
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def quote_table(self, name: str) -> str:
        # Split on the first dot so ``schema.table`` becomes `schema`.`table` rather
        # than one `schema.table` identifier (which MySQL reads as one table in the
        # unset current database -> "1046, No database selected").
        schema, separator, obj = name.partition(".")
        if separator and schema and obj:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(obj)}"
        return self.quote_identifier(name)

    @property
    def integer_pk_types(self) -> frozenset[str]:
        return _MYSQL_INTEGER_PK_TYPES

    def select_column_sql(
        self, column: object, *, target_type: Optional[str] = None
    ) -> str:
        # Spatial columns have no DSQL type -> read as ST_AsBinary (WKB bytes, matching
        # what Debezium delivers for CDC) aliased back to the name; others read as-is.
        from dsql_migrator.core.converter import is_spatial_mysql_type

        quoted = self.quote_identifier(column.name)  # type: ignore[attr-defined]
        if is_spatial_mysql_type(column.mysql_type):  # type: ignore[attr-defined]
            return f"ST_AsBinary({quoted}) AS {quoted}"
        return quoted

    @property
    def snapshot_start_sql(self) -> str:
        # MySQL: InnoDB REPEATABLE READ consistent snapshot (same as watermark capture).
        from dsql_migrator.core.watermark import START_CONSISTENT_SNAPSHOT

        return START_CONSISTENT_SNAPSHOT

    def value_converter(self, table: object, *, target_types: object = None) -> object:
        from dsql_migrator.core.exporter import ValueConverter

        return ValueConverter(table, target_types=target_types)

    def live_number_shape(self, entry: object) -> "Optional[tuple[str, Optional[int]]]":
        # PyMySQL's 7-tuple (name, type_code, display_size, internal_size, precision, scale,
        # null_ok). DECIMAL (0) / NEWDECIMAL (246) carry their declared scale; FLOAT (4) and
        # DOUBLE (5) are floating point.
        try:
            type_code = entry[1]  # type: ignore[index]
            if type_code in (0, 246):
                scale = entry[5]  # type: ignore[index]
                return ("decimal", int(scale) if scale is not None else None)
            if type_code in (4, 5):
                return ("float", None)
        except (IndexError, TypeError, ValueError):
            return None
        return None

    def estimate_row_counts(
        self, connection: object, tables: list[str]
    ) -> "dict[str, Optional[int]]":
        # MySQL: information_schema.tables.table_rows is the storage-engine row estimate;
        # the default schema is the current DATABASE(). (Byte-identical to the query the
        # watermark module issued inline before the dialect seam.)
        return estimate_row_counts_query(
            connection,
            tables,
            current_schema_sql="SELECT DATABASE()",
            select_from="FROM information_schema.tables",
            schema_column="table_schema",
            table_column="table_name",
            estimate_column="table_rows",
        )

    def probe_versions(self, connection: object) -> SourceVersions:
        # VERSION() carries the raw wire version (with the Aurora tag before newer
        # builds dropped it); @@innodb_version exposes the full community patch (e.g.
        # 8.0.42); @@aurora_version gives the Aurora MySQL engine version (Aurora only).
        # Each is best effort -- a non-Aurora/RDS source simply has no @@aurora_version.
        return SourceVersions(
            server_version=probe_scalar(connection, "SELECT VERSION()"),
            engine_version=probe_scalar(connection, "SELECT @@innodb_version"),
            aurora_version=probe_scalar(connection, "SELECT @@aurora_version"),
        )

    def probe_grants(self, connection: object) -> list[str]:
        # MySQL exposes the current user's privileges as raw ACL lines via SHOW GRANTS.
        # Best effort: any error is treated as "no grants visible" (empty), so the
        # privilege check reports a FAIL with remediation rather than crashing the run.
        from sqlalchemy import text

        try:
            rows = connection.execute(  # type: ignore[attr-defined]
                text("SHOW GRANTS")
            ).fetchall()
        except Exception:  # noqa: BLE001 - treated as "no grants visible"
            return []
        return [str(row[0]) for row in rows if row]

    def listing_gaps(self, connection: object, databases) -> list[str]:
        # information_schema lists a trigger only with TRIGGER, an event only with EVENT
        # and a routine only with SHOW_ROUTINE (or global SELECT, or a routine privilege),
        # and a view's body needs SHOW VIEW (that one is graded per view, from the empty
        # definition). The privileges of the session's ACTIVE roles count, so they are
        # expanded with USING; when they cannot be, nothing is claimed.
        from sqlalchemy import text

        try:
            roles = connection.execute(text("SELECT CURRENT_ROLE()")).scalar()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - MySQL 5.7 has no roles
            roles = None
        statement = "SHOW GRANTS"
        if roles and str(roles).strip().upper() != "NONE":
            statement = f"SHOW GRANTS FOR CURRENT_USER() USING {roles}"
        try:
            rows = connection.execute(text(statement)).fetchall()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - unknown: claim nothing
            return []
        return mysql_listing_gaps([str(row[0]) for row in rows if row], databases)

    @property
    def engine_display_name(self) -> str:
        return "MySQL"

    def is_transient_error(self, exc: BaseException) -> bool:
        # MySQL driver codes / socket timeout / message signatures (see the classifier).
        return _mysql_source_transient(exc)

    def is_too_many_connections(self, exc: BaseException) -> bool:
        # MySQL "too many connections" (ER_CON_COUNT_ERROR 1040 / 1203).
        return _mysql_too_many_connections(exc)

    def capture_resume_lsn(self, connection: object) -> Optional[str]:
        # MySQL's CDC resume coordinate is the binlog file:pos / GTID captured by the
        # dedicated WatermarkCapturer (watermark.py), not this seam -> None here.
        return None

    def read_active_query_count(self, connection: object) -> Optional[int]:
        # MySQL live active-query concurrency = global Threads_running (SHOW GLOBAL
        # STATUS). Lazy import to avoid an import cycle (exporter imports the dialect
        # registry). Fail-open (None) is handled inside _read_threads_running.
        from dsql_migrator.core.exporter import _read_threads_running

        return _read_threads_running(connection)

    def probe_cdc_prerequisites(
        self, connection: object, table_names, *, publication_name="", slot_name=""
    ):
        # MySQL CDC readiness is the binlog/GTID checks (not logical-replication slots),
        # so there are no PostgreSQL-style facts to gather here.
        return None

    def read_replication_slot_health(self, connection: object, slot_name: str):
        # MySQL has no logical replication slot (its binlog auto-expires), so there is
        # no WAL-retention health to report.
        return None


__all__ = ["MySQLSourceDialect"]


_GRANT_LINE = re.compile(
    r"^GRANT\s+(?P<privs>.+?)\s+ON\s+(?:(?:TABLE|FUNCTION|PROCEDURE)\s+)?(?P<scope>\S+)\s+TO\s",
    re.IGNORECASE,
)


def _split_privileges(text: str) -> list[str]:
    """``SELECT (a, b), INSERT`` -> ``["SELECT", "INSERT"]`` (column lists dropped)."""
    privileges, depth, current = [], 0, []
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            privileges.append("".join(current))
            current = []
            continue
        if depth == 0 and char not in "()":
            current.append(char)
    privileges.append("".join(current))
    return [" ".join(p.split()).upper() for p in privileges if p.strip()]


def _like(pattern: str, name: str) -> bool:
    """MySQL's grant-scope match: ``%`` and ``_`` wildcards, ``\\`` escapes."""
    out, escaped = [], False
    for char in pattern:
        if escaped:
            out.append(re.escape(char))
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        else:
            out.append(re.escape(char))
    return re.fullmatch("".join(out), name) is not None


def mysql_listing_gaps(grant_lines: "list[str]", databases) -> list[str]:
    """The object kinds a MySQL user with ``grant_lines`` cannot list completely, in
    ``databases``: one human line per kind naming the databases and the privilege. Pure.

    ``grant_lines`` are the SESSION's: direct grants plus those of its active roles
    (``SHOW GRANTS ... USING``); a role that is granted but not active gives the session
    nothing, so its membership line (``GRANT `r`@`%` TO ...``, no ``ON``) is ignored. ``[]``
    when the grants are unknown (none read)."""
    if not grant_lines:
        return []
    global_privs: set[str] = set()
    schema_privs: dict[str, set[str]] = {}
    for line in grant_lines:
        match = _GRANT_LINE.match(line.strip())
        if match is None:
            continue
        privileges = set(_split_privileges(match.group("privs")))
        if "ALL" in privileges:
            privileges.add("ALL PRIVILEGES")
        scope = match.group("scope")
        if scope in ("*.*", "`*`.*"):
            global_privs |= privileges
            continue
        db, _, obj = scope.partition(".")
        if obj != "*":
            continue  # table, column or routine level: not a whole database
        schema_privs.setdefault(db.strip("`'\""), set()).update(privileges)

    def held(database: str, wanted: "set[str]") -> bool:
        if global_privs & (wanted | {"ALL PRIVILEGES"}):
            return True
        return any(
            _like(pattern, database) and privileges & (wanted | {"ALL PRIVILEGES"})
            for pattern, privileges in schema_privs.items()
        )

    routine_global = {"SHOW_ROUTINE", "SELECT", "CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE"}
    checks = (
        ("triggers", "TRIGGER", {"TRIGGER"}, set()),
        ("scheduled events", "EVENT", {"EVENT"}, set()),
        (
            "stored procedures and functions",
            "SHOW_ROUTINE (or SELECT on *.*)",
            {"CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE"},
            routine_global,
        ),
    )
    gaps: list[str] = []
    for label, needs, schema_level, global_level in checks:
        if global_level and global_privs & (global_level | {"ALL PRIVILEGES"}):
            continue
        missing = [db for db in databases if not held(db, schema_level)]
        if missing:
            shown = ", ".join(missing[:5]) + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
            gaps.append(f"{label} in {shown} (needs {needs})")
    return gaps
