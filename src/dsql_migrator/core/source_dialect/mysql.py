# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``MySQLSourceDialect`` -- RDS/Aurora MySQL source-reading behavior.

The original, default engine. Delegates to the existing introspector/exporter/watermark
helpers (lazy-imported inside methods where needed to avoid an import cycle) so the
MySQL path is byte-identical to before the dialect seam was introduced.
"""

from __future__ import annotations

import re

from typing import Mapping, Optional, Sequence

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
    keyset_param_names,
    probe_scalar,
)

# MySQL integer base types (lower-cased, display width / UNSIGNED / ZEROFILL stripped
# before matching). Reader range sharding bands the LEADING PK column, which is only
# safe for a collation-free integer column; a non-integer leading column falls back to
# a single reader.
_MYSQL_INTEGER_PK_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)

# Sent on every new source connection (see ``MySQLSourceDialect.session_setup_sql``):
# ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` opens a snapshot only at REPEATABLE READ.
# Below it MySQL ignores the clause (warning 138) and each statement reads what is committed
# when it runs, so on a source configured READ-COMMITTED (``transaction_isolation``) the Full
# Load read (each keyset page from a later point), the watermark capture and Validation's
# per-table snapshot were not snapshots, and a row whose key moved during a table's read was
# read twice or missed (G136: reproduced with the real single reader). A session SET: the
# server's setting is left alone, no privilege is needed, and it is not a write (the
# read-only guard lets a SET through). The cost is the one a REPEATABLE READ source always
# had: each table read holds one InnoDB read view, which holds back purge (History List
# Length) on a write-heavy source.
_MYSQL_REPEATABLE_READ = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"


# The live definitions of a table's key columns, read once when a keyset stream of a table
# whose key has an ENUM / SET column starts (G130): the cursor names such a column's
# members, and Step 1 (Evaluation)'s record of them goes stale when a member is added or
# reordered (the read would then leave whole members' rows unread, silently). Catalog only,
# keyed on schema + table: no scan, no lock. An unqualified name is the current database's,
# as the read's own FROM resolves it.
_KEY_COLUMN_TYPES_SQL = (
    "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = :name"
)

_ENUM_OR_SET = re.compile(r"\s*(?:enum|set)\s*\(", re.IGNORECASE)


def _column_type_sql(kind: str, members: Sequence[str]) -> str:
    """``COLUMN_TYPE`` as MySQL writes it for these members (``Field_enum::sql_type``: each
    member quoted with ``\\``, ``''``, ``\\0``, ``\\n``, ``\\r`` escaped). A decode that does not
    write back to the catalog's text exactly has misread a member."""
    quoted = (
        "'"
        + member.replace("\\", "\\\\").replace("'", "''").replace("\0", "\\0")
        .replace("\n", "\\n").replace("\r", "\\r")
        + "'"
        for member in members
    )
    return f"{kind}({','.join(quoted)})"


def _key_after_sql(
    quoted: str,
    column: str,
    member: "Optional[tuple[str, tuple[str, ...]]]",
    name: str,
    value: object,
    params: dict,
) -> Optional[str]:
    """One key column's strict keyset term: its values that sort after ``value`` (the
    cursor, bound as ``:name``), or ``None`` when none does. Adds what it binds to ``params``.

    ``col > :name`` -- except for an ENUM or a SET (``member``: its kind and values, as
    :meth:`MySQLSourceDialect.keyset_key_members` read them). ORDER BY sorts those by the
    number MySQL stores (an ENUM's member index, a SET's bitmask) while a comparison with the
    label compares STRINGS, so ``> 'label'`` skipped or re-read rows at every page boundary
    of a member order that is not alphabetical, silently. An ENUM's term is the members
    declared after the cursor's, ``col IN (...)``, which the PK index serves as a range (by
    the index, ``col > 2`` is only a Filter); a key LED by an ENUM is read member by member
    instead (:meth:`MySQLSourceDialect.keyset_walk`). A SET's is its bitmask, ``CAST(col AS
    UNSIGNED) > :name_bits`` -- exact over all 64 bits, where ``col + 0`` is a DOUBLE that
    rounds past bit 53 and turns bit 63 negative -- which no index serves: a LEADING SET
    column re-reads the key from its start per page, a later one only the rows of its equal
    prefix. The equality terms keep the label (``col = :name``), which the index serves and
    which names one member.
    """
    if member is None:
        return f"{quoted} > :{name}"
    kind, labels = member
    if kind == "set":
        parts = str(value).split(",") if value != "" else []
        if not all(part in labels for part in parts):
            raise _unknown_member(column, kind)
        params[f"{name}_bits"] = sum(1 << labels.index(part) for part in set(parts))
        return f"CAST({quoted} AS UNSIGNED) > :{name}_bits"
    if value not in labels:
        raise _unknown_member(column, kind)
    after = labels[labels.index(value) + 1 :]
    if not after:
        return None
    in_names = [f"{name}_in_{j}" for j in range(len(after))]
    params.update(zip(in_names, after))
    return f"{quoted} IN ({', '.join(':' + n for n in in_names)})"


def _unknown_member(column: str, kind: str) -> Exception:
    """The read cannot place a key value its column's members (read live) do not hold."""
    from dsql_migrator.core.exporter import ExportError

    return ExportError(
        f"key column '{column}' holds {'an ENUM' if kind == 'enum' else 'a SET'} value that "
        "is not one of the members the source lists for it (information_schema.COLUMNS), so "
        "the read cannot place it in the key order and stops instead of loading the table "
        "with rows missing. Retry this table; if it stops again, check the column's "
        "definition and character set on the source."
    )


def is_single_precision_float(column_type: Optional[str]) -> bool:
    """Is ``column_type`` (as Step 1 recorded ``COLUMN_TYPE``) a MySQL single-precision
    ``FLOAT`` -- ``float``, ``float(M,D)``, with or without ``unsigned``? ``DOUBLE`` /
    ``REAL`` are not: MySQL writes ``FLOAT(p)`` as one of the two when it is created."""
    if not column_type or not column_type.strip():
        return False
    return column_type.strip().lower().split("(")[0].split()[0] == "float"


def _unreadable_members(column: str, why: str, remedy: str) -> Exception:
    """The read cannot write a cursor that keeps this key column's order."""
    from dsql_migrator.core.exporter import ExportError

    return ExportError(
        f"key column '{column}' {why}, so the read cannot write a cursor that keeps the key "
        f"order and stops instead of loading the table with rows missing. {remedy}"
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

    def keyset_key_members(
        self, connection: object, table: object
    ) -> "dict[str, tuple[str, tuple[str, ...]]]":
        # Only a key Step 1 (Evaluation) saw an ENUM / SET in costs the read (every other
        # table's cursor is the plain one, as before); then each key column is taken as the
        # catalog defines it NOW. An ENUM's values are in key order: '' (the invalid-value
        # index 0, sorted first) unless it is a declared member, then the members as
        # declared; a SET's are its members, bit i the i-th.
        from sqlalchemy import text

        from dsql_migrator.core.converter import mysql_enum_set_members

        primary_key = list(table.primary_key)  # type: ignore[attr-defined]
        recorded = {c.name: c.mysql_type for c in table.columns}  # type: ignore[attr-defined]
        if not any(_ENUM_OR_SET.match(recorded.get(c, "")) for c in primary_key):
            return {}
        schema, separator, name = table.name.partition(".")  # type: ignore[attr-defined]
        params = {"schema": schema, "name": name}
        sql = _KEY_COLUMN_TYPES_SQL.format(schema=":schema")
        if not (separator and schema and name):
            params = {"name": table.name}  # type: ignore[attr-defined]
            sql = _KEY_COLUMN_TYPES_SQL.format(schema="DATABASE()")
        rows = connection.execute(text(sql), params).fetchall()  # type: ignore[attr-defined]
        # MySQL column names are case-insensitive on every platform.
        live = {str(row[0]).lower(): str(row[1]) for row in rows}
        members: dict[str, tuple[str, tuple[str, ...]]] = {}
        for column in primary_key:
            column_type = live.get(column.lower())
            if column_type is None:
                raise _unreadable_members(
                    column,
                    "is not in information_schema.COLUMNS for the table",
                    "Check that the migration user can read the table's columns and that the "
                    "column still exists, then retry this table.",
                )
            if not _ENUM_OR_SET.match(column_type):
                continue
            try:
                kind, values = mysql_enum_set_members(column_type) or ("", ())
            except ValueError:
                kind, values = "", ()
            if _column_type_sql(kind, values) != kind + column_type[len(kind) :]:
                raise _unreadable_members(
                    column,
                    "has members that could not be read exactly from "
                    "information_schema.COLUMNS.COLUMN_TYPE (a member holds a character such as "
                    "NUL)",
                    "Rename that member on the source, then retry this table.",
                )
            if len(set(values)) != len(values):
                raise _unreadable_members(
                    column,
                    "names the same member twice (MySQL accepts that outside strict mode)",
                    "Make the members distinct on the source, then retry this table.",
                )
            if kind == "enum" and "" not in values:
                values = ("", *values)
            members[column] = (kind, values)
        return members

    def keyset_walk(
        self, table: object, members: "Mapping[str, tuple[str, tuple[str, ...]]]"
    ) -> "Optional[tuple[str, ...]]":
        # A key LED by an ENUM is read member by member. As one predicate -- the members
        # after the cursor's, OR the cursor's member and the rest of the key after it -- its
        # cursor is a range MySQL 8.4 / 9.4 do not take when the PK holds every column of
        # the table: (status ENUM, id) is then a covering index scan plus a filter that
        # re-reads every row before the cursor on each page, quadratic in the first
        # member's rows (300k rows: one page touched 100,000 rows for 5,000). A page of
        # ``status = :member [AND <the rest of the key after the cursor>]`` is a ref / range.
        primary_key = list(table.primary_key)  # type: ignore[attr-defined]
        lead = members.get(primary_key[0]) if primary_key else None
        return lead[1] if lead is not None and lead[0] == "enum" else None

    def keyset_after_sql(
        self,
        table: object,
        last_key: "Sequence[object]",
        members: "Mapping[str, tuple[str, tuple[str, ...]]]",
    ) -> "tuple[str, dict[str, object]]":
        # A composite key is the EXPLICIT lexicographic disjunction
        #   (k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR ...
        # rather than the row-value form ``(k0, k1, ...) > (:last_0, ...)``. Both are
        # lexicographically equivalent, but the row-value comparison only gets a PK index
        # range scan on MySQL 8.0.14+; on a 5.7-compatible source (RDS MySQL 5.7 / Aurora
        # MySQL 2 -- both supported) it degrades to a FULL TABLE SCAN per page, making the
        # whole keyset export O(n^2). The disjunction uses the PK index on every version. The
        # named params (:last_i) are REUSED across terms (one value per key column). A
        # one-column key is its strict term alone. A walked key (keyset_walk) is the
        # cursor's member AND the disjunction over the rest of the key.
        primary_key = list(table.primary_key)  # type: ignore[attr-defined]
        # A single-precision FLOAT key column has no cursor value that names its row: the
        # source returns the shortest decimal that rounds to the stored float (0.7 for
        # 0.699999988), the driver binds that decimal, and the comparison runs in double
        # precision. When the decimal lies above the stored value, ``f = :last`` matches
        # nothing and ``f > :last`` skips the cursor's value, so the rest of its rows were
        # never read, silently (measured: 10 of 20 rows read for (f FLOAT, n INT), no
        # error). Refused for every such key column, before any page after the first.
        recorded = {c.name.lower(): c.mysql_type for c in getattr(table, "columns", ())}
        for column in primary_key:
            if is_single_precision_float(recorded.get(column.lower())):
                raise _unreadable_members(
                    column,
                    "is a single-precision FLOAT, which the source returns rounded to its "
                    "shortest decimal: that value does not compare equal to the stored one",
                    "Change the column to DOUBLE or DECIMAL on the source, or give the table "
                    "another primary key, then retry this table.",
                )
        names = keyset_param_names(len(primary_key))
        params: dict[str, object] = dict(zip(names, last_key))
        walked = self.keyset_walk(table, members) is not None
        equal: list[str] = []
        terms: list[str] = []
        for position, (column, name, value) in enumerate(zip(primary_key, names, last_key)):
            quoted = self.quote_identifier(column)
            if not (walked and position == 0):
                after = _key_after_sql(quoted, column, members.get(column), name, value, params)
                if after is not None:
                    terms.append(" AND ".join([*equal, after]))
                equal.append(f"{quoted} = :{name}")
        if not terms:
            # Every key column (after a walked key's member) is an ENUM on its last member:
            # nothing follows.
            return "1 = 0", params
        if len(primary_key) == 1:
            return terms[0], params
        cursor = "(" + " OR ".join(f"({term})" for term in terms) + ")"
        if walked:
            cursor = f"{self.quote_identifier(primary_key[0])} = :{names[0]} AND {cursor}"
        return cursor, params

    @property
    def snapshot_start_sql(self) -> str:
        # MySQL: InnoDB REPEATABLE READ consistent snapshot (same as watermark capture). A
        # snapshot whatever the source's default isolation: see session_setup_sql.
        from dsql_migrator.core.watermark import START_CONSISTENT_SNAPSHOT

        return START_CONSISTENT_SNAPSHOT

    @property
    def session_setup_sql(self) -> tuple[str, ...]:
        # Every source session, not only the Full Load stream: the watermark capture and
        # Validation's per-table reads open the same consistent snapshot, and every other
        # read then runs as it does on a source left at MySQL's default isolation. The
        # session time zone stays in the driver's init_command. See _MYSQL_REPEATABLE_READ.
        return (_MYSQL_REPEATABLE_READ,)

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
        # expanded with USING; when they cannot be, nothing is claimed. How the grants'
        # database names match follows the server (case folding, partial revokes).
        from sqlalchemy import text

        try:
            roles = connection.execute(text("SELECT CURRENT_ROLE()")).scalar()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - MySQL 5.7 has no roles
            roles = None
        statement = "SHOW GRANTS"
        roles_active = bool(roles) and str(roles).strip().upper() != "NONE"
        if roles_active:
            statement = f"SHOW GRANTS FOR CURRENT_USER() USING {roles}"
        try:
            rows = connection.execute(text(statement)).fetchall()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - unknown: claim nothing
            return []
        return mysql_listing_gaps(
            [str(row[0]) for row in rows if row],
            databases,
            case_insensitive=probe_scalar(connection, "SELECT @@lower_case_table_names") not in (None, "0"),
            partial_revokes=probe_scalar(connection, "SELECT @@partial_revokes") in ("1", "ON"),
            roles_active=roles_active,
        )

    def read_next_key_values(self, connection: object, tables) -> "dict[str, int]":
        # information_schema.TABLES.AUTO_INCREMENT is the counter MySQL 8 persists; its
        # statistics are cached up to a day unless the session asks for them live
        # (information_schema_stats_expiry = 0, a session SET: MySQL 5.7 has no cache).
        from sqlalchemy import bindparam, text

        wanted = {t.name for t in tables if getattr(t, "auto_increment_column", None)}
        schemas = sorted({name.partition(".")[0] for name in wanted if "." in name})
        if not schemas:
            return {}
        try:
            connection.execute(text("SET SESSION information_schema_stats_expiry = 0"))  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - MySQL 5.7: always live
            pass
        try:
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, AUTO_INCREMENT "
                    "FROM information_schema.TABLES WHERE TABLE_SCHEMA IN :schemas"
                ).bindparams(bindparam("schemas", expanding=True)),
                {"schemas": schemas},
            ).fetchall()
        except Exception:  # noqa: BLE001 - best effort: no floor
            return {}
        out: dict[str, int] = {}
        for schema, table, counter in rows:
            name = f"{schema}.{table}"
            if name in wanted and counter is not None:
                out[name] = int(counter)
        return out

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


__all__ = ["MySQLSourceDialect", "is_single_precision_float"]


# One scope part: a backtick-quoted name (spaces, dots and doubled backticks allowed)
# or a bare one.
_SCOPE_PART = r"(?:`(?:[^`]|``)*`|[^.\s`]+)"
_GRANT_LINE = re.compile(
    r"^(?P<verb>GRANT|REVOKE)\s+(?P<privs>.+?)\s+ON\s+(?:(?:TABLE|FUNCTION|PROCEDURE)\s+)?"
    rf"(?P<db>{_SCOPE_PART})\.(?P<obj>{_SCOPE_PART})\s+(?:TO|FROM)\s",
    re.IGNORECASE | re.DOTALL,
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


def _unquote_scope(part: str) -> str:
    return part[1:-1].replace("``", "`") if part.startswith("`") else part


def _like(pattern: str, name: str, *, literal: bool = False) -> bool:
    """MySQL's grant-scope match: ``%`` and ``_`` wildcards, ``\\`` escapes. With
    ``literal`` (``partial_revokes`` on) both wildcards are plain characters."""
    out, escaped = [], False
    for char in pattern:
        if escaped:
            out.append(re.escape(char))
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "%" and not literal:
            out.append(".*")
        elif char == "_" and not literal:
            out.append(".")
        else:
            out.append(re.escape(char))
    return re.fullmatch("".join(out), name, re.DOTALL) is not None


def _scope_rank(pattern: str, *, literal: bool = False) -> int:
    """How specific a database-level grant is, as MySQL ranks them (``get_sort``): an
    exact name (128) outranks every pattern, and a pattern whose first wildcard comes
    later outranks one whose comes earlier (a lone ``%`` is the least specific)."""
    if literal:
        return 128
    i = 0
    while i < len(pattern):
        if pattern[i] == "\\" and i + 1 < len(pattern):
            i += 2
            continue
        if pattern[i] in "%_":
            return 1 if pattern == "%" else min(i + 2, 127)
        i += 1
    return 128


def mysql_listing_gaps(
    grant_lines: "list[str]",
    databases,
    *,
    case_insensitive: bool = False,
    partial_revokes: bool = False,
    roles_active: bool = False,
) -> list[str]:
    """The object kinds a MySQL user with ``grant_lines`` cannot list completely, in
    ``databases``: one human line per kind naming the databases and the privilege. Pure.

    ``grant_lines`` are the SESSION's: direct grants plus those of its active roles
    (``SHOW GRANTS ... USING``); a role that is granted but not active gives the session
    nothing, so its membership line (``GRANT `r`@`%` TO ...``, no ``ON``) is ignored. ``[]``
    when the grants are unknown (none read).

    MySQL's own rules decide what is held in a database: a global privilege counts unless
    a partial revoke (``REVOKE ... ON db.*``) takes it back there, and of the
    database-level grants only the most specific matching one applies -- they are not
    added up. When several patterns tie for most specific, which one MySQL picks is not
    observable (and active roles add them up instead), so only what they all grant is
    counted: an unsure answer names the gap rather than hiding it. ``case_insensitive``
    is ``lower_case_table_names`` != 0; ``partial_revokes`` makes ``%`` and ``_`` in a
    grant's database name plain characters, as MySQL then does. ``roles_active``: the
    session has active roles (the lines came from ``SHOW GRANTS ... USING``), and MySQL
    then checks a database against an exact-name grant if there is one and otherwise ADDS
    UP every matching pattern, instead of taking the most specific one."""
    if not grant_lines:
        return []

    def fold(name: str) -> str:
        return name.lower() if case_insensitive else name

    global_privs: set[str] = set()
    revoked: dict[str, set[str]] = {}
    schema_privs: dict[str, set[str]] = {}
    for line in grant_lines:
        match = _GRANT_LINE.match(line.strip())
        if match is None:
            continue
        privileges = set(_split_privileges(match.group("privs")))
        if "ALL" in privileges:
            privileges.add("ALL PRIVILEGES")
        if match.group("obj") != "*":
            continue  # table, column or routine level: not a whole database
        is_global = match.group("db") == "*"
        db = fold(_unquote_scope(match.group("db")))
        if match.group("verb").upper() == "REVOKE":
            if not is_global:  # a partial revoke of a global privilege
                revoked.setdefault(db, set()).update(privileges)
            continue
        if is_global:
            global_privs |= privileges
        else:
            schema_privs.setdefault(db, set()).update(privileges)

    def schema_grant(database: str) -> "set[str]":
        name = fold(database)
        ranked = [
            (_scope_rank(pattern, literal=partial_revokes), privileges)
            for pattern, privileges in schema_privs.items()
            if _like(pattern, name, literal=partial_revokes)
        ]
        if not ranked:
            return set()
        top = max(rank for rank, _ in ranked)
        if roles_active and top < 128:
            merged: set[str] = set()
            for _rank, privileges in ranked:
                merged |= privileges
            return merged
        tied = [privileges for rank, privileges in ranked if rank == top]
        narrower = [privileges for privileges in tied if "ALL PRIVILEGES" not in privileges]
        return set.intersection(*narrower) if narrower else {"ALL PRIVILEGES"}

    def held(database: str, wanted: "set[str]", global_only: "set[str]") -> bool:
        taken_back = revoked.get(fold(database), set())
        for privilege in wanted | global_only:
            if (privilege in global_privs or "ALL PRIVILEGES" in global_privs) and not (
                privilege in taken_back or "ALL PRIVILEGES" in taken_back
            ):
                return True
        return bool(schema_grant(database) & (wanted | {"ALL PRIVILEGES"}))

    checks = (
        ("triggers", "TRIGGER", {"TRIGGER"}, set()),
        ("scheduled events", "EVENT", {"EVENT"}, set()),
        (
            "stored procedures and functions",
            "SHOW_ROUTINE (or SELECT on *.*)",
            {"CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE"},
            {"SHOW_ROUTINE", "SELECT"},
        ),
    )
    gaps: list[str] = []
    for label, needs, schema_level, global_level in checks:
        missing = [db for db in databases if not held(db, schema_level, global_level)]
        if missing:
            shown = ", ".join(missing[:5]) + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
            gaps.append(f"{label} in {shown} (needs {needs})")
    return gaps
