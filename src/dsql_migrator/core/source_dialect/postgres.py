# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``PostgresSourceDialect`` -- RDS/Aurora PostgreSQL source-reading behavior.

The migration TARGET is Aurora DSQL (PostgreSQL-16 wire), so a PostgreSQL source is
near-identity: psycopg driver, double-quote identifiers, a REPEATABLE READ snapshot,
psycopg-native values. It backs the full journey (Evaluation, Schema Conversion, Full
Load, Validation, and CDC): ``enrich`` reads the PG catalog for exact column types
(``format_type``) and the STORED-generated flag, and ``value_converter`` returns the
pass-through :class:`PostgresValueConverter`.
"""

from __future__ import annotations

import re
from typing import Optional

from sqlalchemy import text

from dsql_migrator.core.introspector import SOURCE_CONNECT_TIMEOUT_SECONDS
from dsql_migrator.core.models import ObjectRef, ObjectType, SourceType, ViewDef
from dsql_migrator.core.source_dialect.base import (
    SourceDialect,
    SourceVersions,
    estimate_row_counts_query,
    probe_scalar,
)

# User schemas to restrict to when resolving a bare (unqualified) table's columns and no
# reflected schema is available -- the fallback path in _pg_enrich_columns.
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


def _pg_apply_partitioning(connection: object, nsp: str, tables: list) -> None:
    """Mark partitioned parents and DROP partition children from ``tables`` in place.

    ``get_table_names`` returns both the partitioned parent (relkind 'p') and every
    declarative partition child (``relispartition``) as independent tables. The parent's
    ``SELECT *`` already returns all partitions' rows, so keeping the children would
    migrate their data twice. Reads only ``pg_class`` for the reflected schema: a
    parent -> ``partitioned=True`` (PartitionedTableRule fires); a child (leaf or
    intermediate) -> removed. ``relispartition`` is precise for DECLARATIVE partitioning
    (never true for classic INHERITS children), so ordinary inherited tables are kept.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT c.relname AS relname, c.relkind AS relkind, "
            "c.relispartition AS is_partition "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :nsp AND c.relkind IN ('r', 'p')"
        ),
        {"nsp": nsp},
    ).mappings()
    parents: set[str] = set()
    children: set[str] = set()
    for row in rows:
        name = row["relname"]
        if str(row.get("relkind")) == "p":
            parents.add(name)
        if row.get("is_partition"):
            children.add(name)
    kept = []
    for table in tables:
        if table.name in children:
            continue  # migrated as part of its partitioned parent
        if table.name in parents:
            table.partitioned = True
        kept.append(table)
    tables[:] = kept


def _pg_enrich_columns(connection: object, enrich_db: str, tables: list) -> None:
    """Overwrite each column's type with the EXACT ``format_type`` string + generated flag,
    and flag a serial/identity PRIMARY-KEY column as the table's ``auto_increment_column``.

    Scoped to the reflected schema (``enrich_db``) so a same-named table in another schema
    cannot bleed its column types/flags in (the last-wins bug). A name-embedded schema is
    used only as a FALLBACK when ``enrich_db`` is falsy; if neither is available (a bare
    name with no schema at all) it restricts to user schemas. A column absent from the
    catalog result is left unchanged.

    Setting ``auto_increment_column`` is what makes the converter's primary-key strategy
    apply to a PostgreSQL serial/identity key (MySQL sets it on its AUTO_INCREMENT column):
    without it a ``serial``/``GENERATED ... AS IDENTITY`` key silently migrated as a plain
    integer with no auto-generation and no hot-partition RECOMMENDATION. Detected on a PK
    column via either a ``nextval(...)`` DEFAULT (serial) or ``pg_attribute.attidentity``
    ('a' = GENERATED ALWAYS, 'd' = GENERATED BY DEFAULT); the first such PK column wins.
    """
    for table in tables:
        schema, _, bare = table.name.rpartition(".")
        nsp = enrich_db or schema
        params: dict[str, object] = {"rel": bare}
        if nsp:
            schema_filter = "AND n.nspname = :nsp"
            params["nsp"] = nsp
        else:
            schema_filter = f"AND n.nspname NOT IN {_PG_SYSTEM_SCHEMAS_SQL}"
        rows = connection.execute(  # type: ignore[attr-defined]
            text(
                "SELECT a.attname AS col, "
                "format_type(a.atttypid, a.atttypmod) AS typ, "
                # attgenerated: 's' = STORED generated column, 'v' = VIRTUAL generated
                # column (new in PG18, its DEFAULT kind for a keyword-less GENERATED
                # ALWAYS AS (expr)), '' = ordinary. DSQL has neither, so the converter
                # warns and creates it ordinary -- see _pg_generated_column_warning.
                "a.attgenerated AS gen, "
                # attidentity: 'a' = GENERATED ALWAYS AS IDENTITY, 'd' = GENERATED BY
                # DEFAULT AS IDENTITY, '' = not an identity column. Used (with a nextval
                # DEFAULT for serial) to flag the identity PRIMARY-KEY column so the
                # primary-key strategy governs it (see auto_increment_column below).
                "a.attidentity AS ident "
                "FROM pg_attribute a "
                "JOIN pg_class c ON c.oid = a.attrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = :rel AND a.attnum > 0 "
                f"AND NOT a.attisdropped {schema_filter}"
            ),
            params,
        ).mappings()
        exact = {
            row["col"]: (row["typ"], row.get("gen"), row.get("ident")) for row in rows
        }
        for column in table.columns:
            resolved = exact.get(column.name)
            if resolved:
                column.mysql_type = resolved[0]
                if resolved[1] in ("s", "v"):  # 's'=STORED, 'v'=VIRTUAL (PG18+)
                    column.generated = True
        # A serial/identity PRIMARY-KEY column becomes the auto_increment_column so the
        # converter's primary-key strategy (IDENTITY / UUID / KEEP) applies to it.
        if table.auto_increment_column is None:
            primary_key = set(table.primary_key)
            for column in table.columns:
                if column.name not in primary_key:
                    continue
                identity_flag = exact.get(column.name, (None, None, None))[2]
                has_nextval = "nextval(" in (column.default or "").lower()
                if identity_flag in ("a", "d") or has_nextval:
                    table.auto_increment_column = column.name
                    break


def _pg_correct_fk_schemas(connection: object, nsp: str, tables: list) -> None:
    """Fix each FK's referenced-table qualification using the catalog (confrelid).

    SQLAlchemy returns ``referred_schema=None`` when the parent is visible via the
    search_path (commonly ``public``), and ``_reflect_tables`` then defaults that to the
    CHILD's schema -- so a cross-schema FK points at the wrong (child) schema. Resolve the
    real referenced schema+table from ``pg_constraint`` (search_path-independent) and
    rewrite ``referenced_table`` in place. Skipped entirely when no table has a foreign
    key, so it adds no query to the common case.
    """
    if not any(table.foreign_keys for table in tables):
        return
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT c.relname AS child, con.conname AS conname, "
            "refn.nspname AS ref_schema, refc.relname AS ref_table "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_class refc ON refc.oid = con.confrelid "
            "JOIN pg_namespace refn ON refn.oid = refc.relnamespace "
            "WHERE con.contype = 'f' AND n.nspname = :nsp"
        ),
        {"nsp": nsp},
    ).mappings()
    target: dict[tuple[str, str], str] = {}
    for row in rows:
        target[(row["child"], row["conname"])] = f"{row['ref_schema']}.{row['ref_table']}"
    for table in tables:
        _schema, _, bare = table.name.rpartition(".")
        for fk in table.foreign_keys:
            resolved = target.get((bare, fk.name))
            if resolved:
                fk.referenced_table = resolved


def _pg_collect_triggers(connection: object, nsp: str) -> list:
    """Read user trigger names for the schema (mirrors MySQL ``collect_triggers`` shape).

    Excludes internal triggers (``tgisinternal``, e.g. FK-enforcement triggers) and
    returns bare ``ObjectRef``s of type TRIGGER; the caller qualifies the names. Aurora
    DSQL has no trigger object, so TriggerRule flags each UNSUPPORTED.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT t.tgname AS name FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE NOT t.tgisinternal AND n.nspname = :nsp "
            "ORDER BY t.tgname"
        ),
        {"nsp": nsp},
    ).mappings()
    return [ObjectRef(name=row["name"], object_type=ObjectType.TRIGGER) for row in rows]


def _pg_collect_routines(connection: object, nsp: str) -> list:
    """Read stored functions/procedures for the schema (mirrors ``collect_routines``).

    ``prokind`` distinguishes a procedure ('p') from a function ('f'); an aggregate ('a')
    or window ('w') routine is reported as a FUNCTION (DSQL supports none of them). Bare
    ObjectRefs; the caller qualifies. ProcedureRule flags each UNSUPPORTED.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT p.proname AS name, p.prokind AS kind FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = :nsp ORDER BY p.proname"
        ),
        {"nsp": nsp},
    ).mappings()
    out: list[ObjectRef] = []
    for row in rows:
        object_type = (
            ObjectType.PROCEDURE if str(row.get("kind")) == "p" else ObjectType.FUNCTION
        )
        out.append(ObjectRef(name=row["name"], object_type=object_type))
    return out


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
        # PostgreSQL catalog enrichment for ONE reflected schema (``enrich_db``): at this
        # point every ``table.name`` is still BARE (the caller qualifies with the schema
        # AFTER enrich), so the schema to scope every catalog read to is ``enrich_db``, not
        # a name-embedded prefix. A non-PostgreSQL connection (e.g. the SQLite test double)
        # no-ops (mirrors the MySQL dialect's runtime guard).
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "postgresql":
            return ([], [], [])

        # (1) Partitioning: get_table_names returns the partitioned PARENT (relkind 'p')
        # AND each declarative partition child (relispartition) as independent tables.
        # Migrating both double-represents the data (the parent's SELECT * already returns
        # every partition's rows), so mark the parent ``partitioned`` (PartitionedTableRule)
        # and REMOVE the children from ``tables`` IN PLACE (the caller holds the same list).
        _pg_apply_partitioning(connection, enrich_db, tables)

        # (2) Exact column types + generated flag, scoped to THIS schema (``enrich_db``).
        # format_type keeps array element types (text[], not the lossy "ARRAY"),
        # timestamptz, precision, etc.; attgenerated flags a STORED ('s') / VIRTUAL ('v',
        # PG18+) generated column. Scoping to :nsp is what stops a multi-schema source with
        # same-named tables from bleeding another schema's column types/flags in (last-wins).
        _pg_enrich_columns(connection, enrich_db, tables)

        # (3) Correct each foreign key's REFERENCED schema from the catalog (confrelid):
        # SQLAlchemy reports referred_schema=None when the parent is search_path-visible
        # (commonly public), which _reflect_tables then mis-qualifies to the CHILD's schema.
        _pg_correct_fk_schemas(connection, enrich_db, tables)

        # (4) Stored triggers + routines (functions/procedures) for the schema, returned as
        # the ObjectRef shape the MySQL path uses so TriggerRule/ProcedureRule flag them
        # (DSQL has neither). Events stay empty -- PostgreSQL has no scheduled events.
        triggers = _pg_collect_triggers(connection, enrich_db)
        routines = _pg_collect_routines(connection, enrich_db)
        return (triggers, routines, [])

    def extra_relations(self, connection: object, enrich_db: str) -> list:
        # Materialized views (relkind 'm') and foreign tables (relkind 'f') for the schema.
        # Neither is returned by get_view_names / get_table_names, and Aurora DSQL supports
        # neither, so surface each as a flagged ViewDef (Evaluation reports it UNSUPPORTED)
        # rather than silently omitting it. PG-only (guarded); names are bare (qualified by
        # the caller). MySQL's Inspector.get_materialized_view_names raises, so this seam --
        # not a shared inspector call -- is why it must be gated to the PG dialect.
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "postgresql":
            return []
        rows = connection.execute(  # type: ignore[attr-defined]
            text(
                "SELECT c.relname AS name, c.relkind AS relkind "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :nsp AND c.relkind IN ('m', 'f') "
                "ORDER BY c.relname"
            ),
            {"nsp": enrich_db},
        ).mappings()
        label = {"m": "materialized view", "f": "foreign table"}
        out: list[ViewDef] = []
        for row in rows:
            kind = label.get(str(row.get("relkind")))
            if kind is None:
                continue
            out.append(ViewDef(name=row["name"], unsupported_kind=kind))
        return out

    def list_schemas(self, connection: object) -> Optional[list[str]]:
        # SQLAlchemy's PG get_schema_names() filters `nspname NOT LIKE 'pg_%'` with an
        # UNESCAPED underscore, so `_` matches ANY single char and it wrongly drops user
        # schemas like `pgapp`/`pgdata`/`pghero` (they migrate silently to nothing).
        # Enumerate directly and ESCAPE the underscore so ONLY real system/temp schemas
        # (pg_catalog, pg_toast, pg_temp_*, pg_toast_temp_*) are excluded; the caller
        # then subtracts system_schemas (drops information_schema). Keeps `pgapp`.
        rows = connection.execute(  # type: ignore[attr-defined]
            text(
                r"SELECT nspname FROM pg_catalog.pg_namespace "
                r"WHERE nspname NOT LIKE 'pg\_%' ESCAPE '\' ORDER BY nspname"
            )
        ).mappings()
        return [row["nspname"] for row in rows]

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

    @property
    def supports_shared_snapshot(self) -> bool:
        # PostgreSQL exports a snapshot so all shard readers observe one point-in-time cut,
        # making a range-sharded read consistent even on a live source with no CDC handoff.
        return True

    def export_snapshot_sql(self) -> str:
        # Run inside the anchor's REPEATABLE READ transaction (held open for the load); the
        # returned id is imported by each shard via set_transaction_snapshot_sql.
        return "SELECT pg_export_snapshot()"

    def set_transaction_snapshot_sql(self, snapshot_id: str) -> str:
        # The snapshot id cannot be a bind parameter (SET TRANSACTION SNAPSHOT takes a
        # literal), so validate it strictly before interpolating. pg_export_snapshot ids are
        # short tokens like "00000003-0000001B-1"; reject anything with quotes/whitespace/;.
        if not re.fullmatch(r"[0-9A-Za-z._-]+", snapshot_id or ""):
            raise ValueError(f"unexpected PostgreSQL exported-snapshot id: {snapshot_id!r}")
        return f"SET TRANSACTION SNAPSHOT '{snapshot_id}'"

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
        # non-superuser's table privileges come from information_schema.role_table_grants.
        # The grantee filter uses ``pg_has_role(current_user, grantee, 'USAGE')`` (plus
        # 'PUBLIC'), NOT ``grantee = current_user``: the Full Load connects as current_user
        # WITH inheritance, so a SELECT granted to a group role the user is a member of is
        # EFFECTIVE for it. The old current_user-only filter missed that and reported a
        # FALSE "SELECT missing" that blocked the load for a perfectly-privileged
        # role-based setup. pg_has_role(..., 'USAGE') also matches current_user itself, so
        # direct grants are still included. Scan-free (catalog metadata only). Coarse by
        # design -- grant presence, not per-migrated-table -- matching MySQL's SHOW GRANTS.
        # Best effort: any error yields [] (the check FAILs with remediation).
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
                    "WHERE grantee = 'PUBLIC' "
                    "OR pg_has_role(current_user, grantee, 'USAGE')"
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

    def probe_cdc_prerequisites(self, connection: object, table_names):
        # Gather the PostgreSQL CDC logical-replication readiness facts read-only and
        # best-effort (each field None/False on any failure, so an under-privileged
        # source degrades to "unknown" rather than erroring the gate). All plain SHOW /
        # SELECT on system catalogs, so it passes the read-only guard.
        from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

        def _scalar(sql: str, params=None):
            try:
                return connection.execute(  # type: ignore[attr-defined]
                    text(sql), params or {}
                ).scalar()
            except Exception:  # noqa: BLE001 - best-effort probe
                return None

        def _int(value):
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        wal_level = _scalar("SHOW wal_level")
        is_super = str(
            _scalar("SELECT current_setting('is_superuser')") or ""
        ).lower() == "on"
        # REPLICATION role attribute (self-managed) OR rds_replication membership
        # (RDS/Aurora, where the attribute cannot be granted). The CASE guards
        # pg_has_role against a non-existent rds_replication role (self-managed).
        repl_attr = bool(
            _scalar("SELECT rolreplication FROM pg_roles WHERE rolname = current_user")
        )
        rds_member = bool(
            _scalar(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE "
                "rolname = 'rds_replication') THEN pg_has_role(current_user, "
                "'rds_replication', 'MEMBER') ELSE false END"
            )
        )
        identity: dict[str, str] = {}
        names = list(table_names)
        if names:
            try:
                rows = connection.execute(  # type: ignore[attr-defined]
                    text(
                        "SELECT n.nspname || '.' || c.relname AS qname, "
                        "c.relreplident FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname || '.' || c.relname = ANY(:names)"
                    ),
                    {"names": names},
                ).fetchall()
                identity = {str(r[0]): str(r[1]) for r in rows}
            except Exception:  # noqa: BLE001 - best-effort probe
                identity = {}
        return PostgresCdcFacts(
            wal_level=str(wal_level) if wal_level is not None else None,
            is_superuser=is_super,
            has_replication_role=repl_attr or rds_member,
            max_replication_slots=_int(_scalar("SHOW max_replication_slots")),
            used_replication_slots=_int(
                _scalar("SELECT count(*) FROM pg_replication_slots")
            ),
            max_wal_senders=_int(_scalar("SHOW max_wal_senders")),
            # Active walsender backends (read replicas / other CDC + our own). A full pool
            # means no sender for a new CDC slot even when slot entries are free.
            used_wal_senders=_int(
                _scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender'"
                )
            ),
            is_in_recovery=bool(_scalar("SELECT pg_is_in_recovery()")),
            replica_identity=identity,
        )

    def read_replication_slot_health(self, connection: object, slot_name: str):
        # Read the slot's WAL-retention health from pg_replication_slots (a plain
        # SELECT -> passes the read-only guard). wal_status/safe_wal_size are PG13+; the
        # tool targets PG13-16, and this is best-effort (any failure -> None) so it never
        # disturbs the poll. 0 rows -> the slot does not exist (exists=False).
        from dsql_migrator.core.cdc_postgres import SlotHealth

        try:
            row = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT active, wal_status, safe_wal_size, "
                    "restart_lsn::text, confirmed_flush_lsn::text "
                    "FROM pg_replication_slots WHERE slot_name = :name"
                ),
                {"name": slot_name},
            ).first()
        except Exception:  # noqa: BLE001 - best-effort; never fail the poll
            return None
        if row is None:
            return SlotHealth(slot_name=slot_name, exists=False)

        def _int(value):
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        return SlotHealth(
            slot_name=slot_name,
            exists=True,
            active=bool(row[0]),
            wal_status=str(row[1]) if row[1] is not None else None,
            safe_wal_size=_int(row[2]),
            restart_lsn=str(row[3]) if row[3] is not None else None,
            confirmed_flush_lsn=str(row[4]) if row[4] is not None else None,
        )


__all__ = ["PostgresSourceDialect"]
