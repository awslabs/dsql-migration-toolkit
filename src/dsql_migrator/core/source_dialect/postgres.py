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


def _not_extension_owned(classid: str, oid_expr: str) -> str:
    """SQL fragment excluding rows that belong to an installed EXTENSION.

    ``pg_depend`` records extension membership as ``deptype='e'`` (the PostgreSQL docs'
    ``DEPENDENCY_EXTENSION``: "the dependent object is a member of the extension that is
    the referenced object"), which is exactly the line ``pg_dump`` draws -- it emits an
    extension's objects as ``CREATE EXTENSION``, never as their own DDL. Matching it makes
    the tool's notion of "the user's objects" the same as pg_dump's.

    Parameterised by BOTH the catalog and the oid expression, not by ``classid`` alone,
    because a single fragment does not fit all three call sites:

    * ``('pg_proc', 'p.oid')`` / ``('pg_class', 'c.oid')`` test the object itself.
    * A TRIGGER must test its TABLE (``('pg_class', 'c.oid')``): a trigger created by an
      extension's install script is NOT recorded as an extension member -- verified against
      a real extension, ``pg_depend`` holds only ``'a'``->its table and ``'n'``->its
      function -- so a ``pg_trigger``-keyed test is silently always false. (Keying on the
      trigger's FUNCTION would be wrong the other way: a genuine USER trigger calling
      contrib ``moddatetime`` would vanish.)

    ``classid`` is required, not decorative: ``pg_depend.objid`` "references any OID
    column", so an object is identified by the PAIR (classid, objid) and OIDs are not
    unique across catalogs. Without it a user object whose OID collided with an
    extension member's would be silently dropped -- a MISSING finding, worse than the
    over-reporting this fixes.

    Both arguments are module-owned literals, never user input (the same
    interpolate-dialect-owned-SQL argument as ``base.estimate_row_counts_query``).
    """
    return (
        "AND NOT EXISTS (SELECT 1 FROM pg_depend d "
        f"WHERE d.classid = '{classid}'::regclass AND d.objid = {oid_expr} "
        "AND d.deptype = 'e') "
    )

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
    base = _base_type_name(type_string)
    if base in ("json", "jsonb") or base.startswith("interval"):
        return True
    # Date/time types, for the same reason one step further: psycopg loads them into
    # ``datetime``, which CANNOT represent values PostgreSQL stores happily --
    # ``infinity``/``-infinity``, a year past 9999, or a BC date. Those raise
    # ``psycopg.DataError`` ("timestamp too large (after year 10K)") on the READ, which is
    # not a per-row quarantine but an exception out of the streaming cursor: the whole
    # worker dies and the table fails, with a driver message that names no column.
    # Live-verified on PostgreSQL 16; the text cast reads all three faithfully, and the
    # session already pins ISO DateStyle + UTC so the text is unambiguous and re-parses
    # into the identical value on the target.
    return base in ("date", "timestamp", "timestamptz") or base.startswith("timestamp")


def _base_type_name(type_string: str) -> str:
    """The lower-cased type name with any precision/length modifier stripped."""
    return str(type_string or "").split("(", 1)[0].strip().lower()


# Target types that accept a value's canonical SOURCE TEXT verbatim. Reading a remodelled
# column as ``CAST(col AS text)`` and binding it as an unknown-typed literal (oid 0, which
# the server re-parses) is faithful for these -- the same mechanism json/interval already
# use. This is what makes the tool's own remodel advice ("inet -> text, its canonical
# address string round-trips losslessly", xml/tsvector/point/geometric -> text) actually
# work on the data path; without it the value arrived in the SOURCE type and the target
# rejected it, or -- worse -- accepted a wrong one.
_PG_TEXTUAL_TARGETS = frozenset(
    {"text", "varchar", "character varying", "char", "character", "citext"}
)


def _pg_read_expression(
    quoted: str, source_type: str, target_type: Optional[str]
) -> Optional[str]:
    """The SELECT expression for a column whose TARGET type differs from its source.

    ``None`` when no target-driven cast is needed (the caller falls back to the
    source-driven rule). Each case below exists because the operator was TOLD to remodel
    this way by ``converter_postgres._PG_UNSUPPORTED_REMODEL`` and the loader then had to
    be able to produce it:

    * -> a textual target: read the canonical source text.
    * ``money`` -> ``numeric``: psycopg returns money as a LOCALE-FORMATTED string
      (``'$12.34'``), which numeric rejects (22P02), so cast on the source instead.
    * an ARRAY -> ``jsonb``: the array's own text (``{a,b}``) is not JSON, so read
      ``to_jsonb(col)`` as text.
    """
    if not target_type:
        return None
    src = _base_type_name(source_type)
    tgt = _base_type_name(target_type)
    if src == tgt:
        return None
    if tgt in _PG_TEXTUAL_TARGETS:
        return f"CAST({quoted} AS text) AS {quoted}"
    if src == "money" and tgt in ("numeric", "decimal"):
        return f"CAST({quoted} AS numeric) AS {quoted}"
    if source_type.strip().endswith("[]") and tgt in ("jsonb", "json"):
        return f"CAST(to_jsonb({quoted}) AS text) AS {quoted}"
    return None

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


def _pg_drop_extension_relations(connection: object, nsp: str, tables: list) -> set:
    """DROP extension-owned tables from ``tables`` in place; return their names.

    The most consequential half of the extension problem, and the only one that is not
    merely report noise. ``get_table_names`` / ``get_view_names`` apply no extension filter,
    so an extension's OWN relations (PostGIS ``spatial_ref_sys`` / ``geometry_columns``,
    pg_partman's ``part_config``, a monitoring extension's tables) arrive as ordinary
    migratable tables -- and since the default selection is "all tables", Full Load
    actually WRITES their rows to the target, a view whose body calls a C function is
    handed to Schema Apply and fails there, and Validation then compares objects the user
    never created.

    Structured exactly like :func:`_pg_apply_partitioning` -- one ``pg_class`` read for the
    reflected schema, then a filter of the list the caller holds -- because it is the same
    kind of correction: an object the catalog reports that is not an independent migration
    unit. Returns the dropped names so the caller can prune ``views`` the same way.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            # The POSITIVE form of :func:`_not_extension_owned` -- spelled out rather than
            # derived from it, so what this executes is readable on its own.
            "SELECT c.relname AS relname "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_depend d ON d.classid = 'pg_class'::regclass "
            "  AND d.objid = c.oid AND d.deptype = 'e' "
            "WHERE n.nspname = :nsp"
        ),
        {"nsp": nsp},
    ).mappings()
    owned = {row["relname"] for row in rows}
    if not owned:
        return owned
    tables[:] = [table for table in tables if table.name not in owned]
    return owned


# pg_type.typtype -> the KIND name used in messages. 'b' (base/built-in) is deliberately
# absent: it is normalised to NULL in the query, since only a USER-DEFINED kind changes
# what the operator has to do. "enum" here names a KIND, not a type -- PostgreSQL has no
# type spelled ``enum``.
_PG_TYPE_KINDS = {
    "e": "enum",
    "c": "composite",
    "d": "domain",
    "r": "range",
    "m": "multirange",
}


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
                "a.attidentity AS ident, "
                # A NON-DEFAULT column collation. Aurora DSQL creates the column under
                # the target's default collation (the converter does not re-emit a
                # COLLATE clause), so a source column collated e.g. with a
                # case-insensitive ICU collation silently changes =, LIKE, ORDER BY and
                # UNIQUE semantics after cut over. 'default'/'C'/'POSIX' are excluded
                # because they already match the target, so they are not a change --
                # the same "only report an actual difference" rule the MySQL enricher's
                # _ci filter applies. NULL for a non-collatable type (attcollation = 0).
                "(SELECT cl.collname FROM pg_collation cl "
                " WHERE cl.oid = a.attcollation "
                "   AND cl.collname NOT IN ('default', 'C', 'POSIX')) AS coll "
                # A DOMAIN's format_type is the domain NAME, but PostgreSQL still
                # describes the result with the BASE type's OID (so psycopg applies the
                # base loader). Carry the base type so read decisions see the storage
                # type; NULL for a non-domain (typtype <> 'd').
                ", CASE WHEN t.typtype = 'd' THEN "
                "  format_type(t.typbasetype, a.atttypmod) END AS base_typ "
                # The generated column's EXPRESSION. attgenerated above only says a column
                # IS generated, so the rendered source DDL had to print
                # "/* expression not captured */" -- while the Evaluation finding told the
                # operator to "read the generating expression from the source, since it is
                # not carried over", which they could not do from inside the tool. The
                # expression lives in pg_attrdef like an ordinary default; it needs no
                # privilege beyond reading the catalog.
                ", CASE WHEN a.attgenerated <> '' THEN "
                "  pg_get_expr(ad.adbin, ad.adrelid) END AS gen_expr "
                # WHICH KIND of user-defined type, and (for an enum) its labels. Without
                # these, format_type's bare NAME made an enum, a composite and a domain
                # indistinguishable, so every message hedged across all three -- and a
                # DOMAIN was reported UNSUPPORTED even though Aurora DSQL supports
                # CREATE DOMAIN. 'b' (base) is normalised to NULL so only a user-defined
                # kind is carried.
                ", NULLIF(t.typtype, 'b') AS typkind "
                ", CASE WHEN t.typtype = 'e' THEN ("
                "    SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) "
                "    FROM pg_enum e WHERE e.enumtypid = t.oid) END AS enum_labels "
                "FROM pg_attribute a "
                "JOIN pg_type t ON t.oid = a.atttypid "
                "JOIN pg_class c ON c.oid = a.attrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_attrdef ad "
                "  ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum "
                "WHERE c.relname = :rel AND a.attnum > 0 "
                f"AND NOT a.attisdropped {schema_filter}"
            ),
            params,
        ).mappings()
        exact = {
            row["col"]: (
                row["typ"], row.get("gen"), row.get("ident"), row.get("coll"),
                row.get("base_typ"), row.get("gen_expr"), row.get("typkind"),
                row.get("enum_labels"),
            )
            for row in rows
        }
        for column in table.columns:
            resolved = exact.get(column.name)
            if resolved:
                column.mysql_type = resolved[0]
                if resolved[1] in ("s", "v"):  # 's'=STORED, 'v'=VIRTUAL (PG18+)
                    column.generated = True
                    # The KIND decides the outcome and was being read and thrown away:
                    # Aurora DSQL supports a STORED generated column (and maintains it),
                    # but rejects VIRTUAL outright. Collapsing both into the boolean made
                    # the tool treat the supported case as the unsupported one.
                    column.generated_kind = "STORED" if resolved[1] == "s" else "VIRTUAL"
                    # Carry the expression: for a STORED column it is re-emitted into the
                    # target DDL so DSQL keeps computing the value, and for any kind it is
                    # what the source panel shows.
                    if len(resolved) > 5 and resolved[5]:
                        column.generated_expression = str(resolved[5])
                # 'a'=GENERATED ALWAYS, 'd'=GENERATED BY DEFAULT AS IDENTITY. Recorded for
                # EVERY column, not only the key one resolved below: an identity column has
                # no pg_attrdef default, so a value-generation check keyed on ``default``
                # cannot see it, and a NON-key identity column would lose its generation
                # with nothing said.
                if resolved[2] in ("a", "d"):
                    column.identity = True
                    # Keep WHICH spelling. It was read and dropped, so the rendered source
                    # DDL had to omit the clause entirely -- and ALWAYS vs BY DEFAULT is a
                    # real behavioural difference (ALWAYS rejects an explicit INSERT value
                    # without OVERRIDING SYSTEM VALUE).
                    column.identity_generation = (
                        "ALWAYS" if resolved[2] == "a" else "BY DEFAULT"
                    )
                # SQLAlchemy reflection does not supply a collation, so this field was
                # None for every PostgreSQL column and the collation warning could never
                # fire for a PG source.
                if resolved[3]:
                    column.collation = resolved[3]
                # A DOMAIN column: record the storage type so select_column_sql's
                # text-cast rule matches on it rather than on the domain's own name.
                if len(resolved) > 4 and resolved[4]:
                    column.base_type = str(resolved[4])
                # The type's KIND and, for an enum, its labels. Both are read from the
                # pg_type join that was already there; only the values were missing.
                if len(resolved) > 6 and resolved[6]:
                    column.type_kind = _PG_TYPE_KINDS.get(str(resolved[6]))
                if len(resolved) > 7 and resolved[7]:
                    column.enum_labels = tuple(str(label) for label in resolved[7])
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

    ``tgisinternal`` alone is NOT enough: it marks only SYSTEM-generated constraint
    triggers, so a trigger an extension creates in its install script has
    ``tgisinternal = false`` and was reported as the user's. Filtered via its TABLE, since
    the trigger itself carries no extension-membership row -- see
    :func:`_not_extension_owned`. A trigger an extension puts on a USER table is still
    reported, which is right: that one really does have no DSQL target.

    The name carries its TABLE for exactly the reason the routine collector carries identity
    arguments: a PostgreSQL trigger name is unique only per table (``pg_trigger`` is keyed on
    ``(tgrelid, tgname)``), so ``tgname`` alone produced rows the report could not tell
    apart -- and because the findings bucket is keyed by object name, each duplicate row also
    repeated every sibling's concerns. A conventional name like ``set_updated_at`` on twelve
    tables rendered as twelve identical rows, none of which said which table to reimplement
    the logic for -- the one fact the operator needs.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT c.relname || '.' || t.tgname AS name FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE NOT t.tgisinternal AND n.nspname = :nsp "
            + _not_extension_owned("pg_class", "c.oid")
            + "ORDER BY c.relname, t.tgname"
        ),
        {"nsp": nsp},
    ).mappings()
    return [ObjectRef(name=row["name"], object_type=ObjectType.TRIGGER) for row in rows]


def _pg_collect_routines(connection: object, nsp: str) -> list:
    """Read stored functions/procedures for the schema (mirrors ``collect_routines``).

    ``prokind`` distinguishes a procedure ('p') from a function ('f'); an aggregate ('a')
    or window ('w') routine is reported as a FUNCTION (DSQL supports none of them). Bare
    ObjectRefs; the caller qualifies. ProcedureRule flags each UNSUPPORTED.

    EXTENSION-owned routines are excluded (see :func:`_not_extension_owned`). Without that
    filter a single ``CREATE EXTENSION pgcrypto`` -- which defaults to ``public``, a schema
    the tool always sweeps -- added 36 ``UNSUPPORTED / SIGNIFICANT`` findings advising the
    operator to "reimplement as a LANGUAGE SQL function" a C function they did not write
    and cannot reimplement, and swung the readiness score of a one-table database from
    57/100 to 2/100. The filter is OBJECT-level, so a user function that merely LIVES in an
    extension's schema is still reported.

    The name carries the IDENTITY ARGUMENTS because PostgreSQL allows overloading, and
    ``proname`` alone produced rows the report could not tell apart -- worse than N
    duplicates, since the findings bucket is keyed by object name, so each duplicate row
    repeated every sibling's concerns (3 overloads rendered as 9 table rows).
    ``pg_get_function_identity_arguments`` needs no privilege beyond reading ``pg_proc``
    and renders a zero-argument routine as the idiomatic ``name()``.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT p.proname || '(' "
            "|| pg_get_function_identity_arguments(p.oid) || ')' AS name, "
            "p.prokind AS kind FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = :nsp "
            + _not_extension_owned("pg_proc", "p.oid")
            + "ORDER BY p.proname, 1"
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
        self,
        connection: object,
        enrich_db: str,
        tables: list,
        views: "Optional[list]" = None,
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

        # (1b) DROP the extension's OWN relations. Unlike everything else here this is not
        # report cosmetics: with the default select-all they would be LOADED into the
        # target, and a view whose body calls a C function would fail at Schema Apply.
        owned = _pg_drop_extension_relations(connection, enrich_db, tables)
        if views is not None and owned:
            views[:] = [view for view in views if view.name not in owned]

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
                "SELECT c.relname AS name, c.relkind AS relkind, "
                # The matview's DEFINITION. Without it the UNSUPPORTED finding said
                # "reimplement it as a plain table that the application refreshes" with
                # nothing to reimplement FROM -- the one artifact the operator needs was a
                # single catalog call away. A foreign table has no viewdef (NULL), which is
                # correct: its definition is an external server/options, not a query.
                "CASE WHEN c.relkind = 'm' THEN pg_get_viewdef(c.oid, true) END AS defn "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :nsp AND c.relkind IN ('m', 'f') "
                # An extension's own matview / foreign table is not the user's object to
                # re-create; the extension itself is reported once instead
                # (PgExtensionRule), so the information is kept without N bogus rows.
                + _not_extension_owned("pg_class", "c.oid")
                + "ORDER BY c.relname"
            ),
            {"nsp": enrich_db},
        ).mappings()
        label = {"m": "materialized view", "f": "foreign table"}
        out: list[ViewDef] = []
        for row in rows:
            kind = label.get(str(row.get("relkind")))
            if kind is None:
                continue
            out.append(
                ViewDef(
                    name=row["name"],
                    unsupported_kind=kind,
                    definition=str(row.get("defn") or ""),
                )
            )
        return out

    def list_schemas(self, connection: object) -> Optional[list[str]]:
        # SQLAlchemy's PG get_schema_names() filters `nspname NOT LIKE 'pg_%'` with an
        # UNESCAPED underscore, so `_` matches ANY single char and it wrongly drops user
        # schemas like `pgapp`/`pgdata`/`pghero` (they migrate silently to nothing).
        # Enumerate directly and ESCAPE the underscore so ONLY real system/temp schemas
        # (pg_catalog, pg_toast, pg_temp_*, pg_toast_temp_*) are excluded; the caller
        # then subtracts system_schemas (drops information_schema). Keeps `pgapp`.
        # A schema an EXTENSION created is deliberately NOT excluded here. Filtering it
        # looked like belt-and-braces on top of the per-object filters and was in fact
        # strictly coarser than them: a schema is a CONTAINER, so dropping it also drops
        # whatever the USER put inside it -- and people do (PostGIS's tiger_data holds the
        # loader's tables; teams add their own objects to topology/cron). That table then
        # vanished from Evaluation, could not even be listed for Full Load, and Validation
        # reported MATCH over a set that silently excluded it, with no finding to explain
        # any of it. The per-object ``deptype='e'`` filters already empty such a schema of
        # the extension's OWN objects while keeping the user's, which is the whole point of
        # filtering by object; and the rule this repo applies to itself -- a MISSING finding
        # is strictly worse than an over-reported one -- condemns the coarser layer.
        rows = connection.execute(  # type: ignore[attr-defined]
            text(
                r"SELECT n.nspname FROM pg_catalog.pg_namespace n "
                r"WHERE n.nspname NOT LIKE 'pg\_%' ESCAPE '\' "
                r"ORDER BY n.nspname"
            )
        ).mappings()
        return [row["nspname"] for row in rows]

    def database_collation(self, connection: object) -> "Optional[str]":
        # Best-effort: a failure returns None so introspection is never broken by a report
        # line. ``datcollate`` is what an unqualified text column actually sorts by.
        try:
            row = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT datcollate FROM pg_database "
                    "WHERE datname = current_database()"
                )
            ).first()
            return str(row[0]) if row and row[0] else None
        except Exception:  # noqa: BLE001 - best-effort; never break introspection
            return None

    def sibling_databases(self, connection: object) -> "list[str]":
        # The other connectable, non-template databases on this server. Read from the same
        # catalog ``database_collation`` above already uses, so this costs one extra cheap
        # query and no new privilege. Best-effort: a failure returns none rather than
        # breaking introspection over a hint.
        try:
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT datname FROM pg_database "
                    "WHERE datallowconn AND NOT datistemplate "
                    "  AND datname <> current_database() "
                    "ORDER BY datname"
                )
            ).mappings()
            return [str(row["datname"]) for row in rows]
        except Exception:  # noqa: BLE001 - best-effort; never break introspection
            return []

    def list_extensions(self, connection: object) -> "list[str]":
        # ``plpgsql`` is excluded: it is installed in every PostgreSQL database by default
        # and is not something the operator chose. Best-effort -- a failure returns none
        # rather than breaking introspection, since this feeds a report line, not the
        # migration itself.
        try:
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT e.extname AS name, n.nspname AS nsp "
                    "FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname <> 'plpgsql' ORDER BY e.extname"
                )
            ).mappings()
            return [f"{row['name']} ({row['nsp']})" for row in rows]
        except Exception:  # noqa: BLE001 - best-effort; never break introspection
            return []

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

    def select_column_sql(
        self, column: object, *, target_type: Optional[str] = None
    ) -> str:
        # Most columns read as-is (quoted). json/jsonb/interval are read via a text cast so
        # Full Load streams their EXACT text and binds it back to the identical target
        # column as an unknown-typed literal (oid 0), which the server re-parses --
        # faithful AND fast (see _reads_as_text for why the native psycopg round trip is
        # lossy/parse-heavy for these). json/jsonb can't be a PK; an interval PK reads as
        # this same-name text CAST, and the keyset WHERE boundary rebinds fine (``"col" >
        # :last`` coerces the text `:last` back to interval), but gapless pagination ALSO
        # requires the exporter's ORDER BY to reference the NATIVE column -- a bare
        # ``ORDER BY "col"`` would resolve to THIS text-cast output alias and sort by text
        # order while WHERE advances by native interval, so the exporter table-qualifies
        # the PK in ORDER BY (see keyset_stream) to keep both orderings native and gapless.
        # PostGIS geometry is out of scope (no ST_AsBinary-style case) for a first release.
        quoted = self.quote_identifier(column.name)  # type: ignore[attr-defined]
        # Match on the STORAGE type. A DOMAIN's format_type is the domain's own NAME, so
        # matching ``mysql_type`` alone missed a domain over jsonb/interval -- while
        # PostgreSQL still describes the result with the BASE type's OID, so psycopg
        # applied the base loader and the exact loss this cast exists to prevent happened
        # silently: a JSON literal ``null`` became SQL NULL, and ``1 mon`` became
        # ``30 days``. ``base_type`` is set only for a domain (see ColumnDef.base_type).
        declared = getattr(column, "mysql_type", "")  # type: ignore[attr-defined]
        storage = getattr(column, "base_type", None) or declared
        # The APPLIED target type wins when it differs: the operator may have remodelled a
        # DSQL-unsupported type on the tool's own advice, and until this consulted
        # ``target_type`` that advice was unimplementable on the data path -- the value
        # arrived in the SOURCE type, so money->numeric and array->jsonb were rejected with
        # an opaque driver error and bit->bytea SILENTLY stored the ASCII digits of the bit
        # string instead of the bits.
        retyped = _pg_read_expression(quoted, storage, target_type)
        if retyped is not None:
            return retyped
        if _reads_as_text(declared) or _reads_as_text(storage):
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
            # A partitioned PARENT stores 0/-1 in its own reltuples -- the rows live in the
            # partitions -- so reading it alone left the estimate UNKNOWN for exactly the
            # tables most likely to be huge, and the load panel showed no denominator and no
            # ETA for hours. Summing the LEAF partitions is still catalog-only (no scan).
            # FILTER, not COALESCE/GREATEST: when NO leaf has stats the sum stays NULL ->
            # None ("unknown"), rather than collapsing to a confident and wrong 0.
            estimate_column=(
                "CASE WHEN c.relkind = 'p' THEN ("
                "  SELECT (sum(l.reltuples) FILTER (WHERE l.reltuples >= 0))::bigint"
                "  FROM pg_catalog.pg_partition_tree(c.oid) t"
                "  JOIN pg_catalog.pg_class l ON l.oid = t.relid"
                "  WHERE t.isleaf"
                ") ELSE c.reltuples::bigint END"
            ),
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

    def probe_cdc_prerequisites(
        self, connection: object, table_names, *, publication_name="", slot_name=""
    ):
        # Gather the PostgreSQL CDC logical-replication readiness facts read-only and
        # best-effort (each field None/False on any failure, so an under-privileged
        # source degrades to "unknown" rather than erroring the gate). All plain SHOW /
        # SELECT on system catalogs, so it passes the read-only guard.
        from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

        # ROLL BACK after a failed statement. All ~13 statements share one connection,
        # hence one implicit transaction: swallowing the exception without a rollback left
        # the transaction ABORTED, so every LATER statement failed with 25P02 and silently
        # became None too. One unreadable catalog therefore blanked every fact after it --
        # and because the object returned is still a PostgresCdcFacts (not None), the
        # blocking "readiness could not be verified" FAIL could not engage: a report that
        # had correctly BLOCKED on a REPLICA IDENTITY of 'nothing' flipped to
        # can_proceed=True with nothing actually verified. Now a failure blanks only its
        # own fact. (Verified: with pg_class revoked, statements after it succeed on their
        # own connection, so the Nones were purely transaction poisoning.)
        def _rollback() -> None:
            try:
                connection.rollback()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - best-effort probe
                pass

        def _scalar(sql: str, params=None):
            try:
                return connection.execute(  # type: ignore[attr-defined]
                    text(sql), params or {}
                ).scalar()
            except Exception:  # noqa: BLE001 - best-effort probe
                _rollback()
                return None

        def _bool_or_none(value):
            # None must stay None: "not read" is not "absent". A defaulted False here
            # would assert an absence nobody verified and block the deploy on it.
            return None if value is None else bool(value)

        def _rows(sql: str, params=None):
            try:
                return connection.execute(  # type: ignore[attr-defined]
                    text(sql), params or {}
                ).fetchall()
            except Exception:  # noqa: BLE001 - best-effort probe
                _rollback()
                return None

        def _bool(value):
            """Tri-state: None stays None (unread), anything else becomes a real bool."""
            return None if value is None else bool(value)

        def _int(value):
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        wal_level = _scalar("SHOW wal_level")
        _super_raw = _scalar("SELECT current_setting('is_superuser')")
        is_super = (
            None if _super_raw is None else str(_super_raw).strip().lower() == "on"
        )
        # REPLICATION role attribute (self-managed) OR rds_replication membership
        # (RDS/Aurora, where the attribute cannot be granted). The CASE guards
        # pg_has_role against a non-existent rds_replication role (self-managed).
        repl_attr = _bool(
            _scalar("SELECT rolreplication FROM pg_roles WHERE rolname = current_user")
        )
        rds_member = _bool(
            _scalar(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE "
                "rolname = 'rds_replication') THEN pg_has_role(current_user, "
                "'rds_replication', 'MEMBER') ELSE false END"
            )
        )
        has_repl_role = (
            None
            if repl_attr is None and rds_member is None
            else bool(repl_attr) or bool(rds_member)
        )
        # Counting walsenders needs to SEE other backends: outside pg_monitor (and without
        # superuser) pg_stat_activity masks backend_type for every row but your own, so the
        # count came back 0 and the exhaustion WARN was dead in the least-privilege setup
        # the tool recommends. Only trust the count when the connection can actually see it.
        can_see_backends = bool(is_super) or bool(
            _scalar("SELECT pg_has_role(current_user, 'pg_monitor', 'MEMBER')")
        )
        identity: dict[str, str] = {}
        leaf_identity: dict[str, dict[str, str]] = {}
        identity_index_valid: dict[str, bool] = {}
        identity_index_is_primary: dict[str, bool] = {}
        unlogged: dict[str, bool] = {}
        not_owned: list[str] = []
        names = list(table_names)
        if names:
            # One catalog read per concern, each isolated by _rows so an unreadable one
            # cannot blank the others. All catalog-only: no table scan, no lock.
            rows = _rows(
                "SELECT n.nspname || '.' || c.relname AS qname, c.relreplident, "
                "c.relpersistence, pg_catalog.pg_get_userbyid(c.relowner) AS owner, "
                "EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid "
                "        AND i.indisreplident AND i.indisvalid) AS has_identity_index, "
                "EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid "
                "        AND i.indisreplident AND i.indisvalid "
                "        AND i.indisprimary) AS identity_index_is_pk "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname || '.' || c.relname = ANY(:names)",
                {"names": names},
            )
            for r in rows or ():
                qname = str(r[0])
                identity[qname] = str(r[1])
                unlogged[qname] = str(r[2]) == "u"
                identity_index_valid[qname] = bool(r[4])
                identity_index_is_primary[qname] = bool(r[5])
            # Ownership of every published table is required for CREATE PUBLICATION.
            owned = _rows(
                "SELECT n.nspname || '.' || c.relname AS qname "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname || '.' || c.relname = ANY(:names) "
                "  AND NOT pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE')",
                {"names": names},
            )
            if owned is not None:
                not_owned = sorted(str(r[0]) for r in owned)
            # Leaf partitions of any selected PARTITIONED parent. pg_partition_tree gives
            # the whole tree (PG12+); isleaf picks the relations PostgreSQL actually
            # publishes and whose identity it enforces.
            leaves = _rows(
                "SELECT pn.nspname || '.' || p.relname AS parent, "
                "       ln.nspname || '.' || l.relname AS leaf, l.relreplident "
                "FROM pg_class p "
                "JOIN pg_namespace pn ON pn.oid = p.relnamespace "
                "JOIN LATERAL pg_partition_tree(p.oid) t ON t.isleaf "
                "JOIN pg_class l ON l.oid = t.relid "
                "JOIN pg_namespace ln ON ln.oid = l.relnamespace "
                "WHERE p.relkind = 'p' "
                "  AND pn.nspname || '.' || p.relname = ANY(:names)",
                {"names": names},
            )
            for r in leaves or ():
                leaf_identity.setdefault(str(r[0]), {})[str(r[1])] = str(r[2])
        return PostgresCdcFacts(
            wal_level=str(wal_level) if wal_level is not None else None,
            is_superuser=is_super,
            has_replication_role=has_repl_role,
            max_replication_slots=_int(_scalar("SHOW max_replication_slots")),
            used_replication_slots=_int(
                _scalar("SELECT count(*) FROM pg_replication_slots")
            ),
            max_wal_senders=_int(_scalar("SHOW max_wal_senders")),
            # Active walsender backends (read replicas / other CDC + our own). A full pool
            # means no sender for a new CDC slot even when slot entries are free. Only read
            # when this connection can see other backends (see can_see_backends): a masked
            # count of 0 would claim headroom that may not exist.
            used_wal_senders=(
                _int(
                    _scalar(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE backend_type = 'walsender'"
                    )
                )
                if can_see_backends
                else None
            ),
            is_in_recovery=_bool(_scalar("SELECT pg_is_in_recovery()")),
            replica_identity=identity,
            leaf_replica_identity=leaf_identity,
            identity_index_valid=identity_index_valid,
            identity_index_is_primary=identity_index_is_primary,
            unlogged=unlogged,
            has_database_create=_bool(
                _scalar(
                    "SELECT pg_catalog.has_database_privilege("
                    "current_user, current_database(), 'CREATE')"
                )
            ),
            tables_not_owned=tuple(not_owned),
            # Read from pg_settings, NOT SHOW: SHOW renders a unit-suffixed string ("1GB",
            # "-1") that would need unit parsing, whereas pg_settings.setting is the raw
            # number in pg_settings.unit. The unit is always MB for this GUC, so the value
            # is used as MB directly. PG13+; on an older server the row is absent -> None
            # -> the check reports "unknown" instead of a false alarm.
            max_slot_wal_keep_size_mb=_int(
                _scalar(
                    "SELECT setting FROM pg_settings "
                    "WHERE name = 'max_slot_wal_keep_size'"
                )
            ),
            # Do CDC's objects EXIST? Only asked when the caller knows their names, i.e.
            # when this run must FIND them rather than create them. Each read goes through
            # _scalar/_rows, so one unreadable catalog blanks only its OWN fact instead of
            # poisoning the transaction and nulling every later one. The slot match is
            # scoped to a logical pgoutput slot in THIS database: pg_replication_slots is
            # cluster-wide, so a bare name match can hit a slot the connector cannot use.
            **(
                {
                    "checked_publication_name": publication_name,
                    "checked_slot_name": slot_name,
                    "publication_present": _bool_or_none(
                        _scalar(
                            "SELECT count(*) > 0 FROM pg_publication "
                            "WHERE pubname = :p",
                            {"p": publication_name},
                        )
                    ),
                    "publication_publishes_all_dml": _bool_or_none(
                        _scalar(
                            "SELECT bool_and(pubinsert AND pubupdate AND pubdelete) "
                            "FROM pg_publication WHERE pubname = :p",
                            {"p": publication_name},
                        )
                    ),
                    "publication_tables": tuple(
                        str(r[0])
                        for r in (
                            _rows(
                                "SELECT schemaname || '.' || tablename "
                                "FROM pg_publication_tables WHERE pubname = :p",
                                {"p": publication_name},
                            )
                            or ()
                        )
                    ),
                    "slot_present_any_database": _bool_or_none(
                        _scalar(
                            "SELECT count(*) > 0 FROM pg_replication_slots "
                            "WHERE slot_name = :s",
                            {"s": slot_name},
                        )
                    ),
                    "slot_usable": _bool_or_none(
                        _scalar(
                            "SELECT count(*) > 0 FROM pg_replication_slots "
                            "WHERE slot_name = :s AND slot_type = 'logical' "
                            "AND plugin = 'pgoutput' AND database = current_database() "
                            # Must match read_pg_replication_objects exactly, or a `lost`
                            # slot passes the PREREQUISITE while the runtime probe blocks.
                            "AND coalesce(wal_status, 'reserved') <> 'lost'",
                            {"s": slot_name},
                        )
                    ),
                }
                if publication_name and slot_name
                else {}
            ),
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
