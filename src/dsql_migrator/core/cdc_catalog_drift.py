# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read the captured tables' live source definitions and diff them against the baseline.

CDC replicates row data, never DDL, and the sink rebuilds its SQL from each record, so a
source DDL change reaches Aurora DSQL only as a different row shape. Most shapes the target
cannot take are rejected and dead-lettered (the dead-letter banner), but several are applied
SILENTLY: a dropped nullable / DEFAULT column (inserts store NULL or the DEFAULT, updates keep
the old value), a widened numeric (rounded to the old scale), a MySQL ``tinyint(1)`` widened
to ``int`` on a boolean target (every non-zero value becomes true), a dropped or renamed
table (the target freezes, or a same-name table streams into it), a PostgreSQL column
Debezium cannot convert or a generated column (never sent). No rejected row, no signal.

This module is the engine-side half of the runtime re-check (``ui/cdc_catalog_drift_gate``):

* :func:`read_live_catalog` -- ONE catalog statement for every captured table (MySQL
  ``information_schema.COLUMNS`` + ``STATISTICS``; PostgreSQL ``pg_catalog``), names bound
  as parameters, never a data scan. It RAISES on failure (unreadable is not evidence).
* :func:`diff_table` -- pure: the baseline ``TableDef`` (the Step 1 inventory, plus columns
  added through Fix target schema) against the live read, skipping the columns CDC does not
  capture. It compares exactly what changes what CDC writes: existence, column names
  (case-sensitive, as the sink quotes them), the catalog type, the NULL rule and the primary
  key SET. Defaults, comments, collations, indexes and generated expressions are not
  compared (a change there does not change a CDC row).
* :func:`baseline_digest` -- a short digest of the same baseline facts, so a Step 1 re-run
  that silently changes what a table is compared with is noticed too.

Value-free (Property 7): names, catalog type text and key lists only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Mapping, NamedTuple, Optional, Sequence

from dsql_migrator.core.models import CdcCatalogDriftChange, SourceType

KIND_SOURCE_TABLE_MISSING = "source-table-missing"
KIND_KEY_CHANGE = "key-change"
KIND_TYPE_CHANGE = "type-change"
KIND_DROP_COLUMN = "drop-column"
KIND_RENAME_COLUMN = "rename-column"
KIND_NULLABILITY_CHANGE = "nullability-change"
KIND_ADD_COLUMN = "add-column"
# Never produced by the diff: the gate's fold records it when a Step 1 re-run changed the
# baseline of a table that matched the source (see ui/cdc_catalog_drift_gate).
KIND_DEFINITION_CHANGE = "definition-change"

# Most severe first: the order kinds are listed, labelled and explained in.
KIND_ORDER = (
    KIND_SOURCE_TABLE_MISSING,
    KIND_KEY_CHANGE,
    KIND_TYPE_CHANGE,
    KIND_DROP_COLUMN,
    KIND_RENAME_COLUMN,
    KIND_NULLABILITY_CHANGE,
    KIND_ADD_COLUMN,
    KIND_DEFINITION_CHANGE,
)

# How many changes a finding keeps (the rest are counted), and how much type text each holds.
CHANGE_LIMIT = 6
_TYPE_TEXT_LIMIT = 60

# MySQL's server-side bound on the one statement, below the 10 s PyMySQL socket timeout the
# read engine is built with, so the server stops the query before the client gives up on it
# (a socket timeout alone abandons only the client side). MySQL 5.7.8+ / 8.0; a server that
# does not know the hint treats it as a comment.
MYSQL_MAX_EXECUTION_MS = 8000


class LiveColumn(NamedTuple):
    """One live source column: name, catalog type text, NULL rule, generated, 0-based rank."""

    name: str
    type: str
    nullable: bool
    generated: bool
    position: int


class LiveTable(NamedTuple):
    """One captured table as the source catalog has it now."""

    columns: tuple
    primary_key: frozenset


@dataclass(frozen=True)
class CatalogDiff:
    """How one table's live definition differs from its baseline (bounded, value-free)."""

    kinds: tuple
    changes: tuple
    more_changes: int = 0
    fixable: bool = False


def _is_pg(source_type: object) -> bool:
    return source_type is SourceType.POSTGRES or str(
        getattr(source_type, "value", source_type) or ""
    ).lower() in ("postgres", "postgresql")


def split_qualified(name: str) -> Optional[tuple[str, str]]:
    """``(schema, table)`` of a qualified ``db.table`` / ``schema.table`` name, else None.

    Split on the FIRST dot, like every other qualified-name reader in the tool. An
    unqualified name is never read: it can neither raise nor close a finding.
    """
    schema, sep, table = str(name or "").partition(".")
    if not (sep and schema and table):
        return None
    return schema, table


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


# ---------------------------------------------------------------------------
# The read (ONE statement; raises)
# ---------------------------------------------------------------------------


def build_mysql_catalog_statement(names: Sequence[str]) -> tuple[str, dict]:
    """The ONE MySQL catalog statement for ``names`` and its bound parameters.

    Column rows from ``information_schema.COLUMNS`` (``'c'``) and primary-key rows from
    ``information_schema.STATISTICS`` where ``INDEX_NAME = 'PRIMARY'`` (``'k'``) -- never
    ``COLUMN_KEY = 'PRI'``, which also flags a NOT NULL UNIQUE key on a table without a
    primary key. Names are grouped by schema: ``(TABLE_SCHEMA = :s0 AND TABLE_NAME IN
    (:t0_0, ...))``, the groups joined with OR, so a single-schema migration is one top-level
    equality (MySQL 5.7 then opens only that database's definitions). Every value is bound;
    no name ever appears in the SQL text.
    """
    groups: dict[str, list[str]] = defaultdict(list)
    for name in sorted({str(n) for n in names or () if n}):
        parts = split_qualified(name)
        if parts is not None:
            groups[parts[0]].append(parts[1])
    params: dict[str, str] = {}
    clauses: list[str] = []
    for i, schema in enumerate(sorted(groups)):
        params[f"s{i}"] = schema
        holders: list[str] = []
        for j, table in enumerate(groups[schema]):
            params[f"t{i}_{j}"] = table
            holders.append(f":t{i}_{j}")
        clauses.append(f"(TABLE_SCHEMA = :s{i} AND TABLE_NAME IN ({', '.join(holders)}))")
    where = " OR ".join(clauses) if clauses else "1 = 0"
    sql = (
        f"SELECT /*+ MAX_EXECUTION_TIME({MYSQL_MAX_EXECUTION_MS}) */ 'c' AS part, "
        "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, IS_NULLABLE, "
        "EXTRA "
        f"FROM information_schema.COLUMNS WHERE {where} "
        "UNION ALL "
        "SELECT 'k', TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX, NULL, NULL, NULL "
        f"FROM information_schema.STATISTICS WHERE INDEX_NAME = 'PRIMARY' AND ({where})"
    )
    return sql, params


# One catalog round trip for every captured table: their live columns (attnum order), the
# exact format_type text the Step 1 inventory also holds, the NULL rule, the generated flag
# and primary-key membership. Catalog only -- no table scan, no lock. The 10 s
# statement_timeout comes from the read engine (make_source_engine_factory).
#
# The NULL rule includes the NOT NULL of the column's OWN domain, exactly as the Step 1
# inventory's reflection folds it in (SQLAlchemy: ``nullable and not domain.not_null``; one
# level only, and an array of a domain -- typtype 'b' -- is not folded). ``attnotnull`` alone
# is false for such a column, so without the fold a NOT NULL domain column would read as a
# permanent, false "NULL rule changed" finding that holds Cut over.
PG_CATALOG_SQL = (
    "SELECT n.nspname || '.' || c.relname AS qname, a.attname::text AS col, a.attnum, "
    "       pg_catalog.format_type(a.atttypid, a.atttypmod) AS typ, "
    "       (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS attnotnull, "
    "       a.attgenerated::text AS gen, "
    "       COALESCE(a.attnum = ANY(i.indkey), false) AS in_pk "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_catalog.pg_attribute a "
    "  ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
    "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
    "LEFT JOIN pg_catalog.pg_index i ON i.indrelid = c.oid AND i.indisprimary "
    "WHERE c.relkind IN ('r','p') "
    "  AND n.nspname || '.' || c.relname = ANY(:names)"
)


def _map_mysql_rows(rows: Sequence, wanted: Sequence[str]) -> "dict[str, LiveTable]":
    exact: dict[tuple[str, str], str] = {}
    for name in wanted:
        parts = split_qualified(name)
        if parts is not None:
            exact[parts] = name
    # A case-insensitive fallback (lower_case_table_names): a row whose spelling differs
    # only in case still maps to the name that was asked about, never a false "missing".
    folded = {(s.lower(), t.lower()): n for (s, t), n in exact.items()}
    columns: dict[str, list] = defaultdict(list)
    keys: dict[str, set] = defaultdict(set)
    for row in rows:
        part, schema, table, column = (_text(row[i]) for i in range(4))
        name = exact.get((schema, table)) or folded.get((schema.lower(), table.lower()))
        if name is None or not column:
            continue
        if part == "k":
            keys[name].add(column)
            continue
        extra = _text(row[7]).lower()
        columns[name].append(
            (
                int(row[4] or 0),
                column,
                _text(row[5]),
                _text(row[6]).upper() == "YES",
                # VIRTUAL / STORED GENERATED mark a computed column; DEFAULT_GENERATED (an
                # expression default) does not -- the introspector's own rule.
                "virtual generated" in extra or "stored generated" in extra,
            )
        )
    return {
        name: LiveTable(
            columns=tuple(
                LiveColumn(col, typ, nullable, generated, rank)
                for rank, (_o, col, typ, nullable, generated) in enumerate(sorted(items))
            ),
            primary_key=frozenset(keys.get(name, ())),
        )
        for name, items in columns.items()
    }


def _map_pg_rows(rows: Sequence) -> "dict[str, LiveTable]":
    columns: dict[str, list] = defaultdict(list)
    keys: dict[str, set] = defaultdict(set)
    for row in rows:
        name, column = _text(row[0]), _text(row[1])
        if not name or not column:
            continue
        columns[name].append(
            (int(row[2] or 0), column, _text(row[3]), not bool(row[4]), _text(row[5]) != "")
        )
        if bool(row[6]):
            keys[name].add(column)
    return {
        # attnum has gaps after a DROP COLUMN: the rank among live columns is dense.
        name: LiveTable(
            columns=tuple(
                LiveColumn(col, typ, nullable, generated, rank)
                for rank, (_n, col, typ, nullable, generated) in enumerate(sorted(items))
            ),
            primary_key=frozenset(keys.get(name, ())),
        )
        for name, items in columns.items()
    }


def read_live_catalog(
    connection: object, names: Sequence[str], *, source_type: object
) -> "dict[str, LiveTable]":
    """Read the live definitions of ``names`` (read-only, ONE statement).

    Returns ``{qualified name: LiveTable}`` for every name found; a name absent from the
    result has no column visible to the tool's source user (dropped, renamed, moved -- or,
    on MySQL, no longer visible to that user). Only qualified names are read. RAISES on
    failure, like ``cdc_pg_slot.read_rekeyed_replica_identity``: the caller must tell an
    unreadable source apart from a read. Run it only through the read-only guarded source
    engine (``make_source_engine_factory``).
    """
    from sqlalchemy import text

    wanted = sorted({str(n) for n in names or () if n and split_qualified(str(n))})
    if not wanted:
        return {}
    if _is_pg(source_type):
        rows = connection.execute(  # type: ignore[attr-defined]
            text(PG_CATALOG_SQL), {"names": wanted}
        ).fetchall()
        return _map_pg_rows(rows)
    sql, params = build_mysql_catalog_statement(wanted)
    rows = connection.execute(text(sql), params).fetchall()  # type: ignore[attr-defined]
    return _map_mysql_rows(rows, wanted)


# ---------------------------------------------------------------------------
# The diff (pure)
# ---------------------------------------------------------------------------

_MYSQL_INT_WIDTH = re.compile(r"^(tinyint|smallint|mediumint|int|integer|bigint|year)\((\d+)\)")
_PG_QUALIFIER = re.compile(r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+")\.')


def _normalized_type(value: str, *, source_type: object) -> str:
    text_value = str(value or "").strip()
    if _is_pg(source_type):
        # format_type qualifies a user-defined type by schema only when that schema is not
        # on the reader's search_path, which can differ between introspection and this read.
        return _PG_QUALIFIER.sub("", text_value, count=1)
    match = _MYSQL_INT_WIDTH.match(text_value)
    # An integer (or YEAR) display width is cosmetic (MySQL 8.0.19+ stopped reporting it,
    # so an engine upgrade mid-migration must not read as a change) -- except tinyint(1),
    # the boolean convention the converter maps to a different target type.
    if match and not (match.group(1) == "tinyint" and match.group(2) == "1"):
        return match.group(1) + text_value[match.end():]
    return text_value


def same_type(before: str, after: str, *, source_type: object) -> bool:
    """Whether two catalog type texts name the same type for CDC. Case is kept: an ENUM
    literal changing case is a real change."""
    if str(before or "").strip() == str(after or "").strip():
        return True
    return _normalized_type(before, source_type=source_type) == _normalized_type(
        after, source_type=source_type
    )


def _cut(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value)
    return text_value if len(text_value) <= _TYPE_TEXT_LIMIT else text_value[:_TYPE_TEXT_LIMIT] + "..."


class _Base(NamedTuple):
    name: str
    type: str
    nullable: Optional[bool]
    generated: bool
    position: Optional[int]
    added: bool


def diff_table(
    table_def: object,
    live: Optional[LiveTable],
    *,
    skip: "frozenset[str] | set[str]" = frozenset(),
    target_added: "Optional[Mapping[str, str]]" = None,
    source_type: object = SourceType.MYSQL,
) -> Optional[CatalogDiff]:
    """How ``live`` differs from the baseline ``table_def`` (``None`` when it does not). Pure.

    ``skip`` holds the concrete column names CDC does not capture (excluded columns and
    PostgreSQL STORED generated columns): they never produce a change either way -- a new
    column that merely matches an exclusion RULE is not in it, and is reported.
    ``target_added`` (``{column: source type}``) are columns the operator added to Aurora
    DSQL through Fix target schema: they count as baseline columns (type only, no NULL rule,
    no position), so the live column is not new drift -- unless the source computes it.

    A dropped and an added column at the same position with the same type pair as one
    rename (each column at most once), which also catches a case-only rename. The primary
    key is compared as a SET of the source key, never the composite target key.
    """
    if live is None:
        return CatalogDiff(kinds=(KIND_SOURCE_TABLE_MISSING,), changes=())
    skip = frozenset(skip or ())
    base: list[_Base] = []
    inventory_names: set[str] = set()
    for column in getattr(table_def, "columns", None) or ():
        inventory_names.add(column.name)
        if column.name in skip:
            continue
        base.append(
            _Base(
                column.name, str(column.mysql_type), bool(column.nullable),
                bool(getattr(column, "generated", False)), len(base), False,
            )
        )
    for name, typ in sorted(dict(target_added or {}).items()):
        if name in inventory_names or name in skip:
            continue
        base.append(_Base(str(name), str(typ), None, False, None, True))
    live_columns = [
        c for c in sorted(live.columns, key=lambda c: c.position) if c.name not in skip
    ]
    live_by = {c.name: (rank, c) for rank, c in enumerate(live_columns)}
    base_names = {b.name for b in base}
    added = [(rank, c) for rank, c in enumerate(live_columns) if c.name not in base_names]
    changes: list[CdcCatalogDriftChange] = []
    paired: set[str] = set()
    for b in (b for b in base if b.name not in live_by):
        match = next(
            (
                c for rank, c in added
                if b.position is not None
                and rank == b.position
                and c.name not in paired
                and same_type(b.type, c.type, source_type=source_type)
            ),
            None,
        )
        if match is not None:
            paired.add(match.name)
            changes.append(
                CdcCatalogDriftChange(
                    kind=KIND_RENAME_COLUMN, column=b.name, before=_cut(b.type),
                    after=match.name,
                )
            )
        else:
            changes.append(
                CdcCatalogDriftChange(kind=KIND_DROP_COLUMN, column=b.name, before=_cut(b.type))
            )
    for _rank, c in added:
        if c.name not in paired:
            changes.append(
                CdcCatalogDriftChange(
                    kind=KIND_ADD_COLUMN, column=c.name, after=_cut(c.type),
                    generated=bool(c.generated),
                )
            )
    for b in base:
        if b.name not in live_by:
            continue
        _rank, c = live_by[b.name]
        if b.added and c.generated:
            # A column added to Aurora DSQL as a plain column that the source computes:
            # CDC may never send its values, so it is not resolved drift.
            changes.append(
                CdcCatalogDriftChange(
                    kind=KIND_ADD_COLUMN, column=c.name, after=_cut(c.type), generated=True
                )
            )
            continue
        if not same_type(b.type, c.type, source_type=source_type):
            changes.append(
                CdcCatalogDriftChange(
                    kind=KIND_TYPE_CHANGE, column=b.name, before=_cut(b.type),
                    after=_cut(c.type),
                )
            )
        if (
            b.nullable is not None
            and not b.generated
            and not c.generated
            and b.nullable != bool(c.nullable)
        ):
            changes.append(
                CdcCatalogDriftChange(
                    kind=KIND_NULLABILITY_CHANGE, column=b.name,
                    before="NULL" if b.nullable else "NOT NULL",
                    after="NULL" if c.nullable else "NOT NULL",
                )
            )
    base_key = list(getattr(table_def, "primary_key", None) or ())
    live_key = frozenset(live.primary_key or ())
    if set(base_key) != set(live_key):
        ranked = {c.name: c.position for c in live.columns}
        after_key = sorted(live_key, key=lambda n: (ranked.get(n, len(ranked)), n))
        changes.append(
            CdcCatalogDriftChange(
                kind=KIND_KEY_CHANGE,
                before=_cut(", ".join(base_key)),
                after=_cut(", ".join(after_key)),
            )
        )
    if not changes:
        return None
    order = {kind: i for i, kind in enumerate(KIND_ORDER)}
    changes.sort(key=lambda ch: (order.get(ch.kind, len(order)), ch.column or ""))
    kinds = tuple(k for k in KIND_ORDER if any(ch.kind == k for ch in changes))
    # Over the FULL change list, before the cap. Every added column must be plain: Fix
    # target schema applies its whole plan, which cannot tell a generated column apart, so a
    # table that also gained a generated column would get it as a plain, empty column.
    adds = [ch for ch in changes if ch.kind == KIND_ADD_COLUMN]
    fixable = (
        bool(adds)
        and not any(ch.generated for ch in adds)
        and not ({KIND_RENAME_COLUMN, KIND_KEY_CHANGE, KIND_SOURCE_TABLE_MISSING} & set(kinds))
    )
    return CatalogDiff(
        kinds=kinds,
        changes=tuple(changes[:CHANGE_LIMIT]),
        more_changes=max(len(changes) - CHANGE_LIMIT, 0),
        fixable=fixable,
    )


def baseline_digest(
    table_def: object,
    *,
    skip: "frozenset[str] | set[str]" = frozenset(),
    source_type: object = SourceType.MYSQL,
) -> str:
    """A 16-hex digest of exactly the baseline facts :func:`diff_table` compares (names,
    normalized types, NULL rules of non-generated columns, the primary-key set), over the
    columns CDC captures. A Step 1 re-run that changes any of them changes it; a default,
    comment or collation change does not."""
    skip = frozenset(skip or ())
    columns = sorted(
        (
            str(c.name),
            _normalized_type(str(c.mysql_type), source_type=source_type),
            None if getattr(c, "generated", False) else bool(c.nullable),
        )
        for c in (getattr(table_def, "columns", None) or ())
        if c.name not in skip
    )
    key = sorted(str(n) for n in (getattr(table_def, "primary_key", None) or ()))
    payload = json.dumps([columns, key], separators=(",", ":"), ensure_ascii=False)
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=8).hexdigest()


__all__ = [
    "CHANGE_LIMIT",
    "CatalogDiff",
    "KIND_ADD_COLUMN",
    "KIND_DEFINITION_CHANGE",
    "KIND_DROP_COLUMN",
    "KIND_KEY_CHANGE",
    "KIND_NULLABILITY_CHANGE",
    "KIND_ORDER",
    "KIND_RENAME_COLUMN",
    "KIND_SOURCE_TABLE_MISSING",
    "KIND_TYPE_CHANGE",
    "LiveColumn",
    "LiveTable",
    "MYSQL_MAX_EXECUTION_MS",
    "PG_CATALOG_SQL",
    "baseline_digest",
    "build_mysql_catalog_statement",
    "diff_table",
    "read_live_catalog",
    "same_type",
    "split_qualified",
]
