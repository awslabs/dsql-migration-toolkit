# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read the captured tables' live source definitions and diff them against the baseline.

CDC replicates row data, never DDL, and the sink rebuilds its SQL from each record, so a
source DDL change reaches Aurora DSQL only as a different row shape. Most shapes the target
cannot take are rejected and dead-lettered (the dead-letter banner), but several are applied
SILENTLY: a dropped nullable / DEFAULT column (inserts store NULL or the DEFAULT, updates keep
the old value), a widened numeric or a MySQL ``tinyint(1)`` widened to ``int`` on a boolean
target (the current sink dead-letters a value it would round or store as true, except a
FLOAT/DOUBLE value for a numeric column, which is still rounded; CDC infrastructure deployed
by an earlier version writes it changed, and a value that still fits leaves no dead letter
either way), a dropped or renamed table (the target freezes, or a
same-name table streams into it), a PostgreSQL column Debezium cannot convert or a generated
column (never sent). No rejected row, no signal.

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
* :func:`start_point_fingerprints` / :func:`check_start_point` (MySQL) -- a separate,
  ORDER-sensitive per-table digest over EVERY column (excluded and generated included,
  plus each column's character set), recorded with a CDC start point and compared before
  a ``snapshot.mode=recovery`` Start: recovery decodes older binlog rows BY POSITION with
  the table's current definition, so a reorder or a charset change matters there even
  though it changes nothing :func:`diff_table` compares -- while a CHAR / VARCHAR /
  VARBINARY length or a DECIMAL precision change, which decodes the same, is left out.

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
    """One live source column: name, catalog type text, NULL rule, generated, 0-based rank,
    and (MySQL) its character set -- ``""`` when it has none or it was not read."""

    name: str
    type: str
    nullable: bool
    generated: bool
    position: int
    charset: str = ""


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
    # Fix target schema… has something to apply or explain for this table (the shared
    # cdc_schema_evolve.plan_target_fix's offers_fix): no key change, not only generated adds.
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
    no name ever appears in the SQL text. The column rows also carry ``CHARACTER_SET_NAME``
    (9th column; ``NULL`` on the key rows) for the start-point fingerprint only -- the
    G4 diff does not compare it.
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
        "EXTRA, CHARACTER_SET_NAME "
        f"FROM information_schema.COLUMNS WHERE {where} "
        "UNION ALL "
        "SELECT 'k', TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX, NULL, NULL, NULL, "
        "NULL "
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
# permanent, false "NULL rule changed" finding that holds Cut over. A PostgreSQL 18 NOT NULL
# added NOT VALID reads NOT NULL here (attnotnull is set at once); the inventory has that
# column nullable (G167), so diff_table does not compare its NULL rule.
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
                # CHARACTER_SET_NAME (9th column), read only when present: an 8-column
                # row (a double, or an older statement) maps with no charset.
                _text(row[8]) if len(row) > 8 else "",
            )
        )
    return {
        name: LiveTable(
            columns=tuple(
                LiveColumn(col, typ, nullable, generated, rank, charset)
                for rank, (_o, col, typ, nullable, generated, charset) in enumerate(
                    sorted(items)
                )
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


def same_recorded_type(recorded: Optional[str], live_type: str, *, source_type: object) -> bool:
    """Whether a type text a finding STORED (bounded by :func:`_cut`) names ``live_type``.

    A stored type longer than the limit is matched on its stored prefix (a long ENUM / SET
    is common): a difference past that prefix cannot be seen from the stored text."""
    if same_type(str(recorded or ""), live_type, source_type=source_type):
        return True
    text_value = str(recorded or "")
    return (
        text_value.endswith("...")
        and len(text_value) == _TYPE_TEXT_LIMIT + 3
        and _cut(str(live_type or "")) == text_value
    )


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
    excluded: "Optional[frozenset[str] | set[str]]" = None,
    rekey_columns: "frozenset[str] | set[str]" = frozenset(),
) -> Optional[CatalogDiff]:
    """How ``live`` differs from the baseline ``table_def`` (``None`` when it does not). Pure.

    ``skip`` holds the concrete column names CDC does not capture (excluded columns and
    PostgreSQL STORED generated columns): they never produce a change either way -- a new
    column that merely matches an exclusion RULE is not in it, and is reported.
    ``target_added`` (``{column: source type}``) are columns the operator added to Aurora
    DSQL through Fix target schema: they count as baseline columns (type only, no NULL rule,
    no position), so the live column is not new drift -- unless the source computes it.
    A PostgreSQL 18 column whose NOT NULL was added NOT VALID (``not_null_not_valid``,
    G167) has no NULL rule compared either: the read reports it NOT NULL while the
    inventory, and the target column, have it nullable.

    A dropped and an added column at the same position with the same type pair as one
    rename (each column at most once), which also catches a case-only rename. The primary
    key is compared as a SET of the source key, never the composite target key.

    ``fixable`` comes from the SAME plan Fix target schema… shows
    (``cdc_schema_evolve.plan_target_fix``), with the target assumed to hold the baseline
    names plus ``target_added`` and no rule evaluated (the dialog reads the real target and
    the saved rules): ``excluded`` are the effective exclusions (``None``: ``skip``) and
    ``rekey_columns`` the table's ``message.key.columns``, so a renamed key or re-key column
    is never fixable.
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
                column.name, str(column.mysql_type),
                # A PostgreSQL 18 NOT NULL added NOT VALID (G167): the inventory records the
                # column nullable and the target column has no NOT NULL, while attnotnull
                # (this read) is already true -- and stays true once the source VALIDATEs
                # it. Neither changes what CDC writes, so its NULL rule is not compared.
                None if getattr(column, "not_null_not_valid", False) else bool(column.nullable),
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
    # Over the FULL definitions, not the capped change list: the one plan Fix target schema…
    # shows decides (it refuses a key change or a renamed key column, withholds a generated
    # column and holds an excluded one).
    fixable = False
    if KIND_SOURCE_TABLE_MISSING not in kinds and (
        {KIND_ADD_COLUMN, KIND_RENAME_COLUMN} & set(kinds)
    ):
        from dsql_migrator.core.cdc_schema_evolve import plan_target_fix

        names = [c.name for c in (getattr(table_def, "columns", None) or ())]
        names += [str(n) for n in sorted(target_added or {}) if n not in inventory_names]
        fixable = plan_target_fix(
            str(getattr(table_def, "name", "") or ""),
            live,
            names,
            baseline=table_def,
            rekey_columns=frozenset(rekey_columns or ()),
            excluded=frozenset(skip if excluded is None else excluded),
            rule_matches={},
            source_type=SourceType.POSTGRES if _is_pg(source_type) else SourceType.MYSQL,
        ).offers_fix
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


# ---------------------------------------------------------------------------
# The CDC start-point fingerprint (MySQL; pure)
# ---------------------------------------------------------------------------
#
# Debezium's recovery mode rebuilds its schema history from the tables AS THEY ARE NOW and
# then reads OLDER binlog rows with it, matching row values to columns BY POSITION. So what
# matters here is exactly what Debezium's in-memory Table holds: EVERY column in ordinal
# order (column.exclude.list drops a column only when the event is emitted, after the row
# was matched), each one's type, NULL rule, character set and generated flag, and the
# primary key. One short value per table, so a watermark stays O(tables) (it is saved with
# the job on every update). Defaults, comments, collations and indexes are left out: they
# do not change how a row is decoded. So are two type details that decode the same in BOTH
# directions (checked against the shipped Debezium 2.7.4 converters with the cdc-stack's
# modes): the length of a CHAR / VARCHAR / VARBINARY (each binlog row carries its own
# length, and the string converter reads only the character set) and the precision of a
# DECIMAL at the same scale. Every other part stays exact: a BINARY length (values are
# padded to the CURRENT length), any integer change (unsigned values arrive signed and
# are re-based on the current type; a narrowing wraps), the scale, signedness and the
# ENUM / SET literal lists (values are positional indexes). A change that is safe in one
# direction only (a signed integer widened, an ENUM value appended) still differs: one
# value per table cannot tell the direction. Version 2 added the two exclusions; a
# version-1 value is unverified, never compared.

START_POINT_FINGERPRINT_VERSION = 2

REASON_MISSING = "no longer on the source"
REASON_COUNT = "columns added or dropped: {n} → {m}"
REASON_NAMES = "columns renamed, replaced or reordered"
REASON_DEF = (
    "a column's type, NULL rule, character set or generated flag, or the primary key, "
    "changed"
)

_START_POINT_RE = re.compile(r"^(\d+):(\d+):([0-9a-f]{8}):([0-9a-f]{16})$")
# The type details the start-point fingerprint leaves out (see above). BINARY is not here.
_MYSQL_STRING_LENGTH = re.compile(r"^(char|varchar|varbinary)\(\d+\)")
_MYSQL_DECIMAL_PRECISION = re.compile(r"^(decimal|numeric)\(\d+(?:,(\d+))?\)")


def _digest(payload: object, size: int) -> str:
    text_value = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return hashlib.blake2b(text_value.encode("utf-8"), digest_size=size).hexdigest()


def _decode_type(value: str) -> str:
    """A MySQL catalog type reduced to what changes how a binlog row decodes. Pure.

    :func:`_normalized_type` (integer display width dropped, ``tinyint(1)`` kept), then a
    CHAR / VARCHAR / VARBINARY length dropped and a DECIMAL precision reduced to its scale
    (``decimal(10,2)`` -> ``decimal(,2)``); whatever follows (``unsigned``, ``zerofill``)
    is kept. Used by the start-point fingerprint only -- G4's :func:`same_type` must still
    see a widening.
    """
    text_value = _normalized_type(value, source_type=SourceType.MYSQL)
    match = _MYSQL_STRING_LENGTH.match(text_value)
    if match:
        return match.group(1) + text_value[match.end():]
    match = _MYSQL_DECIMAL_PRECISION.match(text_value)
    if match:
        return f"{match.group(1)}(,{match.group(2) or '0'})" + text_value[match.end():]
    return text_value


def start_point_comparable(value: object) -> bool:
    """Whether a recorded start-point value can be compared: this format AND this
    :data:`START_POINT_FINGERPRINT_VERSION` (an older version is never compared). Pure."""
    match = _START_POINT_RE.match(str(value or ""))
    return match is not None and int(match.group(1)) == START_POINT_FINGERPRINT_VERSION


def start_point_value(live: LiveTable) -> str:
    """The start-point fingerprint of one live MySQL table: ``2:<ncols>:<names>:<def>``.

    ``<names>`` is a 4-byte digest of the column names in ordinal order (a rename, a
    replacement or a pure reorder changes it); ``<def>`` an 8-byte digest of each column's
    decode-relevant type (:func:`_decode_type`: integer display width, CHAR / VARCHAR /
    VARBINARY length and DECIMAL precision dropped; ``tinyint(1)``, a BINARY length, the
    scale, ENUM/SET literals and ``unsigned`` kept), NULL rule, generated flag and
    character set, plus the primary-key set. At most 40 characters. Pure.
    """
    columns = sorted(live.columns, key=lambda c: c.position)
    names = [str(c.name) for c in columns]
    definition = [
        [
            str(c.name),
            _decode_type(str(c.type)),
            bool(c.nullable),
            bool(c.generated),
            str(getattr(c, "charset", "") or ""),
        ]
        for c in columns
    ]
    key = sorted(str(n) for n in (live.primary_key or ()))
    return (
        f"{START_POINT_FINGERPRINT_VERSION}:{len(columns)}:"
        f"{_digest(names, 4)}:{_digest([definition, key], 8)}"
    )


def start_point_fingerprints(live_map: "Mapping[str, LiveTable]") -> "dict[str, str]":
    """``{table: start_point_value}`` for every table read. Pure."""
    return {str(name): start_point_value(live) for name, live in (live_map or {}).items()}


@dataclass(frozen=True)
class StartPointCheck:
    """The captured tables' live definitions against their start-point fingerprints.

    ``changed``: ``(table, reason)`` for each table that differs (a ``REASON_*`` text).
    ``unverified``: captured tables with no usable fingerprint (not recorded, unqualified,
    another fingerprint version, or malformed) -- they are not compared.
    """

    changed: tuple = ()
    unverified: tuple = ()


def check_start_point(
    recorded: "Mapping[str, str]",
    live: "Mapping[str, LiveTable]",
    names: Sequence[str],
) -> StartPointCheck:
    """Compare each captured table in ``names`` with its recorded fingerprint. Pure.

    A recorded table absent from ``live`` counts as changed (``REASON_MISSING``); otherwise
    the first difference is named: the column count, then the names in order, then the
    definitions. Value-free: names and fixed reason texts only.
    """
    recorded = dict(recorded or {})
    live = dict(live or {})
    changed: list[tuple[str, str]] = []
    unverified: list[str] = []
    for name in sorted({str(n) for n in names or () if n}):
        value = recorded.get(name)
        if not split_qualified(name) or not start_point_comparable(value):
            unverified.append(name)
            continue
        match = _START_POINT_RE.match(str(value))
        table = live.get(name)
        if table is None:
            changed.append((name, REASON_MISSING))
            continue
        now = _START_POINT_RE.match(start_point_value(table))
        if now is None:  # not reachable: start_point_value writes this format
            unverified.append(name)
            continue
        if now.group(2) != match.group(2):
            changed.append((name, REASON_COUNT.format(n=match.group(2), m=now.group(2))))
        elif now.group(3) != match.group(3):
            changed.append((name, REASON_NAMES))
        elif now.group(4) != match.group(4):
            changed.append((name, REASON_DEF))
    return StartPointCheck(changed=tuple(changed), unverified=tuple(unverified))


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
    "REASON_COUNT",
    "REASON_DEF",
    "REASON_MISSING",
    "REASON_NAMES",
    "START_POINT_FINGERPRINT_VERSION",
    "StartPointCheck",
    "baseline_digest",
    "build_mysql_catalog_statement",
    "check_start_point",
    "diff_table",
    "read_live_catalog",
    "same_recorded_type",
    "same_type",
    "split_qualified",
    "start_point_comparable",
    "start_point_fingerprints",
    "start_point_value",
]
