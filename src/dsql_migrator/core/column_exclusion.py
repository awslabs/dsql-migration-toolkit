# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ONE rule for which source columns may be excluded from a migration.

Every path that honours an exclusion -- Full Load, the CDC ``column.exclude.list``,
Validation, the pre-load gates and the post-quarantine "exclude & reload" action --
reads the SAME answer from here, so the paths can no longer disagree about which
columns are excluded. Before this module the only filter was the UI's candidate list:
the stored selection itself was never validated (``DataMigrationState.set_lob_exclusion``
accepts any table/column, and so does session restore), Full Load silently skipped a
source-PK entry (``core.models.apply_lob_exclusions``), and the CDC path passed every
entry -- PK included -- straight into Debezium's ``column.exclude.list``.

BLOCKED columns (never excludable; the reason is always shown):

* **B1** a source primary-key column -- it anchors keyset streaming, the ON CONFLICT /
  SKIP_EXISTING key, Validation's row anchor and the CDC record key.
* **B2** a TARGET primary-key column that is not a source PK -- a Schema Conversion
  composite key's leading column, and every Debezium ``message.key.columns`` column. The
  Full Load importer reads each target-key column out of every row, so an excluded one
  fails the load, and CDC cannot build the record key without it.
* **B3** a column that another SELECTED table's preserved foreign key references (only
  while foreign keys are preserved): every child row would become an orphan and the
  constraint could not be added.
* **B4** a column Aurora DSQL computes (a preserved ``GENERATED ... STORED`` column):
  it is never written anyway, so excluding it means nothing.

B1/B2 entries are always DROPPED from the effective set (before and after data moved),
because keeping them would break the load or the stream. B3/B4 block a stored entry only
before data has moved; afterwards they are reported but no longer block, since the set is
already baked into what was loaded or captured.

Pure: no UI, no database, no AWS. The UI builds an :class:`ExclusionContext` once and
passes it in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Iterable, Mapping, Optional

if TYPE_CHECKING:
    from dsql_migrator.core.models import SourceInventory, TableDef


class ExclusionStatus(Enum):
    OK = "ok"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class ExclusionFinding:
    """One reason a column cannot be excluded. ``message`` is user-facing."""

    code: str
    message: str


@dataclass(frozen=True)
class ColumnVerdict:
    status: ExclusionStatus
    findings: tuple[ExclusionFinding, ...] = ()

    @property
    def blocked(self) -> bool:
        return self.status is ExclusionStatus.BLOCKED


_OK = ColumnVerdict(ExclusionStatus.OK)

B1 = "B1"  # source primary key
B2 = "B2"  # target primary key / re-key
B3 = "B3"  # referenced by a preserved foreign key of a selected table
B4 = "B4"  # computed by Aurora DSQL
# Codes whose entries are dropped from the effective set on every path, always.
ALWAYS_DROPPED = frozenset({B1, B2})

_MESSAGES = {
    B1: "Primary key: needed to read, de-duplicate and replicate rows.",
    B2: "Part of the target primary key chosen in Schema Conversion.",
    B4: "Computed by Aurora DSQL; never written.",
}


def _b3_message(referencers: Iterable[str]) -> str:
    refs = ", ".join(sorted(referencers))
    return (
        f"Referenced by {refs}: every child row would become an orphan and the foreign "
        "key could not be added."
    )


@dataclass(frozen=True)
class ExclusionContext:
    """What the predicate needs to know about the migration, built once per render/gate.

    ``target_pk`` / ``generated`` come from the APPLIED target DDL (Schema Conversion,
    edits honoured); ``message_key_columns`` is the CDC re-key map; ``fk_referenced`` maps
    a parent table to ``{column: ("child.fk_name", ...)}`` for the preserved foreign keys
    of SELECTED child tables (empty when foreign keys are stripped).
    """

    tables: Mapping[str, "TableDef"] = field(default_factory=dict)
    target_pk: Mapping[str, frozenset[str]] = field(default_factory=dict)
    message_key_columns: Mapping[str, frozenset[str]] = field(default_factory=dict)
    generated: Mapping[str, frozenset[str]] = field(default_factory=dict)
    fk_referenced: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)


@lru_cache(maxsize=4096)
def _ddl_key_facts(ddl: str) -> "tuple[tuple[str, ...], frozenset[str]]":
    """(target primary key, generated columns) of an applied CREATE TABLE, cached by text.

    Two sqlglot parses per table; the context is built on every render (the card, the
    gates, the recovery routes), so without the cache a 1,000-table page re-parsed the same
    DDL many times per render.
    """
    from dsql_migrator.core.converter import (
        parse_target_generated_columns,
        parse_target_primary_key,
    )

    return (
        tuple(parse_target_primary_key(ddl, for_exclusion=True)),
        frozenset(parse_target_generated_columns(ddl)),
    )


# The FK pass applies each ``conv.foreign_key_ddls`` statement (an ``ALTER TABLE ... ADD
# CONSTRAINT <name> FOREIGN KEY (...) REFERENCES <table> (<cols>) NOT VALID``); these read
# its constraint name and REFERENCES clause. The name pattern mirrors the Full Load
# engine's ``_FK_CONSTRAINT_NAME_RE`` (core must not import the UI layer).
_FK_NAME_RE = re.compile(
    r'ADD\s+CONSTRAINT\s+("(?:[^"]|"")+"|[^\s("]+)\s+FOREIGN\s+KEY', re.IGNORECASE
)
_IDENT = r'"(?:[^"]|"")+"|[^\s(".,)]+'
# The column list is matched identifier by identifier, so a quoted name containing ')'
# (e.g. "code)x") is not cut short.
_FK_REFERENCES_RE = re.compile(
    rf"REFERENCES\s+((?:{_IDENT})(?:\s*\.\s*(?:{_IDENT}))?)"
    rf"\s*\(\s*((?:{_IDENT})(?:\s*,\s*(?:{_IDENT}))*)\s*\)",
    re.IGNORECASE,
)


def _unquote(identifier: str) -> str:
    """A quoted identifier as written; an unquoted one folded as PostgreSQL folds it."""
    from dsql_migrator.core.converter import fold_identifier

    identifier = identifier.strip()
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1].replace('""', '"')
    return fold_identifier(identifier)


def _applied_foreign_keys(
    child, conversion, known_tables: "Optional[Iterable[str]]" = None
) -> "list[tuple[str, str, list[str]]]":
    """``(constraint, referenced table, referenced columns)`` the FK pass will ADD.

    Read from the applied ``foreign_key_ddls`` -- the statements the pass actually runs,
    already empty when foreign keys are stripped -- so an FK deleted from an edited script
    no longer blocks, and one ADDED to it does. The REFERENCES clause of each statement
    decides, because that text is what executes: an edit that keeps a constraint's NAME
    but changes its columns follows the edit. The source FK of the same name is used only
    when the clause cannot be read; a statement readable neither way is skipped (no claim).

    A one-part parent name resolves the way the FK pass's connection does with DSQL's
    default ``search_path`` ("$user", public): to ``public.<name>`` when that table is
    migrated (``known_tables``); otherwise it is not one of this migration's tables and
    makes no claim.
    """
    by_name = {
        fk.name: fk
        for fk in (
            list(getattr(child, "foreign_keys", None) or ())
            + list(getattr(conversion, "preserved_foreign_keys", None) or ())
        )
    }
    known = set(known_tables or ())
    found: list[tuple[str, str, list[str]]] = []
    for ddl in getattr(conversion, "foreign_key_ddls", None) or ():
        name_match = _FK_NAME_RE.search(ddl)
        name = _unquote(name_match.group(1)) if name_match else ""
        ref = _FK_REFERENCES_RE.search(ddl)
        if ref:
            parts = [_unquote(part) for part in re.findall(_IDENT, ref.group(1))]
            table = ".".join(parts)
            if len(parts) == 1:
                table = f"public.{parts[0]}"
                if table not in known:
                    continue
            columns = [_unquote(c) for c in re.findall(_IDENT, ref.group(2))]
            found.append((name or "unnamed", table, columns))
            continue
        source_fk = by_name.get(name) if name else None
        if source_fk is not None:
            found.append((name, source_fk.referenced_table, list(source_fk.referenced_columns)))
    return found


def build_exclusion_context(
    inventory: "Optional[SourceInventory]",
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    *,
    message_key_columns: "Optional[Mapping[str, Iterable[str]]]" = None,
    preserve_foreign_keys: bool = True,
    selected_tables: "Optional[Iterable[str]]" = None,
    focus_tables: "Optional[Iterable[str]]" = None,
) -> ExclusionContext:
    """Build the context from the source inventory and the APPLIED conversions.

    ``applied_conversions`` maps a table name to its applied ``TableConversion`` (anything
    with a ``target_ddl``); ``None`` or a missing table falls back to the source only (the
    CDC builders do not hold the Schema Conversion state, and there the re-key reaches the
    predicate through ``message_key_columns``, which is itself derived from the applied
    DDL). ``selected_tables`` scopes B3 to child tables that will actually be migrated;
    ``None`` means every table in the inventory.

    ``focus_tables`` limits the DDL parsing (B2/B4) to the tables the caller will ask
    about -- the tables that HAVE stored entries. The gates run on every render, and
    parsing every applied CREATE TABLE of a 1,000-table schema each time would stall the
    page; a table outside the focus simply has no B2/B4 facts. ``None`` parses all.
    """
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    tables = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    conversions = applied_conversions or {}
    target_pk: dict[str, frozenset[str]] = {}
    generated: dict[str, frozenset[str]] = {}
    focus = set(tables) if focus_tables is None else set(focus_tables) & set(tables)
    for name in focus:
        table = tables[name]
        ddl = getattr(conversions.get(name), "target_ddl", None) or ""
        if ddl:
            pk, gen = _ddl_key_facts(ddl)
        else:
            # No applied DDL (a restored session before its first render): the source's
            # preserved STORED generated columns are what the conversion would emit. With
            # a DDL, the DDL alone decides -- an edit that removed the GENERATED clause
            # made the column ordinary, and excludable.
            pk = ()
            gen = frozenset(c.name for c in pg_preserved_generated_columns(table))
        if pk:
            target_pk[name] = frozenset(pk)
        if gen:
            generated[name] = frozenset(gen)
    mkc = {
        t: frozenset(cols) for t, cols in (message_key_columns or {}).items() if cols
    }
    selected = set(tables) if selected_tables is None else set(selected_tables)
    fk_ref: dict[str, dict[str, list[str]]] = {}
    for child_name in selected:
        child = tables.get(child_name)
        conversion = conversions.get(child_name)
        if conversion is not None:
            # What the FK pass will actually ADD (empty when foreign keys are stripped).
            fks = _applied_foreign_keys(child, conversion, tables)
        elif preserve_foreign_keys:
            fks = [
                (fk.name, fk.referenced_table, list(fk.referenced_columns))
                for fk in getattr(child, "foreign_keys", None) or ()
            ]
        else:
            fks = []
        for fk_name, referenced_table, referenced_columns in fks:
            label = f"{child_name}.{fk_name}"
            for col in referenced_columns:
                fk_ref.setdefault(referenced_table, {}).setdefault(col, []).append(label)
    return ExclusionContext(
        tables=tables,
        target_pk=target_pk,
        message_key_columns=mkc,
        generated=generated,
        fk_referenced={
            t: {c: tuple(sorted(refs)) for c, refs in cols.items()}
            for t, cols in fk_ref.items()
        },
    )


def column_exclusion_verdict(
    table_name: str, column: str, ctx: ExclusionContext
) -> ColumnVerdict:
    """BLOCKED (with every applicable reason) or OK for excluding ``table_name.column``."""
    findings: list[ExclusionFinding] = []
    table = ctx.tables.get(table_name)
    if table is not None and column in table.primary_key:
        findings.append(ExclusionFinding(B1, _MESSAGES[B1]))
    elif column in ctx.target_pk.get(table_name, ()) or column in ctx.message_key_columns.get(
        table_name, ()
    ):
        findings.append(ExclusionFinding(B2, _MESSAGES[B2]))
    refs = ctx.fk_referenced.get(table_name, {}).get(column)
    if refs:
        findings.append(ExclusionFinding(B3, _b3_message(refs)))
    if column in ctx.generated.get(table_name, ()):
        findings.append(ExclusionFinding(B4, _MESSAGES[B4]))
    if not findings:
        return _OK
    return ColumnVerdict(ExclusionStatus.BLOCKED, tuple(findings))


@dataclass(frozen=True)
class DroppedEntry:
    table: str
    column: str
    finding: ExclusionFinding


@dataclass(frozen=True)
class EffectiveExclusions:
    """The exclusion set every path must use, plus the entries it had to drop."""

    columns: Mapping[str, frozenset[str]]
    dropped: tuple[DroppedEntry, ...] = ()


def effective_exclusions(
    stored: "Mapping[str, Iterable[str]]", ctx: ExclusionContext
) -> EffectiveExclusions:
    """``stored`` minus every B1/B2 (key) entry, which would break the load or the stream.

    B3/B4 entries are kept here: before data moved they are blocked by
    :func:`check_stored_exclusions` at the gates; afterwards the set is already baked in.
    """
    kept: dict[str, frozenset[str]] = {}
    dropped: list[DroppedEntry] = []
    for table in sorted(stored or {}):
        cols: set[str] = set()
        for column in sorted(stored[table] or ()):
            verdict = column_exclusion_verdict(table, column, ctx)
            key = next((f for f in verdict.findings if f.code in ALWAYS_DROPPED), None)
            if key is not None:
                dropped.append(DroppedEntry(table, column, key))
            else:
                cols.add(column)
        if cols:
            kept[table] = frozenset(cols)
    return EffectiveExclusions(kept, tuple(dropped))


def missing_target_columns(
    tables: "Iterable[TableDef]",
    applied_conversions: "Optional[Mapping[str, object]]",
    excluded: "Optional[Mapping[str, Iterable[str]]]" = None,
) -> dict[str, tuple[str, ...]]:
    """Source columns the load would still write that the applied target DDL lacks.

    The Full Load INSERT names every source column that is not excluded (and not computed
    by the target, which the DDL then declares anyway), so a column removed from the target
    DDL in Schema Conversion -- and not excluded -- fails every batch with 42703 (undefined
    column), and CDC dead-letters every change to it. It is NEVER excluded automatically:
    silently dropping data the user did not choose to drop is exactly what the explicit
    exclusion exists to prevent.

    ``tables`` are the tables the migration moves; a table without an applied conversion,
    or whose DDL does not parse, is unknown and never reported. Order follows ``tables``.
    """
    from dsql_migrator.core.converter import parse_target_column_names

    conversions = applied_conversions or {}
    skip = {t: set(cols) for t, cols in (excluded or {}).items()}
    missing: dict[str, tuple[str, ...]] = {}
    for table in tables:
        ddl = getattr(conversions.get(table.name), "target_ddl", None)
        target = parse_target_column_names(ddl) if ddl else None
        if not target:
            continue
        gone = tuple(
            c.name
            for c in table.columns
            if c.name not in target and c.name not in skip.get(table.name, ())
        )
        if gone:
            missing[table.name] = gone
    return missing


class IssueSeverity(Enum):
    ERROR = "error"      # blocks Start Full Load, Deploy infrastructure and Start CDC
    WARNING = "warning"  # shown; does not block
    INFO = "info"


@dataclass(frozen=True)
class StoredEntryIssue:
    table: str
    column: str
    severity: IssueSeverity
    message: str


def check_stored_exclusions(
    stored: "Mapping[str, Iterable[str]]",
    ctx: ExclusionContext,
    *,
    committed: bool,
) -> tuple[StoredEntryIssue, ...]:
    """Judge the STORED selection (which may come from an old session or a restore).

    Before data moved (``committed`` False) an entry that is BLOCKED or no longer exists is
    an ERROR: nothing has been loaded or captured under it yet, so it must be removed
    before anything starts. After data moved nothing blocks -- the set is baked into what
    was loaded/captured -- so those become WARNINGs (B1/B2 are dropped from the effective
    set regardless), and an entry for a DSQL-computed column is INFO ("already omitted").
    """
    issues: list[StoredEntryIssue] = []
    for table in sorted(stored or {}):
        tdef = ctx.tables.get(table)
        for column in sorted(stored[table] or ()):
            if tdef is None or all(c.name != column for c in tdef.columns):
                what = "table" if tdef is None else "column"
                issues.append(
                    StoredEntryIssue(
                        table,
                        column,
                        IssueSeverity.WARNING if committed else IssueSeverity.ERROR,
                        f"{table}.{column}: this saved exclusion names a {what} that is "
                        "not in the source schema any more.",
                    )
                )
                continue
            verdict = column_exclusion_verdict(table, column, ctx)
            if not verdict.blocked:
                continue
            codes = {f.code for f in verdict.findings}
            reason = " ".join(f.message for f in verdict.findings)
            if not committed:
                severity, text = IssueSeverity.ERROR, f"{table}.{column} cannot be excluded: {reason}"
            elif codes & ALWAYS_DROPPED:
                # Dropped from the effective set on every path, so it IS migrated.
                severity, text = IssueSeverity.WARNING, f"{table}.{column} is still migrated: {reason}"
            elif codes == {B4}:
                severity, text = IssueSeverity.INFO, f"{table}.{column}: computed by Aurora DSQL; already omitted."
            else:
                # B3 is kept in the effective set once the set is baked in: it IS excluded.
                severity, text = IssueSeverity.WARNING, f"{table}.{column} stays excluded, but: {reason}"
            issues.append(StoredEntryIssue(table, column, severity, text))
    return tuple(issues)


__all__ = [
    "ALWAYS_DROPPED",
    "B1",
    "B2",
    "B3",
    "B4",
    "ColumnVerdict",
    "DroppedEntry",
    "EffectiveExclusions",
    "ExclusionContext",
    "ExclusionFinding",
    "ExclusionStatus",
    "IssueSeverity",
    "StoredEntryIssue",
    "build_exclusion_context",
    "check_stored_exclusions",
    "column_exclusion_verdict",
    "effective_exclusions",
    "missing_target_columns",
]
