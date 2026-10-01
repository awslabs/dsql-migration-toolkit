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
  it is never written anyway, so excluding it means nothing. A non-key IDENTITY column
  counts too: an omitted one would get the next identity value rather than NULL, because
  dropping a DEFAULT cannot make an identity column NULL.

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
from types import SimpleNamespace
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence

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

# The exclusion semantics a session snapshot is written under. 1 (0.1.562): an excluded
# column is NULL on the target through the overlay (DROP NOT NULL, DROP DEFAULT). A
# snapshot without it predates the overlay; its entries are classified by evidence of
# writes (legacy = omitted only, never ALTERed).
EXCLUSION_SEMANTICS_VERSION = 1

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
    edits honoured; ``generated`` is every B4 column, non-key identity included);
    ``message_key_columns`` is the CDC re-key map; ``fk_referenced`` maps
    a parent table to ``{column: ("child.fk_name", ...)}`` for the preserved foreign keys
    of SELECTED child tables (empty when foreign keys are stripped).
    """

    tables: Mapping[str, "TableDef"] = field(default_factory=dict)
    target_pk: Mapping[str, frozenset[str]] = field(default_factory=dict)
    message_key_columns: Mapping[str, frozenset[str]] = field(default_factory=dict)
    generated: Mapping[str, frozenset[str]] = field(default_factory=dict)
    fk_referenced: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)
    # WARNING facts (W1-W6), filled only by ``build_exclusion_context(warnings=True)`` and
    # only for the focus tables. The verdict paths never read them.
    # ``{table: {column: (not_null, default_sql)}}`` from the applied DDL (else the source).
    target_constraints: Mapping[str, Mapping[str, "tuple[bool, Optional[str]]"]] = field(
        default_factory=dict
    )
    # ``{child table: {column: (parent table, ...)}}`` for the preserved FKs the pass ADDs.
    fk_children: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)
    unique_columns: Mapping[str, frozenset[str]] = field(default_factory=dict)
    view_ddls: Mapping[str, str] = field(default_factory=dict)
    # ``{table: {generated column: its expression}}`` from the applied DDL (else the source).
    generated_expressions: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    # Lazily filled per table: the views that read it (one scan per table, not per column).
    _views_memo: dict = field(default_factory=dict, compare=False, repr=False)


@lru_cache(maxsize=4096)
def _ddl_key_facts(ddl: str) -> "tuple[tuple[str, ...], frozenset[str]]":
    """(target primary key, B4 columns) of an applied CREATE TABLE, cached by text.

    The B4 set is the generated columns plus the identity columns outside the key (an
    identity key column is already B2). Three sqlglot parses per table; the context is
    built on every render (the card, the gates, the recovery routes), so without the cache
    a 1,000-table page re-parsed the same DDL many times per render.
    """
    from dsql_migrator.core.converter import (
        parse_target_generated_columns,
        parse_target_identity_columns,
        parse_target_primary_key,
    )

    pk = tuple(parse_target_primary_key(ddl, for_exclusion=True))
    identity = parse_target_identity_columns(ddl) - set(pk)
    # Folded like the other two readers: the names PostgreSQL stores.
    return pk, frozenset(parse_target_generated_columns(ddl, folded=True)) | identity


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


_FK_CHILD_COLUMNS_RE = re.compile(
    rf"FOREIGN\s+KEY\s*\(\s*((?:{_IDENT})(?:\s*,\s*(?:{_IDENT}))*)\s*\)", re.IGNORECASE
)
# An applied ``CREATE UNIQUE INDEX [ASYNC] [name] ON table (col [ASC|DESC], ...)``. An
# expression or otherwise unreadable index makes no claim.
_UNIQUE_INDEX_RE = re.compile(
    rf"CREATE\s+UNIQUE\s+INDEX\s+(?:ASYNC\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:{_IDENT})\s+)?"
    rf"ON\s+(?:ONLY\s+)?(?:{_IDENT})(?:\s*\.\s*(?:{_IDENT}))?\s*(?:USING\s+\w+\s*)?"
    rf"\(\s*((?:{_IDENT})(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:{_IDENT})(?:\s+(?:ASC|DESC))?)*)\s*\)",
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
) -> "list[tuple[str, str, list[str], list[str]]]":
    """``(constraint, referenced table, referenced columns, child columns)`` the FK pass
    will ADD.

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
    found: list[tuple[str, str, list[str], list[str]]] = []
    for ddl in getattr(conversion, "foreign_key_ddls", None) or ():
        name_match = _FK_NAME_RE.search(ddl)
        name = _unquote(name_match.group(1)) if name_match else ""
        source_fk = by_name.get(name) if name else None
        own = _FK_CHILD_COLUMNS_RE.search(ddl)
        if own:
            child_columns = [_unquote(c) for c in re.findall(_IDENT, own.group(1))]
        else:
            child_columns = list(source_fk.columns) if source_fk is not None else []
        ref = _FK_REFERENCES_RE.search(ddl)
        if ref:
            parts = [_unquote(part) for part in re.findall(_IDENT, ref.group(1))]
            table = ".".join(parts)
            if len(parts) == 1:
                table = f"public.{parts[0]}"
                if table not in known:
                    continue
            columns = [_unquote(c) for c in re.findall(_IDENT, ref.group(2))]
            found.append((name or "unnamed", table, columns, child_columns))
            continue
        if source_fk is not None:
            found.append((
                name, source_fk.referenced_table, list(source_fk.referenced_columns),
                child_columns,
            ))
    return found


def build_exclusion_context(
    inventory: "Optional[SourceInventory]",
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    *,
    message_key_columns: "Optional[Mapping[str, Iterable[str]]]" = None,
    preserve_foreign_keys: bool = True,
    selected_tables: "Optional[Iterable[str]]" = None,
    focus_tables: "Optional[Iterable[str]]" = None,
    view_ddls: "Optional[Mapping[str, str]]" = None,
    warnings: bool = False,
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

    ``warnings`` also fills the W1-W6 facts for the focus tables (each DDL read is
    cached by its text); ``view_ddls`` are the APPLIED view DDLs, read for W6 and the
    table findings. Without ``warnings`` the context is exactly what the gates used.
    """
    from dsql_migrator.core.converter import (
        parse_target_column_constraints,
        pg_preserved_generated_columns,
    )

    tables = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    conversions = applied_conversions or {}
    target_pk: dict[str, frozenset[str]] = {}
    generated: dict[str, frozenset[str]] = {}
    target_constraints: dict[str, Mapping[str, "tuple[bool, Optional[str]]"]] = {}
    unique_columns: dict[str, frozenset[str]] = {}
    generated_expressions: dict[str, dict[str, str]] = {}
    fk_children: dict[str, dict[str, list[str]]] = {}
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
        if not warnings:
            continue
        if ddl:
            facts = parse_target_column_constraints(ddl)
            unique = _unique_members(
                ddl, tuple(getattr(conversions.get(name), "index_ddls", None) or ())
            )
        else:
            facts = {c.name: (not c.nullable, c.default) for c in table.columns}
            unique = frozenset(
                c for index in table.indexes if index.unique for c in index.columns
            )
        if facts:
            target_constraints[name] = facts
        if unique:
            unique_columns[name] = unique
        expressions = (
            dict(_ddl_generated_expressions(ddl)) if ddl
            else {c.name: c.generated_expression for c in pg_preserved_generated_columns(table)
                  if c.generated_expression}
        )
        if expressions:
            generated_expressions[name] = expressions
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
                (fk.name, fk.referenced_table, list(fk.referenced_columns), list(fk.columns))
                for fk in getattr(child, "foreign_keys", None) or ()
            ]
        else:
            fks = []
        for fk_name, referenced_table, referenced_columns, child_columns in fks:
            label = f"{child_name}.{fk_name}"
            for col in referenced_columns:
                fk_ref.setdefault(referenced_table, {}).setdefault(col, []).append(label)
            if warnings and child_name in focus:
                for col in child_columns:
                    parents = fk_children.setdefault(child_name, {}).setdefault(col, [])
                    if referenced_table not in parents:
                        parents.append(referenced_table)
    return ExclusionContext(
        tables=tables,
        target_pk=target_pk,
        message_key_columns=mkc,
        generated=generated,
        fk_referenced={
            t: {c: tuple(sorted(refs)) for c, refs in cols.items()}
            for t, cols in fk_ref.items()
        },
        target_constraints=target_constraints,
        fk_children={
            t: {c: tuple(sorted(parents)) for c, parents in cols.items()}
            for t, cols in fk_children.items()
        },
        unique_columns=unique_columns,
        view_ddls=dict(view_ddls or {}) if warnings else {},
        generated_expressions=generated_expressions,
    )


@lru_cache(maxsize=4096)
def _ddl_generated_expressions(ddl: str) -> "tuple[tuple[str, str], ...]":
    """``((column, expression), ...)`` of the GENERATED columns of an applied CREATE TABLE,
    names as PostgreSQL stores them; the expression as sqlglot renders it. Cached by text;
    an unreadable DDL gives ``()``."""
    import sqlglot
    from sqlglot import exp

    from dsql_migrator.core.converter import _strip_identity_clauses, fold_identifier

    try:
        parsed = sqlglot.parse_one(_strip_identity_clauses(ddl), read="postgres")
    except Exception:  # noqa: BLE001 - unparseable DDL -> no claim
        return ()
    if not isinstance(parsed, exp.Create):
        return ()
    out = []
    for column_def in parsed.find_all(exp.ColumnDef):
        for constraint in column_def.args.get("constraints") or []:
            kind = constraint.args.get("kind")
            if isinstance(kind, exp.ComputedColumnConstraint) and kind.this is not None:
                ident = column_def.this
                quoted = isinstance(ident, exp.Identifier) and bool(ident.quoted)
                name = column_def.name if quoted else fold_identifier(column_def.name)
                out.append((name, kind.this.sql(dialect="postgres")))
    return tuple(out)


@lru_cache(maxsize=4096)
def _view_table_refs(ddl: str) -> "Optional[frozenset[str]]":
    """The tables an applied CREATE VIEW reads (FROM/JOIN/subqueries), each as
    ``schema.table`` the way PostgreSQL resolves the names (quoted as written, unquoted
    folded; a one-part name in ``public``). ``None`` when the DDL cannot be read."""
    import sqlglot
    from sqlglot import exp

    from dsql_migrator.core.converter import fold_identifier

    try:
        parsed = sqlglot.parse_one(ddl, read="postgres")
    except Exception:  # noqa: BLE001 - unreadable -> the text heuristic decides
        return None
    body = parsed.expression if isinstance(parsed, exp.Create) else parsed
    if body is None:
        return None

    def _name(node) -> str:
        if node is None:
            return ""
        if isinstance(node, exp.Identifier):
            return node.name if node.quoted else fold_identifier(node.name)
        return fold_identifier(getattr(node, "name", "") or "")

    refs = set()
    for table in body.find_all(exp.Table):
        name = _name(table.this)
        if not name:
            continue
        schema = _name(table.args.get("db")) or "public"
        refs.add(f"{schema}.{name}")
    return frozenset(refs)


@lru_cache(maxsize=4096)
def _unique_members(ddl: str, index_ddls: "tuple[str, ...]") -> frozenset[str]:
    """Columns in a UNIQUE constraint or index of an applied table: inline ``UNIQUE``,
    a table-level ``UNIQUE (...)``, and each readable ``CREATE UNIQUE INDEX``. Read
    here so the converter stays untouched; an unreadable text makes no claim."""
    import sqlglot
    from sqlglot import exp

    from dsql_migrator.core.converter import _strip_identity_clauses, fold_identifier

    def _name(node) -> str:
        quoted = isinstance(node, exp.Identifier) and bool(node.quoted)
        return node.name if quoted else fold_identifier(node.name)

    members: set[str] = set()
    try:
        parsed = sqlglot.parse_one(_strip_identity_clauses(ddl), read="postgres")
    except Exception:  # noqa: BLE001 - unparseable DDL -> no claim
        parsed = None
    if isinstance(parsed, exp.Create):
        for unique in parsed.find_all(exp.UniqueColumnConstraint):
            if isinstance(unique.this, exp.Schema):
                members.update(_name(e) for e in unique.this.expressions if e.name)
            else:
                column_def = unique.find_ancestor(exp.ColumnDef)
                if column_def is not None and column_def.name:
                    members.add(_name(column_def.this))
    for index_ddl in index_ddls:
        match = _UNIQUE_INDEX_RE.search(index_ddl)
        if match:
            members.update(
                _unquote(c)
                for c in re.findall(rf"({_IDENT})(?:\s+(?:ASC|DESC))?", match.group(1))
            )
    return frozenset(members)


# --- column WARNINGS (W1-W6) and table-exclusion findings ---------------------------
# Allowed, shown in the preview and repeated in the Start dialog. A SEPARATE function from
# the verdict on purpose: check_stored_exclusions and effective_exclusions read only the
# verdict, so a warning can never block an entry or leak into a stored-entry message.

W1 = "W1"  # NOT NULL on the target
W2 = "W2"  # has a DEFAULT
W3 = "W3"  # FK child column of a preserved FK
W4 = "W4"  # member of a UNIQUE constraint or index
W5 = "W5"  # named in a kept generated column's expression
W6 = "W6"  # named in a view definition (text match)
# Table-exclusion findings. T1 and T4 are information; T2 and T3 are warnings.
T1 = "T1"  # not loaded, left as is
T2 = "T2"  # a selected child's FK skipped by the orphan check
T3 = "T3"  # a view that reads the table
T4 = "T4"  # column rules on it are inert


def views_referencing(
    view_ddls: Mapping[str, str], tables: "frozenset[str]"
) -> list[str]:
    """Return the CREATE-VIEW DDLs whose SQL references any of ``tables``.

    A view that SELECTs from a table being DROP+recreated blocks that table's
    DROP. We select exactly the views that name one of the replace ``tables`` --
    matching either the qualified ``schema.table`` or the bare table name as a
    word in the view's DDL -- so an unrelated view is never needlessly dropped.
    Case-insensitive, word-boundary match to avoid matching a substring of a
    longer identifier. Deterministic order (by view name) for stable behavior.
    """
    if not view_ddls or not tables:
        return []
    wanted: list[str] = []
    for view_name in sorted(view_ddls):
        ddl = view_ddls[view_name]
        low = ddl.lower()
        for table in tables:
            bare = table.split(".")[-1].lower()
            qualified = table.lower()
            if names_in(low, (qualified, bare)):
                wanted.append(ddl)
                break
    return wanted


def names_in(haystack_lower: str, candidates: "Sequence[str]") -> bool:
    """True if any candidate appears in ``haystack_lower`` as a bounded token.

    Guards against a bare table name matching a substring of a longer identifier
    (e.g. ``orders`` inside ``orders_archive``) by requiring a non-identifier
    character (or string edge) on both sides.
    """
    for name in candidates:
        if not name:
            continue
        if re.search(rf"(?<![\w.]){re.escape(name)}(?![\w])", haystack_lower):
            return True
    return False


def _mentions(text: str, name: str, *, ignore_case: bool = False) -> bool:
    """``name`` in ``text`` at an identifier boundary (a quoted ``"name"`` counts)."""
    flags = re.IGNORECASE if ignore_case else 0
    return re.search(rf"(?<![\w$]){re.escape(name)}(?![\w$])", text, flags) is not None


def _views_reading(table_name: str, ctx: ExclusionContext) -> list[str]:
    """The applied views that READ ``table_name``: its FROM/JOIN references, so a view on
    another schema's same-named table, or one with a column named like the table, is not
    counted; the text heuristic only when a view cannot be parsed. Memoized per context
    (one scan per table, not per excluded column)."""
    memo = ctx._views_memo
    if table_name in memo:
        return memo[table_name]
    bare = table_name.split(".")[-1].lower()
    found = []
    for view in sorted(ctx.view_ddls):
        ddl = ctx.view_ddls[view]
        refs = _view_table_refs(ddl)
        if refs is None:
            if names_in(ddl.lower().replace('"', ""), (table_name.lower(), bare)):
                found.append(view)
        elif table_name in refs:
            found.append(view)
    memo[table_name] = found
    return found


def column_exclusion_warnings(
    table_name: str, column: str, ctx: ExclusionContext
) -> tuple[ExclusionFinding, ...]:
    """W1-W6 for excluding ``table_name.column`` (in that order); empty for a context
    built without ``warnings``. Judges only what excluding it does, never eligibility."""
    found: list[ExclusionFinding] = []
    constraints = ctx.target_constraints.get(table_name, {})
    not_null, default_sql = constraints.get(column, (False, None))
    if not_null:
        found.append(ExclusionFinding(W1, (
            "NOT NULL will be dropped on Aurora DSQL. One-way: Aurora DSQL cannot add NOT NULL "
            "back in place (a later Drop & reload of this table restores it).")))
    if default_sql is not None:
        found.append(ExclusionFinding(W2, (
            f"DEFAULT {default_sql} is dropped for the migration and restored at Cut over "
            "(new rows only).")))
    for parent in ctx.fk_children.get(table_name, {}).get(column, ()):
        found.append(ExclusionFinding(W3, (
            f"Its relationship to {parent} is not migrated (NULL passes the foreign key).")))
    if column in ctx.unique_columns.get(table_name, ()):
        found.append(ExclusionFinding(
            W4, "Uniqueness is not enforced for migrated rows (all NULL)."))
    table = ctx.tables.get(table_name)
    source_columns = {c.name: c for c in (table.columns if table is not None else ())}
    expressions = ctx.generated_expressions.get(table_name, {})
    for other_name in sorted(ctx.generated.get(table_name, ())):
        expression = expressions.get(other_name)
        if other_name == column or not expression or not _mentions(expression, column):
            continue
        source = source_columns.get(other_name)
        fallback = (not source.nullable) if source is not None else False
        other_not_null = constraints.get(other_name, (fallback, None))[0]
        other = SimpleNamespace(name=other_name)
        if other_not_null:
            message = (
                f"{other.name} is NOT NULL and computed from this column: if {expression} "
                "yields NULL when this column is NULL, every row is rejected (23502).")
        else:
            message = f"{other.name} will be computed from NULL and differ from the source."
        found.append(ExclusionFinding(W5, message))
    for view in _views_reading(table_name, ctx):
        if _mentions(ctx.view_ddls[view], column, ignore_case=True):
            found.append(ExclusionFinding(
                W6, f"View {view} mentions it (text match) and will read NULL."))
    return tuple(found)


WL = "WL"  # an omit-only (pre-0.1.562) entry: information
WL_REJECT = "WLR"  # the same, where every row is rejected: a warning


def legacy_exclusion_notes(
    table_name: str, column: str, ctx: ExclusionContext
) -> tuple[ExclusionFinding, ...]:
    """What an omit-only (legacy) entry does instead of the W1-W6 overlay story: no
    target change, so Aurora DSQL keeps NOT NULL and fills the DEFAULT -- which a UNIQUE
    member turns into 23505 after the first row, and NOT NULL without one into 23502."""
    not_null, default_sql = ctx.target_constraints.get(table_name, {}).get(column, (False, None))
    text = "Excluded before v0.1.562: omitted only, with no change on Aurora DSQL."
    code = WL
    if default_sql is not None:
        text += f" Aurora DSQL fills its DEFAULT {default_sql} in every row."
        if column in ctx.unique_columns.get(table_name, ()):
            text += " A UNIQUE index on it rejects every row after the first (23505)."
            code = WL_REJECT
    elif not_null:
        text += (" It is NOT NULL with no DEFAULT on Aurora DSQL, so every row is rejected "
                 "(23502).")
        code = WL_REJECT
    else:
        text += " Aurora DSQL stores NULL in it."
    return (ExclusionFinding(code, text),)


def table_exclusion_findings(
    table_name: str, ctx: ExclusionContext
) -> tuple[ExclusionFinding, ...]:
    """What unselecting ``table_name`` means (the context's ``fk_referenced`` covers the
    SELECTED children only, so build it for the selection after the Save). Never "stays
    empty": the table may already exist on the target with rows."""
    found = [ExclusionFinding(
        T1, "Not loaded by this migration; left as is on Aurora DSQL (not dropped).")]
    labels = sorted({label for refs in ctx.fk_referenced.get(table_name, {}).values()
                     for label in refs})
    found.extend(ExclusionFinding(T2, (
        f"{label} will be skipped by the orphan check when foreign keys are applied (Data "
        "Migration for Full load only; Cut over for CDC).")) for label in labels)
    found.extend(ExclusionFinding(T3, f"View {view} will return no rows.")
                 for view in _views_reading(table_name, ctx))
    found.append(ExclusionFinding(
        T4, "Column rules on it have no effect while it is unselected."))
    return tuple(found)


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


def set_digest(columns: Iterable[str]) -> str:
    """A stable fingerprint of an exclusion COLUMN SET (the overlay ledger's key field).

    ``sha256("v1|" + NUL-joined sorted distinct names)``: NUL cannot occur in a PostgreSQL
    or MySQL identifier, so two different sets can never collide; never Python ``hash()``
    (salted per process). The empty set is ``sha256("v1|")``. The names are hashed
    verbatim -- the source names the loader quotes. FROZEN: every ledger record carries
    this value, so changing the encoding would make every recorded table mismatch.
    """
    import hashlib

    return hashlib.sha256(
        ("v1|" + "\x00".join(sorted(set(columns)))).encode("utf-8")
    ).hexdigest()


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
    "EXCLUSION_SEMANTICS_VERSION",
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
    "T1",
    "T2",
    "T3",
    "T4",
    "W1",
    "W2",
    "W3",
    "W4",
    "W5",
    "W6",
    "WL",
    "WL_REJECT",
    "build_exclusion_context",
    "check_stored_exclusions",
    "column_exclusion_verdict",
    "column_exclusion_warnings",
    "effective_exclusions",
    "legacy_exclusion_notes",
    "missing_target_columns",
    "names_in",
    "set_digest",
    "table_exclusion_findings",
    "views_referencing",
]
