# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query (DML) converter: MySQL -> Aurora DSQL (PostgreSQL 16) + lock checks.

This module implements the :class:`QueryConverter` component (design.md section
"4. Query Converter"). It transpiles MySQL DML/SELECT statements to the
PostgreSQL dialect with ``sqlglot`` and inspects the parsed AST for the DSQL
``FOR UPDATE`` lock anti-pattern (Requirements 4.1, 4.2, 4.3, 4.4).

What it does:

- **Transpile** (Requirement 4.1): MySQL DML/SELECT is rendered in the
  ``postgres`` dialect. ``sqlglot`` already maps many idioms (MySQL-only
  functions such as ``IFNULL`` -> ``COALESCE``, ``CONCAT`` -> ``||``, and the
  ``LIMIT offset, count`` form -> ``LIMIT count OFFSET offset``). The
  ``INSERT ... ON DUPLICATE KEY UPDATE`` idiom is rewritten to
  ``INSERT ... ON CONFLICT DO UPDATE SET`` with ``VALUES(col)`` references
  rewritten to ``EXCLUDED.col``.
- **Lock anti-pattern detection** (Requirement 4.2): DSQL only allows
  ``SELECT ... FOR UPDATE`` when the locked query targets a *single table* and
  carries a *full primary-key equality predicate*. This converter has no schema
  or primary-key metadata, so it detects the structural anti-pattern
  conservatively and never asserts a definite primary-key violation it cannot
  prove (see :func:`_for_update_warnings`).
- **No silent data loss** (Requirement 4.3 / Property 6): unparseable SQL, or an
  idiom that cannot be converted losslessly (such as ``ON DUPLICATE KEY UPDATE``
  whose conflict target columns are unknown), is never silently dropped. It is
  flagged for manual review and the original SQL is always returned.
- **Side-by-side pair** (Requirement 4.4): every result carries both the
  ``original_sql`` and the ``converted_sql`` (``None`` only when the statement
  could not be parsed/rendered at all) so the two can be compared.

Classification mapping: results and warnings reuse the shared
:class:`~dsql_migrator.core.models.Classification` enum to stay consistent with
the assessor and the schema converter's ``ConversionWarning``. The design's
``MANUAL_REVIEW`` status maps to :attr:`Classification.MANUAL` (a human must
review/complete the conversion); statements with no findings are
:attr:`Classification.AUTO`.

Security: input SQL is treated as untrusted (Requirement 9.4). It is only parsed
and transpiled with ``sqlglot``; it is never executed or evaluated.

This converter intentionally has no primary-key/schema knowledge: that metadata
lives in the source inventory and is not threaded in here. Where a definite
``FOR UPDATE`` primary-key violation cannot be proven without it, the converter
emits a *verify* warning rather than asserting a violation.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Optional, Sequence

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType
from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.converter import (
    MYSQL_EXPRESSION_DIALECT,
    inline_having_aliases,
    rewrite_mysql_expressions,
    unquote_json_extracts,
)
from dsql_migrator.core.models import Classification, SourceType

_MYSQL = "mysql"
_POSTGRES = "postgres"


class StatementKind(str, Enum):
    """The category of a parsed SQL statement (drives what is safe to test-run).

    The Query Playground only ever probes the target non-destructively, and what
    "non-destructive" means depends on the statement:

    - ``SELECT`` -- a read; safe to validate against the live target with
      ``EXPLAIN`` (returns a plan, not data, and never writes).
    - ``DML`` -- ``INSERT`` / ``UPDATE`` / ``DELETE`` / ``REPLACE``; mutates data,
      so it is NEVER executed against the (production) target -- converted and
      analyzed only.
    - ``DDL`` -- ``CREATE`` / ``ALTER`` / ``DROP`` / ``TRUNCATE``; executable as a
      dry run inside a transaction that is always rolled back, so it proves the
      statement is accepted by DSQL without persisting anything.
    - ``OTHER`` -- anything else (e.g. ``SET``, ``SHOW``, transaction control, or a
      statement that could not be parsed); not test-run.
    """

    SELECT = "SELECT"
    DML = "DML"
    DDL = "DDL"
    OTHER = "OTHER"


# sqlglot expression types that represent data-mutating DML and schema DDL.
_DML_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
)
_DDL_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Create,
    exp.Alter,
    exp.Drop,
    exp.TruncateTable,
)


# Leading keywords used to classify a statement sqlglot could only parse as an
# opaque ``Command`` (unsupported-syntax fallback, e.g. MySQL ``REPLACE INTO``).
# Erring toward the mutating/DDL buckets here keeps such a statement OUT of the
# read-only test-run path.
_COMMAND_DML_KEYWORDS = frozenset({"REPLACE", "INSERT", "UPDATE", "DELETE", "MERGE", "LOAD"})
_COMMAND_DDL_KEYWORDS = frozenset(
    {"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"}
)


def _classify_command(tree: exp.Command) -> StatementKind:
    """Classify an opaque ``Command`` by its leading keyword (conservative)."""
    keyword = (tree.name or "").strip().upper()
    if keyword in _COMMAND_DML_KEYWORDS:
        return StatementKind.DML
    if keyword in _COMMAND_DDL_KEYWORDS:
        return StatementKind.DDL
    return StatementKind.OTHER


def classify_statement(tree: exp.Expression) -> StatementKind:
    """Classify a parsed statement into a :class:`StatementKind`.

    Inspects only the parsed ``sqlglot`` AST (never executes anything). A
    top-level ``SELECT`` (including one wrapped in a CTE/``WITH`` or set
    operation, or a parenthesized subquery) is ``SELECT``; data-mutating verbs
    are ``DML``; schema verbs are ``DDL``; everything else is ``OTHER``. A
    statement sqlglot can only parse as an opaque ``Command`` (e.g. MySQL
    ``REPLACE INTO``) is classified by its leading keyword, erring toward
    DML/DDL so it is never sent down the read-only test-run path.
    """
    if isinstance(tree, _DML_TYPES):
        return StatementKind.DML
    if isinstance(tree, _DDL_TYPES):
        return StatementKind.DDL
    if isinstance(tree, exp.Command):
        return _classify_command(tree)
    # A SELECT may be wrapped: WITH (cte) -> the inner Select, a UNION (SetOperation),
    # or a parenthesized subquery. Treat any statement that ultimately reads as
    # SELECT, but only when it is NOT a DML/DDL (guarded above).
    if isinstance(tree, (exp.Select, exp.Union, exp.Subquery)):
        return StatementKind.SELECT
    inner = tree.find(exp.Select)
    if inner is not None and tree.find(*_DML_TYPES, *_DDL_TYPES) is None:
        return StatementKind.SELECT
    return StatementKind.OTHER


def classify_sql(
    sql: str, *, source_type: "Optional[SourceType]" = None
) -> StatementKind:
    """Parse ``sql`` in the SOURCE dialect and classify it; ``OTHER`` when unparseable.

    A convenience wrapper over :func:`classify_statement` for callers that have a
    raw string rather than a parsed tree. Parsing only -- never executed.

    ``source_type`` selects the read dialect and defaults to MySQL (the original engine).
    It matters because the classification gates the read-only target probe: a statement
    that does not parse falls to ``OTHER``, which is not testable, so hard-coding MySQL
    made every PostgreSQL-only construct untestable on a PostgreSQL migration.
    """
    read = _POSTGRES if source_type is SourceType.POSTGRES else _MYSQL
    try:
        tree = sqlglot.parse_one(sql, read=read)
    except sqlglot.errors.ParseError:
        return StatementKind.OTHER
    if tree is None:
        return StatementKind.OTHER
    return classify_statement(tree)

# A single, reused description of the DSQL FOR UPDATE constraint so every lock
# warning explains the same rule (Requirement 4.2).
_FOR_UPDATE_CONSTRAINT = (
    "Aurora DSQL allows SELECT ... FOR UPDATE only on a single table with a "
    "full primary-key equality predicate"
)

# Warning codes. Plain string constants (not an enum) keep the result model
# minimal while still letting callers/tests branch on the kind of finding.
CODE_PARSE_ERROR = "PARSE_ERROR"
CODE_RENDER_ERROR = "RENDER_ERROR"
CODE_ON_DUPLICATE_KEY_UPDATE = "ON_DUPLICATE_KEY_UPDATE"
CODE_FOR_UPDATE_MULTI_TABLE = "FOR_UPDATE_MULTI_TABLE"
CODE_FOR_UPDATE_NO_EQUALITY = "FOR_UPDATE_NO_EQUALITY_PREDICATE"
CODE_FOR_UPDATE_VERIFY_PK = "FOR_UPDATE_VERIFY_PK"
# JSON_UNQUOTE that wraps something other than JSON_EXTRACT (no direct PG
# equivalent), left as-is and flagged for manual review.
CODE_JSON_UNQUOTE_UNSUPPORTED = "JSON_UNQUOTE_UNSUPPORTED"
# An unquoted table/column name with upper-case ASCII letters (a MySQL source).
CODE_MIXED_CASE_NAME = "MIXED_CASE_NAME"
# An unquoted mixed-case select-list alias: the result column's label is folded.
CODE_MIXED_CASE_LABEL = "MIXED_CASE_LABEL"
# A name quoted in one place and not in another (`O`.id over orders O).
CODE_QUOTE_MISMATCH = "QUOTE_MISMATCH"
# Two unquoted table aliases that differ only by case, in one scope or nested ones.
CODE_CASE_ONLY_ALIASES = "CASE_ONLY_ALIASES"
# MySQL's multi-table UPDATE / DELETE (UPDATE ... JOIN, DELETE t FROM ... JOIN).
CODE_MULTI_TABLE_DML = "MULTI_TABLE_DML"
# A MySQL-only DML clause Aurora DSQL has no syntax for: DELETE / UPDATE ... ORDER BY /
# LIMIT, the IGNORE modifier, REPLACE.
CODE_MYSQL_ONLY_DML = "MYSQL_ONLY_DML"
# A MySQL call the conversion leaves as written (a MySQL-only or user function): it fails
# on Aurora DSQL unless the function exists there.
CODE_UNTRANSLATED_CALL = "UNTRANSLATED_CALL"
# A DATE_FORMAT / STR_TO_DATE specifier Aurora DSQL has no pattern for.
CODE_UNMAPPED_DATE_FORMAT = "UNMAPPED_DATE_FORMAT"
# A MySQL expression whose result Aurora DSQL computes differently (STR_TO_DATE on a value
# that does not match, a format read at run time, ...).
CODE_MYSQL_SEMANTICS = "MYSQL_SEMANTICS"

# Severity ordering used to derive the overall result classification from its
# warnings (higher value is more severe). Matches the assessor's convention.
_SEVERITY: dict[Classification, int] = {
    Classification.AUTO: 0,
    Classification.MANUAL: 1,
    Classification.UNSUPPORTED: 2,
}


class QueryWarning(BaseModel):
    """A structured finding about a single converted query.

    Mirrors the schema converter's ``ConversionWarning`` shape but is
    query-specific. ``code`` is one of the module ``CODE_*`` constants and lets
    callers branch on the kind of finding (lock anti-pattern, unconvertible
    idiom, parse failure). ``classification`` reuses the shared
    :class:`Classification` enum (``MANUAL`` for the design's ``MANUAL_REVIEW``).
    """

    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1, description="Stable warning code (CODE_* constant).")
    classification: Classification = Field(
        description="Severity: MANUAL (review) or UNSUPPORTED (redesign)."
    )
    message: str = Field(min_length=1, description="Human-readable reason (English).")


class QueryConversionResult(BaseModel):
    """The converted form of one query plus its warnings (Requirement 4.4).

    ``converted_sql`` holds the PostgreSQL-dialect SQL alongside the unchanged
    ``original_sql`` so the two can be shown side by side. ``converted_sql`` is
    ``None`` only when the statement could not be parsed or rendered at all; in
    that case the original SQL is still returned and a warning explains why
    (Property 6: no silent data loss). ``classification`` is the most severe
    warning classification, or ``AUTO`` when there are no warnings.
    """

    model_config = ConfigDict(extra="forbid")

    original_sql: str = Field(min_length=1, description="The unmodified input SQL.")
    converted_sql: Optional[str] = Field(
        default=None,
        description="PostgreSQL-dialect SQL, or None when conversion was not possible.",
    )
    classification: Classification = Field(
        description="Overall classification: AUTO, MANUAL (review), or UNSUPPORTED."
    )
    statement_kind: StatementKind = Field(
        default=StatementKind.OTHER,
        description="Statement category (SELECT/DML/DDL/OTHER) for safe test-run.",
    )
    warnings: list[QueryWarning] = Field(default_factory=list)


def _overall_classification(warnings: list[QueryWarning]) -> Classification:
    """Return the most severe classification among ``warnings`` (AUTO if none)."""
    if not warnings:
        return Classification.AUTO
    return max(warnings, key=lambda w: _SEVERITY[w.classification]).classification


def _rewrite_on_duplicate_key_update(tree: exp.Expression) -> Optional[QueryWarning]:
    """Rewrite ``ON DUPLICATE KEY UPDATE`` to a PostgreSQL ``ON CONFLICT`` form.

    MySQL's ``INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)`` is rewritten
    in place to ``INSERT ... ON CONFLICT DO UPDATE SET col = EXCLUDED.col``. The
    PostgreSQL ``DO UPDATE`` form requires an explicit conflict target (the
    unique/primary-key columns), which cannot be inferred without schema
    metadata. Rather than silently emit a statement that may not match the
    intended key, this returns a ``MANUAL`` warning so the user supplies the
    conflict target (Property 6). Returns ``None`` when the statement has no
    ``ON DUPLICATE KEY UPDATE`` clause.
    """
    on_conflict = tree.find(exp.OnConflict)
    if on_conflict is None or not on_conflict.args.get("duplicate"):
        return None

    # Convert the MySQL "duplicate key" form into a PostgreSQL ON CONFLICT form.
    on_conflict.set("duplicate", False)
    on_conflict.set("action", exp.var("DO UPDATE"))

    # MySQL's VALUES(col) refers to the row proposed for insertion; the
    # PostgreSQL equivalent is EXCLUDED.col.
    for anonymous in list(on_conflict.find_all(exp.Anonymous)):
        if anonymous.name.upper() == "VALUES" and anonymous.expressions:
            column = anonymous.expressions[0]
            anonymous.replace(exp.column(column.name, table="EXCLUDED"))

    return QueryWarning(
        code=CODE_ON_DUPLICATE_KEY_UPDATE,
        classification=Classification.MANUAL,
        message=(
            "INSERT ... ON DUPLICATE KEY UPDATE was converted to "
            "INSERT ... ON CONFLICT DO UPDATE SET. PostgreSQL/DSQL requires an "
            "explicit conflict target (the unique/primary-key columns), which "
            "cannot be inferred here; add the conflict target and verify the "
            "rewrite."
        ),
    )


def _is_json_unquote(node: exp.Expression) -> bool:
    """Return ``True`` for a ``JSON_UNQUOTE(...)`` call (parsed as ``Anonymous``)."""
    return (
        isinstance(node, exp.Anonymous)
        and node.name.upper() == "JSON_UNQUOTE"
        and len(node.expressions) == 1
    )


def _rewrite_json_unquote(tree: exp.Expression) -> list[QueryWarning]:
    """Rewrite MySQL ``JSON_UNQUOTE`` to a DSQL-compatible form (Property 6).

    PostgreSQL/Aurora DSQL has no ``JSON_UNQUOTE`` function. The common MySQL idiom
    ``JSON_UNQUOTE(JSON_EXTRACT(col, '$.k'))`` means "extract the value as
    unquoted text", which is exactly PostgreSQL's scalar extraction
    (``JSON_EXTRACT_PATH_TEXT`` / the ``->>`` operator). sqlglot renders a bare
    ``JSON_EXTRACT`` as ``JSON_EXTRACT_PATH`` (returns ``json``, keeps quotes) and
    leaves ``JSON_UNQUOTE`` as an unknown function -- which the target rejects with
    ``function json_unquote(json) does not exist``. This rewrites the
    ``JSON_UNQUOTE(JSON_EXTRACT(...))`` pair in place to a single
    :class:`sqlglot.exp.JSONExtractScalar`, which the expression rewrite writes as MySQL
    reads it (``JSON_EXTRACT_PATH_TEXT(col, 'k', ...)``, a JSON null as ``'null'``) and runs
    on DSQL -- the converter's own rewrite (:func:`unquote_json_extracts`), which a view's
    ``->>`` gets too.

    A ``JSON_UNQUOTE`` wrapping anything else has no automatic equivalent, so it is
    left unchanged and flagged ``MANUAL`` rather than silently emitting SQL the
    target will reject (Property 6) -- one over a JSON_EXTRACT whose path Aurora DSQL
    cannot follow is reported by the expression rewrite, which names the path. Returns
    the warnings produced (empty when there was no ``JSON_UNQUOTE`` or every occurrence
    was rewritten cleanly).
    """
    unquote_json_extracts(tree)
    warnings: list[QueryWarning] = []
    for node in list(tree.find_all(exp.Anonymous)):
        if _is_json_unquote(node) and not isinstance(node.expressions[0], exp.JSONExtract):
            warnings.append(
                QueryWarning(
                    code=CODE_JSON_UNQUOTE_UNSUPPORTED,
                    classification=Classification.MANUAL,
                    message=(
                        "JSON_UNQUOTE has no Aurora DSQL (PostgreSQL) equivalent and "
                        "was left unchanged. It is auto-converted only in the common "
                        "JSON_UNQUOTE(JSON_EXTRACT(col, '$.key')) form (-> "
                        "JSON_EXTRACT_PATH_TEXT / the ->> operator); rewrite this "
                        "usage manually to extract JSON as text."
                    ),
                )
            )
    return warnings


def _inline_having_aliases(tree: exp.Expression, *, case_insensitive: bool = True) -> None:
    """MySQL's HAVING aliases inlined as a view's are: :func:`inline_having_aliases`."""
    inline_having_aliases(tree, case_insensitive=case_insensitive)


def _mysql_expression_warnings(
    untranslated: "Sequence[str]", misformatted: "Sequence[str]", issues: "Sequence[str]"
) -> "list[QueryWarning]":
    """The Query Converter's findings for what :func:`rewrite_mysql_expressions` could not
    write faithfully -- the same as a view's in Schema Conversion."""
    warnings: list[QueryWarning] = []
    if untranslated:
        one = len(untranslated) == 1
        warnings.append(QueryWarning(
            code=CODE_UNTRANSLATED_CALL,
            classification=Classification.MANUAL,
            message=(
                f"The statement calls {', '.join(f'{name}()' for name in untranslated)}, "
                "which the conversion does not translate (a MySQL-only or user-defined "
                f"function): it fails on Aurora DSQL unless {'it exists' if one else 'they exist'} "
                f"there. Rewrite {'it' if one else 'them'} before running it."
            ),
        ))
    if misformatted:
        one = sum(entry.count("%") for entry in misformatted) == 1
        warnings.append(QueryWarning(
            code=CODE_UNMAPPED_DATE_FORMAT,
            classification=Classification.MANUAL,
            message=(
                f"The statement formats a date with {', '.join(misformatted)}: "
                f"{'that MySQL format token has' if one else 'those MySQL format tokens have'} "
                "no Aurora DSQL (TO_CHAR / TO_TIMESTAMP) pattern (MySQL's week numbers other "
                f"than ISO's, and its 0-based day of week), so {'it is' if one else 'they are'} "
                "written as literal text -- rewrite the format before running it."
            ),
        ))
    for text in issues:
        warnings.append(QueryWarning(
            code=CODE_MYSQL_SEMANTICS, classification=Classification.MANUAL,
            message=text,
        ))
    return warnings


def _has_equality_predicate(where: exp.Where) -> bool:
    """Return ``True`` if ``where`` contains a top-level ``column = value``.

    Only top-level AND-connected conjuncts are considered an equality predicate;
    a top-level ``OR`` matches multiple rows and is treated as not a simple
    equality. An equality is an ``=`` comparison with a column on one side and a
    non-column (literal or bound parameter) on the other.
    """
    predicate = where.this
    if predicate is None:
        return False
    # A top-level OR can match multiple rows: not a simple equality lock.
    if isinstance(predicate, exp.Or):
        return False
    conjuncts = predicate.flatten() if isinstance(predicate, exp.And) else [predicate]
    for conjunct in conjuncts:
        if not isinstance(conjunct, exp.EQ):
            continue
        left, right = conjunct.this, conjunct.expression
        left_is_column = isinstance(left, exp.Column)
        right_is_column = isinstance(right, exp.Column)
        if left_is_column != right_is_column:
            return True
    return False


def _for_update_warnings(tree: exp.Expression) -> list[QueryWarning]:
    """Return warnings for every ``SELECT ... FOR UPDATE`` in ``tree``.

    DSQL only permits ``FOR UPDATE`` on a single table with a full primary-key
    equality predicate (Requirement 4.2). Detection is conservative because no
    primary-key metadata is available here:

    - More than one table (a join or comma-join) is a definite violation.
    - A single table with no ``WHERE`` or no equality predicate (a range or
      multi-row lock) is a definite violation.
    - A single table with an equality predicate cannot be proven to satisfy the
      *full primary-key* requirement without schema metadata, so it yields a
      *verify* warning instead of a false assertion.
    """
    warnings: list[QueryWarning] = []
    for lock in tree.find_all(exp.Lock):
        if not lock.args.get("update"):
            continue
        select = lock.find_ancestor(exp.Select)
        if select is None:
            continue

        joins = select.args.get("joins") or []
        from_clause = next(
            (arg for arg in select.args.values() if isinstance(arg, exp.From)), None
        )
        table_count = (1 if from_clause is not None else 0) + len(joins)
        where = select.args.get("where")

        if table_count > 1:
            warnings.append(
                QueryWarning(
                    code=CODE_FOR_UPDATE_MULTI_TABLE,
                    classification=Classification.MANUAL,
                    message=(
                        f"{_FOR_UPDATE_CONSTRAINT}; this FOR UPDATE locks "
                        f"{table_count} tables (join), which violates the "
                        "constraint. Lock a single table by primary key instead."
                    ),
                )
            )
        elif where is None or not _has_equality_predicate(where):
            warnings.append(
                QueryWarning(
                    code=CODE_FOR_UPDATE_NO_EQUALITY,
                    classification=Classification.MANUAL,
                    message=(
                        f"{_FOR_UPDATE_CONSTRAINT}; this FOR UPDATE has no "
                        "primary-key equality predicate (a range or multi-row "
                        "lock), which violates the constraint."
                    ),
                )
            )
        else:
            warnings.append(
                QueryWarning(
                    code=CODE_FOR_UPDATE_VERIFY_PK,
                    classification=Classification.MANUAL,
                    message=(
                        f"{_FOR_UPDATE_CONSTRAINT}; this FOR UPDATE targets a "
                        "single table with an equality predicate. Verify the "
                        "predicate covers the full primary key, as primary-key "
                        "metadata is not available to confirm it here."
                    ),
                )
            )
    return warnings


def _drop_from_dual(tree: exp.Expression) -> None:
    """``SELECT ... FROM DUAL`` without its FROM, in place: MySQL's DUAL is no table on
    Aurora DSQL (the converted SQL failed with 'relation "dual" does not exist'), and a
    PostgreSQL SELECT needs no FROM, WHERE included. Only a FROM that is DUAL alone, and
    never a backquoted `dual`: MySQL's keyword is never quoted, so that is a real table."""
    for select in tree.find_all(exp.Select):
        source = select.args.get("from_") or select.args.get("from")
        table = getattr(source, "this", None)
        if (
            isinstance(table, exp.Table)
            and _is_dual_keyword(table)
            and not table.alias
            and not select.args.get("joins")
        ):
            select.set("from_" if "from_" in select.args else "from", None)


_ASCII_FOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _is_dual_keyword(table: exp.Table) -> bool:
    """MySQL's DUAL: unqualified and unquoted (a backquoted `dual` is a real table)."""
    this = table.args.get("this")
    return (
        not table.args.get("db")
        and isinstance(this, exp.Identifier)
        and not this.quoted
        and this.name.lower() == "dual"
    )


def _key(identifier: object) -> str:
    """How PostgreSQL names ``identifier``: as written when quoted, folded when not -- so a
    backquoted `O` and an unquoted O are two names on Aurora DSQL."""
    if isinstance(identifier, exp.TableAlias):
        identifier = identifier.this
    if not isinstance(identifier, exp.Identifier):
        return ""
    name = identifier.name or ""
    return name if identifier.quoted else name.translate(_ASCII_FOLD)


def _output_identifiers(
    query: Optional[exp.Expression], alias: Optional[exp.Expression]
) -> "list[exp.Identifier]":
    """The identifiers that name what a derived table or CTE puts out: its column list,
    else its select list's labels (an alias, or a column's own name)."""
    columns = [c for c in (getattr(alias, "columns", None) or []) if isinstance(c, exp.Identifier)]
    if columns:
        return columns
    while isinstance(query, (exp.SetOperation, exp.Subquery)):
        query = query.this  # a UNION's names are its first branch's
    if not isinstance(query, exp.Select):
        return []
    found: list[exp.Identifier] = []
    for projection in query.expressions:
        if isinstance(projection, exp.Alias):
            label = projection.args.get("alias")
        elif isinstance(projection, exp.Column):
            label = projection.args.get("this")
        else:
            label = None
        if isinstance(label, exp.Identifier):
            found.append(label)
    return found


def _with_of(query: exp.Expression) -> Optional[exp.With]:
    found = query.args.get("with_") or query.args.get("with")
    return found if isinstance(found, exp.With) else None


# The nodes a name's scope is decided at: a query level or a DML statement.
_SCOPE_NODES = (exp.Select, exp.SetOperation, exp.Update, exp.Delete, exp.Insert)


def _scope_of(node: exp.Expression) -> Optional[exp.Expression]:
    node = node.parent
    while node is not None and not isinstance(node, _SCOPE_NODES):
        node = node.parent
    return node


def _from_items(select: exp.Expression) -> "list[exp.Expression]":
    """The FROM items a query level or DML statement names (a Select's FROM and JOINs, an
    UPDATE / DELETE target and its joins, DELETE's USING), a parenthesised join's own
    items included."""
    items: list = []
    if isinstance(select, (exp.Update, exp.Delete)):
        items.append(select.this)
        for part in ("from_", "from"):
            source = select.args.get(part)
            if source is not None:
                items.append(getattr(source, "this", None))
        using = select.args.get("using")
        if isinstance(using, list):
            items.extend(using)
    elif isinstance(select, exp.Select):
        source = select.args.get("from_") or select.args.get("from")
        items.append(getattr(source, "this", None))
        items.extend(j.this for j in (select.args.get("joins") or []))
    out: list = []
    for item in items:
        if isinstance(item, exp.Expression):
            out.extend(_source_items(item))
    return out


def _is_join_container(item: Optional[exp.Expression]) -> bool:
    """A parenthesised join -- (b JOIN c) reads as Subquery(Table(b, joins=[...])),
    ((b JOIN c) JOIN d) as Subquery(Subquery(...), joins=[...]), and ((SELECT ...) x JOIN
    c), the form MySQL stores every join that starts with a derived table in, as
    Subquery(Subquery(..., alias=x, joins=[...])) -- not a derived table: no alias, and a
    table or an aliased derived table at the bottom (``((SELECT ...)) AS x`` is one)."""
    if not isinstance(item, exp.Subquery) or item.args.get("alias"):
        return False
    inner = item.this
    while isinstance(inner, exp.Subquery) and not inner.args.get("alias"):
        inner = inner.this
    return isinstance(inner, (exp.Table, exp.Subquery))


def _source_items(item: Optional[exp.Expression]) -> "list[exp.Expression]":
    """The sources one FROM entry brings into scope, in written order: a table, a derived
    table, a LATERAL, and a parenthesised join's own entries -- whose joins sqlglot hangs
    on its first entry, a table or a derived table alike."""
    out: list = []
    pending = [item] if isinstance(item, exp.Expression) else []
    while pending:
        current = pending.pop(0)
        joined = [j.this for j in (current.args.get("joins") or []) if j.this is not None]
        if _is_join_container(current):
            pending[:0] = [current.this] + joined
            continue
        out.append(current)
        pending[:0] = joined  # ((b JOIN c) JOIN d): c before d
    return out


class _Scopes:
    """The statement's own names, resolved per scope and memoized for one scan (a wide
    statement asked for them once per column made the scan quadratic)."""

    def __init__(self, tree: exp.Expression) -> None:
        self.aliases = {_key(n) for n in tree.find_all(exp.TableAlias) if n.name}
        self._ctes: dict = {}
        self._outputs: dict[int, Optional[set]] = {}
        self._labels: dict[int, set] = {}
        self._items: dict[int, list] = {}
        self._outer: dict[int, Optional[exp.Expression]] = {}
        self._names: dict = {}
        self._bound: dict = {}
        # Each node's enclosing scope, query level, and whether it sits in its query's
        # ORDER BY / GROUP BY / HAVING (None inside a window, which cannot name an alias)
        # -- in ONE top-down pass: walked up per column, a long AND/OR chain made the scan
        # quadratic in its depth.
        self._enclosing: dict[int, Optional[exp.Expression]] = {}
        self._query: dict[int, Optional[exp.Expression]] = {}
        self._clause: dict[int, Optional[bool]] = {}
        stack: list = [(tree, None, None, False)]
        while stack:
            node, scope, query, clause = stack.pop()
            self._enclosing[id(node)] = scope
            self._query[id(node)] = query
            self._clause[id(node)] = clause
            child_scope = node if isinstance(node, _SCOPE_NODES) else scope
            child_query = node if isinstance(node, (exp.Select, exp.SetOperation)) else query
            if isinstance(node, exp.Select):
                child_clause: Optional[bool] = False
            elif isinstance(node, exp.Window) or clause is None:
                child_clause = None
            elif isinstance(node, (exp.Order, exp.Group, exp.Having)):
                child_clause = True
            else:
                child_clause = clause
            for child in node.iter_expressions():
                stack.append((child, child_scope, child_query, child_clause))

    def scope_of(self, node: exp.Expression) -> Optional[exp.Expression]:
        """The scope enclosing ``node`` (its query level or DML statement)."""
        if id(node) in self._enclosing:
            return self._enclosing[id(node)]
        return _scope_of(node)

    def in_alias_clause(self, column: exp.Column) -> bool:
        """In ORDER BY / GROUP BY / HAVING -- where a bare name may be a select-list alias
        -- and not in a window's ORDER BY, which cannot name one."""
        if id(column) in self._clause:
            return self._clause[id(column)] is True
        return _in_alias_clause(column)

    def visible_ctes(self, node: exp.Expression) -> "dict[str, exp.CTE]":
        """The CTEs a name at ``node`` can refer to, by key, innermost first: every CTE of
        an enclosing WITH whose query ``node`` is in, and inside a CTE's own body only the
        CTEs before it (all of them in a WITH RECURSIVE). Memoized per scope, each scope's
        from its enclosing one's, so a deep UNION chain costs one walk."""
        anchor = node if isinstance(node, _SCOPE_NODES) else self.scope_of(node)
        if anchor is None:
            return {}
        if id(anchor) in self._ctes:
            return self._ctes[id(anchor)]
        visible: dict[str, exp.CTE] = {}
        own = _with_of(anchor)
        if own is not None:
            for cte in own.expressions:
                if isinstance(cte, exp.CTE):
                    visible.setdefault(_key(cte.args.get("alias")), cte)
        for key, cte in self._above(anchor).items():
            visible.setdefault(key, cte)
        self._ctes[id(anchor)] = visible
        return visible

    def _above(self, scope: exp.Expression) -> "dict[str, exp.CTE]":
        """What the scopes enclosing ``scope`` make visible to it (iterative: a UNION of
        thousands of branches nests that deep)."""
        chain: list = []
        node = scope
        while node is not None and ("above", id(node)) not in self._ctes:
            chain.append(node)
            node = self.scope_of(node)
        inherited = self._ctes[("above", id(node))] if node is not None else {}
        for current in reversed(chain):
            visible: dict[str, exp.CTE] = {}
            child, parent = current, current.parent
            through_with = False
            while parent is not None and not isinstance(parent, _SCOPE_NODES):
                if isinstance(parent, exp.With):
                    through_with = True
                    ctes = [c for c in parent.expressions if isinstance(c, exp.CTE)]
                    position = next((i for i, c in enumerate(ctes) if c is child), None)
                    allowed = (
                        ctes if position is None or parent.args.get("recursive") else ctes[:position]
                    )
                    for cte in allowed:
                        visible.setdefault(_key(cte.args.get("alias")), cte)
                child, parent = parent, parent.parent
            if parent is not None:
                own = _with_of(parent)
                if own is not None and not (through_with and child is own):
                    for cte in own.expressions:
                        if isinstance(cte, exp.CTE):
                            visible.setdefault(_key(cte.args.get("alias")), cte)
                for key, cte in inherited.items():
                    visible.setdefault(key, cte)
            self._ctes[("above", id(current))] = visible
            inherited = visible
        return self._ctes[("above", id(scope))]

    def outputs(self, query, alias) -> "Optional[set]":
        """The output keys of a derived table or CTE: its explicit labels, as an
        :class:`_OpenOutputs` when it also puts out a ``*`` (more columns, a real table's,
        unknown here)."""
        memo = id(alias) if alias is not None else id(query)
        if memo not in self._outputs:
            labels = {_key(i) for i in _output_identifiers(query, alias) if i.name}
            self._outputs[memo] = _OpenOutputs(labels) if _projects_star(query, alias) else labels
        return self._outputs[memo]

    def source_outputs(self, item: Optional[exp.Expression]) -> "Optional[set]":
        """The output keys of a FROM item the statement defines itself (a derived table, a
        LATERAL, or a table naming a visible CTE), or ``None`` for a real table (or one
        whose outputs are a real table's: a ``SELECT *``)."""
        if isinstance(item, exp.Lateral) and isinstance(item.this, exp.Subquery):
            return self.outputs(item.this.this, item.args.get("alias"))
        if isinstance(item, exp.Subquery) and not _is_join_container(item):
            return self.outputs(item.this, item.args.get("alias"))
        if isinstance(item, exp.Table) and not item.args.get("db") and item.name:
            cte = self.visible_ctes(item).get(_key(item.args.get("this")))
            if cte is not None:
                return self.outputs(cte.this, cte.args.get("alias"))
        return None

    def items(self, scope: exp.Expression) -> list:
        if id(scope) not in self._items:
            self._items[id(scope)] = _from_items(scope)
        return self._items[id(scope)]

    def labels(self, column: exp.Column) -> set:
        """The keys a bare ORDER BY / GROUP BY / HAVING name of ``column``'s query can mean:
        its select-list aliases, and for a set operation its first branch's labels."""
        if id(column) in self._query:
            node = self._query[id(column)]
        else:
            node = column.parent
            while node is not None and not isinstance(node, (exp.Select, exp.SetOperation)):
                node = node.parent
        if node is None:
            return set()
        if id(node) in self._labels:
            return self._labels[id(node)]
        if isinstance(node, exp.SetOperation):
            found = {_key(i) for i in _output_identifiers(node, None)}
        else:
            found = {
                _key(projection.args.get("alias"))
                for projection in node.expressions
                if isinstance(projection, exp.Alias) and projection.alias
            }
        self._labels[id(node)] = found
        return found

    def qualified_source(self, column: exp.Column, qualifier: str) -> "tuple[bool, Optional[set]]":
        """``(found, outputs)`` for the FROM item ``qualifier`` names in the column's scope
        (its own query level or statement, then the enclosing ones a correlated reference
        reaches): ``outputs`` is the item's output keys when the statement defines it,
        ``None`` for a real table."""
        scope = self.scope_of(column)
        item = self.bound(scope, qualifier) if scope is not None else None
        if item is None:
            return False, None
        return True, self.source_outputs(item)

    def bound(
        self, scope: exp.Expression, name: str, *, written: bool = False
    ) -> "Optional[exp.Expression]":
        """The FROM item a qualifier ``name`` in ``scope`` reaches: the scope's own, else the
        nearest enclosing scope's (a correlated reference). ``written`` matches the name as
        MySQL does (as written, quoted or not), else as PostgreSQL keys it. Memoized per
        scope and name, each scope's from the one around it: walked per column, a UNION of
        thousands of branches reaching one outer alias was quadratic."""
        chain: list = []
        node: Optional[exp.Expression] = scope
        found = None
        while node is not None:
            memo = (id(node), written, name)
            if memo in self._bound:
                found = self._bound[memo]
                break
            chain.append(node)
            found = self._named(node, written).get(name)
            if found is not None:
                break
            node = self._enclosing_items_scope(node)
        for current in chain:
            self._bound[(id(current), written, name)] = found
        return found

    def _named(self, scope: exp.Expression, written: bool) -> dict:
        """``scope``'s FROM items by the name a qualifier reaches them by (the first of a
        name wins, as in the scope's own list)."""
        memo = (id(scope), written)
        if memo not in self._names:
            named: dict = {}
            for item in self.items(scope):
                key = _written_name(item) if written else _item_key(item)
                if key:
                    named.setdefault(key, item)
            self._names[memo] = named
        return self._names[memo]

    def _enclosing_items_scope(self, scope: exp.Expression) -> "Optional[exp.Expression]":
        """The nearest scope around ``scope`` that lists FROM items: a UNION level lists none,
        so every branch of a long UNION steps straight to the query around it."""
        if id(scope) in self._outer:
            return self._outer[id(scope)]
        chain = [scope]
        node = self.scope_of(scope)
        while node is not None and not self.items(node):
            if id(node) in self._outer:
                node = self._outer[id(node)]
                break
            chain.append(node)
            node = self.scope_of(node)
        for current in chain:
            self._outer[id(current)] = node
        return node


class _OpenOutputs(set):
    """A derived table's / CTE's explicit labels when it also puts out ``*``: a name among
    them is its own (MySQL refuses a duplicate column there), any other may be a real
    table's column."""


def _holds(outputs: "Optional[set]", key: str) -> "Optional[bool]":
    """Whether a source puts out ``key``: True / False when known, None when it may (a
    real table, or a ``*`` beyond its explicit labels)."""
    if outputs is None:
        return None
    if key in outputs:
        return True
    return None if isinstance(outputs, _OpenOutputs) else False


def _own_name(sources: "list", key: str) -> bool:
    """``key`` is a name the statement's own sources put out: one of them holds it for
    certain. MySQL refuses an unqualified name two sources hold ('Column ... is
    ambiguous'), so in a statement that runs there it resolves to that one, even beside a
    real table (the top-N-per-group idiom joined to the table it names)."""
    return any(_holds(o, key) is True for o in sources)


def _projects_star(query: Optional[exp.Expression], alias: Optional[exp.Expression]) -> bool:
    """A derived table or CTE whose first branch puts out ``*`` / ``t.*`` and that names no
    columns of its own."""
    if [c for c in (getattr(alias, "columns", None) or []) if isinstance(c, exp.Identifier)]:
        return False
    while isinstance(query, (exp.SetOperation, exp.Subquery)):
        query = query.this
    return isinstance(query, exp.Select) and any(
        isinstance(p, exp.Star) or isinstance(p, exp.Column) and isinstance(p.this, exp.Star)
        for p in query.expressions
    )


def _item_key(item: Optional[exp.Expression]) -> str:
    """The key a FROM item is referenced by: its alias, else a table's name."""
    if item is None:
        return ""
    alias = item.args.get("alias")
    if isinstance(alias, exp.TableAlias) and alias.name:
        return _key(alias)
    if isinstance(item, exp.Table):
        return _key(item.args.get("this"))
    return ""


def _written_name(item: Optional[exp.Expression]) -> str:
    """The name MySQL references a FROM item by -- its alias, else a table's name -- as
    written, quoted or not: MySQL compares table aliases by case either way."""
    if item is None:
        return ""
    alias = item.args.get("alias")
    if isinstance(alias, exp.TableAlias) and alias.name:
        return alias.name
    if isinstance(item, exp.Table):
        return item.name
    return ""


def _in_alias_clause(column: exp.Column) -> bool:
    """A column in ORDER BY / GROUP BY / HAVING -- where a bare name may be a select-list
    alias -- and not in a window's ORDER BY, which cannot name one."""
    node = column.parent
    while node is not None and not isinstance(node, exp.Select):
        if isinstance(node, exp.Window):
            return False
        if isinstance(node, (exp.Order, exp.Group, exp.Having)):
            return True
        node = node.parent
    return False


def _mixed_case_names(
    tree: exp.Expression,
    scopes: "Optional[_Scopes]" = None,
    mismatched: "Optional[list[str]]" = None,
) -> "list[str]":
    """The table and column names the statement writes unquoted with upper-case ASCII
    letters, in order: see :func:`_mixed_case_name_warning`. ``mismatched`` collects the
    names quoted in one place and not in another (`O`.id over orders O), which Aurora DSQL
    no longer matches: see :func:`_quote_mismatch_warning`."""
    # Only REFERENCES to a name the statement defines are skipped -- resolved in their own
    # scope, and matched as PostgreSQL names them (a backquoted `O` is not an unquoted O)
    # -- because they name the same thing where defined and where used: a qualifier naming
    # a FROM item's alias or a visible CTE, a table naming a visible CTE, a bare ORDER BY /
    # GROUP BY / HAVING name equal to its query's select alias, a derived table's or CTE's
    # own output column, a USING name only the statement's own sources hold, and DUAL. The
    # table that carries an alias, the column inside an aliased expression and an INSERT's
    # column list are real names.
    scopes = scopes or _Scopes(tree)
    names: list[str] = []

    def _mismatch(part) -> None:
        if mismatched is not None and isinstance(part, exp.Identifier) and part.name not in mismatched:
            mismatched.append(part.name)

    def _flag(part) -> None:
        if (
            isinstance(part, exp.Identifier)
            and not part.quoted
            and any("A" <= ch <= "Z" for ch in part.name)
            and part.name not in names
        ):
            names.append(part.name)

    # MySQL's DELETE t FROM ... lists the aliases it deletes from: names of the statement's
    # own (the statement is flagged as MySQL-only DML anyway).
    delete_targets = {
        id(t) for d in tree.find_all(exp.Delete) for t in (d.args.get("tables") or [])
    } | {
        id(d.this) for d in tree.find_all(exp.Delete)
        if isinstance(d.args.get("using"), list) and isinstance(d.this, exp.Table)
    }
    for table in tree.find_all(exp.Table):
        this = table.args.get("this")
        if _is_dual_keyword(table):
            continue
        if id(table) in delete_targets and _key(this) in scopes.aliases:
            continue
        if (
            not table.args.get("db")
            and isinstance(this, exp.Identifier)
            and _key(this) in scopes.visible_ctes(table)
        ):
            continue
        for part in (this, table.args.get("db"), table.args.get("catalog")):
            _flag(part)
    # An INSERT's column list names the target table's columns.
    for schema in tree.find_all(exp.Schema):
        if isinstance(schema.parent, exp.Insert):
            for part in schema.expressions:
                _flag(part)
    for join in tree.find_all(exp.Join):
        using = join.args.get("using") or []
        if not using:
            continue
        # The join's right side, and what it joins to (every source listed before it).
        right = [scopes.source_outputs(i) for i in _source_items(join.this)]
        scope = scopes.scope_of(join)
        listed = scopes.items(scope) if scope is not None else []
        right_ids = {id(i) for i in _source_items(join.this)}
        left: list = []
        for item in listed:
            if id(item) in right_ids:
                break
            left.append(scopes.source_outputs(item))
        for part in using:
            key = _key(part)
            if right and left and _own_name(right, key) and _own_name(left, key):
                continue  # only the statement's own outputs, on both sides
            _flag(part)
    for column in tree.find_all(exp.Column):
        qualifier = column.args.get("table")
        this = column.args.get("this")
        qualifier_key = (
            _key(qualifier)
            if isinstance(qualifier, exp.Identifier) and not column.args.get("db")
            else None
        )
        found, outputs = (
            scopes.qualified_source(column, qualifier_key)
            if qualifier_key is not None
            else (False, None)
        )
        if (
            qualifier_key is not None
            and not found
            and isinstance(qualifier, exp.Identifier)
            and qualifier.quoted
        ):
            # `O`.id over orders O: the alias folds to o, the quoted reference stays O.
            folded = qualifier.name.translate(_ASCII_FOLD)
            if folded != qualifier.name and scopes.qualified_source(column, folded)[0]:
                _mismatch(qualifier)
                found, outputs = scopes.qualified_source(column, folded)
        # A qualifier naming a FROM item's alias, or a CTE, is the statement's own name --
        # and so is one naming an alias defined anywhere else in it (an INSERT row alias).
        if not (
            qualifier_key is not None
            and (
                (found and qualifier_key in scopes.aliases)
                or outputs is not None
                or qualifier_key in scopes.visible_ctes(column)
                or (not found and qualifier_key in scopes.aliases)
            )
        ):
            _flag(qualifier)
        _flag(column.args.get("db"))
        if isinstance(this, exp.Identifier):
            key = _key(this)
            if outputs is not None and key in outputs:
                continue  # a derived table's / CTE's own output column
            if (
                outputs is not None
                and this.quoted
                and key.translate(_ASCII_FOLD) != key
                and key.translate(_ASCII_FOLD) in outputs
            ):
                _mismatch(this)  # d.`UserId` over an unquoted output UserId (userid)
                continue
            if qualifier is None:
                if key in scopes.labels(column) and scopes.in_alias_clause(column):
                    continue
                if not this.quoted and key in ("utc_timestamp", "utc_date", "utc_time"):
                    continue  # MySQL's UTC clock written without parentheses
                scope = scopes.scope_of(column)
                sources = [scopes.source_outputs(i) for i in scopes.items(scope)] if scope is not None else []
                if _own_name(sources, key):
                    continue  # only the statement's own outputs are in scope
                folded = key.translate(_ASCII_FOLD)
                if this.quoted and folded != key and _own_name(sources, folded):
                    _mismatch(this)  # `UserId` over an unquoted derived output UserId
                    continue
        _flag(this)
    return names


def _mixed_case_name_warning(names: "Sequence[str]") -> "Optional[QueryWarning]":
    """MANUAL when the statement names a table or column unquoted with upper-case ASCII
    letters (``names``, from :func:`_mixed_case_names`). MySQL matched it whatever the
    case; Aurora DSQL folds an unquoted name to lower case while Schema Conversion keeps
    the source spelling (quoted), so the same SQL finds nothing after cut over. Quoting is
    not done for the operator: this converter does not know the schema, and "Orders"
    would break a table actually named orders."""
    if not names:
        return None
    return QueryWarning(
        code=CODE_MIXED_CASE_NAME,
        classification=Classification.MANUAL,
        message=(
            f"Name(s) {', '.join(names[:5])}{' ...' if len(names) > 5 else ''} are written "
            "unquoted with upper-case letters. Aurora DSQL folds an unquoted name to lower "
            "case, while Schema Conversion keeps each name as the source spells it (quoted), "
            "so this SQL finds nothing after cut over: quote each exactly as the schema "
            "spells it (\"CustomerId\"). Evaluation lists such names "
            "(MIXED_CASE_IDENTIFIERS)."
        ),
    )


def _star_labels(item: Optional[exp.Expression], scopes: "_Scopes") -> "list[exp.Identifier]":
    """The labels a ``*`` over FROM item ``item`` returns, when the statement defines it (a
    derived table or a CTE); a real table's columns are its schema's, already quoted."""
    if isinstance(item, exp.Lateral) and isinstance(item.this, exp.Subquery):
        return _output_identifiers(item.this.this, item.args.get("alias"))
    if isinstance(item, exp.Subquery) and not _is_join_container(item):
        return _output_identifiers(item.this, item.args.get("alias"))
    if isinstance(item, exp.Table) and not item.args.get("db") and item.name:
        cte = scopes.visible_ctes(item).get(_key(item.args.get("this")))
        if cte is not None:
            return _output_identifiers(cte.this, cte.args.get("alias"))
    return []


def _mixed_case_label_warning(
    tree: exp.Expression, flagged: "Sequence[str]" = (), scopes: "Optional[_Scopes]" = None
) -> "Optional[QueryWarning]":
    """MANUAL when a result column's label is written unquoted with upper-case ASCII
    letters: the query runs, but Aurora DSQL folds the label (id AS UserId comes back as
    userid where MySQL returned UserId), so a client reading columns by name misses it.
    Only the statement's own result (a derived table's alias never reaches the client),
    and not a name already ``flagged`` as a schema name: quoting that keeps its label."""
    query = tree
    while isinstance(query, (exp.SetOperation, exp.Subquery)):
        query = query.this  # a UNION's labels are its first branch's
    if not isinstance(query, exp.Select):
        return None
    scopes = scopes or _Scopes(tree)
    labels = []
    candidates: list = []
    for projection in query.expressions:
        if isinstance(projection, exp.Alias):
            candidates.append(projection.args.get("alias"))
        elif isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
            # t.*: the labels of the derived table or CTE ``t`` names
            qualifier = _key(projection.args.get("table"))
            for item in _from_items(query):
                if _item_key(item) == qualifier:
                    candidates.extend(_star_labels(item, scopes))
        elif isinstance(projection, exp.Column):
            candidates.append(projection.args.get("this"))  # the column's own name labels it
        elif isinstance(projection, exp.Star):
            for item in _from_items(query):
                candidates.extend(_star_labels(item, scopes))
    for label in candidates:
        if (
            isinstance(label, exp.Identifier)
            and not label.quoted
            and any("A" <= ch <= "Z" for ch in label.name)
            and label.name not in labels
            and label.name not in flagged
        ):
            labels.append(label.name)
    if not labels:
        return None
    shown = ", ".join(labels[:5]) + (" ..." if len(labels) > 5 else "")
    return QueryWarning(
        code=CODE_MIXED_CASE_LABEL,
        classification=Classification.MANUAL,
        message=(
            f"Result column label(s) {shown} come back from Aurora DSQL in lower case "
            f"({', '.join(name.translate(_ASCII_FOLD) for name in labels[:5])}): an "
            "unquoted label is folded, where MySQL kept its case. The query itself runs; a "
            "client that reads result columns by name (an ORM, dictionary rows) no longer "
            f"finds {labels[0]}. If yours does, label it quoted (AS \"{labels[0]}\") and "
            f"write every reference to that alias the same way (ORDER BY \"{labels[0]}\")."
        ),
    )


def _quote_mismatch_warning(names: "Sequence[str]") -> "Optional[QueryWarning]":
    """MANUAL when a name is quoted in one place and not in another (``names``, from
    :func:`_mixed_case_names`): MySQL matched them whatever the case, Aurora DSQL folds the
    unquoted spelling to lower case, so the two no longer name the same alias or column."""
    if not names:
        return None
    first = names[0]
    return QueryWarning(
        code=CODE_QUOTE_MISMATCH,
        classification=Classification.MANUAL,
        message=(
            f"{', '.join(names[:5])}{' ...' if len(names) > 5 else ''} "
            f"{'is' if len(names) == 1 else 'are'} quoted in one place and not in another: "
            "Aurora DSQL folds the unquoted spelling to lower case, so the two no longer name "
            f'the same alias or column ("{first}" is not {first.translate(_ASCII_FOLD)}) and '
            "the statement fails. Write each the same way everywhere."
        ),
    )


def _case_only_alias_warning(tree: exp.Expression, scopes: "_Scopes") -> "Optional[QueryWarning]":
    """MANUAL when table aliases that differ only by case (or quoting) meet where Aurora DSQL
    makes them one: two in one FROM ('table name specified more than once'), or a reference
    MySQL resolves to one and Aurora DSQL to the other -- (SELECT ... FROM orders O WHERE
    O.cid = o.cid) under FROM orders o: MySQL keeps table aliases apart by case, so o is
    the outer row; folded, the inner O captures it, and the correlated subquery compares a
    row with itself, returning other rows with no error. A quoted alias takes part as
    written: a backquoted `o` is what an unquoted O folds to. Aliases in scopes no
    reference crosses (a CTE body, an uncorrelated subquery) never meet."""

    def shown(item: exp.Expression) -> str:
        alias = item.args.get("alias")
        aliased = isinstance(alias, exp.TableAlias) and alias.name
        named = alias.this if aliased else item.args.get("this")
        name = _written_name(item)
        return f"`{name}`" if isinstance(named, exp.Identifier) and named.quoted else name

    clashes: list[tuple[str, str]] = []

    def clash(first: exp.Expression, second: exp.Expression) -> None:
        pair = (shown(first), shown(second))
        if pair not in clashes and pair[::-1] not in clashes:
            clashes.append(pair)

    for node in tree.walk(bfs=True):
        if not isinstance(node, _SCOPE_NODES):
            continue
        seen: dict[str, exp.Expression] = {}
        for item in scopes.items(node):
            if not _written_name(item):
                continue
            other = seen.setdefault(_item_key(item), item)
            if _written_name(other) != _written_name(item):
                clash(other, item)
    for column in tree.find_all(exp.Column):
        qualifier = column.args.get("table")
        scope = scopes.scope_of(column)
        if (
            not isinstance(qualifier, exp.Identifier)
            or not qualifier.name
            or column.args.get("db") is not None
            or scope is None
        ):
            continue
        # Each engine's own reading of the qualifier: MySQL by the name as written, Aurora
        # DSQL by its key (a quoted name as written, an unquoted one folded).
        mysql_item = scopes.bound(scope, qualifier.name, written=True)
        postgres_item = scopes.bound(scope, _key(qualifier))
        if mysql_item is not None and postgres_item is not None and mysql_item is not postgres_item:
            clash(mysql_item, postgres_item)
    if not clashes:
        return None
    first, second = clashes[0]
    return QueryWarning(
        code=CODE_CASE_ONLY_ALIASES,
        classification=Classification.MANUAL,
        message=(
            f"Aliases {first} and {second} differ only by case or quoting. MySQL keeps table "
            "aliases apart by case, quoted or not, but Aurora DSQL folds an unquoted alias to "
            "lower case, so a reference MySQL reads as one of them reaches the other: a "
            "correlated subquery then compares a row with itself (other rows come back, with "
            "no error), and a self-join fails ('table name specified more than once'). Rename "
            "one of them."
        ),
    )


# MySQL's DML modifiers, by the verb they follow (MySQL 8.4 takes DELETE's in any order).
# LOW_PRIORITY / HIGH_PRIORITY / QUICK only schedule MySQL's table-level locks (MyISAM,
# MEMORY) or tune its index upkeep, and MySQL 8 ignores DELAYED: Aurora DSQL has none of
# that, so the conversion leaves them out and the statement does what it did. IGNORE is
# no such modifier: see _ignore_warning.
_DML_MODIFIERS: "dict[str, frozenset[str]]" = {
    "INSERT": frozenset({"LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"}),
    "UPDATE": frozenset({"LOW_PRIORITY", "IGNORE"}),
    "DELETE": frozenset({"LOW_PRIORITY", "QUICK", "IGNORE"}),
}
_VERB_TOKENS = (TokenType.SELECT, TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE)
# A statement with none of these words has no modifier to remove, and is not tokenized twice.
_MODIFIER_WORD = re.compile(
    r"\b(?:LOW_PRIORITY|HIGH_PRIORITY|DELAYED|QUICK|IGNORE)\b", re.IGNORECASE
)


def _verb_index(tokens: "Sequence") -> "Optional[int]":
    """Where the statement's verb is among its tokens: the first, or after a leading WITH
    the first outside its CTEs' parentheses (a CTE's name is never a verb, all reserved)."""
    if not tokens:
        return None
    if tokens[0].token_type is not TokenType.WITH:
        return 0
    depth = 0
    for index, token in enumerate(tokens):
        if token.token_type is TokenType.L_PAREN:
            depth += 1
        elif token.token_type is TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type in _VERB_TOKENS:
            return index
    return None


def _without_dml_modifiers(sql: str) -> "tuple[str, str, list[str]]":
    """``sql`` without the MySQL modifiers after its INSERT / UPDATE / DELETE, its verb, and
    the modifiers removed (upper-case). sqlglot refuses DELETE / UPDATE IGNORE and INSERT
    LOW_PRIORITY, and reads UPDATE LOW_PRIORITY t as a table aliased t, so they go before
    parsing. Unquoted there, each is always a modifier: MySQL reserves all but QUICK, and
    reads QUICK after DELETE as one even before a table's name. INSERT IGNORE stays: sqlglot
    reads it."""
    if not _MODIFIER_WORD.search(sql):
        return sql, "", []
    try:
        tokens = MYSQL_EXPRESSION_DIALECT().tokenize(sql)
    except sqlglot.errors.TokenError:
        return sql, "", []  # parsing reports it
    index = _verb_index(tokens)
    if index is None:
        return sql, "", []
    verb = tokens[index].text.upper()
    allowed = _DML_MODIFIERS.get(verb, frozenset())
    removed: list = []
    for token in tokens[index + 1 :]:
        word = token.token_type in (TokenType.VAR, TokenType.IGNORE)  # unquoted
        if not word or token.text.upper() not in allowed:
            break
        removed.append(token)
    for token in reversed(removed):
        sql = sql[: token.start] + sql[token.end + 1 :]
    return sql, verb, [token.text.upper() for token in removed]


def _with_ignore(converted: str) -> str:
    """``converted`` with IGNORE back after its verb: an UPDATE / DELETE IGNORE is left as
    written, so the statement fails on Aurora DSQL until it is rewritten, instead of
    running with its errors no longer ignored."""
    tokens = sqlglot.Dialect.get_or_raise(_POSTGRES).tokenize(converted)
    index = _verb_index(tokens)
    if index is None:
        return converted
    end = tokens[index].end + 1
    return f"{converted[:end]} IGNORE{converted[end:]}"


def _ignore_warning(verb: str) -> QueryWarning:
    """MANUAL for MySQL's ``INSERT / UPDATE / DELETE IGNORE``, left as written: it turns the
    statement's errors into warnings and goes on, which Aurora DSQL has no form for -- and
    ON CONFLICT DO NOTHING covers only the duplicate keys, so it is no rewrite of it."""
    instead = {
        "INSERT": "For the duplicate rows, write INSERT ... ON CONFLICT DO NOTHING",
        "UPDATE": "Leave out the rows that would fail instead (a NOT EXISTS predicate for "
        "the duplicates)",
        "DELETE": "Leave out the rows that would fail instead (a NOT EXISTS predicate for the "
        "rows still referenced)",
    }[verb]
    return QueryWarning(
        code=CODE_MYSQL_ONLY_DML,
        classification=Classification.MANUAL,
        message=(
            f"{verb} IGNORE has no Aurora DSQL form and is left as written. MySQL's IGNORE "
            "turns the statement's errors into warnings and goes on: it skips a row that "
            "would duplicate a unique key or break a foreign key, cuts a value too long for "
            "its column and stores the type's default for a NULL in a NOT NULL column. Aurora "
            f"DSQL fails the whole statement on each. {instead}, and make sure the data hits "
            "none of the others, before running it."
        ),
    )


def _mysql_only_clause_warnings(tree: exp.Expression) -> "list[QueryWarning]":
    """MANUAL for the MySQL-only clauses of a DML statement Aurora DSQL's grammar lacks, left
    as written (a DML statement is never test-run, so nothing else would tell): REPLACE,
    INSERT IGNORE, and DELETE / UPDATE ... ORDER BY / LIMIT -- the chunked delete a MySQL
    application runs."""
    warnings: list[QueryWarning] = []
    if isinstance(tree, exp.Command) and (tree.name or "").strip().upper() == "REPLACE":
        warnings.append(QueryWarning(
            code=CODE_MYSQL_ONLY_DML,
            classification=Classification.MANUAL,
            message=(
                "REPLACE has no Aurora DSQL form and is left as written: MySQL deletes every "
                "row the new row's primary or unique key matches, then inserts it. The nearest "
                "is INSERT ... ON CONFLICT (<primary key>) DO UPDATE SET col = EXCLUDED.col for "
                "each column; it updates the row in place, so a column the statement leaves out "
                "keeps its value instead of taking its default, and a clash on another unique "
                "key still fails. Rewrite it before running it."
            ),
        ))
    if isinstance(tree, exp.Insert) and tree.args.get("ignore"):
        warnings.append(_ignore_warning("INSERT"))
    for node in tree.find_all(exp.Delete, exp.Update):
        if not (node.args.get("order") or node.args.get("limit")):
            continue
        verb = "DELETE" if isinstance(node, exp.Delete) else "UPDATE"
        written = " ... ".join(
            clause
            for clause, part in (("ORDER BY", "order"), ("LIMIT", "limit"))
            if node.args.get(part)
        )
        if node.args.get("limit"):
            remedy = (
                "Pick the rows by key in a subquery instead -- "
                f"{'DELETE FROM t' if verb == 'DELETE' else 'UPDATE t SET ...'} WHERE <primary "
                "key> IN (SELECT <primary key> FROM t WHERE ... ORDER BY ... LIMIT n) -- keeping n "
                "within Aurora DSQL's 3,000 rows per transaction."
            )
        else:
            remedy = "Without a LIMIT, the order does not change which rows are touched: remove it."
        warnings.append(QueryWarning(
            code=CODE_MYSQL_ONLY_DML,
            classification=Classification.MANUAL,
            message=(
                f"{verb} ... {written} is MySQL-only: Aurora DSQL's {verb} takes no ORDER BY or "
                f"LIMIT, so it is left as written. {remedy}"
            ),
        ))
    return warnings


def _mysql_only_dml_warning(tree: exp.Expression) -> "Optional[QueryWarning]":
    """MANUAL for MySQL's multi-table UPDATE / DELETE (``UPDATE a JOIN b ... SET a.x = ...``,
    ``UPDATE a, b ...``, ``DELETE a FROM a JOIN b ...``, ``DELETE FROM a USING a JOIN b``):
    PostgreSQL has none of these shapes, and a qualified SET target is refused. A
    single-table UPDATE's ``SET t.col`` naming its own target is written unqualified
    instead (rewritten in place)."""
    shapes: list[str] = []
    for update in tree.find_all(exp.Update):
        target = update.this
        if isinstance(target, exp.Table) and target.args.get("joins"):
            if "UPDATE ... JOIN" not in shapes:
                shapes.append("UPDATE ... JOIN")
            continue
        own = {_item_key(target)} if isinstance(target, exp.Table) else set()
        for assignment in update.expressions:
            column = assignment.this if isinstance(assignment, exp.EQ) else None
            if isinstance(column, exp.Column) and column.table and _key(column.args.get("table")) in own:
                # SET o.status = ... / SET db.orders.status = ...: the target's own column.
                for part in ("table", "db", "catalog"):
                    column.set(part, None)
    for delete in tree.find_all(exp.Delete):
        using = delete.args.get("using")
        target = delete.this
        if not isinstance(target, exp.Expression):
            # DELETE t WHERE ... (no FROM): neither engine's syntax.
            return QueryWarning(
                code=CODE_MULTI_TABLE_DML,
                classification=Classification.MANUAL,
                message=(
                    "DELETE without FROM is not MySQL's or PostgreSQL's syntax: write DELETE "
                    "FROM t WHERE ... before running the statement."
                ),
            )
        # DELETE t FROM ... lists its targets (MySQL's modifiers never get here: see
        # _without_dml_modifiers), as does DELETE FROM t USING ... t.
        if delete.args.get("tables") or (
            isinstance(using, list)
            and isinstance(target, exp.Table)
            and any(_item_key(i) == _item_key(target) for u in using for i in _source_items(u))
        ):
            if "DELETE t FROM ... JOIN" not in shapes:
                shapes.append("DELETE t FROM ... JOIN")
    if not shapes:
        return None
    return QueryWarning(
        code=CODE_MULTI_TABLE_DML,
        classification=Classification.MANUAL,
        message=(
            f"MySQL's multi-table {' / '.join(shapes)} has no PostgreSQL form as written: "
            "Aurora DSQL needs UPDATE t SET col = ... FROM other WHERE <join condition> "
            "(DELETE FROM t USING other WHERE <join condition>), with each SET target "
            "unqualified. Rewrite it before running it -- a DML statement is not test-run "
            "on the target."
        ),
    )


class QueryConverter:
    """Converts source DML/SELECT to DSQL PostgreSQL and flags lock anti-patterns.

    See the module docstring for the full contract (Requirements 4.1-4.4,
    Property 6). The converter is stateless and has no schema/primary-key
    knowledge; lock detection is therefore conservative and never asserts a
    primary-key violation it cannot prove.

    ``source_type`` selects the READ dialect, defaulting to MySQL (the original engine) --
    the same constructor shape as ``converter.SchemaConverter``. It is not cosmetic: read
    as MySQL, a PostgreSQL statement using ``||`` or a double-quoted identifier either
    fails to parse (so the screen refuses to convert or test it, naming the wrong engine)
    or -- worse -- parses to something ELSE, because MySQL reads ``||`` as OR and
    ``"x"`` as a string literal. That produced a silently DIFFERENT query labelled AUTO
    ("fully automatic, no review needed") which the Test button would then run against the
    real target.
    """

    def __init__(
        self,
        *,
        source_type: SourceType = SourceType.MYSQL,
        column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]" = (),
    ) -> None:
        self._source_type = source_type
        # The Evaluation inventory's column types (table_type_index entries), so a MySQL
        # expression is written by what its columns hold -- a BLOB's LENGTH its own bytes,
        # CONVERT(blob USING ...) its decoded text -- as a view's is.
        self._column_types = tuple(column_types)

    @property
    def _is_postgres(self) -> bool:
        return self._source_type is SourceType.POSTGRES

    def convert(self, sql: str, *, pretty: bool = False) -> QueryConversionResult:
        """:meth:`_convert`, which never raises: a statement nested deeper than the
        conversion walks (a generated expression of thousands of operators or parentheses)
        is flagged for manual review, as a view is."""
        try:
            return self._convert(sql, pretty=pretty)
        except RecursionError:
            return self._manual_review(
                sql,
                CODE_RENDER_ERROR,
                "The statement is nested too deeply to convert (an expression of thousands of "
                "operators or parentheses); flag for manual review, or split the expression.",
            )

    def _convert(self, sql: str, *, pretty: bool = False) -> QueryConversionResult:
        """Convert one source statement and return the original/converted pair.

        Parses ``sql`` in the source dialect, rewrites ``ON DUPLICATE KEY UPDATE``,
        inspects the AST for ``FOR UPDATE`` anti-patterns, and renders the result in the
        ``postgres`` dialect. ``pretty`` renders the converted SQL multi-line and
        indented (sqlglot pretty-print) so a long statement is readable in the UI;
        it changes only the formatting, never the semantics. Unparseable or
        unrenderable input is flagged for manual review with ``converted_sql=None``
        rather than being dropped silently (Property 6). The input is only
        parsed/transpiled, never executed (Requirement 9.4).

        The MySQL-only rewrites (``ON DUPLICATE KEY UPDATE`` -> ``ON CONFLICT``,
        ``JSON_UNQUOTE``) are skipped for a PostgreSQL source: neither construct exists
        there, so running them could only misfire.
        """
        engine = "PostgreSQL" if self._is_postgres else "MySQL"
        # MySQL's LOW_PRIORITY / QUICK / IGNORE ... after the verb, which sqlglot refuses
        # or reads as a table: the parse is of the statement without them.
        parsed, verb, modifiers = (
            (sql, "", []) if self._is_postgres else _without_dml_modifiers(sql)
        )
        try:
            tree = sqlglot.parse_one(
                parsed, read=_POSTGRES if self._is_postgres else MYSQL_EXPRESSION_DIALECT
            )
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as exc:
            # A TokenError (an unterminated string or quote) used to escape to the caller.
            return self._manual_review(
                sql,
                CODE_PARSE_ERROR,
                f"Unable to parse the SQL as {engine}; flag for manual review: {exc}",
            )

        if tree is None:
            return self._manual_review(
                sql,
                CODE_PARSE_ERROR,
                "The SQL parsed to an empty statement; flag for manual review.",
            )

        # Classify BEFORE the in-place ON CONFLICT rewrite so the kind reflects
        # the source statement (the rewrite does not change the verb anyway).
        statement_kind = classify_statement(tree)
        warnings: list[QueryWarning] = []

        if not self._is_postgres:
            # On the SOURCE's names, before a rewrite adds its own (EXCLUDED.col).
            scopes = _Scopes(tree)
            mismatched: list[str] = []
            mixed_names = _mixed_case_names(tree, scopes, mismatched)
            mixed_case = _mixed_case_name_warning(mixed_names)
            mixed_label = _mixed_case_label_warning(tree, mixed_names, scopes)
            for found in (
                _quote_mismatch_warning(mismatched),
                _case_only_alias_warning(tree, scopes),
                _mysql_only_dml_warning(tree),
            ):
                if found is not None:
                    warnings.append(found)
            if "IGNORE" in modifiers:
                warnings.append(_ignore_warning(verb))
            warnings.extend(_mysql_only_clause_warnings(tree))
            on_duplicate_warning = _rewrite_on_duplicate_key_update(tree)
            if on_duplicate_warning is not None:
                warnings.append(on_duplicate_warning)
            warnings.extend(_rewrite_json_unquote(tree))
            # The MySQL expression semantics a view gets (Schema Conversion), so the two
            # steps never disagree on one: VARIANCE is the population one, CAST AS CHAR is
            # text, GREATEST is NULL for a NULL argument, NOW() whole seconds, ...
            untranslated, misformatted, issues = rewrite_mysql_expressions(tree, self._column_types)
            # JSON_UNQUOTE that is not over JSON_EXTRACT is reported by its own code above.
            untranslated = [name for name in untranslated if name.upper() != "JSON_UNQUOTE"]
            warnings.extend(_mysql_expression_warnings(untranslated, misformatted, issues))
            _drop_from_dual(tree)
            if mixed_case is not None:
                warnings.append(mixed_case)
            if mixed_label is not None:
                warnings.append(mixed_label)
        if not self._is_postgres:
            # MySQL-only: PostgreSQL's HAVING never names an output alias, so on a
            # PostgreSQL source a HAVING name matching one is a real column.
            _inline_having_aliases(tree)
        warnings.extend(_for_update_warnings(tree))

        try:
            converted_sql = tree.sql(dialect=_POSTGRES, pretty=pretty)
            if "IGNORE" in modifiers:
                converted_sql = _with_ignore(converted_sql)
        except sqlglot.errors.SqlglotError as exc:
            return self._manual_review(
                sql,
                CODE_RENDER_ERROR,
                "Parsed but could not be rendered for PostgreSQL/DSQL; flag for "
                f"manual review: {exc}",
                statement_kind=statement_kind,
            )

        return QueryConversionResult(
            original_sql=sql,
            converted_sql=converted_sql,
            classification=_overall_classification(warnings),
            statement_kind=statement_kind,
            warnings=warnings,
        )

    @staticmethod
    def _manual_review(
        sql: str,
        code: str,
        message: str,
        statement_kind: StatementKind = StatementKind.OTHER,
    ) -> QueryConversionResult:
        """Build a ``MANUAL`` result with no converted SQL but the original kept."""
        return QueryConversionResult(
            original_sql=sql,
            converted_sql=None,
            classification=Classification.MANUAL,
            statement_kind=statement_kind,
            warnings=[
                QueryWarning(
                    code=code,
                    classification=Classification.MANUAL,
                    message=message,
                )
            ],
        )


__all__ = [
    "QueryWarning",
    "QueryConversionResult",
    "QueryConverter",
    "StatementKind",
    "classify_statement",
    "classify_sql",
    "CODE_PARSE_ERROR",
    "CODE_RENDER_ERROR",
    "CODE_ON_DUPLICATE_KEY_UPDATE",
    "CODE_FOR_UPDATE_MULTI_TABLE",
    "CODE_FOR_UPDATE_NO_EQUALITY",
    "CODE_FOR_UPDATE_VERIFY_PK",
    "CODE_JSON_UNQUOTE_UNSUPPORTED",
    "CODE_MIXED_CASE_NAME",
    "CODE_MIXED_CASE_LABEL",
    "CODE_QUOTE_MISMATCH",
    "CODE_CASE_ONLY_ALIASES",
    "CODE_MULTI_TABLE_DML",
    "CODE_MYSQL_ONLY_DML",
    "CODE_UNTRANSLATED_CALL",
    "CODE_UNMAPPED_DATE_FORMAT",
    "CODE_MYSQL_SEMANTICS",
]
