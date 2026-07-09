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

from enum import Enum
from typing import Optional

import sqlglot
from sqlglot import exp
from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import Classification

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


def classify_sql(sql: str) -> StatementKind:
    """Parse ``sql`` as MySQL and classify it; ``OTHER`` when it cannot be parsed.

    A convenience wrapper over :func:`classify_statement` for callers that have a
    raw string rather than a parsed tree. Parsing only -- never executed.
    """
    try:
        tree = sqlglot.parse_one(sql, read=_MYSQL)
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
    :class:`sqlglot.exp.JSONExtractScalar`, so it renders as
    ``JSON_EXTRACT_PATH_TEXT(col, 'k', ...)`` and runs on DSQL.

    A ``JSON_UNQUOTE`` wrapping anything else has no automatic equivalent, so it is
    left unchanged and flagged ``MANUAL`` rather than silently emitting SQL the
    target will reject (Property 6). Returns the warnings produced (empty when
    there was no ``JSON_UNQUOTE`` or every occurrence was rewritten cleanly).
    """
    warnings: list[QueryWarning] = []
    for node in list(tree.find_all(exp.Anonymous)):
        if not _is_json_unquote(node):
            continue
        inner = node.expressions[0]
        if isinstance(inner, exp.JSONExtract):
            # JSON_UNQUOTE(JSON_EXTRACT(col, path)) -> scalar (text) extraction.
            node.replace(
                exp.JSONExtractScalar(this=inner.this, expression=inner.expression)
            )
        else:
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


def _inline_having_aliases(tree: exp.Expression) -> None:
    """Inline SELECT-list aliases referenced in ``HAVING`` (MySQL -> PostgreSQL).

    MySQL lets a ``HAVING`` clause reference a column ALIAS defined in the SELECT
    list (e.g. ``SELECT SUM(x) AS total ... HAVING total > 10``). PostgreSQL /
    Aurora DSQL do NOT: ``HAVING`` may only reference grouped columns or aggregate
    expressions, so the alias resolves to nothing and the target rejects the
    statement with ``column "total" does not exist``. (``ORDER BY`` may use output
    aliases on both engines, so it is intentionally left untouched.)

    For each ``SELECT`` that has a ``HAVING``, this replaces any unqualified column
    in ``HAVING`` whose name matches a SELECT-list alias with a copy of that
    alias's underlying expression, so the predicate runs on DSQL. A qualified
    reference (``t.col``) is never treated as an output alias. Done in place on the
    parsed tree before rendering; formatting/semantics of the predicate are
    preserved (the aggregate expression is simply written out in full).
    """
    for select in tree.find_all(exp.Select):
        having = select.args.get("having")
        if having is None:
            continue
        aliases = {
            projection.alias: projection.this
            for projection in select.expressions
            if isinstance(projection, exp.Alias)
        }
        if not aliases:
            continue
        for column in list(having.find_all(exp.Column)):
            # A qualified column (table.col) is a real column reference, never an
            # output alias; only bare names can shadow a SELECT-list alias.
            if column.table:
                continue
            replacement = aliases.get(column.name)
            if replacement is not None:
                column.replace(replacement.copy())


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


class QueryConverter:
    """Converts MySQL DML/SELECT to DSQL PostgreSQL and flags lock anti-patterns.

    See the module docstring for the full contract (Requirements 4.1-4.4,
    Property 6). The converter is stateless and has no schema/primary-key
    knowledge; lock detection is therefore conservative and never asserts a
    primary-key violation it cannot prove.
    """

    def convert(self, sql: str, *, pretty: bool = False) -> QueryConversionResult:
        """Convert one MySQL statement and return the original/converted pair.

        Parses ``sql`` as MySQL, rewrites ``ON DUPLICATE KEY UPDATE``, inspects
        the AST for ``FOR UPDATE`` anti-patterns, and renders the result in the
        ``postgres`` dialect. ``pretty`` renders the converted SQL multi-line and
        indented (sqlglot pretty-print) so a long statement is readable in the UI;
        it changes only the formatting, never the semantics. Unparseable or
        unrenderable input is flagged for manual review with ``converted_sql=None``
        rather than being dropped silently (Property 6). The input is only
        parsed/transpiled, never executed (Requirement 9.4).
        """
        try:
            tree = sqlglot.parse_one(sql, read=_MYSQL)
        except sqlglot.errors.ParseError as exc:
            return self._manual_review(
                sql,
                CODE_PARSE_ERROR,
                f"Unable to parse the SQL as MySQL; flag for manual review: {exc}",
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

        on_duplicate_warning = _rewrite_on_duplicate_key_update(tree)
        if on_duplicate_warning is not None:
            warnings.append(on_duplicate_warning)

        warnings.extend(_rewrite_json_unquote(tree))
        _inline_having_aliases(tree)
        warnings.extend(_for_update_warnings(tree))

        try:
            converted_sql = tree.sql(dialect=_POSTGRES, pretty=pretty)
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
]
