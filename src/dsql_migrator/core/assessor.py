# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility assessment rule engine for MySQL/PostgreSQL -> Aurora DSQL migration.

The :class:`CompatibilityAssessor` evaluates a source inventory against a
declarative, extensible list of :class:`Rule` objects and produces an
:class:`~dsql_migrator.core.models.AssessmentReport`. Each inventory object
(table, view, trigger, routine) is classified as ``AUTO``, ``MANUAL``, or
``UNSUPPORTED`` together with a risk description and a recommended action, and
the report carries a difficulty summary (object counts per classification).

Design guarantees:

- Assessment completeness (Property 8 / Requirement 2.1): every object in the
  inventory ends up with exactly one classification. The engine emits exactly
  one :class:`AssessmentItem` per object; objects matched by zero rules receive
  a default ``AUTO`` classification, so nothing is left unclassified.
- No silent data loss (Property 6 / Requirement 2.2, 2.3): objects that hit a
  DSQL constraint are surfaced as ``MANUAL`` or ``UNSUPPORTED`` with the reason
  and a recommended alternative; they are never silently treated as compatible.

Aggregation strategy when several rules match the same object: the most severe
classification wins (``UNSUPPORTED`` > ``MANUAL`` > ``AUTO``). The governing
rule determines ``rule_id``/``classification`` while the risks and
recommendations of all matched rules are combined so no finding is lost. Rules
are evaluated in declaration order, which breaks ties deterministically.
"""

from __future__ import annotations

import html
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, ClassVar, Optional, Sequence

from dsql_migrator.core.models import (
    AiAssessmentReport,
    AssessmentConcern,
    AssessmentItem,
    AssessmentReport,
    Classification,
    ConversionNoteKind,
    EffortLevel,
    ForeignKeyDef,
    ObjectType,
    SourceInventory,
    SourceType,
    TableDef,
    TargetInventory,
)

if TYPE_CHECKING:
    from dsql_migrator.core.source_time_zone import SourceZone

# Ordering used to pick the governing classification when several rules match
# the same object. Higher value means more severe.
_SEVERITY: dict[Classification, int] = {
    Classification.AUTO: 0,
    Classification.MANUAL: 1,
    Classification.UNSUPPORTED: 2,
}

# Ordering used to pick the most demanding effort when several rules match the
# same object. Higher value means more effort.
_EFFORT_ORDER: dict[EffortLevel, int] = {
    EffortLevel.SIMPLE: 0,
    EffortLevel.MEDIUM: 1,
    EffortLevel.SIGNIFICANT: 2,
}

# Object kinds used as the canonical unit of classification (Property 8).
KIND_TABLE = "table"
KIND_VIEW = "view"
KIND_TRIGGER = "trigger"
KIND_ROUTINE = "routine"
KIND_PROCEDURE = "procedure"
KIND_FUNCTION = "function"
KIND_EVENT = "event"
# A PostgreSQL sequence no column owns. Not an inventory object: it is read by the
# Evaluation probe (see core.catalog_postgres) and reported on its own.
KIND_SEQUENCE = "sequence"

# User-facing label per object kind. Lives here, beside the KIND_* constants, because all
# three surfaces need it: the Evaluation list headings, the UI chart axis and the HTML
# export's chart. It was UI-only, so the chart axes showed the raw enum ("PROCEDURE") while
# the list beside them said "Stored procedures" -- the same object named two ways on one
# screen.
KIND_LABELS: dict[str, str] = {
    "TABLE": "Tables",
    "VIEW": "Views",
    "TRIGGER": "Triggers",
    "ROUTINE": "Routines",
    "PROCEDURE": "Stored procedures",
    "FUNCTION": "Functions",
    "EVENT": "Events",
    "SEQUENCE": "Sequences",
    "DATABASE": "Database / cluster-level",
}


def kind_label(kind: str) -> str:
    """Return the friendly label for an object ``kind`` (unknown kinds title-case)."""
    return KIND_LABELS.get(kind, kind.title())


# Map a collected routine's subtype to the assessment kind so stored procedures
# and functions are categorized separately (a generic routine falls back).
_ROUTINE_KIND_BY_TYPE: dict[ObjectType, str] = {
    ObjectType.PROCEDURE: KIND_PROCEDURE,
    ObjectType.FUNCTION: KIND_FUNCTION,
}


def _routine_kind(routine: object) -> str:
    """Return the assessment kind for a collected routine (procedure/function)."""
    return _ROUTINE_KIND_BY_TYPE.get(
        getattr(routine, "object_type", None), KIND_ROUTINE
    )
# Synthetic kind for cluster/inventory-level findings (not a single object).
KIND_DATABASE = "database"

# Aurora DSQL database limits ("Cluster quotas and database limits", CHAP_quotas): none is
# configurable, and each is enforced at CREATE time with SQLSTATE 54000 -- so a migration
# that needs more is a planning-time blocker, not something an apply can work around.
_MAX_TABLES_PER_DATABASE = 1000
_MAX_SCHEMAS_PER_DATABASE = 10
_MAX_VIEWS_PER_DATABASE = 5000

# Rule id -> (plural noun, limit) of each database-level limit check. The Evaluation score
# names the limit from this when an item blocks the migration, so the wording and the
# number live in one place.
DSQL_DATABASE_LIMITS: dict[str, tuple[str, int]] = {
    "TABLE_COUNT_LIMIT": ("tables", _MAX_TABLES_PER_DATABASE),
    "SCHEMA_COUNT_LIMIT": ("schemas", _MAX_SCHEMAS_PER_DATABASE),
    "VIEW_COUNT_LIMIT": ("views", _MAX_VIEWS_PER_DATABASE),
}

# Aurora DSQL creates ``public`` in every database, so the migration never adds it. Whether
# it counts toward the 10-schema limit is not documented; it is left out of the count so a
# source that needs exactly 10 schemas of its own is not reported as blocked on a guess.
_BUILTIN_TARGET_SCHEMA = "public"

# MySQL spatial base types with no native DSQL (PostgreSQL 16) equivalent. The
# converter does NOT block these: it auto-substitutes each column to ``bytea``
# and preserves the raw WKB bytes end-to-end, so they are flagged MANUAL (review
# whether raw bytea suffices) rather than UNSUPPORTED (redesign required).
_SPATIAL_TYPE_BASES = frozenset(
    {
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
    }
)

# DSQL hard limit: at most 255 columns per table (cluster quotas/database limits).
_MAX_COLUMNS_PER_TABLE = 255

# DSQL hard limit: at most 24 indexes per table -- error 54000 "more than 24 indexes
# per table are not allowed" (MySQL allows 64). The PRIMARY KEY index COUNTS toward
# this budget: verified against a live cluster, where the 24th CREATE INDEX on a table
# that already had a PK failed and pg_indexes then showed 24 rows including the PK. So
# a migrated table (DSQL always requires a PK) can carry at most 23 SECONDARY indexes,
# which is what the source's reflected index list is compared against.
_MAX_INDEXES_PER_TABLE = 24
_MAX_SECONDARY_INDEXES_PER_TABLE = _MAX_INDEXES_PER_TABLE - 1

# DSQL hard limit: a primary key or secondary index may span at most 8 columns --
# error 54011 "more than 8 column keys are not allowed". MySQL allows up to 16
# columns per index, so a 9..16-column source key is a real schema the assessment
# has to flag: the PK failure lands when the table DDL is applied, and a secondary
# index failure lands only AFTER Full Load (indexes are built by post-load
# CREATE INDEX ASYNC) -- the same late-failure shape as _MAX_INDEXES_PER_TABLE.
_MAX_KEY_COLUMNS = 8

# DSQL's documented maximum numeric precision. Was 38 -- the limit the service enforced
# when this rule was written -- and the service has since raised it to 1000 ("Supported
# data types": maximum precision 1000, scale -1000..1000, default numeric(18,6)),
# re-confirmed live: numeric(1000,1000) is accepted, numeric(1001,0) is rejected with
# "NUMERIC precision 1001 must be between 1 and 1000", and a 500+500-digit value
# round-trips EXACT. While it said 38 this rule graded a perfectly migratable
# numeric(40,10) UNSUPPORTED ("a redesign is required") and the converter narrowed it,
# losing digits DSQL would have stored. MySQL caps DECIMAL at 65, so in practice only a
# PostgreSQL source can reach this at all.
_MAX_NUMERIC_PRECISION = 1000

# The MySQL base types whose values can exceed Aurora DSQL's 1 MiB per-value limit, so a
# large enough value cannot be stored and must be reviewed: the MEDIUM*/LONG* TEXT and BLOB
# families (16 MiB / 4 GiB), and ``json``. A MySQL JSON document is bounded only by
# max_allowed_packet (at most 1 GiB) and converts to DSQL ``json``, whose 1 MiB limit
# applies to the COMPRESSED size -- that moves the cap rather than removing it (see
# ``_PG_OVERSIZED_LOB_BASES`` below: a compression measurement does not overrule a
# documented quota). Leaving ``json`` out kept Evaluation, the Schema Conversion note, the
# exclusion offer and the size check silent, so an oversized document surfaced only as a
# quarantined or dead-lettered row. Plain TEXT/BLOB (64 KiB) stay out. Like the PostgreSQL
# set, this is a ceiling, not a prediction.
_OVERSIZED_LOB_BASES = frozenset(
    {"mediumtext", "longtext", "mediumblob", "longblob", "json"}
)

# The PostgreSQL counterpart. Lives here, beside the MySQL set, because three layers need
# the SAME answer -- Evaluation (PgOversizedLobRule), Schema Conversion
# (_pg_oversized_lob_warning) and the UI's exclusion offer
# (ui/data_migration/_models.lob_exclusion_candidates) -- and it was previously defined in
# the UI layer only, which is why Evaluation and Schema Conversion stayed silent.
#
# ``json``/``jsonb`` ARE included. An earlier comment excluded them as "stored differently
# and not hit by the text 1 MiB cap the same way"; that is wrong, and the mistake it repeats
# is the one v0.1.468 spent a release reverting -- a compression measurement does not
# overrule a documented quota. Per the Aurora DSQL "Supported data types" page: the 1 MiB
# per-value limit applies to bytea, text, json AND jsonb; DSQL auto-compresses
# text/varchar/bpchar AND json/jsonb, so compression cannot discriminate between them (if it
# justified dropping json it would equally justify dropping text, leaving the set empty);
# and for json/jsonb the limit applies to the COMPRESSED size, which relocates the cap
# rather than removing it. This set answers "can a value here exceed 1 MiB", i.e. it is a
# ceiling, not a prediction -- which is all the exclusion offer and the MANUAL flag claim.
# https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-unsupported-features.html
_PG_OVERSIZED_LOB_BASES = frozenset({"text", "bytea", "json", "jsonb"})


def _is_unbounded_pg_character(mysql_type: str) -> bool:
    """True for a PostgreSQL ``character varying`` / ``character`` with NO length limit.

    An unbounded ``varchar`` is semantically identical to ``text`` and can likewise exceed
    1 MiB, but it cannot be expressed through :func:`_base_type` alone: ``format_type``
    renders it as ``character varying`` and ``_base_type`` collapses BOTH that and
    ``character varying(50)`` to ``character``, so matching on the base type would wrongly
    flag every bounded varchar. The length modifier is therefore read off the FULL type
    string: no ``(`` means no limit.
    """
    normalized = " ".join((mysql_type or "").strip().lower().split())
    if "(" in normalized:
        return False
    # ``character``/``char`` are deliberately absent: PostgreSQL renders a modifier-less
    # char as ``character(1)`` (format_type always emits the length), so a bare spelling
    # cannot arrive from a PG source -- and if it somehow did, treating a 1-character column
    # as unbounded would be a false alarm. ``bpchar`` is the one bare unbounded spelling.
    return normalized in ("character varying", "varchar", "bpchar")


# The PostgreSQL oversized-LOB types split by what the type DECLARES, which is what decides
# how severely to grade the finding:
#
# * ``bytea``/``json``/``jsonb`` exist to hold a document or a blob -- choosing one IS the
#   intent to store something large, so "no length limit" is the risk itself. Graded a LOSS,
#   the same as Evaluation grades MySQL's mediumblob/longblob/mediumtext/longtext/json, every
#   member of which is likewise a deliberately-large type. This keeps the two engines in
#   step: a ``bytea`` column holding 1,114,112 bytes must not read "Ready" on PostgreSQL
#   while the identical ``longblob`` reads "Moderate effort" on MySQL.
# * An unbounded ``text``/``varchar`` is PostgreSQL's IDIOMATIC way to spell an ordinary short
#   string (names, emails, SKUs). Grading those a LOSS put an 18-table schema of perfectly
#   ordinary columns into per-table manual work -- measured -- which is the noise the v0.1.495
#   downgrade was for. Graded a RECOMMENDATION: something to check, not work to budget.
#
# MySQL has no idiomatic-short member in its set (plain ``text`` is excluded as too small to
# exceed 1 MiB, and ``json`` is a document type like PostgreSQL json/jsonb), which is why it
# needs no such split -- in EVALUATION a MySQL ``json`` column grades exactly like a
# PostgreSQL one. Schema Conversion does not: its MySQL note stays a RECOMMENDATION (as it
# always has been for LONGTEXT/LONGBLOB), while the PostgreSQL note for bytea/json/jsonb
# is a LOSS (``converter._oversized_lob_warning`` vs ``_pg_oversized_lob_warning``).
_PG_LOB_DELIBERATELY_LARGE = frozenset({"bytea", "json", "jsonb"})


def pg_lob_is_deliberately_large(mysql_type: str) -> bool:
    """True when the PG type itself declares an intent to store something large."""
    return _base_type(mysql_type) in _PG_LOB_DELIBERATELY_LARGE


def pg_columns_bounded_by_check(table: "TableDef") -> "set[str]":
    """Column names a CHECK constraint restricts to a finite set of LITERALS.

    Such a column cannot hold an oversized value however unbounded its TYPE is: a
    ``status text`` limited to ``('active','inactive','out_of_stock')`` can only ever hold
    one of three short strings. Reporting it as a 1 MiB risk is a false statement, and it
    buries the genuine one -- a workshop Evaluation listed a real 1,114,112-byte ``bytea``
    at the same grade as two CHECK-limited enums.

    The constraint text is ALREADY captured and already displayed in the same report (the
    dropped-CHECK note quotes each name and expression), so this reads data the tool holds
    rather than adding a probe. Parsed with sqlglot, not matched with a regex, because
    PostgreSQL rewrites ``IN (...)`` to ``= ANY (ARRAY[...::text])`` -- the literal form is
    not what the user typed.

    Only the finite-literal-set form is recognised. A ``length(col) <= n`` CHECK also bounds
    a column, but deciding "small enough" needs a byte-vs-character judgement (a 1 MiB cap
    against a character count that can be 4 bytes each), so it is deliberately NOT claimed
    here -- such a column is still reported, which is the safe direction. A CHECK that merely
    mentions the column (``notes <> ''``) bounds nothing and is correctly not matched.
    """
    import sqlglot
    from sqlglot import exp

    bounded: set[str] = set()
    for check in getattr(table, "check_constraints", None) or []:
        expression = (getattr(check, "expression", "") or "").strip()
        if not expression:
            continue
        # A NOT VALID constraint was never checked against the rows ALREADY stored, so it
        # proves nothing about existing data: a multi-megabyte value written before the
        # constraint was added is still there, and Full Load reads exactly those rows.
        # Treating it as a bound hid precisely the value that would fail to load.
        if getattr(check, "not_valid", False):
            continue
        try:
            tree = sqlglot.parse_one(f"SELECT {expression}", read="postgres")
        except Exception:  # noqa: BLE001 - an unparsable CHECK simply bounds nothing
            continue

        def _conjuncts(node):
            """The top-level AND-conjuncts of the expression.

            Only a conjunct BOUNDS the column. Searching the whole tree for an EQ/In node
            instead made two shapes suppress a genuine risk, both verified live:
            ``status = ANY (ARRAY['a','b']) OR body IS NOT NULL`` (the OR means a row can
            satisfy the CHECK via the other branch, so ``status`` may hold anything), and
            ``NOT (other = ANY (ARRAY['x','y']))`` (the negation permits everything EXCEPT
            those two). Both excluded the column and hid the risk.
            """
            if isinstance(node, exp.Paren):
                return _conjuncts(node.this)
            if isinstance(node, exp.And):
                return _conjuncts(node.this) + _conjuncts(node.expression)
            return [node]

        def _all_literals(nodes) -> bool:
            # CAST('a' AS TEXT) is how PostgreSQL stores 'a'::text, so unwrap one level.
            for node in nodes:
                inner = node.this if isinstance(node, exp.Cast) else node
                if not isinstance(inner, exp.Literal):
                    return False
            return bool(nodes)

        root = tree.expressions[0] if isinstance(tree, exp.Select) else tree
        for conjunct in _conjuncts(root):
            if isinstance(conjunct, exp.Paren):
                conjunct = conjunct.this
            # `col = ANY (ARRAY[...])` -- what PostgreSQL stores an IN-list as.
            if isinstance(conjunct, exp.EQ):
                eq = conjunct
                column = eq.this
                right = eq.expression
                if not isinstance(column, exp.Column):
                    continue
                if not isinstance(right, exp.Any):
                    continue
                # ``Any.this`` is a Paren wrapping the Array for the PostgreSQL
                # ``= ANY (ARRAY[...])`` spelling, so unwrap before looking for the Array.
                array = right.this
                while isinstance(array, exp.Paren):
                    array = array.this
                if isinstance(array, exp.Array) and _all_literals(array.expressions):
                    bounded.add(column.name)
            # A literal `col IN (...)`, for a source that stores it that way.
            elif isinstance(conjunct, exp.In):
                column = conjunct.this
                if isinstance(column, exp.Column) and _all_literals(
                    conjunct.args.get("expressions") or []
                ):
                    bounded.add(column.name)
    return bounded


def pg_oversized_lob_column_names(table: "TableDef") -> "list[str]":
    """Columns of ``table`` that can genuinely hold a value over DSQL's 1 MiB limit.

    The TYPE predicate plus the CHECK exclusion above, in one place, so Evaluation, Schema
    Conversion and the UI's exclusion offer cannot disagree about which columns are at risk.
    """
    bounded = pg_columns_bounded_by_check(table)
    return [
        column.name
        for column in table.columns
        if is_pg_oversized_lob_type(column.mysql_type) and column.name not in bounded
    ]


def is_pg_oversized_lob_type(mysql_type: str) -> bool:
    """True when this PostgreSQL column type can hold a value over DSQL's 1 MiB limit."""
    return (
        _base_type(mysql_type) in _PG_OVERSIZED_LOB_BASES
        or _is_unbounded_pg_character(mysql_type)
    )

# MySQL base types with no native DSQL equivalent that map to text, losing their
# allowed-value/domain semantics (manual review).
_ENUM_SET_BASES = frozenset({"enum", "set"})

# MySQL fixed-point base types whose precision must fit the DSQL numeric maximum.
_DECIMAL_BASES = frozenset({"decimal", "numeric", "dec", "fixed"})

# MySQL index types with no Aurora DSQL equivalent.
_UNSUPPORTED_INDEX_TYPES = frozenset({"fulltext", "spatial"})

# Captures the precision (first parenthesized integer) of a DECIMAL declaration.
_DECIMAL_PRECISION_RE = re.compile(r"\(\s*(\d+)")

# TINYINT(1) is MySQL's boolean convention (also the BOOL/BOOLEAN alias, which
# information_schema reports as ``tinyint(1)``); a wider TINYINT(n) is a normal
# small integer. Matches an explicit ``(1)`` display width only.
_TINYINT_ONE_RE = re.compile(r"^\s*tinyint\s*\(\s*1\s*\)", re.IGNORECASE)

# A trailing UNSIGNED attribute (also implied by ZEROFILL). Used to keep the boolean
# convention for SIGNED tinyint(1) only -- tinyint(1) unsigned maps to smallint.
_UNSIGNED_RE = re.compile(r"\b(unsigned|zerofill)\b", re.IGNORECASE)


@dataclass(frozen=True)
class ObjectKey:
    """Stable identity of an inventory object: its kind plus name."""

    kind: str
    name: str


@dataclass(frozen=True)
class Finding:
    """A single rule match against one inventory object."""

    object: ObjectKey
    rule_id: str
    classification: Classification
    risk: str
    recommendation: str
    effort: Optional[EffortLevel] = None
    # Defaults to LOSS -- a real gap -- which is what every rule historically reported.
    # A rule whose finding is throughput advice on an otherwise-correct conversion opts
    # into RECOMMENDATION; see AutoIncrementRule.
    note_kind: ConversionNoteKind = ConversionNoteKind.LOSS


def _base_type(mysql_type: str) -> str:
    """Return the lower-cased base type token of a MySQL type declaration.

    For example ``"VARCHAR(100)"`` -> ``"varchar"`` and ``"INT UNSIGNED"`` ->
    ``"int"``.
    """
    token = mysql_type.strip().lower()
    for separator in ("(", " "):
        index = token.find(separator)
        if index != -1:
            token = token[:index]
    return token


def _is_case_insensitive_collation(collation: str | None) -> bool:
    """Return ``True`` for a MySQL case-insensitive collation (``*_ci``)."""
    return bool(collation) and collation.strip().lower().endswith("_ci")


def _is_tinyint_one(mysql_type: str) -> bool:
    """Return ``True`` for ``TINYINT(1)`` (MySQL's boolean convention / BOOL).

    ``TINYINT(1) UNSIGNED`` is EXCLUDED to match the converter: an unsigned tinyint(1)
    parses to UTINYINT and is range-widened to smallint (not boolean), so flagging it
    as "mapped to boolean" would warn about a 0/1-load-failure that never happens and
    misdescribe the real smallint mapping. Signed ``tinyint(1)`` is the boolean case.
    """
    text = mysql_type or ""
    if _UNSIGNED_RE.search(text):
        return False
    return bool(_TINYINT_ONE_RE.match(text))


# ---------------------------------------------------------------------------
# Rule abstraction and concrete rules
# ---------------------------------------------------------------------------


class Rule(ABC):
    """A single compatibility rule.

    A rule inspects the inventory and emits a :class:`Finding` for every object
    it flags. Rules are stateless and extensible: new rules can be added to the
    list passed to :class:`CompatibilityAssessor` without changing the engine.
    """

    rule_id: ClassVar[str]

    @abstractmethod
    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        """Return findings for every object this rule flags in ``inventory``."""
        raise NotImplementedError


def foreign_key_clauses_kept(foreign_keys: "Sequence[ForeignKeyDef]") -> str:
    """The sentence naming each key's MATCH FULL and DEFERRABLE / INITIALLY clauses, which
    the re-created key keeps (G152), or ``""`` when no key has one (every MySQL key). Shared
    by FK_PRESERVED and the Schema Conversion note, so both steps say the same."""
    entries = []
    for fk in foreign_keys:
        clauses = ["MATCH FULL"] if fk.match_full else []
        if fk.is_deferrable:
            clauses.append(
                "DEFERRABLE INITIALLY " + ("DEFERRED" if fk.initially_deferred else "IMMEDIATE")
            )
        if clauses:
            entries.append(f"{fk.name} ({', '.join(clauses)})")
    if not entries:
        return ""
    return (
        " Their deferral and matching are kept as the source declares them: "
        + ", ".join(entries)
        + "."
    )


class ForeignKeyRule(Rule):
    """Note tables that declare foreign keys (Aurora DSQL enforces them, as of 2026-08).

    Foreign keys convert cleanly: the converter preserves them and re-creates them on
    DSQL as a post-load ``ADD CONSTRAINT`` pass (at cut over for a CDC migration), so
    this is ADVICE, not a compatibility gap -- filed as a ``RECOMMENDATION`` (like
    ``AutoIncrementRule``) so it reads "RECOMMENDED", never inflates the effort
    estimate, and stays consistent with the Schema Conversion note for the same key.
    The wording leads with what is true ("converts cleanly") and carries the runtime
    caveats DSQL documents so the operator sees them at planning time. A PostgreSQL key's
    DEFERRABLE / INITIALLY DEFERRED and MATCH FULL are kept, and named (G152); a PostgreSQL
    18 NOT ENFORCED key is not created, and has its own finding (PG_FK_NOT_ENFORCED).
    """

    rule_id = "FK_PRESERVED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            enforced = [fk for fk in table.foreign_keys if fk.created_on_target]
            if enforced:
                names = ", ".join(fk.name for fk in enforced)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Foreign key constraints ({names}) convert cleanly and "
                            "are re-created on Aurora DSQL after the data load."
                            + foreign_key_clauses_kept(enforced)
                            + " DSQL enforces them with commit-time optimistic "
                            "concurrency, so DML on referenced/referencing tables incurs "
                            "extra reads."
                        ),
                        recommendation=(
                            "No action needed -- foreign keys are preserved "
                            "automatically. Benchmark write-heavy child/parent tables "
                            "(the extra reads add cost), and note that CASCADE / SET "
                            "NULL / SET DEFAULT actions count toward DSQL's 3000-row "
                            "transaction limit. To skip re-creating them, disable "
                            "foreign-key preservation in Schema Conversion."
                        ),
                        note_kind=ConversionNoteKind.RECOMMENDATION,
                    )
                )
        return findings


# How many CHECK expressions / view matches one finding spells out before it summarises
# the rest. A finding is rendered into a UI card, a text report and an HTML report, so an
# unbounded string is unusable in all three -- but the remainder is always COUNTED, never
# silently dropped (an invisible truncation would be its own false reassurance).
_MAX_CHECKS_SHOWN = 6
_MAX_VIEW_MATCHES_SHOWN = 6


class CheckConstraintRule(Rule):
    """Flag tables with a CHECK constraint (not re-emitted by the converter).

    Aurora DSQL supports CHECK, but the converter does not translate an arbitrary
    source expression (it may use MySQL-only functions/operators), so a source-enforced
    CHECK would otherwise be silently dropped -- the table would read AUTO/"no issues"
    while losing a constraint. Flagging it MANUAL surfaces the loss (Property 8) so the
    operator can re-create the check on the target by hand where the expression is
    DSQL-compatible.
    """

    rule_id = "CHECK_CONSTRAINT_DROPPED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            if table.check_constraints:
                # Show the EXPRESSION, because that is the only thing that answers the
                # question this finding asks. MySQL auto-names an unnamed CHECK
                # ``<table>_chk_N``, so from names alone a trivially portable
                # ``amount > 0`` and an unportable ``json_valid(payload)`` were
                # indistinguishable -- yet the recommendation asked the operator to judge
                # "if its expression is Aurora DSQL-compatible" and to size the work.
                # ``CheckConstraintDef.expression`` is populated by both introspectors and
                # its own docstring says it is "kept for display so the operator can
                # re-create the check on the target by hand"; Schema Conversion already
                # prints it. Bounded like the v0.1.525 fix: the first few in full, the rest
                # named with a count, since one finding renders into a UI card and two
                # report formats. Value-free by nature -- a CHECK expression is schema
                # DDL, not row data.
                shown = table.check_constraints[:_MAX_CHECKS_SHOWN]
                rest = table.check_constraints[_MAX_CHECKS_SHOWN:]
                names = ", ".join(
                    f"{c.name}: CHECK ({c.expression})" if c.expression else c.name
                    for c in shown
                )
                if rest:
                    names += (
                        f", and {len(rest)} more ("
                        + ", ".join(c.name for c in rest)
                        + ")"
                    )
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"CHECK constraint(s) are not carried over by the converter, "
                            f"so the source's value rules would be lost on the target -- "
                            f"{names}."
                        ),
                        recommendation=(
                            "Re-create each CHECK on the target by hand when its "
                            "expression above uses only functions and operators Aurora "
                            "DSQL accepts (a plain comparison usually ports as-is; a "
                            "MySQL-specific function such as json_valid() does not), or "
                            "enforce the rule in the application layer."
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class CascadeForeignKeyRule(Rule):
    """Flag foreign keys whose referential ACTION cannot survive CDC.

    Distinct from :class:`ForeignKeyRule` (which flags the constraint itself). A
    ``ON DELETE/UPDATE CASCADE``, ``SET NULL`` or ``SET DEFAULT`` makes MySQL change
    CHILD rows on its own, and InnoDB performs that change INSIDE the storage engine
    -- so the resulting child-row writes never reach the binary log (MySQL bug
    #32506, closed as documented behavior; the same reason cascaded actions do not
    fire triggers). Debezium reads the binary log, so a CDC stream simply never sees
    them; and the tool applies foreign keys only at cut over (never while the sink
    streams), so no target-side FK re-performs the cascade in flight. The child rows
    the source cascade removed/nulled are therefore left as ORPHANS on the target: the
    key is still added at cut over (``NOT VALID``, it refuses every new orphan), but
    **Aurora DSQL's validation of the loaded rows fails on them**, and Cut over holds
    Finish on such a key until it validates (G142) -- an actionable per-FK report (the
    orphan count and a query that lists the rows), not silent divergence.

    This is a limitation of every binlog-based CDC tool (Debezium, DMS, Maxwell),
    not of this migrator, but it is invisible unless it is called out BEFORE the
    stream starts -- hence flagging it here, at planning time, rather than leaving
    the operator to discover the blocked constraint at cut-over.
    """

    rule_id = "FK_CASCADE_CDC_GAP"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            affected = [fk for fk in table.foreign_keys if fk.has_cascade_action]
            if not affected:
                continue
            detail = ", ".join(
                f"{fk.name} ("
                + ", ".join(
                    part
                    for part in (
                        f"ON DELETE {fk.on_delete.upper()}" if fk.on_delete else None,
                        f"ON UPDATE {fk.on_update.upper()}" if fk.on_update else None,
                    )
                    if part
                )
                + ")"
                for fk in affected
            )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"Foreign keys with automatic referential actions "
                        f"({detail}) change CHILD rows inside the InnoDB engine, so "
                        "those changes are NOT written to the binary log. CDC reads "
                        "the binary log, so it cannot replicate them: the parent "
                        "change arrives on the target but the cascaded child change "
                        "does not, leaving orphaned rows during replication. This "
                        "affects every binlog-based CDC tool (MySQL bug #32506)."
                    ),
                    recommendation=(
                        "Before starting CDC, replace the automatic action with "
                        "EXPLICIT child-row statements in the application (e.g. "
                        "delete the children, then the parent) so the changes are "
                        "logged and replicated. The tool re-creates the foreign key "
                        "on Aurora DSQL only at cut over -- never during replication, "
                        "so the sink is not tripped by out-of-order rows -- and any "
                        "orphaned child rows left by an un-replicated cascade then "
                        "fail Aurora DSQL's validation of that key, which holds the "
                        "cut over until they are fixed, and are reported by the "
                        "Validation orphan check. Quiesce source writes before the "
                        "final cut-over comparison so the divergence is caught."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class TriggerRule(Rule):
    """Flag triggers (Aurora DSQL has no trigger object at all)."""

    rule_id = "TRIGGER_UNSUPPORTED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        return [
            Finding(
                object=ObjectKey(KIND_TRIGGER, trigger.name),
                rule_id=self.rule_id,
                classification=Classification.UNSUPPORTED,
                risk=(
                    "Aurora DSQL has no trigger object; there is no target to "
                    "migrate the trigger into."
                ),
                recommendation=(
                    "Reimplement the trigger logic in the application or with "
                    "event-driven processing (e.g., EventBridge)."
                ),
                effort=EffortLevel.SIGNIFICANT,
            )
            for trigger in inventory.triggers
        ]


class ProcedureRule(Rule):
    """Flag stored procedures/functions (Aurora DSQL has no procedural routines)."""

    rule_id = "PROC_PLPGSQL"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for routine in inventory.routines:
            kind = _routine_kind(routine)
            noun = (
                "stored procedure"
                if kind == KIND_PROCEDURE
                else "function"
                if kind == KIND_FUNCTION
                else "routine"
            )
            findings.append(
                Finding(
                    object=ObjectKey(kind, routine.name),
                    rule_id=self.rule_id,
                    classification=Classification.UNSUPPORTED,
                    risk=(
                        f"Aurora DSQL does not support procedural {noun}s; there "
                        f"is no target object to migrate the {noun} into."
                    ),
                    recommendation=(
                        "Reimplement as a LANGUAGE SQL function or move the logic "
                        "to the application or a Lambda function."
                    ),
                    effort=EffortLevel.SIGNIFICANT,
                )
            )
        return findings


class EventRule(Rule):
    """Flag scheduled events (Aurora DSQL has no event scheduler)."""

    rule_id = "EVENT_UNSUPPORTED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        return [
            Finding(
                object=ObjectKey(KIND_EVENT, event.name),
                rule_id=self.rule_id,
                classification=Classification.UNSUPPORTED,
                risk=(
                    "Aurora DSQL has no event scheduler; there is no target to "
                    "migrate the scheduled event into."
                ),
                recommendation=(
                    "Reimplement the schedule outside the database, e.g. with "
                    "Amazon EventBridge Scheduler invoking a Lambda function."
                ),
                effort=EffortLevel.SIGNIFICANT,
            )
            for event in inventory.events
        ]


# Why a different key buys throughput (the optional half of every AUTO_INCREMENT finding).
_MONOTONIC_KEY_NOTE = (
    " Separately, for higher insert throughput: DSQL stores rows in primary-key order, so "
    "a monotonically increasing key concentrates writes on one partition."
)


class AutoIncrementRule(Rule):
    """What becomes of a MySQL AUTO_INCREMENT key's value generation, per table.

    Schema Conversion defaults a single-column integer AUTO_INCREMENT key that bigint holds
    to a cached identity (``default_primary_key_strategy``), so the key keeps generating --
    worded like the PostgreSQL rule, whose serial / identity key has the same default. It
    used to keep a plain integer, which reached cut over with no generator: the
    application's INSERTs that leave the key out failed. A ``bigint unsigned`` key (wider
    than an identity's bigint) and a key column inside a composite primary key still lose
    the generation, and say so.
    """

    rule_id = "AUTO_INCREMENT"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        # Lazy: converter imports this module at load time.
        from dsql_migrator.core.converter import (
            IDENTITY_FALLBACK_CAVEAT,
            MYSQL_IDENTITY_DIFFERENCES,
            generated_key_is_integer,
            identity_strategy_applies,
            mysql_identity_is_lossless,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            column = table.auto_increment_column
            if not column:
                continue
            single = table.primary_key == [column]
            declared = next((c.mysql_type for c in table.columns if c.name == column), "")
            keeps = single and mysql_identity_is_lossless(table)
            if keeps:
                risk = (
                    f"The key from AUTO_INCREMENT column '{column}' converts cleanly AND "
                    "keeps generating values: the default conversion makes it a cached "
                    "identity, so the application still does not supply the key. Two "
                    "things change. The column is WIDENED to bigint (Aurora DSQL supports "
                    "an identity only on bigint) — lossless for the values, but check "
                    "anything that assumes a 4-byte id. And because each DSQL node draws "
                    "its own block of values from the cache, the key is no longer "
                    "gap-free or strictly increasing, which also spreads inserts instead of "
                    "concentrating them on one partition (DSQL stores rows in primary-key "
                    "order). " + MYSQL_IDENTITY_DIFFERENCES
                )
                recommendation = (
                    "Schema Conversion defaults to 'Server-generated (IDENTITY)' for this "
                    "table, and Full Load loads the source's own ids before advancing the "
                    "sequence past them and past the source's own counter (again at cut "
                    "over); check the insert idioms above in the application. Switch to "
                    "'Keep source PK' only if the application supplies the key itself — "
                    "that, like a 'Composite key', leaves the column a plain integer with "
                    "no generator."
                )
            elif single and not identity_strategy_applies(table):
                risk = (
                    f"The {declared} key from AUTO_INCREMENT column '{column}' is not an "
                    "integer, and an Aurora DSQL identity is (bigint), so Aurora DSQL will "
                    "NOT generate it: the APPLICATION must supply the value on every "
                    "insert — code that relied on the database generating it fails or "
                    "collides." + _MONOTONIC_KEY_NOTE
                )
                recommendation = (
                    "Supply the value from the application, or remodel the key as an "
                    "integer on the source before migrating it."
                )
            elif single:
                risk = (
                    f"The bigint unsigned key from AUTO_INCREMENT column '{column}' is "
                    "kept as numeric(20,0) to hold its whole range, so Aurora DSQL will NOT "
                    "generate it: an identity must be bigint. The APPLICATION must supply "
                    "the value on every insert — code that relied on the database "
                    "generating it fails or collides." + _MONOTONIC_KEY_NOTE
                )
                recommendation = (
                    "Decide at Schema Conversion who generates the key: keep it and supply "
                    "the value from the application, or choose 'Server-generated "
                    "(IDENTITY)' if every value fits a signed bigint (up to "
                    "9223372036854775807) — a larger one fails to load."
                )
            elif generated_key_is_integer(table):
                risk = (
                    f"The integer key from AUTO_INCREMENT column '{column}' converts "
                    "cleanly, but Aurora DSQL will NOT generate it: the column stays a "
                    "plain integer, so the APPLICATION must supply the value on every "
                    "insert — code that relied on the database generating it fails or "
                    "collides." + _MONOTONIC_KEY_NOTE
                )
                # Only name a strategy the picker OFFERS: its IDENTITY tile is rendered
                # only for a single-column key that IS the generated column.
                recommendation = (
                    "IDENTITY cannot be used for this table because its primary key is "
                    f"composite ({', '.join(table.primary_key)}) and a DSQL identity "
                    "applies to a single column, so the value must come from the "
                    "application — or add an identity to the column on the target after "
                    "apply, " + IDENTITY_FALLBACK_CAVEAT + "."
                )
            else:
                # A bigint unsigned (numeric(20,0) on the target) or a non-integer key: no
                # identity can be added to it later either (an identity is bigint, and
                # Aurora DSQL cannot change a column's type).
                risk = (
                    f"The {declared} key from AUTO_INCREMENT column '{column}' is part of "
                    f"a composite primary key ({', '.join(table.primary_key)}) and is not "
                    "converted to an integer Aurora DSQL could generate, so Aurora DSQL "
                    "will NOT generate it: the APPLICATION must supply the value on every "
                    "insert — code that relied on the database generating it fails or "
                    "collides." + _MONOTONIC_KEY_NOTE
                )
                recommendation = (
                    "Supply the value from the application: IDENTITY cannot be used for "
                    "this table (a DSQL identity is a single bigint column)."
                )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=risk,
                    recommendation=recommendation
                    + " Optional, for throughput only: a UUID/random key spreads the "
                    "writes further.",
                    effort=EffortLevel.MEDIUM,
                    # Advice only where the key keeps generating; where nothing generates
                    # it, the inserts that leave it out fail after cut over -- a gap the
                    # table is graded for, not optional advice.
                    note_kind=(
                        ConversionNoteKind.RECOMMENDATION if keeps else ConversionNoteKind.LOSS
                    ),
                )
            )
        return findings


class NoPrimaryKeyRule(Rule):
    """Flag tables without a primary key: Aurora DSQL creates one (verified live), but this
    tool cannot migrate it -- Full Load reads by primary-key keyset and CDC applies by key."""

    rule_id = "NO_PRIMARY_KEY"

    def __init__(self, *, is_postgres: bool = False) -> None:
        """``is_postgres`` selects which column types Aurora DSQL cannot key."""
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import unkeyable_columns

        findings: list[Finding] = []
        for table in inventory.tables:
            if not table.primary_key:
                # A UNIQUE index whose columns are all NOT NULL already identifies a row:
                # the natural key to promote, named rather than a generic "add a UUID".
                # Not a partial one (unique only over the rows its WHERE matches: soft-
                # deleted rows may share the email), an expression one (reflected without
                # its expression, so its columns are not what is unique), nor one over a
                # column Aurora DSQL cannot key (bytea, json, interval: promoted, the table
                # would be refused at both steps).
                nullable = {c.name for c in table.columns if c.nullable}
                expression = set(table.expression_indexes)
                unkeyable = unkeyable_columns(table, is_postgres=self._is_postgres)
                candidates = [
                    f"{index.name} ({', '.join(index.columns)})"
                    for index in table.indexes
                    if index.unique
                    and index.columns
                    and not set(index.columns) & nullable
                    and not set(index.columns) & set(unkeyable)
                    and not (index.where or "").strip()
                    and index.name not in expression
                ]
                recommendation = (
                    f"Promote a unique key to the primary key on the source -- "
                    f"{', '.join(candidates[:3])} already identifies each row -- or add one "
                    "(e.g. a UUID/random key) before migrating the table."
                    if candidates
                    else "Add a primary key on the source (e.g. a UUID/random key) before "
                    "migrating the table."
                )
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk=(
                            "This table has no primary key. Aurora DSQL can create a table "
                            "without one, but this migrator cannot migrate it: Full Load "
                            "reads rows by primary-key keyset and CDC replicates by primary "
                            "key, so there is no unit to paginate or stable row identity."
                        ),
                        recommendation=recommendation,
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class CaseInsensitiveCollationRule(Rule):
    """Flag tables with a case-insensitive (``*_ci``) collation."""

    rule_id = "CI_COLLATION"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            ci_columns = [
                column.name
                for column in table.columns
                if _is_case_insensitive_collation(column.collation)
            ]
            if ci_columns:
                names = ", ".join(ci_columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Case-insensitive collation on columns ({names}) "
                            "changes sorting and uniqueness under the DSQL 'C' "
                            "collation."
                        ),
                        recommendation=(
                            "Review sorting/uniqueness behavior and adjust "
                            "comparisons for the 'C' collation."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class PartitionedTableRule(Rule):
    """Flag tables that use source-native partitioning (MySQL or PostgreSQL).

    The risk text is TAKEN FROM THE CONVERTER (``_partitioned_table_warning``) rather than
    written again here, because the two had drifted into giving the operator different
    answers about the same table: Evaluation said "Manual partitioning is not used by
    Aurora DSQL" and "Remove the manual partitioning", graded MANUAL/LOSS with MEDIUM
    effort, while Schema Conversion graded the identical fact a RECOMMENDATION with zero
    losses and a clean target DDL. The Evaluation text was also wrong twice over for a
    PostgreSQL source: it used MySQL vocabulary, and "remove the manual partitioning" is
    work the operator must NOT do -- this tool treats the source as read-only, and nothing
    has to be removed on the TARGET either (the converter emits no PARTITION BY and
    collapses the partitions into one table).
    """

    rule_id = "PARTITIONED_TABLE"

    def __init__(self, *, is_postgres: bool = False) -> None:
        """``is_postgres`` selects the engine-correct wording (declarative vs native)."""
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import _partitioned_table_warning

        findings: list[Finding] = []
        for table in inventory.tables:
            if table.partitioned:
                conversion_note = _partitioned_table_warning(
                    table, is_postgres=self._is_postgres
                )
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            conversion_note.message
                            if conversion_note is not None
                            else "The source table is partitioned, which Aurora DSQL does "
                            "not have — it distributes rows automatically by primary key."
                        ),
                        recommendation=(
                            "No action is required to migrate it: Schema Conversion drops "
                            "the partitioning automatically and no data is lost (do NOT "
                            "change the source -- this tool only reads it). What needs a "
                            "decision is whatever depended on the partitions, as described "
                            "above. If you replicate with CDC, pause partition maintenance "
                            "until cut over, or plan to repair the table on the target: "
                            + (
                                "dropping, detaching or truncating a partition (a TRUNCATE "
                                "on a partition is not replicated at all) removes rows, and "
                                "attaching one that already holds rows adds them, with no "
                                "row-level change CDC can carry -- so Aurora DSQL keeps the "
                                "removed rows and lacks the attached ones."
                                if self._is_postgres
                                else "DROP or TRUNCATE PARTITION removes rows without a "
                                "row-level delete CDC can carry, so they stay on Aurora DSQL."
                            )
                        ),
                        effort=EffortLevel.MEDIUM,
                        # Advisory, matching the converter's own grading of the same fact:
                        # the conversion succeeds and the target DDL is valid, so this is
                        # review work to plan, not a defect blocking the migration.
                        note_kind=ConversionNoteKind.RECOMMENDATION,
                    )
                )
        return findings


class SpatialTypeRule(Rule):
    """Flag spatial columns: auto-substituted to bytea, but review is needed.

    MySQL spatial types have no native Aurora DSQL type. The converter does NOT
    block the table -- it substitutes each spatial column to ``bytea`` and keeps
    the raw WKB bytes (verified end-to-end through Full Load and CDC). But the
    spatial type, its operators, and spatial indexes are lost, so the column is
    flagged ``MANUAL`` for review rather than ``UNSUPPORTED`` (which would imply
    a redesign is required before the table can migrate at all).
    """

    rule_id = "SPATIAL_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            spatial = [
                f"{column.name} ({column.mysql_type})"
                for column in table.columns
                if _base_type(column.mysql_type) in _SPATIAL_TYPE_BASES
            ]
            if spatial:
                names = ", ".join(spatial)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Spatial columns ({names}) have no native Aurora DSQL "
                            "type; they are auto-converted to bytea (raw WKB bytes "
                            "preserved), but spatial operators and spatial indexes "
                            "are not available on the target."
                        ),
                        recommendation=(
                            "Confirm raw WKB in a bytea column is sufficient; if "
                            "spatial queries/indexes are needed, handle them in the "
                            "application or a spatial service (e.g. Amazon "
                            "OpenSearch Service)."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


def _decimal_precision(mysql_type: str) -> Optional[int]:
    """Return the declared precision of a DECIMAL/NUMERIC type, or ``None``."""
    match = _DECIMAL_PRECISION_RE.search(mysql_type)
    return int(match.group(1)) if match else None


class TooManyColumnsRule(Rule):
    """Flag tables that exceed the DSQL per-table column limit."""

    rule_id = "TOO_MANY_COLUMNS"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            count = len(table.columns)
            if count > _MAX_COLUMNS_PER_TABLE:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk=(
                            f"The table has {count} columns; Aurora DSQL allows at "
                            f"most {_MAX_COLUMNS_PER_TABLE} columns per table."
                        ),
                        recommendation=(
                            "Reduce the column count (drop/merge columns) or split "
                            "the table vertically before migrating."
                        ),
                        effort=EffortLevel.SIGNIFICANT,
                    )
                )
        return findings


class TooManyIndexesRule(Rule):
    """Flag tables that exceed the DSQL per-table index limit.

    Caught here, at planning time, because the failure otherwise surfaces at the
    WORST possible moment: the secondary indexes are created by post-load
    ``CREATE INDEX ASYNC`` statements, so the limit is hit only AFTER Full Load has
    finished writing every row -- turning a multi-hour load into a failed table that
    a re-run cannot fix (the limit is not transient). The operator has to change the
    schema and start over.

    The budget is compared against SECONDARY indexes only: DSQL always requires a
    primary key and its index counts toward the 24, leaving 23 for the source's
    reflected indexes (which exclude the PK).
    """

    rule_id = "TOO_MANY_INDEXES"

    def __init__(self, *, is_postgres: bool = False) -> None:
        """``is_postgres`` selects which indexes the conversion can emit (see evaluate)."""
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        # Count what Schema Conversion can actually create (``_emittable_secondary_indexes``,
        # the filter its own budget note uses): an index over a bytea column, or over more
        # than 8 columns, is skipped and reported for that reason, so counting it here
        # claimed a budget overflow -- and a number of kept indexes -- that the generated
        # DDL never had.
        from dsql_migrator.core.converter import _emittable_secondary_indexes

        findings: list[Finding] = []
        for table in inventory.tables:
            count = len(
                _emittable_secondary_indexes(table, is_postgres=self._is_postgres)
            )
            if count <= _MAX_SECONDARY_INDEXES_PER_TABLE:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    # The outcome, not a failure that this tool's own pipeline makes
                    # impossible. Since v0.1.433 Schema Conversion truncates the index
                    # list at the budget and names what it dropped, so error 54000 never
                    # fires -- but this text was left predicting it, telling the operator
                    # to prepare for a post-load failure and a re-run decision when the
                    # real result is a target that loads cleanly with specific indexes
                    # silently absent. Opposite outcomes from the two steps, and the
                    # quieter one is the one that actually happens.
                    risk=(
                        f"The table has {count} secondary indexes Aurora DSQL can create; "
                        f"with its primary key that is {count + 1} against Aurora DSQL's "
                        "limit of "
                        f"{_MAX_INDEXES_PER_TABLE} indexes per table. Schema Conversion "
                        "therefore OMITS the over-budget indexes from the applied script "
                        f"(it names them) and keeps {_MAX_SECONDARY_INDEXES_PER_TABLE}, "
                        "because emitting them would fail with error 54000 \"more than "
                        f"{_MAX_INDEXES_PER_TABLE} indexes per table are not allowed\" "
                        "after Full Load had already written every row. So the load "
                        "SUCCEEDS and the target is left missing those indexes -- queries "
                        "relying on them fall back to scans."
                    ),
                    recommendation=(
                        "Choose which indexes to keep rather than letting the cut-off "
                        f"choose: drop the ones you no longer need so at most "
                        f"{_MAX_SECONDARY_INDEXES_PER_TABLE} secondary indexes remain "
                        "(unused or redundant indexes are common — review the source's "
                        "index usage), or split the table. Decide before generating the "
                        "DDL: re-running Full Load will not clear this, since the limit is "
                        "not transient, and Schema Conversion's own warning lists exactly "
                        "which indexes it left out."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class TooManyKeyColumnsRule(Rule):
    """Flag primary keys / secondary indexes that span more than 8 columns.

    Aurora DSQL caps a key at 8 columns (error 54011 "more than 8 column keys are
    not allowed"); MySQL allows 16, so a 9..16-column key migrates from a perfectly
    valid source. The two cases fail at different times, which is why both are
    reported on one finding but described separately:

    - a **primary key** over 8 columns fails when the table DDL is applied, so
      nothing loads at all -- UNSUPPORTED, the table cannot migrate as-is;
    - a **secondary index** over 8 columns fails its post-load
      ``CREATE INDEX ASYNC``, i.e. only after Full Load has written every row --
      the same late-failure shape as :class:`TooManyIndexesRule`, and the reason
      this belongs at planning time rather than in the apply log.

    Classified by the worst case present: UNSUPPORTED when the primary key itself
    is over the limit (no data can land), MANUAL when only secondary indexes are
    (the data lands; those indexes do not).
    """

    rule_id = "TOO_MANY_KEY_COLUMNS"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            pk_columns = len(table.primary_key)
            pk_over = pk_columns > _MAX_KEY_COLUMNS
            wide_indexes = [
                (index.name, len(index.columns))
                for index in table.indexes
                if len(index.columns) > _MAX_KEY_COLUMNS
            ]
            if not pk_over and not wide_indexes:
                continue

            risk_parts: list[str] = []
            fix_parts: list[str] = []
            if pk_over:
                risk_parts.append(
                    f"The primary key spans {pk_columns} columns "
                    f"({', '.join(table.primary_key)})"
                )
                fix_parts.append(
                    "narrow the primary key to at most "
                    f"{_MAX_KEY_COLUMNS} columns (the trailing columns are rarely "
                    "needed for uniqueness -- move them to a secondary index)"
                )
            if wide_indexes:
                listed = ", ".join(
                    f"{name} ({count} columns)" for name, count in wide_indexes
                )
                risk_parts.append(
                    f"{len(wide_indexes)} secondary "
                    f"{'index' if len(wide_indexes) == 1 else 'indexes'} span more "
                    f"than {_MAX_KEY_COLUMNS} columns: {listed}"
                )
                fix_parts.append(
                    f"trim each wide index to at most {_MAX_KEY_COLUMNS} columns, "
                    "keeping the most selective ones leftmost"
                )

            when = []
            if pk_over:
                when.append(
                    "The primary key is rejected when the table DDL is applied, so "
                    "the table -- and its data -- never migrates"
                )
            if wide_indexes:
                when.append(
                    "A wide secondary index is left out of the converted DDL (it would "
                    "fail its CREATE INDEX ASYNC after Full Load), so queries relying on it "
                    "fall back to a scan, and a UNIQUE one's uniqueness is not enforced"
                )

            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=(
                        Classification.UNSUPPORTED if pk_over else Classification.MANUAL
                    ),
                    risk=(
                        f"{'; '.join(risk_parts)}. Aurora DSQL allows at most "
                        f"{_MAX_KEY_COLUMNS} columns in a primary key or secondary "
                        "index (error 54011 "
                        f'"more than {_MAX_KEY_COLUMNS} column keys are not allowed"). '
                        f"{'. '.join(when)}."
                    ),
                    recommendation=(
                        f"Before converting, {' and '.join(fix_parts)}. Decide now: "
                        "re-running the migration will not clear this, since the limit "
                        "is not transient."
                    ),
                    effort=(
                        EffortLevel.SIGNIFICANT if pk_over else EffortLevel.MEDIUM
                    ),
                )
            )
        return findings


class ByteaKeyRule(Rule):
    """Flag a primary key or secondary index over a column that converts to ``bytea``.

    Aurora DSQL cannot index ``bytea`` ("datatype bytea is not supported in a key"), so a
    primary key over a BINARY/VARBINARY/BLOB column (a PostgreSQL ``bytea`` one, for a PG
    source) makes the generated CREATE TABLE REJECTED, and a secondary index over one is
    not created -- a UNIQUE one then stops enforcing uniqueness. Evaluation used to read
    these tables AUTO ("No DSQL compatibility issues") with a 100 'Ready' score while the
    very next step graded the same table UNSUPPORTED; the common MySQL key of a
    ``UUID_TO_BIN()`` value in ``BINARY(16)`` is exactly this case.

    The detection and the risk text are the converter's own (``bytea_key_parts`` and
    ``_bytea_key_warning`` / ``_pg_bytea_key_warning``), the :class:`PartitionedTableRule`
    pattern, so the two steps cannot disagree about the same table.
    """

    rule_id = "BYTEA_KEY"

    def __init__(self, *, is_postgres: bool = False) -> None:
        """``is_postgres`` selects the PostgreSQL detection (genuine ``bytea`` only)."""
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import (
            _bytea_key_warning,
            _pg_bytea_key_warning,
            bytea_key_parts,
        )

        note_for = _pg_bytea_key_warning if self._is_postgres else _bytea_key_warning
        findings: list[Finding] = []
        for table in inventory.tables:
            note = note_for(table)
            if note is None:
                continue
            pk_bytea, _indexes, unique_bytea = bytea_key_parts(
                table, is_postgres=self._is_postgres
            )
            if pk_bytea:
                recommendation = (
                    "Plan a re-key before migrating: no conversion option changes a bytea "
                    "key, so this table cannot be created on Aurora DSQL as it is. Give it "
                    "a key DSQL can index on the source (for example the UUID as text, or "
                    "the hash as its hex string), and update the foreign keys that "
                    "reference it."
                )
                effort = EffortLevel.SIGNIFICANT
            else:
                recommendation = (
                    "Decide whether the target needs these indexes: a non-unique one can "
                    "simply be left out (those queries scan instead), or replaced after the "
                    "load by an index on an indexable form of the value, such as its hex "
                    "text."
                )
                if unique_bytea:
                    recommendation += (
                        " A UNIQUE one must be enforced another way (in the application, "
                        "or by a UNIQUE index on such a derived value), or the target "
                        "accepts duplicates."
                    )
                effort = EffortLevel.MEDIUM if unique_bytea else EffortLevel.SIMPLE
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=note.classification,
                    risk=note.message,
                    recommendation=recommendation,
                    effort=effort,
                )
            )
        return findings


class IdentifierLengthRule(Rule):
    """Flag identifiers PostgreSQL/Aurora DSQL truncate to 63 bytes.

    MySQL allows 64 characters (more bytes in UTF-8); Aurora DSQL silently truncates to 63
    BYTES. Two column names that share their first 63 bytes collide and the CREATE TABLE is
    REJECTED (UNSUPPORTED); a longer name that does not collide just migrates shortened
    (advice). Graded by the converter's own ``_identifier_length_warning``, which Schema
    Conversion shows on the same table.
    """

    rule_id = "IDENTIFIER_LENGTH"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import (
            IDENTIFIER_COLLISION_REMEDY,
            _identifier_length_warning,
            index_renames,
        )

        # The index names Schema Conversion emits (a colliding index is renamed).
        renames = index_renames(inventory)
        findings: list[Finding] = []
        for table in inventory.tables:
            note = _identifier_length_warning(table, index_names=renames.get(table.name))
            if note is None:
                continue
            collides = note.classification is Classification.UNSUPPORTED
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=note.classification,
                    risk=note.message,
                    recommendation=(
                        IDENTIFIER_COLLISION_REMEDY
                        if collides
                        else "Optional: shorten the names on the source if they must match "
                        "exactly on the target."
                    ),
                    effort=EffortLevel.MEDIUM if collides else EffortLevel.SIMPLE,
                    note_kind=note.kind,
                )
            )
        return findings


class NameCollisionRule(Rule):
    """Report names that tables, views and indexes would share in their target schema.

    Aurora DSQL keeps a schema's tables, views and indexes in one namespace (names cut to 63
    bytes), while MySQL scopes an index name to its table. Schema Conversion renames the
    colliding indexes itself (:func:`~dsql_migrator.core.converter.index_renames`), so an
    index collision -- the auto-named foreign-key index ``user_id`` on every child table, an
    index named after another table -- is advice (RECOMMENDATION): every index and table is
    still created. What no rename can fix is two tables or views whose names agree in their
    first 63 bytes: only one can exist and the other's rows would land in it (UNSUPPORTED,
    shorten the names on the source). Detection is the converter's own, which Schema
    Conversion reports on the same objects.
    """

    rule_id = "NAME_COLLISION"

    def __init__(self, *, is_postgres: bool = False) -> None:
        """``is_postgres`` selects which indexes the conversion emits (and so can collide)."""
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import (
            INDEX_RENAME_ADVICE,
            index_rename_detail,
            index_renames,
            name_collision_detail,
            name_collision_remedy,
            name_collisions,
            relation_collisions,
        )

        collisions = name_collisions(inventory, is_postgres=self._is_postgres)
        renames = index_renames(inventory, is_postgres=self._is_postgres)
        findings: list[Finding] = []
        objects = [(KIND_TABLE, table.name) for table in inventory.tables] + [
            (KIND_VIEW, view.name) for view in inventory.views
        ]
        for kind, name in objects:
            hits = collisions.get(name) or []
            clashes = relation_collisions(hits)
            if clashes:
                findings.append(
                    Finding(
                        object=ObjectKey(kind, name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk=name_collision_detail(name, clashes),
                        recommendation=name_collision_remedy(clashes),
                        effort=EffortLevel.MEDIUM,
                    )
                )
            if kind == KIND_TABLE and name in renames:
                findings.append(
                    Finding(
                        object=ObjectKey(kind, name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=index_rename_detail(name, renames[name], hits),
                        recommendation=INDEX_RENAME_ADVICE,
                        note_kind=ConversionNoteKind.RECOMMENDATION,
                    )
                )
        return findings


# Two or more columns that can each hold up to their own limit can, together, exceed the
# row limit -- which the per-column SOURCE_VALUE_SIZE probe cannot see.
OVERSIZED_ROW_SENTENCE = (
    " Together they can also exceed Aurora DSQL's 2 MiB row limit (maximum row size "
    "exceeded) while each value fits its own limit, which the per-column size probe cannot "
    "see: check the combined size of the widest rows before loading."
)


class OversizedLobRule(Rule):
    """Flag columns whose MySQL LOB/TEXT or JSON type can exceed the DSQL 1 MiB limit."""

    rule_id = "OVERSIZED_LOB"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) in _OVERSIZED_LOB_BASES
            ]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        # The dead letter is per row only while the change event still
                        # fits the Kafka producer limit (MaxMessageBytes, 4 MiB by default
                        # in deploy/cdc-stack). A larger event fails the single-task source
                        # connector (it sets no errors.tolerance), which stops capture for
                        # every table. binlog_row_image=FULL (a CDC prerequisite) puts the
                        # value in both images of an UPDATE, so a ~2 MiB document is enough.
                        risk=(
                            f"Columns ({names}) use a large MySQL LOB/TEXT or JSON type "
                            "whose values can exceed the Aurora DSQL 1 MiB per-value "
                            "limit. An oversized value cannot be stored: the row is "
                            "quarantined during Full Load or dead-lettered during CDC, "
                            "and reloading cannot fix it. A change event larger than "
                            "the CDC message limit (4 MiB by default; an UPDATE carries "
                            "the value twice) is not dead-lettered: it stops CDC for "
                            "every table until the column is excluded at capture."
                            + (OVERSIZED_ROW_SENTENCE if len(columns) > 1 else "")
                        ),
                        # "Confirm no value exceeds 1 MiB" was homework the tool can now
                        # do: the Data Migration prerequisite checks probe each column
                        # (PrerequisiteCheckId.SOURCE_VALUE_SIZE). The PG sibling was
                        # pointed at it in v0.1.531; this is the MySQL half.
                        recommendation=(
                            "Run the Data Migration prerequisite checks: they probe each "
                            "column and report whether a value ALREADY exceeds that "
                            "column's limit. If one does, move the content to external "
                            "storage (e.g. Amazon S3) and store a reference instead, or "
                            "exclude the column on the Data Migration step. For a TEXT "
                            "family or JSON column the limit applies to the COMPRESSED "
                            "size (outside a key), so a highly compressible value may "
                            "still fit; a BLOB family column becomes bytea, which Aurora "
                            "DSQL does not compress."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class DecimalPrecisionRule(Rule):
    """Flag DECIMAL/NUMERIC columns whose precision exceeds the DSQL maximum."""

    rule_id = "NUMERIC_PRECISION"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            over = [
                f"{column.name} ({column.mysql_type})"
                for column in table.columns
                if _base_type(column.mysql_type) in _DECIMAL_BASES
                and (precision := _decimal_precision(column.mysql_type)) is not None
                and precision > _MAX_NUMERIC_PRECISION
            ]
            if over:
                names = ", ".join(over)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk=(
                            f"Columns ({names}) exceed the Aurora DSQL numeric "
                            f"maximum precision of {_MAX_NUMERIC_PRECISION} digits, "
                            "so the value cannot be stored without loss."
                        ),
                        recommendation=(
                            f"Reduce the precision to {_MAX_NUMERIC_PRECISION} digits "
                            "or fewer, or store "
                            "the value as text if full precision must be kept."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class EnumSetRule(Rule):
    """Flag ENUM/SET columns (no native DSQL type; mapped to text)."""

    rule_id = "ENUM_SET_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            enums, sets = [], []
            for column in table.columns:
                base = _base_type(column.mysql_type)
                if base not in _ENUM_SET_BASES:
                    continue
                (enums if base == "enum" else sets).append(column.name)
            if not (enums or sets):
                continue
            # ENUM and SET are NOT the same problem, and merging them stated a loss the
            # converter does not have. For an ENUM the converter emits
            # ``TEXT CHECK (col IN (...))``, so the allowed values ARE preserved -- the
            # user manual documents exactly that mapping in all three languages and names
            # ORDERING as the loss. The old text told the operator to "Re-enforce the
            # allowed values in the application layer" at effort=SIMPLE, i.e. it budgeted
            # work Schema Conversion had already done, and never mentioned ordering; the
            # column that genuinely loses its domain (SET, whose multi-value combinations
            # get no CHECK) was described under the same sentence as the same problem.
            # The hedge "(or with a CHECK constraint if supported)" also equivocated about
            # a feature DSQL supports and this tool already relies on.
            risks, remedies = [], []
            if enums:
                risks.append(
                    f"ENUM column(s) {', '.join(enums)} are mapped to text with a "
                    "generated CHECK (col IN (...)), so the ALLOWED VALUES are kept; "
                    "what is lost is ENUM ORDERING -- comparisons and ORDER BY follow "
                    "text collation, not the declaration order -- and MySQL's empty-string "
                    "sentinel for an invalid value would now violate that CHECK"
                )
                remedies.append(
                    "For the ENUM column(s), review any comparison or ORDER BY that "
                    "relied on the declaration order (store an explicit sort key if it "
                    "matters); no application-side value check is needed."
                )
            if sets:
                risks.append(
                    f"SET column(s) {', '.join(sets)} are mapped to text as the "
                    "comma-joined value with NO CHECK, so both the allowed-value set and "
                    "the multi-value semantics are lost"
                )
                remedies.append(
                    "For the SET column(s), enforce the allowed values and the "
                    "multi-value semantics in the application (or remodel to a child "
                    "table or a jsonb array)."
                )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        "Aurora DSQL has no ENUM or SET type: " + "; ".join(risks) + "."
                    ),
                    recommendation=" ".join(remedies),
                    effort=EffortLevel.SIMPLE,
                )
            )
        return findings


class TinyIntBooleanRule(Rule):
    """Flag TINYINT(1)/BOOL columns (mapped to boolean by convention).

    The converter maps ``TINYINT(1)`` to Aurora DSQL ``boolean`` (MySQL's boolean
    convention). A stored value outside ``{0, 1}`` has no boolean representation
    and aborts Full Load, so the column needs review -- ``MANUAL``, matching the
    converter's own classification, not a silent AUTO verdict.
    """

    rule_id = "TINYINT_BOOLEAN"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import (
            PrimaryKeyStrategy,
            default_primary_key_strategy,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            # A tinyint(1) AUTO_INCREMENT key the default conversion makes a bigint
            # identity is no boolean (its own finding says what it becomes).
            identity_key = (
                table.auto_increment_column
                if default_primary_key_strategy(table) is PrimaryKeyStrategy.IDENTITY_WITH_CACHE
                else None
            )
            columns = [
                column.name
                for column in table.columns
                if _is_tinyint_one(column.mysql_type) and column.name != identity_key
            ]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) are MySQL TINYINT(1) (the BOOL "
                            "convention), mapped to Aurora DSQL boolean; a value "
                            "outside 0/1 has no boolean representation and fails "
                            "the load."
                        ),
                        recommendation=(
                            "Confirm every value is 0 or 1, or migrate the column "
                            "as a small integer instead of boolean."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class BitTypeRule(Rule):
    """Flag BIT(n) columns (no DSQL bit type; mapped to a sized integer)."""

    rule_id = "BIT_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) == "bit"
            ]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) use MySQL BIT, which Aurora DSQL "
                            "does not support; the value is mapped to the smallest "
                            "integer that holds the bits, so bit-string semantics "
                            "and operators are not preserved."
                        ),
                        recommendation=(
                            "Adjust application code that relied on bit-string "
                            "behavior to work with the integer value."
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class YearTypeRule(Rule):
    """Flag YEAR columns (no DSQL YEAR type; mapped to a smallint year)."""

    rule_id = "YEAR_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) == "year"
            ]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) use MySQL YEAR, which Aurora DSQL "
                            "does not support; the value is mapped to a smallint "
                            "integer year, so YEAR's display formatting and type "
                            "semantics are not preserved."
                        ),
                        recommendation=(
                            "Treat the column as an integer year in the "
                            "application."
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class ConversionWarningRule(Rule):
    """A Step 1 finding that IS a Schema Conversion warning: the converter's own helper
    decides and words it, so the two steps cannot grade or describe the table differently
    (the reverse direction the Evaluation/conversion parity test checks). ``warning`` names
    the converter helper; it is resolved lazily because the converter imports this module."""

    def __init__(
        self, rule_id: str, warning: str, recommendation: str, effort: EffortLevel
    ) -> None:
        self.rule_id = rule_id
        self._warning = warning
        self._recommendation = recommendation
        self._effort = effort

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core import converter

        helper = getattr(converter, self._warning)
        findings: list[Finding] = []
        for table in inventory.tables:
            warning = helper(table)
            if warning is None:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=warning.classification,
                    risk=warning.message,
                    recommendation=self._recommendation,
                    effort=self._effort,
                    # Advice where the conversion says so (a harmless index method change),
                    # a gap otherwise: graded the way the conversion files it.
                    note_kind=warning.kind,
                )
            )
        return findings


def expression_index_rule() -> ConversionWarningRule:
    """Expression indexes (MySQL functional, PostgreSQL expression), which the conversion
    does not carry (EXPRESSION_INDEX)."""
    return ConversionWarningRule(
        "EXPRESSION_INDEX",
        "_expression_index_warning",
        "Recreate the index on Aurora DSQL after the load as an expression index "
        "(CREATE INDEX ASYNC ... ((expression))), which Aurora DSQL supports.",
        EffortLevel.SIMPLE,
    )


def pg_index_not_preserved_rule() -> ConversionWarningRule:
    """PostgreSQL non-btree indexes, and partial ones whose predicate reads a re-typed
    column (PG_INDEX_NOT_PRESERVED)."""
    return ConversionWarningRule(
        "PG_INDEX_NOT_PRESERVED",
        "_pg_partial_or_nonbtree_index_warning",
        "After apply, re-create a partial index the conversion could not carry with a "
        "predicate over the column's new type; for a non-btree index, move that search "
        "outside the database or redesign the query.",
        EffortLevel.MEDIUM,
    )


_DEFAULT_DROPPED_ADVICE = (
    "Before cut over, have the application supply these values, or add an equivalent "
    "default with ALTER TABLE ... ALTER COLUMN ... SET DEFAULT after apply; a NOT NULL "
    "column without its default rejects every INSERT that omits it."
)


def default_dropped_rule(*, is_postgres: bool = False) -> ConversionWarningRule:
    """Column DEFAULTs Schema Conversion cannot carry (DEFAULT_DROPPED): a MySQL zero date
    or an expression with no equivalent, a PostgreSQL default calling a function the target
    lacks or casting to a type it lacks -- the converter's own decision and words."""
    return ConversionWarningRule(
        "DEFAULT_DROPPED",
        "_pg_dropped_default_warning" if is_postgres else "_mysql_dropped_default_warning",
        _DEFAULT_DROPPED_ADVICE,
        EffortLevel.SIMPLE,
    )


def pg_unindexable_column_rule() -> ConversionWarningRule:
    """Keys and indexes over PostgreSQL json/jsonb/interval/timetz columns, which Aurora
    DSQL cannot index (PG_UNINDEXABLE_COLUMN)."""
    return ConversionWarningRule(
        "PG_UNINDEXABLE_COLUMN",
        "_pg_unindexable_column_warning",
        "Give a table keyed on such a column a key of indexable columns before applying; "
        "for an index, index the looked-up value instead with an expression index after "
        "the load, and enforce a UNIQUE one's rule in the application meanwhile.",
        EffortLevel.MEDIUM,
    )


def pg_exclusion_constraint_rule() -> ConversionWarningRule:
    """PostgreSQL EXCLUDE constraints, which Aurora DSQL rejects, so the rule is not
    enforced after cut over (PG_EXCLUSION_CONSTRAINT, G151)."""
    return ConversionWarningRule(
        "PG_EXCLUSION_CONSTRAINT",
        "_pg_exclusion_constraint_warning",
        "Before cut over, enforce each rule above in the application -- the transaction "
        "that writes a row checks for a conflicting one, and concurrent writers of the same "
        "scope also update one shared row, so Aurora DSQL's optimistic concurrency fails one "
        "of them (40001) instead of letting both pass the check -- or change the design so "
        "the rule becomes a unique key.",
        EffortLevel.MEDIUM,
    )


def pg_temporal_key_rule() -> ConversionWarningRule:
    """PostgreSQL 18 keys declared WITHOUT OVERLAPS, created as ordinary keys, so their
    no-overlap rule is not enforced after cut over (PG_TEMPORAL_KEY, G151)."""
    return ConversionWarningRule(
        "PG_TEMPORAL_KEY",
        "_pg_temporal_key_warning",
        "Before cut over, enforce the no-overlap rule in the application -- the transaction "
        "that writes a period checks for an overlapping one of the same key, and concurrent "
        "writers of that key also update one shared row, so Aurora DSQL's optimistic "
        "concurrency fails one of them (40001) -- or change the design.",
        EffortLevel.MEDIUM,
    )


def pg_deferrable_key_rule() -> ConversionWarningRule:
    """PostgreSQL PRIMARY KEY / UNIQUE constraints declared DEFERRABLE, created as ordinary
    keys that Aurora DSQL checks immediately (PG_DEFERRABLE_KEY, G152)."""
    return ConversionWarningRule(
        "PG_DEFERRABLE_KEY",
        "_pg_deferrable_key_warning",
        "Before cut over, find the writes that rely on the deferred check -- a transaction "
        "or a multi-row UPDATE that swaps or renumbers key values -- and change them so no "
        "two rows hold the same key value at any point (move one row to a free value first), "
        "or change the design.",
        EffortLevel.SIMPLE,
    )


def pg_not_enforced_foreign_key_rule() -> ConversionWarningRule:
    """PostgreSQL 18 foreign keys declared NOT ENFORCED, which Schema Conversion does not
    create (PG_FK_NOT_ENFORCED, G152): advice, the target does not check them either."""
    return ConversionWarningRule(
        "PG_FK_NOT_ENFORCED",
        "_pg_not_enforced_foreign_key_warning",
        "No action needed: the target behaves as the source does. To have Aurora DSQL "
        "enforce such a key after all, add its ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY "
        "... NOT VALID to the table's target script in Schema Conversion: it is then added "
        "and validated with the other foreign keys, and its rows must satisfy it.",
        EffortLevel.SIMPLE,
    )


def pg_period_foreign_key_rule() -> ConversionWarningRule:
    """PostgreSQL 18 foreign keys declared with PERIOD, which Schema Conversion does not
    create, so their rule is not enforced after cut over (PG_FK_PERIOD, G163)."""
    return ConversionWarningRule(
        "PG_FK_PERIOD",
        "_pg_period_foreign_key_warning",
        "Before cut over, enforce the rule in the application -- the transaction that writes "
        "a child row checks that the referenced key's periods cover its period, one that "
        "shortens or deletes a parent period checks the child rows, and concurrent writers "
        "of the same key also update one shared row, so Aurora DSQL's optimistic "
        "concurrency fails one of them (40001) -- or change the design.",
        EffortLevel.MEDIUM,
    )


def pg_collation_rule() -> ConversionWarningRule:
    """PostgreSQL non-default column collations, which the target does not keep
    (PG_COLLATION_DROPPED)."""
    return ConversionWarningRule(
        "PG_COLLATION_DROPPED",
        "_pg_collation_warning",
        "Check the comparisons, sorting and unique keys on these columns under Aurora "
        "DSQL's default collation before cut over; where they must ignore case or accents, "
        "have the application store a normalised key column and match on that.",
        EffortLevel.MEDIUM,
    )


class TimeTypeRule(Rule):
    """Flag MySQL TIME columns: a duration type, where Aurora DSQL's time is a time of day.

    Schema Conversion graded TIME MANUAL while Evaluation said nothing, so a table of TIME
    durations read Ready at Step 1 and then failed row by row. The text is the conversion
    note's, plus what CDC does with an out-of-range value since sink plugin v54."""

    rule_id = "TIME_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import MYSQL_TIME_NOTE

        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column.name for column in table.columns if _base_type(column.mysql_type) == "time"
            ]
            # A key or index over the column refuses an interval (no index support on
            # Aurora DSQL), so the remap advice has to name text for those.
            keyed = [
                name
                for name in columns
                if name in table.primary_key
                or any(name in index.columns for index in table.indexes)
            ]
            if columns:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({', '.join(columns)}): {MYSQL_TIME_NOTE} If you "
                            "replicate with CDC, a change carrying such a value is "
                            "dead-lettered with that reason for a time column; with the "
                            "column remapped to interval, CDC writes it as an interval."
                        ),
                        recommendation=(
                            "Only time-of-day values: no action. Durations: remap the "
                            "column to interval in Schema Conversion before loading"
                            + (
                                f" -- to text for {', '.join(keyed)}, which "
                                f"{'an index or the primary key uses' if len(keyed) == 1 else 'indexes or the primary key use'}"
                                ": Aurora DSQL cannot index an interval."
                                if keyed
                                else "."
                            )
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class GeneratedColumnRule(Rule):
    """Flag MySQL generated/computed columns for review."""

    rule_id = "GENERATED_COLUMN"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import (
            mysql_generated_column_advice,
            mysql_generated_column_detail,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            # The conversion note's own text, so Step 1 and Step 2 agree (it used to claim
            # CDC never carries the value, and advised a GENERATED target column that makes
            # every CDC insert and update to the table dead-letter).
            detail = mysql_generated_column_detail(table)
            if detail is not None:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=detail,
                        recommendation=mysql_generated_column_advice(table),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class AutoUpdateTimestampRule(Rule):
    """Flag columns using MySQL ON UPDATE CURRENT_TIMESTAMP."""

    rule_id = "ON_UPDATE_TIMESTAMP"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [c.name for c in table.columns if c.auto_update_timestamp]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) use MySQL ON UPDATE "
                            "CURRENT_TIMESTAMP (auto-updated on every row change); "
                            "Aurora DSQL does not apply this automatically."
                        ),
                        recommendation=(
                            "Set the timestamp explicitly in the application on "
                            "each update (DSQL has no ON UPDATE clause or "
                            "triggers)."
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class UnsupportedIndexTypeRule(Rule):
    """Flag FULLTEXT/SPATIAL indexes (no Aurora DSQL equivalent).

    MANUAL/LOSS, not UNSUPPORTED: the table and its data migrate; only the search the index
    served is gone (the conversion leaves the index out). The risk is the converter's own
    note (:func:`converter._unsupported_index_type_warning`), so both steps say the same."""

    rule_id = "UNSUPPORTED_INDEX_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import _unsupported_index_type_warning

        findings: list[Finding] = []
        for table in inventory.tables:
            warning = _unsupported_index_type_warning(table)
            if warning is None:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=warning.classification,
                    risk=warning.message,
                    recommendation=(
                        "Implement full-text/spatial search outside the database (e.g. "
                        "Amazon OpenSearch Service), or redesign the query, before cut over."
                    ),
                    effort=EffortLevel.SIGNIFICANT,
                )
            )
        return findings


class ViewDefinitionUnreadableRule(Rule):
    """A view whose definition the source did not return, so it cannot be converted.

    Introspection reads a view's definition best-effort and keeps an EMPTY one when the read
    fails -- on MySQL the usual cause is a migration user without SHOW VIEW -- so one bad
    view does not abort the inventory. Graded AUTO, it read as "Ready" while Schema
    Conversion (which needs the definition) could only flag it for manual work."""

    rule_id = "VIEW_DEFINITION_UNREADABLE"

    def __init__(self, *, is_postgres: bool = False) -> None:
        self._is_postgres = is_postgres

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        causes = (
            "the read of its definition failed (a view dropped or replaced while the "
            "catalog was read is reported separately, as a change during the read)"
            if self._is_postgres
            else "the migration user may lack the SHOW VIEW privilege on it, or the view "
            "may be invalid (a table it reads was dropped)"
        )
        fix = (
            "Repair the view on the source"
            if self._is_postgres
            else "Grant SHOW VIEW on its database to the migration user (a read-only "
            "privilege), or repair the view on the source"
        )
        findings: list[Finding] = []
        for view in inventory.views:
            if view.unsupported_kind or (view.definition or "").strip():
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_VIEW, view.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        "The source returned no definition for this view, so it cannot be "
                        f"converted: {causes}."
                    ),
                    recommendation=(
                        f"{fix}, then re-run Step 1 (Evaluation); otherwise recreate the "
                        "view on Aurora DSQL by hand."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class MySqlViewConversionRule(Rule):
    """Grade a MySQL view by what Schema Conversion does with it: a body it cannot convert
    (the conversion leaves a placeholder), or one calling a function it leaves untranslated
    (FIND_IN_SET, UNIX_TIMESTAMP, a user function) and that therefore fails its CREATE VIEW.
    The converter's own notes, so the two steps agree -- Evaluation used to read such a
    view AUTO beside Schema Conversion's MANUAL."""

    rule_id = "VIEW_NOT_CONVERTED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.converter import SchemaConverter
        from dsql_migrator.core.source_time_zone import resolve_source_zone

        # In the source's time zone, as Schema Conversion converts each view (G149): under
        # one that is not UTC its SYSDATE() is written, not left untranslated.
        converter = SchemaConverter(
            source_type=SourceType.MYSQL,
            source_zone=resolve_source_zone(inventory.server_settings, SourceType.MYSQL),
        )
        findings: list[Finding] = []
        # With the tables' column types, as Schema Conversion converts each view (a binary
        # column's bytes, a CHAR(n) column's padding): the two steps grade it alike.
        from dsql_migrator.core.converter import table_type_index

        table_index = table_type_index(inventory.tables)
        for view in inventory.views:
            if getattr(view, "unsupported_kind", None) or not (view.definition or "").strip():
                continue  # graded by VIEW_DEFINITION_UNREADABLE and the kind rules
            converted = converter.convert_view(
                view, tables=inventory.tables, table_index=table_index
            )
            for warning in converted.warnings:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_VIEW, view.name),
                        rule_id=self.rule_id,
                        classification=warning.classification,
                        risk=warning.message,
                        recommendation=(
                            "Rewrite the view's target DDL in Schema Conversion before "
                            "applying, or re-create the view by hand after the load."
                        ),
                        effort=EffortLevel.MEDIUM,
                        note_kind=warning.kind,
                    )
                )
        return findings


class ViewCompatibilityRule(Rule):
    """Flag views whose definition uses DSQL-unsupported/risky SQL constructs.

    Reuses the application anti-pattern linter (unsupported MySQL functions,
    ``FOR UPDATE``, trigger/routine calls) on each view's SELECT body; a view
    that matches any anti-pattern is flagged ``MANUAL`` for review. Views with a
    clean (or empty) definition stay ``AUTO``.
    """

    rule_id = "VIEW_UNSUPPORTED_SQL"

    def __init__(self, linter: object = None) -> None:
        """Create the rule, optionally injecting a linter (for tests)."""
        if linter is None:
            from dsql_migrator.core.linter import AppLinter

            linter = AppLinter()
        self._linter = linter

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        from dsql_migrator.core.linter import AppSource, SourceFile

        findings: list[Finding] = []
        for view in inventory.views:
            definition = (view.definition or "").strip()
            if not definition:
                continue
            source = AppSource(
                files=[SourceFile(path=view.name, content=definition)]
            )
            matches = self._linter.scan(source)  # type: ignore[attr-defined]
            # A function call is judged by the conversion instead (VIEW_NOT_CONVERTED):
            # GROUP_CONCAT, DATE_FORMAT, STR_TO_DATE and FROM_UNIXTIME are translated, and
            # the name list missed every function that is not (FIND_IN_SET, a user one).
            matches = [
                match for match in matches
                if getattr(getattr(match, "pattern", None), "value", None) != "UNSUPPORTED_FUNCTION"
            ]
            if not matches:
                continue
            # The linter already returns the matched TEXT, its line, and a per-match
            # recommendation. Reporting ``match.pattern.value`` instead handed the
            # operator internal enum tokens (PESSIMISTIC_LOCK, UNSUPPORTED_FUNCTION) that
            # appear nowhere in their view and cannot be searched for, and the
            # recommendation sent them to an "application anti-pattern report" that NO
            # surface of this tool produces -- not a page, not an export, not a CLI
            # command (the linter's only caller is this rule). So the one artifact holding
            # the locations was the thing being pointed at, and it does not exist.
            located = ", ".join(
                f"{match.matched_text} (line {match.line})"
                for match in sorted(matches, key=lambda m: (m.line, m.column))[
                    :_MAX_VIEW_MATCHES_SHOWN
                ]
            )
            extra = len(matches) - _MAX_VIEW_MATCHES_SHOWN
            actions = list(dict.fromkeys(match.recommendation for match in matches))
            findings.append(
                Finding(
                    object=ObjectKey(KIND_VIEW, view.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        "The view definition uses constructs Aurora DSQL does not "
                        f"support or that are risky on it: {located}"
                        + (f", and {extra} more" if extra > 0 else "")
                        + "."
                    ),
                    recommendation=(
                        "Rewrite the view to avoid them. "
                        + " ".join(actions[:_MAX_VIEW_MATCHES_SHOWN])
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


# ---------------------------------------------------------------------------
# Inventory-level (cluster/database-wide) checks -- not tied to one object
# ---------------------------------------------------------------------------

# An inventory rule classifies the source as a whole (e.g. spanning multiple
# databases) rather than a single object; it returns ready-made items. It also receives the
# browsed TARGET catalog when the caller has one (Evaluation browses it before assessing),
# because a database-level limit is about what the target database will hold AFTER the
# migration -- the source's objects plus what the cluster already has -- and ``None``
# otherwise (then only the source is counted).
InventoryRule = Callable[
    [SourceInventory, Optional[TargetInventory]], list[AssessmentItem]
]


def _source_databases(inventory: SourceInventory) -> list[str]:
    """Return the distinct source database prefixes from qualified object names.

    Objects are qualified as ``database.object``; the part before the first dot
    is the database. Unqualified names contribute no database. Order preserved.
    """
    databases: list[str] = []
    names = [table.name for table in inventory.tables]
    names += [view.name for view in inventory.views]
    for name in names:
        if "." in name:
            database = name.split(".", 1)[0]
            if database and database not in databases:
                databases.append(database)
    return databases


# Inventory-level checks build their AssessmentItem directly rather than going through
# _aggregate, so they must populate ``concerns`` themselves. Leaving it empty made the UI
# fall back to its pre-concerns rendering: a cluster-level finding showed bare "Risk" /
# "Recommendation" paragraphs while every table beside it used the labeled card treatment,
# so one row in the list looked like a different application. One finding is still a
# finding -- the list should not change shape because there is only one.
def _single_concern(
    item: AssessmentItem, note_kind: ConversionNoteKind = ConversionNoteKind.LOSS
) -> list[AssessmentConcern]:
    """Wrap an item's own risk/recommendation as its single concern."""
    return [
        AssessmentConcern(
            rule_id=item.rule_id,
            classification=item.classification,
            risk=item.risk,
            recommendation=item.recommendation,
            effort=item.effort,
            note_kind=note_kind,
        )
    ]


def _schemas_needed(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory]
) -> tuple[list[str], list[str]]:
    """``(source schemas, other schemas already on the target)``, in first-seen order.

    Every source database (MySQL) or schema (PostgreSQL) becomes a target schema of the
    same name -- the part before the first dot of the qualified object name -- and an
    unqualified name lands in ``public``, which is never counted (see
    :data:`_BUILTIN_TARGET_SCHEMA`). Names are compared as Aurora DSQL stores them, cut to
    63 bytes: a long database name is not counted again once its schema exists, and two
    databases whose names agree in their first 63 bytes are one target schema. The browse
    lists empty schemas too (they count toward the limit).
    """
    from dsql_migrator.core.target_introspector import pg_stored_name

    source = list(
        dict.fromkeys(
            pg_stored_name(name)
            for name in _source_databases(inventory)
            if name != _BUILTIN_TARGET_SCHEMA
        )
    )
    wanted = set(source)
    existing = [
        schema.name
        for schema in (target_inventory.schemas if target_inventory is not None else [])
        if schema.name != _BUILTIN_TARGET_SCHEMA and schema.name not in wanted
    ]
    return source, existing


def _relations_after_migration(
    names: list[str],
    target_inventory: Optional[TargetInventory],
    *,
    views: bool,
) -> tuple[int, int, int]:
    """``(total, new from the source, already on the target)`` tables or views afterwards.

    A source relation that is already on the target is skipped or replaced, never added
    twice, so it adds nothing; matching is the Evaluation name-conflict rule
    (:func:`~dsql_migrator.core.target_introspector.relations_existing_on_target`).
    """
    if target_inventory is None:
        return len(names), len(names), 0
    from dsql_migrator.core.target_introspector import relations_existing_on_target

    on_target = sum(
        len(schema.views if views else schema.tables)
        for schema in target_inventory.schemas
    )
    new = len(names) - len(
        relations_existing_on_target(names, target_inventory, views=names if views else ())
    )
    return on_target + new, new, on_target


def _ceil_div(value: int, limit: int) -> int:
    return -(-value // limit)


SOURCE_VISIBILITY_RULE_ID = "SOURCE_VISIBILITY"


CATALOG_CHANGED_RULE_ID = "CATALOG_CHANGED_DURING_READ"


def catalog_changed_items(names: "Sequence[str]") -> list[AssessmentItem]:
    """The database-level item for tables and views the read listed but found gone, twice
    (dropped at that moment, or replaced by a statement that was still running): they are
    in neither this report nor the migration, which a silent skip used to hide."""
    if not names:
        return []
    shown = ", ".join(list(names)[:10]) + (f" and {len(names) - 10} more" if len(names) > 10 else "")
    item = AssessmentItem(
        object_name="source catalog",
        rule_id=CATALOG_CHANGED_RULE_ID,
        classification=Classification.MANUAL,
        risk=(
            f"{len(names)} table(s) or view(s) disappeared while the catalog was read, and "
            f"were still gone when looked for again: {shown}. They were dropped at that "
            "moment, or are being replaced (a DROP + CREATE ... AS still running, a rename "
            "swap), so they are not in this report or the migration."
        ),
        recommendation=(
            "If they should migrate, re-run Step 1 (Evaluation) once whatever replaces them "
            "has finished."
        ),
        effort=EffortLevel.SIMPLE,
        kind=KIND_DATABASE.upper(),
    )
    return [item.model_copy(update={"concerns": _single_concern(item)})]


def source_visibility_items(gaps: "Sequence[str]") -> list[AssessmentItem]:
    """The database-level item for objects the source user cannot list (``gaps``, from the
    source dialect's listing probe): what is not listed is not assessed, converted or
    migrated, so a report built without it read "Ready" over objects it never saw."""
    if not gaps:
        return []
    item = AssessmentItem(
        object_name="source privileges",
        rule_id=SOURCE_VISIBILITY_RULE_ID,
        classification=Classification.MANUAL,
        risk=(
            "The source user cannot list every object, so this report may be missing some: "
            + "; ".join(gaps)
            + ". Objects that are not listed are not assessed, converted or migrated."
        ),
        recommendation=(
            "Grant the missing privileges to the migration user and re-run Step 1 "
            "(Evaluation), or list those objects as an administrator (SHOW TRIGGERS, SHOW "
            "EVENTS, SHOW PROCEDURE STATUS / SHOW FUNCTION STATUS) and review them by hand. "
            "SHOW_ROUTINE only reads; TRIGGER and EVENT also allow changing triggers and "
            "events, which is why listing them by hand may be preferable."
        ),
        effort=EffortLevel.SIMPLE,
        kind=KIND_DATABASE.upper(),
    )
    return [item.model_copy(update={"concerns": _single_concern(item, ConversionNoteKind.LOSS)})]


def source_visibility_not_checked_items() -> list[AssessmentItem]:
    """The database-level item for a report refreshed from ANOTHER source than its run
    read: that run's listing-privilege probe described its own source, and only an
    Evaluation run probes this one."""
    item = AssessmentItem(
        object_name="source privileges",
        rule_id=SOURCE_VISIBILITY_RULE_ID,
        classification=Classification.MANUAL,
        risk=(
            "Whether the source user can list every object was not checked for this "
            "source: the report was refreshed from a catalog read of another source than "
            "the Evaluation run probed. Objects the user cannot list are not assessed, "
            "converted or migrated."
        ),
        recommendation="Re-run Step 1 (Evaluation) to probe this source's privileges.",
        effort=EffortLevel.SIMPLE,
        kind=KIND_DATABASE.upper(),
    )
    return [item.model_copy(update={"concerns": _single_concern(item, ConversionNoteKind.LOSS)})]


# Findings from the PostgreSQL catalog probe (core.catalog_postgres; graded in
# core.assessor_postgres): row-level security on a table, a sequence no column owns, and the
# probe itself failing.
PG_ROW_LEVEL_SECURITY_RULE_ID = "PG_ROW_LEVEL_SECURITY"
PG_STANDALONE_SEQUENCE_RULE_ID = "PG_STANDALONE_SEQUENCE"
PG_CATALOG_UNREAD_RULE_ID = "PG_CATALOG_UNREAD"
# The note that the run's probe read the catalog at all: without it a report from before the
# probe (restored or refreshed) and a probe that found nothing looked the same.
PG_CATALOG_CHECKED_RULE_ID = "PG_CATALOG_CHECKED"

# Findings that come from probing the SOURCE at Evaluation time rather than from the
# inventory, so a re-assessment from the inventory alone (a catalog refresh, a restored
# session) cannot recompute them and has to carry them over (:func:`carry_probe_results`).
# Two shapes: an item of its own (the privilege gap, a sequence, an unread probe), and a
# concern on an inventory object's item (row-level security on a table).
_PROBE_ITEM_RULE_IDS = frozenset(
    {
        SOURCE_VISIBILITY_RULE_ID,
        PG_STANDALONE_SEQUENCE_RULE_ID,
        PG_CATALOG_UNREAD_RULE_ID,
        PG_CATALOG_CHECKED_RULE_ID,
    }
)
_PROBE_CONCERN_RULE_IDS = frozenset({PG_ROW_LEVEL_SECURITY_RULE_ID})


def _findings_of(item: AssessmentItem) -> list[Finding]:
    """The findings ``item`` was aggregated from, so it can be graded again with more. An
    item built without concerns (a caller-supplied assessor) keeps its own headline."""
    key = ObjectKey(item.kind.lower(), item.object_name)
    if item.concerns:
        return [
            Finding(
                object=key,
                rule_id=concern.rule_id,
                classification=concern.classification,
                risk=concern.risk,
                recommendation=concern.recommendation,
                effort=concern.effort,
                note_kind=concern.note_kind,
            )
            for concern in item.concerns
        ]
    if item.rule_id == "COMPATIBLE":
        return []
    return [
        Finding(
            object=key,
            rule_id=item.rule_id,
            classification=item.classification,
            risk=item.risk,
            recommendation=item.recommendation,
            effort=item.effort,
        )
    ]


def with_object_findings(
    report: AssessmentReport, findings: "Sequence[Finding]"
) -> AssessmentReport:
    """``report`` with ``findings`` added to the items of the objects they are about.

    Each affected item is re-aggregated exactly as :class:`CompatibilityAssessor` grades an
    object -- its own findings plus the new ones, the most severe gap governing -- so a
    finding read outside the inventory (row-level security on a table) grades the TABLE,
    instead of becoming a second, unrelated row for it. A finding replaces an existing
    concern of the same rule (re-applying is idempotent); one about an object the report
    does not hold is dropped, since there is no object to grade. Pure.
    """
    added: dict[tuple[str, str], list[Finding]] = defaultdict(list)
    for finding in findings:
        added[(finding.object.kind.upper(), finding.object.name)].append(finding)
    if not added:
        return report
    items: list[AssessmentItem] = []
    for item in report.items:
        extra = added.get((item.kind, item.object_name))
        if not extra:
            items.append(item)
            continue
        replaced = {finding.rule_id for finding in extra}
        own = [f for f in _findings_of(item) if f.rule_id not in replaced]
        items.append(_aggregate(ObjectKey(item.kind.lower(), item.object_name), own + extra))
    return AssessmentReport.from_items(items)


def carry_probe_results(
    previous: AssessmentReport, recomputed: AssessmentReport
) -> AssessmentReport:
    """``recomputed`` (assessed from the inventory alone) plus what ``previous`` held from
    probing the source: the probe's own items, and its concerns re-attached to the objects
    ``recomputed`` still holds (a table no longer in the inventory loses its concern with
    it). Re-run Step 1 to probe again. Pure."""
    whole = [item for item in previous.items if item.rule_id in _PROBE_ITEM_RULE_IDS]
    attached = [
        finding
        for item in previous.items
        if item.rule_id not in _PROBE_ITEM_RULE_IDS
        for finding in _findings_of(item)
        if finding.rule_id in _PROBE_CONCERN_RULE_IDS
    ]
    report = with_object_findings(recomputed, attached)
    if whole:
        report = AssessmentReport.from_items([*report.items, *whole])
    return report


def check_multiple_source_databases(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Note that a multi-database MySQL source lands as one schema per database.

    Advisory, not a gap. Aurora DSQL has one database per cluster, and Schema Conversion
    already creates each source database as its own schema in it (``db.table`` becomes
    ``"db"."table"``), so nothing has to be consolidated by hand. This used to be a
    MANUAL/MEDIUM finding telling the operator to "consolidate the databases into one
    cluster using separate schemas" -- the conversion's own work, and impossible past
    Aurora DSQL's 10-schema limit. A migration over that limit gets
    :func:`check_schema_count`'s blocker instead of this note.
    """
    databases = _source_databases(inventory)
    if len(databases) <= 1:
        return []
    source, existing = _schemas_needed(inventory, target_inventory)
    needed = len(source) + len(existing)
    if needed > _MAX_SCHEMAS_PER_DATABASE:
        return []
    names = ", ".join(databases)
    # Unless two database names agree in their first 63 bytes: those are ONE schema (and
    # their tables' clashes are reported as NAME_COLLISION).
    apart = len(source) == len(databases)
    items = [
        AssessmentItem(
            object_name=f"{len(databases)} source databases",
            rule_id="MULTIPLE_DATABASES",
            classification=Classification.AUTO,
            risk=(
                f"The source spans {len(databases)} databases ({names}). Aurora DSQL has "
                "one database per cluster, so Schema Conversion creates each of them as "
                "its own schema in it (db.table becomes schema.table): nothing has to be "
                "consolidated by hand"
                + (", and names cannot collide across databases." if apart else ".")
            ),
            recommendation=(
                "Point the application at the schema-qualified names, or set its "
                "search_path to the schema, as for any database that moves into an "
                f"Aurora DSQL schema. Aurora DSQL allows {_MAX_SCHEMAS_PER_DATABASE} "
                f"schemas per database; this migration needs {needed}, counting the "
                "cluster's existing schemas."
            ),
            effort=None,
            kind=KIND_DATABASE.upper(),
        )
    ]
    return [
        item.model_copy(
            update={
                "concerns": _single_concern(item, ConversionNoteKind.RECOMMENDATION)
            }
        )
        for item in items
    ]


def check_table_count(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Flag a migration that would put more tables in one DSQL database than it allows.

    The limit is on the DATABASE, so what counts is what the target will hold afterwards:
    the source's tables plus the tables already on the cluster, less the source tables
    that are already there. Without a target catalog only the source is counted.
    """
    names = [table.name for table in inventory.tables]
    total, new, on_target = _relations_after_migration(
        names, target_inventory, views=False
    )
    if total <= _MAX_TABLES_PER_DATABASE:
        return []
    limit = _MAX_TABLES_PER_DATABASE
    failure = (
        f"Schema Conversion's apply would fail part-way (\"creating more than {limit} "
        "tables not allowed\"), after the first tables were created"
    )
    if on_target:
        risk = (
            f"After this migration the target database would hold {total:,} tables "
            f"({new:,} new from the source plus {on_target:,} already on the cluster), but "
            f"Aurora DSQL allows at most {limit:,} tables per database (a hard limit). "
            f"{failure}."
        )
    else:
        risk = (
            f"The source has {total:,} tables, but Aurora DSQL allows at most {limit:,} "
            f"tables per database (a hard limit). {failure}."
        )
    # Advice follows the SOURCE's own size: a source that alone exceeds the limit needs
    # several clusters whatever this one holds (counting only its tables NOT yet on the
    # cluster understated that once some of them were already there).
    if len(names) > limit:
        recommendation = (
            f"Split the tables across at least {_ceil_div(len(names), limit)} Aurora DSQL "
            f"clusters (at most {limit:,} tables each) before migrating."
        )
        if on_target:
            recommendation += " Tables already on this cluster count toward its limit too."
    else:
        recommendation = (
            f"The source's {len(names):,} tables fit on their own, but the tables already "
            "on this cluster push it past the limit: migrate into a cluster with room, or "
            "remove the tables you no longer need from this one."
        )
    items = [
        AssessmentItem(
            object_name=f"{total:,} tables",
            rule_id="TABLE_COUNT_LIMIT",
            classification=Classification.UNSUPPORTED,
            risk=risk,
            recommendation=recommendation,
            effort=EffortLevel.SIGNIFICANT,
            kind=KIND_DATABASE.upper(),
        )
    ]
    return [item.model_copy(update={"concerns": _single_concern(item)}) for item in items]


def check_schema_count(
    inventory: SourceInventory,
    target_inventory: Optional[TargetInventory] = None,
    *,
    is_postgres: bool = False,
) -> list[AssessmentItem]:
    """Flag a migration that needs more schemas than one Aurora DSQL database allows.

    Each source database (MySQL) or schema (PostgreSQL) becomes a target schema, and Aurora
    DSQL allows at most 10 per database: a hard limit (54000 "more than 10 schemas not
    allowed") that previously surfaced only part-way through Schema Conversion's apply,
    after the first schemas' tables had been created. Schemas already on the cluster count
    too; ``public`` does not (see :data:`_BUILTIN_TARGET_SCHEMA`). ``is_postgres`` selects
    the engine's wording (and :func:`check_pg_schema_count` is the PostgreSQL entry).
    """
    source, existing = _schemas_needed(inventory, target_inventory)
    total = len(source) + len(existing)
    limit = _MAX_SCHEMAS_PER_DATABASE
    if total <= limit:
        return []
    unit = "schema" if is_postgres else "database"
    on_cluster = (
        f" plus {len(existing)} already on the cluster ({', '.join(existing)})"
        if existing
        else ""
    )
    risk = (
        f"This migration needs {total} schemas in the target database ({len(source)} "
        f"from the source, one per source {unit}{on_cluster}), but Aurora DSQL allows at "
        f"most {limit} schemas per database (a hard limit). Schema Conversion's apply "
        "would create the first ones and then fail with \"more than 10 schemas not "
        "allowed\", leaving a partially created target."
    )
    if len(source) > limit:
        clusters = _ceil_div(len(source), limit)
        if is_postgres:
            recommendation = (
                f"Consolidate the schemas on the source to at most {limit}, or plan for "
                f"at least {clusters} Aurora DSQL clusters (at most {limit} schemas each). "
                "This tool reads every schema of the connected database together, so a "
                "split is done by selection: for each cluster, generate and apply only that "
                "group's objects in Schema Conversion and select the same tables in Data "
                "Migration (this verdict keeps counting the whole database)."
            )
        else:
            recommendation = (
                f"Plan for at least {clusters} Aurora DSQL clusters (at most {limit} "
                "schemas each): set Database on the Connect step to migrate one database "
                f"per run, and put at most {limit} databases into each cluster."
            )
        if existing:
            recommendation += " Schemas already on this cluster count toward its limit too."
    else:
        recommendation = (
            f"The source's {len(source)} schemas fit on their own, but the schemas "
            "already on this cluster push it past the limit: migrate into a cluster with "
            "room, or remove the schemas you no longer need from this one."
        )
    items = [
        AssessmentItem(
            object_name=f"{total} schemas",
            rule_id="SCHEMA_COUNT_LIMIT",
            classification=Classification.UNSUPPORTED,
            risk=risk,
            recommendation=recommendation,
            effort=EffortLevel.SIGNIFICANT,
            kind=KIND_DATABASE.upper(),
        )
    ]
    return [item.model_copy(update={"concerns": _single_concern(item)}) for item in items]


def check_pg_schema_count(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """PostgreSQL-worded :func:`check_schema_count` (source schemas, not databases)."""
    return check_schema_count(inventory, target_inventory, is_postgres=True)


def check_view_count(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Flag a migration that would put more views in one DSQL database than it allows.

    Counted like :func:`check_table_count`: the source's views plus the views already on
    the cluster, less the source views that are already there.
    """
    names = [view.name for view in inventory.views]
    total, new, on_target = _relations_after_migration(names, target_inventory, views=True)
    limit = _MAX_VIEWS_PER_DATABASE
    if total <= limit:
        return []
    where = (
        f"({new:,} new from the source plus {on_target:,} already on the cluster)"
        if on_target
        else "from the source"
    )
    if len(names) > limit:
        recommendation = (
            f"Split the views across at least {_ceil_div(len(names), limit)} Aurora DSQL "
            "clusters, or re-create only the views the application still uses."
        )
    else:
        recommendation = (
            f"The source's {len(names):,} views fit on their own, but the views already on "
            "this cluster push it past the limit: migrate into a cluster with room, or "
            "remove the views you no longer need from this one."
        )
    items = [
        AssessmentItem(
            object_name=f"{total:,} views",
            rule_id="VIEW_COUNT_LIMIT",
            classification=Classification.UNSUPPORTED,
            risk=(
                f"This migration needs {total:,} views in the target database {where}, but "
                f"Aurora DSQL allows at most {limit:,} views per database (a hard limit), "
                f"so Schema Conversion's apply would fail part-way (\"creating more than "
                f"{limit} views not allowed\")."
            ),
            recommendation=recommendation,
            effort=EffortLevel.SIGNIFICANT,
            kind=KIND_DATABASE.upper(),
        )
    ]
    return [item.model_copy(update={"concerns": _single_concern(item)}) for item in items]


def check_postgres_extensions(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Report each installed PostgreSQL EXTENSION -- Aurora DSQL provides none.

    This is the pairing for filtering extension-owned objects out of the inventory. That
    filter is right (an extension's functions and tables are not the user's to re-create,
    and one ``CREATE EXTENSION pgcrypto`` otherwise added 36 UNSUPPORTED findings advising
    the operator to reimplement C functions they did not write), but on its own it would
    hide a REAL incompatibility: DSQL ships no user extensions, so any application SQL
    calling ``crypt()``, ``ST_Contains()`` or ``uuid_generate_v4()`` has to change.

    One item per extension keeps that signal in a line or two instead of hundreds of rows,
    and says the actionable thing -- which extension, and that the calls must be replaced.
    Inventory-level rather than per-object because the extension is a property of the
    database, not of any one object.
    """
    items = [
        AssessmentItem(
            object_name=name,
            rule_id="PG_EXTENSION_UNSUPPORTED",
            classification=Classification.MANUAL,
            risk=(
                f"The source has the PostgreSQL extension {name} installed. Aurora DSQL "
                "provides no user extensions, so its functions, operators and types do "
                "not exist on the target. The extension's own objects are excluded from "
                "this migration (they are not yours to re-create), but any application "
                "SQL that calls it will fail after cut over."
            ),
            recommendation=(
                "Find the queries that use this extension and replace them: move the "
                "logic to the application, or to a DSQL-supported equivalent (e.g. "
                "gen_random_uuid() is built in; hashing/encryption can move to the "
                "application or AWS KMS). Data stored in an extension-provided TYPE "
                "needs a supported column type instead."
            ),
            effort=EffortLevel.MEDIUM,
            kind=KIND_DATABASE.upper(),
        )
        for name in inventory.extensions
    ]
    return [item.model_copy(update={"concerns": _single_concern(item)}) for item in items]


def check_postgres_database_collation(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Report a source DATABASE collation that Aurora DSQL does not match.

    The per-COLUMN collation capture deliberately ignores the collation named ``default``,
    because that is not a collation -- it is "this database's default" -- and flagging every
    text column would be noise, and a false alarm when the source default already IS ``C``.
    But that left the ORDINARY case invisible: on a stock RDS/Aurora PostgreSQL the database
    collation is something like ``en_US.utf8`` and every text column inherits it, while
    Aurora DSQL runs ``C``. Measured on a live cluster: DSQL reports
    ``datcollate = 'C'``, so ``'Bob' < 'alice'`` is TRUE and ``ORDER BY`` yields
    ``A, B, a, b``; the same query on ``en_US.utf8`` gives FALSE and ``a, A, b, B``.

    So this is the DATABASE-level half of that capture, in one item rather than one per
    column -- the same pairing shape as :func:`check_postgres_extensions`. ``C`` and
    ``POSIX`` need no item: they are byte ordering, which is what the target does.

    Equality and UNIQUE are NOT affected by a deterministic collation change (only ordering
    and pattern/range behaviour), so the wording must not overstate it.
    """
    collation = (inventory.database_collation or "").strip()
    if not collation or collation.upper() in ("C", "POSIX"):
        return []
    items = [
        AssessmentItem(
            object_name=collation,
            rule_id="PG_DATABASE_COLLATION",
            classification=Classification.MANUAL,
            risk=(
                f"The source database's default collation is {collation}; Aurora DSQL runs "
                "C for BOTH collation and character classification (live-verified: "
                "datcollate = datctype = 'C'). Every text column that does not name its own "
                "collation inherits this, and two things change after cut over. Ordering: "
                "ORDER BY returns a DIFFERENT order and range predicates (BETWEEN, <, >) "
                "change on mixed-case or accented data. Case folding: lower()/upper()/"
                "initcap() fold ASCII ONLY, so lower('JOSÉ') is 'josÉ' on the target but "
                "'josé' on the source, and ILIKE / ~* stop matching case-insensitively on "
                "non-ASCII text -- that changes WHICH ROWS a query returns, not just their "
                "order. Every row still migrates exactly and every row count and checksum "
                "matches, so nothing else flags it. Equality and UNIQUE enforcement on the "
                "stored values are unchanged."
            ),
            recommendation=(
                "Review the queries whose RESULT ORDER matters (paged listings, "
                "'first/last by name', range filters on text), and separately any predicate "
                "using ILIKE, ~*, lower() or upper() on data that is not pure ASCII. Note "
                "what is NOT available on the target: Aurora DSQL accepts only the C / "
                "POSIX / default collations -- 'unicode' and 'ucs_basic' are listed in "
                "pg_collation but rejected in use ('provided collation is not supported') "
                "-- so a COLLATE clause cannot restore linguistic ordering, and there is no "
                "unaccent() either. Where the source behaviour must be preserved, have the "
                "APPLICATION store a normalised sort/match key alongside the text (lower- "
                "and accent-folded at write time) and order or filter on that column."
            ),
            effort=EffortLevel.MEDIUM,
            kind=KIND_DATABASE.upper(),
        )
    ]
    return [item.model_copy(update={"concerns": _single_concern(item)}) for item in items]


MIXED_CASE_RULE_ID = "MIXED_CASE_IDENTIFIERS"


def check_mixed_case_identifiers(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Note the MySQL names with upper-case letters, which unquoted SQL cannot reach on DSQL.

    Schema Conversion keeps every name exactly as the source spells it by quoting it, and
    Aurora DSQL, like PostgreSQL, folds an UNQUOTED name to lower case. MySQL matches column
    names case-insensitively (and table names too where lower_case_table_names is set), so
    application SQL that writes ``CustomerId`` or ``Orders`` unquoted worked there and finds
    nothing after cut over -- with no finding anywhere until then. Tables whose names differ
    only by case (possible where MySQL's table names are case-sensitive) cannot both be
    reached unquoted at all."""
    # Aurora DSQL (a UTF8 database) folds only ASCII A-Z in an unquoted name, so a name
    # whose only capitals are non-ASCII (Ürün, Сумма) is reached unquoted as spelled.
    def upper(name: str) -> bool:
        return any("A" <= ch <= "Z" for ch in name)

    def folded(name: str) -> str:
        return "".join(ch.lower() if "A" <= ch <= "Z" else ch for ch in name)

    schemas: set[str] = set()
    relations: list[str] = []
    columns: list[str] = []
    by_folded: dict[tuple[str, str], list[str]] = {}
    for relation in (*inventory.tables, *inventory.views):
        schema, _, bare = relation.name.partition(".")
        if not bare:
            schema, bare = "", schema
        if schema and upper(schema):
            schemas.add(schema)
        if upper(bare):
            relations.append(relation.name)
        by_folded.setdefault((folded(schema), folded(bare)), []).append(relation.name)
    for table in inventory.tables:
        columns.extend(f"{table.name}.{c.name}" for c in table.columns if upper(c.name))
    if not (schemas or relations or columns):
        return []
    counts = ", ".join(
        part
        for part in (
            f"{len(schemas)} database name(s)" if schemas else "",
            f"{len(relations)} table/view name(s)" if relations else "",
            f"{len(columns)} column name(s)" if columns else "",
        )
        if part
    )
    examples = ", ".join([*sorted(schemas), *relations, *columns][:5])
    pairs = [names for names in by_folded.values() if len(names) > 1]
    risk = (
        f"{counts} contain upper-case letters (for example {examples}). Schema Conversion "
        "keeps every name exactly as spelled by quoting it, and Aurora DSQL folds an "
        "UNQUOTED name to lower case -- so application SQL that writes these names "
        "unquoted, which MySQL accepted (it matches column names case-insensitively), "
        "finds nothing after cut over."
    )
    if pairs:
        risk += (
            " These names differ only by case, so they cannot both be reached unquoted: "
            + "; ".join(" / ".join(names) for names in pairs[:5])
            + "."
        )
    item = AssessmentItem(
        object_name="identifier case",
        rule_id=MIXED_CASE_RULE_ID,
        classification=Classification.MANUAL,
        risk=risk,
        recommendation=(
            "Before cut over, quote these names in the application's SQL exactly as they "
            "are spelled (\"CustomerId\"). Or rename them to lower case on the SOURCE "
            "before Schema Conversion (MySQL still finds a column whatever case unquoted SQL "
            "writes; a table rename needs lower_case_table_names or the application's own "
            "change). Never rename them on the target before cut over: Full Load, CDC, "
            "Validation and the cut-over DDL all address the source spelling."
        ),
        effort=EffortLevel.MEDIUM,
        kind=KIND_DATABASE.upper(),
    )
    return [item.model_copy(update={"concerns": _single_concern(item)})]


# ---------------------------------------------------------------------------
# Source server settings (G148): what MySQL does for the application under them
# ---------------------------------------------------------------------------

SQL_MODE_RULE_ID = "SQL_MODE"
ZERO_DATES_RULE_ID = "ZERO_DATES"
IMPLICIT_TIMESTAMP_RULE_ID = "EXPLICIT_DEFAULTS_FOR_TIMESTAMP"
AUTO_INCREMENT_STEP_RULE_ID = "AUTO_INCREMENT_STEP"

# MySQL's composite modes as the flags they stand for. A GLOBAL value lists the flags as well,
# but one that names only the composite reads the same.
_SQL_MODE_COMPOSITES: dict[str, tuple[str, ...]] = {
    "ANSI": (
        "REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "ONLY_FULL_GROUP_BY",
    ),
    "TRADITIONAL": (
        "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE",
        "ERROR_FOR_DIVISION_BY_ZERO", "NO_ENGINE_SUBSTITUTION",
    ),
}
# Where the setting is changed on RDS / Aurora. Aurora MySQL's auto_increment_increment and
# auto_increment_offset are cluster-level parameters (DB cluster parameter group only), while
# sql_mode and explicit_defaults_for_timestamp are instance-level: the DB cluster parameter
# group holds their default, and an instance's DB parameter group that sets one wins there.
_SETTINGS_PARAMETER_GROUP = (
    "the source's DB parameter group (Aurora MySQL: its DB cluster parameter group)"
)
_INSTANCE_SETTINGS_PARAMETER_GROUP = (
    "the source's DB parameter group (Aurora MySQL: its DB cluster parameter group, or the "
    "DB parameter group of an instance that overrides it)"
)


def _sql_mode_flags(value: str) -> set[str]:
    """The flags of a MySQL ``sql_mode`` value, upper case, composites expanded."""
    flags = {part.strip().upper() for part in value.split(",") if part.strip()}
    for composite, parts in _SQL_MODE_COMPOSITES.items():
        if composite in flags:
            flags.update(parts)
    return flags


def _readable_sql_mode(value: Optional[str]) -> bool:
    """A ``sql_mode`` whose flags can be read: names separated by commas -- not an old
    server's number, nor an expression init_connect sets it to (CONCAT(@@sql_mode, ...))."""
    return isinstance(value, str) and re.fullmatch(
        r"\s*(?:[A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)?\s*", value
    ) is not None


def _init_connect_sql_mode(settings: "dict[str, str]") -> Optional[str]:
    """The sql_mode the source's init_connect sets for every session of a user without
    CONNECTION_ADMIN or SUPER -- the application's -- after the GLOBAL value (G161), as the
    MySQL dialect recorded it; None when it sets none."""
    from dsql_migrator.core.source_time_zone import init_connect_key

    value = settings.get(init_connect_key("sql_mode"))
    return value if isinstance(value, str) else None


def _unread_init_connect_mode(assigned: Optional[str]) -> str:
    """The sentence for an init_connect sql_mode that is an expression, else ``''``."""
    if assigned is None or _readable_sql_mode(assigned):
        return ""
    return (
        f" Its init_connect sets the application's sessions' sql_mode to {assigned}, which the "
        "tool cannot read: check SELECT @@SESSION.sql_mode as the application's user."
    )


def _first_names(names: "Sequence[str]", limit: int = 10) -> str:
    """``a, b, c and N more``: the first ``limit`` names, then how many are left."""
    shown = ", ".join(names[:limit])
    return shown + (f" and {len(names) - limit:,} more" if len(names) > limit else "")


def _database_item(
    *,
    object_name: str,
    rule_id: str,
    risk: str,
    recommendation: str,
    effort: Optional[EffortLevel],
    advisory: bool = False,
) -> AssessmentItem:
    """One database-level item with its single concern: a MANUAL gap, or -- ``advisory`` --
    an AUTO note with no effort, shown as a recommendation."""
    item = AssessmentItem(
        object_name=object_name,
        rule_id=rule_id,
        classification=Classification.AUTO if advisory else Classification.MANUAL,
        risk=risk,
        recommendation=recommendation,
        effort=None if advisory else effort,
        kind=KIND_DATABASE.upper(),
    )
    note_kind = ConversionNoteKind.RECOMMENDATION if advisory else ConversionNoteKind.LOSS
    return item.model_copy(update={"concerns": _single_concern(item, note_kind)})


def check_mysql_sql_mode(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """The flags of the source's GLOBAL ``sql_mode`` that matter after the move, in one item.

    Every application session starts from that mode (the tool's own sessions read with their
    own, ALLOW_INVALID_DATES alone), so it says what the application and the source's data
    were allowed to do:
    without ONLY_FULL_GROUP_BY a view or query may select a column it neither groups nor
    aggregates, which Aurora DSQL rejects (42803); under ANSI_QUOTES or PIPES_AS_CONCAT the
    application's SQL means something else in MySQL's default mode, which the Query
    Converter reads. None of these is a fault of the conversion, so the item names what to
    check, not a fix it made. What the zero-date and strict flags let the source store is
    its own item, ZERO_DATES (:func:`check_mysql_zero_dates`). Where the source's
    init_connect sets the mode (G161), the application's sessions run with that one, which
    is graded instead. Nothing when the mode was not read, is numeric (an old server's
    form), or none of the flags matter."""
    value = inventory.server_settings.get("sql_mode")
    assigned = _init_connect_sql_mode(inventory.server_settings)
    if assigned is not None and _readable_sql_mode(assigned):
        graded = assigned
        subject = (
            f"The source's init_connect sets sql_mode '{assigned}' for every session of a "
            "user without CONNECTION_ADMIN or SUPER -- the application's"
            + (f" (the global sql_mode is '{value}')" if value is not None else "")
            + ". "
        )
    else:
        graded = value
        subject = f"The source's global sql_mode is '{value}'.{_unread_init_connect_mode(assigned)} "
    if graded is None or graded.strip().isdigit():
        return []
    flags = _sql_mode_flags(graded)
    risks: list[str] = []
    advice: list[str] = []
    if "ONLY_FULL_GROUP_BY" not in flags:
        risks.append(
            "Without ONLY_FULL_GROUP_BY, a view or an application query may select a column "
            "that is neither grouped, aggregated, nor fixed by a grouped primary key, and "
            "MySQL returns a value from one of the group's rows. Aurora DSQL rejects such a "
            "query (SQLSTATE 42803: the column must appear in the GROUP BY clause or be used "
            "in an aggregate function) -- a view when Schema Conversion applies it, an "
            "application query when it runs after cut over."
        )
        advice.append(
            "Group or aggregate every selected column of a view the apply rejects (MIN() or "
            "MAX() keeps one value where MySQL picked one), and test the application's GROUP "
            "BY queries on Aurora DSQL before cut over -- the Query Converter can probe them."
        )
    special = [flag for flag in ("ANSI_QUOTES", "PIPES_AS_CONCAT") if flag in flags]
    if special:
        meanings = []
        if "ANSI_QUOTES" in flags:
            meanings.append('"name" is an identifier, not a string')
        if "PIPES_AS_CONCAT" in flags:
            meanings.append("a || b concatenates instead of meaning OR")
        risks.append(
            f"With {' and '.join(special)}, application SQL written for that mode "
            f"({'; '.join(meanings)}) means something else in MySQL's default mode, which is "
            "how the Query Converter reads SQL: it converts such a statement into a different "
            "one. The tool's own reads are not affected: its source sessions run with sql_mode "
            "'ALLOW_INVALID_DATES', which has neither."
        )
        advice.append(
            "Rewrite application SQL for MySQL's default mode before pasting it into the Query "
            "Converter (backticks around names, CONCAT() for ||)."
        )
    if not risks:
        return []
    advice.append(
        "To change what the source accepts from now on, set sql_mode in "
        + ("the init_connect of " if assigned is not None else "")
        + _INSTANCE_SETTINGS_PARAMETER_GROUP
        + (", which sets it after the global value" if assigned is not None else "")
        + "; it reaches new connections without a reboot. "
        "Test the application first: a stricter mode rejects writes it makes today."
    )
    return [
        _database_item(
            object_name="sql_mode",
            rule_id=SQL_MODE_RULE_ID,
            risk=subject + " ".join(risks),
            recommendation=" ".join(advice),
            effort=EffortLevel.MEDIUM,
        )
    ]


# How many tables' check queries the ZERO_DATES item spells out; the rest are counted.
_ZERO_DATE_QUERIES_SHOWN = 5


def check_mysql_zero_dates(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """Zero and invalid dates (G150): the source's GLOBAL ``sql_mode`` lets it store dates
    Aurora DSQL has no value for (no NO_ZERO_DATE or no NO_ZERO_IN_DATE, no strict mode, or
    ALLOW_INVALID_DATES), or a column's DEFAULT writes a zero date, so the selected tables'
    DATE / DATETIME / TIMESTAMP columns may hold one. Evaluation reads no rows: the item
    lists those columns, says what Full Load and CDC write for such a value
    (:mod:`dsql_migrator.core.mysql_zero_dates` -- and that a day the month does not have
    stops CDC), and gives the query per table that counts the rows holding one. Nothing when
    no table has such a column, or neither the mode (unread counts as nothing let in, like
    the other setting checks) nor a DEFAULT lets one in."""
    from dsql_migrator.core.converter import _is_zero_temporal_literal
    from dsql_migrator.core.mysql_zero_dates import check_query, temporal_base

    columns = [
        f"{table.name}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        if temporal_base(column) is not None
    ]
    if not columns:
        return []
    zero_defaults = [
        f"{table.name}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        if temporal_base(column) is not None
        and column.default is not None
        and not column.default_is_expression
        and _is_zero_temporal_literal(column.default)
    ]
    value = inventory.server_settings.get("sql_mode")
    assigned = _init_connect_sql_mode(inventory.server_settings)
    # The modes the source's sessions run with: the global one, and the one its init_connect
    # sets for the application's (G161). What either lets in may be there, so each flag test
    # below holds when it holds for either (the NO_ flags kept only where both have them).
    modes = [mode for mode in (value, assigned) if mode is not None and _readable_sql_mode(mode)]
    read = bool(modes)
    each = [_sql_mode_flags(mode) for mode in modes]
    flags = set.intersection(*each) | (set.union(*each) & {"ALLOW_INVALID_DATES"}) if each else set()
    strict = read and all(mode & {"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} for mode in each)
    reasons = [
        reason
        for reason, applies in (
            ("no NO_ZERO_DATE", "NO_ZERO_DATE" not in flags),
            ("no NO_ZERO_IN_DATE", "NO_ZERO_IN_DATE" not in flags),
            ("no strict mode (STRICT_TRANS_TABLES or STRICT_ALL_TABLES)", not strict),
            ("ALLOW_INVALID_DATES", "ALLOW_INVALID_DATES" in flags),
        )
        if applies and read
    ]
    if not reasons and not zero_defaults:
        return []
    # Only what these flags let in: a zero date unless strict mode and NO_ZERO_DATE refuse it
    # (without strict mode NO_ZERO_DATE only warns) -- or a DEFAULT writes one --, a zero month
    # or day unless NO_ZERO_IN_DATE is set (without strict mode it becomes a zero date), and a
    # day the month does not have only under ALLOW_INVALID_DATES.
    zero_date = bool(zero_defaults) or (read and ("NO_ZERO_DATE" not in flags or not strict))
    zero_part = read and "NO_ZERO_IN_DATE" not in flags
    invalid_day = "ALLOW_INVALID_DATES" in flags
    risks: list[str] = []
    if reasons:
        examples = [
            example
            for example, applies in (
                ("a zero date ('0000-00-00')", "NO_ZERO_DATE" not in flags or not strict),
                ("a zero month or day ('2024-00-15')", zero_part),
                ("a day the month does not have ('2024-02-31')", invalid_day),
            )
            if applies
        ]
        listed = (
            ", ".join(examples[:-1]) + " or " + examples[-1] if len(examples) > 1 else examples[0]
        )
        lenient = (
            " Without strict mode MySQL also stores an invalid date it is given as a zero "
            "date, with only a warning."
            if not strict
            else ""
        )
        if assigned is None or not _readable_sql_mode(assigned):
            subject = f"The source's global sql_mode is '{value}', which lets"
        elif len(modes) > 1:
            subject = (
                f"The source's global sql_mode is '{value}' and its init_connect sets "
                f"'{assigned}' for the application's sessions, which between them let"
            )
        else:
            subject = f"The source's init_connect sets sql_mode '{assigned}' for the application's sessions, which lets"
        risks.append(
            f"{subject} it store dates Aurora DSQL has no value for ({'; '.join(reasons)}): "
            f"{listed}.{lenient}{_unread_init_connect_mode(assigned)}"
        )
    if zero_defaults:
        risks.append(
            "A column whose DEFAULT is the zero date holds one in every row written without "
            f"a value for it ({_counted(zero_defaults, 'column')}); Schema Conversion drops "
            "that DEFAULT (DEFAULT_DROPPED)."
        )
    risks.append(
        "The DATE, DATETIME and TIMESTAMP columns of the selected tables may hold such "
        f"values ({_counted(columns, 'column')})."
    )
    zero_kinds = [
        kind
        for kind, applies in (("a zero date", zero_date), ("a zero month or day", zero_part))
        if applies
    ]
    mapped = []
    if zero_kinds:
        subject = (
            "a zero date, or a zero month or day," if len(zero_kinds) > 1 else zero_kinds[0]
        )
        mapped.append(
            f"{subject} becomes NULL where the column allows NULL, and otherwise the "
            "column's DEFAULT when that is a date, or else 1970-01-01 (1970-01-01 00:00:00 in "
            "a DATETIME); the zero TIMESTAMP becomes 1970-01-01 00:00:00 UTC"
        )
    if invalid_day:
        mapped.append(
            "a day the month does not have becomes 1970-01-01 (1970-01-01 00:00:00 in a "
            "DATETIME), Debezium's substitute for a date it cannot read"
        )
    risks.append(
        "Full Load writes each such value as the CDC connector (Debezium) writes it from the "
        f"binary log, and Validation compares the source the same way: {'; '.join(mapped)}."
        + (
            " CDC cannot carry a day the month does not have: the source connector stops at "
            "the first change to a row that holds one, in any table the binary log records, "
            "and stops there again each time it is restarted."
            if invalid_day
            else ""
        )
    )
    # A key column's value is not mapped (G154): mapped keys fall onto one.
    key_columns = [
        f"{table.name}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        if column.name in table.primary_key and temporal_base(column) is not None
    ]
    if key_columns:
        risks.append(
            f"A primary key column is the exception ({_counted(key_columns, 'column')}): "
            "mapped, rows whose keys hold such dates would fall onto one key, so Full Load "
            "leaves the value as stored, which Aurora DSQL rejects (SQLSTATE 22008) -- the row "
            "is quarantined and Validation reports the difference -- while CDC writes a change "
            "to such a row at the mapped key, where it can overwrite or delete the row whose "
            "key that is."
        )
    unique_columns = [
        f"{table.name}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        if column.name not in table.primary_key
        and temporal_base(column) is not None
        and any(index.unique and column.name in index.columns for index in table.indexes)
    ]
    if unique_columns:
        risks.append(
            f"In a unique key ({_counted(unique_columns, 'column')}), different such dates can "
            "be written as one value, or as the value another row holds: its index then fails "
            "to build after the load, and CDC dead-letters the row that collides (SQLSTATE "
            "23505)."
        )
    queries = [
        query
        for table in inventory.tables
        if (query := check_query(table)) is not None
    ]
    shown = " ".join(queries[:_ZERO_DATE_QUERIES_SHOWN])
    more = len(queries) - _ZERO_DATE_QUERIES_SHOWN
    advice = [
        "Evaluation reads no rows. To count the rows that hold such a date, run on the source, "
        f"for each table (each reads its table once): {shown}"
        + (
            f" -- and {more:,} more table{'' if more == 1 else 's'}, the same test over "
            "their DATE, DATETIME and TIMESTAMP columns."
            if more > 0
            else ""
        ),
        "Where the value Full Load writes is not what the row should hold"
        + (", and in every primary or unique key that holds such a date"
           if key_columns or unique_columns else "")
        + ", correct the row on the source before the Full Load."
        + (
            # G156: a correction is itself a change CDC replays from its start point.
            " Correct every day the month does not have before CDC's start point is "
            "recorded -- the Full Load in Full load + CDC, Start CDC in CDC only: CDC stops at "
            "the first binary-log change to a row that holds one, and a correction made "
            "after that point is such a change. Corrected after it, take a newer start point "
            "(Re-run Full Load choosing Drop & reload) before Start CDC; a CDC that already "
            "stopped at one starts again only from a newer start point."
            if invalid_day
            else ""
        ),
        "Before cut over, change the application SQL that writes or compares with such a "
        "date: Aurora DSQL rejects one in an INSERT, an UPDATE or a WHERE (SQLSTATE 22008), "
        "and the rows a comparison with it found (deleted_at = '0000-00-00 00:00:00') now "
        "hold NULL, the column's DEFAULT or 1970-01-01, as above -- compare with IS NULL or "
        "that value instead.",
    ]
    if reasons:
        advice.append(
            f"To make the source refuse them from now on, set sql_mode in "
            f"{_INSTANCE_SETTINGS_PARAMETER_GROUP}"
            + (" (and in its init_connect, which sets it after the global value)" if assigned is not None else "")
            + " with strict mode, NO_ZERO_DATE and "
            "NO_ZERO_IN_DATE, and without ALLOW_INVALID_DATES; it reaches new connections "
            "without a reboot. Test the application first: a stricter mode rejects writes it "
            "makes today."
        )
    return [
        _database_item(
            object_name="zero and invalid dates",
            rule_id=ZERO_DATES_RULE_ID,
            risk=" ".join(risks),
            recommendation=" ".join(advice),
            effort=EffortLevel.MEDIUM,
        )
    ]


def check_mysql_explicit_defaults_for_timestamp(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """``explicit_defaults_for_timestamp=OFF``: MySQL's implicit TIMESTAMP behaviours.

    Under it, writing NULL to a TIMESTAMP column not declared NULL stores the current time
    (decided by the session's value when the row is written, so every such column of the
    selected tables is affected), and a table created under it gets implicit DEFAULT / ON
    UPDATE attributes -- which are in the definitions Schema Conversion reads, with findings
    of their own. The NULL write is what nothing carries: the target column keeps its NOT
    NULL. Nothing when the setting is ON or unread, or no table has such a column."""
    value = (inventory.server_settings.get("explicit_defaults_for_timestamp") or "").strip()
    if value.upper() not in ("OFF", "0"):
        return []
    columns = [
        f"{table.name}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        if _base_type(column.mysql_type) == "timestamp" and not column.nullable
    ]
    if not columns:
        return []
    return [
        _database_item(
            object_name="explicit_defaults_for_timestamp",
            rule_id=IMPLICIT_TIMESTAMP_RULE_ID,
            risk=(
                "The source runs explicit_defaults_for_timestamp=OFF. Under it, MySQL "
                "stores the current date and time when NULL is written to a TIMESTAMP column "
                "that is not declared NULL, and gives a table created under it implicit "
                "TIMESTAMP attributes (the first such column DEFAULT CURRENT_TIMESTAMP ON "
                "UPDATE CURRENT_TIMESTAMP, the others a zero default). The attributes are in "
                "the column definitions Schema Conversion reads, and their ON UPDATE and "
                "zero-default parts have findings of their own (ON_UPDATE_TIMESTAMP, "
                "DEFAULT_DROPPED). The NULL write is not reproduced: on Aurora DSQL the "
                "converted column keeps NOT NULL, so an INSERT or UPDATE that writes NULL to "
                "it fails (SQLSTATE 23502). TIMESTAMP columns declared NOT NULL "
                f"({len(columns):,}): {_first_names(columns)}."
            ),
            recommendation=(
                "Before cut over, make the application write the time itself wherever it "
                "writes NULL to these columns -- or leave the column out of the INSERT, so "
                "its default applies. To find such writes while the source still serves the "
                "application, run it against explicit_defaults_for_timestamp=ON, under which "
                "MySQL no longer turns them into the current time: set it in "
                f"{_INSTANCE_SETTINGS_PARAMETER_GROUP}, and reboot where the parameter is static."
            ),
            effort=EffortLevel.SIMPLE,
        )
    ]


def check_mysql_auto_increment_step(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """``auto_increment_increment`` above 1: MySQL issues AUTO_INCREMENT values that far
    apart (``auto_increment_offset`` + N x ``auto_increment_increment``), usually so that
    several writers never issue the same key, while the target's identity steps by 1 from
    where the migration moves it. An increment of 1 issues consecutive values whatever the
    offset (MySQL ignores an offset larger than the increment), so there is nothing to tell.
    Nothing either when no table has an AUTO_INCREMENT key, or the setting was not read."""
    def _number(name: str) -> Optional[int]:
        try:
            return int(str(inventory.server_settings.get(name, "")).strip())
        except ValueError:
            return None

    increment = _number("auto_increment_increment")
    if increment is None or increment <= 1:
        return []
    tables = [table.name for table in inventory.tables if table.auto_increment_column]
    if not tables:
        return []
    offset = _number("auto_increment_offset")
    offset_text = f", auto_increment_offset {offset}" if offset is not None else ""
    return [
        _database_item(
            object_name="auto_increment_increment",
            rule_id=AUTO_INCREMENT_STEP_RULE_ID,
            risk=(
                f"The source's auto_increment_increment is {increment}{offset_text}: MySQL "
                f"issues each table's AUTO_INCREMENT values {increment} apart "
                "(auto_increment_offset + N x auto_increment_increment), usually so that "
                "several servers that each take writes never issue the same key. On Aurora "
                "DSQL a key the conversion keeps generating is an identity that steps by 1 "
                "from where the migration moves it (past the loaded rows and the source's own "
                "counter), so new keys no longer follow that pattern: they take the values in "
                "between and interleave differently with keys issued anywhere else. Tables "
                f"with an AUTO_INCREMENT key ({len(tables):,}): {_first_names(tables)}."
            ),
            recommendation=(
                "The migration needs no change on the source. Before cut over, check whether "
                "anything relies on the step or the offset -- another writer issuing the keys "
                "in between, a key read to tell which server wrote the row, ranges reserved "
                "per server -- and move that logic to the application. The values come from "
                f"{_SETTINGS_PARAMETER_GROUP}."
            ),
            effort=EffortLevel.SIMPLE,
        )
    ]


# ---------------------------------------------------------------------------
# The source's time zone (G149): what Schema Conversion writes in it, and what it cannot
# ---------------------------------------------------------------------------

SOURCE_TIME_ZONE_RULE_ID = "SOURCE_TIME_ZONE"


def _zone_set_statement(zone: "SourceZone") -> str:
    """The statement that gives an Aurora DSQL session ``zone``. A fixed offset as an
    INTERVAL: quoted, '+09:00' is read as a POSIX zone name, west of UTC."""
    if zone.kind == "offset":
        return f"SET TIME ZONE INTERVAL '{zone.zone}' HOUR TO MINUTE"
    return f"SET TIME ZONE '{zone.zone}'"


def _zone_clock_text(zone: "SourceZone") -> str:
    """:meth:`SourceZone.clock_sql` as a sentence quotes it, without its outer parentheses."""
    return zone.clock_sql()[1:-1]


def _counted(names: "Sequence[str]", noun: str) -> str:
    """``3 columns: a, b, c`` (the first ten, then how many more)."""
    return f"{len(names):,} {noun}{'' if len(names) == 1 else 's'}: {_first_names(names)}"


def _pg_role_zones(settings: "dict[str, str]") -> str:
    """The TimeZone defaults ALTER ROLE gives single roles (G148 records them), as a sentence
    -- an application that connects as such a role gets its zone, not the database's."""
    entries = [
        f"{key[len('TimeZone ('):-1]} '{value}'"
        for key, value in settings.items()
        if key.startswith("TimeZone (ALTER ROLE ") and not key.startswith("TimeZone (ALTER ROLE ALL")
    ]
    if not entries:
        return ""
    return (
        f" Some roles have a TimeZone default of their own ({', '.join(entries)}): an "
        "application that connects as one of them gets that zone instead."
    )


def _mysql_time_zone_item(inventory: SourceInventory, zone: "SourceZone") -> AssessmentItem:
    from dsql_migrator.core.converter import zoned_default_columns
    from dsql_migrator.core.source_time_zone import views_reading_the_clock

    if not zone.shifts:
        assumes = (
            "Schema Conversion assumes UTC: a DATETIME column's clock default becomes DEFAULT "
            "(now() AT TIME ZONE 'UTC'), and the views' NOW() and CURDATE() read the "
            "session's clock (UTC on Aurora DSQL unless the session sets another zone). Full "
            "Load and CDC copy DATETIME values unchanged."
        )
        if zone.setting.startswith("time_zone 'SYSTEM'"):
            risk = (
                f"The source runs in the server's own time zone ({zone.setting}), which MySQL "
                "reports by its abbreviation: that names no single zone, and daylight saving "
                "may change it, so " + assumes
            )
        else:
            risk = (
                f"The source's time zone ({zone.setting}) is not a zone name or a fixed offset "
                "the conversion can write, so " + assumes
            )
        return _database_item(
            object_name="time_zone",
            rule_id=SOURCE_TIME_ZONE_RULE_ID,
            risk=risk,
            recommendation=(
                "If the source runs in a zone other than UTC ("
                + (
                    "SELECT @@SESSION.time_zone, @@system_time_zone as the application's user"
                    if "init_connect" in zone.setting
                    else "SELECT @@GLOBAL.time_zone, @@system_time_zone"
                )
                + "), edit those defaults and views in Schema Conversion to "
                "name it (now() AT TIME ZONE 'Area/City'), and have the application run SET "
                "TIME ZONE 'Area/City' on every new Aurora DSQL connection."
            ),
            effort=None,
            advisory=True,
        )
    columns = [
        f"{table.name}.{name}"
        for table in inventory.tables
        for name in zoned_default_columns(table, zone, is_postgres=False)
    ]
    views = views_reading_the_clock(inventory.views, SourceType.MYSQL)
    written = (
        f"a DATETIME column's clock default becomes DEFAULT {zone.clock_sql()}, a DATE or "
        f"TIME column's its date or time of day ({_counted(columns, 'column')})"
        if columns
        else "no column default reads the clock"
    )
    if views:
        written += (
            ", and the views' NOW(), SYSDATE(), CURDATE() and CURTIME() read that zone's "
            f"clock ({_counted(views, 'view')})"
        )
    if any(not view.unsupported_kind for view in inventory.views):
        # G157: what a view reads in the session's zone in MySQL is read in the source's.
        written += (
            "; the views read FROM_UNIXTIME() and a TIMESTAMP column's date, time and text "
            "(DATE(), HOUR(), DATE_FORMAT() ...) in that zone too, and compare that zone's "
            "clock with a TIMESTAMP column as the instant it names there, as MySQL does"
        )
    offset_note = (
        f"; written as the string '{zone.zone}', the offset would be read as a POSIX zone, "
        "west of UTC"
        if zone.kind == "offset"
        else ""
    )
    return _database_item(
        object_name="time_zone",
        rule_id=SOURCE_TIME_ZONE_RULE_ID,
        risk=(
            f"The source runs in time zone {zone.label} ({zone.setting}). MySQL's NOW(), "
            "CURRENT_TIMESTAMP, CURDATE() and CURTIME() return that zone's wall clock, and "
            "DATETIME, DATE and TIME columns store it as it is; Full Load and CDC copy those "
            "values unchanged. An Aurora DSQL session runs in UTC unless it sets another "
            "zone, so Schema Conversion writes the source's zone where the schema reads the "
            f"clock: {written}. TIMESTAMP columns hold instants and keep DEFAULT now(), and "
            "UTC_TIMESTAMP() and UTC_DATE() stay UTC."
        ),
        recommendation=(
            "An application that relies on the session's time zone -- NOW(), "
            "CURRENT_TIMESTAMP or CURDATE() written into DATETIME or DATE columns, or "
            "compared with them -- gets UTC on Aurora DSQL: have it run "
            f"{_zone_set_statement(zone)} on every new connection (Aurora DSQL allows "
            f"it{offset_note}), or write the zone explicitly ({_zone_clock_text(zone)}). Test "
            "those statements on Aurora DSQL before cut over; the Query Converter writes them "
            "for the session's zone."
        ),
        effort=EffortLevel.SIMPLE,
    )


def _pg_time_zone_item(inventory: SourceInventory, zone: "SourceZone") -> AssessmentItem:
    from dsql_migrator.core.converter import zoned_default_columns
    from dsql_migrator.core.source_time_zone import views_reading_the_clock

    roles = _pg_role_zones(inventory.server_settings)
    if not zone.shifts:
        assumes = (
            "Schema Conversion assumes UTC: a clock default on a timestamp without time "
            "zone, date or time column is carried as written and computes in the session's "
            "zone (UTC on Aurora DSQL unless the session sets another). Full Load and CDC "
            "copy those values unchanged."
        )
        if not zone.setting:
            risk = (
                "The source's default TimeZone could not be read (the short connection "
                "Evaluation opens to read it failed), so " + assumes
            )
        else:
            risk = (
                f"The source's default time zone ({zone.setting}) is not a zone name or a fixed "
                "offset the conversion can write (a POSIX rule with daylight saving, the "
                "server's local zone), so " + assumes
            )
        return _database_item(
            object_name="TimeZone",
            rule_id=SOURCE_TIME_ZONE_RULE_ID,
            risk=risk + roles,
            recommendation=(
                ("Re-run Evaluation to read it. " if not zone.setting else "")
                + "If the source runs in a zone other than UTC (SHOW TimeZone in a new "
                "session of the application's role), edit those defaults in Schema Conversion "
                "to name it (now() AT TIME ZONE 'Area/City'), and have the application run "
                "SET TIME ZONE 'Area/City' on every new Aurora DSQL connection."
            ),
            effort=None,
            advisory=True,
        )
    columns = [
        f"{table.name}.{name}"
        for table in inventory.tables
        for name in zoned_default_columns(table, zone, is_postgres=True)
    ]
    views = views_reading_the_clock(inventory.views, SourceType.POSTGRES)
    written = (
        f"DEFAULT now() on a timestamp column becomes DEFAULT {zone.clock_sql()}, on a date "
        f"or time column its date or time of day ({_counted(columns, 'column')})"
        if columns
        else "no column default reads the clock"
    )
    viewed = (
        " Views are carried as PostgreSQL deparsed them, so a view that reads the clock "
        f"computes in the session's zone ({_counted(views, 'view')})."
        if views
        else ""
    )
    offset_note = (
        f"; written as the string '{zone.zone}', the offset would be read as a POSIX zone, "
        "west of UTC"
        if zone.kind == "offset"
        else ""
    )
    return _database_item(
        object_name="TimeZone",
        rule_id=SOURCE_TIME_ZONE_RULE_ID,
        risk=(
            f"The source's default time zone is {zone.label} ({zone.setting}, what a new "
            "session gets when the client sets none). now(), CURRENT_TIMESTAMP, "
            "LOCALTIMESTAMP and CURRENT_DATE give that zone's wall clock where a timestamp "
            "without time zone, date or time column stores them; Full Load and CDC copy those "
            "values unchanged. An Aurora DSQL session runs in UTC unless it sets another "
            "zone, so Schema Conversion writes the source's zone into those columns' clock "
            f"defaults: {written}; timestamp with time zone columns hold instants and keep "
            f"theirs.{viewed}{roles}"
        ),
        recommendation=(
            "An application that relies on the session's time zone -- now() or CURRENT_DATE "
            "written into such columns, or compared with them -- gets UTC on Aurora DSQL: "
            f"have it run {_zone_set_statement(zone)} on every new connection (Aurora DSQL "
            f"allows it{offset_note}), or write the zone explicitly ({_zone_clock_text(zone)})."
            + (
                " Review the views listed: where one depends on the zone, write it there in "
                "the view's target DDL before applying, or rely on that SET TIME ZONE."
                if views
                else ""
            )
        ),
        effort=EffortLevel.SIMPLE,
    )


def check_mysql_time_zone(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """The source's time zone (G149), from its GLOBAL ``time_zone`` -- or the one its
    init_connect sets for the application's sessions (G161): in a zone other than UTC,
    what Schema Conversion writes in it -- DATETIME / DATE / TIME clock defaults, the views'
    NOW() / CURDATE() / CURTIME() -- and what the application has to do (MANUAL); a zone it
    could not read or tell, that the conversion assumes UTC (an advisory note). Nothing for
    UTC, nor when ``time_zone`` was not read (a failed read, or a report from before
    Evaluation read the settings), like the other setting checks."""
    from dsql_migrator.core.source_time_zone import resolve_source_zone

    if "time_zone" not in inventory.server_settings:
        return []
    zone = resolve_source_zone(inventory.server_settings, SourceType.MYSQL)
    return [] if zone.kind == "utc" else [_mysql_time_zone_item(inventory, zone)]


def check_postgres_time_zone(
    inventory: SourceInventory, target_inventory: Optional[TargetInventory] = None
) -> list[AssessmentItem]:
    """:func:`check_mysql_time_zone` for a PostgreSQL source's default ``TimeZone``: the clock
    defaults of timestamp without time zone / date / time columns, the views that read the
    clock (carried as deparsed, so listed for review), the roles with a zone of their own.
    Its own connection reads the TimeZone, so a read that captured the ALTER DATABASE /
    ALTER ROLE defaults but not it says so; one that captured nothing says nothing."""
    from dsql_migrator.core.source_time_zone import resolve_source_zone

    if not inventory.server_settings:
        return []
    zone = resolve_source_zone(inventory.server_settings, SourceType.POSTGRES)
    return [] if zone.kind == "utc" else [_pg_time_zone_item(inventory, zone)]


def default_inventory_rules(
    source_type: SourceType = SourceType.MYSQL,
) -> list[InventoryRule]:
    """Return the default inventory-level (cluster-wide) checks for ``source_type``.

    ``check_multiple_source_databases`` is MySQL-only: a PostgreSQL source is
    introspected SCHEMA-qualified (``schema.table``), and its multiple schemas live
    within one database, which Aurora DSQL supports and this tool migrates
    schema-qualified automatically -- so the "spans N databases" note is meaningless
    for a PG source. The database-level limits (tables, schemas, views per database)
    apply to every engine. The server-setting checks (sql_mode and the zero / invalid dates
    it lets in -- G150, which also reads the columns' DEFAULTs --,
    explicit_defaults_for_timestamp, the auto_increment step) read MySQL settings; the
    time zone check reads each engine's (G149).
    """
    if source_type is SourceType.POSTGRES:
        return [
            check_table_count,
            check_pg_schema_count,
            check_view_count,
            check_postgres_extensions,
            check_postgres_database_collation,
            check_postgres_time_zone,
        ]
    return [
        check_multiple_source_databases,
        check_table_count,
        check_schema_count,
        check_view_count,
        check_mixed_case_identifiers,
        check_mysql_sql_mode,
        check_mysql_zero_dates,
        check_mysql_explicit_defaults_for_timestamp,
        check_mysql_auto_increment_step,
        check_mysql_time_zone,
    ]


def default_rules(source_type: SourceType = SourceType.MYSQL) -> list[Rule]:
    """Return the default, ordered list of compatibility rules for ``source_type``.

    The order is significant: it breaks ties when several rules of equal
    severity match the same object. Many rules inspect source-engine column types
    (ENUM/SET, TINYINT(1), BIT, YEAR, ...) or MySQL-specific features, so the list
    is source-engine-specific. Only MySQL is defined so far; PostgreSQL rules arrive
    with PostgreSQL-source support, so an unsupported source type raises rather than
    silently applying MySQL rules to it.
    """
    if source_type is SourceType.POSTGRES:
        # PostgreSQL rules live in their own module (see assessor_postgres); imported
        # lazily so that module can import the shared rule classes from here.
        from dsql_migrator.core.assessor_postgres import default_rules as _pg_rules

        return _pg_rules()
    if source_type is not SourceType.MYSQL:
        raise NotImplementedError(
            f"No default assessment rules for source type {source_type!r} yet."
        )
    return [
        ForeignKeyRule(),
        CheckConstraintRule(),
        CascadeForeignKeyRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        AutoIncrementRule(),
        NoPrimaryKeyRule(),
        ByteaKeyRule(),
        CaseInsensitiveCollationRule(),
        PartitionedTableRule(),
        SpatialTypeRule(),
        TooManyColumnsRule(),
        TooManyIndexesRule(),
        TooManyKeyColumnsRule(),
        NameCollisionRule(),
        IdentifierLengthRule(),
        OversizedLobRule(),
        DecimalPrecisionRule(),
        EnumSetRule(),
        TinyIntBooleanRule(),
        BitTypeRule(),
        YearTypeRule(),
        TimeTypeRule(),
        expression_index_rule(),
        default_dropped_rule(),
        GeneratedColumnRule(),
        AutoUpdateTimestampRule(),
        UnsupportedIndexTypeRule(),
        ViewDefinitionUnreadableRule(),
        MySqlViewConversionRule(),
        ViewCompatibilityRule(),
    ]


# ---------------------------------------------------------------------------
# Assessment engine
# ---------------------------------------------------------------------------


def _inventory_objects(inventory: SourceInventory) -> list[ObjectKey]:
    """Return the ordered, canonical list of objects to classify.

    Tables first, then views, triggers, and routines, preserving inventory
    order. This is the exact set over which Property 8 must hold.
    """
    objects: list[ObjectKey] = []
    objects.extend(ObjectKey(KIND_TABLE, table.name) for table in inventory.tables)
    objects.extend(ObjectKey(KIND_VIEW, view.name) for view in inventory.views)
    objects.extend(
        ObjectKey(KIND_TRIGGER, trigger.name) for trigger in inventory.triggers
    )
    objects.extend(
        ObjectKey(_routine_kind(routine), routine.name)
        for routine in inventory.routines
    )
    objects.extend(
        ObjectKey(KIND_EVENT, event.name) for event in inventory.events
    )
    return objects


def _aggregate(key: ObjectKey, findings: list[Finding]) -> AssessmentItem:
    """Collapse all findings for one object into a single assessment item.

    The most severe classification among the object's REAL GAPS governs the item;
    risks and recommendations of every matched rule are combined so no finding is
    lost. An object with no findings -- or whose findings are ALL advisory
    (``RECOMMENDATION``, e.g. a preserved foreign key or an AUTO_INCREMENT
    throughput note) -- is classified ``AUTO``, since advisory findings report no
    defect and require no work (the advice is still surfaced as a ``RECOMMENDED``
    concern).
    """
    if not findings:
        return AssessmentItem(
            object_name=key.name,
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            risk="",
            recommendation="No DSQL compatibility issues detected.",
            effort=None,
            kind=key.kind.upper(),
        )

    # Order: real gaps before advice, then by descending severity. A stable sort keeps
    # rule declaration order for ties.
    #
    # Advice sinks to the bottom regardless of its classification. Sorting by severity
    # alone interleaved them -- an advisory MANUAL finding landed above a genuine MANUAL
    # gap purely by declaration order, so the reader met an optional throughput note
    # before the foreign key they actually have to deal with. What must be acted on now
    # belongs first; "you could also tune this" belongs last.
    ordered = sorted(
        findings,
        key=lambda f: (
            f.note_kind is ConversionNoteKind.RECOMMENDATION,
            -_SEVERITY[f.classification],
        ),
    )
    # The governing rule is therefore a real gap whenever the object has one, so the row
    # header never advertises an optional recommendation as the object's headline.
    governing = ordered[0]
    # Keep each finding as its own concern, paired with ITS recommendation. The joined
    # strings below stay for back-compat and flat CSV-style exports, but a table that
    # matches five rules produced one 400-character run-on sentence in which no risk was
    # matched to its own fix -- so anything rendering to a human uses `concerns`.
    concerns = [
        AssessmentConcern(
            rule_id=f.rule_id,
            classification=f.classification,
            risk=f.risk,
            recommendation=f.recommendation,
            effort=f.effort,
            note_kind=f.note_kind,
        )
        for f in ordered
    ]
    risk = "; ".join(f.risk for f in ordered if f.risk)
    recommendation = "; ".join(f.recommendation for f in ordered if f.recommendation)
    # The most demanding effort across REAL GAPS governs the estimate -- advisory
    # findings are excluded. Effort answers "how much work must I do to migrate this
    # object", and optional throughput advice is not work the migration requires: a
    # table needing only a foreign-key workaround (SIMPLE, under two hours) was being
    # reported as MEDIUM (two to six) purely because it also carried the AUTO_INCREMENT
    # recommendation, inflating the estimate for the most common table shape there is.
    # Advisory findings keep their own per-concern effort, so the cost of taking the
    # advice is still visible on the finding itself.
    gap_findings = [
        f for f in findings if f.note_kind is not ConversionNoteKind.RECOMMENDATION
    ]
    gap_efforts = [f.effort for f in gap_findings if f.effort is not None]
    # An object whose findings are ALL advisory has no required work, so it carries no
    # effort at all rather than borrowing the advice's estimate.
    effort = max(gap_efforts, key=lambda e: _EFFORT_ORDER[e]) if gap_efforts else None
    # ...and for the SAME reason it is classified AUTO, not by the advisory finding's
    # (MANUAL) severity. An advisory finding (a foreign key, an AUTO_INCREMENT throughput
    # note) reports NO defect -- the object converts automatically -- so an object with
    # only advisory findings is AUTO, mirroring the effort rule above. Leaving it MANUAL
    # produced a "MANUAL with no effort" object that read as AUTO/Recommended in the
    # object list (which labels advisory findings RECOMMENDED) but as "Review needed" in
    # every count derived from ``classification`` (report, chart, summary, score) -- the
    # two disagreed on the same table. The advice is still surfaced as a RECOMMENDED
    # concern below, so nothing is silently marked compatible (Property 8 holds).
    classification = governing.classification if gap_findings else Classification.AUTO
    return AssessmentItem(
        object_name=key.name,
        rule_id=governing.rule_id,
        classification=classification,
        risk=risk,
        recommendation=recommendation,
        effort=effort,
        kind=key.kind.upper(),
        concerns=concerns,
    )


class CompatibilityAssessor:
    """Classifies inventory objects as AUTO / MANUAL / UNSUPPORTED (Req 2)."""

    def __init__(
        self,
        rules: list[Rule] | None = None,
        *,
        inventory_rules: list[InventoryRule] | None = None,
        source_type: SourceType = SourceType.MYSQL,
    ) -> None:
        """Create an assessor.

        ``rules`` overrides the per-object rule list, enabling extension or
        customization. The order of the rules breaks classification ties.
        ``inventory_rules`` overrides the cluster/database-wide checks (e.g.
        multiple source databases, table-count limit) that produce findings not
        tied to a single object. ``source_type`` selects the default rule set for the
        source engine (MySQL vs PostgreSQL) when ``rules`` is not given.
        """
        self._rules: list[Rule] = (
            list(rules) if rules is not None else default_rules(source_type)
        )
        self._inventory_rules: list[InventoryRule] = (
            list(inventory_rules)
            if inventory_rules is not None
            else default_inventory_rules(source_type)
        )

    def assess(
        self,
        inventory: SourceInventory,
        target_inventory: Optional[TargetInventory] = None,
    ) -> AssessmentReport:
        """Assess ``inventory`` and return a complete report.

        Guarantees Property 8: every inventory object appears in exactly one
        per-object :class:`AssessmentItem`, so no object is left unclassified.
        Inventory-level (cluster/database-wide) checks may append additional
        items (kind ``DATABASE``) that are not tied to a single object.
        ``target_inventory`` is the browsed target catalog, when the caller has one:
        the database-level limits then count what the target database will hold after
        the migration, not only what the source brings.
        """
        findings_by_object: dict[ObjectKey, list[Finding]] = defaultdict(list)
        for rule in self._rules:
            for finding in rule.evaluate(inventory):
                findings_by_object[finding.object].append(finding)

        items = [
            _aggregate(key, findings_by_object.get(key, []))
            for key in _inventory_objects(inventory)
        ]
        for inventory_rule in self._inventory_rules:
            items.extend(inventory_rule(inventory, target_inventory))
        return AssessmentReport.from_items(items)


# ---------------------------------------------------------------------------
# Report export
# ---------------------------------------------------------------------------


def importance_order(items: "Sequence[AssessmentItem]") -> "list[AssessmentItem]":
    """``items`` most critical first: UNSUPPORTED > MANUAL > AUTO, then effort SIGNIFICANT >
    MEDIUM > SIMPLE > none, then the name -- the screen's order, so an export reads in the
    same order as the report it was taken from. Pure."""
    severity = {Classification.UNSUPPORTED: 2, Classification.MANUAL: 1, Classification.AUTO: 0}
    effort_rank = {EffortLevel.SIGNIFICANT: 3, EffortLevel.MEDIUM: 2, EffortLevel.SIMPLE: 1}
    return sorted(
        items,
        key=lambda item: (
            -severity.get(item.classification, 0),
            -effort_rank.get(item.effort, 0),
            item.object_name,
        ),
    )


def _attention_share(attention: int, total: int) -> str:
    """``attention / total`` as a whole percentage that never reads 0% or 100% when it is
    neither (1 of 1,000 is "<1%", not "0%")."""
    share = attention / total * 100
    if 0 < share < 1:
        return "<1%"
    if 99 < share < 100:
        return ">99%"
    return f"{round(share)}%"


def render_text_report(
    report: AssessmentReport,
    *,
    metadata: "Sequence[tuple[str, str]]" = (),
    conflicts: "Sequence[str]" = (),
    blocked: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Render a human-readable text version of an assessment report (English).

    ``metadata`` is the ``(label, value)`` header saying what was assessed (see the
    Evaluation export); ``conflicts`` / ``blocked`` are the target's existing and held
    names, listed after the items."""
    lines = ["Compatibility Assessment Report", "=" * 31, ""]
    if metadata:
        width = max(len(label) for label, _value in metadata) + 2
        lines.extend(f"{label + ':':<{width}}{value}" for label, value in metadata)
        lines.append("")
    lines.append("Difficulty summary (objects by classification):")
    for classification in Classification:
        lines.append(f"  {classification.value}: {report.summary.get(classification, 0)}")
    lines.append("")
    lines.append("Estimated manual effort (non-automatic objects):")
    for level in EffortLevel:
        lines.append(f"  {level.value}: {report.effort_summary.get(level, 0)}")
    lines.append("")
    lines.append(f"Items ({len(report.assessed_items)}):")
    for item in importance_order(report.assessed_items):
        effort = item.effort.value if item.effort is not None else "NONE"
        lines.append(
            f"- {item.object_name} [{item.classification.value}] "
            f"(effort: {effort}) ({item.rule_id})"
        )
        # One numbered block per matched rule, each risk beside its own fix. The joined
        # item.risk/item.recommendation are a single run-on sentence once an object
        # matches more than one rule, which is exactly when the report matters most.
        concerns = list(item.concerns or [])
        if concerns:
            for number, concern in enumerate(concerns, start=1):
                # An advisory finding is labeled RECOMMENDED rather than by severity: the
                # conversion is complete and correct, so its classification describes how
                # big the optional change would be, not that anything is wrong. Its
                # effort is likewise excluded from the object's estimate above, so the
                # text spells out that it is the cost of opting in.
                advisory = concern.is_advisory
                tag = "RECOMMENDED" if advisory else concern.classification.value
                head = f"    {number}. [{tag}] {concern.rule_id}"
                if concern.effort is not None:
                    head += (
                        f" (effort if you take it: {concern.effort.value})"
                        if advisory
                        else f" (effort: {concern.effort.value})"
                    )
                lines.append(head)
                if concern.risk:
                    label = "Note" if advisory else "Risk"
                    lines.append(f"       {label}: {concern.risk}")
                if concern.recommendation:
                    lines.append(f"       Fix:  {concern.recommendation}")
        else:
            if item.risk:
                lines.append(f"    Risk: {item.risk}")
            if item.recommendation:
                lines.append(f"    Recommendation: {item.recommendation}")
    if conflicts:
        lines.append("")
        lines.append(f"Already on the target ({len(conflicts)}):")
        lines.extend(f"- {name}" for name in conflicts)
    if blocked:
        lines.append("")
        lines.append(f"Names held on the target by another object ({len(blocked)}):")
        lines.extend(f"- {name}: {holder}" for name, holder in blocked)
    return "\n".join(lines)


def export_report(
    report: AssessmentReport,
    fmt: str = "json",
    *,
    source_type: SourceType = SourceType.MYSQL,
    metadata: "Sequence[tuple[str, str]]" = (),
    target_endpoint: Optional[str] = None,
    conflicts: "Sequence[str]" = (),
    blocked: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Export an assessment report as ``"json"``, ``"text"``, or ``"html"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable), with the
    ``metadata`` header under ``"metadata"`` and the target's endpoint and names under
    ``"target"`` beside the model's own keys; text is a readable summary; HTML is a
    styled, standalone document suitable for sharing (its title reflects the
    ``source_type``). Each lists the assessed objects only (never a report marker).
    Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        import json

        data: dict[str, object] = {}
        if metadata:
            data["metadata"] = {
                re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_"): value
                for label, value in metadata
            }
        # The report's own order (the human-read text and HTML are sorted by importance):
        # the JSON reloads as the same report.
        data.update(
            report.model_copy(update={"items": report.assessed_items}).model_dump(mode="json")
        )
        if target_endpoint or conflicts or blocked:
            data["target"] = {
                "endpoint": target_endpoint,
                "conflicts": list(conflicts),
                "blocked": [{"name": name, "holder": holder} for name, holder in blocked],
            }
        return json.dumps(data, indent=2)
    if normalized == "text":
        return render_text_report(
            report, metadata=metadata, conflicts=conflicts, blocked=blocked
        )
    if normalized == "html":
        return render_html_report(report, source_type=source_type, metadata=metadata)
    raise ValueError(
        f"unsupported report format: {fmt!r} (use 'json', 'text', or 'html')"
    )


# Background colors for the classification cell in the HTML report.
_HTML_CLASS_COLOR: dict[Classification, str] = {
    Classification.AUTO: "#e8f5e9",
    Classification.MANUAL: "#fff8e1",
    Classification.UNSUPPORTED: "#ffebee",
}

# Background for an ADVISORY finding's cell -- a calm info-blue, deliberately outside the
# green/amber/red severity ramp above, because such a finding reports no defect at all.
# Matches the sky tone the Evaluation screen uses for the same finding.
_HTML_ADVISORY_COLOR = "#e1f5fe"

# Solid bar colors per CLASSIFICATION for the HTML chart. The _HTML_CLASS_COLOR values
# above are pale table-cell backgrounds; a bar needs full-strength fills to be legible at
# 18px. Same green/amber/red severity ramp as the classification badges in the UI.
# Fixed order for the chart: best outcome first, so a bar reads left-to-right from
# "converts by itself" to "cannot be converted".
_CHART_ORDER: tuple[Classification, ...] = (
    Classification.AUTO,
    Classification.MANUAL,
    Classification.UNSUPPORTED,
)

# Chart labels. "Review needed" rather than the raw MANUAL, matching the wording the UI
# badges use, so the exported report and the screen agree.
_CLASS_CHART_LABELS: dict[Classification, str] = {
    Classification.AUTO: "Automatic",
    Classification.MANUAL: "Review needed",
    Classification.UNSUPPORTED: "Unsupported",
}

# The design system's one palette (Tailwind green / amber / red 600): amber, never orange,
# for a warning tone.
_CHART_CLASS_COLORS: dict[Classification, str] = {
    Classification.AUTO: "#16a34a",
    Classification.MANUAL: "#d97706",
    Classification.UNSUPPORTED: "#dc2626",
}


def classification_stats_by_kind(
    report: AssessmentReport,
) -> list[tuple[str, dict[Classification, int], int]]:
    """Per-kind counts split by CLASSIFICATION, largest kind first.

    This is what both the UI chart and the HTML export are built from, so the two always
    agree. Classification rather than effort, because the chart sits beside the
    classification summary and a table whose Classification column uses the same three
    words -- splitting the bars by effort instead made the reader translate between two
    vocabularies to reconcile them. Effort is still reported, in its own summary and per
    object; it is just not what this chart answers.

    Ordered by TOTAL OBJECT COUNT descending (ties broken by kind name), so the bars step
    down in length and the chart reads as a size ranking -- TABLE, then PROCEDURE, and so
    on. Ordering by trouble-share instead put a single unsupported TRIGGER above 200
    tables: it made short bars float above long ones, which reads as a broken chart, and
    the "most blocked" reading was already carried by each bar's own red segment and its
    "% need attention" caption.
    """
    counts: dict[str, dict[Classification, int]] = {}
    for item in report.assessed_items:
        bucket = counts.setdefault(item.kind, {c: 0 for c in Classification})
        bucket[item.classification] += 1

    def sort_key(entry: tuple[str, dict[Classification, int]]):
        kind, by_class = entry
        return (-sum(by_class.values()), kind)

    return [
        (kind, by_class, sum(by_class.values()))
        for kind, by_class in sorted(counts.items(), key=sort_key)
    ]


def _render_html_chart(report: AssessmentReport) -> str:
    """Render the per-kind chart as a self-contained HTML/CSS block.

    Split by CLASSIFICATION (auto / review needed / unsupported), not by effort. The chart
    sits directly above the classification summary and a table whose third column is
    Classification, so an effort-based split made the reader translate between two
    vocabularies to reconcile them. Effort is still reported -- in its own summary list
    and per row -- it is just not what this bar answers.

    Bars are scaled to the largest kind so lengths reflect counts, and no external script
    is used, so the exported report stays portable.
    """
    stats = classification_stats_by_kind(report)
    max_total = max((total for _kind, _by_class, total in stats), default=0)
    if not stats or max_total == 0:
        return ""

    legend = "".join(
        f'<span class="chip"><i style="background:{_CHART_CLASS_COLORS[cls]}"></i>'
        f"{html.escape(_CLASS_CHART_LABELS[cls])}</span>"
        for cls in _CHART_ORDER
    )

    rows: list[str] = []
    for kind, by_class, total in stats:
        bars = "".join(
            f'<span style="width:{by_class[cls] / max_total * 100:.2f}%;'
            f'background:{_CHART_CLASS_COLORS[cls]}"'
            f' title="{_CLASS_CHART_LABELS[cls]}: {by_class[cls]}"></span>'
            for cls in _CHART_ORDER
            if by_class[cls] > 0
        )
        # "needs attention" = anything not AUTO, which is the number an operator plans
        # around; the per-class counts are on the segment tooltips.
        attention = total - by_class[Classification.AUTO]
        meta = f"{total} object{'s' if total != 1 else ''}"
        if attention:
            meta += f" &middot; {_attention_share(attention, total)} need attention"
        rows.append(
            '<div class="bar-row">'
            f'<div class="bar-label">{html.escape(kind_label(kind))}</div>'
            f'<div class="bar">{bars}</div>'
            f'<div class="bar-meta">{meta}</div>'
            "</div>"
        )

    return (
        "<h2>Compatibility by object kind</h2>\n"
        f'<div class="legend">{legend}</div>\n'
        f'<div class="chart">{"".join(rows)}</div>\n'
    )


def _render_ai_html_section(ai_report: AiAssessmentReport) -> str:
    """Render the optional AI-led assessment as an HTML section (advisory)."""
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    parts = [
        '<section class="ai-report">',
        '<div class="ai-head"><span class="ai-badge">AI &middot; advisory</span>'
        "<h2>AI-led migration assessment</h2></div>",
        f'<p class="advisory">Generated by model {esc(ai_report.model_id)}. The '
        "deterministic classification and effort above remain authoritative.</p>",
    ]

    if ai_report.strategy_summary:
        # The AI reply is a free-form Markdown narrative; render it to HTML so the
        # export reads like a report (headings, lists, tables) instead of a wall
        # of escaped text. Untrusted output -> escape any embedded raw HTML.
        try:
            import markdown2

            body = markdown2.markdown(
                ai_report.strategy_summary,
                safe_mode="escape",
                extras=["tables", "fenced-code-blocks", "cuddled-lists"],
            )
        except Exception:  # noqa: BLE001 - degrade to plain text if rendering fails
            body = "<p>" + esc(ai_report.strategy_summary).replace("\n", "<br>") + "</p>"
        parts.append(f'<div class="ai-body">{body}</div>')

    if ai_report.insights:
        rows = "\n".join(
            "<tr>"
            f"<td>{esc(i.object_name)}</td>"
            f"<td>{esc(i.ai_effort.value if i.ai_effort is not None else '-')}</td>"
            f"<td>{esc(i.recommendation)}</td>"
            f"<td>{esc(i.rationale)}</td>"
            "</tr>"
            for i in ai_report.insights
        )
        parts.append("<h3>Per-object AI guidance</h3>")
        parts.append(
            "<table>\n<thead><tr><th>Object</th><th>AI effort</th>"
            "<th>AI recommendation</th><th>Rationale</th></tr></thead>\n"
            f"<tbody>\n{rows}\n</tbody>\n</table>"
        )

    if ai_report.additional_findings:
        rows = "\n".join(
            "<tr>"
            f"<td>{esc(f.area)}</td>"
            f"<td>{esc(f.risk)}</td>"
            f"<td>{esc(f.recommendation)}</td>"
            "</tr>"
            for f in ai_report.additional_findings
        )
        parts.append("<h3>Additional findings (AI, advisory)</h3>")
        parts.append(
            "<table>\n<thead><tr><th>Area</th><th>Risk</th>"
            "<th>Recommendation</th></tr></thead>\n"
            f"<tbody>\n{rows}\n</tbody>\n</table>"
        )

    parts.append("</section>")
    return "\n".join(parts)


def _render_target_html_section(
    target: TargetInventory,
    conflicts: "list[str]",
    blocked: "Sequence[tuple[str, str]]" = (),
    endpoint: "Optional[str]" = None,
) -> str:
    """Render the Target analysis (Aurora DSQL) section for the HTML export.

    Mirrors the on-screen Target analysis: the cluster it was read from (the limit items
    count what that cluster holds), the target catalog summary (schemas / tables / views),
    the source objects that already exist on the target, and those whose name another kind
    of object holds there (``blocked``: neither apply mode can create them). The all-clear
    is printed only when both lists are empty.
    """
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    table_count = sum(len(schema.tables) for schema in target.schemas)
    view_count = sum(len(schema.views) for schema in target.schemas)
    parts = [
        '<section class="target-section">',
        "<h2>Target analysis (Aurora DSQL)</h2>",
    ]
    if endpoint:
        parts.append(f"<p>Assessed against <strong>{esc(endpoint)}</strong>.</p>")
    parts.append(
        f"<p>Target catalog: <strong>{len(target.schemas)}</strong> schemas, "
        f"<strong>{table_count}</strong> tables, <strong>{view_count}</strong> "
        "views.</p>"
    )
    if conflicts:
        parts.append(
            f'<p class="warn">{len(conflicts)} source object(s) already exist on '
            "the target and may conflict when applying converted DDL:</p>"
        )
        chips = " ".join(
            f'<span class="conflict">{esc(name)}</span>' for name in conflicts
        )
        parts.append(f"<p>{chips}</p>")
        parts.append(
            '<p class="muted">Resolve these in Schema Conversion: choose SKIP '
            "(keep the existing object) or REPLACE (recreate it) when you "
            "apply.</p>"
        )
    if blocked:
        from dsql_migrator.core.target_introspector import BLOCKED_NAMES_ADVICE

        parts.append(
            f'<p class="warn">{len(blocked)} source object(s) cannot be created in either '
            "apply mode while another object holds the name on the target:</p>"
        )
        chips = " ".join(
            f'<span class="conflict">{esc(name)}: {esc(holder)}</span>'
            for name, holder in blocked
        )
        parts.append(f"<p>{chips}</p>")
        parts.append(f'<p class="muted">{esc(BLOCKED_NAMES_ADVICE)}</p>')
    if not conflicts and not blocked:
        parts.append(
            '<p class="muted">No source objects conflict with existing target '
            "objects.</p>"
        )
    parts.append("</section>")
    return "\n".join(parts)


def render_html_report(
    report: AssessmentReport,
    *,
    ai_report: Optional[AiAssessmentReport] = None,
    target: Optional[TargetInventory] = None,
    conflicts: Optional[list[str]] = None,
    source_type: SourceType = SourceType.MYSQL,
    blocked: "Optional[Sequence[tuple[str, str]]]" = None,
    target_endpoint: Optional[str] = None,
    metadata: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Render a styled, standalone HTML assessment report (English).

    The document is self-contained (inline CSS) so it can be saved and opened
    directly. It shows the classification and effort summaries and a per-object
    table with color-coded classifications. When ``ai_report`` is provided, an
    AI-led assessment section (strategy, per-object guidance, and additional
    findings) is appended as advisory content; the deterministic facts remain
    authoritative.
    """
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    source_engine = "PostgreSQL" if source_type is SourceType.POSTGRES else "MySQL"
    report_title = f"{source_engine} to Aurora DSQL Compatibility Assessment"

    classification_summary = "".join(
        f"<li>{c.value}: <strong>{report.summary.get(c, 0)}</strong></li>"
        for c in Classification
    )
    effort_summary = "".join(
        f"<li>{level.value}: <strong>{report.effort_summary.get(level, 0)}</strong></li>"
        for level in EffortLevel
    )

    rows = []
    for item in importance_order(report.assessed_items):
        color = _HTML_CLASS_COLOR.get(item.classification, "#ffffff")
        effort = item.effort.value if item.effort is not None else "-"
        # ONE ROW PER CONCERN, with the object/kind cells spanning them. A single row
        # holding a <ul> of risks beside a <ul> of fixes still asked the reader to count
        # list positions across two cells to pair them; a row per finding puts each risk
        # physically beside its own fix, its own rule id, class and effort. The object
        # name is not repeated -- rowspan keeps the grouping visible.
        concerns = list(item.concerns or [])
        # Filter attributes go on EVERY row of the group so hiding an object hides all of
        # its findings; ``data-concern`` marks the continuation rows so the "n of m shown"
        # counter still counts OBJECTS, not findings.
        attrs = (
            f'data-kind="{esc(item.kind)}" '
            f'data-classification="{esc(item.classification.value)}" '
            f'data-effort="{esc(effort)}" '
            f'data-name="{esc(item.object_name.lower())}"'
        )
        if not concerns:
            rows.append(
                f"<tr {attrs}>"
                f"<td>{esc(item.object_name)}</td>"
                f"<td>{esc(item.kind)}</td>"
                f'<td style="background:{color}">{esc(item.classification.value)}</td>'
                f"<td>{esc(effort)}</td>"
                f"<td>{esc(item.rule_id)}</td>"
                f"<td>{esc(item.risk)}</td>"
                f"<td>{esc(item.recommendation)}</td>"
                "</tr>"
            )
            continue
        span = f' rowspan="{len(concerns)}"' if len(concerns) > 1 else ""
        for index, concern in enumerate(concerns):
            concern_color = _HTML_CLASS_COLOR.get(concern.classification, "#ffffff")
            concern_effort = (
                concern.effort.value if concern.effort is not None else "-"
            )
            cells = ""
            if index == 0:
                cells += f"<td{span}>{esc(item.object_name)}</td>"
                cells += f"<td{span}>{esc(item.kind)}</td>"
            # An advisory finding reads RECOMMENDED on the calm info-blue background
            # instead of a severity on amber/red: the conversion is complete and correct,
            # so nothing about it is a defect. Its effort is marked as the cost of opting
            # in, matching its exclusion from the object's own estimate.
            if concern.is_advisory:
                concern_color = _HTML_ADVISORY_COLOR
                class_cell = "RECOMMENDED"
                if concern_effort != "-":
                    concern_effort = f"{concern_effort} (if taken)"
            else:
                class_cell = concern.classification.value
            cells += (
                f'<td style="background:{concern_color}">'
                f"{esc(class_cell)}</td>"
                f"<td>{esc(concern_effort)}</td>"
                f"<td>{esc(concern.rule_id)}</td>"
                f"<td>{esc(concern.risk)}</td>"
                f"<td>{esc(concern.recommendation)}</td>"
            )
            marker = "" if index == 0 else ' data-concern="1"'
            rows.append(f"<tr {attrs}{marker}>{cells}</tr>")
    table_rows = "\n".join(rows) if rows else (
        '<tr><td colspan="7">No objects were assessed.</td></tr>'
    )

    # Distinct kinds present, for the Kind filter dropdown.
    kinds_present = sorted({item.kind for item in report.assessed_items})

    def _options(values: "list[str]") -> str:
        return "".join(f'<option value="{esc(v)}">{esc(v)}</option>' for v in values)

    filter_bar = (
        '<div class="filters" id="assessed-filters">\n'
        '<label>Type <select id="f-kind"><option value="">All</option>'
        f"{_options(kinds_present)}</select></label>\n"
        '<label>Classification <select id="f-class"><option value="">All</option>'
        f"{_options([c.value for c in Classification])}</select></label>\n"
        '<label>Effort <select id="f-effort"><option value="">All</option>'
        f'{_options([level.value for level in EffortLevel] + ["-"])}</select></label>\n'
        '<label>Search <input id="f-search" type="search" '
        'placeholder="object name"></label>\n'
        '<span id="f-count" class="f-count"></span>\n'
        "</div>"
    )
    filter_script = (
        "<script>\n"
        "(function(){\n"
        "function apply(){\n"
        "var k=document.getElementById('f-kind').value;\n"
        "var c=document.getElementById('f-class').value;\n"
        "var e=document.getElementById('f-effort').value;\n"
        "var q=(document.getElementById('f-search').value||'').toLowerCase();\n"
        "var rows=document.querySelectorAll('#assessed-objects tbody tr[data-kind]');\n"
        "var shown=0;\n"
        "rows.forEach(function(r){\n"
        "var ok=(!k||r.dataset.kind===k)&&(!c||r.dataset.classification===c)"
        "&&(!e||r.dataset.effort===e)"
        "&&(!q||(r.dataset.name||'').indexOf(q)>=0);\n"
        # An object with several findings now spans several rows: hide/show them all
        # together, but count only the FIRST row of each group so the counter keeps
        # reading "n of m objects" rather than jumping to the number of findings.
        "r.style.display=ok?'':'none';\n"
        "if(ok&&!r.dataset.concern)shown++;});\n"
        "var total=document.querySelectorAll("
        "'#assessed-objects tbody tr[data-kind]:not([data-concern])').length;\n"
        "var cnt=document.getElementById('f-count');\n"
        "if(cnt)cnt.textContent=shown+' of '+total+' shown';\n"
        "}\n"
        "['f-kind','f-class','f-effort'].forEach(function(id){\n"
        "var el=document.getElementById(id);if(el)el.addEventListener('change',apply);});\n"
        "var s=document.getElementById('f-search');if(s)s.addEventListener('input',apply);\n"
        "apply();\n"
        "})();\n"
        "</script>"
    )

    ai_section = _render_ai_html_section(ai_report) if ai_report is not None else ""
    target_section = (
        _render_target_html_section(
            target, conflicts or [], blocked or (), target_endpoint
        )
        if target is not None
        else ""
    )
    chart_section = _render_html_chart(report)
    # What was assessed, against what, when and by which version: a shared copy has to
    # stand on its own.
    meta_section = (
        '<table class="meta">'
        + "".join(
            f"<tr><th>{esc(label)}</th><td>{esc(value)}</td></tr>" for label, value in metadata
        )
        + "</table>\n"
        if metadata
        else ""
    )

    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        f"<title>{report_title}</title>\n"
        "<style>\n"
        "body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
        "margin:24px;color:#222;}\n"
        "h1{font-size:22px;} h2{font-size:16px;margin-top:24px;} "
        "h3{font-size:14px;margin-top:16px;}\n"
        "ul{margin:4px 0;} li{margin:2px 0;}\n"
        "table{border-collapse:collapse;width:100%;margin-top:8px;font-size:13px;}\n"
        "th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;"
        "vertical-align:top;}\n"
        "th{background:#f5f5f5;}\n"
        ".legend{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0;font-size:12px;}\n"
        ".chip{display:inline-flex;align-items:center;gap:4px;}\n"
        ".chip i{width:12px;height:12px;border-radius:2px;display:inline-block;}\n"
        ".chart{display:flex;flex-direction:column;gap:6px;margin-top:8px;}\n"
        ".bar-row{display:flex;align-items:center;gap:8px;font-size:12px;}\n"
        ".bar-label{width:90px;flex:none;font-weight:600;}\n"
        ".bar{flex:1;display:flex;height:18px;border-radius:3px;overflow:hidden;"
        "background:#f0f0f0;}\n"
        ".bar span{display:inline-block;height:100%;}\n"
        ".bar-meta{width:150px;flex:none;color:#666;}\n"
        ".filters{display:flex;flex-wrap:wrap;gap:12px;align-items:center;"
        "margin:8px 0;font-size:13px;}\n"
        ".filters label{display:inline-flex;align-items:center;gap:4px;}\n"
        ".filters select,.filters input{font-size:13px;padding:2px 4px;}\n"
        ".f-count{color:#666;}\n"
        ".ai-report,.target-section{border:1px solid #e0e0e0;border-radius:6px;"
        "padding:12px 16px;margin-top:24px;background:#fafafa;}\n"
        ".ai-head{display:flex;align-items:center;gap:8px;}\n"
        ".ai-head h2{margin:0;} .ai-report h3,.target-section h3{margin-top:14px;}\n"
        ".ai-badge{font-size:11px;font-weight:600;color:#fff;background:#5b6dcd;"
        "border-radius:10px;padding:2px 8px;}\n"
        ".advisory{color:#666;font-size:12px;margin:4px 0 8px;}\n"
        ".ai-body{font-size:13px;} .ai-body h2{font-size:15px;} "
        ".ai-body h3{font-size:13px;} .ai-body ul{margin:4px 0;padding-left:20px;}\n"
        ".target-section .warn{color:#b35900;} .target-section .muted{color:#666;"
        "font-size:12px;}\n"
        ".conflict{display:inline-block;border:1px solid #e0a800;color:#8a6d00;"
        "border-radius:10px;padding:1px 8px;margin:2px;font-size:12px;}\n"
        "table.meta{width:auto;} table.meta th{width:9em;}\n"
        "</style>\n</head>\n<body>\n"
        f"<h1>{report_title}</h1>\n"
        f"{meta_section}"
        "<h2>Classification summary</h2>\n"
        f"<ul>{classification_summary}</ul>\n"
        "<h2>Estimated manual effort (non-automatic objects)</h2>\n"
        f"<ul>{effort_summary}</ul>\n"
        f"{chart_section}"
        "<h2>Assessed objects</h2>\n"
        f"{filter_bar}\n"
        '<table id="assessed-objects">\n<thead><tr>'
        "<th>Object</th><th>Kind</th><th>Classification</th><th>Effort</th>"
        "<th>Rule</th><th>Risk</th><th>Recommendation</th>"
        "</tr></thead>\n<tbody>\n"
        f"{table_rows}\n"
        "</tbody>\n</table>\n"
        f"{filter_script}\n"
        f"{target_section}\n"
        f"{ai_section}\n"
        "</body>\n</html>\n"
    )


__all__ = [
    "ObjectKey",
    "Finding",
    "Rule",
    "ForeignKeyRule",
    "TriggerRule",
    "ProcedureRule",
    "EventRule",
    "AutoIncrementRule",
    "NoPrimaryKeyRule",
    "ByteaKeyRule",
    "CaseInsensitiveCollationRule",
    "PartitionedTableRule",
    "SpatialTypeRule",
    "TooManyColumnsRule",
    "NameCollisionRule",
    "IdentifierLengthRule",
    "OversizedLobRule",
    "DecimalPrecisionRule",
    "EnumSetRule",
    "TinyIntBooleanRule",
    "BitTypeRule",
    "YearTypeRule",
    "GeneratedColumnRule",
    "AutoUpdateTimestampRule",
    "UnsupportedIndexTypeRule",
    "ViewCompatibilityRule",
    "InventoryRule",
    "check_multiple_source_databases",
    "check_pg_schema_count",
    "check_schema_count",
    "check_table_count",
    "check_view_count",
    "DSQL_DATABASE_LIMITS",
    "default_inventory_rules",
    "catalog_changed_items",
    "check_mixed_case_identifiers",
    "check_mysql_time_zone",
    "check_postgres_time_zone",
    "SOURCE_TIME_ZONE_RULE_ID",
    "pg_unindexable_column_rule",
    "pg_exclusion_constraint_rule",
    "pg_temporal_key_rule",
    "pg_deferrable_key_rule",
    "pg_not_enforced_foreign_key_rule",
    "foreign_key_clauses_kept",
    "default_dropped_rule",
    "importance_order",
    "default_rules",
    "CompatibilityAssessor",
    "render_text_report",
    "render_html_report",
    "export_report",
    "classification_stats_by_kind",
    "KIND_LABELS",
    "kind_label",
    "KIND_TABLE",
    "KIND_VIEW",
    "KIND_TRIGGER",
    "KIND_ROUTINE",
    "KIND_PROCEDURE",
    "KIND_FUNCTION",
    "KIND_DATABASE",
    "KIND_EVENT",
    "KIND_SEQUENCE",
    "PG_CATALOG_UNREAD_RULE_ID",
    "PG_CATALOG_CHECKED_RULE_ID",
    "PG_ROW_LEVEL_SECURITY_RULE_ID",
    "PG_STANDALONE_SEQUENCE_RULE_ID",
    "carry_probe_results",
    "with_object_findings",
]
