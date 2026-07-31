# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility assessment rule engine for MySQL -> Aurora DSQL migration.

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
from typing import Callable, ClassVar, Optional

from dsql_migrator.core.models import (
    AiAssessmentReport,
    AssessmentConcern,
    AssessmentItem,
    AssessmentReport,
    Classification,
    EffortLevel,
    ObjectType,
    SourceInventory,
    TargetInventory,
)

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

# DSQL limit: at most 1,000 tables per database (cluster quotas).
_MAX_TABLES_PER_DATABASE = 1000

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

# DSQL numeric maximum precision (numeric supports a precision of up to 38).
_MAX_NUMERIC_PRECISION = 38

# MySQL LOB/TEXT base types whose maximum size exceeds the DSQL text/bytea 1 MiB
# limit, so a large enough value cannot be stored and must be reviewed.
_OVERSIZED_LOB_BASES = frozenset(
    {"mediumtext", "longtext", "mediumblob", "longblob"}
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
    """Return ``True`` for ``TINYINT(1)`` (MySQL's boolean convention / BOOL)."""
    return bool(_TINYINT_ONE_RE.match(mysql_type or ""))


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


class ForeignKeyRule(Rule):
    """Flag tables that declare foreign keys (DSQL does not support them)."""

    rule_id = "FK_UNSUPPORTED"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            if table.foreign_keys:
                names = ", ".join(fk.name for fk in table.foreign_keys)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Foreign key constraints ({names}) are not "
                            "supported by Aurora DSQL."
                        ),
                        recommendation=(
                            "Remove the foreign key and enforce referential "
                            "integrity in the application layer."
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
    them, and DSQL has no foreign keys to re-perform the cascade on its own. The
    child rows are therefore left behind on the target with **no error and no
    warning** -- silently diverging while everything reports healthy.

    This is a limitation of every binlog-based CDC tool (Debezium, DMS, Maxwell),
    not of this migrator, but it is invisible unless it is called out BEFORE the
    stream starts -- hence flagging it here, at planning time, rather than leaving
    the operator to discover orphaned rows after cut-over.
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
                        "the binary log, so it cannot replicate them and Aurora DSQL "
                        "(no foreign keys) cannot re-perform them -- the child rows "
                        "are left behind on the target with no error or warning. "
                        "This affects every binlog-based CDC tool (MySQL bug #32506)."
                    ),
                    recommendation=(
                        "Before starting CDC, replace the automatic action with "
                        "EXPLICIT child-row statements in the application (e.g. "
                        "delete the children, then the parent) so the changes are "
                        "logged and replicated. You need this application logic on "
                        "DSQL anyway, since DSQL has no foreign keys to cascade for "
                        "you. Until then, enable the orphan-record check in "
                        "Validation and quiesce source writes before the final "
                        "cut-over comparison, so any divergence is caught."
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


class AutoIncrementRule(Rule):
    """Note tables whose AUTO_INCREMENT key is worth revisiting for throughput.

    This is THROUGHPUT ADVICE, not a compatibility gap. An AUTO_INCREMENT integer key
    converts cleanly and works correctly on DSQL -- nothing is dropped and no query
    returns a different answer. Moving to a UUID/random or cached-identity key buys
    insert throughput, because DSQL stores rows in primary-key order so a monotonic key
    concentrates writes on one partition.

    The wording therefore leads with what is true of the table ("converts cleanly")
    rather than with a consequence ("causes hot partitions"), which described a tuning
    opportunity as though it were a failure. Schema Conversion already made exactly this
    correction in v0.1.151 -- see ``ConversionNoteKind.RECOMMENDATION`` in
    ``core/converter.py``, which files the same condition as advice rather than a loss --
    and this rule was missed at the time, so the two screens contradicted each other
    about the same key.
    """

    rule_id = "AUTO_INCREMENT"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            column = table.auto_increment_column
            if column:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"The integer key from AUTO_INCREMENT column '{column}' "
                            "converts cleanly and works as-is. For higher insert "
                            "throughput, consider a different key: DSQL stores rows in "
                            "primary-key order, so a monotonically increasing key "
                            "concentrates writes on one partition."
                        ),
                        recommendation=(
                            "Optional, for throughput only: use a UUID/random key, or "
                            "an identity/sequence with cache tuning."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class NoPrimaryKeyRule(Rule):
    """Flag tables without a primary key (DSQL requires a primary key)."""

    rule_id = "NO_PRIMARY_KEY"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            if not table.primary_key:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk="Aurora DSQL requires every table to have a primary key.",
                        recommendation=(
                            "Add a primary key (e.g., a UUID/random key) before "
                            "migrating the table."
                        ),
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
    """Flag tables that use MySQL native partitioning."""

    rule_id = "PARTITIONED_TABLE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            if table.partitioned:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            "Manual partitioning is not used by Aurora DSQL, "
                            "which distributes data automatically."
                        ),
                        recommendation=(
                            "Remove the manual partitioning and rely on DSQL "
                            "automatic data distribution."
                        ),
                        effort=EffortLevel.MEDIUM,
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

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            count = len(table.indexes)
            if count <= _MAX_SECONDARY_INDEXES_PER_TABLE:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"The table has {count} secondary indexes; with its primary "
                        f"key that is {count + 1} against Aurora DSQL's limit of "
                        f"{_MAX_INDEXES_PER_TABLE} indexes per table (MySQL allows "
                        "64). The excess index fails with error 54000 \"more than "
                        f"{_MAX_INDEXES_PER_TABLE} indexes per table are not "
                        "allowed\" — and because secondary indexes are built AFTER "
                        "the data loads, that failure appears only once Full Load has "
                        "already written every row."
                    ),
                    recommendation=(
                        f"Drop indexes you no longer need so at most "
                        f"{_MAX_SECONDARY_INDEXES_PER_TABLE} secondary indexes remain "
                        "(unused or redundant indexes are common — check "
                        "sys.schema_unused_indexes on the source), or split the table. "
                        "Decide before loading: re-running Full Load will not clear "
                        "this, since the limit is not transient."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class OversizedLobRule(Rule):
    """Flag columns whose MySQL LOB/TEXT type can exceed the DSQL 1 MiB limit."""

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
                        risk=(
                            f"Columns ({names}) use a large MySQL LOB/TEXT type "
                            "whose values can exceed the Aurora DSQL 1 MiB limit "
                            "for text/bytea; an oversized value fails to load."
                        ),
                        recommendation=(
                            "Confirm no value exceeds 1 MiB, or move large objects "
                            "to external storage (e.g. Amazon S3) and store a "
                            "reference instead."
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
                            "Reduce the precision to 38 digits or fewer, or store "
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
            columns = [
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) in _ENUM_SET_BASES
            ]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) use MySQL ENUM/SET, for which "
                            "Aurora DSQL has no native type; the allowed-value "
                            "constraint is lost when the column is mapped to text."
                        ),
                        recommendation=(
                            "Re-enforce the allowed values in the application "
                            "layer (or with a CHECK constraint if supported)."
                        ),
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
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column.name
                for column in table.columns
                if _is_tinyint_one(column.mysql_type)
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


class GeneratedColumnRule(Rule):
    """Flag MySQL generated/computed columns for review."""

    rule_id = "GENERATED_COLUMN"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [c.name for c in table.columns if c.generated]
            if columns:
                names = ", ".join(columns)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({names}) are MySQL generated/computed "
                            "columns; their generation expression is not "
                            "auto-converted to Aurora DSQL."
                        ),
                        recommendation=(
                            "Recreate as a PostgreSQL GENERATED column if "
                            "supported on the target, or compute the value in "
                            "the application."
                        ),
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
    """Flag FULLTEXT/SPATIAL indexes (no Aurora DSQL equivalent)."""

    rule_id = "UNSUPPORTED_INDEX_TYPE"

    def evaluate(self, inventory: SourceInventory) -> list[Finding]:
        findings: list[Finding] = []
        for table in inventory.tables:
            flagged = [
                f"{index.name} ({index.index_type})"
                for index in table.indexes
                if index.index_type
                and index.index_type.lower() in _UNSUPPORTED_INDEX_TYPES
            ]
            if flagged:
                names = ", ".join(flagged)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.UNSUPPORTED,
                        risk=(
                            f"Indexes ({names}) use a MySQL index type Aurora "
                            "DSQL does not support (FULLTEXT/SPATIAL)."
                        ),
                        recommendation=(
                            "Drop the index and implement full-text/spatial "
                            "search outside the database (e.g. Amazon OpenSearch "
                            "Service), or redesign the query."
                        ),
                        effort=EffortLevel.SIGNIFICANT,
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
            if not matches:
                continue
            patterns = sorted({match.pattern.value for match in matches})
            findings.append(
                Finding(
                    object=ObjectKey(KIND_VIEW, view.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        "The view definition uses DSQL-unsupported or risky "
                        f"constructs ({', '.join(patterns)})."
                    ),
                    recommendation=(
                        "Rewrite the view to avoid these constructs; see the "
                        "application anti-pattern report for the exact locations."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


# ---------------------------------------------------------------------------
# Inventory-level (cluster/database-wide) checks -- not tied to one object
# ---------------------------------------------------------------------------

# An inventory rule classifies the source as a whole (e.g. spanning multiple
# databases) rather than a single object; it returns ready-made items.
InventoryRule = Callable[[SourceInventory], list[AssessmentItem]]


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


def check_multiple_source_databases(
    inventory: SourceInventory,
) -> list[AssessmentItem]:
    """Flag a source that spans multiple databases (DSQL has one DB per cluster)."""
    databases = _source_databases(inventory)
    if len(databases) <= 1:
        return []
    names = ", ".join(databases)
    return [
        AssessmentItem(
            object_name=f"{len(databases)} source databases",
            rule_id="MULTIPLE_DATABASES",
            classification=Classification.MANUAL,
            risk=(
                f"The source spans {len(databases)} databases ({names}); Aurora "
                "DSQL provides a single database per cluster."
            ),
            recommendation=(
                "Consolidate the databases into one cluster using separate "
                "schemas, or provision a separate Aurora DSQL cluster per "
                "database. Resolve any cross-database name collisions."
            ),
            effort=EffortLevel.MEDIUM,
            kind=KIND_DATABASE.upper(),
        )
    ]


def check_table_count(inventory: SourceInventory) -> list[AssessmentItem]:
    """Flag a source whose table count exceeds the DSQL per-database limit."""
    count = len(inventory.tables)
    if count <= _MAX_TABLES_PER_DATABASE:
        return []
    return [
        AssessmentItem(
            object_name=f"{count} tables",
            rule_id="TABLE_COUNT_LIMIT",
            classification=Classification.UNSUPPORTED,
            risk=(
                f"The source has {count} tables; Aurora DSQL allows at most "
                f"{_MAX_TABLES_PER_DATABASE} tables per database."
            ),
            recommendation=(
                "Reduce the table count or split the workload across multiple "
                "Aurora DSQL clusters before migrating."
            ),
            effort=EffortLevel.SIGNIFICANT,
            kind=KIND_DATABASE.upper(),
        )
    ]


def default_inventory_rules() -> list[InventoryRule]:
    """Return the default inventory-level (cluster-wide) checks."""
    return [check_multiple_source_databases, check_table_count]


def default_rules() -> list[Rule]:
    """Return the default, ordered list of compatibility rules.

    The order is significant: it breaks ties when several rules of equal
    severity match the same object.
    """
    return [
        ForeignKeyRule(),
        CascadeForeignKeyRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        AutoIncrementRule(),
        NoPrimaryKeyRule(),
        CaseInsensitiveCollationRule(),
        PartitionedTableRule(),
        SpatialTypeRule(),
        TooManyColumnsRule(),
        TooManyIndexesRule(),
        OversizedLobRule(),
        DecimalPrecisionRule(),
        EnumSetRule(),
        TinyIntBooleanRule(),
        BitTypeRule(),
        YearTypeRule(),
        GeneratedColumnRule(),
        AutoUpdateTimestampRule(),
        UnsupportedIndexTypeRule(),
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

    The most severe classification governs the item; risks and recommendations
    of every matched rule are combined so no finding is lost. Objects with no
    findings are classified ``AUTO``.
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

    # Stable sort by descending severity keeps declaration order for ties.
    ordered = sorted(findings, key=lambda f: -_SEVERITY[f.classification])
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
        )
        for f in ordered
    ]
    risk = "; ".join(f.risk for f in ordered if f.risk)
    recommendation = "; ".join(f.recommendation for f in ordered if f.recommendation)
    # The most demanding effort across all matched rules governs the estimate.
    efforts = [f.effort for f in findings if f.effort is not None]
    effort = max(efforts, key=lambda e: _EFFORT_ORDER[e]) if efforts else None
    return AssessmentItem(
        object_name=key.name,
        rule_id=governing.rule_id,
        classification=governing.classification,
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
    ) -> None:
        """Create an assessor.

        ``rules`` overrides the per-object rule list, enabling extension or
        customization. The order of the rules breaks classification ties.
        ``inventory_rules`` overrides the cluster/database-wide checks (e.g.
        multiple source databases, table-count limit) that produce findings not
        tied to a single object.
        """
        self._rules: list[Rule] = list(rules) if rules is not None else default_rules()
        self._inventory_rules: list[InventoryRule] = (
            list(inventory_rules)
            if inventory_rules is not None
            else default_inventory_rules()
        )

    def assess(self, inventory: SourceInventory) -> AssessmentReport:
        """Assess ``inventory`` and return a complete report.

        Guarantees Property 8: every inventory object appears in exactly one
        per-object :class:`AssessmentItem`, so no object is left unclassified.
        Inventory-level (cluster/database-wide) checks may append additional
        items (kind ``DATABASE``) that are not tied to a single object.
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
            items.extend(inventory_rule(inventory))
        return AssessmentReport.from_items(items)


# ---------------------------------------------------------------------------
# Report export
# ---------------------------------------------------------------------------


def render_text_report(report: AssessmentReport) -> str:
    """Render a human-readable text version of an assessment report (English)."""
    lines = ["Compatibility Assessment Report", "=" * 31, ""]
    lines.append("Difficulty summary (objects by classification):")
    for classification in Classification:
        lines.append(f"  {classification.value}: {report.summary.get(classification, 0)}")
    lines.append("")
    lines.append("Estimated manual effort (non-automatic objects):")
    for level in EffortLevel:
        lines.append(f"  {level.value}: {report.effort_summary.get(level, 0)}")
    lines.append("")
    lines.append(f"Items ({len(report.items)}):")
    for item in report.items:
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
                head = f"    {number}. [{concern.classification.value}] {concern.rule_id}"
                if concern.effort is not None:
                    head += f" (effort: {concern.effort.value})"
                lines.append(head)
                if concern.risk:
                    lines.append(f"       Risk: {concern.risk}")
                if concern.recommendation:
                    lines.append(f"       Fix:  {concern.recommendation}")
        else:
            if item.risk:
                lines.append(f"    Risk: {item.risk}")
            if item.recommendation:
                lines.append(f"    Recommendation: {item.recommendation}")
    return "\n".join(lines)


def export_report(report: AssessmentReport, fmt: str = "json") -> str:
    """Export an assessment report as ``"json"``, ``"text"``, or ``"html"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable);
    text is a readable summary; HTML is a styled, standalone document suitable
    for sharing. Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        return report.model_dump_json(indent=2)
    if normalized == "text":
        return render_text_report(report)
    if normalized == "html":
        return render_html_report(report)
    raise ValueError(
        f"unsupported report format: {fmt!r} (use 'json', 'text', or 'html')"
    )


# Background colors for the classification cell in the HTML report.
_HTML_CLASS_COLOR: dict[Classification, str] = {
    Classification.AUTO: "#e8f5e9",
    Classification.MANUAL: "#fff8e1",
    Classification.UNSUPPORTED: "#ffebee",
}

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
    Classification.AUTO: "Auto-converted",
    Classification.MANUAL: "Review needed",
    Classification.UNSUPPORTED: "Unsupported",
}

_CHART_CLASS_COLORS: dict[Classification, str] = {
    Classification.AUTO: "#2e7d32",
    Classification.MANUAL: "#ef6c00",
    Classification.UNSUPPORTED: "#c62828",
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
    for item in report.items:
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
            meta += f" &middot; {round(attention / total * 100)}% need attention"
        rows.append(
            '<div class="bar-row">'
            f'<div class="bar-label">{html.escape(kind)}</div>'
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
    target: TargetInventory, conflicts: "list[str]"
) -> str:
    """Render the Target analysis (Aurora DSQL) section for the HTML export.

    Mirrors the on-screen Target analysis: the target catalog summary (schemas /
    tables / views) and any source objects that already exist on the target and
    may conflict when applying converted DDL.
    """
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    table_count = sum(len(schema.tables) for schema in target.schemas)
    view_count = sum(len(schema.views) for schema in target.schemas)
    parts = [
        '<section class="target-section">',
        "<h2>Target analysis (Aurora DSQL)</h2>",
        f"<p>Target catalog: <strong>{len(target.schemas)}</strong> schemas, "
        f"<strong>{table_count}</strong> tables, <strong>{view_count}</strong> "
        "views.</p>",
    ]
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
    else:
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

    classification_summary = "".join(
        f"<li>{c.value}: <strong>{report.summary.get(c, 0)}</strong></li>"
        for c in Classification
    )
    effort_summary = "".join(
        f"<li>{level.value}: <strong>{report.effort_summary.get(level, 0)}</strong></li>"
        for level in EffortLevel
    )

    rows = []
    for item in report.items:
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
            cells += (
                f'<td style="background:{concern_color}">'
                f"{esc(concern.classification.value)}</td>"
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
    kinds_present = sorted({item.kind for item in report.items})

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
        _render_target_html_section(target, conflicts or [])
        if target is not None
        else ""
    )
    chart_section = _render_html_chart(report)

    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        "<title>MySQL to Aurora DSQL Compatibility Assessment</title>\n"
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
        "</style>\n</head>\n<body>\n"
        "<h1>MySQL to Aurora DSQL Compatibility Assessment</h1>\n"
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
    "CaseInsensitiveCollationRule",
    "PartitionedTableRule",
    "SpatialTypeRule",
    "TooManyColumnsRule",
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
    "check_table_count",
    "default_inventory_rules",
    "default_rules",
    "CompatibilityAssessor",
    "render_text_report",
    "render_html_report",
    "export_report",
    "classification_stats_by_kind",
    "KIND_TABLE",
    "KIND_VIEW",
    "KIND_TRIGGER",
    "KIND_ROUTINE",
    "KIND_PROCEDURE",
    "KIND_FUNCTION",
    "KIND_DATABASE",
    "KIND_EVENT",
]
