# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source compatibility rules for the Evaluation step.

Kept in its own module -- not tangled into ``assessor.py``'s MySQL rules -- so each
source engine's assessment rules stay separate.

v1 is the **source-neutral, target-DSQL structural** rule set: foreign keys, check
constraints, triggers / procedures / events, missing primary key, partitioning, and the
DSQL column / index / key-column-count limits. These read structural inventory fields
(not source type strings), so they are correct for a PostgreSQL source as-is.

Excluded (all inspect MySQL specifics, so they would misfire or mislead on a PostgreSQL
source): the MySQL type/feature rules (ENUM/SET, TINYINT(1), BIT, YEAR, MySQL collation,
MySQL spatial, ON UPDATE CURRENT_TIMESTAMP, MySQL index types), the MySQL-binlog
CDC cascade rule (its guidance is entirely MySQL/Debezium-framed; PostgreSQL CDC uses its
own logical-replication readiness checks instead), and the view rule (its linter targets
MySQL application-query anti-patterns).

Note what is NOT excluded, and why -- the distinction that matters here is whether a rule
reads a MySQL TYPE STRING or a structural field:

* ``DecimalPrecisionRule`` is shared and included. It was once excluded as "DECIMAL
  precision parsed from a MySQL type", but ``_DECIMAL_BASES`` already contains
  ``numeric`` and the precision comes from the first parenthesised integer, so
  ``numeric(40,10)`` is caught and the message names only the DSQL limit.
* ``GeneratedColumnRule`` and ``AutoIncrementRule`` read pure structural fields that PG
  introspection populates (``ColumnDef.generated`` from ``attgenerated``,
  ``TableDef.auto_increment_column`` for a sequence/identity PK), so the CONDITION is
  engine-neutral and only the MySQL prose was wrong. They are replaced here by
  :class:`PgGeneratedColumnRule` / :class:`PgIdentityKeyRule`, which keep the rule ids and
  mirror the Schema Conversion wording for the same condition -- the same
  separate-PG-variant shape ``converter._pg_generated_column_warning`` already uses.
* ``OversizedLobRule`` needed BOTH: the 1 MiB cap is a TARGET limit that applies to
  PostgreSQL ``text``/``bytea``/``json``/``jsonb`` just as much, but the rule matches MySQL
  type NAMES, so registering it here would have found nothing forever.
  :class:`PgOversizedLobRule` keeps the ``OVERSIZED_LOB`` id and reads the shared
  ``assessor.is_pg_oversized_lob_type`` predicate, which Schema Conversion and the UI's
  exclusion offer also use -- one definition, three layers.

The DSQL-unsupported PostgreSQL TYPE rule IS included (``UnsupportedPostgresTypeRule``,
below) so Evaluation flags an unsupported column type the same as Schema Conversion does.
GIN/GiST/BRIN index-method notes and stored trigger/function/event flagging (which depends
on PostgreSQL-catalog enrichment) are still later refinements.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dsql_migrator.core.assessor import KIND_TABLE, KIND_VIEW, Finding, ObjectKey, Rule
from dsql_migrator.core.models import Classification, ConversionNoteKind, EffortLevel

if TYPE_CHECKING:
    from dsql_migrator.core.models import SourceInventory


class UnsupportedPostgresTypeRule(Rule):
    """Flag columns whose PostgreSQL type Aurora DSQL does not support as a column type.

    Surfaces at Evaluation (Step 1) the SAME DSQL-unsupported PG column types the Schema
    Conversion step warns about via ``unsupported_dsql_reason`` (arrays, geometric,
    network, xml, money, bit, range, tsvector, enum/composite, pgvector). Without it a
    table using such a type reads AUTO at Evaluation and the problem only appears at
    Schema Conversion. Reuses that single source of truth so the two steps never drift.
    PostgreSQL-source only.
    """

    rule_id = "PG_UNSUPPORTED_TYPE"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

        findings: list[Finding] = []
        for table in inventory.tables:
            bad = [
                (col.name, col.mysql_type)
                for col in table.columns
                if unsupported_dsql_reason(col.mysql_type) is not None
            ]
            if not bad:
                continue
            cols = ", ".join(f"{name} ({typ})" for name, typ in bad)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.UNSUPPORTED,
                    risk=(
                        f"Column(s) {cols} use PostgreSQL types Aurora DSQL does not "
                        "support as column types, so this table's CREATE would be "
                        "rejected as-is."
                    ),
                    recommendation=(
                        "Remodel each to a DSQL-supported type before migrating; Schema "
                        "Conversion names the target per type (array -> jsonb or a child "
                        "table; inet/cidr/xml/tsvector/bit -> text; money -> numeric; "
                        "range -> text; enum -> text; composite -> columns or jsonb)."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class UnsupportedRelationRule(Rule):
    """Flag PostgreSQL materialized views and foreign tables (no Aurora DSQL equivalent).

    Introspection carries these (relkinds 'm'/'f') as :class:`ViewDef`s flagged via
    ``unsupported_kind`` -- ``get_view_names`` / ``get_table_names`` return neither, so
    without capturing them they would be silently absent from the migration. Aurora DSQL
    supports neither a materialized view nor a foreign table, so each is UNSUPPORTED (a
    target has to be built by hand). PostgreSQL-source only; a plain view (unsupported_kind
    None) is untouched.
    """

    rule_id = "PG_UNSUPPORTED_RELATION"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        findings: list[Finding] = []
        for view in inventory.views:
            kind = getattr(view, "unsupported_kind", None)
            if not kind:
                continue
            if kind == "materialized view":
                recommendation = (
                    "Reimplement it as a plain table that the application (or a scheduled "
                    "job) refreshes, since Aurora DSQL has no REFRESH MATERIALIZED VIEW."
                )
            else:
                recommendation = (
                    "Access the external data from the application or land it into a "
                    "regular DSQL table, since Aurora DSQL has no foreign-data wrapper."
                )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_VIEW, view.name),
                    rule_id=self.rule_id,
                    classification=Classification.UNSUPPORTED,
                    risk=(
                        f"'{view.name}' is a PostgreSQL {kind}, which Aurora DSQL does "
                        "not support; there is no target object to migrate it into."
                    ),
                    recommendation=recommendation,
                    effort=EffortLevel.SIGNIFICANT,
                )
            )
        return findings


class PgGeneratedColumnRule(Rule):
    """Flag PostgreSQL generated (computed) columns at Evaluation.

    Shares the rule id with the MySQL ``GeneratedColumnRule`` but not its wording: that
    one names MySQL and recommends "recreate as a PostgreSQL GENERATED column", which is
    meaningless advice for a source that already IS PostgreSQL and a target that has no
    such column. The condition itself is engine-neutral (``ColumnDef.generated``, which
    PG introspection sets from ``pg_attribute.attgenerated``), so the only thing that
    kept PostgreSQL silent here was the MySQL text.

    Mirrors :func:`converter._pg_generated_column_warning` -- the same condition already
    surfaces at Schema Conversion -- so the go/no-go Evaluation report and the conversion
    preview no longer disagree about the same column.
    """

    rule_id = "GENERATED_COLUMN"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [column.name for column in table.columns if column.generated]
            if not columns:
                continue
            names = ", ".join(columns)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"Columns ({names}) are PostgreSQL generated (computed) columns "
                        "(STORED or VIRTUAL); Aurora DSQL has no equivalent, so they "
                        "become ORDINARY columns. Full Load copies the values the source "
                        "already computed, so the target starts correct -- but nothing "
                        "maintains them afterwards, and the CDC stream does not carry "
                        "them either, so any write that does not supply the value drifts."
                    ),
                    recommendation=(
                        "Compute the value in the application (or in the query) before "
                        "cut over; read the generating expression from the source, since "
                        "it is not carried over."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


class PgOversizedLobRule(Rule):
    """Flag PostgreSQL columns with no length limit, whose values can exceed 1 MiB.

    Shares the rule id with the MySQL ``OversizedLobRule`` but not its type set: that one
    matches ``mediumtext``/``longblob``, which a PostgreSQL inventory never contains
    (``column.mysql_type`` holds ``format_type`` output), so REGISTERING the shared rule
    here would have reported zero findings forever. The 1 MiB cap is a TARGET (DSQL) limit
    and applies to PostgreSQL ``text``/``bytea``/``json``/``jsonb`` and an unbounded
    ``varchar`` identically -- the reason the rule was excluded was the MySQL type names,
    not the risk being MySQL-specific.

    This matters more than the MySQL case, not less: PostgreSQL ``text``/``bytea`` are
    unbounded by DEFAULT, and this is the one DSQL limit whose breach cannot be undone by
    reloading -- the value simply does not fit, so the row is quarantined (Full Load) or
    dead-lettered (CDC). The manual documents ``OVERSIZED_LOB`` as the flag for exactly
    this, naming PostgreSQL explicitly, so a PostgreSQL source reading AUTO here left the
    documented signal with nothing behind it.
    """

    rule_id = "OVERSIZED_LOB"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.assessor import (
            pg_lob_is_deliberately_large,
            pg_oversized_lob_column_names,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            # A CHECK that limits the column to a finite literal set is an exclusion, not
            # just noise-reduction: such a column CANNOT hold an oversized value, and
            # reporting it both states something false and buries the genuine one.
            at_risk = set(pg_oversized_lob_column_names(table))
            by_type = {column.name: column.mysql_type for column in table.columns}
            # Split by what the TYPE declares, and emit one finding per family present, so a
            # genuinely-large column is never averaged in with an idiomatic short string.
            # Grading them together is what went wrong twice: as one LOSS an 18-table schema
            # of ordinary `text` read per-table manual work, and as one RECOMMENDATION a
            # bytea holding 1,114,112 bytes read "Ready" while MySQL's identical longblob
            # read "Moderate effort".
            groups = [
                (
                    [n for n in sorted(at_risk) if pg_lob_is_deliberately_large(by_type[n])],
                    ConversionNoteKind.LOSS,
                    (
                        "are binary/document columns with no length limit, so a value can "
                        "exceed the Aurora DSQL 1 MiB per-value limit"
                    ),
                ),
                (
                    [
                        n
                        for n in sorted(at_risk)
                        if not pg_lob_is_deliberately_large(by_type[n])
                    ],
                    ConversionNoteKind.RECOMMENDATION,
                    (
                        "are unbounded character columns, so a value CAN exceed the Aurora "
                        "DSQL 1 MiB per-value limit -- though an unbounded text/varchar is "
                        "PostgreSQL's idiomatic spelling for an ordinary short string, so "
                        "this is a ceiling to confirm rather than work to budget"
                    ),
                ),
            ]
            for names, note_kind, clause in groups:
                if not names:
                    continue
                listed = ", ".join(f"{n} ({by_type[n]})" for n in names)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({listed}) {clause}. An oversized value cannot be "
                            "stored: the row is quarantined during Full Load or "
                            "dead-lettered during CDC, and reloading cannot fix it."
                        ),
                        recommendation=(
                            "Check the largest value in each column. If any exceeds 1 MiB, "
                            "move that content to external storage (e.g. Amazon S3) and "
                            "store a reference instead, or exclude the column on the Data "
                            "Migration step. For json/jsonb and text the limit applies to "
                            "the COMPRESSED size, so a highly compressible document may "
                            "still fit."
                        ),
                        effort=EffortLevel.MEDIUM,
                        note_kind=note_kind,
                    )
                )
        return findings


class PgNonKeySequenceRule(Rule):
    """Flag a NON-primary-key ``serial`` / ``GENERATED AS IDENTITY`` column at Evaluation.

    The same contradiction :class:`PgIdentityKeyRule` was written to close, one column over.
    Schema Conversion warns that such a column reaches the target with neither identity nor
    default -- so an INSERT that omits it writes NULL instead of the next number -- while
    Evaluation said nothing at all about it, because its identity rule only looks at
    ``table.auto_increment_column``, which PostgreSQL enrichment sets for a PRIMARY KEY only.
    The go/no-go artifact and the conversion preview therefore disagreed about the same
    column.

    Graded a LOSS, unlike the key rule's RECOMMENDATION: for the KEY column the operator gets
    a CHOICE at Schema Conversion (Server-generated IDENTITY fills it), but for a non-key
    column there is no such offer -- the value generation is simply gone and the application
    must supply it.
    """

    rule_id = "NON_KEY_SEQUENCE"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        findings: list[Finding] = []
        for table in inventory.tables:
            key_columns = set(table.primary_key or ())
            if table.auto_increment_column:
                key_columns.add(table.auto_increment_column)
            columns = [
                column.name
                for column in table.columns
                if column.name not in key_columns
                and (
                    column.identity
                    or "nextval(" in (column.default or "").lower()
                )
            ]
            if not columns:
                continue
            names = ", ".join(columns)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"Columns ({names}) take their value from a sequence (serial / "
                        "GENERATED AS IDENTITY) but are NOT the primary key. Aurora DSQL has "
                        "no source sequence to point at, and the primary-key strategy "
                        "generates values only for the key column, so each lands on the "
                        "target with neither identity nor default: an INSERT that omits it "
                        "writes NULL instead of the next number."
                    ),
                    recommendation=(
                        "Supply the value from the application, or add an identity to the "
                        "column on the target before cutting over. Already-loaded rows are "
                        "unaffected -- Full Load copies the existing values."
                    ),
                    effort=EffortLevel.MEDIUM,
                    note_kind=ConversionNoteKind.LOSS,
                )
            )
        return findings


class PgIdentityKeyRule(Rule):
    """Throughput advice for a serial / ``GENERATED AS IDENTITY`` primary key.

    The PostgreSQL counterpart of the MySQL ``AutoIncrementRule``: same condition
    (``TableDef.auto_increment_column``, which PG introspection sets for a PK backed by a
    sequence or an identity attribute) and same advice, but worded for the mechanism the
    source actually has. Schema Conversion already gives this recommendation for a
    PostgreSQL source, so without it the two screens differed for no reason.

    ADVICE, not a compatibility gap -- an integer key converts cleanly and returns the
    same answers; a different key only buys insert throughput, because DSQL stores rows
    in primary-key order and a monotonic key concentrates writes on one partition. Filed
    as a RECOMMENDATION so it is not presented as a loss.
    """

    rule_id = "AUTO_INCREMENT"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        findings: list[Finding] = []
        for table in inventory.tables:
            column = table.auto_increment_column
            if not column:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"The integer key from serial / identity column '{column}' "
                        "converts cleanly, but Aurora DSQL will NOT generate it: the "
                        "default conversion keeps a plain integer with no identity and no "
                        "sequence, so the APPLICATION must supply the value on every insert "
                        "— code that relied on the database generating it fails or "
                        "collides. Separately, for higher insert throughput consider a "
                        "different key: DSQL stores rows in primary-key order, so a "
                        "monotonically increasing key concentrates writes on one partition."
                    ),
                    recommendation=(
                        "Decide at Schema Conversion who generates the key: choose the "
                        "'Server-generated (IDENTITY)' strategy to have DSQL fill it (the "
                        "column is widened to bigint, which DSQL requires for an identity), "
                        "or keep the plain integer and supply the value from the "
                        "application. Optional, for throughput only: a UUID/random key or a "
                        "cached identity spreads the writes."
                    ),
                    effort=EffortLevel.MEDIUM,
                    note_kind=ConversionNoteKind.RECOMMENDATION,
                )
            )
        return findings


def default_rules() -> "list[Rule]":
    """Ordered PostgreSQL-source compatibility rules (v1: structural, source-neutral).

    Reuses the shared, source-neutral rule classes from :mod:`assessor` (imported
    lazily to avoid an import cycle: ``assessor.default_rules`` delegates here). Order
    is significant -- it breaks classification ties -- and mirrors the shared rules'
    order in the MySQL list.
    """
    from dsql_migrator.core.assessor import (
        CheckConstraintRule,
        DecimalPrecisionRule,
        EventRule,
        ForeignKeyRule,
        NoPrimaryKeyRule,
        PartitionedTableRule,
        ProcedureRule,
        TooManyColumnsRule,
        TooManyIndexesRule,
        TooManyKeyColumnsRule,
        TriggerRule,
    )

    return [
        # PG-specific: DSQL-unsupported column types (the only UNSUPPORTED-level rule here;
        # first so it is prominent, though severity ordering already makes it win ties).
        UnsupportedPostgresTypeRule(),
        # PG-specific: materialized views / foreign tables (no DSQL equivalent).
        UnsupportedRelationRule(),
        # PG-worded variants of shared conditions whose MySQL text/type set cannot be
        # reused. The oversized-LOB one is first of the three: it is the only DSQL limit
        # here whose breach cannot be undone by reloading.
        PgOversizedLobRule(),
        PgGeneratedColumnRule(),
        PgIdentityKeyRule(),
        PgNonKeySequenceRule(),
        ForeignKeyRule(),
        CheckConstraintRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        NoPrimaryKeyRule(),
        PartitionedTableRule(),
        TooManyColumnsRule(),
        TooManyIndexesRule(),
        TooManyKeyColumnsRule(),
        # Shared, and correct as-is for PostgreSQL: _DECIMAL_BASES already contains
        # "numeric" and the precision is parsed from the first parenthesised integer, so
        # numeric(40,10) is caught, and the message names only the DSQL limit -- no MySQL
        # wording. Previously excluded as "DECIMAL precision parsed from a MySQL type",
        # which left a numeric that Schema Conversion will CLAMP reading AUTO at
        # Evaluation, i.e. the go/no-go report understated a lossy conversion.
        DecimalPrecisionRule(),
    ]


__all__ = ["default_rules"]
