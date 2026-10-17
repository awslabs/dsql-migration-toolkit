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
  type NAMES, so registering it here found nothing (today it would match only ``json``,
  the one spelling the engines share, and word it for MySQL).
  :class:`PgOversizedLobRule` keeps the ``OVERSIZED_LOB`` id and reads the shared
  ``assessor.is_pg_oversized_lob_type`` predicate, which Schema Conversion and the UI's
  exclusion offer also use -- one definition, three layers.

The DSQL-unsupported PostgreSQL TYPE rule IS included (``UnsupportedPostgresTypeRule``,
below) so Evaluation flags an unsupported column type the same as Schema Conversion does.
GIN/GiST/BRIN index-method notes and stored trigger/function/event flagging (which depends
on PostgreSQL-catalog enrichment) are still later refinements.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional, Sequence

from dsql_migrator.core.assessor import (
    KIND_DATABASE,
    KIND_SEQUENCE,
    KIND_TABLE,
    KIND_VIEW,
    PG_CATALOG_CHECKED_RULE_ID,
    PG_CATALOG_UNREAD_RULE_ID,
    PG_ROW_LEVEL_SECURITY_RULE_ID,
    PG_STANDALONE_SEQUENCE_RULE_ID,
    Finding,
    ObjectKey,
    Rule,
    _single_concern,
    with_object_findings,
)
from dsql_migrator.core.catalog_postgres import (
    PgCatalogFacts,
    RowSecurityFacts,
    RowSecurityPolicy,
    RowSecurityTable,
    SourceLogin,
    StandaloneSequence,
    row_security_exemption,
)
from dsql_migrator.core.models import (
    AssessmentItem,
    AssessmentReport,
    Classification,
    ConversionNoteKind,
    EffortLevel,
)

if TYPE_CHECKING:
    from dsql_migrator.core.models import SourceInventory


# How many columns one finding spells out in full before it summarises the rest. A wide
# table can carry dozens of unsupported columns, and the finding is rendered into a UI
# card, a text report and an HTML report -- a message that grows without bound is unusable
# in all three. The remaining columns are still COUNTED and named, just without their
# individual reason.
_MAX_DETAILED_COLUMNS = 6


def _render_bad_columns(bad: "list[tuple[str, object, str]]") -> str:
    """Name each offending column and its TYPE, saying which is which. Pure.

    The type of a user-defined PostgreSQL enum or composite is schema-qualified
    (``ecommerce.order_status``), and this tool writes ``schema.table`` everywhere else --
    so the original ``status (ecommerce.order_status)`` was read as a reference to another
    TABLE, and an operator asked why a different table appeared in this one's finding.
    Naming the kind is what removes the ambiguity; parentheses alone cannot.
    """
    return ", ".join(
        f"column \"{name}\" of type {typ}" + (f" (a user-defined {kind} type)" if kind else "")
        for name, typ, _reason, kind in bad
    )


def _render_remodel_guidance(bad: "list[tuple[str, object, str, object]]") -> str:
    """One actionable line per column, from the reason already computed for its type.

    Bounded: the first :data:`_MAX_DETAILED_COLUMNS` get their own reason and the rest are
    named with a count, so a very wide table cannot produce an unbounded message.
    """
    lines = [
        f"{name}: {reason}"
        for name, _typ, reason, _kind in bad[:_MAX_DETAILED_COLUMNS]
    ]
    rest = bad[_MAX_DETAILED_COLUMNS:]
    if rest:
        names = ", ".join(name for name, _typ, _reason, _kind in rest)
        lines.append(
            f"The same applies to {len(rest)} more column(s) ({names}) -- Schema "
            "Conversion states the target type for each."
        )
    return " ".join(lines)


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
        from dsql_migrator.core.converter_postgres import (
            substitute_pg_unsupported_type,
            unsupported_dsql_reason,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            bad = []
            retyped = []
            guidance = []  # every such column's own remodel line, in table order
            for col in table.columns:
                reason = unsupported_dsql_reason(
                    col.mysql_type,
                    type_kind=col.type_kind,
                    enum_labels=col.enum_labels,
                )
                if reason is None:
                    continue
                # What Schema Conversion does with the column decides the grade, as it
                # decides the conversion's: a type it re-types (an array as jsonb, an enum
                # as text with a CHECK, inet as text, money as numeric) applies -- the
                # application adapts to the new type -- while one with no stand-in (a
                # composite, a domain, a label-less enum) is rejected as-is.
                target = (
                    "text" if col.type_kind == "enum" and col.enum_labels
                    else substitute_pg_unsupported_type(col.mysql_type, type_kind=col.type_kind)[0]
                )
                if target is not None:
                    retyped.append((col.name, col.mysql_type, target))
                    guidance.append((col.name, col.mysql_type, reason, col.type_kind))
                else:
                    bad.append((col.name, col.mysql_type, reason, col.type_kind))
                    guidance.append((col.name, col.mysql_type, reason, col.type_kind))
            if retyped and not bad:
                listed = ", ".join(f'"{name}" ({old} -> {new})' for name, old, new in retyped)
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"{len(retyped)} column(s) of this table use a PostgreSQL type "
                            "Aurora DSQL does not have, so Schema Conversion re-types each "
                            f"to one that holds the same data: {listed}. The table applies "
                            "and Full Load carries every value, but the TYPE changes, so "
                            "application code that relies on the old type's operators or "
                            "functions (array subscripts, range or network operators, enum "
                            "ordering) must change."
                        ),
                        recommendation=_render_remodel_guidance(guidance),
                        effort=EffortLevel.MEDIUM,
                    )
                )
                continue
            if not bad:
                continue
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.UNSUPPORTED,
                    risk=(
                        f"{len(bad)} column(s) of this table use a PostgreSQL data type "
                        "Aurora DSQL does not support as a column type, so its CREATE "
                        f"would be rejected as-is: {_render_bad_columns(bad)}."
                        + (
                            " Schema Conversion re-types the others to a type that holds the "
                            "same data, which the application must adapt to: "
                            + ", ".join(f'"{n}" ({o} -> {t})' for n, o, t in retyped)
                            + "."
                            if retyped
                            else ""
                        )
                        # The KIND is what the operator needs first -- a user-defined type
                        # cannot be created on DSQL at all (no CREATE TYPE), which is a
                        # different problem from a built-in type that is merely outside the
                        # supported set. Without it, the finding read as if
                        # `ecommerce.order_status` were a type DSQL had simply left out.
                        + (
                            " Aurora DSQL has no CREATE TYPE, so a user-defined type cannot "
                            "be created there."
                            if any(kind for *_rest, kind in bad)
                            else ""
                        )
                    ),
                    # The per-type reason, not a catalogue to match against. Each reason
                    # names the faithful remodel target for THAT type, and it was already
                    # being computed and thrown away -- so the operator was handed eight
                    # generic mappings and left to work out which of them applied, while
                    # Schema Conversion (the same function, same column) told them
                    # precisely. Two steps, one column, different answers.
                    recommendation=_render_remodel_guidance(guidance),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


# A matview definition is unbounded (a whole SELECT), and this text lands in an HTML
# report and a UI card -- quote enough to rebuild from, then point at the source.
_MAX_VIEWDEF_CHARS = 600


def _one_line(text: str) -> str:
    """Collapse a multi-line catalog definition into one whitespace-normalised line."""
    return " ".join(text.split())


def _clip(text: str, limit: int) -> str:
    """Return ``text`` bounded to ``limit`` chars, saying so when it was cut."""
    if len(text) <= limit:
        return text
    return (
        text[:limit].rstrip()
        + f" ... [truncated at {limit} characters; read the full definition with "
        "pg_get_viewdef() on the source]"
    )


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
                # Quote the DEFINING QUERY. "Reimplement it" is not actionable without the
                # thing to reimplement, and the operator cannot read it off the source once
                # the report is exported -- introspection already has it (pg_get_viewdef).
                definition = _one_line(getattr(view, "definition", "") or "")
                if definition:
                    recommendation += (
                        " Its defining query, to build the replacement from: "
                        + _clip(definition, _MAX_VIEWDEF_CHARS)
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


def _generated_not_null_cdc_note(columns, primary_key=()) -> str:
    """What CDC does to a NOT NULL generated column it never carries: every insert and
    update it writes leaves the column out, so the target refuses it (23502). A key column
    cannot drop its NOT NULL -- nor stream at all, the change event's key lacking it -- so
    that table can only move with Full load only."""
    if not columns:
        return ""
    keyed = [c.name for c in columns if c.name in set(primary_key or ())]
    other = [c.name for c in columns if c.name not in set(keyed)]
    note = ""
    if other:
        names = ", ".join(other)
        note += (
            f" {names} {'is' if len(other) == 1 else 'are'} NOT NULL: if you replicate with "
            "CDC, every insert and update CDC writes leaves the column out, so Aurora DSQL "
            "refuses it (SQLSTATE 23502, shown on the CDC step) -- choose Full load only for "
            "this table, or drop the NOT NULL from the target DDL in Schema Conversion "
            "before applying (Aurora DSQL cannot add it back afterwards)."
        )
    if keyed:
        names = ", ".join(keyed)
        note += (
            f" {names} {'is' if len(keyed) == 1 else 'are'} in the primary key, which CDC "
            "cannot write without (a key column keeps its NOT NULL): this table can only be "
            "migrated with Full load only."
        )
    return note


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
        # Mirrors converter._pg_generated_column_warning so the go/no-go report and the
        # conversion preview cannot disagree -- including the SPLIT: Aurora DSQL supports a
        # STORED generated column and maintains it (live-verified), so grading that as a
        # loss and telling the operator to rewrite the application was both wrong and
        # expensive. Only what genuinely cannot be carried is a loss.
        from dsql_migrator.core.converter import pg_preserved_generated_columns

        findings: list[Finding] = []
        for table in inventory.tables:
            preserved = [c.name for c in pg_preserved_generated_columns(table)]
            preserved_set = set(preserved)
            lost = [
                c.name for c in table.columns if c.generated and c.name not in preserved_set
            ]
            if preserved:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({', '.join(preserved)}) are PostgreSQL STORED "
                            "generated columns. Aurora DSQL supports STORED generated "
                            "columns and maintains them, so Schema Conversion PRESERVES "
                            "the expression and the target keeps computing the value -- "
                            "nothing to re-implement and no drift. The consequence to know "
                            "is that a write must OMIT the column: DSQL rejects an "
                            "INSERT/UPDATE that supplies a value for it, exactly as "
                            "PostgreSQL does, which is why Full Load and the CDC sink leave "
                            "it out."
                        ),
                        recommendation=(
                            "No action needed. Decide only if you would rather NOT have the "
                            "target compute it: remove the GENERATED clause from the DDL in "
                            "Schema Conversion before applying, because Aurora DSQL can "
                            "DROP an expression from a column later but cannot ADD one -- "
                            "after apply, making the column generated needs the table "
                            "recreated."
                        ),
                        effort=EffortLevel.SIMPLE,
                        note_kind=ConversionNoteKind.RECOMMENDATION,
                    )
                )
            if lost:
                by_name = {c.name: c for c in table.columns}
                kinds = ", ".join(
                    f"{name} ("
                    + (
                        "VIRTUAL -- Aurora DSQL accepts only STORED"
                        if by_name[name].generated_kind == "VIRTUAL"
                        else "generating expression not captured"
                        if not by_name[name].generated_expression
                        else "part of the primary key, which cannot be loaded as generated"
                    )
                    + ")"
                    for name in lost
                )
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=(
                            f"Columns ({kinds}) are PostgreSQL generated columns that "
                            "cannot be carried over, so each becomes an ORDINARY column. "
                            "Full Load copies the value the source already computed, so the "
                            "target starts correct -- but nothing maintains it afterwards, "
                            "and the CDC stream does not carry it either, so any write that "
                            "does not supply the value drifts."
                            + _generated_not_null_cdc_note(
                                [by_name[name] for name in lost if not by_name[name].nullable],
                                table.primary_key,
                            )
                        ),
                        recommendation=(
                            "Compute the value in the application (or in the query) before "
                            "cut over. The expression is shown on the source DDL panel in "
                            "Schema Conversion when it was captured. Note that Aurora DSQL "
                            "cannot ADD an expression to an existing column, so this cannot "
                            "be retrofitted after apply."
                        ),
                        effort=EffortLevel.MEDIUM,
                    )
                )
        return findings


class PgOversizedLobRule(Rule):
    """Flag PostgreSQL columns with no length limit, whose values can exceed 1 MiB.

    Shares the rule id with the MySQL ``OversizedLobRule`` but not its type set: that one
    matches ``mediumtext``/``longblob``/``json``, of which a PostgreSQL inventory can contain
    only ``json`` (``column.mysql_type`` holds ``format_type`` output), so REGISTERING the
    shared rule here would miss ``text``/``bytea``/``jsonb`` and word the one ``json`` match
    for MySQL (before MySQL ``json`` joined that set, it reported zero findings forever).
    The 1 MiB cap is a TARGET (DSQL) limit and applies to PostgreSQL
    ``text``/``bytea``/``json``/``jsonb`` and an unbounded ``varchar`` identically -- the
    reason the rule was excluded was the MySQL type names, not the risk being
    MySQL-specific.

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
        # Lazy import: `prerequisites` is the single source of truth for DSQL's per-type
        # value ceilings, and importing it at module scope would close an import cycle.
        from dsql_migrator.core.prerequisites import (
            _format_limit,
            dsql_value_limit_bytes,
            dsql_value_limit_is_compressed,
        )

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
                        "exceed the Aurora DSQL per-value limit for its type"
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
                        "DSQL per-value limit for its type -- though an unbounded "
                        "text/varchar is PostgreSQL's idiomatic spelling for an ordinary "
                        "short string, so this is a ceiling to confirm rather than work to "
                        "budget"
                    ),
                ),
            ]
            for names, note_kind, clause in groups:
                if not names:
                    continue
                # The ceiling is PER TYPE, and it was previously printed as a flat 1 MiB
                # for all of them: an unbounded `character varying` is capped at 65535
                # bytes and a `character(n)` at 4096, so the finding overstated the headroom
                # 16x/256x and an operator sizing a 200 KB value would have been told it
                # fits (live-verified: 65536 bytes -> "datatype limit greater than 65535
                # bytes not supported for varchar").
                listed = ", ".join(
                    f"{n} ({by_type[n]}, limit "
                    f"{_format_limit(dsql_value_limit_bytes(by_type[n]))})"
                    for n in names
                )
                # Compression applies to the character and json types, NOT to bytea, and
                # only outside a key -- saying "for json/jsonb and text" on a bytea finding
                # implied headroom that does not exist.
                compressible = [
                    n for n in names if dsql_value_limit_is_compressed(by_type[n])
                ]
                compression_clause = (
                    " For "
                    + ", ".join(compressible)
                    + " the limit applies to the COMPRESSED size (and only outside a key -- "
                    "a key column is always stored uncompressed), so a highly compressible "
                    "value may still fit."
                    if compressible
                    else " This limit is on the stored bytes: Aurora DSQL does not compress "
                    "bytea, so there is no headroom beyond it."
                )
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_TABLE, table.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        # Same CDC caveat as the MySQL rule: a change event over the Kafka
                        # producer limit (MaxMessageBytes, 4 MiB by default) fails the
                        # single-task source connector rather than dead-lettering a row.
                        # No "an UPDATE carries it twice" here: that holds only for a table
                        # on REPLICA IDENTITY FULL (e.g. a re-keyed one). On the default
                        # identity the before image is not sent, and an unchanged TOASTed
                        # value is not re-sent either.
                        risk=(
                            f"Columns ({listed}) {clause}. An oversized value cannot be "
                            "stored: the row is quarantined during Full Load or "
                            "dead-lettered during CDC, and reloading cannot fix it. A "
                            "change event larger than the CDC message limit (4 MiB by "
                            "default) is not dead-lettered: it stops CDC for every table "
                            "until the column is excluded at capture."
                        ),
                        recommendation=(
                            "Run the Data Migration prerequisite checks: they probe each "
                            "column and report whether a value ALREADY exceeds that "
                            "column's limit. If one does, move that content to external "
                            "storage (e.g. Amazon S3) and store a reference instead, or "
                            "exclude the column on the Data Migration step."
                            + compression_clause
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
        # The identity fallback is the converter's own clause, so Step 1 and Step 2 give the
        # same (bigint-aware) instruction for the same column.
        from dsql_migrator.core.converter_postgres import pg_identity_fallback

        findings: list[Finding] = []
        for table in inventory.tables:
            key_columns = set(table.primary_key or ())
            if table.auto_increment_column:
                key_columns.add(table.auto_increment_column)
            not_null, nullable, sequenced = [], [], []
            for column in table.columns:
                if column.name in key_columns:
                    continue
                if not (
                    column.identity
                    or "nextval(" in (column.default or "").lower()
                ):
                    continue
                mechanism = "GENERATED AS IDENTITY" if column.identity else "serial"
                label = f"{column.name} ({mechanism})"
                (nullable if column.nullable else not_null).append(label)
                sequenced.append(column)
            if not (not_null or nullable):
                continue
            # The outcome depends on the column's NULLABILITY, and the two are not
            # remotely equivalent. A ``serial`` expands to NOT NULL DEFAULT nextval(...)
            # and GENERATED AS IDENTITY implies NOT NULL -- confirmed against a live
            # PostgreSQL catalog AND against this tool's own introspector -- so for the
            # dominant case the target column is NOT NULL with no default, and an INSERT
            # that omits it is REJECTED (not-null violation, SQLSTATE 23502): an
            # application outage at cut over. The old text stated only the NULL outcome,
            # which requires a hand-attached sequence default on a NULLABLE column and is
            # the rarer case -- so the go/no-go artifact described a data-quality item to
            # clean up later while Schema Conversion, which DOES branch on nullability,
            # said "REJECTED on Aurora DSQL" about the same column. This risk text is also
            # copied into the AI strategist's prompt, so the wrong outcome propagated into
            # the generated migration plan.
            clauses, remedies = [], []
            if not_null:
                clauses.append(
                    f"{', '.join(not_null)} -- NOT NULL, so an INSERT that omits the "
                    "column succeeds on the source but is REJECTED on Aurora DSQL "
                    "(not-null violation, SQLSTATE 23502)"
                )
                remedies.append(
                    "For the NOT NULL column(s) this must be handled BEFORE cut over or "
                    "writes fail outright: set the value explicitly in the application, "
                    "or have Aurora DSQL generate it with an identity."
                )
            if nullable:
                clauses.append(
                    f"{', '.join(nullable)} -- nullable, so an INSERT that omits the "
                    "column writes NULL instead of the next number"
                )
                remedies.append(
                    "For the nullable column(s), supply the value from the application, "
                    "or have Aurora DSQL generate it with an identity, to stop NULLs "
                    "accumulating."
                )
            # Which identity route applies depends on the column's type: Aurora DSQL allows
            # an identity only on bigint and cannot change a column's type after apply, so
            # the bare "add an identity on the target" could not be followed on an integer
            # column -- which is what a plain serial is.
            remedies.append("For an identity: " + pg_identity_fallback(sequenced) + ".")
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"{len(not_null) + len(nullable)} column(s) take their value from a "
                        "sequence but are NOT the primary key. Their sequences are not "
                        "created on Aurora DSQL, and the primary-key strategy generates "
                        "values only for the key column, so each lands on the target with "
                        f"neither identity nor default: {'; '.join(clauses)}."
                    ),
                    recommendation=(
                        " ".join(remedies)
                        + " Already-loaded rows are unaffected -- Full Load copies the "
                        "existing values."
                    ),
                    effort=EffortLevel.MEDIUM,
                    note_kind=ConversionNoteKind.LOSS,
                )
            )
        return findings


class PgCharacterLengthRule(Rule):
    """Flag a PostgreSQL character column whose DECLARED LENGTH exceeds DSQL's ceiling.

    Aurora DSQL caps a declared character length at 65535 bytes for ``varchar`` and 4096
    for ``char`` -- live-verified: ``CREATE ... VARCHAR(65536)`` fails with
    ``Datatype limit greater than 65535 bytes not supported for varchar``. PostgreSQL
    allows up to ~1 GB, so such a column made the WHOLE ``CREATE TABLE`` fail at apply
    with nothing having warned (``dsql_lint`` does not catch it either).

    Mirrors the ``NUMERIC_PRECISION`` pair: Evaluation states the issue and the remedy,
    Schema Conversion emits the clamped type (``text``) with its own MANUAL warning, so
    the two steps agree instead of the operator meeting this at apply time.
    """

    rule_id = "PG_CHARACTER_LENGTH"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.converter_postgres import clamp_pg_character

        findings: list[Finding] = []
        for table in inventory.tables:
            over: list[tuple[str, str]] = []
            for column in table.columns:
                _, note = clamp_pg_character(column.mysql_type)
                if note is not None:
                    over.append((column.name, column.mysql_type))
            if not over:
                continue
            # No KIND here: an over-long varchar/char is a BUILT-IN type, so the
            # user-defined-type suffix must stay off.
            listed = _render_bad_columns([(name, typ, "", None) for name, typ in over])
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"{len(over)} column(s) declare a character length Aurora DSQL "
                        "does not accept (the maximum is 65535 bytes for varchar and 4096 "
                        f"for char), so the table's CREATE would be rejected as-is: "
                        f"{listed}. Schema Conversion converts each to text, which stores "
                        "MORE of the source's range than a clamped length would (up to "
                        "Aurora DSQL's 1 MiB per-value limit), but the declared length is "
                        "then no longer enforced by the database."
                    ),
                    recommendation=(
                        "The conversion itself is automatic; what needs a decision is the "
                        "length limit, which the target no longer enforces. If the "
                        "application relies on it, add a CHECK "
                        "(length(col) <= n) to the target, or shorten the declaration at "
                        "the source to 65535/4096 or less before converting. A "
                        "fixed-length char also loses its blank padding semantics "
                        "(comparisons stop ignoring trailing spaces)."
                    ),
                    effort=EffortLevel.SIMPLE,
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
        from dsql_migrator.core.assessor import _MONOTONIC_KEY_NOTE
        from dsql_migrator.core.converter import (
            IDENTITY_FALLBACK_CAVEAT,
            identity_strategy_applies,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            column = table.auto_increment_column
            if not column:
                continue
            keeps = identity_strategy_applies(table)
            declared = next((c.mysql_type for c in table.columns if c.name == column), "")
            # The headline says what the default conversion does with THIS key: a single
            # integer key keeps generating; a composite or non-integer one does not.
            if keeps:
                risk = (
                    f"The key from serial / identity column '{column}' converts cleanly "
                    "AND keeps generating values: the default conversion makes it a "
                    "cached identity, so the application still does not supply the key. "
                    "Two things change. The column is WIDENED to bigint (Aurora DSQL "
                    "supports an identity only on bigint) — lossless for an integer key (a "
                    "numeric one is narrowed instead: every value a sequence generated fits, "
                    "a larger one supplied by hand would not load), but check anything that "
                    "assumes a 4-byte id. And because each DSQL node draws its own block of values "
                    "from the cache, the key is no longer gap-free or strictly increasing, "
                    "which also spreads inserts instead of concentrating them on one "
                    "partition (DSQL stores rows in primary-key order)."
                )
            elif table.primary_key == [column]:
                risk = (
                    f"The {declared} key from serial / identity column '{column}' is not "
                    "an integer, and an Aurora DSQL identity is (bigint), so Aurora DSQL "
                    "will NOT generate it: the APPLICATION must supply the value on every "
                    "insert — code that relied on the database generating it fails or "
                    "collides." + _MONOTONIC_KEY_NOTE
                )
            else:
                risk = (
                    f"The key from serial / identity column '{column}' converts cleanly, "
                    "but Aurora DSQL will NOT generate it: the primary key is composite "
                    f"({', '.join(table.primary_key)}) and an identity applies to a single "
                    "column, so the column stays a plain integer and the APPLICATION must "
                    "supply the value on every insert — code that relied on the database "
                    "generating it fails or collides." + _MONOTONIC_KEY_NOTE
                )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=risk,
                    # State the DEFAULT, then the opt-out -- see the MySQL rule.
                    recommendation=(
                        (
                            "No action needed to keep the behaviour: Schema Conversion "
                            "defaults to 'Server-generated (IDENTITY)' for this table, and "
                            "Full Load loads the source's own ids before advancing the "
                            "sequence past them. Switch to 'Keep source PK' only if the "
                            "application supplies the key itself — that leaves the column a "
                            "plain integer with neither identity nor sequence."
                            if keeps
                            else "Supply the value from the application, or remodel the "
                            "key as an integer before migrating it."
                            if table.primary_key == [column]
                            else "IDENTITY cannot be used for this table because its "
                            f"primary key is composite ({', '.join(table.primary_key)}) and "
                            "a DSQL identity applies to a single column, so the key is "
                            "converted as a plain integer: the application must supply the "
                            "value on every insert, or add an identity to the column on the "
                            "target after apply, "
                            + IDENTITY_FALLBACK_CAVEAT
                            + "."
                        )
                        + " Optional, for throughput only: a UUID/random key spreads the "
                        "writes further."
                    ),
                    effort=EffortLevel.MEDIUM,
                    # Advice where the key keeps generating; a gap where nothing does.
                    note_kind=(
                        ConversionNoteKind.RECOMMENDATION if keeps else ConversionNoteKind.LOSS
                    ),
                )
            )
        return findings


class PgUnconstrainedNumericRule(Rule):
    """Flag bare ``numeric`` / ``decimal`` columns, which Aurora DSQL stores as numeric(18,6).

    A PostgreSQL ``numeric`` with no declared precision is arbitrary-precision; Aurora
    DSQL stores it at its default ``numeric(18,6)``, so a value with more than 6
    fractional digits is ROUNDED on load and one needing more than 12 integer digits does
    not fit. Nothing later catches the rounding -- Full Load and CDC deliberately accept it,
    and Validation compares such a column at scale 6, so a rounded value still reads MATCH
    -- which makes this the one place to decide before the data moves. The text is the
    converter's own (``unconstrained_numeric_note``), which Schema Conversion shows on the
    same columns.
    """

    rule_id = "PG_UNCONSTRAINED_NUMERIC"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.converter_postgres import unconstrained_numeric_note

        findings: list[Finding] = []
        for table in inventory.tables:
            columns = [
                column
                for column in table.columns
                if unconstrained_numeric_note(column.mysql_type) is not None
            ]
            if not columns:
                continue
            note = unconstrained_numeric_note(columns[0].mysql_type)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=f"Column(s) {', '.join(c.name for c in columns)}: {note}",
                    recommendation=(
                        "Check what these columns hold. If any value has more than 6 "
                        "fractional digits or more than 12 integer digits, give the column "
                        "an explicit numeric(p,s) in Schema Conversion's target DDL before "
                        "applying; otherwise numeric(18,6) keeps every value as it is."
                    ),
                    effort=EffortLevel.SIMPLE,
                )
            )
        return findings


# ---------------------------------------------------------------------------
# Source catalog facts the inventory does not carry (core.catalog_postgres): row-level
# security and standalone sequences. Read by the Evaluation probe beside the inventory,
# graded here, and carried over a re-assessment by ``assessor.carry_probe_results``.
# ---------------------------------------------------------------------------

# A policy's expressions are unbounded, and the finding lands in a UI card, the text report
# and the HTML report: show enough to act on, then say where the rest is.
_MAX_POLICIES_SHOWN = 6
_MAX_POLICY_EXPRESSION_CHARS = 300

# Commands whose policies also decide which rows may be WRITTEN (WITH CHECK, or USING where
# the policy has no WITH CHECK).
_WRITE_COMMANDS = frozenset({"ALL", "INSERT", "UPDATE"})


def _policy_expression(text: str) -> str:
    """One line, bounded, saying where the full expression is when it was cut."""
    line = _one_line(text)
    if len(line) <= _MAX_POLICY_EXPRESSION_CHARS:
        return line
    return (
        line[:_MAX_POLICY_EXPRESSION_CHARS].rstrip()
        + " ... [truncated; the full expression is in pg_policies on the source]"
    )


def _render_policy(policy: RowSecurityPolicy) -> str:
    """``name (FOR cmd TO roles) USING (...) WITH CHECK (...)``, the way CREATE POLICY reads."""
    roles = ", ".join(policy.roles) if policy.roles else "public"
    restrictive = "" if policy.permissive else "AS RESTRICTIVE, "
    parts = [f"{policy.name} ({restrictive}FOR {policy.command} TO {roles})"]
    if policy.using:
        parts.append(f"USING ({_policy_expression(policy.using)})")
    if policy.with_check:
        parts.append(f"WITH CHECK ({_policy_expression(policy.with_check)})")
    return " ".join(parts)


def _row_security_finding(
    table: RowSecurityTable, login: "Optional[SourceLogin]"
) -> Finding:
    """The UNSUPPORTED finding for one table with row-level security enabled.

    UNSUPPORTED because Aurora DSQL has no row-level security at all (its ALTER TABLE has no
    ENABLE ROW LEVEL SECURITY, and pg_policy / pg_policies are not part of it), so the
    access rule has no target and has to be redesigned in the application -- the same
    grade a trigger gets, and for the same reason. The table's DATA still migrates, which is
    why the finding sits on the table's own item rather than replacing it.
    """
    enabled = "Row-level security is enabled on this table" + (
        " and FORCED (it applies to the table's owner too)" if table.forced else ""
    )
    if table.policies:
        count = len(table.policies)
        shown = "; ".join(
            _render_policy(p) for p in table.policies[:_MAX_POLICIES_SHOWN]
        )
        rest = count - _MAX_POLICIES_SHOWN
        more = (
            f"; and {rest} more {'policy' if rest == 1 else 'policies'} (pg_policies on "
            "the source)"
            if rest > 0
            else ""
        )
        what = (
            f"{enabled}, with {count} {'policy' if count == 1 else 'policies'}: "
            f"{shown}{more}."
        )
    else:
        what = (
            f"{enabled}, with no policy: on the source, every role it applies to reads "
            "and writes no rows at all (default deny)."
        )
    risk = what + (
        " Aurora DSQL has no row-level security, so none of this exists on the target: "
        "after cut over, every role that can read or write the table reaches every row."
    )
    exemption = row_security_exemption(table, login)
    if exemption is None:
        risk += (
            " Whether row-level security applies to the source user could not be read. The "
            "tool reads with row_security = off, as pg_dump does, so if it does, the Full "
            "Load and Validation stop on this table with an error rather than copy and "
            "compare only the rows it shows that user."
        )
    elif exemption:
        risk += (
            f" The source user {login.name} reads every row as {exemption}, so the Full "  # type: ignore[union-attr]
            "Load and Validation still copy and compare the whole table."
        )
    else:
        ownership = (
            "it owns the table, but FORCE ROW LEVEL SECURITY applies it to the owner too"
            if table.login_owns
            else f"it is not the table's owner, {table.owner}"
        )
        # With no policy, what the user would have read is nothing at all (default deny).
        instead = (
            "copy and compare only the rows the policies show it"
            if table.policies
            else "copy an empty table"
        )
        risk += (
            f" Row-level security applies to the source user {login.name} (not a "  # type: ignore[union-attr]
            f"superuser, no BYPASSRLS, and {ownership}), so the Full Load and Validation "
            f"stop on this table with an error rather than {instead}: the tool reads with "
            "row_security = off, as pg_dump does."
        )
    if table.policies:
        recommendation = (
            "Before cut over, enforce this in the application: every query on the table "
            "has to carry the policies' conditions itself (for example a tenant predicate "
            "bound from the application's own session), because Aurora DSQL will not add "
            "them."
        )
        if any(p.command.upper() in _WRITE_COMMANDS for p in table.policies):
            recommendation += (
                " The write side -- which rows a role may insert or update (WITH CHECK, or "
                "USING where a policy has none) -- has to be checked by the application too."
            )
    else:
        recommendation = (
            "Before cut over, decide who may read this table on Aurora DSQL: on the source "
            "only roles exempt from row-level security can read it, while on the target "
            "every role granted SELECT reads every row -- restrict the grants, or filter in "
            "the application."
        )
    if exemption == "":
        recommendation += (
            " To migrate its rows, run the migration as a role that reads every row: a "
            "superuser or a role with BYPASSRLS"
            + (
                " -- not its owner, whom FORCE ROW LEVEL SECURITY subjects to the "
                "policies."
                if table.forced
                else f", or the table's owner ({table.owner})."
            )
        )
    if not exemption:  # unknown, or the policies apply to the source user
        recommendation += (
            " The Data Migration prerequisite checks re-check this before the load."
        )
    return Finding(
        object=ObjectKey(KIND_TABLE, table.table),
        rule_id=PG_ROW_LEVEL_SECURITY_RULE_ID,
        classification=Classification.UNSUPPORTED,
        risk=risk,
        recommendation=recommendation,
        effort=EffortLevel.MEDIUM,
        note_kind=ConversionNoteKind.LOSS,
    )


def row_security_findings(facts: RowSecurityFacts) -> "list[Finding]":
    """One UNSUPPORTED finding per table with row-level security enabled. Pure."""
    return [_row_security_finding(table, facts.login) for table in facts.tables]


def _sql_literal(text: str) -> str:
    """``text`` as a SQL string literal (single quotes doubled)."""
    return "'" + text.replace("'", "''") + "'"


def _default_callers(inventory) -> "dict[str, list[tuple[str, str, str]]]":
    """``{called sequence: [(table, column, DEFAULT)]}`` from the inventory's DEFAULT text,
    in one pass -- the fallback when the probe could not read the catalog's own links. A
    bare name counts under the table's own schema (and the ``public`` default)."""
    from dsql_migrator.core.converter_postgres import pg_nextval_sequence

    calls: dict[str, list[tuple[str, str, str]]] = {}
    for table in getattr(inventory, "tables", None) or ():
        table_schema = table.name.partition(".")[0]
        for column in table.columns:
            called = pg_nextval_sequence(column.default)
            if not called:
                continue
            names = {called} if "." in called else {f"{table_schema}.{called}", f"public.{called}"}
            for name in names:
                calls.setdefault(name, []).append((table.name, column.name, column.default or ""))
    return calls


def _calls_nextval_of(expression: str, sequence_name: str) -> bool:
    """Whether ``expression`` draws from ``sequence_name`` (``schema.name``; a bare name in
    the expression counts) -- a currval() or a cast only reads it."""
    from dsql_migrator.core.converter_postgres import _NEXTVAL_RE, pg_nextval_sequence

    bare = sequence_name.rsplit(".", 1)[-1]
    for match in _NEXTVAL_RE.finditer(expression or ""):
        called = pg_nextval_sequence(match.group(0)) or ""
        if called == sequence_name or called == bare:
            return True
    return False


def _reattached_default(expression: str, sequence_name: str, quoted: str) -> str:
    """``expression`` with each ``nextval`` of ``sequence_name`` pointing at the sequence
    created on Aurora DSQL -- the column's own DEFAULT, never a bare ``nextval`` standing in
    for a formatted one (``'ORD-' || lpad(nextval(...)::text, 8, '0')``)."""
    from dsql_migrator.core.converter_postgres import _NEXTVAL_RE, pg_nextval_sequence

    bare = sequence_name.rsplit(".", 1)[-1]

    def _point(match: "re.Match[str]") -> str:
        called = pg_nextval_sequence(match.group(0)) or ""
        if called.rsplit(".", 1)[-1] == bare:
            return f"nextval({_sql_literal(quoted)})"
        return match.group(0)

    return _NEXTVAL_RE.sub(_point, expression or "")


def standalone_sequence_items(
    sequences: "Sequence[StandaloneSequence]",
    inventory=None,
) -> "list[AssessmentItem]":
    """One MANUAL item per sequence no column owns, carrying the Aurora DSQL statement that
    re-creates it after its source value. Pure.

    Schema Conversion creates no such sequence (only a column's own serial/identity is
    converted, by its column), so the application's -- or a column DEFAULT's -- nextval()
    on it fails after cut over until it exists on the target. Creating it, and moving it
    past the source's final value at cut over, is left to the operator: this tool neither
    emits nor syncs it. The columns of ``inventory`` whose DEFAULT calls it are named, with
    the statement that re-attaches that DEFAULT (Schema Conversion drops it): an identity
    per column instead would give each table its own counter.
    """
    from dsql_migrator.core.converter_postgres import (
        dsql_create_sequence_sql,
        pg_sequence_next_value,
    )
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.source_dialect import dialect_for

    from dsql_migrator.core.converter import PrimaryKeyStrategy, default_primary_key_strategy

    pg = dialect_for(SourceType.POSTGRES)
    tables_by_name = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    # One pass over the inventory, used only for a sequence the catalog read gave no
    # callers for (a probe that could not read pg_depend).
    fallback = _default_callers(inventory) if inventory is not None else {}
    items: list[AssessmentItem] = []
    for sequence in sequences:
        quoted = pg.quote_table(sequence.name)
        statement = dsql_create_sequence_sql(sequence)
        following = pg_sequence_next_value(sequence)
        if not sequence.readable:
            state = (
                "Its current value could not be read (the source user has neither SELECT "
                "nor USAGE on it), so the statement below has a placeholder for where it "
                "starts."
            )
        elif sequence.last_value is None:
            state = (
                "Nothing has been drawn from it since it was created or last reset, but its "
                "next value could not be read (that needs SELECT on the sequence), so the "
                "statement below has a placeholder for where it starts."
            )
        elif sequence.is_called is False:
            state = (
                "Nothing has been drawn from it since it was created or last reset (a "
                f"RESTART or setval(..., false)): its next value is {sequence.last_value}."
            )
        elif following is None:
            bound = "MAXVALUE" if sequence.increment > 0 else "MINVALUE"
            state = (
                f"It is exhausted: its last value {sequence.last_value} is at its {bound} "
                "and it does not CYCLE, so nextval() already fails on the source -- choose "
                "a new range before creating it."
            )
        else:
            state = (
                f"Its last value on the source is {sequence.last_value}, read now: it keeps "
                "advancing while the source is in use."
            )
        if sequence.data_type.lower() not in ("bigint", "int8"):
            state += (
                f" Aurora DSQL sequences are always bigint; the statement keeps the source's "
                f"{sequence.data_type} range as MINVALUE/MAXVALUE."
            )
        if sequence.cache_size <= 1:
            cache_note = (
                "CACHE 1 hands values out one at a time and in order, as the source does (an "
                "aborted or retried transaction still uses one up, there as here); Aurora DSQL "
                "also accepts CACHE 65536 or more, which serves concurrent sessions faster but "
                "hands each a block, so values arrive out of order with large gaps."
            )
        else:
            cache_note = (
                f"The source caches {sequence.cache_size} values per session, so the "
                "application already tolerates gaps; Aurora DSQL accepts only CACHE 1 or at "
                "least 65536, so the statement uses 65536."
            )
        # The catalog's own links when they were read (None: the read failed, so the
        # DEFAULT text decides) -- but only callers this migration has: a partition (its
        # parent carries the DEFAULT), another session's temp table, and a DEFAULT that only
        # reads the sequence (currval, which the conversion keeps) are none of them.
        calling = [
            (table, column, expr)
            for table, column, expr in (
                list(sequence.callers)
                if sequence.callers is not None
                else list(fallback.get(sequence.name, ()))
            )
            if (not tables_by_name or table in tables_by_name)
            and _calls_nextval_of(expr, sequence.name)
        ]
        # A primary key whose DEFAULT calls the sequence becomes an identity of its own under
        # the default strategy: a DEFAULT cannot be re-attached to it, and keeping the shared
        # numbering means choosing Keep source PK for those tables first.
        keyed = [
            (table, column, expr)
            for table, column, expr in calling
            if (owner := tables_by_name.get(table)) is not None
            and owner.auto_increment_column == column
            and default_primary_key_strategy(owner, is_postgres=True)
            is PrimaryKeyStrategy.IDENTITY_WITH_CACHE
        ]
        dropped = [entry for entry in calling if entry not in keyed]
        defaults = ""
        if dropped:
            shown = ", ".join(f"{table}.{column}" for table, column, _expr in dropped)
            state += (
                f" A DEFAULT calls it on {shown}; Schema Conversion drops that DEFAULT, so an "
                "INSERT that omits the column gets no value."
            )
            reattach = " ".join(
                f"ALTER TABLE {pg.quote_table(table)} ALTER COLUMN "
                f"{pg.quote_identifier(column)} SET DEFAULT "
                f"{_reattached_default(expr, sequence.name, quoted) or f'nextval({_sql_literal(quoted)})'};"
                for table, column, expr in dropped
            )
            defaults = (
                f" Then re-attach {'that DEFAULT' if len(dropped) == 1 else 'those DEFAULTs'}, "
                f"as written on the source: {reattach} Do not give "
                f"{'that column' if len(dropped) == 1 else 'those columns'} an identity instead "
                "(the NON_KEY_SEQUENCE advice): each table would get its own counter, and a "
                "sequence they share would hand out the same numbers twice."
            )
        if keyed:
            shown = ", ".join(f"{table}.{column}" for table, column, _expr in keyed)
            several = len(keyed) > 1 or bool(dropped)
            state += (
                f" It is the primary key DEFAULT of {shown}, which Schema Conversion makes an "
                "identity of its own"
                + (", so those tables stop sharing its numbers." if several else ".")
            )
            reattach_keys = " ".join(
                f"ALTER TABLE {pg.quote_table(table)} ALTER COLUMN "
                f"{pg.quote_identifier(column)} SET DEFAULT "
                f"{_reattached_default(expr, sequence.name, quoted) or f'nextval({_sql_literal(quoted)})'};"
                for table, column, expr in keyed
            )
            defaults += (
                f" To keep {'them sharing it' if several else 'the key drawing from it'}, choose "
                f"'Keep source PK' for {'those tables' if len(keyed) > 1 else 'that table'} in "
                "Schema Conversion (an identity column takes no DEFAULT), then run after apply: "
                f"{reattach_keys}"
            )
        item = AssessmentItem(
            object_name=sequence.name,
            rule_id=PG_STANDALONE_SEQUENCE_RULE_ID,
            classification=Classification.MANUAL,
            risk=(
                "A standalone sequence: no column owns it, so Schema Conversion does not "
                "create it and it does not exist on Aurora DSQL -- an application (or a "
                "column DEFAULT) that calls nextval() on it fails after cut over. "
                + state
            ),
            recommendation=(
                f"Create it on Aurora DSQL before cut over: {statement}{defaults} Once source "
                f"writes stop, read its final value (SELECT last_value FROM {quoted}) and move "
                f"the target past it with SELECT setval({_sql_literal(quoted)}, <that value>) "
                "-- this tool neither creates nor syncs it. " + cache_note
            ),
            effort=EffortLevel.SIMPLE,
            kind=KIND_SEQUENCE.upper(),
        )
        items.append(item.model_copy(update={"concerns": _single_concern(item)}))
    return items


def catalog_unread_items(
    facts: "Optional[PgCatalogFacts]", *, not_checked: bool = False
) -> "list[AssessmentItem]":
    """The database-level item for a probe part that could not be read (``facts`` ``None``:
    none could). Without it a failed read would look exactly like a source with no
    row-level security and no standalone sequence -- a "Ready" over what was never checked.
    ``not_checked``: the report never ran the probe (made before it, or re-read without it).
    """
    if not_checked:
        unread = ["row-level security", "sequences"]
        reason = "this report was made without that check; re-run Step 1"
    elif facts is None:
        unread = ["row-level security", "sequences"]
        reason = "the catalog read failed"
    else:
        unread = [
            part
            for part, value in (
                ("row-level security", facts.row_security),
                ("sequences", facts.sequences),
            )
            if value is None
        ]
        reason = "; ".join(facts.read_errors) or "the read failed"
    if not unread:
        return []
    unknowns = {
        "row-level security": (
            "whether a table has row-level security policies (which Aurora DSQL does not "
            "keep)"
        ),
        "sequences": (
            "whether the application uses a standalone sequence (which Schema Conversion "
            "does not create)"
        ),
    }
    item = AssessmentItem(
        object_name="source catalog",
        rule_id=PG_CATALOG_UNREAD_RULE_ID,
        classification=Classification.MANUAL,
        risk=(
            f"The source's {' and '.join(unread)} "
            + ("were not checked" if not_checked else "could not be read")
            + f" ({reason}), so this report cannot say "
            + " or ".join(unknowns[part] for part in unread)
            + "."
        ),
        recommendation=(
            "Re-run Step 1 (Evaluation). If the read keeps failing, list them on the "
            "source as a user who can: SELECT relname FROM pg_class WHERE relrowsecurity; "
            "and SELECT * FROM pg_sequences;"
        ),
        effort=EffortLevel.SIMPLE,
        kind=KIND_DATABASE.upper(),
    )
    return [item.model_copy(update={"concerns": _single_concern(item)})]


def catalog_checked_item(facts: "PgCatalogFacts") -> AssessmentItem:
    """The database-level note that this run read the catalog probe (row-level security and
    standalone sequences), saying what it found. Automatic and out of the score: it is what
    tells a report from before the probe -- restored or refreshed, where a clean probe and
    no probe would otherwise look the same -- from one whose probe found nothing."""
    parts = []
    if facts.row_security is not None:
        count = len(facts.row_security.tables)
        parts.append(
            f"{count} table(s) with row-level security, graded on each table"
            if count
            else "no table with row-level security"
        )
    if facts.sequences is not None:
        count = len(facts.sequences)
        parts.append(
            f"{count} standalone sequence(s), each its own item" if count
            else "no standalone sequence"
        )
    found = "; ".join(parts) or "nothing it could read (see the source catalog item)"
    item = AssessmentItem(
        object_name="source catalog checks",
        rule_id=PG_CATALOG_CHECKED_RULE_ID,
        classification=Classification.AUTO,
        risk=f"This run read the source catalog: {found}.",
        recommendation=(
            "Re-run Step 1 after enabling row-level security on a table or creating a "
            "sequence on the source."
        ),
        effort=None,
        kind=KIND_DATABASE.upper(),
    )
    return item.model_copy(update={"concerns": _single_concern(item)})


def with_pg_catalog_facts(
    report: AssessmentReport, facts: "Optional[PgCatalogFacts]", inventory=None
) -> AssessmentReport:
    """``report`` with the PostgreSQL catalog probe's findings: row-level security graded on
    each table's own item, an item per standalone sequence, and an item for any part that
    could not be read (``facts`` ``None``: the probe could not read the source). Pure."""
    if facts is not None and facts.row_security is not None:
        report = with_object_findings(report, row_security_findings(facts.row_security))
    extra = [
        *standalone_sequence_items(
            (facts.sequences if facts is not None else None) or (), inventory
        ),
        *catalog_unread_items(facts),
        *([catalog_checked_item(facts)] if facts is not None else []),
    ]
    if extra:
        report = AssessmentReport.from_items([*report.items, *extra])
    return report


class PgViewRemodelledColumnRule(Rule):
    """A PostgreSQL view over columns Schema Conversion re-types (enum, array, range, ...).

    The view was graded AUTO and emitted verbatim, so an expression relying on the old
    type reached apply: a cast to the enum type failed CREATE VIEW (that cast is now
    rewritten to text), and an array subscript or a range operator fails or changes. The
    conversion note's own text, decided by the same detection."""

    rule_id = "VIEW_REMODELLED_COLUMN"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.converter import (
            pg_views_remodelling,
            remodelled_reference_note,
        )

        findings: list[Finding] = []
        remodellings = pg_views_remodelling(inventory.views, inventory.tables)
        for view in inventory.views:
            if getattr(view, "unsupported_kind", None):
                continue
            remodelling = remodellings.get(view.name)
            if remodelling is not None and remodelling.flagged:
                findings.append(
                    Finding(
                        object=ObjectKey(KIND_VIEW, view.name),
                        rule_id=self.rule_id,
                        classification=Classification.MANUAL,
                        risk=remodelled_reference_note(
                            remodelling.references, remodelling.rewritten
                        ),
                        recommendation=(
                            "Open the view in Schema Conversion and adapt the expressions "
                            "on those columns to their new type (the target DDL is "
                            "editable) before applying it."
                        ),
                        effort=EffortLevel.SIMPLE,
                    )
                )
        return findings


class PgCdcUncarryableColumnRule(Rule):
    """Columns whose PostgreSQL type a CDC change event cannot carry to Aurora DSQL.

    Step 3's ``COLUMN_REPLICABLE`` check found them only after the operator had planned on
    Evaluation's verdict, where such a table read like any other: ``text[]`` graded the same
    as ``timetz[]`` (every change dead-letters) or ``interval[]`` (arrives NULL). Judged from
    the inventory's column types with the same classifier, and worded with the same
    sentences, prefixed by the condition: none of it applies to a Full load only migration,
    which carries every one of these columns."""

    rule_id = "CDC_UNCARRYABLE_COLUMN"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.prerequisites_postgres import (
            uncarryable_column_texts,
            uncarryable_columns_of,
        )

        findings: list[Finding] = []
        for table in inventory.tables:
            found = uncarryable_columns_of(table)
            if not found:
                continue
            detail, remediation = uncarryable_column_texts(found)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk="If you replicate with CDC: " + detail,
                    recommendation=remediation,
                    effort=EffortLevel.SIMPLE,
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
        ByteaKeyRule,
        CheckConstraintRule,
        DecimalPrecisionRule,
        NameCollisionRule,
        EventRule,
        ForeignKeyRule,
        NoPrimaryKeyRule,
        PartitionedTableRule,
        ProcedureRule,
        TooManyColumnsRule,
        TooManyIndexesRule,
        TooManyKeyColumnsRule,
        TriggerRule,
        ViewDefinitionUnreadableRule,
        pg_collation_rule,
        default_dropped_rule,
        expression_index_rule,
        pg_index_not_preserved_rule,
        pg_unindexable_column_rule,
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
        PgCharacterLengthRule(),
        PgIdentityKeyRule(),
        PgNonKeySequenceRule(),
        ForeignKeyRule(),
        CheckConstraintRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        NoPrimaryKeyRule(),
        ByteaKeyRule(is_postgres=True),
        # PG wording: declarative partitioning, leaves are separate tables.
        PartitionedTableRule(is_postgres=True),
        TooManyColumnsRule(),
        TooManyIndexesRule(is_postgres=True),
        TooManyKeyColumnsRule(),
        # Same check as Schema Conversion's, so the two steps agree on a PG source too.
        NameCollisionRule(is_postgres=True),
        # Shared, and correct as-is for PostgreSQL: _DECIMAL_BASES already contains
        # "numeric" and the precision is parsed from the first parenthesised integer, so
        # numeric(40,10) is caught, and the message names only the DSQL limit -- no MySQL
        # wording. Previously excluded as "DECIMAL precision parsed from a MySQL type",
        # which left a numeric that Schema Conversion will CLAMP reading AUTO at
        # Evaluation, i.e. the go/no-go report understated a lossy conversion.
        DecimalPrecisionRule(),
        # The far more common lossy numeric: no declared precision at all.
        PgUnconstrainedNumericRule(),
        ViewDefinitionUnreadableRule(is_postgres=True),
        # What CDC cannot carry, said at Step 1 as the Step 3 check says it.
        PgCdcUncarryableColumnRule(),
        PgViewRemodelledColumnRule(),
        # Conversion warnings Evaluation had no finding for (the parity test's known gaps).
        pg_index_not_preserved_rule(),
        expression_index_rule(),
        pg_unindexable_column_rule(),
        default_dropped_rule(is_postgres=True),
        pg_collation_rule(),
    ]


__all__ = ["default_rules"]
