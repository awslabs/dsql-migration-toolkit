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
        f"column \"{name}\" of type {typ}" for name, typ, _reason in bad
    )


def _render_remodel_guidance(bad: "list[tuple[str, object, str]]") -> str:
    """One actionable line per column, from the reason already computed for its type.

    Bounded: the first :data:`_MAX_DETAILED_COLUMNS` get their own reason and the rest are
    named with a count, so a very wide table cannot produce an unbounded message.
    """
    lines = [
        f"{name}: {reason}"
        for name, _typ, reason in bad[:_MAX_DETAILED_COLUMNS]
    ]
    rest = bad[_MAX_DETAILED_COLUMNS:]
    if rest:
        names = ", ".join(name for name, _typ, _reason in rest)
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
        from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

        findings: list[Finding] = []
        for table in inventory.tables:
            bad = []
            for col in table.columns:
                reason = unsupported_dsql_reason(col.mysql_type)
                if reason is not None:
                    bad.append((col.name, col.mysql_type, reason))
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
                    ),
                    # The per-type reason, not a catalogue to match against. Each reason
                    # names the faithful remodel target for THAT type, and it was already
                    # being computed and thrown away -- so the operator was handed eight
                    # generic mappings and left to work out which of them applied, while
                    # Schema Conversion (the same function, same column) told them
                    # precisely. Two steps, one column, different answers.
                    recommendation=_render_remodel_guidance(bad),
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
                        risk=(
                            f"Columns ({listed}) {clause}. An oversized value cannot be "
                            "stored: the row is quarantined during Full Load or "
                            "dead-lettered during CDC, and reloading cannot fix it."
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
        findings: list[Finding] = []
        for table in inventory.tables:
            key_columns = set(table.primary_key or ())
            if table.auto_increment_column:
                key_columns.add(table.auto_increment_column)
            not_null, nullable = [], []
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
                    "or add an identity to the column on the target."
                )
            if nullable:
                clauses.append(
                    f"{', '.join(nullable)} -- nullable, so an INSERT that omits the "
                    "column writes NULL instead of the next number"
                )
                remedies.append(
                    "For the nullable column(s), supply the value from the application "
                    "(or add an identity on the target) to stop NULLs accumulating."
                )
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk=(
                        f"{len(not_null) + len(nullable)} column(s) take their value from a "
                        "sequence but are NOT the primary key. Aurora DSQL has no source "
                        "sequence to point at, and the primary-key strategy generates "
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
            listed = _render_bad_columns([(name, typ, "") for name, typ in over])
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
                        f"The key from serial / identity column '{column}' converts cleanly "
                        "AND keeps generating values: the default conversion makes it a "
                        "cached identity, so the application still does not supply the key. "
                        "Two things change. The column is WIDENED to bigint (Aurora DSQL "
                        "supports an identity only on bigint) — lossless for the values, "
                        "but check anything that assumes a 4-byte id. And because each DSQL "
                        "node draws its own block of values from the cache, the key is no "
                        "longer gap-free or strictly increasing, which also spreads inserts "
                        "instead of concentrating them on one partition (DSQL stores rows "
                        "in primary-key order)."
                    ),
                    # State the DEFAULT, then the opt-out -- see the MySQL rule.
                    recommendation=(
                        (
                            "No action needed to keep the behaviour: Schema Conversion "
                            "defaults to 'Server-generated (IDENTITY)' for this table, and "
                            "Full Load loads the source's own ids before advancing the "
                            "sequence past them. Switch to 'Keep source PK' only if the "
                            "application supplies the key itself — that leaves the column a "
                            "plain integer with neither identity nor sequence."
                            if table.primary_key == [column]
                            else "IDENTITY cannot be used for this table because its "
                            f"primary key is composite ({', '.join(table.primary_key)}) and "
                            "a DSQL identity applies to a single column, so the key is "
                            "converted as a plain integer: the application must supply the "
                            "value on every insert, or add an identity to the column on the "
                            "target after apply."
                        )
                        + " Optional, for throughput only: a UUID/random key spreads the "
                        "writes further."
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
        PgCharacterLengthRule(),
        PgIdentityKeyRule(),
        PgNonKeySequenceRule(),
        ForeignKeyRule(),
        CheckConstraintRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        NoPrimaryKeyRule(),
        # PG wording: declarative partitioning, leaves are separate tables.
        PartitionedTableRule(is_postgres=True),
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
