# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL CDC prerequisite checks (logical-replication readiness).

The PostgreSQL analog of the MySQL binlog/GTID CDC checks in ``prerequisites.py``,
kept in its own module (repo convention: ``assessor_postgres`` / ``converter_postgres``
/ ``exporter_postgres`` / ``validator_postgres`` / ``cdc_postgres``). PostgreSQL CDC uses
Debezium ``pgoutput`` -- a logical replication slot + a publication -- so the source must
have ``wal_level=logical``, a role that can create a slot, slot/wal-sender headroom, be a
writer (not a standby), and each captured table must have a usable REPLICA IDENTITY.

The facts are gathered ONCE by a read-only dialect probe
(:meth:`PostgresSourceDialect.probe_cdc_prerequisites`) into :class:`PostgresCdcFacts`;
each pure ``check_*`` here turns those facts into a :class:`PrerequisiteResult`. Only
imports ``core.models`` (no dependency on ``prerequisites``), so ``prerequisites`` can
import this lazily without a cycle. All strings are English, credential-free (Property 7).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.models import (
    PrerequisiteCheckId,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceType,
    TableDef,
)

if TYPE_CHECKING:
    from dsql_migrator.core.catalog_postgres import RowSecurityFacts

# pg_class.relreplident values that let Debezium key an UPDATE/DELETE: 'd' default
# (uses the primary key -- which the migration already requires), 'f' full, 'i' a
# unique index. 'n' (nothing) makes UPDATE/DELETE on the table ERROR on the publisher.
_USABLE_REPLICA_IDENTITY = frozenset({"d", "f", "i"})


def _quoted(name: str) -> str:
    """``schema.table`` as the quoted identifier a printed statement needs -- unquoted,
    PostgreSQL folds ``MixS.Orders`` to ``mixs.orders`` and the remedy fails (G25). The
    tool's own source writes quote the same way."""
    # Lazy: the dialect package imports this module.
    from dsql_migrator.core.source_dialect import dialect_for

    return dialect_for(SourceType.POSTGRES).quote_table(name)


# --- Columns PostgreSQL CDC cannot carry -----------------------------------------------
# Full Load loads every one of these CORRECTLY (the exporter reads
# ``CAST(to_jsonb(col) AS text)``) and Schema Conversion maps every ``<t>[]`` to jsonb
# (Aurora DSQL has no array type), so the divergence appears ONLY on rows that arrive via
# CDC. A Debezium after-image carries every column, so the FIRST unrenderable one decides
# the outcome for the whole row. Live-established on a PostgreSQL 17.7 -> MSK -> DSQL run;
# until this check the operator learned it from a Validation CHECKSUM mismatch -- after the
# billable MSK infrastructure existed and the target already held wrong data.
#
# The reason codes are the three CONSEQUENCES, because the consequence is what the operator
# decides on and what the prerequisite row has to SAY. It is not what sets the grade: all
# three are WARN / non-blocking, including the dead-lettering class -- see
# :func:`check_columns_replicable` for why a FAIL here would block the Full Load that
# carries these columns correctly, with no route in the UI to clear it.
UNCARRYABLE_ROW_DEAD_LETTERS = "row_dead_letters"
UNCARRYABLE_COLUMN_ARRIVES_NULL = "column_arrives_null"
UNCARRYABLE_COLUMN_ARRIVES_WRONG = "column_arrives_wrong"

# Keyed by the pg_catalog name of an ARRAY column's ELEMENT type (``_timetz`` -> timetz).
# Built-in only: the probe's query requires ``typnamespace = 'pg_catalog'``, so a
# user-defined type spelled ``money`` is never matched -- verified on PostgreSQL 17.11, a
# ``cdcprobe.money[]`` enum column is not returned while a ``pg_catalog.money[]`` one is.
# An array of a user-defined ENUM must NOT be listed here: enums arrive as plain strings
# and replicate correctly.
_CDC_UNCARRYABLE_ARRAY_ELEMENTS = {
    # (A) the whole ROW dead-letters -- no row reaches the target at all, for EVERY change
    # to that table, because the sink throws on the first unrenderable column.
    #   timetz[]  -- Debezium's ZonedTime normalises to UTC and discards the offset that
    #                to_jsonb preserves.
    #   bytea[]   -- the spelling depends on the SOURCE's bytea_output GUC, which the
    #                change event does not carry.
    #   numeric[] -- ONLY when UNCONSTRAINED (see classify_uncarryable_cdc_column):
    #                Debezium sends VariableScaleDecimal and stripTrailingZeros() has
    #                already run, so the display scale is gone before the sink sees it.
    "timetz": UNCARRYABLE_ROW_DEAD_LETTERS,
    "bytea": UNCARRYABLE_ROW_DEAD_LETTERS,
    "numeric": UNCARRYABLE_ROW_DEAD_LETTERS,
    # (B) only that COLUMN is lost: the row lands, nothing dead-letters, and ONLY the
    # Validation CHECKSUM can see it. Two mechanisms, one outcome (the column is NULL) --
    # for interval/varbit/money/xml/point/name the SOURCE connector logs "No converter
    # found for column ... The column will not be part of change events for that table"
    # (established from the shipped Debezium 2.7.4 jar and confirmed live); for bit(n)[]
    # the field IS present and the connector's CONVERSION fails ("Failed to properly
    # convert data value ... Failed to read value of array").
    "interval": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "varbit": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "bit": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "money": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "xml": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "point": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "name": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    # The types the shipped Debezium 2.7.4 PostgresValueConverter has no converter for at
    # all (scalar or array: it handles none of PGbox/PGcircle/PGline/PGlseg/PGpath/
    # PGpolygon, nor tsquery), so the column is left out of every change event.
    "box": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "circle": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "line": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "lseg": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "path": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "polygon": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "tsquery": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    "tsvector": UNCARRYABLE_COLUMN_ARRIVES_NULL,
    # (B) but WRONG rather than absent: to_jsonb writes ["42"] (a quoted string) while the
    # event carries an unnamed int64 array, so the target renders [42].
    "oid": UNCARRYABLE_COLUMN_ARRIVES_WRONG,
}

# SCALAR types with the same problem. Scalar timetz / interval / money / xml / point have
# real Debezium converters and replicate fine (a point since sink plugin v54, which writes it
# as its own text rather than WKB bytes) -- it is only their ARRAY forms above that do not --
# so listing them here would warn about columns that work. The types with no converter at
# all are listed in both maps.
#
# ``bit`` / ``varbit`` are the exception and are NOT in this map: their outcome depends on
# the DECLARED WIDTH, so they are handled by the length-aware arm in
# :func:`classify_uncarryable_cdc_column`. An earlier version of this comment asserted that
# scalar bit/varbit "replicate fine"; live measurement on Aurora PostgreSQL 17.7 refuted it
# for three of the four width bands (see that function).
_CDC_UNCARRYABLE_SCALARS = {
    name: UNCARRYABLE_COLUMN_ARRIVES_NULL
    for name in ("tsvector", "tsquery", "box", "circle", "line", "lseg", "path", "polygon")
}

# The widest bit string the remodeled target column can hold. MUST stay equal to the sink's
# own ``DebeziumTypeConverter.MAX_STORABLE_BIT_LENGTH`` (Aurora DSQL's documented
# ``character varying`` limit): the sink REFUSES a declared length above it, so this is the
# boundary between "dead-letters" and "arrives". Pinned by a test so the two cannot drift.
_MAX_STORABLE_BIT_LENGTH = 65535

# The candidate names the PROBE filters on server-side, so the query returns only columns
# worth classifying instead of every column of every selected table. Derived from the maps
# above rather than spelled out again, so the SQL filter cannot drift from the classifier.
CDC_UNCARRYABLE_CANDIDATE_ARRAY_ELEMENTS = tuple(sorted(_CDC_UNCARRYABLE_ARRAY_ELEMENTS))
# ``bit``/``varbit`` are appended explicitly: they are not in the scalar MAP (their reason
# depends on the declared width, see classify_uncarryable_cdc_column) but the probe must
# still RETURN them, or the classifier never gets the chance to grade them.
CDC_UNCARRYABLE_CANDIDATE_SCALARS = tuple(
    sorted(set(_CDC_UNCARRYABLE_SCALARS) | {"bit", "varbit"})
)


def classify_uncarryable_cdc_column(
    *, is_array: bool, base_type_name: Optional[str], element_typmod: int
) -> Optional[str]:
    """Why CDC cannot carry this column, or ``None`` when it can.

    Pure, so the whole type taxonomy is unit-testable without a PostgreSQL. Takes the raw
    catalog discriminators the probe reads -- whether the column's effective type is an
    array, the pg_catalog name of its element (or of the scalar itself), and the element's
    ``atttypmod`` -- and applies the tables above.

    ``numeric`` is the one type whose MODIFIER decides: ``numeric(p,s)[]`` carries fine
    (Debezium sends a fixed-scale Decimal), while an UNCONSTRAINED ``numeric[]`` becomes a
    VariableScaleDecimal whose display scale is already lost. Verified on PostgreSQL 17.11:
    ``atttypmod`` is -1 for ``numeric[]``, 655366 for ``numeric(10,2)[]`` and 655364 for
    ``numeric(10)[]`` -- and for a DOMAIN over ``numeric(10,2)[]`` the modifier sits on
    ``pg_type.typtypmod``, which the probe's query folds in.

    Deliberately silent on the DATA-DEPENDENT losses, which no catalog read can see: a
    date/timestamp/timestamptz array element outside years 0001..9999 (incl. +/-infinity),
    ``time '24:00:00'``, a NaN element in an otherwise-fine ``numeric(p,s)[]``, and a
    multi-dimensional value (``attndims`` is only the DECLARED dimensionality and
    PostgreSQL enforces it in neither direction -- see
    ``_PG_CDC_UNCARRYABLE_COLUMNS_SQL``).
    """
    if not base_type_name:
        return None
    if not is_array:
        if base_type_name in ("bit", "varbit"):
            return _classify_scalar_bit_width(base_type_name, element_typmod)
        return _CDC_UNCARRYABLE_SCALARS.get(base_type_name)
    if base_type_name == "numeric" and element_typmod != -1:
        return None
    return _CDC_UNCARRYABLE_ARRAY_ELEMENTS.get(base_type_name)


# format_type spellings whose pg_catalog name differs (the classifier keys on the latter).
_FORMAT_TYPE_CATALOG_NAMES = {"time with time zone": "timetz", "bit varying": "varbit"}


def _catalog_discriminators(declared: str) -> "tuple[bool, str, int]":
    """``(is_array, pg_catalog base name, atttypmod stand-in)`` from a ``format_type``
    spelling such as ``numeric(10,2)[]``, ``bit varying(5)`` or ``time with time zone[]``.
    The typmod stand-in is what the classifier reads: -1 for an unconstrained numeric or an
    unbounded bit varying, the declared width for bit / bit varying, and 1 for any
    constrained numeric (only "is it -1" matters there)."""
    text = " ".join((declared or "").lower().split())
    is_array = text.endswith("[]")
    while text.endswith("[]"):
        text = text[:-2].rstrip()
    modifier = re.search(r"\(([^)]*)\)", text)
    base = " ".join(re.sub(r"\([^)]*\)", "", text).split())
    if base.startswith("interval"):
        base = "interval"  # interval day to second, interval(3)
    base = _FORMAT_TYPE_CATALOG_NAMES.get(base, base)
    first = (modifier.group(1).split(",")[0].strip() if modifier else "")
    if base in ("bit", "varbit"):
        if first.isdigit():
            typmod = int(first)
        else:
            typmod = 1 if base == "bit" else -1  # bare bit means bit(1)
    elif base == "numeric":
        typmod = 1 if modifier else -1
    else:
        typmod = -1
    return is_array, base, typmod


def uncarryable_columns_of(table: TableDef) -> "list[UncarryableCdcColumn]":
    """The columns of ``table`` CDC cannot carry, judged from the inventory's own column
    types -- the same verdict the Step 3 probe reaches from the catalog, for Evaluation,
    which reads no catalog for it. A DOMAIN over such a type is not seen here (the inventory
    names the domain); the probe still sees it. Pure."""
    found: list[UncarryableCdcColumn] = []
    for column in table.columns:
        is_array, base, typmod = _catalog_discriminators(column.mysql_type)
        reason = classify_uncarryable_cdc_column(
            is_array=is_array, base_type_name=base, element_typmod=typmod
        )
        if reason is not None:
            found.append(
                UncarryableCdcColumn(
                    column=column.name,
                    declared_type=column.mysql_type,
                    reason=reason,
                    nullable=column.nullable,
                )
            )
    return found


def _classify_scalar_bit_width(base_type_name: str, typmod: int) -> Optional[str]:
    """Grade a SCALAR ``bit(n)`` / ``bit varying(n)`` by its declared width.

    Three of the four width bands lose data, all live-measured on Aurora PostgreSQL 17.7
    against the shipped sink, and NONE of them is fixable in the sink at any plugin version
    -- which is why this has to be a prerequisite:

    * **unbounded, or wider than 65535** -> the whole ROW dead-letters. An unbounded ``bit
      varying`` has ``atttypmod = -1`` and Debezium reports its length as 2147483647, and
      the sink refuses any declared length above
      :data:`_MAX_STORABLE_BIT_LENGTH` (it once tried to pad to it and the JVM raised
      ``OutOfMemoryError``, killing the task). ``bit varying(70000)`` dead-letters the same
      way. This is the CORRECT sink behaviour -- the value cannot be stored -- so the fix
      belongs here, telling the operator before the billable infrastructure exists.
    * **width <= 1** (bare ``bit``, ``bit(1)``, ``bit varying(1)``) -> the column arrives
      WRONG, silently: Debezium maps a 1-bit column to a Connect BOOLEAN, so the value
      lands as ``true``/``false`` where the source (and Full Load) render ``1``/``0``. The
      wire carries a bare boolean indistinguishable from a real PostgreSQL ``boolean``, so
      the sink cannot recover the source type.
    * **``bit varying(n)``, 2..65535** -> arrives WRONG when a value is SHORTER than ``n``:
      the Bits payload carries no per-value length, so the sink left-pads to the declared
      width (``bit varying(8)``: ``1010`` -> ``00001010``). An exactly-``n``-bit value round
      trips, so this warns on a column whose values may all happen to be full width -- kept
      because variable length is the entire purpose of ``varbit``, and the warning is
      non-blocking and names the consequence.
    * **``bit(n)``, 2..65535** -> exact. Fixed width means the padding IS the value.
    """
    if typmod == -1 or typmod > _MAX_STORABLE_BIT_LENGTH:
        return UNCARRYABLE_ROW_DEAD_LETTERS
    if typmod <= 1:
        return UNCARRYABLE_COLUMN_ARRIVES_WRONG
    if base_type_name == "varbit":
        return UNCARRYABLE_COLUMN_ARRIVES_WRONG
    return None


@dataclass(frozen=True)
class UncarryableCdcColumn:
    """One column of one table that PostgreSQL CDC cannot carry, and why.

    ``declared_type`` is the source's own ``format_type`` spelling (e.g. ``time with time
    zone[]``, or a domain's own name), so the prerequisite row can NAME the column and its
    type instead of describing a category. ``reason`` is one of the ``UNCARRYABLE_*`` codes.
    """

    column: str
    declared_type: str
    reason: str
    # NOT NULL on the source (and so on the target): a column left out of the change event
    # then has every insert and update refused (23502), not landed NULL. None: not read.
    nullable: Optional[bool] = None


@dataclass(frozen=True)
class PostgresCdcFacts:
    """Read-only source facts for the PostgreSQL CDC prerequisite checks.

    Every field is best-effort: ``None`` means the probe could not read it (e.g.
    insufficient privilege), which the checks treat as "unknown" (a non-blocking INFO)
    rather than a false failure. ``replica_identity`` maps a qualified ``schema.table``
    to its ``pg_class.relreplident`` code.
    """

    wal_level: Optional[str] = None
    # Tri-state, NOT plain bools: these three used to default to ``False``, which for
    # ``is_in_recovery`` meant an unreadable source produced a confidently green
    # "Source accepts writes (pg_is_in_recovery=false)" on a REQUIRED check -- a
    # fail-OPEN on the one fact that decides whether a slot can exist at all. ``None``
    # now means "not read", which the checks surface as a non-blocking INFO instead of
    # asserting a fact nobody verified.
    is_superuser: Optional[bool] = None
    has_replication_role: Optional[bool] = None
    max_replication_slots: Optional[int] = None
    used_replication_slots: Optional[int] = None
    max_wal_senders: Optional[int] = None
    # ``None`` when the connection cannot see OTHER backends' ``backend_type``: a
    # non-superuser outside ``pg_monitor`` gets its own row only, so counting walsenders
    # silently returned 0 and the exhaustion WARN could never fire -- dead in exactly the
    # least-privilege setup the tool recommends. Unknown is reported as unknown.
    used_wal_senders: Optional[int] = None
    is_in_recovery: Optional[bool] = None
    replica_identity: Mapping[str, str] = field(default_factory=dict)
    # Per-table replicability facts, all keyed by the same qualified ``schema.table``.
    #
    # ``leaf_replica_identity`` maps a PARTITIONED parent to ``{leaf qname: relreplident}``.
    # This is not redundant with ``replica_identity``: the tool migrates the parent (the
    # children are dropped from the inventory by ``_pg_apply_partitioning``), but a
    # publication on a parent expands to the LEAVES and PostgreSQL enforces and logs the
    # LEAF's identity -- so ``ALTER TABLE <parent> REPLICA IDENTITY FULL`` leaves every
    # leaf untouched and a leaf with 'nothing' makes UPDATE/DELETE on the parent ERROR.
    leaf_replica_identity: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    # For ``relreplident='i'``: whether a live ``pg_index.indisreplident`` index still
    # backs it. PostgreSQL does NOT reset ``relreplident`` when that index is dropped, so
    # the table keeps reporting 'i' while behaving exactly like 'nothing'.
    identity_index_valid: Mapping[str, bool] = field(default_factory=dict)
    # For ``relreplident='i'``: whether that index is the PRIMARY KEY. When it is not, the
    # UPDATE/DELETE before-image carries only the index's columns, so a primary-key column
    # outside that index is not carried. (Offline-proven against the shipped Debezium 2.7.4
    # jar: its key generator fills an absent NOT NULL key column with a placeholder -- 0, ''
    # or the column's DEFAULT -- not NULL, so the change addresses a row that is not there.)
    identity_index_is_primary: Mapping[str, bool] = field(default_factory=dict)
    # ``relpersistence='u'``: an UNLOGGED table cannot be published at all.
    unlogged: Mapping[str, bool] = field(default_factory=dict)
    # Per selected table, the columns CDC cannot carry (see
    # :func:`classify_uncarryable_cdc_column`). ``None`` -- NOT an empty dict -- means the
    # catalog read failed, because the two are different answers: an absent/empty entry
    # means "read, nothing offending" (PASS) while None means "unknown" and must degrade
    # to a non-blocking INFO. The same tri-state discipline as the fields above: a defaulted
    # {} would assert a clean bill of health nobody verified, and this is the only check
    # that looks at column types at all, so nothing else would contradict it.
    cdc_uncarryable_columns: Optional[Mapping[str, Sequence[UncarryableCdcColumn]]] = None
    # Whether the source user can CREATE a publication: CREATE on the database AND
    # ownership of every table to be published. ``tables_not_owned`` names the offenders
    # so the remediation is actionable rather than "permission denied".
    has_database_create: Optional[bool] = None
    tables_not_owned: Sequence[str] = field(default_factory=tuple)
    # ``max_slot_wal_keep_size`` in MEGABYTES (PG13+). ``-1`` means unlimited: the server
    # never discards WAL a slot still needs. Any other value caps that retention, so a
    # slow Full Load can outlive the slot's WAL and invalidate it.
    max_slot_wal_keep_size_mb: Optional[int] = None
    # Do CDC's objects EXIST? Only read when the run must FIND them (a CDC-only start);
    # ``None`` means not read, never a defaulted bool -- the same tri-state discipline as
    # the facts above, and for the same reason: a defaulted False here would assert an
    # absence nobody verified and block the deploy on it.
    publication_present: Optional[bool] = None
    publication_publishes_all_dml: Optional[bool] = None
    slot_present_any_database: Optional[bool] = None
    slot_usable: Optional[bool] = None
    publication_tables: Sequence[str] = field(default_factory=tuple)
    # The names actually checked, so the row can name them and a stack rename cannot make
    # the row silently describe a different object than the connector will use.
    checked_publication_name: str = ""
    checked_slot_name: str = ""
    # G20: per selected table, why publishing it would make its UPDATE/DELETE fail on the
    # source -- the value-free phrase of ``cdc_pg_slot.replica_identity_problem``, graded
    # from the live catalog by the SAME rule the Full Load's provisioning and the one-table
    # ADD refuse on (a valid immediate primary key behind DEFAULT, a valid unique identity
    # index behind USING INDEX, every partition of a partitioned table) -- or ``None`` for
    # a table that is fine. ``None`` for the whole map means not read (a failed read, or
    # facts built without it): grading then falls back to the relreplident code alone, as
    # before. A table absent from a provided map was not found as a plain or partitioned
    # table, and is graded by its code too.
    replica_identity_problems: Optional[Mapping[str, Optional[str]]] = None
    # G25, from the same read: per table with a problem, the fix for THAT cause
    # (``cdc_pg_slot.replica_identity_remedy``, with the exact quoted statement where FULL is
    # it), so this check, the Full Load's refusal and its per-table row name the same fix.
    # ``None`` when not read; a table missing from it gets the general advice.
    replica_identity_remedies: Optional[Mapping[str, str]] = None
    # G25: the tables on which PostgreSQL 18+ refuses UPDATE/DELETE under a published
    # REPLICA IDENTITY FULL -- they have a generated column, which CDC's publication does not
    # publish -- so no remedy here may offer FULL for them. Absent means not refused or not
    # read (PostgreSQL 13-17 never refuse it).
    full_identity_refused: Mapping[str, bool] = field(default_factory=dict)
    # G25: per selected plain table with inheritance (INHERITS) child tables, ``(child count,
    # first child names)`` (``cdc_pg_slot.read_inheritance_children``). ``None`` when not
    # read; a table missing from a read map has none.
    inheritance_children: Optional[Mapping[str, tuple]] = None


def check_wal_level_logical(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """PASS when ``wal_level=logical`` -- required for pgoutput logical replication."""
    known = facts.wal_level is not None
    ok = (facts.wal_level or "").strip().lower() == "logical"
    if not known:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
            title="Source wal_level is 'logical'",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail="Could not read wal_level on the source.",
            remediation=(
                "Verify wal_level=logical on the source (the connection lacked the "
                "privilege to read it)."
            ),
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        title="Source wal_level is 'logical'",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail=(
            "wal_level=logical."
            if ok
            else f"wal_level is '{facts.wal_level}', not 'logical'."
        ),
        remediation=""
        if ok
        else (
            "Enable logical replication on the source and reboot. On RDS/Aurora set "
            "the parameter-group value rds.logical_replication=1 (a static parameter "
            "-- it requires a reboot); on self-managed PostgreSQL set wal_level=logical "
            "and restart."
        ),
    )


def check_replication_role(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """PASS when the source user can create a logical replication slot.

    That needs the REPLICATION role attribute or, on RDS/Aurora (where the attribute
    cannot be granted), membership in the ``rds_replication`` role; a superuser has it
    implicitly. Not the community REPLICATION *privilege* -- RDS blocks that.
    """
    if facts.is_superuser is None and facts.has_replication_role is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICATION_ROLE,
            title="Source user can create a replication slot",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail="Could not read the source user's replication rights.",
            remediation=(
                "Confirm the source user has replication rights (rds_replication "
                "membership on RDS/Aurora, the REPLICATION attribute on self-managed)."
            ),
        )
    ok = bool(facts.is_superuser) or bool(facts.has_replication_role)
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICATION_ROLE,
        title="Source user can create a replication slot",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Replication role present."
        if ok
        else "The source user lacks the REPLICATION role / rds_replication membership.",
        remediation=""
        if ok
        else (
            "Grant the source user replication rights: on RDS/Aurora "
            "GRANT rds_replication TO <user>; on self-managed PostgreSQL "
            "ALTER ROLE <user> WITH REPLICATION. Prefer a dedicated least-privilege "
            "user over an admin account."
        ),
    )


def check_replication_slot_headroom(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """WARN (non-blocking) when there is no room for another logical slot / WAL sender.

    A new slot needs a free ``max_replication_slots`` entry AND a free ``max_wal_senders``
    (the connector's walsender). A source whose OTHER replication (read replicas, other
    CDC) has consumed the wal_sender pool has slot room but no sender room, so the connector
    would fail to attach at Start with a confusing "all replication slots are in use"-style
    error -- catch it here (WARN) instead. Unknown counts (unreadable) are a non-blocking
    INFO.
    """
    walsenders_full = (
        facts.used_wal_senders is not None
        and facts.max_wal_senders is not None
        and facts.used_wal_senders >= facts.max_wal_senders
    )
    if facts.max_replication_slots is None or facts.used_replication_slots is None:
        status = PrerequisiteStatus.INFO
        detail = "Could not read replication-slot capacity on the source."
    elif facts.max_replication_slots < 1 or (
        facts.max_wal_senders is not None and facts.max_wal_senders < 1
    ):
        # A CONFIGURED zero is categorically different from a full pool: nothing can be
        # freed, so no CDC slot can ever be created on this source, and the remediation
        # ("drop an unused slot") cannot work. Logical replication is switched off outright
        # -- and because the Full Load takes the snapshot through the same slot, the run
        # cannot even start. Blocking, with a remediation that says so.
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICATION_SLOTS,
            title="Source has replication-slot headroom",
            status=PrerequisiteStatus.FAIL,
            required=True,
            detail=(
                f"max_replication_slots={facts.max_replication_slots}, "
                f"max_wal_senders={facts.max_wal_senders}: replication is disabled on "
                "this source, so no CDC slot can be created at all."
            ),
            remediation=(
                "Raise max_replication_slots and max_wal_senders above 0 on the source "
                "(both are static parameters -- a reboot is required on RDS/Aurora). "
                "Nothing can be freed to make room: at zero the setting itself forbids "
                "every replication slot, so the Full Load + CDC run cannot begin."
            ),
        )
    elif (
        facts.used_replication_slots >= facts.max_replication_slots
        or (facts.max_wal_senders is not None and facts.max_wal_senders < 1)
        or walsenders_full
    ):
        status = PrerequisiteStatus.WARN
        senders = (
            f"{facts.used_wal_senders}/{facts.max_wal_senders}"
            if facts.used_wal_senders is not None
            else str(facts.max_wal_senders)
        )
        detail = (
            f"{facts.used_replication_slots}/{facts.max_replication_slots} replication "
            f"slots in use (wal_senders={senders}); no headroom for a new CDC slot/sender."
        )
    else:
        status = PrerequisiteStatus.PASS
        detail = (
            f"{facts.used_replication_slots}/{facts.max_replication_slots} slots in use "
            f"(max_wal_senders={facts.max_wal_senders})."
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICATION_SLOTS,
        title="Source has replication-slot headroom",
        status=status,
        required=False,  # non-blocking: a stale slot can be freed
        detail=detail,
        remediation=""
        if status in (PrerequisiteStatus.PASS, PrerequisiteStatus.INFO)
        else (
            "Free or raise max_replication_slots / max_wal_senders on the source "
            "(both are static parameters and need a reboot on RDS/Aurora), or drop an "
            "unused replication slot, so CDC can create its slot."
        ),
    )


def check_source_is_writer(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """PASS when the source is a writer (not a standby): a standby cannot host a slot.

    An UNREADABLE ``pg_is_in_recovery()`` is a non-blocking INFO, not a PASS. It used to
    default to ``False``, so a source whose probe failed produced a confidently green
    "Source accepts writes (pg_is_in_recovery=false)" on a REQUIRED check -- asserting a
    specific catalog value nobody had read, and failing OPEN on the one fact that decides
    whether a replication slot can exist at all.
    """
    if facts.is_in_recovery is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.SOURCE_IS_WRITER,
            title="Source is a writer (not a standby)",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail="Could not read whether the source is a standby.",
            remediation=(
                "Confirm the connection points at the cluster WRITER endpoint: a standby "
                "cannot create the logical replication slot CDC needs."
            ),
        )
    ok = not facts.is_in_recovery
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SOURCE_IS_WRITER,
        title="Source is a writer (not a standby)",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Source accepts writes (pg_is_in_recovery=false)."
        if ok
        else "Source is a read replica / standby (pg_is_in_recovery=true).",
        remediation=""
        if ok
        else (
            "Point CDC at the writer (primary) instance: a standby cannot create a "
            "logical replication slot. Use the cluster writer endpoint, not a reader."
        ),
    )


def check_replica_identity(
    table: TableDef, facts: PostgresCdcFacts
) -> PrerequisiteResult:
    """PASS when ``table`` has a REPLICA IDENTITY usable for UPDATE/DELETE replication.

    'n' (nothing) -- on the table or on one of its partitions -- makes an UPDATE or DELETE
    on it ERROR on the publisher, and so does an 'i' whose index is gone. Beyond the code,
    the table is graded from the source's live catalog by the rule the Full Load's
    provisioning refuses on before any write (``facts.replica_identity_problems``, G20):
    'd' needs a valid primary key that is not DEFERRABLE -- PostgreSQL keeps 'd' when the
    key is dropped, and the Step 1 inventory ``check_table_primary_key`` reads can predate
    that -- 'i' a valid unique identity index, and a partitioned table a usable identity on
    every partition. Any of those is a required FAIL, whose remedy is the one for its cause
    (``facts.replica_identity_remedies``, G25). When that grading was not read, the
    code alone decides, as before ('d'/'f'/'i' PASS). An identity index that is not the
    primary key is a WARN; an unknown/unreadable identity is a non-blocking INFO (the
    connector, and the Full Load's own fail-closed check, remain the final authority).
    A printed statement quotes the table's name; no remedy offers FULL for a table
    PostgreSQL 18 refuses it on (``facts.full_identity_refused``).
    """
    identity = facts.replica_identity.get(table.name)
    quoted = _quoted(table.name)
    full_refused = bool(facts.full_identity_refused.get(table.name))
    if identity is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
            title="Table has a usable REPLICA IDENTITY",
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail="Could not read the table's REPLICA IDENTITY.",
        )

    def _fail(detail: str, remediation: str) -> PrerequisiteResult:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
            title="Table has a usable REPLICA IDENTITY",
            status=PrerequisiteStatus.FAIL,
            required=True,
            target=table.name,
            detail=detail,
            remediation=remediation,
        )

    # A PARTITIONED parent is what the migration selects (``_pg_apply_partitioning``
    # drops the children), but a publication on a parent expands to the LEAVES and
    # PostgreSQL enforces and logs the LEAF's identity. Live-verified: with the parent set
    # to REPLICA IDENTITY FULL and one leaf set to NOTHING, an UPDATE through the parent
    # still fails -- 'cannot update table "<leaf>" because it does not have a replica
    # identity and publishes updates'. So the parent's own code says nothing about whether
    # the table is replicable, and ALTER TABLE on the parent does not fix a leaf.
    unusable_leaves = sorted(
        leaf
        for leaf, code in (facts.leaf_replica_identity.get(table.name) or {}).items()
        if code not in _USABLE_REPLICA_IDENTITY
    )
    if unusable_leaves:
        shown = ", ".join(unusable_leaves[:5])
        more = (
            f" (and {len(unusable_leaves) - 5} more)" if len(unusable_leaves) > 5 else ""
        )
        return _fail(
            f"{len(unusable_leaves)} partition(s) of {table.name} have REPLICA IDENTITY "
            f"'nothing': {shown}{more}. PostgreSQL enforces the PARTITION's identity, "
            "so UPDATE/DELETE through the parent would fail on the publisher.",
            "Set the identity on each PARTITION, not the parent -- ALTER TABLE on the "
            "parent does not propagate to existing partitions. For each one run "
            + (
                "ALTER TABLE <partition> REPLICA IDENTITY DEFAULT with a primary key that "
                "is not DEFERRABLE (on PostgreSQL 18, FULL fails every UPDATE and DELETE "
                "of a published table with generated columns)"
                if full_refused
                else "ALTER TABLE <partition> REPLICA IDENTITY FULL (or DEFAULT if it has "
                "a primary key)"
            )
            + ", and set it on new partitions as they are created.",
        )

    if identity not in _USABLE_REPLICA_IDENTITY:
        # PostgreSQL 18's generated-column rule is checked on the table an UPDATE/DELETE
        # runs on: a partitioned parent's own identity never is (its partitions' are, and
        # they are graded above), so FULL on the parent stays a fix (G25).
        partitioned = table.partitioned or table.name in facts.leaf_replica_identity
        return _fail(
            "REPLICA IDENTITY is 'nothing' -- UPDATE/DELETE would fail on the publisher.",
            f"Give {table.name} a primary key that is not DEFERRABLE and run ALTER TABLE "
            f"{quoted} REPLICA IDENTITY DEFAULT: on PostgreSQL 18, FULL fails every UPDATE "
            "and DELETE of a published table with generated columns."
            if full_refused and not partitioned
            else f"Set a REPLICA IDENTITY on {table.name}: with a primary key the default is "
            f"enough; otherwise run ALTER TABLE {quoted} REPLICA IDENTITY FULL so Debezium "
            "can replicate UPDATE/DELETE.",
        )

    if identity == "i":
        # PostgreSQL does NOT reset relreplident when the index backing
        # REPLICA IDENTITY USING INDEX is dropped (live-verified on PG 16): the column
        # stays 'i' while the table behaves exactly like 'nothing', so accepting 'i' by
        # code alone graded a table PASS whose every UPDATE/DELETE the source then
        # refuses. ``pg_index.indisreplident`` is the authoritative signal.
        if facts.identity_index_valid.get(table.name) is False:
            return _fail(
                "REPLICA IDENTITY is 'index' but no valid index backs it (the designated "
                "index was dropped), so the table behaves as 'nothing' and UPDATE/DELETE "
                "would fail on the publisher.",
                f"Re-point the identity on {table.name}: recreate the unique index and "
                f"run ALTER TABLE {quoted} REPLICA IDENTITY USING INDEX <index>, or use "
                "REPLICA IDENTITY DEFAULT (primary key)"
                + ("." if full_refused else " / FULL."),
            )

    # G20: the live-catalog grading the Full Load's provisioning refuses on before any
    # write -- the same rule, so the operator learns it here, before the load. It stays
    # AHEAD of the non-PK index WARN below (do not merge the two 'i' blocks): a partitioned
    # parent can have a valid non-PK identity index while a partition has no usable key.
    problems = facts.replica_identity_problems
    problem = problems.get(table.name) if problems is not None else None
    if problem:
        # Lazy: cdc_pg_slot imports source_dialect, which imports this module.
        from dsql_migrator.core.cdc_pg_slot import (
            INDEX_GONE_PROBLEM,
            NO_PRIMARY_KEY_PROBLEM,
        )

        remedy = (facts.replica_identity_remedies or {}).get(table.name)
        # Only a key or index that is GONE leaves its setting behind; a DEFERRABLE key, a
        # generated column or a partition is there to see (G25).
        kept = (
            " PostgreSQL keeps the REPLICA IDENTITY setting when the key or index behind "
            "it is dropped."
            if problem in (NO_PRIMARY_KEY_PROBLEM, INDEX_GONE_PROBLEM)
            else ""
        )
        return _fail(
            f"{table.name} has no usable REPLICA IDENTITY on the source ({problem}), so "
            "once it is in CDC's publication every UPDATE and DELETE on it would fail "
            f"there.{kept}",
            f"To fix {table.name}, {remedy}, then re-run the prerequisite checks."
            if remedy
            else f"Give {table.name} a valid primary key that is not DEFERRABLE, or run "
            f"ALTER TABLE {quoted} REPLICA IDENTITY FULL (on a partitioned table, on each "
            "partition: ALTER TABLE on the parent does not reach existing partitions), "
            "then re-run the prerequisite checks.",
        )

    if identity == "i":
        # A non-PK identity index publishes only ITS columns in the before-image, so a
        # primary-key column outside that index is not carried for UPDATE/DELETE (Debezium
        # fills an absent NOT NULL key column with a placeholder -- 0, '' or its DEFAULT --
        # offline-proven against the shipped jar, not NULL). Non-blocking (the load itself
        # is fine and the user may have keyed the sink differently), but it is a real
        # change-replication risk, so WARN rather than a silent PASS.
        if facts.identity_index_is_primary.get(table.name) is False:
            return PrerequisiteResult(
                check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
                title="Table has a usable REPLICA IDENTITY",
                status=PrerequisiteStatus.WARN,
                required=False,
                target=table.name,
                detail=(
                    "REPLICA IDENTITY is an index that is NOT the primary key, so an "
                    "UPDATE/DELETE before-image carries only that index's columns: a "
                    "primary-key column outside it is not carried, and the change cannot "
                    "locate its row on the target."
                ),
                remediation=(
                    f"Prefer ALTER TABLE {quoted} REPLICA IDENTITY DEFAULT (the primary "
                    f"key){'' if full_refused else ' or FULL'}, so every replicated change "
                    "carries the key Aurora DSQL needs."
                ),
            )

    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
        title="Table has a usable REPLICA IDENTITY",
        status=PrerequisiteStatus.PASS,
        required=True,
        target=table.name,
        detail="REPLICA IDENTITY is set for change replication.",
    )


def check_replica_identity_covers_key(
    table: TableDef,
    facts: PostgresCdcFacts,
    key_columns: Sequence[str],
) -> Optional[PrerequisiteResult]:
    """Can the before-image supply every column the CDC record is KEYED on?

    A different question from :func:`check_replica_identity`, with a different remedy, so it
    is a separate check (the precedent this repo already set for PUBLICATION_PRIVILEGE vs
    CDC_REPLICATION_OBJECTS). That one asks "is an identity set at all" and accepts 'd';
    this one asks whether 'd' is ENOUGH -- and for a re-keyed table it is not.

    **The failure it exists to stop is silent.** When Schema Conversion's per-table
    "Composite key" option prepends a high-cardinality column to the target primary key (a
    DSQL hot-partition remedy), a DELETE must carry that column to find its row. Under
    ``REPLICA IDENTITY DEFAULT`` PostgreSQL limits the UPDATE/DELETE before-image to the
    SOURCE primary key, so the prepended column is ABSENT from a DELETE, and on a record keyed
    on the composite key (``message.key.columns``) the delete does not remove its row on
    Aurora DSQL -- no error, no dead-letter, no log. Live-observed on Aurora PostgreSQL 17.7:
    one deleted order stayed on the target while its child rows (no re-key, so keyed on the
    source PK) deleted correctly.

    How it is lost is not the sink's NULL-key guard: Debezium's key generator converts the
    absent column with no null check, and for a NOT NULL column (a composite key's leading
    column always is) yields a placeholder -- 0, '' or the column's DEFAULT -- so the record
    key is non-NULL and addresses a row that is not there (or, when the key omits part of the
    source primary key, possibly a different row). Live-confirmed on PostgreSQL 17
    (2026-10-03): the DELETE reached the sink keyed ``{lead: 7, id: 2}`` -- 7 the column
    DEFAULT, for a row whose lead was 5 -- matched 0 rows, and nothing was dead-lettered.
    Since G15, CDC infrastructure with sink plugin v51 or later keys such a table on its SOURCE
    primary key and deletes by it once Aurora DSQL's unique index on it is valid; an UPDATE that
    changes only the added column (it arrives with no old image) then replaces the row holding
    that key. FULL is still required: until that index is valid and on infrastructure deployed
    by an earlier version a weak identity still loses the change (such an UPDATE is
    dead-lettered, the old row kept). INSERTs and other UPDATEs are not affected -- their
    after-image carries every column.

    The tool DOES widen the identity itself (``cdc_pg_slot.set_replica_identity_full``), but
    only on the path where the Full Load provisions the replication slot. A Full-load-only
    run continued to CDC, a CDC-only start, an identity reset after provisioning, and a
    PARTITIONED table (``ALTER TABLE`` on the parent does not reach existing partitions,
    while PostgreSQL logs the LEAF's identity) all reach streaming with the identity still
    DEFAULT. This check verifies the source's actual catalog instead of trusting that the
    widening ran, so every route is covered by the same gate -- and the CDC monitor re-reads
    the same catalog facts while CDC runs and grades them with the SAME rule
    (:func:`replica_identity_covers_key_result`), so an identity dropped after Start holds
    Cut over.

    Returns ``None`` when there is nothing to grade: no re-key for this table, or the key
    adds nothing beyond the source primary key (a pure reorder). Pure.
    """
    weak_leaves = sorted(
        leaf
        for leaf, code in (facts.leaf_replica_identity.get(table.name) or {}).items()
        if code != "f"
    )
    return replica_identity_covers_key_result(
        table,
        key_columns,
        identity=facts.replica_identity.get(table.name),
        weak_partition_names=weak_leaves,
        weak_partition_count=len(weak_leaves),
        full_refused=bool(facts.full_identity_refused.get(table.name)),
    )


def replica_identity_covers_key_result(
    table: TableDef,
    key_columns: Sequence[str],
    *,
    identity: Optional[str],
    weak_partition_names: Sequence[str],
    weak_partition_count: int,
    full_refused: bool = False,
) -> Optional[PrerequisiteResult]:
    """The REPLICA_IDENTITY_COVERS_KEY rule on explicit catalog facts. Pure.

    Shared by the Start prerequisite (:func:`check_replica_identity_covers_key`) and the CDC
    monitor's runtime re-check (``cdc_pg_slot.grade_rekeyed_replica_identity``), so a state
    that passes Start can never raise the runtime finding and the reverse. In order: ``None``
    when the key adds nothing beyond ``table.primary_key``; INFO when ``identity`` is unknown;
    FAIL when any partition is not FULL (``weak_partition_count``; at most five of
    ``weak_partition_names`` are named); PASS on 'f'; FAIL on 'd', 'n' and 'i'.
    ``full_refused`` (PostgreSQL 18, a generated column: a published FULL refuses every
    UPDATE/DELETE) makes 'f' a FAIL too, and every FAIL's remedy removing the re-key (G25).
    """
    extra = [c for c in (key_columns or []) if c not in set(table.primary_key or ())]
    if not extra:
        return None
    title = "Re-keyed table's before-image carries its composite key"
    key_desc = ", ".join(key_columns)
    missing = ", ".join(extra)
    if identity is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY,
            title=title,
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail=(
                "Could not read the table's REPLICA IDENTITY, so it is unknown whether a "
                f"DELETE would carry {missing}."
            ),
        )

    # FULL is no remedy when PostgreSQL 18 refuses it: published, it fails the table's every
    # UPDATE/DELETE -- so every FAIL then says to remove the re-key instead (G25).
    keep_source_pk = (
        f"Set {table.name} back to \"Keep source PK\" in Schema Conversion, which removes "
        "the re-key and the need for REPLICA IDENTITY FULL: on PostgreSQL 18, FULL fails "
        "every UPDATE and DELETE of a published table with generated columns, and DEFAULT "
        "does not carry the composite key."
    )

    def _fail(detail: str, remediation: str) -> PrerequisiteResult:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
            target=table.name,
            detail=detail,
            remediation=keep_source_pk if full_refused else remediation,
        )

    # PARTITIONS FIRST, and they are graded even when the parent itself is FULL: PostgreSQL
    # enforces and logs the LEAF's identity, and ALTER TABLE on a parent does not propagate
    # to existing partitions (live-verified elsewhere in this module). So a re-keyed
    # partitioned table whose parent the tool widened still loses deletes on every leaf.
    count = max(int(weak_partition_count or 0), 0)
    if count:
        names = list(weak_partition_names or ())[:5]
        shown = ", ".join(names)
        more = f" (and {count - len(names)} more)" if count > len(names) else ""
        return _fail(
            f"The Aurora DSQL primary key of {table.name} is ({key_desc}), which adds "
            f"{missing} beyond the source primary key, but {count} partition(s) are not "
            f"REPLICA IDENTITY FULL: {shown}{more}. PostgreSQL logs the PARTITION's "
            f"identity, so a DELETE would not carry {missing} and can miss its row on "
            "Aurora DSQL with no error.",
            "Run ALTER TABLE <partition> REPLICA IDENTITY FULL on each PARTITION (the "
            "parent does not propagate to existing partitions), and set it on new "
            "partitions as they are created. Keep them FULL for the whole run: while CDC "
            "runs the tool re-checks it and holds Cut over if it drops.",
        )

    if identity == "f" and full_refused:
        # FULL carries the key, but on PostgreSQL 18 it is the very identity that fails
        # the table's UPDATE/DELETE once published (the REPLICA_IDENTITY check refuses it
        # too), and the identity it then needs, DEFAULT, does not carry the key (G25).
        return _fail(
            f"REPLICA IDENTITY FULL carries {missing}, but {table.name} has generated "
            "columns: on PostgreSQL 18 a published FULL table with generated columns fails "
            f"every UPDATE and DELETE on the source, and DEFAULT does not carry {missing}.",
            keep_source_pk,
        )

    if identity == "f":
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY,
            title=title,
            status=PrerequisiteStatus.PASS,
            required=True,
            target=table.name,
            detail=(
                f"REPLICA IDENTITY FULL, so a DELETE carries {missing} and the composite key "
                f"({key_desc}) locates the target row."
            ),
        )

    # Everything else -- 'd', 'n', and 'i' whether or not the index is the primary key --
    # publishes at most the primary key (or that index's columns) in the before-image, so
    # the added key column is absent. 'i' is graded FAIL rather than probed for its column
    # list: FULL is the remedy either way, and a WARN here would let the silent loss ship.
    _reason = {
        "d": (
            "REPLICA IDENTITY is DEFAULT, so the UPDATE/DELETE before-image carries only "
            "the source primary key"
        ),
        "n": (
            "REPLICA IDENTITY is 'nothing', so there is no before-image at all (and "
            "UPDATE/DELETE would fail on the publisher)"
        ),
    }.get(
        identity,
        "REPLICA IDENTITY is an index, so the before-image carries only that index's "
        "columns",
    )
    return _fail(
        f"The Aurora DSQL primary key of {table.name} is ({key_desc}) -- the composite key "
        f"chosen in Schema Conversion -- which adds {missing} beyond the source primary "
        f"key ({', '.join(table.primary_key or ()) or 'none'}). {_reason}, so a DELETE, or "
        f"an UPDATE that changes the source primary key, does not carry {missing} and can miss "
        "its row on Aurora DSQL with no error and no dead letter; an UPDATE of "
        f"{missing} alone can be dead-lettered (a unique constraint rejected the row) and "
        "leave the old row. INSERTs and other UPDATEs are not affected (their after-image "
        "carries every column).",
        f"Run ALTER TABLE {_quoted(table.name)} REPLICA IDENTITY FULL on the source, then "
        "re-run these checks. DEFAULT is not enough for a re-keyed table. The alternative is to "
        f"set {table.name} back to \"Keep source PK\" in Schema Conversion, which removes "
        "the re-key and the requirement with it. Keep it FULL for the whole run: while CDC "
        "runs the tool re-checks it and holds Cut over if it drops.",
    )


def check_table_replicable(
    table: TableDef, facts: PostgresCdcFacts
) -> PrerequisiteResult:
    """FAIL when the table cannot be in a publication at all (UNLOGGED).

    An UNLOGGED table's changes are never WAL-logged, so PostgreSQL refuses to add it to a
    publication -- which aborts the ENTIRE ``CREATE PUBLICATION`` for the migration, not
    just that table, surfacing as a raw driver error at Start CDC. Caught here with the
    table named instead.
    """
    is_unlogged = facts.unlogged.get(table.name)
    if is_unlogged is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.TABLE_REPLICABLE,
            title="Table can be replicated (not UNLOGGED)",
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail="Could not read the table's persistence.",
        )
    if not is_unlogged:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.TABLE_REPLICABLE,
            title="Table can be replicated (not UNLOGGED)",
            status=PrerequisiteStatus.PASS,
            required=True,
            target=table.name,
            detail="Table is logged, so its changes reach the WAL.",
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TABLE_REPLICABLE,
        title="Table can be replicated (not UNLOGGED)",
        status=PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail=(
            "Table is UNLOGGED: its changes never reach the WAL, so PostgreSQL rejects it "
            "from a publication and the whole CDC publication creation fails."
        ),
        remediation=(
            f"Either convert it with ALTER TABLE {_quoted(table.name)} SET LOGGED, or "
            "deselect it and migrate it with Full Load only (an UNLOGGED table can never "
            "be change-replicated)."
        ),
    )


_ROW_SECURITY_TITLE = "Source user reads every row (row-level security)"


def check_row_security(
    table: TableDef, facts: "RowSecurityFacts"
) -> Optional[PrerequisiteResult]:
    """Whether the source user reads EVERY row of ``table`` despite its row-level security,
    or ``None`` when row-level security is not enabled on it. Pure. Every mode.

    The tool reads the source with ``row_security = off``, as pg_dump does, so a user the
    policies apply to has the Full Load and Validation stop on this table with "query would
    be affected by row-level security policy" instead of copying -- and then validating --
    only the rows the policies show it. That is the loud failure the setting exists for;
    this row says so before the run, as a required FAIL, rather than mid-load. A user that
    is exempt (a superuser, BYPASSRLS, or the owner of a table that is not FORCE ROW LEVEL
    SECURITY) PASSes; an unreadable login is a non-blocking INFO, since the setting still
    turns a subset into a refusal. That the policies themselves do not exist on Aurora DSQL
    is Evaluation's finding (``PG_ROW_LEVEL_SECURITY``), not a precondition of the load.
    """
    from dsql_migrator.core.catalog_postgres import row_security_exemption

    entry = next((t for t in facts.tables if t.table == table.name), None)
    if entry is None:
        return None
    exemption = row_security_exemption(entry, facts.login)
    if exemption is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.ROW_SECURITY,
            title=_ROW_SECURITY_TITLE,
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail=(
                f"Row-level security is enabled on {table.name}, and whether it applies to "
                "the source user could not be read."
            ),
            remediation=(
                "If it applies to the source user, the Full Load and Validation stop on "
                f"{table.name} with an error (the tool reads with row_security = off): run "
                "the migration as a superuser, a role with BYPASSRLS, or the table's owner."
            ),
        )
    login = facts.login.name if facts.login is not None else "the source user"
    if exemption:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.ROW_SECURITY,
            title=_ROW_SECURITY_TITLE,
            status=PrerequisiteStatus.PASS,
            required=True,
            target=table.name,
            detail=(
                f"Row-level security is enabled on {table.name}; the source user {login} "
                f"reads every row as {exemption}."
            ),
        )
    ownership = (
        "it owns the table, but FORCE ROW LEVEL SECURITY applies it to the owner too"
        if entry.login_owns
        else f"it is not the table's owner, {entry.owner}"
    )
    # With no policy, what this user would have read is nothing at all (default deny).
    instead = (
        "copy and compare only the rows its policies show this user"
        if entry.policies
        else "copy an empty table (it has no policy, so it shows this user no rows)"
    )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.ROW_SECURITY,
        title=_ROW_SECURITY_TITLE,
        status=PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail=(
            f"Row-level security is enabled on {table.name} and applies to the source user "
            f"{login} (not a superuser, no BYPASSRLS, and {ownership}). The tool reads "
            "with row_security = off, as pg_dump does, so the Full Load and Validation "
            f"would stop on {table.name} rather than {instead}."
        ),
        remediation=(
            f"Run the migration as a role that reads every row of {table.name}: a superuser "
            "or a role with BYPASSRLS"
            + (
                " (not the owner: FORCE ROW LEVEL SECURITY subjects it to the policies)"
                if entry.forced
                else f", or its owner {entry.owner}"
            )
            + f". Or untick {table.name}."
        ),
    )


_INHERITANCE_TITLE = "Inheritance child tables are migrated as tables of their own"


def check_inheritance_children(
    table: TableDef, facts: PostgresCdcFacts, *, snapshots: bool, started: bool = False
) -> Optional[PrerequisiteResult]:
    """The rows of ``table``'s inheritance (INHERITS) child tables are not migrated into it
    (G25) -- or ``None`` when it has none, or that was not read (Start CDC reads it again
    and refuses a snapshot it cannot check). Pure.

    Aurora DSQL has no table inheritance, and CDC captures only the table's own changes (its
    publication lists it ``ONLY``), so the Full Load and Validation read only its own rows
    too (``FROM ONLY``), and each child table migrates as a table of its own, when it is
    selected. A non-blocking WARN: a child table that is not selected is not migrated at
    all, and a query of the table on Aurora DSQL no longer returns its child tables' rows. A
    required FAIL when this run's Start CDC snapshots (``snapshots``: CDC only, or a recorded
    re-snapshot): Debezium's snapshot reads the table WITH its child tables' rows, which CDC
    then never changes on Aurora DSQL, so Start CDC refuses it
    (``cdc_pg_slot.pg_replication_objects_blocker``) -- said here, before the billable CDC
    infrastructure is deployed. Its fix is a Full load + CDC run, a new start point, so on a
    stack whose connectors were started before (``started``) the route rule's route follows
    it, new CDC topics first, as in the Start CDC dialog's block (G25, rule B).
    """
    count, sample = (facts.inheritance_children or {}).get(table.name) or (0, ())
    count, sample = int(count or 0), tuple(sample or ())
    if count <= 0:
        return None
    shown = ", ".join(sample) + (" …" if count > len(sample) else "")
    children = f"{count} inheritance (INHERITS) child table{'s' if count != 1 else ''}: {shown}"
    if snapshots:
        route = ""
        if started:
            # Lazy: cdc_pg_slot imports source_dialect, which imports this module.
            from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER, pg_cdc_route

            route = " " + pg_cdc_route(INHERITANCE_SNAPSHOT_HEADER, started=True, cdc_only=None)
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.TABLE_REPLICABLE,
            title=_INHERITANCE_TITLE,
            status=PrerequisiteStatus.FAIL,
            required=True,
            target=table.name,
            detail=(
                f"{table.name} has {children}. This migration's Start CDC takes a snapshot "
                f"(snapshot.mode=initial), and Debezium's snapshot reads {table.name} with "
                f"its child tables' rows: they would be copied into {table.name} on Aurora "
                "DSQL, where CDC never updates or deletes them, so Start CDC refuses it."
            ),
            remediation=(
                f"Untick {table.name} (each child table migrates as a table of its own), or "
                'migrate it in a "Full load + CDC" run whose CDC starts from its Full Load\'s '
                f"snapshot point: that Full Load reads only {table.name}'s own rows, and its "
                f"CDC takes no snapshot.{route}"
            ),
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TABLE_REPLICABLE,
        title=_INHERITANCE_TITLE,
        status=PrerequisiteStatus.WARN,
        required=False,
        target=table.name,
        detail=(
            f"{table.name} has {children}. Aurora DSQL has no table inheritance, so the Full "
            f"Load, CDC and Validation take only {table.name}'s own rows (FROM ONLY): the "
            "rows of a child table migrate only as that table, and only if it is selected. A "
            f"query of {table.name} that returns its child tables' rows on the source "
            "returns only its own on Aurora DSQL."
        ),
        remediation=(
            "Select each child table whose rows you need: it migrates as a table of its own "
            "and needs a primary key of its own (PostgreSQL does not inherit one)."
        ),
    )


_COLUMN_REPLICABLE_TITLE = "Every column's type can be carried by CDC"


def _describe_columns(columns: "Sequence[UncarryableCdcColumn]") -> str:
    """``\\`col\\` (type)`` for each column, in catalog order -- the house listing shape."""
    return ", ".join(f"`{c.column}` ({c.declared_type})" for c in columns)


def _is_are(columns: "Sequence[UncarryableCdcColumn]") -> str:
    return "is" if len(columns) == 1 else "are"


def _it_they(columns: "Sequence[UncarryableCdcColumn]") -> str:
    return "it arrives" if len(columns) == 1 else "they arrive"


def check_columns_replicable(
    table: TableDef, facts: PostgresCdcFacts, *, excluded: "Iterable[str]" = ()
) -> Optional[PrerequisiteResult]:
    """WARN when a column's type cannot survive the CDC hop, NAMING each column and type.

    One level below :func:`check_table_replicable`: the table publishes fine, but
    :func:`classify_uncarryable_cdc_column` found a column whose PostgreSQL type a Debezium
    pgoutput event cannot carry to the jsonb target. Two consequences, and the row states
    which applies because they read nothing alike -- "every change to this table
    dead-letters" is not "this column arrives NULL".

    Returns ``None`` when the table has no such column, deliberately: no per-table PASS.
    The precedent is :func:`prerequisites.check_source_value_size`, the other non-blocking,
    TYPE-CONDITIONAL per-table check ("a schema of ordinary columns adds no row to the
    report") -- and it is the right one, because a clean table is the norm here. The
    per-table PASS rows this module does emit (TABLE_REPLICABLE, REPLICA_IDENTITY) are
    REQUIRED gates, where the PASS row is the evidence a blocking check cleared; a green
    row on every table for a non-blocking check would bury the few tables that matter.
    Unreadable facts still get their INFO row -- unknown is reported as unknown.

    **WARN, not FAIL, and that is argued rather than lax** -- including for the
    dead-lettering class, which loses every row of the table:

    * A required FAIL would block the FULL LOAD, not just the deploy. Executed:
      ``PrerequisiteReport.build`` sets ``can_proceed=False``, and
      ``full_load_run_guard_reason`` ends in ``prerequisite_block_reason(report,
      mode=CDC)`` -- the mode a "Full load + CDC" run checks -- so the Run button goes
      dead. Full Load carries every one of these columns CORRECTLY (the exporter reads
      ``CAST(to_jsonb(col) AS text)``), so a FAIL would stop the one operation that works.
    * The operator decides, and every option is a choice, not a fix: exclude the column
      in Exclude tables & columns (it then migrates as NULL and CDC never captures it --
      an ``excluded`` column is not judged here at all), remodel the type on the source,
      or leave the table out of the selection. A FAIL would hold the Full Load over a
      choice.
    * The consequence shape is ``SOURCE_VALUE_SIZE``'s, not ``TABLE_REPLICABLE``'s: rows
      the target cannot store, dead-lettered VISIBLY, which that check grades WARN /
      ``required=False`` as "a decision the operator owns". An UNLOGGED table, by contrast,
      aborts the whole ``CREATE PUBLICATION`` for every other table too.
    * The gap is recoverable: the table is correct after Full Load and correct again after
      a reload with writes stopped -- specifically a **'Drop & reload'**, because an
      append reload only inserts primary keys the target is missing and so cannot rewrite
      the NULL a row already carries -- and Validation reports it meanwhile.

    The silent class is WARN rather than INFO for the opposite reason: ``INFO`` means
    expected / optional / no action needed, and a column that arrives NULL with no error is
    none of those -- it fails the table's CHECKSUM, which the cut-over gate reads.
    """
    per_table = facts.cdc_uncarryable_columns
    if per_table is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.COLUMN_REPLICABLE,
            title=_COLUMN_REPLICABLE_TITLE,
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail=(
                "Could not read the table's column types, so it is unknown whether any "
                "column holds a type CDC cannot carry."
            ),
            remediation=(
                "Confirm the migration user can read pg_catalog (pg_attribute, pg_type), "
                "then re-run the checks."
            ),
        )
    # Only columns this migration still writes: an excluded column is never captured.
    skip = set(excluded or ())
    found = [c for c in (per_table.get(table.name) or ()) if c.column not in skip]
    if not found:
        return None
    detail, remediation = uncarryable_column_texts(
        found, defaulted=target_defaulted_columns(table)
    )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.COLUMN_REPLICABLE,
        title=_COLUMN_REPLICABLE_TITLE,
        status=PrerequisiteStatus.WARN,
        required=False,
        target=table.name,
        detail=detail,
        remediation=remediation,
    )


def target_defaulted_columns(table: TableDef) -> "set[str]":
    """The columns of a PostgreSQL ``table`` that Schema Conversion gives a DEFAULT on the
    target: an insert that leaves one out takes it rather than NULL. Pure."""
    from dsql_migrator.core.converter_postgres import pg_column_default_sql

    return {
        column.name
        for column in table.columns
        if pg_column_default_sql(column, is_key_column=column.name in table.primary_key)[0]
        is not None
    }


def uncarryable_column_texts(
    found: "Sequence[UncarryableCdcColumn]",
    *,
    defaulted: "Iterable[str]" = (),
) -> "tuple[str, str]":
    """``(what happens, what to do)`` for ``found``: the Step 3 ``COLUMN_REPLICABLE`` row's
    wording, which Evaluation's ``CDC_UNCARRYABLE_COLUMN`` finding repeats, so both steps
    describe the same columns the same way. ``defaulted`` names the columns that have a
    DEFAULT on the target. Pure."""
    fatal = [c for c in found if c.reason == UNCARRYABLE_ROW_DEAD_LETTERS]
    has_default = set(defaulted)
    lost = [c for c in found if c.reason == UNCARRYABLE_COLUMN_ARRIVES_NULL]
    # A bit(n)[] stays IN the event, as a field Debezium failed to convert: a nullable one
    # is NULL on every insert and update, but a NOT NULL one is a required field left null,
    # which stops the PostgreSQL source connector (event.processing.failure.handling.mode
    # defaults to fail) -- nothing is dead-lettered, and every captured table stops.
    present = [c for c in lost if _catalog_discriminators(c.declared_type)[:2] == (True, "bit")]
    stopped = [c for c in present if c.nullable is False]
    nulled_always = [c for c in present if c.nullable is not False]
    # The others are left OUT of the event, so the sink's upsert never names them: an
    # insert takes the column's DEFAULT (or NULL) and an update leaves it as it was -- but
    # a NOT NULL one with no DEFAULT has every insert and update refused, because Aurora
    # DSQL checks NOT NULL before ON CONFLICT.
    dropped = [c for c in lost if c not in present]
    refused = [c for c in dropped if c.nullable is False and c.column not in has_default]
    landed_default = [c for c in dropped if c.column in has_default]
    nulled = [c for c in dropped if c.nullable is not False and c.column not in has_default]
    wrong = [c for c in found if c.reason == UNCARRYABLE_COLUMN_ARRIVES_WRONG]

    sentences: list[str] = []
    if fatal:
        sentences.append(
            f"CDC cannot render {_describe_columns(fatal)} for the remodeled target column, "
            "and a Debezium change event carries every column, so the sink rejects the whole "
            "record: every insert, update and delete that POPULATES this column dead-letters "
            "(counted on the CDC step's dead-letter panel) and does not reach the target. A "
            "row that leaves the column NULL still lands -- the sink's converter is only "
            "reached for a non-NULL value, live-verified -- so this is not necessarily the "
            "whole table."
        )
    if stopped:
        sentences.append(
            f"{_describe_columns(stopped)} {_is_are(stopped)} NOT NULL, and Debezium cannot "
            f"convert {'it' if len(stopped) == 1 else 'them'}: the first change to this table "
            "leaves a required field null, which STOPS the PostgreSQL source connector -- "
            "CDC halts for every captured table, and nothing is dead-lettered."
        )
    if refused:
        sentences.append(
            f"{_describe_columns(refused)} {_is_are(refused)} dropped from the change event "
            "but NOT NULL with no DEFAULT on the target, so Aurora DSQL refuses EVERY insert "
            "and update CDC writes for this table (SQLSTATE 23502): they are dead-lettered "
            "(counted on the CDC step's dead-letter panel) and do not reach the target."
        )
    # "Separately" because when the table already dead-letters, a column-level loss is a
    # SECOND finding about the same table rather than a milder description of the first.
    lead = "Separately, " if fatal or refused or stopped else ""
    if nulled_always:
        sentences.append(
            f"{lead}{_describe_columns(nulled_always)} {_is_are(nulled_always)} sent "
            f"unconverted, so {_it_they(nulled_always)} NULL on every row CDC delivers."
        )
        lead = ""
    for group, lands in ((nulled, "NULL"), (landed_default, "the column's DEFAULT")):
        if group:
            sentences.append(
                f"{lead}{_describe_columns(group)} {_is_are(group)} dropped from the change "
                f"event, so a row CDC inserts gets {lands} and an update leaves "
                f"{'it' if len(group) == 1 else 'them'} as before (stale)."
            )
            lead = ""
    if wrong:
        sentences.append(
            f"{lead}{_describe_columns(wrong)} "
            f"{'arrives' if len(wrong) == 1 else 'arrive'} with a different value than "
            "Full Load writes for the same row."
        )
    if nulled or wrong:
        sentences.append(
            "That column-level loss is SILENT: the row lands, nothing is dead-lettered, "
            "nothing is logged, and only a Validation CHECKSUM can see it."
        )
    sentences.append(
        "Full Load itself carries all of these correctly -- it reads each value on the "
        "source in the form the target column takes (jsonb for an array) -- so the "
        "divergence exists only on rows that arrive via CDC."
    )

    # The one route that genuinely repairs the type on the source, offered only when it
    # applies: numeric(p,s)[] carries fine, an unconstrained numeric[] does not.
    numeric_route = (
        "give an unconstrained numeric[] a precision and scale on the source "
        "(numeric(p,s)[] IS carried), "
        if any(c.declared_type.startswith("numeric[") for c in fatal)
        else ""
    )
    not_null_route = (
        "drop the NOT NULL from the target DDL in Schema Conversion before applying (the "
        "column then lands NULL, as on a nullable one), "
        if refused
        else ""
    )
    remediation = (
        "Decide before the CDC infrastructure is created: "
        + numeric_route
        + not_null_route
        + "exclude the column in Exclude tables & columns (under the table list; it "
        "then migrates as NULL and CDC never captures it), "
        "remodel the column on the source to a type CDC does carry (text / text[] "
        "carries every one of these), or leave this table out of the selection and "
        "migrate it with Full Load only -- the capture set follows the table "
        "selection, so there is no per-table \"load but do not stream\" switch. "
        "Accepting it is also a valid choice: every other table streams normally, "
        "Validation will report this one as a mismatch, and the table is made correct "
        "again by stopping writes to it and using the Full Load step's per-table "
        "Reload before you cut over."
    )
    return " ".join(sentences), remediation


def check_publication_privilege(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """FAIL when the source user cannot CREATE the publication CDC needs.

    Separate from :func:`check_replication_role`, which covers the SLOT. PostgreSQL also
    requires CREATE on the database plus OWNERSHIP of every published table, so a user
    granted exactly the documented replication rights still fails at the first step of a
    CDC start -- after the gate said "can proceed" and after the Full Load has run.
    """
    if facts.is_superuser:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
            title="Source user can create the CDC publication",
            status=PrerequisiteStatus.PASS,
            required=True,
            detail="Superuser: publication creation and table ownership are implicit.",
        )
    if facts.has_database_create is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
            title="Source user can create the CDC publication",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail="Could not read the source user's CREATE privilege on the database.",
            remediation=(
                "Confirm the source user has CREATE on the database and owns every "
                "selected table; CDC needs both to create its publication."
            ),
        )
    missing: list[str] = []
    if not facts.has_database_create:
        missing.append("CREATE on the database")
    if facts.tables_not_owned:
        shown = ", ".join(list(facts.tables_not_owned)[:5])
        more = (
            f" (and {len(facts.tables_not_owned) - 5} more)"
            if len(facts.tables_not_owned) > 5
            else ""
        )
        missing.append(f"ownership of {shown}{more}")
    if not missing:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
            title="Source user can create the CDC publication",
            status=PrerequisiteStatus.PASS,
            required=True,
            detail="CREATE on the database, and every selected table is owned.",
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
        title="Source user can create the CDC publication",
        status=PrerequisiteStatus.FAIL,
        required=True,
        detail="The source user cannot create the publication: missing "
        + "; ".join(missing)
        + ".",
        remediation=(
            "CDC creates a publication for exactly the selected tables, which needs "
            "CREATE on the database and ownership of each one. Grant CREATE ON DATABASE "
            "<db> TO <user>, and make the user a member of each table's owning role "
            "(GRANT <owner> TO <user>) -- membership is enough, a transfer of ownership "
            "is not required."
        ),
    )


def check_postgres_cdc_facts_unavailable() -> PrerequisiteResult:
    """A blocking FAIL when the source CDC readiness could not be probed at all.

    For a PostgreSQL source in CDC mode, ``None`` facts mean the read-only probe failed
    (unreachable source, insufficient privilege) -- NOT that CDC is unsupported. CDC must
    not proceed against a source whose logical-replication readiness (wal_level, slot
    role, REPLICA IDENTITY) is unverified: starting it could create a publication that
    breaks source UPDATE/DELETE, or resume from a slot that cannot be created. So this is a
    required FAIL, not an advisory INFO.
    """
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        title="PostgreSQL CDC readiness could not be verified",
        status=PrerequisiteStatus.FAIL,
        required=True,
        detail="Could not read the source's logical-replication settings.",
        remediation=(
            "Verify the source is reachable and the migration user can read the "
            "server settings, then re-run the checks. CDC will not start until the "
            "PostgreSQL logical-replication prerequisites can be confirmed."
        ),
    )


def check_slot_wal_retention(facts: PostgresCdcFacts) -> PrerequisiteResult:
    """WARN when the source may discard WAL the CDC slot still needs.

    The PostgreSQL counterpart of :func:`prerequisites.check_binlog_retention`, and the
    same failure shape: CDC resumes from the replication slot created at the Full Load
    snapshot point, so the source must still hold that WAL when CDC begins. With a finite
    ``max_slot_wal_keep_size`` the server is free to discard it and INVALIDATE the slot
    (``pg_replication_slots.wal_status = 'lost'``) -- after which the connector cannot
    resume from the watermark, so the changes made during the load are never replayed: a
    SILENT data gap, exactly what the MySQL binlog check exists to prevent.

    Follows that check's calibration deliberately. ``-1`` (unlimited, and the PostgreSQL
    default) is a PASS. A finite cap never hard-blocks -- WARN / ``required=False``, per
    Property 14 -- because a fast load followed by a prompt Start CDC can easily fit
    inside the cap; it is a real but non-obvious risk, not a certainty. Unknown (the probe
    lacked the privilege, or a pre-13 server has no such GUC) degrades to a non-blocking
    INFO rather than a false alarm.
    """
    cap = facts.max_slot_wal_keep_size_mb
    if cap is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.SLOT_WAL_RETENTION,
            title="WAL retention covers the CDC handoff",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail="Could not read max_slot_wal_keep_size on the source.",
            remediation=(
                "Check max_slot_wal_keep_size on the source: -1 (unlimited) guarantees "
                "the replication slot keeps the WAL that CDC resumes from."
            ),
        )
    if cap < 0:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.SLOT_WAL_RETENTION,
            title="WAL retention covers the CDC handoff",
            status=PrerequisiteStatus.PASS,
            required=False,
            detail=(
                "max_slot_wal_keep_size is -1 (unlimited), so the source keeps every WAL "
                "segment the replication slot still needs."
            ),
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SLOT_WAL_RETENTION,
        title="WAL retention covers the CDC handoff",
        status=PrerequisiteStatus.WARN,
        required=False,
        detail=(
            f"max_slot_wal_keep_size caps slot WAL retention at {cap} MB, so the source "
            "may discard WAL the CDC slot still needs and invalidate it."
        ),
        remediation=(
            "If Full Load takes long enough to write more than this much WAL, the slot is "
            "invalidated and the changes made during the load are lost — CDC cannot "
            "resume from the watermark. Raise max_slot_wal_keep_size (or set -1 for "
            "unlimited) before starting, or start CDC promptly after the load and watch "
            "the slot's WAL-pressure panel."
        ),
    )


def check_cdc_replication_objects(
    facts: PostgresCdcFacts,
    tables: Sequence[TableDef],
    *,
    provisions_replication: bool,
    cdc_start_resnapshots: bool = False,
    pending_tables: Sequence[str] = (),
    cdc_stack_started: bool = False,
    _scope: str = "selected tables",
) -> PrerequisiteResult:
    """Do CDC's publication + replication slot EXIST? Distinct from the PRIVILEGE check.

    ``provisions_replication`` is the whole non-regression guarantee. In "Full load + CDC"
    the Full Load CREATES both at the snapshot point, so they are EXPECTED to be absent
    beforehand -- this must read as SKIP there, never as a problem. Only a run that has to
    FIND them (a CDC-only start) is graded.

    The SKIP detail states that CONDITIONALLY, and must keep doing so. Start CDC is reachable
    under the combined tile with NO Full Load at all (nothing gates on the load), so a detail
    asserting "the Full Load creates them for this run" is false for that session -- and "they
    must not exist beforehand" was false even on the happy path, since ``create_publication``
    REUSES a publication that covers the table set and ``provision_pg_replication`` DROPS a
    same-named stale slot. Gating instead of re-wording is NOT available: with no watermark the
    tool cannot tell "the load is still coming" from "there will never be one", and guessing
    would re-block the deploy-the-infra-DURING-the-load flow the tool itself recommends
    (v0.1.509/510). Start's own probe is the gate for that route; this row must not claim a
    past or future fact it cannot know.

    Grading: an absent publication, or one that omits a selected table or narrows its
    publish list, is a FAIL -- each makes the connector either die at once or report
    RUNNING while replicating nothing. A missing SLOT is only a WARN when no slot is
    RECORDED, because then ``build_pg_source_config`` selects ``initial`` and the connector
    creates its own -- a re-read, not a gap.

    ``cdc_start_resnapshots`` is the operator's recorded decision to re-snapshot
    (``publication.autocreate.mode=filtered`` + ``snapshot.mode=initial``). It re-grades the
    two absences it genuinely repairs to a non-blocking WARN, which is what lets ONE
    re-graded report clear every downstream gate at once instead of leaving a red FAIL
    beside a button the operator has already been told how to unblock. It also splits the
    final PASS: present objects mean nothing is MISSING, NOT that this start resumes from the
    slot. ``pg_snapshot_mode`` keys ``never`` on the slot RECORDED on this run's watermark,
    never on the slot found on the source, so only that branch may promise a resume.

    ``pending_tables`` (G17: selected tables a Full load + CDC Full Load did not load, whose
    guarded load -- Retry unfinished tables -- adds each to this publication before its copy
    starts) are not counted as missing from it, so this row cannot hold the very Retry that
    adds them; the passing detail says so. A CAPTURED table missing from the publication
    still FAILs, and the ownership and REPLICA IDENTITY rows keep grading the pending ones.
    The rest of the selection is graded as "the selected tables CDC captures now"
    (``_scope``, private), so its count never reads as the whole selection's, and with no
    such table left the row says the publication "exists" instead of "covers all 0". The
    pending sentence is added only where their guarded load CAN add them -- a usable slot
    and a publication that publishes every change type; otherwise the row already blocks or
    warns, and their load would refuse them.

    ``cdc_stack_started`` (G25, route rule B): the CDC stack's connectors were started before
    (``_cdc_status.cdc_stack_has_streamed``: stopped since, in place, or a Start
    CloudFormation rolled back). Its Start continues from the committed offset -- Debezium
    skips the snapshot once its source connector has streamed -- and its CDC topics may hold
    changes no sink applied, so none of the copy below holds there (a re-snapshot, "run as
    Full load + CDC", "nothing is lost"). The row is then INFO: what the source shows, and
    that the Start CDC dialog judges it against the stack's own record and names the route
    (``cdc_pg_slot.pg_cdc_route``) when the stream cannot continue as is.
    """
    title = "CDC's publication and replication slot exist on the source"
    if provisions_replication:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.SKIP,
            required=True,
            detail=(
                "Not graded here: this run's Full Load creates the publication and "
                "replication slot at its snapshot LSN, so absent is the expected state "
                "until the load has run. Start CDC checks both again, so a start that "
                "skips the load is still graded there."
            ),
        )
    pub = facts.checked_publication_name
    slot = facts.checked_slot_name
    if not pub or not slot:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.INFO,
            required=False,
            detail=(
                "The CDC stack name is not set yet, so the publication and slot names "
                "this run needs cannot be derived. They are checked again before Start CDC."
            ),
        )
    if facts.publication_present is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.INFO,
            required=False,
            detail=(
                "Could not read the source's publications and replication slots, so "
                f'whether "{pub}" and "{slot}" exist is unknown. They are checked again '
                "before Start CDC."
            ),
        )
    if cdc_stack_started:
        return _started_stack_objects_result(facts, tables, title=title, pub=pub, slot=slot)
    resnapshot_note = (
        "Otherwise choose the re-snapshot option on the CDC start card, which re-reads "
        "every selected table and loses nothing; a slot created now begins at the "
        "source's CURRENT WAL position and can never replay the changes committed since "
        "the Full Load."
    )
    gapless_note = (
        'For a handoff with no gap, run this migration as "Full load + CDC", which '
        "creates the publication and the slot at the snapshot point before the load. "
    )
    # The consequence of a re-snapshotting start, in the operator's terms. Shared by the
    # two branches that lead to one (no publication at all / publication but no slot), so
    # the same route cannot be described two different ways depending on which object the
    # source happens to be missing.
    #
    # WHY this is spelled out at all: an earlier version answered the no-publication case
    # with INFO and an EMPTY remediation, on the reasoning that a re-snapshotting start had
    # already "chosen" this. Nothing chooses it -- it is DERIVED from the absence of a slot
    # on the Full Load's watermark (``pg_snapshot_mode``), so the operator who ran
    # "Full load only" and came back for CDC was never told. This report is the one surface
    # on the critical path to the Deploy button (that button is disabled until the CDC
    # checks run), which is exactly why the disclosure belongs here and not only on a card.
    resnapshot_cost_note = (
        "Nothing is lost: there is no window between the snapshot and the stream, and the "
        "target apply is idempotent. The cost is that the whole selection is read from the "
        "source a second time and every row crosses MSK into DSQL -- through the streaming "
        "pipeline rather than the bulk loader, so expect it to take LONGER than the Full "
        "Load did. Rows DELETED on the source since the Full Load are also never removed "
        "from the target, because a fresh snapshot only reports the rows that still exist."
    )
    if not facts.publication_present:
        if cdc_start_resnapshots:
            return PrerequisiteResult(
                check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
                title=title,
                # WARN, not INFO: a second full read of a production source and a second
                # pass of the whole dataset through MSK is a real -- if non-blocking --
                # issue, and WARN is also what auto-expands the section so the row is read
                # rather than collapsed under a green badge. Verified that WARN keeps
                # ``can_proceed`` True and leaves ``cdc_prerequisite_block_reason`` None,
                # so it informs without blocking a route that is legitimate and lossless.
                status=PrerequisiteStatus.WARN,
                required=False,
                detail=(
                    f'Publication "{pub}" does not exist, so this start cannot resume from '
                    "a Full Load snapshot point. The connector creates its own publication "
                    "over exactly the captured tables and takes a FRESH SNAPSHOT of every "
                    "one of them before it streams any change."
                ),
                remediation=resnapshot_cost_note + " " + gapless_note.strip(),
            )
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
            resolvable_by_resnapshot=True,
            detail=(
                f'Publication "{pub}" does not exist on the source. The connector does '
                "not create it (publication.autocreate.mode=disabled), so the Debezium "
                "source task is killed seconds after the connectors are created: "
                '"Publication autocreation is disabled, please create one and restart '
                'the connector".'
            ),
            remediation=(
                "Fix this BEFORE deploying the CDC infrastructure — MSK Serverless and "
                "both connectors are billed from creation. " + gapless_note + resnapshot_note
            ),
        )
    published = set(facts.publication_tables or ())
    _names = {t.name for t in tables}
    pending = [
        n for n in dict.fromkeys(str(t) for t in pending_tables or ())
        if n in _names and n not in published
    ]
    if pending:
        # Graded over the other selected tables -- the ones CDC captures now -- then the
        # pending ones named (G17), where their load can add them.
        pending_set = set(pending)
        result = check_cdc_replication_objects(
            facts,
            [t for t in tables if t.name not in pending_set],
            provisions_replication=provisions_replication,
            cdc_start_resnapshots=cdc_start_resnapshots,
            _scope="selected tables CDC captures now",
        )
        if not facts.slot_usable or facts.publication_publishes_all_dml is False:
            return result
        return result.model_copy(
            update={"detail": f"{result.detail}{_pending_publication_sentence(pending)}"}
        )
    selected = [t.name for t in tables]
    # The graded set, worded for what it is (``_scope``); none left: the publication exists.
    covers = f"covers all {len(selected)} {_scope}" if selected else "exists"
    missing = sorted(set(selected) - published)
    if missing:
        shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
            detail=(
                f'Publication "{pub}" exists but does not include {len(missing)} of the '
                f"{len(selected)} {_scope}: {shown}."
            ),
            remediation=(
                "Those tables would replicate NOTHING while the connector reported "
                "RUNNING, which is worse than a visible failure. " + gapless_note
                + resnapshot_note
            ),
        )
    if facts.publication_publishes_all_dml is False:
        # Lazy: cdc_pg_slot imports source_dialect, which imports this module.
        from dsql_migrator.core.cdc_pg_slot import (
            PUBLISH_LIST_HEADER,
            pg_cdc_route,
            publish_list_fix,
        )

        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
            detail=(
                f'Publication "{pub}" is restricted to a subset of '
                "INSERT/UPDATE/DELETE, so the change types it omits would never reach "
                "the target while the connector still reported RUNNING."
            ),
            # Not a re-snapshot, nor a Full Load re-run: both reuse the publication as it is
            # (G25), which is also why a re-snapshot does not re-grade this row. Nor is the fix
            # alone enough (decision C): once the list is set back this row passes and the
            # Start CDC dialog's block no longer fires, but pgoutput judged each change against
            # the publication as it stood at its commit, so a Start that continues from the
            # slot would skip the changes committed while it was narrowed -- the route rule's
            # route follows the fix, as in that block. (A stack started before is INFO above.)
            remediation=(
                f"{publish_list_fix(pub)} first (a re-snapshot and a Full Load re-run both "
                "reuse the publication as it is), then re-run the prerequisite checks. "
                "Setting it back does not bring back the changes committed while it was "
                "narrowed, so a Start that continues from the slot would skip them. "
                + pg_cdc_route(PUBLISH_LIST_HEADER, started=False, cdc_only=None)
            ),
        )
    if not facts.slot_usable:
        if not cdc_start_resnapshots:
            # snapshot.mode=never with the slot GONE is the one unusable-slot state that
            # loses data, so it must FAIL rather than reassure. This run's watermark recorded
            # a slot name -- which is why the mode is ``never`` -- but the slot is not on the
            # source any more (a teardown drops it; an over-retained one is INVALIDATED with
            # wal_status='lost'). A ``never`` start does NOT snapshot, so Debezium creates a
            # replacement slot positioned at the CURRENT WAL and streams forward from there:
            # every change between the Full Load's consistency point and that moment is
            # skipped SILENTLY, with the connector reporting RUNNING. The re-snapshot
            # reassurance below is false here -- there is no snapshot to fall back on -- so
            # this row must block the spend, matching what ``pg_replication_objects_blocker``
            # already refuses at Start.
            return PrerequisiteResult(
                check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
                title=title,
                status=PrerequisiteStatus.FAIL,
                required=True,
                detail=(
                    f'Publication "{pub}" {covers}, but '
                    f'replication slot "{slot}" -- the one this run\'s Full Load recorded -- '
                    "is no longer on this database, so there is no WAL position to resume "
                    "from. This start is configured snapshot.mode=never and would NOT "
                    "re-snapshot: the connector would create a fresh slot at the current WAL "
                    "position and skip every change since the Full Load, silently, while "
                    "reporting RUNNING."
                ),
                remediation=(
                    "Fix this BEFORE deploying the CDC infrastructure — MSK Serverless and "
                    "both connectors are billed from creation. Re-run the Full Load under "
                    '"Full load + CDC" to create a new slot at a fresh consistency point, '
                    "or choose to re-snapshot every selected table instead (lossless, but "
                    "it reads the source again). " + gapless_note.strip()
                ),
            )
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.WARN,
            required=False,
            detail=(
                f'Publication "{pub}" {covers}, but '
                "there is no logical pgoutput replication slot named "
                f'"{slot}" on this database.'
            ),
            remediation=(
                "CDC can still start — Debezium creates the slot itself and takes a fresh "
                "snapshot of every selected table first. " + resnapshot_cost_note + " "
                + gapless_note.strip()
            ),
        )
    if cdc_start_resnapshots:
        # Nothing is MISSING -- but the slot's PRESENCE does not make this start a resume:
        # ``pg_snapshot_mode`` keys ``never`` on the slot RECORDED on this run's watermark,
        # not on the slot the probe FOUND, so this state ships ``snapshot.mode=initial`` and
        # the old sentence promised a resume on the one row an operator reads to confirm the
        # handoff. Still PASS, deliberately, and NOT the WARN the two absences above get:
        # there is no object to create and nothing to fix, the cause-keyed disclosure and its
        # remedy already render on the CDC start card (which, unlike this pure check, sees the
        # committed offset and so can tell a resume from a re-snapshot instead of hedging),
        # and the Deploy dialog withholds the same alarm from a stand-alone CDC-only start for
        # exactly that calibration reason -- the cause which reaches this branch most often.
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.PASS,
            required=True,
            detail=(
                f'Publication "{pub}" {covers} and '
                f'replication slot "{slot}" is present, so nothing this start needs is '
                "missing from the source. It is still configured snapshot.mode=initial "
                "rather than to resume from that slot: if this pipeline has already "
                "streamed it continues from the offset committed on MSK, otherwise the "
                "connector snapshots every selected table before any change streams. "
                "Nothing is lost either way, but a first start reads the whole selection "
                "through the streaming pipeline rather than the bulk loader. The CDC start "
                "card resolves which of the two applies."
            ),
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        title=title,
        status=PrerequisiteStatus.PASS,
        required=True,
        detail=(
            f'Publication "{pub}" {covers} and '
            f'replication slot "{slot}" is present, and this start is configured '
            "snapshot.mode=never, so CDC resumes from that slot without re-reading any "
            "table."
        ),
    )


def _pending_publication_sentence(pending: Sequence[str]) -> str:
    """C-PREREQ: the pending tables a guarded load adds to the publication (G17)."""
    names = list(pending)
    shown = ", ".join(names[:6]) + (f" (+{len(names) - 6} more)" if len(names) > 6 else "")
    one = len(names) == 1
    return (
        f" {shown} {'is' if one else 'are'} not in it yet: "
        f"{'its' if one else 'their'} load (Retry unfinished tables) adds "
        f"{'it' if one else 'them'} before {'its' if one else 'their'} copy starts."
    )


def _started_stack_objects_result(
    facts: PostgresCdcFacts, tables: Sequence[TableDef], *, title: str, pub: str, slot: str
) -> PrerequisiteResult:
    """CDC_REPLICATION_OBJECTS for a CDC stack whose connectors were started before (G25,
    route rule B): INFO -- what the source shows, then that the Start CDC dialog judges it
    against the stack's own record. That dialog is the one gate there: it names the route
    rule's route (new CDC topics first) when the stream cannot continue as is, so this row
    names no route of its own -- the never-started copy (a re-snapshot, "run as Full load +
    CDC", "nothing is lost") would contradict it. Pure."""
    selected = [t.name for t in tables]
    missing = set(selected) - set(facts.publication_tables or ())
    if not facts.publication_present:
        found = f'Publication "{pub}" does not exist on the source'
    elif missing:
        found = (
            f'Publication "{pub}" does not include {len(missing)} of the {len(selected)} '
            "selected tables"
        )
    elif facts.publication_publishes_all_dml is False:
        found = f'Publication "{pub}" does not publish every change type'
    elif not facts.slot_usable:
        found = (
            f'Publication "{pub}" covers the selected tables, but replication slot "{slot}" '
            "is not usable on this database"
        )
    else:
        found = f'Publication "{pub}" and replication slot "{slot}" are present'
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        title=title,
        status=PrerequisiteStatus.INFO,
        required=False,
        detail=(
            f"{found}. This CDC stack's connectors were started before, so Start CDC judges "
            "its publication and slot against the stack's own record: it continues from the "
            "slot when the stream can continue as is, and otherwise refuses the Start and "
            "names the way on, new CDC topics first (Debezium skips the snapshot once its "
            "source connector has streamed, so a re-snapshot cannot be relied on here)."
        ),
    )


def check_postgres_cdc_prerequisites(
    facts: PostgresCdcFacts,
    tables: Sequence[TableDef],
    *,
    provisions_replication: bool = True,
    cdc_start_resnapshots: bool = False,
    message_key_columns: Optional[Mapping[str, Sequence[str]]] = None,
    excluded_columns: "Optional[Mapping[str, Iterable[str]]]" = None,
    pending_tables: Sequence[str] = (),
    cdc_stack_started: bool = False,
) -> list[PrerequisiteResult]:
    """Run all PostgreSQL CDC readiness checks (global + per-table REPLICA IDENTITY).

    ``provisions_replication`` defaults True -- "this run creates the objects itself" --
    so every existing caller and the whole Full-load-+-CDC path is unchanged and the
    existence check can never block them. ``pending_tables`` (G17) and
    ``cdc_stack_started`` (G25) -- see :func:`check_cdc_replication_objects`.

    ``message_key_columns`` is the connector's re-key map (qualified table -> the target
    primary key the change record is keyed on). Only tables in it can fail
    :func:`check_replica_identity_covers_key`; an empty/None map means no table was re-keyed,
    so that check emits nothing at all.
    """
    results = [
        check_wal_level_logical(facts),
        check_replication_role(facts),
        check_publication_privilege(facts),
        check_cdc_replication_objects(
            facts,
            tables,
            provisions_replication=provisions_replication,
            cdc_start_resnapshots=cdc_start_resnapshots,
            pending_tables=pending_tables,
            cdc_stack_started=cdc_stack_started,
        ),
        check_replication_slot_headroom(facts),
        check_slot_wal_retention(facts),
        check_source_is_writer(facts),
    ]
    _keys = dict(message_key_columns or {})
    # Will Start CDC snapshot? The same signals the publication/slot row reads: a run that
    # still provisions its slot (Full load + CDC before its load) does not (G25).
    _snapshots = cdc_start_resnapshots and not provisions_replication
    for table in tables:
        results.append(check_table_replicable(table, facts))
        _inheritance = check_inheritance_children(
            table, facts, snapshots=_snapshots, started=cdc_stack_started
        )
        if _inheritance is not None:
            results.append(_inheritance)
        results.append(check_replica_identity(table, facts))
        _columns = check_columns_replicable(
            table, facts, excluded=(excluded_columns or {}).get(table.name, ()))
        if _columns is not None:
            results.append(_columns)
        _covers = check_replica_identity_covers_key(
            table, facts, _keys.get(table.name) or ()
        )
        if _covers is not None:
            results.append(_covers)
    return results


# The CDC-critical facts. When the probe read NONE of them the source is not "partially
# unknown", it is unverified -- and a PostgresCdcFacts full of Nones is not None, so the
# blocking check_postgres_cdc_facts_unavailable() could never engage: every check degraded
# itself to a non-blocking INFO and the report proceeded. This lets the checker route that
# state to the blocking FAIL, the same as a probe that returned nothing at all.
_CRITICAL_FACT_NAMES = (
    "wal_level",
    "is_superuser",
    "has_replication_role",
    "is_in_recovery",
    "max_replication_slots",
)


def postgres_cdc_facts_are_unverified(facts: PostgresCdcFacts) -> bool:
    """True when not one CDC-critical fact could be read (see :data:`_CRITICAL_FACT_NAMES`)."""
    return all(getattr(facts, name, None) is None for name in _CRITICAL_FACT_NAMES)


# Title per skipped check, so the Full-Load preview rows read EXACTLY like the CDC rows
# they stand in for. Kept next to the checks themselves rather than in prerequisites.py:
# duplicating the strings there is how a retitled check silently grows a second wording.
_SKIPPED_TITLES: "tuple[tuple, ...]" = (
    (PrerequisiteCheckId.WAL_LEVEL_LOGICAL, "Source wal_level is 'logical'"),
    (PrerequisiteCheckId.REPLICATION_ROLE, "Source user can create a replication slot"),
    (
        PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
        "Source user can create the CDC publication",
    ),
    # The two PROVISIONING-DEPENDENT rows get a bespoke preview detail. This report is the
    # LAST moment the gapless choice is still available: once a Full-load-only run
    # finishes, no slot was retaining WAL and the changes made during the load can never
    # be replayed -- only re-snapshotted. "Not applicable for this mode." hides that.
    (
        PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        "CDC's publication and replication slot exist on the source",
        "Not applicable for this mode — and note that a Full-load-only run does NOT "
        'create them. If you may want to stream changes afterwards, choose "Full load + '
        'CDC" now: the replication slot has to exist BEFORE the load, or the changes made '
        "during it can never be replayed.",
    ),
    (PrerequisiteCheckId.REPLICATION_SLOTS, "Source has replication-slot headroom"),
    (
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
        "WAL retention covers the CDC handoff",
        "Not applicable for this mode — a Full-load-only run creates no replication "
        "slot, so no WAL is being retained for a later CDC start.",
    ),
    (PrerequisiteCheckId.SOURCE_IS_WRITER, "Source is a writer (not a standby)"),
    (PrerequisiteCheckId.TABLE_REPLICABLE, "Table can be replicated (not UNLOGGED)"),
    (PrerequisiteCheckId.REPLICA_IDENTITY, "Table has a usable REPLICA IDENTITY"),
    # Bespoke preview detail, for the same reason as the two rows above: this is the one
    # requirement the mode itself REMOVES rather than defers. The bulk loader reads every
    # column as jsonb, so a Full-load-only run has no such thing as an uncarryable column --
    # which is exactly what an operator weighing "add CDC later" needs to know.
    (
        PrerequisiteCheckId.COLUMN_REPLICABLE,
        _COLUMN_REPLICABLE_TITLE,
        "Not applicable for this mode — Full Load carries every column type correctly (it "
        "reads each value in the form the target column takes, e.g. an array as jsonb). "
        "Some of those types cannot be carried by the CDC stream, so this is checked per "
        "table once CDC is in scope.",
    ),
)


def postgres_cdc_prerequisites_skipped() -> list[PrerequisiteResult]:
    """The PostgreSQL CDC readiness checks as SKIP rows, for Full-Load-only mode.

    A Full Load creates no replication slot, so none of these RUN -- but a PostgreSQL
    operator still needs to see them. The point of leaving the CDC rows visible in
    Full-Load-only mode is to preview what switching to CDC will additionally require;
    for a PostgreSQL source that preview used to list MySQL's binlog/GTID checks and
    none of these six, so it previewed another engine's requirements and hid the
    operator's own. One row per check (REPLICA IDENTITY is per-table when it actually
    runs, but a not-applicable row per table would just be noise).
    """
    return [
        PrerequisiteResult(
            check_id=row[0],
            title=row[1],
            status=PrerequisiteStatus.SKIP,
            required=True,
            # A row may carry its own preview detail (element 3); the rest say only that
            # the mode does not apply, which is all there is to say about them.
            detail=row[2] if len(row) > 2 else "Not applicable for this mode.",
        )
        for row in _SKIPPED_TITLES
    ]


__all__ = [
    "PostgresCdcFacts",
    "UncarryableCdcColumn",
    "classify_uncarryable_cdc_column",
    "CDC_UNCARRYABLE_CANDIDATE_ARRAY_ELEMENTS",
    "CDC_UNCARRYABLE_CANDIDATE_SCALARS",
    "UNCARRYABLE_ROW_DEAD_LETTERS",
    "UNCARRYABLE_COLUMN_ARRIVES_NULL",
    "UNCARRYABLE_COLUMN_ARRIVES_WRONG",
    "check_postgres_cdc_facts_unavailable",
    "postgres_cdc_prerequisites_skipped",
    "check_wal_level_logical",
    "check_replication_role",
    "check_replication_slot_headroom",
    "check_slot_wal_retention",
    "check_source_is_writer",
    "check_replica_identity",
    "check_table_replicable",
    "check_inheritance_children",
    "check_row_security",
    "check_columns_replicable",
    "check_publication_privilege",
    "check_cdc_replication_objects",
    "check_postgres_cdc_prerequisites",
    "postgres_cdc_facts_are_unverified",
]
