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

from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

from dsql_migrator.core.models import (
    PrerequisiteCheckId,
    PrerequisiteResult,
    PrerequisiteStatus,
    TableDef,
)

# pg_class.relreplident values that let Debezium key an UPDATE/DELETE: 'd' default
# (uses the primary key -- which the migration already requires), 'f' full, 'i' a
# unique index. 'n' (nothing) makes UPDATE/DELETE on the table ERROR on the publisher.
_USABLE_REPLICA_IDENTITY = frozenset({"d", "f", "i"})


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
    # UPDATE/DELETE before-image carries only the index's columns, so the primary key the
    # DSQL sink upserts on arrives NULL.
    identity_index_is_primary: Mapping[str, bool] = field(default_factory=dict)
    # ``relpersistence='u'``: an UNLOGGED table cannot be published at all.
    unlogged: Mapping[str, bool] = field(default_factory=dict)
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

    'd' (default, keyed on the primary key -- which ``check_table_primary_key`` already
    requires), 'f' (full) and 'i' (index) are usable; 'n' (nothing) makes an UPDATE or
    DELETE on the table ERROR on the publisher. An unknown/unreadable identity is a
    non-blocking INFO (the connector remains the final authority).
    """
    identity = facts.replica_identity.get(table.name)
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
            "ALTER TABLE <partition> REPLICA IDENTITY FULL (or DEFAULT if it has a "
            "primary key), and set it on new partitions as they are created.",
        )

    if identity not in _USABLE_REPLICA_IDENTITY:
        return _fail(
            "REPLICA IDENTITY is 'nothing' -- UPDATE/DELETE would fail on the publisher.",
            f"Set a REPLICA IDENTITY on {table.name}: with a primary key the default is "
            "enough; otherwise run ALTER TABLE ... REPLICA IDENTITY FULL so Debezium can "
            "replicate UPDATE/DELETE.",
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
                "run ALTER TABLE ... REPLICA IDENTITY USING INDEX <index>, or use "
                "REPLICA IDENTITY DEFAULT (primary key) / FULL.",
            )
        # A non-PK identity index publishes only ITS columns in the before-image, so the
        # primary key the DSQL sink upserts on arrives NULL for UPDATE/DELETE. Non-blocking
        # (the load itself is fine and the user may have keyed the sink differently), but
        # it is a real change-replication risk, so WARN rather than a silent PASS.
        if facts.identity_index_is_primary.get(table.name) is False:
            return PrerequisiteResult(
                check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
                title="Table has a usable REPLICA IDENTITY",
                status=PrerequisiteStatus.WARN,
                required=False,
                target=table.name,
                detail=(
                    "REPLICA IDENTITY is an index that is NOT the primary key, so an "
                    "UPDATE/DELETE before-image carries only that index's columns and the "
                    "primary key the target upserts on arrives NULL."
                ),
                remediation=(
                    f"Prefer ALTER TABLE {table.name} REPLICA IDENTITY DEFAULT (the "
                    "primary key) or FULL, so every replicated change carries the key "
                    "Aurora DSQL needs."
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
            f"Either convert it with ALTER TABLE {table.name} SET LOGGED, or deselect it "
            "and migrate it with Full Load only (an UNLOGGED table can never be "
            "change-replicated)."
        ),
    )


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
) -> PrerequisiteResult:
    """Do CDC's publication + replication slot EXIST? Distinct from the PRIVILEGE check.

    ``provisions_replication`` is the whole non-regression guarantee. In "Full load + CDC"
    the Full Load CREATES both at the snapshot point, so they are SUPPOSED to be absent
    beforehand -- this must read as SKIP there, never as a problem. Only a run that has to
    FIND them (a CDC-only start) is graded.

    Grading: an absent publication, or one that omits a selected table or narrows its
    publish list, is a FAIL -- each makes the connector either die at once or report
    RUNNING while replicating nothing. A missing SLOT is only a WARN, because with the
    recorded-slot invariant in ``build_pg_source_config`` such a start re-snapshots and is
    gapless; it costs a re-read, not correctness.
    """
    title = "CDC's publication and replication slot exist on the source"
    if provisions_replication:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.SKIP,
            required=True,
            detail=(
                "Full Load creates the publication and replication slot at the snapshot "
                "LSN for this run, so they must not exist beforehand."
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
    if not facts.publication_present:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
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
    selected = [t.name for t in tables]
    missing = sorted(set(selected) - set(facts.publication_tables or ()))
    if missing:
        shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.FAIL,
            required=True,
            detail=(
                f'Publication "{pub}" exists but does not include {len(missing)} of the '
                f"{len(selected)} selected tables: {shown}."
            ),
            remediation=(
                "Those tables would replicate NOTHING while the connector reported "
                "RUNNING, which is worse than a visible failure. " + gapless_note
                + resnapshot_note
            ),
        )
    if facts.publication_publishes_all_dml is False:
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
            remediation=(
                "Recreate the publication with all three operations, or "
                + resnapshot_note[0].lower() + resnapshot_note[1:]
            ),
        )
    if not facts.slot_usable:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
            title=title,
            status=PrerequisiteStatus.WARN,
            required=False,
            detail=(
                f'Publication "{pub}" covers all {len(selected)} selected tables, but '
                "there is no logical pgoutput replication slot named "
                f'"{slot}" on this database.'
            ),
            remediation=(
                "CDC can still start — Debezium creates the slot itself and takes a fresh "
                "snapshot of every selected table first, so nothing is lost, but the "
                "whole selection is read from the source again. To stream without "
                're-reading, run the migration as "Full load + CDC", which creates the '
                "slot at the snapshot point."
            ),
        )
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        title=title,
        status=PrerequisiteStatus.PASS,
        required=True,
        detail=(
            f'Publication "{pub}" covers all {len(selected)} selected tables and '
            f'replication slot "{slot}" is present, so CDC can resume from it.'
        ),
    )


def check_postgres_cdc_prerequisites(
    facts: PostgresCdcFacts,
    tables: Sequence[TableDef],
    *,
    provisions_replication: bool = True,
) -> list[PrerequisiteResult]:
    """Run all PostgreSQL CDC readiness checks (global + per-table REPLICA IDENTITY).

    ``provisions_replication`` defaults True -- "this run creates the objects itself" --
    so every existing caller and the whole Full-load-+-CDC path is unchanged and the
    existence check can never block them.
    """
    results = [
        check_wal_level_logical(facts),
        check_replication_role(facts),
        check_publication_privilege(facts),
        check_cdc_replication_objects(
            facts, tables, provisions_replication=provisions_replication
        ),
        check_replication_slot_headroom(facts),
        check_slot_wal_retention(facts),
        check_source_is_writer(facts),
    ]
    for table in tables:
        results.append(check_table_replicable(table, facts))
        results.append(check_replica_identity(table, facts))
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
    "check_postgres_cdc_facts_unavailable",
    "postgres_cdc_prerequisites_skipped",
    "check_wal_level_logical",
    "check_replication_role",
    "check_replication_slot_headroom",
    "check_slot_wal_retention",
    "check_source_is_writer",
    "check_replica_identity",
    "check_table_replicable",
    "check_publication_privilege",
    "check_cdc_replication_objects",
    "check_postgres_cdc_prerequisites",
    "postgres_cdc_facts_are_unverified",
]
