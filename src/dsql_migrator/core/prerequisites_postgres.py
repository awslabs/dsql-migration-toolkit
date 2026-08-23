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
    is_superuser: bool = False
    has_replication_role: bool = False
    max_replication_slots: Optional[int] = None
    used_replication_slots: Optional[int] = None
    max_wal_senders: Optional[int] = None
    is_in_recovery: bool = False
    replica_identity: Mapping[str, str] = field(default_factory=dict)


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
    ok = facts.is_superuser or facts.has_replication_role
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

    A new slot needs a free ``max_replication_slots`` entry and a free
    ``max_wal_senders``. Unknown counts (unreadable) are a non-blocking INFO.
    """
    if facts.max_replication_slots is None or facts.used_replication_slots is None:
        status = PrerequisiteStatus.INFO
        detail = "Could not read replication-slot capacity on the source."
    elif facts.used_replication_slots >= facts.max_replication_slots or (
        facts.max_wal_senders is not None and facts.max_wal_senders < 1
    ):
        status = PrerequisiteStatus.WARN
        detail = (
            f"{facts.used_replication_slots}/{facts.max_replication_slots} replication "
            f"slots in use (max_wal_senders={facts.max_wal_senders}); no headroom for a "
            "new CDC slot."
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
    """PASS when the source is a writer (not a standby): a standby cannot host a slot."""
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
    ok = identity in _USABLE_REPLICA_IDENTITY
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICA_IDENTITY,
        title="Table has a usable REPLICA IDENTITY",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail="REPLICA IDENTITY is set for change replication."
        if ok
        else "REPLICA IDENTITY is 'nothing' -- UPDATE/DELETE would fail on the publisher.",
        remediation=""
        if ok
        else (
            f"Set a REPLICA IDENTITY on {table.name}: with a primary key the default is "
            "enough; otherwise run ALTER TABLE ... REPLICA IDENTITY FULL so Debezium can "
            "replicate UPDATE/DELETE."
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


def check_postgres_cdc_prerequisites(
    facts: PostgresCdcFacts, tables: Sequence[TableDef]
) -> list[PrerequisiteResult]:
    """Run all PostgreSQL CDC readiness checks (global + per-table REPLICA IDENTITY)."""
    results = [
        check_wal_level_logical(facts),
        check_replication_role(facts),
        check_replication_slot_headroom(facts),
        check_source_is_writer(facts),
    ]
    results.extend(check_replica_identity(table, facts) for table in tables)
    return results


__all__ = [
    "PostgresCdcFacts",
    "check_postgres_cdc_facts_unavailable",
    "check_wal_level_logical",
    "check_replication_role",
    "check_replication_slot_headroom",
    "check_source_is_writer",
    "check_replica_identity",
    "check_postgres_cdc_prerequisites",
]
