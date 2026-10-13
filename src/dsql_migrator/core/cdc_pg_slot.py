# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ONE audited source-write path: PostgreSQL CDC replication slot + publication.

Everywhere else in this tool the migration source is strictly READ-ONLY (Property 1),
enforced by :func:`~dsql_migrator.core.introspector.install_read_only_guard` on every
source engine. PostgreSQL CDC is the sole, deliberate exception: to hand off from Full
Load to CDC gaplessly the tool must, ON THE SOURCE,

  1. create a **publication** scoped to exactly the migrated tables (``FOR TABLE ONLY``:
     an inheritance child is never pulled in with its parent), and
  2. create a **logical replication slot** (``pgoutput``) at the Full Load consistency
     point -- the slot pins the source WAL from its returned LSN until CDC consumes it,

and DROP both on teardown. Around them it also sets ``REPLICA IDENTITY FULL`` on a
re-keyed table (its before-image must carry the leading key column), repairs a
publication's ``publish_via_partition_root``, and -- for a table selected after a Full
load + CDC Full Load, before CDC starts (G17) -- adds that ONE table to the run's
publication just before its copy, then takes one transaction ID to wait out the
transactions open at that moment (:func:`take_xid_after_change`). These are the only
writes the tool ever issues to a source, so they are confined to this module, which:

- builds its OWN engine that deliberately does NOT install the read-only guard and runs
  in **AUTOCOMMIT** (``pg_create_logical_replication_slot`` cannot run inside a wrapping
  transaction). The shared read-only guard is left fully intact; this is a separate,
  explicit path, never a bypass token on the guarded factory. (The guard could not police
  these anyway: it is first-keyword based, so ``SELECT pg_create_logical_replication_slot``
  slips through unaudited while ``CREATE PUBLICATION`` is wrongly blocked.)
- is gated on ``source_type is SourceType.POSTGRES`` (a MySQL source never gets a write
  engine -- MySQL uses the binlog offset-seeder),
- issues ONLY a fixed allowlist of statements (:data:`_ALLOWED_WRITE_PREFIXES`), asserted
  before every write, so a bug can never send arbitrary SQL down the un-guarded path,
- takes the same in-memory :class:`~dsql_migrator.config.SecretValue` as the read path
  (Property 7 -- no new credential surface; revealed only at connect), and
- reports each write through an ``on_log`` callback so the caller can audit it.

Before its first write, provisioning refuses to publish any table whose UPDATE/DELETE would
then fail on the source -- no usable REPLICA IDENTITY, or one that cannot be read (G20) --
with the same strict, fail-closed rule as the one-table ADD (G17), and refuses to drop a
same-named replication slot that is in use or belongs to another database (G25).

Slot/publication CREATION is wired at the Full Load consistency point
(``_capture_postgres_watermark``); DROP is wired into CDC teardown BEFORE the source
secret is deleted (a slot left behind pins WAL and can fill the source disk); the one-table
ADD and the wait are wired into a guarded Full load + CDC Retry
(``BatchedTableMigrator.guard_cdc_additions``), before any snapshot of the retry opens.
This module is the primitive they call.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, replace
from typing import Callable, Mapping, NamedTuple, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.config import SecretValue
from dsql_migrator.core.models import SourceConnectionConfig, SourceType
from dsql_migrator.core.source_dialect import dialect_for

# Deterministic object-name prefixes (parallel to cdc_source_secret_name). Slot names
# are restricted to [a-z0-9_] and <= 63 chars by PostgreSQL; we sanitize the stack name
# to that charset and use the same convention for the publication so both are derivable
# from the stack name alone (teardown / orphan-sweep can reconstruct them).
_SLOT_PREFIX = "dsqlmig_"
_PUBLICATION_PREFIX = "dsqlmig_pub_"
_MAX_SLOT_NAME = 63

# The COMPLETE set of statement shapes this module may execute against the source. Every
# write is asserted against this before running, so the un-guarded engine can only ever
# perform these intended operations (create/drop slot, create/drop publication, add ONE
# table to a publication, take a transaction ID, and set a re-keyed table's REPLICA
# IDENTITY FULL) plus the read-only existence checks. The ALTER TABLE prefix is
# additionally constrained in ``_assert_allowed`` to the exact ``... REPLICA IDENTITY FULL``
# tail, so it can never DROP/rename a column; ALTER PUBLICATION and the xid read are pinned
# to their exact shapes there too.
_ALLOWED_WRITE_PREFIXES: tuple[str, ...] = (
    "CREATE PUBLICATION ",
    "DROP PUBLICATION ",
    # Constrained in ``_assert_allowed`` to two shapes: the partition-root property flip
    # (needed because a publication DEBEZIUM created -- publication.autocreate.mode=filtered
    # -- lands at PostgreSQL's default publish_via_partition_root=false, and there is no
    # connector property to set it) and ONE quoted table added (G17). Never SET TABLE /
    # DROP TABLE / OWNER TO / RENAME / several tables / a column list / a row filter.
    "ALTER PUBLICATION ",
    "SELECT LSN FROM PG_CREATE_LOGICAL_REPLICATION_SLOT",
    "SELECT PG_DROP_REPLICATION_SLOT",
    # Pinned to the exact statement (``_XID_STATEMENT``): it assigns this autocommit
    # transaction an ID -- a write in PostgreSQL's terms, so a read-only engine refuses it.
    "SELECT PG_CURRENT_XACT_ID",
    "ALTER TABLE ",
)

# The ONLY ALTER PUBLICATION ... SET this module may run.
_PUBLICATION_VIA_ROOT_TAIL = " SET (PUBLISH_VIA_PARTITION_ROOT = TRUE)"
# The ONLY ALTER PUBLICATION ... ADD TABLE: one sanitized publication name and ONE
# double-quoted schema.table (a doubled quote is the escape) after ``ONLY``, matched on the
# normalized text. ONLY is REQUIRED (G25): without it PostgreSQL also adds every
# inheritance descendant, whose REPLICA IDENTITY nothing here checked -- a child with none
# then fails every UPDATE/DELETE through the parent too. (A partitioned table's partitions
# stay published either way.) A missing ONLY / * / a column list / WHERE / TABLES IN SCHEMA
# / a second table / a trailing statement all fail the anchored match.
_PUBLICATION_ADD_TABLE_RE = re.compile(
    r'^ALTER PUBLICATION "[A-Z0-9_]+" ADD TABLE ONLY "(?:[^"]|"")+"\."(?:[^"]|"")+"$'
)
# The ONLY transaction-ID statement (normalized form).
_XID_STATEMENT = "SELECT PG_CURRENT_XACT_ID()::TEXT"


class PgReplicationError(RuntimeError):
    """A replication slot / publication create or drop failed on the source."""


class PgPublicationMissingError(PgReplicationError):
    """The publication a one-table add targets does not exist on the source (G17)."""


class PgReplicaIdentityError(PgReplicationError):
    """A one-table add (G17) -- or the Full Load's provisioning of its publication (G20) --
    was refused BEFORE any write: a table has no replica identity PostgreSQL accepts for
    UPDATE/DELETE once it is published (``cause``, the value-free phrase of
    :func:`replica_identity_problem`; for provisioning, the first refused table's -- and
    ``remedy``, the fix for that cause, :func:`replica_identity_remedy`, set whenever
    ``cause`` is), or whether it has one could not be read (``unreadable``, ``cause`` and
    ``remedy`` None) -- fail closed, never "usable". Provisioning's lenient REPLICA IDENTITY
    NOTHING refusal (:func:`_verify_tables_replicable`) raises it too (G21)."""

    def __init__(
        self,
        message: str,
        *,
        cause: Optional[str] = None,
        remedy: Optional[str] = None,
        unreadable: bool = False,
    ) -> None:
        super().__init__(message)
        self.cause = cause
        self.remedy = remedy
        self.unreadable = unreadable


@dataclass(frozen=True)
class PgReplicationHandles:
    """The slot + publication created for a PostgreSQL CDC handoff, and the resume LSN.

    ``consistent_lsn`` is the ``pg_create_logical_replication_slot`` consistent point --
    the exact WAL position CDC resumes streaming from (recorded as ``Watermark.wal_lsn``).
    """

    slot_name: str
    publication_name: str
    consistent_lsn: str


def _sanitize(stack_name: str) -> str:
    """Return ``stack_name`` reduced to the PostgreSQL slot-name charset ([a-z0-9_])."""
    return re.sub(r"[^a-z0-9_]", "_", (stack_name or "").lower())


# Length of the case-preserving hash discriminator appended to slot/publication names.
_STACK_HASH_LEN = 8


def _stack_hash(stack_name: str) -> str:
    """Return a short deterministic hash of the ORIGINAL stack name (case-preserving)."""
    import hashlib

    return hashlib.sha1((stack_name or "").encode("utf-8")).hexdigest()[:_STACK_HASH_LEN]


def _derive_name(prefix: str, stack_name: str) -> str:
    """``<prefix><sanitized>_<hash8>`` within the 63-char limit; collision-resistant.

    The lowercasing + 63-char truncation of the raw sanitized name could map two DISTINCT
    stack names (case-only differences, or suffixes differing only past the truncation
    boundary) to one slot/publication -- so two migrations of the same source would fight
    over one slot. Appending a hash of the ORIGINAL (case-preserving) full stack name makes
    the name stack-unique, which also makes ``provision``'s drop-of-a-stale-slot safe (a
    pre-existing slot with our name is genuinely OUR stack's prior slot, not another's).
    The sanitized base is truncated (never the hash) to fit 63 chars.
    """
    suffix = "_" + _stack_hash(stack_name)
    keep = _MAX_SLOT_NAME - len(prefix) - len(suffix)
    return f"{prefix}{_sanitize(stack_name)[:keep]}{suffix}"


def pg_slot_name(stack_name: str) -> str:
    """Return the deterministic, collision-resistant logical-replication slot name.

    ``dsqlmig_<sanitized-stack>_<hash8>`` within PostgreSQL's 63-char slot-name limit, so
    the connector param (``PgSlotName``), the creation, and teardown all name the SAME slot
    from the stack name alone -- and two distinct stacks never collide (see
    :func:`_derive_name`).
    """
    return _derive_name(_SLOT_PREFIX, stack_name)


def pg_publication_name(stack_name: str) -> str:
    """Return the deterministic, collision-resistant publication name for ``stack_name``."""
    return _derive_name(_PUBLICATION_PREFIX, stack_name)


def rekeyed_tables_needing_full_identity(
    message_key_columns: Mapping[str, Sequence[str]],
    table_primary_keys: Mapping[str, Sequence[str]],
) -> list[str]:
    """Captured tables whose CDC re-key adds a column OUTSIDE the source PK -> need FULL identity.

    A ``COMPOSITE_KEY`` schema conversion gives the target the primary key
    ``(leading, *source_pk)`` (:func:`~dsql_migrator.core.cdc.composite_key_columns_for_cdc`).
    Under PostgreSQL ``REPLICA IDENTITY DEFAULT`` the UPDATE/DELETE before-image carries ONLY
    the source PRIMARY-KEY columns, so the prepended ``leading`` column is ABSENT and Debezium
    fills it with a placeholder (live-confirmed on PostgreSQL 17: the column DEFAULT). A
    DELETE keyed on the composite key then misses its row on Aurora DSQL SILENTLY -- no error,
    no dead letter. Since G15 a sink with plugin v51 or later keys such a table on the source
    primary key and deletes by it once the unique index on it is valid (an UPDATE of the leading
    column, which then has no old image, replaces the row holding that key); until then and on
    older infrastructure the weak identity still loses the change. Such a table
    needs ``REPLICA IDENTITY FULL`` (the before-image then carries every column). A re-key
    that only REORDERS existing PK columns adds nothing outside the PK, so DEFAULT identity
    already covers it and it is NOT returned. (MySQL is immune -- its binlog before-image is
    always the full row -- so this is a PostgreSQL-only prerequisite.) Returns the offending
    qualified ``db.table`` names (sorted); empty means none need FULL identity. Pure.
    """
    offending: list[str] = []
    for table, key_cols in message_key_columns.items():
        pk = set(table_primary_keys.get(table, ()))
        if set(key_cols) - pk:
            offending.append(table)
    return sorted(offending)


class RekeyedIdentityRead(NamedTuple):
    """One re-keyed table's live REPLICA IDENTITY facts, as the runtime re-check reads them.

    ``identity`` is ``pg_class.relreplident`` of the table itself ('d'/'f'/'i'/'n');
    ``primary_key`` its LIVE primary-key columns (column order); ``weak_partitions`` how many
    of its LEAF partitions are not FULL (0 for a plain table), and ``weak_partition_names``
    at most five of them, sorted. ``full_refused`` (G25): PostgreSQL 18+ and the table has a
    generated column (its partitions inherit it), so a published REPLICA IDENTITY FULL fails
    its every UPDATE/DELETE -- FULL is no fix for it. Value-free: names, codes and a flag.
    """

    identity: str
    primary_key: tuple
    weak_partitions: int
    weak_partition_names: tuple
    full_refused: bool = False


# ONE catalog round trip for every scoped table. Leaves are aggregated SERVER-side (a count
# plus at most five names), never one row per leaf, so a heavily partitioned table costs the
# same result size as a plain one. Catalog only: no table scan, no lock. pg_partition_tree is
# PostgreSQL 12+ (the tool targets 13-18); ``a.attnum = ANY(i.indkey)`` reads the int2vector
# directly. The approved live check runs this on PG 13-17 (see the G2 notes).
# Completed below, once its PostgreSQL 18 generated-column test is defined.
_REKEYED_IDENTITY_TEMPLATE = (
    "SELECT n.nspname || '.' || c.relname AS qname, "
    "       c.relreplident::text AS identity, "
    "       ARRAY(SELECT a.attname::text "
    "             FROM pg_catalog.pg_index i "
    "             JOIN pg_catalog.pg_attribute a "
    "               ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
    "             WHERE i.indrelid = c.oid AND i.indisprimary "
    "             ORDER BY a.attnum) AS pk_columns, "
    "       CASE WHEN c.relkind = 'p' THEN "
    "         (SELECT count(*) "
    "          FROM pg_catalog.pg_partition_tree(c.oid) t "
    "          JOIN pg_catalog.pg_class l ON l.oid = t.relid "
    "          WHERE t.isleaf AND l.relreplident <> 'f') "
    "       ELSE 0 END AS weak_partitions, "
    "       CASE WHEN c.relkind = 'p' THEN "
    "         ARRAY(SELECT ln.nspname || '.' || l.relname "
    "               FROM pg_catalog.pg_partition_tree(c.oid) t "
    "               JOIN pg_catalog.pg_class l ON l.oid = t.relid "
    "               JOIN pg_catalog.pg_namespace ln ON ln.oid = l.relnamespace "
    "               WHERE t.isleaf AND l.relreplident <> 'f' "
    "               ORDER BY 1 LIMIT 5) "
    "       ELSE ARRAY[]::text[] END AS weak_sample, "
    "       {full_refused} AS full_refused "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r','p') "
    "  AND n.nspname || '.' || c.relname = ANY(:names)"
)


def read_rekeyed_replica_identity(
    connection: object, names: Sequence[str]
) -> "dict[str, RekeyedIdentityRead]":
    """Read the live REPLICA IDENTITY facts of the re-keyed ``names`` (read-only, ONE statement).

    Returns ``{qualified name: RekeyedIdentityRead}`` for each name found on the source; a
    name absent from the result was not found (the caller grades it "unknown", never FULL).
    RAISES on failure, like :func:`read_pg_replication_objects`: unreadable is not evidence
    of anything, and the caller must tell it apart from a read. Run it only through the
    read-only guarded source engine (``make_source_engine_factory``) -- never this module's
    write engine.
    """
    wanted = sorted({str(n) for n in names or () if n})
    if not wanted:
        return {}
    rows = connection.execute(  # type: ignore[attr-defined]
        text(_REKEYED_REPLICA_IDENTITY_SQL), {"names": wanted}
    ).fetchall()
    out: dict[str, RekeyedIdentityRead] = {}
    for row in rows:
        sample = tuple(str(v) for v in (row[4] or ()) if v is not None)[:5]
        values = tuple(row)
        out[str(row[0])] = RekeyedIdentityRead(
            identity=str(row[1]) if row[1] is not None else "",
            primary_key=tuple(str(v) for v in (row[2] or ()) if v is not None),
            weak_partitions=max(int(row[3] or 0), 0),
            weak_partition_names=sample,
            # G25, after the G2 columns; a row without it reads as False.
            full_refused=bool(values[5]) if len(values) > 5 else False,
        )
    return out


class ReplicaIdentityUsability(NamedTuple):
    """One table's live facts for "would publishing it make its UPDATE/DELETE fail?" (G17).

    ``kind`` is ``pg_class.relkind`` ('r' a plain table, 'p' a partitioned one); ``identity``
    its own ``relreplident``; ``usable`` whether PostgreSQL has a replica identity for its
    UPDATE/DELETE once published (graded server-side by :func:`_identity_usable_sql`); and
    ``unusable_partitions`` how many of its LEAF partitions have none (0 for a plain table),
    at most five of them named (sorted) in ``unusable_partition_names``. A plain table's
    inheritance children (``INHERITS``) are not graded: nothing this tool runs publishes them
    -- its publication lists each table ``ONLY`` -- and Start CDC refuses the snapshot that
    would let Debezium's own publication add them (:func:`pg_replication_objects_blocker`).
    G25 also reads why a key is unusable: ``pk_deferrable`` (its valid primary key is
    DEFERRABLE), ``full_refused`` (PostgreSQL 18+: it has a generated column, so a published
    REPLICA IDENTITY FULL refuses every UPDATE/DELETE -- FULL is no fix for it) and
    ``key_generated`` (PostgreSQL 18+: the key DEFAULT or USING INDEX would use has a
    generated column). Value-free: names, codes and flags only.
    """

    kind: str
    identity: str
    usable: bool
    unusable_partitions: int
    unusable_partition_names: tuple
    pk_deferrable: bool = False
    full_refused: bool = False
    key_generated: bool = False


# PostgreSQL 18 refuses UPDATE/DELETE on a published table whose replica identity contains
# a generated column that the publication does not publish ("Replica identity must not
# contain unpublished generated columns", CheckCmdReplicaIdentity): under FULL every column
# is in the identity; under DEFAULT / USING INDEX the key's columns are. A VIRTUAL one can
# never be published, a STORED one only with publish_generated_columns = stored -- which
# CDC's publication never sets (the tool creates it without, and so does Debezium), so both
# count. PostgreSQL 13-17 do not check this, and their grading is unchanged.
_PG18 = "pg_catalog.current_setting('server_version_num')::int >= 180000"


def _has_generated_column_sql(alias: str) -> str:
    """PostgreSQL 18+ and relation ``alias`` has a generated column: FULL is refused."""
    return (
        f"({_PG18} AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a "
        f"WHERE a.attrelid = {alias}.oid AND a.attnum > 0 AND NOT a.attisdropped "
        "AND a.attgenerated <> ''))"
    )


_REKEYED_REPLICA_IDENTITY_SQL = _REKEYED_IDENTITY_TEMPLATE.format(
    full_refused=_has_generated_column_sql("c")
)


def _generated_key_sql(index: str) -> str:
    """PostgreSQL 18+ and a KEY column of index ``index`` is generated (INCLUDE columns are
    not part of a replica identity: ``indnkeyatts`` key columns lead ``indkey``)."""
    return (
        f"({_PG18} AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a "
        f"WHERE a.attrelid = {index}.indrelid AND a.attgenerated <> '' "
        f"AND a.attnum = ANY(({index}.indkey::int2[])[0:{index}.indnkeyatts - 1])))"
    )


def _identity_usable_sql(alias: str) -> str:
    """Does relation ``alias`` keep a replica identity PostgreSQL accepts for UPDATE/DELETE
    once published? The relcache's own rule (``RelationGetIndexList`` /
    ``CheckCmdReplicaIdentity``): FULL; DEFAULT backed by a valid, immediate (not deferrable)
    primary key; or USING INDEX backed by a valid, unique, immediate, non-partial identity
    index -- on PostgreSQL 18, none of them with a generated column in the identity
    (:data:`_PG18`). PostgreSQL does NOT reset ``relreplident`` when that key or index is
    dropped, so the code alone proves nothing. Catalog only."""
    return (
        f"({alias}.relreplident = 'f' AND NOT {_has_generated_column_sql(alias)} "
        f"OR ({alias}.relreplident = 'd' AND EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        f"WHERE i.indrelid = {alias}.oid AND i.indisprimary AND i.indisvalid "
        f"AND i.indimmediate AND NOT {_generated_key_sql('i')})) "
        f"OR ({alias}.relreplident = 'i' AND EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        f"WHERE i.indrelid = {alias}.oid AND i.indisreplident AND i.indisvalid "
        f"AND i.indisunique AND i.indimmediate AND i.indpred IS NULL "
        f"AND NOT {_generated_key_sql('i')})))"
    )


# ONE catalog round trip for every named table, like the re-keyed read above: the leaves of a
# partitioned table are graded and aggregated SERVER-side (a count plus at most five names).
# PostgreSQL skips the check for a partitioned parent itself (the UPDATE/DELETE runs on the
# leaf), so a parent is graded by its leaves -- plus its own NOTHING, which the prerequisite
# also refuses. A plain table's inheritance children are not graded (see
# :class:`ReplicaIdentityUsability`). Catalog only: no table scan, no lock.
_REPLICA_IDENTITY_USABLE_SQL = (
    "SELECT n.nspname || '.' || c.relname AS qname, "
    "       c.relkind::text AS kind, "
    "       c.relreplident::text AS replident, "
    f"      {_identity_usable_sql('c')} AS identity_usable, "
    "       CASE WHEN c.relkind = 'p' THEN "
    "         (SELECT count(*) "
    "          FROM pg_catalog.pg_partition_tree(c.oid) t "
    "          JOIN pg_catalog.pg_class l ON l.oid = t.relid "
    f"         WHERE t.isleaf AND NOT {_identity_usable_sql('l')}) "
    "       ELSE 0 END AS unusable_partitions, "
    "       CASE WHEN c.relkind = 'p' THEN "
    "         ARRAY(SELECT ln.nspname || '.' || l.relname "
    "               FROM pg_catalog.pg_partition_tree(c.oid) t "
    "               JOIN pg_catalog.pg_class l ON l.oid = t.relid "
    "               JOIN pg_catalog.pg_namespace ln ON ln.oid = l.relnamespace "
    f"              WHERE t.isleaf AND NOT {_identity_usable_sql('l')} "
    "               ORDER BY 1 LIMIT 5) "
    "       ELSE ARRAY[]::text[] END AS unusable_sample, "
    "       EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid "
    "               AND i.indisprimary AND i.indisvalid AND NOT i.indimmediate) "
    "         AS pk_deferrable, "
    f"      {_has_generated_column_sql('c')} AS full_refused, "
    "       EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid "
    "               AND i.indisvalid AND i.indimmediate "
    "               AND ((c.relreplident = 'd' AND i.indisprimary) "
    "                 OR (c.relreplident = 'i' AND i.indisreplident AND i.indisunique "
    "                     AND i.indpred IS NULL)) "
    f"              AND {_generated_key_sql('i')}) AS key_generated "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r','p') "
    "  AND n.nspname || '.' || c.relname = ANY(:names)"
)


def read_replica_identity_usability(
    connection: object, names: Sequence[str]
) -> "dict[str, ReplicaIdentityUsability]":
    """Read whether each of ``names`` keeps a usable replica identity once published (G17).

    One read-only catalog statement. Returns ``{qualified name: ReplicaIdentityUsability}``
    for each plain or partitioned table found; a name absent from the result was not found
    (the caller refuses it -- never graded usable). RAISES on failure, like
    :func:`read_rekeyed_replica_identity`: unreadable is not evidence of anything.
    """
    wanted = sorted({str(n) for n in names or () if n})
    if not wanted:
        return {}
    rows = connection.execute(  # type: ignore[attr-defined]
        text(_REPLICA_IDENTITY_USABLE_SQL), {"names": wanted}
    ).fetchall()
    out: dict[str, ReplicaIdentityUsability] = {}
    for row in rows:
        # The G25 flags follow the G17 columns; a row without them reads as False.
        flags = [bool(v) for v in tuple(row)[6:9]]
        flags += [False] * (3 - len(flags))
        out[str(row[0])] = ReplicaIdentityUsability(
            str(row[1]) if row[1] is not None else "",
            str(row[2]) if row[2] is not None else "",
            bool(row[3]),
            max(int(row[4] or 0), 0),
            tuple(str(v) for v in (row[5] or ()) if v is not None)[:5],
            *flags,
        )
    return out


# replica_identity_problem's phrases for a key or index that is GONE: PostgreSQL keeps
# ``relreplident`` when it is dropped, which the prerequisite explains for these causes only.
NO_PRIMARY_KEY_PROBLEM = "REPLICA IDENTITY DEFAULT, but no valid primary key"
INDEX_GONE_PROBLEM = "REPLICA IDENTITY USING INDEX, but that index is gone or not valid"


def _partitions(read: ReplicaIdentityUsability) -> str:
    count = read.unusable_partitions
    shown = ", ".join(read.unusable_partition_names)
    names = len(read.unusable_partition_names)
    more = f" and {count - names} more" if count > names else ""
    return f"{count} of its partitions {'has' if count == 1 else 'have'} none: {shown}{more}"


def replica_identity_problem(read: ReplicaIdentityUsability) -> Optional[str]:
    """Why publishing the table would make its UPDATE/DELETE fail on the source -- a short,
    value-free phrase -- or ``None`` when it would not (G17). Pure.

    A partitioned table is graded by its leaves (PostgreSQL enforces the LEAF's identity:
    ``ALTER TABLE <parent>`` does not reach existing partitions) and by its own NOTHING; a
    plain one by its own identity alone -- its inheritance children are never published
    with it (:class:`ReplicaIdentityUsability`). Each cause says what is wrong with the key:
    none, DEFERRABLE, or (PostgreSQL 18) generated.
    """
    if read.kind == "p":
        if read.identity == "n":
            return "REPLICA IDENTITY NOTHING"
        return _partitions(read) if read.unusable_partitions > 0 else None
    if read.usable:
        return None
    if read.identity == "n":
        return "REPLICA IDENTITY NOTHING"
    if read.identity == "f" and read.full_refused:
        return "REPLICA IDENTITY FULL, but it has generated columns (PostgreSQL 18)"
    if read.key_generated:
        return (
            "REPLICA IDENTITY DEFAULT, but its primary key has a generated column"
            if read.identity == "d"
            else "REPLICA IDENTITY USING INDEX, but that index has a generated column"
        )
    if read.identity == "d":
        if read.pk_deferrable:
            return "REPLICA IDENTITY DEFAULT, but its primary key is DEFERRABLE"
        return NO_PRIMARY_KEY_PROBLEM
    if read.identity == "i":
        return INDEX_GONE_PROBLEM
    return "a REPLICA IDENTITY that PostgreSQL cannot use"


def replica_identity_remedy(
    read: ReplicaIdentityUsability, *, table: Optional[str] = None
) -> str:
    """The fix for one refused table, by its cause -- a lowercase phrase for mid-sentence use
    (every refusal of an unusable replica identity names it, G21). Pure.

    A new primary key does not repair NOTHING or a USING INDEX whose index is gone
    (PostgreSQL keeps ``relreplident``; only ALTER TABLE ... REPLICA IDENTITY changes it),
    and a DEFERRABLE key is no replica identity -- so "give it a primary key" fits DEFAULT
    alone. A partitioned table's leaves carry the identity (ALTER TABLE on the parent does
    not reach existing partitions); its own NOTHING is the parent's to fix. On PostgreSQL 18
    a table with a generated column (``full_refused``) gets no FULL advice: published, FULL
    refuses its UPDATE/DELETE (a partitioned parent's own FULL is never checked, only its
    leaves'). ``table`` -- the qualified name, for a prerequisite that has room for it --
    adds the exact, quoted statement wherever FULL on the table itself is the fix (G25).
    """
    sql = (
        f" (ALTER TABLE {dialect_for(SourceType.POSTGRES).quote_table(table)} "
        "REPLICA IDENTITY FULL)"
        if table
        else ""
    )
    refused = read.full_refused
    if read.kind == "p":
        if read.identity == "n":
            if read.unusable_partitions <= 0:
                return f"set REPLICA IDENTITY FULL on it{sql}, or DEFAULT with a primary key"
            if refused:
                return (
                    f"set REPLICA IDENTITY FULL on it{sql} and DEFAULT with a primary key "
                    "on each partition"
                )
            return f"set REPLICA IDENTITY FULL on it{sql} and on each partition"
        if refused:
            return (
                "give each partition a primary key (not DEFERRABLE) and REPLICA IDENTITY "
                "DEFAULT"
            )
        return "set REPLICA IDENTITY FULL on each partition"
    if read.key_generated:
        return "use a key with no generated column as its identity"
    or_full = "" if refused else f", or REPLICA IDENTITY FULL{sql}"
    if read.identity == "d":
        if read.pk_deferrable:  # PostgreSQL cannot ALTER a key to NOT DEFERRABLE in place
            return "re-add the key NOT DEFERRABLE" + (
                "" if refused else f", or set REPLICA IDENTITY FULL{sql}"
            )
        return f"give it a primary key that is not DEFERRABLE{or_full}"
    if refused:
        return "give it a primary key and REPLICA IDENTITY DEFAULT"
    return f"set REPLICA IDENTITY FULL{sql}, or DEFAULT with a primary key"


REPLICA_IDENTITY_WEAK = "weak"
REPLICA_IDENTITY_FULL = "full"
REPLICA_IDENTITY_UNKNOWN = "unknown"


def grade_rekeyed_replica_identity(
    table: str,
    key_columns: Sequence[str],
    read: "Optional[RekeyedIdentityRead]",
) -> Optional[str]:
    """Grade one re-keyed table's live identity with the Start prerequisite's OWN rule. Pure.

    ``"weak"`` (the prerequisite would FAIL: deletes can be lost), ``"full"`` (it would
    PASS), ``"unknown"`` (INFO, or no row was read), or ``None`` when the key adds nothing
    beyond the LIVE primary key (not re-keyed in the sense that matters). A missing ``read``
    is always ``"unknown"`` -- never graded as not-re-keyed, which would close a finding on
    the strength of a read that found nothing.
    """
    if read is None:
        return REPLICA_IDENTITY_UNKNOWN
    from dsql_migrator.core.models import PrerequisiteStatus, TableDef
    from dsql_migrator.core.prerequisites_postgres import (
        replica_identity_covers_key_result,
    )

    result = replica_identity_covers_key_result(
        TableDef(name=table, primary_key=list(read.primary_key)),
        list(key_columns or ()),
        identity=read.identity or None,
        weak_partition_names=read.weak_partition_names,
        weak_partition_count=read.weak_partitions,
    )
    if result is None:
        return None
    if result.status is PrerequisiteStatus.FAIL:
        return REPLICA_IDENTITY_WEAK
    if result.status is PrerequisiteStatus.PASS:
        return REPLICA_IDENTITY_FULL
    return REPLICA_IDENTITY_UNKNOWN


def build_pg_source_write_engine(
    source_config: SourceConnectionConfig,
    password: Optional[SecretValue],
    *,
    read_timeout_seconds: Optional[int] = None,
) -> Engine:
    """Build the dedicated, NON-read-only-guarded, AUTOCOMMIT PostgreSQL source engine.

    This is the ONLY source engine in the tool without :func:`install_read_only_guard`;
    it exists solely so this module can create/drop the replication slot + publication.
    AUTOCOMMIT is required because ``pg_create_logical_replication_slot`` must not run
    inside a wrapping transaction. Refuses a non-PostgreSQL source (MySQL never writes
    the source). The password is revealed only here (Property 7), mirroring
    ``make_source_engine_factory`` but WITHOUT the guard. ``read_timeout_seconds`` sets the
    dialect's ``statement_timeout`` (and keepalives), so a write that waits on a lock -- a
    one-table ADD behind a VACUUM or an index build (G17) -- fails instead of hanging.
    """
    if source_config.source_type is not SourceType.POSTGRES:
        raise PgReplicationError(
            "source-write engine is PostgreSQL-only "
            f"(got {source_config.source_type.value})"
        )
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    dialect = dialect_for(SourceType.POSTGRES)
    url = URL.create(
        dialect.driver_scheme,
        username=source_config.username,
        password=password.reveal() if password is not None else None,
        host=source_config.host,
        port=source_config.port,
        database=source_config.database,
    )
    # NB: deliberately NO install_read_only_guard here -- this is the sanctioned write
    # path. AUTOCOMMIT so pg_create_logical_replication_slot is not wrapped in a txn.
    return create_engine(
        url, **dialect.engine_kwargs(read_timeout_seconds=read_timeout_seconds)
    ).execution_options(isolation_level="AUTOCOMMIT")


def _assert_allowed(statement: str) -> None:
    """Raise unless ``statement`` is one of the fixed allowlisted source writes.

    ``ALTER TABLE`` is allowlisted ONLY for the exact ``... REPLICA IDENTITY FULL`` shape
    (a re-keyed table's before-image widening); any other ALTER (DROP/rename/type change)
    is refused, so the broad prefix cannot become an arbitrary DDL surface.
    """
    normalized = " ".join(statement.strip().upper().split())
    if not any(normalized.startswith(p) for p in _ALLOWED_WRITE_PREFIXES):
        raise PgReplicationError(
            f"refused non-allowlisted source write: {normalized[:60]!r}"
        )
    if normalized.startswith("ALTER TABLE ") and not normalized.endswith(
        " REPLICA IDENTITY FULL"
    ):
        raise PgReplicationError(
            f"refused non-allowlisted ALTER TABLE (only REPLICA IDENTITY FULL): "
            f"{normalized[:60]!r}"
        )
    if normalized.startswith("ALTER PUBLICATION ") and not (
        normalized.endswith(_PUBLICATION_VIA_ROOT_TAIL)
        or _PUBLICATION_ADD_TABLE_RE.match(normalized)
    ):
        raise PgReplicationError(
            "refused non-allowlisted ALTER PUBLICATION (only SET "
            "(publish_via_partition_root = true), or ADD TABLE ONLY of one quoted table): "
            f"{normalized[:60]!r}"
        )
    if normalized.startswith("SELECT PG_CURRENT_XACT_ID") and normalized != _XID_STATEMENT:
        raise PgReplicationError(
            f"refused non-allowlisted transaction-ID read: {normalized[:60]!r}"
        )


def _run_write(
    connection: object,
    statement: str,
    *,
    action: str,
    on_log: Optional[Callable[[str], None]],
    params: Optional[dict] = None,
):
    """Execute one allowlisted source write, logging it for audit. Returns the result.

    The audit line is written AFTER the statement commits, and a failure gets its own
    line. Logging the intent first made the audit trail assert source writes that never
    happened -- and these are writes on the CUSTOMER'S production source (creating a
    publication, widening a table's REPLICA IDENTITY permanently, dropping a
    WAL-pinning slot), so a record that claims one landed when it did not is the worst
    kind of false reassurance: the operator stops looking for the thing still there.
    """
    _assert_allowed(statement)
    try:
        result = connection.execute(text(statement), params or {})  # type: ignore[attr-defined]
    except Exception as exc:
        if on_log is not None:
            # First line only, no parameter values -- the statements are tool-built and
            # their params are identifiers, but the server message is not ours.
            on_log(f"FAILED: {action} - {str(exc).splitlines()[0][:200]}")
        raise
    if on_log is not None:
        on_log(action)
    return result


def publication_exists(connection: object, name: str) -> bool:
    """Return whether a publication named ``name`` already exists (read-only)."""
    row = connection.execute(  # type: ignore[attr-defined]
        text("SELECT 1 FROM pg_publication WHERE pubname = :name"), {"name": name}
    ).first()
    return row is not None


def slot_exists(connection: object, name: str) -> bool:
    """Return whether a replication slot named ``name`` already exists (read-only)."""
    row = connection.execute(  # type: ignore[attr-defined]
        text("SELECT 1 FROM pg_replication_slots WHERE slot_name = :name"),
        {"name": name},
    ).first()
    return row is not None


def publication_tables(connection: object, name: str) -> set:
    """Return the set of qualified ``schema.table`` names a publication covers.

    ``pg_publication_tables`` is the EFFECTIVE membership: for a partitioned table it
    reports the PARENT only because :func:`create_publication` sets
    ``publish_via_partition_root = true``. Without that option it expands to the LEAVES --
    which is what pgoutput then streams -- so the reconcile reads it rolled up to each
    partition root (:func:`_publication_membership`) and provisioning repairs the setting
    before its slot exists. Do not "fix" that by reading ``pg_publication_rel`` (which
    always reports the registered parent) -- that would hide the real defect, which is that
    leaf-named changes are what pgoutput would then stream.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
            "WHERE pubname = :name"
        ),
        {"name": name},
    ).fetchall()
    return {str(r[0]) for r in rows}


def publication_publishes_via_partition_root(
    connection: object, name: str
) -> "Optional[bool]":
    """Is ``name`` set to publish changes under the partition ROOT? ``None`` = no such publication.

    The ONE fact :func:`publication_tables` cannot give you: that view shows the
    CONSEQUENCE (leaves vs parent), not the setting, so a repair has to read the setting.
    ``pubviaroot`` is PostgreSQL 13+, and this tool targets 13-18.
    """
    row = connection.execute(  # type: ignore[attr-defined]
        text("SELECT pubviaroot FROM pg_publication WHERE pubname = :name"),
        {"name": name},
    ).first()
    return None if row is None else bool(row[0])


def ensure_publication_via_partition_root(
    connection: object,
    *,
    name: str,
    on_log: "Optional[Callable[[str], None]]" = None,
    before_slot: bool = False,
) -> str:
    """Make ``name`` publish under the partition ROOT. Returns what happened.

    ``before_slot``: the Full Load's provisioning repairs a REUSED publication before it
    creates the replication slot (G25), so nothing decoded from that slot is lost and the
    audit line says so.

    WHY this exists as a repair and not only as a create option: the tool's own
    :func:`create_publication` already sets the property, but it only runs on the GAPLESS
    route, where the Full Load provisions the publication at the consistency point. On a
    ``Full load only`` -> CDC start, and on a CDC-only start, the effective
    ``snapshot.mode`` is ``initial``, which derives
    ``publication.autocreate.mode=filtered`` -- and then DEBEZIUM creates the publication.
    The shipped 2.7.4 PostgreSQL plugin issues a bare
    ``CREATE PUBLICATION <n> FOR TABLE <t...>;`` and exposes NO property for this, so the
    publication lands at PostgreSQL's default ``publish_via_partition_root = false``.

    Under that default, pgoutput names every change after the LEAF partition while
    ``table.include.list`` names the collapsed PARENT (the target has no partitioning, so
    Full Load folds the leaves into one table). Debezium's include filter is an anchored
    full match, so every change to a partitioned table is filtered out and DISCARDED: the
    offset advances, the connector stays RUNNING, the DLQ stays empty, and the rows simply
    never arrive. Live-observed on Aurora PostgreSQL 17.7 (7 rows lost with no signal
    anywhere), then reproduced on PostgreSQL 17.11 by decoding the raw pgoutput stream --
    pre-flip the Insert carries the leaf relation, post-flip the parent.

    Three idempotent outcomes: ``"created"`` (absent -> create WITH the option, which
    :func:`create_publication` also does), ``"repaired"`` (present but false -> ``ALTER``
    in place: no DROP, the slot and its LSN survive, and an already-connected walsender
    picks the new relation naming up without a connector restart), ``"unchanged"``
    (present and already true -> one read, no write, so the gapless route is untouched).

    NOT retroactive. Changes COMMITTED before the repair keep decoding under the leaf name,
    so a repair recovers nothing already missed -- re-running Full Load for the affected
    table is what closes that gap (it is idempotent ``INSERT ... ON CONFLICT``).

    Takes NO table list on purpose. On the autocreate route Debezium issues its own
    ``ALTER PUBLICATION ... SET TABLE <table.include.list>``, so reconciling membership
    here would turn a changed selection -- which the connector fixes by itself -- into a
    hard failure. Coverage stays owned by :func:`pg_replication_objects_blocker`.
    """
    present = publication_publishes_via_partition_root(connection, name)
    if present is True:
        return "unchanged"
    if present is None:
        # An EMPTY publication carrying the property. Membership is deliberately not chosen
        # here: on this route Debezium owns it and issues its own
        # ``ALTER PUBLICATION ... SET TABLE <table.include.list>`` (verified behaviour --
        # ``filtered`` alters an existing publication rather than refusing it), so creating
        # it empty pre-sets the one property the connector cannot while leaving the table
        # set to the component that keeps it in sync with the capture list.
        _run_write(
            connection,
            f'CREATE PUBLICATION "{name}" WITH (publish_via_partition_root = true)',
            action=(
                f'created publication "{name}" with publish_via_partition_root = true '
                "(the connector adds its own tables)"
            ),
            on_log=on_log,
        )
        return "created"
    _run_write(
        connection,
        f'ALTER PUBLICATION "{name}" SET (publish_via_partition_root = true)',
        action=(
            f'repaired publication "{name}": publish_via_partition_root false -> true, '
            "before this Full Load's replication slot is created, so every change it "
            "streams for a partitioned table carries the parent the target has"
            if before_slot
            else f'repaired publication "{name}": publish_via_partition_root false -> true, '
            "so changes to a partitioned table stream under the parent the target has (in "
            "place -- the replication slot and its position are untouched). Changes "
            "committed BEFORE this repair are not recovered by it."
        ),
        on_log=on_log,
    )
    return "repaired"


@dataclass(frozen=True)
class PgReplicationObjects:
    """What the source actually has, for a CDC-start pre-flight. All read-only facts.

    ``publication_publishes_all_dml`` is ``None`` when no publication exists -- NULL means
    UNKNOWN, never False. ``slot_present_any_database`` vs ``slot_usable`` is a real
    distinction: ``pg_replication_slots`` is CLUSTER-wide, so a bare name match can hit a
    slot bound to a DIFFERENT database or created with another plugin, which the connector
    cannot use. That is why :func:`slot_exists` must not be reused as a pre-flight assertion.
    """

    publication_name: str
    slot_name: str
    publication_present: bool
    publication_publishes_all_dml: Optional[bool]
    publication_tables: frozenset
    slot_present_any_database: bool
    slot_usable: bool
    # Is the publication set to publish under the partition ROOT? ``None`` when there is no
    # publication (UNKNOWN, never False). ``False`` is a REPAIRABLE state, not a missing
    # table: the CDC start flips it in place before the connector exists
    # (:func:`ensure_publication_via_partition_root`), which is why
    # ``publication_tables`` below is rolled up to the parent rather than left as the leaves
    # ``pg_publication_tables`` reports under that setting. Without the roll-up the coverage
    # check reported a captured partitioned parent as MISSING and made a retry a dead end.
    publication_via_partition_root: Optional[bool] = None
    # The usable slot's ``confirmed_flush_lsn`` (text), ``None`` when there is no usable slot
    # or it was not read. A slot nothing has streamed from is still exactly at the consistent
    # point it was created at, which the Full Load records as ``Watermark.wal_lsn`` (G25).
    slot_confirmed_flush_lsn: Optional[str] = None
    # The asked tables that are plain tables with inheritance (INHERITS) child tables, as
    # ``(table, child count, first child names)`` -- read only for a Start that snapshots
    # (:func:`with_inheritance_children`), so empty means "none, or not asked" (G25), and
    # ``None`` that the read failed, which refuses such a Start: unreadable is not "none".
    inheritance_children: Optional[tuple] = ()


# A plain table's inheritance children (G25): a bare ``SELECT ... FROM`` of the table -- the
# query Debezium's snapshot runs -- returns their rows too. Catalog only: no scan, no lock.
# Direct children are enough to tell (a grandchild has a child as its parent); another
# session's temporary table is left out, as PostgreSQL's own expansion leaves it out.
_INHERITANCE_CHILDREN_SQL = (
    "SELECT n.nspname || '.' || c.relname AS parent, count(*) AS children, "
    "(array_agg(kn.nspname || '.' || k.relname ORDER BY kn.nspname, k.relname))[1:3] "
    "AS sample "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_catalog.pg_inherits i ON i.inhparent = c.oid "
    "JOIN pg_catalog.pg_class k ON k.oid = i.inhrelid "
    "JOIN pg_catalog.pg_namespace kn ON kn.oid = k.relnamespace "
    "WHERE c.relkind = 'r' AND k.relpersistence <> 't' "
    "AND n.nspname || '.' || c.relname = ANY(:names) "
    "GROUP BY n.nspname, c.relname ORDER BY 1"
)


def read_inheritance_children(connection: object, tables: Sequence[str]) -> tuple:
    """Which of ``tables`` are plain tables with inheritance child tables? Read-only (G25).

    ``((table, child count, first child names), ...)`` in name order; a partitioned table is
    never one (its partitions are CDC's own: they publish under it). One catalog statement,
    none for no table. Raises on failure, like :func:`read_pg_replication_objects`."""
    names = sorted({str(t) for t in tables or () if t})
    if not names:
        return ()
    rows = connection.execute(  # type: ignore[attr-defined]
        text(_INHERITANCE_CHILDREN_SQL), {"names": names}
    ).fetchall()
    return tuple(
        (str(r[0]), int(r[1] or 0), tuple(str(n) for n in (r[2] or ()))) for r in rows
    )


def read_pg_replication_objects(
    connection: object, *, publication_name: str, slot_name: str
) -> PgReplicationObjects:
    """Read the CDC publication + slot state from the source (read-only).

    Needs no special privilege: ``pg_publication`` / ``pg_replication_slots`` /
    ``pg_publication_tables`` are readable by an ordinary role (live-verified with
    ``rolsuper=f rolreplication=f`` and no grants, inside ``BEGIN TRANSACTION READ ONLY``),
    so this is strictly cheaper than the publication-privilege check that already runs.
    Which tables have inheritance child tables is a separate read
    (:func:`with_inheritance_children`), for a Start that snapshots only.

    RAISES on failure rather than returning "absent": unreadable is NOT evidence of
    absence, and the caller must be able to tell the two apart before it blocks anything.
    """
    row = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT "
            "(SELECT count(*) FROM pg_publication WHERE pubname = :pub) AS pub_n, "
            "(SELECT bool_and(pubinsert AND pubupdate AND pubdelete) "
            "   FROM pg_publication WHERE pubname = :pub) AS pub_all_dml, "
            "(SELECT count(*) FROM pg_replication_slots WHERE slot_name = :slot) "
            "  AS slot_any_n, "
            "(SELECT count(*) FROM pg_replication_slots WHERE slot_name = :slot "
            "   AND slot_type = 'logical' AND plugin = 'pgoutput' "
            "   AND database = current_database() "
            # An INVALIDATED slot still EXISTS (so slot_any_n counts it) but the connector
            # cannot stream from it -- grading it usable turns a recoverable re-snapshot
            # into a dead Debezium task. wal_status is PG13+ and the tool targets PG13-18,
            # so coalesce keeps it NULL-safe on an older server.
            "   AND coalesce(wal_status, 'reserved') <> 'lost') AS slot_ok_n"
        ),
        {"pub": publication_name, "slot": slot_name},
    ).first()
    pub_n = int(row[0] or 0)
    all_dml = row[1]
    covered = publication_tables(connection, publication_name) if pub_n else set()
    via_root = (
        publication_publishes_via_partition_root(connection, publication_name)
        if pub_n
        else None
    )
    if via_root is False and covered:
        # Roll each published LEAF up to its partition root. Under
        # publish_via_partition_root=false, ``pg_publication_tables`` EXPANDS a registered
        # partitioned parent into its leaves -- so a coverage check against the captured
        # (collapsed) parent name found it "missing" and blocked the start, even though the
        # parent is registered and the CDC start repairs the property before the connector
        # exists. Compare against what the publication will EFFECTIVELY cover after that
        # repair. The raw :func:`publication_tables` reader is left alone: what it reports
        # is what pgoutput would stream under the current setting.
        covered = {_partition_roots(connection).get(name, name) for name in covered}
    usable = int(row[3] or 0) > 0
    confirmed: Optional[str] = None
    if usable:
        position = connection.execute(  # type: ignore[attr-defined]
            text(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                "WHERE slot_name = :slot AND database = current_database()"
            ),
            {"slot": slot_name},
        ).first()
        if position is not None and position[0] is not None:
            confirmed = str(position[0])
    return PgReplicationObjects(
        publication_name=publication_name,
        slot_name=slot_name,
        publication_present=pub_n > 0,
        publication_publishes_all_dml=(None if all_dml is None else bool(all_dml)),
        publication_tables=frozenset(covered),
        slot_present_any_database=int(row[2] or 0) > 0,
        slot_usable=usable,
        publication_via_partition_root=via_root,
        slot_confirmed_flush_lsn=confirmed,
    )


def with_inheritance_children(
    connection: object, objects: PgReplicationObjects, tables: Sequence[str]
) -> PgReplicationObjects:
    """``objects`` with :attr:`~PgReplicationObjects.inheritance_children` read for
    ``tables`` -- for a Start that snapshots, which reads each captured table without ONLY
    (G25). Unreadable: ``None`` there, which refuses that Start
    (:func:`pg_replication_objects_blocker`), unlike a source the pre-flight could not
    connect to at all (``_cdc_ui._probe_pg_replication_objects`` does not block on that; it
    says the checks did not run): nothing else stops that snapshot from copying a child
    table's rows into its parent, or Debezium's own publication -- it names each table
    without ``ONLY`` -- from adding the child tables, whose REPLICA IDENTITY nothing
    grades."""
    try:
        children = read_inheritance_children(connection, tables)
    except Exception:  # noqa: BLE001 - unreadable is not "none": refuse the snapshot
        return replace(objects, inheritance_children=None)
    return replace(objects, inheritance_children=children)


def _partition_roots(connection: object) -> dict:
    """``{"schema.leaf": "schema.root"}`` for every partition on the source. Read-only.

    Scoped to ``relispartition`` so it stays small on a large catalog, and keyed on
    ``pg_partition_root`` (PostgreSQL 12+; the tool targets 13-18) so a multi-level
    partition tree rolls all the way up to the table the target actually has.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT n.nspname || '.' || c.relname, rn.nspname || '.' || r.relname "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_class r ON r.oid = pg_partition_root(c.oid) "
            "JOIN pg_namespace rn ON rn.oid = r.relnamespace "
            "WHERE c.relispartition"
        )
    ).fetchall()
    return {str(r[0]): str(r[1]) for r in rows}


# The slot-position block (G25). It judges only a stack whose connectors were never started --
# none stopped, in place or detected, or created by a Start whose update CloudFormation rolled
# back, whatever the rollback status (``_cdc_ui._pg_stack_started``); its way out is the route
# rule's (:func:`pg_cdc_route`), like every block's.
SLOT_POSITION_HEADER = "CDC's replication slot is not at this Full Load's start point"
# The other blocks whose stream cannot continue as is (G25), each routed by pg_cdc_route.
SLOT_MISSING_HEADER = "CDC's replication slot does not exist on the source"
SLOT_UNUSABLE_HEADER = "A replication slot of that name is not usable for CDC"
PUBLICATION_MISSING_HEADER = "CDC's publication does not exist on the source"
PUBLICATION_COVERAGE_HEADER = "CDC's publication does not cover every selected table"
# Blocks that name their own fix (G25), so the Start CDC dialog does not judge a re-snapshot
# for them (``_cdc_ui._pg_objects_gate_block``): a re-snapshot fixes none of them -- Debezium
# reuses an existing publication as it is, and its snapshot is what reads a parent's child
# tables. A read again works on any stack; the inheritance block's own fix is a Full Load run,
# which pg_cdc_route puts after new topics on a stack started before. The publish list's fix
# comes first but does not bring back the changes committed while the list was narrowed
# (pgoutput publishes from the publication as it stood at each change's commit), so
# pg_cdc_route names the route after it, as for every block whose stream cannot continue.
PUBLISH_LIST_HEADER = "CDC's publication does not publish every change type"
INHERITANCE_SNAPSHOT_HEADER = "CDC cannot snapshot a table that has inheritance child tables"
# The read behind that block failed: fail closed, and its fix is to read again (G25).
INHERITANCE_UNREADABLE_HEADER = (
    "Whether a captured table has inheritance child tables could not be read"
)
BLOCKS_WITH_THEIR_OWN_FIX = frozenset(
    {PUBLISH_LIST_HEADER, INHERITANCE_SNAPSHOT_HEADER, INHERITANCE_UNREADABLE_HEADER}
)

# The re-snapshot remedy's standing on a stack never started (``pg_cdc_route``'s
# ``resnapshot``): the Start CDC dialog judges the Start it would unlock before it offers it.
RESNAPSHOT_OFFERED = "offered"  # that Start's own checks passed: the dialog offers the remedy
RESNAPSHOT_REFUSED = "refused"  # they refused it, for the reason ``block`` names

_RERUN_FULL_LOAD = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
_SLOT_AGAIN = "it creates the slot again at its own snapshot point"
_NEW_TOPICS = (
    "use Delete CDC infrastructure and deploy it again (the new stack starts with empty topics)"
)
# Why a stack started before needs new topics before a Full Load re-run (G25): Stop CDC deletes
# the source and the sink in parallel, with no drain, and a rolled-back Start deletes them
# mid-stream, so the topics may hold changes no sink applied; this stack's sink group resumes
# from its last commit and applies them over the reloaded tables -- silently.
_UNAPPLIED_IN_TOPICS = (
    "its CDC topics may still hold changes that no sink applied (Stop CDC and a rolled-back "
    "Start remove the connectors without draining them), which this stack's sink would apply "
    "over a reload"
)
_SNAPSHOT_FIRST = "it snapshots every selected table"
# The blocks a stack started before gets the new-topics route for (G25): every one but a
# failed inheritance read, whose own fix (a read again) works on any stack.
_STARTED_ROUTED_BLOCKS = frozenset(
    {
        SLOT_POSITION_HEADER,
        SLOT_MISSING_HEADER,
        SLOT_UNUSABLE_HEADER,
        PUBLICATION_MISSING_HEADER,
        PUBLICATION_COVERAGE_HEADER,
        PUBLISH_LIST_HEADER,
        INHERITANCE_SNAPSHOT_HEADER,
    }
)
# The blocks whose own fix comes first, before the route (G25): a slot of that name it cannot
# use (drop it, or another CDC stack name -- a Full Load re-run refuses such a slot before any
# write), and a narrowed publish list (set it back -- which does not bring back the changes
# committed while it was narrowed).
_FIX_FIRST_BLOCKS = frozenset({SLOT_UNUSABLE_HEADER, PUBLISH_LIST_HEADER})


def _sentence(clause: str) -> str:
    """``clause`` as a sentence: its first letter capitalised, a period added. Pure."""
    return f"{clause[:1].upper()}{clause[1:]}."


def _new_topics_route(cdc_only: Optional[bool]) -> str:
    """The route of a stack started before, as a clause: new topics first (G25). Pure."""
    full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN})"
    if cdc_only is None:
        return (
            f"{_NEW_TOPICS}; then, for Full load + CDC, {full_load} and Start CDC, and for CDC "
            f"only, Start CDC on the new stack ({_SNAPSHOT_FIRST})"
        )
    if cdc_only:
        return (
            f"{_NEW_TOPICS}, then Start CDC on the new stack, which snapshots every selected "
            "table (a row deleted on the source in between stays on the target; Validation "
            "reports it as an EXTRA row)"
        )
    return f"{_NEW_TOPICS}, then {full_load}, then Start CDC"


def _no_snapshot_route(cdc_only: Optional[bool], *, note: str = "") -> str:
    """The route of a stack never started once a re-snapshot is ruled out, as a clause: a Full
    Load run, which creates the slot again, so the Start CDC after it takes no snapshot -- after
    a switch to Full load + CDC with CDC only, which has no Full Load step (G25). Pure."""
    full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN}{note}), then Start CDC"
    if cdc_only is None:
        return f"{full_load} (with CDC only, switch the migration type to Full load + CDC first)"
    if cdc_only:
        return f"switch the migration type to Full load + CDC, then {full_load}"
    return full_load


def pg_cdc_route(
    block: Optional[str],
    *,
    started: bool,
    cdc_only: Optional[bool],
    resnapshot: Optional[str] = None,
) -> str:
    """The way out of a PostgreSQL CDC block whose stream cannot continue as is -- THE route
    rule (G25). Every surface that names a route takes it from here: the Start CDC dialog's
    block, its withheld re-snapshot notice, the Start CDC job's refusal, the "Publication
    autocreation is disabled" diagnosis and the Start CDC / start-point cards. Pure.

    ``block``: the block's header; ``None`` for a card naming the route of a stack started
    before mid-sentence (the bare clause, no capital, no period). ``started``: the stack's
    connectors were started before (``_cdc_ui._pg_stack_started``: stopped since, in place, or
    created by a Start CloudFormation rolled back). ``cdc_only``: the migration type is CDC
    only (``None``: not known here, so both types are named).

    A stack started before: its CDC topics may hold changes no sink applied, and its sink would
    resume from its last commit, so its ONLY route is new topics first -- Delete CDC
    infrastructure and deploy it again, then (Full load + CDC) Re-run Full Load with Drop &
    reload, then Start CDC; (CDC only) Start CDC on the new stack, which snapshots. A Full Load
    re-run on its topics would have those changes applied over the reloaded tables, and a
    re-snapshot cannot be relied on there: Debezium continues from a stored offset whose
    snapshot completed.

    A stack never started: the re-snapshot remedy when its own checks pass (``resnapshot`` is
    :data:`RESNAPSHOT_OFFERED`: nothing to add, the dialog offers it), else a Start that takes
    no snapshot after a Full Load re-run -- a step after the block's own fix when the block
    names one -- or, with CDC only, the Start once its checks pass, Delete CDC infrastructure
    for a publication that misses a table, and a switch to Full load + CDC where only a Start
    with no snapshot can run (never a Full Load step it does not have). ``resnapshot`` is
    :data:`RESNAPSHOT_REFUSED` when ``block`` is the refusal of that remedy (or of the Start for
    the same reason), ``None`` when it was not judged (the Start CDC job): both are named.

    A narrowed publish list's own fix comes first, and the route follows it: setting the list
    back does not bring back the changes committed while it was narrowed (pgoutput publishes
    from the publication as it stood when each change committed), so a Start that continues
    from the slot would skip them -- a Full Load re-run after the fix (with CDC only, behind a
    switch to Full load + CDC), or new topics first on a stack started before. A failed
    inheritance read names its own fix, which works on any stack: no route. So does the
    inheritance block of a never-started stack's own Start."""
    if not started:
        return _never_started_route(block, cdc_only=cdc_only, resnapshot=resnapshot)
    if block is None:
        return _new_topics_route(cdc_only)
    if block not in _STARTED_ROUTED_BLOCKS:
        return ""
    if block == INHERITANCE_SNAPSHOT_HEADER:
        # Its own fix is a Full load + CDC run: new topics first (either type).
        return (
            f"This stack's connectors were started before, and {_UNAPPLIED_IN_TOPICS}. So "
            f"before that Full Load, {_NEW_TOPICS}."
        )
    # A block that names its own fix (a slot of that name it cannot use, a narrowed publish
    # list): the route comes after it.
    head = "The gapless route"
    if block in _FIX_FIRST_BLOCKS:
        head = "After that fix, the gapless route"
    return (
        "This stack's connectors were started before: if its source connector streamed, "
        "Debezium skips the snapshot on every later Start, so a re-snapshot cannot be relied on "
        f"here, and {_UNAPPLIED_IN_TOPICS}. {head}: {_new_topics_route(cdc_only)}."
    )


def _never_started_route(
    block: Optional[str], *, cdc_only: Optional[bool], resnapshot: Optional[str]
) -> str:
    """:func:`pg_cdc_route` for a stack whose connectors were never started. Pure."""
    if block == PUBLICATION_COVERAGE_HEADER:
        # A re-snapshot grades coverage too, so it never helps: a Full Load re-run adds each
        # missing table to the reused publication (G17); Delete CDC infrastructure drops it.
        full_load = (
            f"{_RERUN_FULL_LOAD}, which adds each missing selected table to the publication and "
            "creates the slot again at its own snapshot point, then Start CDC"
        )
        delete = (
            "use Delete CDC infrastructure and deploy it again: it drops the publication, so the "
            "new stack's Start CDC can create one over the selected tables"
        )
        if cdc_only is None:
            return f"{full_load} (with CDC only, {delete})."
        return _sentence(delete if cdc_only else full_load)
    if block == SLOT_UNUSABLE_HEADER:
        # After its own fix (drop that slot, or another CDC stack name): a Full Load re-run
        # would refuse such a slot before any write. CDC only: the Start, its checks then pass.
        full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN}), then Start CDC"
        if cdc_only is None:
            return f"Then {full_load} (with CDC only, Start CDC again)."
        return "Then Start CDC again." if cdc_only else f"Then {full_load}."
    if block == PUBLISH_LIST_HEADER:
        # After its own fix (the list set back): a Start that continues from the slot would
        # still skip the changes committed while it was narrowed, and a re-snapshot is not
        # offered (it reuses the publication as it is), so a Full Load re-run takes a new start
        # point -- with CDC only, after a switch to Full load + CDC.
        return f"Then {_no_snapshot_route(cdc_only)}."
    if block in (INHERITANCE_SNAPSHOT_HEADER, INHERITANCE_UNREADABLE_HEADER):
        # The refusal of the re-snapshot remedy: a Start with no snapshot works. As the block of
        # the Start itself, its own fix stands alone.
        if resnapshot != RESNAPSHOT_REFUSED:
            return ""
        no_snapshot = f"{_SLOT_AGAIN}, and the Start CDC after it takes no snapshot"
        if cdc_only:
            return (
                f"Or switch the migration type to Full load + CDC, then {_RERUN_FULL_LOAD}: "
                f"{no_snapshot}."
            )
        if cdc_only is None:
            return (
                f"{_RERUN_FULL_LOAD} instead (with CDC only, switch the migration type to Full "
                f"load + CDC first): {no_snapshot}."
            )
        return f"{_RERUN_FULL_LOAD} instead: {no_snapshot}."
    if block in (SLOT_POSITION_HEADER, SLOT_MISSING_HEADER, PUBLICATION_MISSING_HEADER):
        # A Start that resumes from the Full Load's slot: a re-snapshot fixes it when its own
        # checks pass, else a Full Load run creates the slot again.
        if resnapshot == RESNAPSHOT_OFFERED:
            return ""
        note = (
            "; give this migration its own CDC stack name if another migration shares this one"
            if block == SLOT_POSITION_HEADER
            else ""
        )
        route = _no_snapshot_route(cdc_only, note=note)
        if resnapshot == RESNAPSHOT_REFUSED:
            return _sentence(route)
        return (
            "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
            f"re-snapshots passes its own checks), or {route}."
        )
    return ""


def publish_list_fix(publication_name: str) -> str:
    """The fix for a publication restricted to a subset of INSERT/UPDATE/DELETE: its publish
    list set in place, by its owner. A re-snapshot and a Full Load re-run both reuse an
    existing publication as it is; only this -- or Delete CDC infrastructure, which drops
    it -- changes the list (G25). Pure."""
    quoted = dialect_for(SourceType.POSTGRES).quote_identifier(str(publication_name))
    return (
        f"As its owner, run ALTER PUBLICATION {quoted} SET (publish = 'insert, update, "
        "delete') on the source"
    )


def _inheritance_snapshot_block(parents: Sequence[tuple], *, untick: bool = True) -> tuple:
    """The block for a Start that snapshots a plain table with inheritance child tables
    (``parents``: :func:`read_inheritance_children` rows, at least one). Pure (G25).

    ``untick``: the table set can still change, so unticking the parent is a fix the UI can
    carry out. Once a Start was submitted (or with CDC only, while its infrastructure exists)
    the table picker is locked, and the block names only the Start from a Full Load's snapshot
    point."""
    table, count, sample = parents[0]
    shown = ", ".join(sample) + (" …" if count > len(sample) else "")
    others = len(parents) - 1
    more = (
        f" (and {others} more captured table{'s' if others != 1 else ''} like it)"
        if others
        else ""
    )
    alternative = (
        f" -- or untick {table}: each child table migrates as a table of its own."
        if untick
        else "."
    )
    return (
        INHERITANCE_SNAPSHOT_HEADER,
        f"{table} has {count} inheritance (INHERITS) child table{'s' if count != 1 else ''}: "
        f"{shown}{more}. This Start sends snapshot.mode=initial, and Debezium's snapshot "
        f"reads {table} with its child tables' rows: they would be copied into {table} on "
        f"Aurora DSQL, where CDC never updates or deletes them (it captures only {table}'s "
        "own changes). Start CDC from a Full Load's snapshot point instead -- a \"Full load "
        f"+ CDC\" run whose Full Load has finished reads only {table}'s own rows, and its "
        f"CDC starts without a snapshot{alternative}",
    )


# Fail closed (G25): a snapshot of a parent copies its child tables' rows into it, and
# Debezium's own publication then names it without ONLY, adding child tables whose REPLICA
# IDENTITY nothing grades -- so a Start that snapshots never goes ahead without that read.
_INHERITANCE_UNREADABLE_BLOCK = (
    INHERITANCE_UNREADABLE_HEADER,
    "This Start sends snapshot.mode=initial. Debezium's snapshot reads a table with "
    "inheritance (INHERITS) child tables together with their rows, which CDC never updates "
    "or deletes on Aurora DSQL, so Start CDC first reads which captured tables have any, and "
    "that read failed. Check that the source is reachable from the tool, then Start CDC "
    "again.",
)


def _lsn_value(lsn: Optional[str]) -> Optional[int]:
    """A PostgreSQL ``X/Y`` LSN as one integer, or ``None`` when it is not one. Pure."""
    high, separator, low = str(lsn or "").strip().partition("/")
    try:
        return (int(high, 16) << 32) | int(low, 16) if separator else None
    except ValueError:
        return None


def pg_replication_objects_blocker(
    objects: PgReplicationObjects,
    selected_tables: Sequence[str],
    *,
    resumes_from_slot: bool,
    connector_autocreates_publication: bool = False,
    start_lsn: Optional[str] = None,
    stack_started: bool = False,
    snapshots: bool = False,
    untick: bool = True,
) -> Optional[tuple]:
    """Why CDC must not start against these objects, as ``(header, body)``, or None. Pure.

    The two keywords are the connector's INTENT and must mirror
    ``build_pg_source_config`` exactly, or this refuses a configuration that would have
    worked:

    * ``resumes_from_slot`` <-> ``snapshot.mode=never``. True only when the watermark
      carries the slot the Full Load created. With ``initial`` instead, Debezium creates
      the slot itself and snapshots every captured table, so a missing slot is not a gap
      and must not block.
    * ``connector_autocreates_publication`` <-> ``publication.autocreate.mode=filtered``.
      True when the connector's own DB user will create the publication over exactly the
      captured tables. Without this, an absent publication blocked UNCONDITIONALLY -- so
      the Start dialog's "Re-snapshot every table instead" button un-disabled Start and
      the worker backstop, which calls this function right before creating anything, then
      raised "Start CDC blocked". The escape hatch shipped dead in v0.1.507.

    Coverage is graded, not just existence: a publication that omits a selected table lets
    Debezium reach RUNNING while replicating NOTHING for it -- strictly worse than a task
    that dies loudly. Deliberately NOT graded here: ``puballtables`` and whether the slot is
    currently ``active`` (neither is a failure of THIS migration).

    The slot's POSITION too (G25), on a stack's FIRST Start only: ``start_lsn`` is the Full
    Load watermark's LSN -- the consistent point of the slot it created. While the stack
    records no Start of its connectors (``stack_started`` False), none of them has streamed
    from the slot, so it must still be exactly there; anywhere else, another run using the
    same CDC stack name re-created it (or read it), and resuming would silently skip the
    changes in between. Its way out, like every block's, is the route rule's
    (:func:`pg_cdc_route`), which knows the migration type and the re-snapshot remedy's
    standing: the block itself names none. A stack whose connectors were started before is
    never judged on it: its connector moved the slot legitimately, and Debezium
    continues from the later of its stored offset and the slot's confirmed point (a Full Load
    that takes a new start point waits for new topics there: the Full Load step refuses it on
    a stack that has streamed, ``_cdc_status.cdc_stack_has_streamed``). A Start whose
    CloudFormation update rolled back counts as one, whatever the rollback status (complete,
    in progress or failed), and so do connectors of the stack still in place
    (``_cdc_ui._pg_stack_started``): its source connector may have streamed before the
    rollback deleted it. Stateless -- what the
    stack records decides, nothing the UI keeps -- so a refused Start, one that failed before
    it updated the stack, an app restart and Start over all leave the next Start judged the
    same way. Residual, like a re-created slot after this stack streamed (each migration
    needs its own CDC stack name): another migration re-creates the slot after this stack's
    first Start rolled back.

    ``snapshots`` <-> ``snapshot.mode=initial`` (G25): a captured plain table with inheritance
    child tables (``objects.inheritance_children``) blocks, because Debezium's snapshot reads
    it without ``ONLY``, so its children's rows would land in its target, and CDC never
    changes them there -- and so does a failed read of them (``None``): fail closed, as this
    block is all that keeps Debezium's own publication from adding child tables nothing
    grades. ``untick`` (:func:`_inheritance_snapshot_block`): the table set can still change,
    so that block also names unticking the parent. A slot of that name it cannot use is
    reported before it: every route needs that slot's own fix first. A narrowed publish list
    names its own fix (:func:`publish_list_fix`), which no re-snapshot or Full Load re-run
    makes, and which does not bring back the changes committed while the list was narrowed:
    the route rule's route follows it (:func:`pg_cdc_route`).
    """
    pub = objects.publication_name
    if not objects.publication_present and not connector_autocreates_publication:
        return (
            PUBLICATION_MISSING_HEADER,
            f"The connector is configured with publication.autocreate.mode=disabled and "
            f"expects a publication named '{pub}', which is not on the source. Only a "
            "'Full load + CDC' run creates it; a 'Full load only' run does not. Starting "
            "now would deploy the connectors and the Debezium task would die immediately "
            "with \"Publication autocreation is disabled\".",
        )
    # Coverage and publish-list grading READ the publication, so they are meaningless when
    # it does not exist yet and the connector is about to create it: an empty
    # publication_tables would otherwise report every selected table as "missing" and
    # block on the very next branch.
    missing = (
        sorted(set(selected_tables) - set(objects.publication_tables))
        if objects.publication_present
        else []
    )
    if missing:
        shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        return (
            PUBLICATION_COVERAGE_HEADER,
            f"Publication '{pub}' exists but does not include {len(missing)} of the "
            f"selected tables: {shown}. Those tables would replicate NOTHING while the "
            "connector reported RUNNING, which is worse than a visible failure.",
        )
    if objects.publication_present and objects.publication_publishes_all_dml is False:
        # Its own fix first, then the route rule's (decision C, G25): pgoutput decides what to
        # publish from the publication as it stood when each change committed, so setting the
        # list back does not bring back what was left out while it was narrowed.
        return (
            PUBLISH_LIST_HEADER,
            f"Publication '{pub}' is restricted to a subset of INSERT/UPDATE/DELETE, so "
            "the change types it omits would never reach the target and the connector "
            "would still report RUNNING. A re-snapshot and a Full Load re-run both reuse "
            f"it as it is. {publish_list_fix(pub)} first. Setting the list back does not "
            "bring back the changes committed while it was narrowed: pgoutput decides what to "
            "publish from the publication as it stood when each change committed, so a Start "
            "that continues from the slot would still skip them.",
        )
    if resumes_from_slot and not objects.slot_usable:
        # A same-named slot it cannot use (G25): its own fix comes before any route -- a Full
        # Load re-run drops only an inactive slot of this database, and refuses another
        # database's (or another type's) before any write.
        elsewhere = (
            " A slot of that name does exist in this cluster (invalidated, in another database, "
            "or not a logical pgoutput slot): if it is another database's or of another type, "
            "drop it or give this migration its own CDC stack name first -- a Full Load re-run "
            "refuses such a slot before any write."
            if objects.slot_present_any_database
            else ""
        )
        return (
            SLOT_MISSING_HEADER,
            f"This start is configured to resume from replication slot "
            f"'{objects.slot_name}' (snapshot.mode=never), and the source has no usable "
            "logical pgoutput slot of that name in this database. A slot cannot be created "
            "at a past position, so one created now would start from the CURRENT WAL and "
            f"silently skip every change committed since the Full Load.{elsewhere}",
        )
    # Before the inheritance check (G25): every route needs this fix first -- a Start that
    # snapshots cannot create its own slot under that name, and a Full Load re-run refuses
    # such a slot before any write -- so the refusal of a re-snapshot names it, not a Full
    # Load re-run that would be refused too.
    if objects.slot_present_any_database and not objects.slot_usable:
        return (
            SLOT_UNUSABLE_HEADER,
            f"A slot named '{objects.slot_name}' exists in this cluster but is not a "
            "logical pgoutput slot in this database, so the connector cannot use it and "
            "cannot create its own under that name. Drop it, or use a different CDC stack "
            "name so the derived slot name differs.",
        )
    if snapshots:
        if objects.inheritance_children is None:
            return _INHERITANCE_UNREADABLE_BLOCK
        captured = set(selected_tables)
        parents = [p for p in objects.inheritance_children if p[0] in captured]
        if parents:
            return _inheritance_snapshot_block(parents, untick=untick)
    at = _lsn_value(objects.slot_confirmed_flush_lsn)
    wanted = _lsn_value(start_lsn)
    if (
        resumes_from_slot
        and not stack_started
        and at is not None
        and wanted is not None
        and at != wanted
    ):
        return (
            SLOT_POSITION_HEADER,
            f"Slot '{objects.slot_name}' is at {objects.slot_confirmed_flush_lsn}, not at "
            f"{start_lsn} where this Full Load's CDC starts, and this CDC stack records no "
            "Start of its connectors (not even one that rolled back), so none of them has "
            "streamed from it: another run with the same CDC stack name re-created or read "
            "it, so starting now would silently skip the changes in between.",
        )
    return None


def _verify_tables_replicable(
    connection: object,
    tables: Sequence[str],
    *,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """Raise if any table lacks a REPLICA IDENTITY usable for UPDATE/DELETE replication.

    A last-line source-safety guard at the write layer (independent of the advisory
    prerequisite check, which can be bypassed): adding a REPLICA IDENTITY NOTHING table to
    a publication ARMS a source-write outage -- every UPDATE/DELETE on it then ERRORs on
    the publisher. relreplident 'n' (nothing) is refused; 'd'/'f'/'i' are usable ('d'
    relies on the primary key, which the prerequisite gate separately requires). Best-effort
    read: if relreplident is unreadable the table is not blocked here (the prereq owns that).

    Only :func:`provision_pg_replication` uses this lenient check, and since G20 only
    second: the strict, fail-closed :func:`_verify_tables_publishable` runs first and grades
    every table that provisioning would ADD to the publication (a valid immediate primary
    key behind DEFAULT, a valid identity index behind USING INDEX, every partition) from
    the source's live catalog -- the Step 1 inventory can predate a dropped key. This check
    still refuses NOTHING for EVERY requested table, members of a reused publication
    included, so nothing it refused before passes now. A one-table ADD after a Full Load
    (:func:`add_table_to_publication`, G17) runs the strict per-table
    :func:`_verify_table_publishable`.

    A refusal is :class:`PgReplicaIdentityError` in the strict check's one-line format
    (:func:`_publish_refusal_message`: the first table, how many more, and the fix -- a new
    primary key does not clear NOTHING, only ALTER TABLE ... REPLICA IDENTITY does), with
    one audit line per refused table through ``on_log`` (G21).
    """
    names = list(tables)
    if not names:
        return
    try:
        rows = connection.execute(  # type: ignore[attr-defined]
            text(
                "SELECT n.nspname || '.' || c.relname, c.relreplident FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname || '.' || c.relname = ANY(:names)"
            ),
            {"names": names},
        ).fetchall()
    except Exception:  # noqa: BLE001 - if unreadable, let the prereq gate own it
        return
    nothing = sorted(str(r[0]) for r in rows if str(r[1]) == "n")
    if not nothing:
        return
    if on_log is not None:
        for name in nothing:
            on_log(f"refused before any write: {name} has REPLICA IDENTITY NOTHING")
    # relkind is not read here: NOTHING's fix is the same for a plain table and for a
    # partitioned table's own NOTHING, and this read claims no partition facts.
    read = ReplicaIdentityUsability(
        kind="r", identity="n", usable=False, unusable_partitions=0,
        unusable_partition_names=(),
    )
    problem = "REPLICA IDENTITY NOTHING"  # replica_identity_problem's phrase for it
    raise PgReplicaIdentityError(
        _publish_refusal_message([(name, problem, read) for name in nothing], []),
        cause=problem,
        remedy=replica_identity_remedy(read),
    )


def _verify_table_publishable(connection: object, table: str) -> None:
    """Raise :class:`PgReplicaIdentityError` unless ``table`` keeps a replica identity
    PostgreSQL accepts for UPDATE/DELETE once published (G17) -- FAIL CLOSED.

    The last line before a one-table ADD to a publication that publishes UPDATE and DELETE:
    without a usable identity every UPDATE/DELETE on the table then fails on the source, and
    nothing removes the member afterwards. One catalog read
    (:func:`read_replica_identity_usability`, the relcache's rule: a valid primary key behind
    DEFAULT, a valid identity index behind USING INDEX, FULL, and every leaf of a partitioned
    table). An unreadable answer, or a table not found, refuses too: unreadable is not
    usable. Every refusal is one bounded line (the name clipped, the next step last), like
    the provisioning refusal's.
    """
    try:
        reads = read_replica_identity_usability(connection, [table])
    except Exception as exc:  # noqa: BLE001 - unreadable is not usable
        raise PgReplicaIdentityError(
            f"{_clip_table_name(table)}: whether it has a usable REPLICA IDENTITY could not "
            f"be read ({type(exc).__name__[:64]}), so it was not added: check that the "
            "source is reachable from the tool, then re-run the Full Load.",
            unreadable=True,
        ) from exc
    read = reads.get(table)
    if read is None:
        raise PgReplicaIdentityError(
            f"{_clip_table_name(table)}: not found on the source as a table, so it was not "
            "added: re-run Step 1 (Evaluation) if it was dropped or renamed, then re-run "
            "the Full Load.",
            unreadable=True,
        )
    problem = replica_identity_problem(read)
    if problem:
        remedy = replica_identity_remedy(read)
        # One bounded line: its only displayed channel is the Full Load's error (a reused
        # publication's ADD refused after the up-front check), whose first 300 characters the
        # JobManager keeps -- the name clipped, partition names left to ``cause`` (the
        # per-table row shows them).
        raise PgReplicaIdentityError(
            f"{_clip_table_name(table)}: {problem.split(': ', 1)[0]}. Once published, every "
            "UPDATE and DELETE on it would fail on the source, so it was not added: "
            f"{remedy}, then re-run the Full Load.",
            cause=problem,
            remedy=remedy,
        )


# The provisioning refusal's one-line messages (G20). The JobManager keeps the first line of
# the error, capped at 300 characters, so the leading table's name is clipped and partition
# names are left to the per-table audit lines.
_PUBLISH_NAME_LIMIT = 32
_PUBLISH_READ_FAILED = (
    "Whether each table to publish for CDC (and each partition) has a usable REPLICA "
    "IDENTITY could not be read ({cls}). Nothing was changed on the source: check that it "
    "is reachable from the tool, then run the Full Load again."
)


def _clip_table_name(name: str) -> str:
    """``name`` for a one-line message: at most 32 characters, and a character that is not
    printable -- a quoted identifier may hold a line separator, after which the JobManager
    keeps nothing -- shown as ``?``."""
    name = "".join(ch if ch.isprintable() else "?" for ch in name)
    if len(name) <= _PUBLISH_NAME_LIMIT:
        return name
    return name[: _PUBLISH_NAME_LIMIT - 3] + "..."


# A re-keyed table provisioning would set REPLICA IDENTITY FULL on (its composite key needs
# the full before-image) where PostgreSQL 18 refuses UPDATE/DELETE under a published FULL:
# it has a generated column (``full_refused``). Its own cause and remedy -- DEFAULT would
# lose its deletes, so the fix is to drop the re-key (G25).
_REKEYED_FULL_REFUSED = (
    "needs REPLICA IDENTITY FULL, but has generated columns (PostgreSQL 18)"
)
_REKEYED_FULL_REFUSED_REMEDY = 'set it back to "Keep source PK" in Schema Conversion'
# Public, for the other surfaces of that cause (the Full load + CDC Retry guard, G25).
REKEYED_FULL_REFUSED_PROBLEM = _REKEYED_FULL_REFUSED
REKEYED_FULL_REFUSED_REMEDY = _REKEYED_FULL_REFUSED_REMEDY


def _refusal_remedy(problem: str, read: ReplicaIdentityUsability) -> str:
    if problem == _REKEYED_FULL_REFUSED:
        return _REKEYED_FULL_REFUSED_REMEDY
    return replica_identity_remedy(read)


def _publish_refusal_message(
    unusable: "Sequence[tuple[str, str, ReplicaIdentityUsability]]",
    missing: Sequence[str],
) -> str:
    """The one-line refusal for :func:`_verify_tables_publishable`: the leading table (the
    first unusable one, else the first not found), its cause (partition names dropped:
    they are in the audit lines), how many more were refused, and the remedy for that
    cause (:func:`replica_identity_remedy`). Pure."""
    others = len(unusable) + len(missing) - 1
    more = f" (+{others} more table{'s' if others != 1 else ''})" if others > 0 else ""
    if unusable:
        name, problem, read = unusable[0]
        cause = problem.split(": ", 1)[0]
        remedy = _refusal_remedy(problem, read)
        return (
            f"{_clip_table_name(name)}: {cause}{more}. Published for CDC, its UPDATE/DELETE "
            f"would fail on the source. Nothing was changed there: {remedy}, then re-run the "
            "Full Load."
        )
    return (
        f"{_clip_table_name(missing[0])}: not found on the source as a table{more}, so its "
        "REPLICA IDENTITY could not be checked. Nothing was changed there: re-run Step 1 "
        "(Evaluation) if it was dropped or renamed, then run the Full Load again."
    )


def _verify_tables_publishable(
    connection: object,
    tables: Sequence[str],
    *,
    publication: str,
    widened: Sequence[str] = (),
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """Refuse -- BEFORE any write -- to publish a table whose UPDATE/DELETE would then fail
    on the source (G20): :class:`PgReplicaIdentityError`, fail closed.

    The Full Load's provisioning check (:func:`provision_pg_replication`), with the strict
    rule of :func:`_verify_table_publishable` (G17) for every table this provisioning would
    add to ``publication``: all of ``tables`` when it does not exist yet, those it lacks
    (membership rolled up to the partition root, as :func:`add_table_to_publication`
    decides) when it does, none for a ``FOR ALL TABLES`` one. Members are not graded here
    (the lenient NOTHING check still covers every requested table). One membership read and
    ONE catalog read (:func:`read_replica_identity_usability`). A table in ``widened`` --
    one provisioning sets REPLICA IDENTITY FULL on just before the publication -- is graded
    as FULL: a plain table then passes, while a partitioned one is still graded by its
    leaves (only the parent is widened) -- except a plain one with a generated column on
    PostgreSQL 18 (``full_refused``), member or not: published, the FULL it needs would
    refuse its every UPDATE/DELETE, so it is refused with its own cause and remedy (G25). A
    failed read, or a table not found as a plain or partitioned table, refuses too:
    unreadable is not usable. Every refused table gets one audit line through ``on_log``
    (sorted, so the result is deterministic); the error message is one bounded line
    (:func:`_publish_refusal_message`).
    """
    requested = sorted({str(t) for t in tables or () if t})
    widened_set = {str(t) for t in widened or ()}
    try:
        membership = _publication_membership(connection, publication)
        if membership is None:
            candidates = requested
        elif membership[0]:
            candidates = []  # FOR ALL TABLES: nothing is added
        else:
            candidates = sorted(set(requested) - set(membership[1]))
        to_read = sorted(set(candidates) | widened_set)
        reads = read_replica_identity_usability(connection, to_read) if to_read else {}
    except Exception as exc:  # noqa: BLE001 - unreadable is not usable
        cls = type(exc).__name__[:64]
        if on_log is not None:
            on_log(
                f"refused before any write: the REPLICA IDENTITY read failed ({cls}), so "
                "nothing is published"
            )
        raise PgReplicaIdentityError(
            _PUBLISH_READ_FAILED.format(cls=cls), unreadable=True
        ) from exc
    unusable: list[tuple[str, str, ReplicaIdentityUsability]] = []
    missing: list[str] = []
    candidate_set = set(candidates)
    for name in to_read:
        read = reads.get(name)
        if name in widened_set and read is not None:
            if read.kind != "p" and read.full_refused:
                unusable.append((name, _REKEYED_FULL_REFUSED, read))
                continue
            # Provisioning sets it FULL before the publication; a partitioned table's
            # leaves are not widened, so they are still graded.
            read = read._replace(identity="f", usable=True)
        if name not in candidate_set:
            continue  # a member: only the widening is this run's change
        if read is None:
            missing.append(name)
            continue
        problem = replica_identity_problem(read)
        if problem:
            unusable.append((name, problem, read))
    if not unusable and not missing:
        return
    if on_log is not None:
        for name, problem, _read in unusable:
            if problem != _REKEYED_FULL_REFUSED:
                on_log(
                    f"refused before any write: {name} has no usable REPLICA IDENTITY "
                    f"({problem}), so it is not published"
                )
                continue
            # A member of the reused publication is published already, and it stays.
            outcome = (
                "and it is not published"
                if name in candidate_set
                else "(it stays in the publication)"
            )
            on_log(
                f"refused before any write: {name} is re-keyed, so it needs REPLICA "
                "IDENTITY FULL, and on PostgreSQL 18 a published FULL table with generated "
                "columns refuses every UPDATE and DELETE; its REPLICA IDENTITY was not "
                f"changed {outcome}"
            )
        for name in missing:
            on_log(
                f"refused before any write: {name} was not found on the source as a table, "
                "so it is not published"
            )
    raise PgReplicaIdentityError(
        _publish_refusal_message(unusable, missing),
        cause=unusable[0][1] if unusable else None,
        remedy=_refusal_remedy(unusable[0][1], unusable[0][2]) if unusable else None,
        unreadable=not unusable,
    )


def pg_sqlstate(exc: BaseException) -> Optional[str]:
    """The SQLSTATE a PostgreSQL error carries, or ``None``. Looks at the exception, the
    driver error SQLAlchemy wraps (``.orig``) and the cause (psycopg 3 ``sqlstate``, psycopg 2
    ``pgcode``). Pure."""
    for candidate in (exc, getattr(exc, "orig", None), getattr(exc, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if code:
            return str(code)
    return None


def _publication_membership(connection: object, name: str) -> "Optional[tuple[bool, set]]":
    """``(for_all_tables, members)`` of publication ``name`` (read-only), or ``None`` when it
    does not exist. ``members`` is the EFFECTIVE membership rolled up to each partition root
    when ``publish_via_partition_root`` is false (the same roll-up
    :func:`read_pg_replication_objects` applies), so a registered partitioned parent reads
    as a member either way."""
    row = connection.execute(  # type: ignore[attr-defined]
        text("SELECT puballtables, pubviaroot FROM pg_publication WHERE pubname = :name"),
        {"name": name},
    ).first()
    if row is None:
        return None
    members = publication_tables(connection, name)
    if row[1] is False and members:
        roots = _partition_roots(connection)
        members = {roots.get(member, member) for member in members}
    return bool(row[0]), members


def add_table_to_publication(
    connection: object,
    *,
    name: str,
    table: str,
    on_log: Optional[Callable[[str], None]] = None,
) -> bool:
    """Add ONE table to publication ``name``; return whether this call added it (G17).

    ``ALTER PUBLICATION "<name>" ADD TABLE ONLY <one quoted table>`` -- the one shape the
    allowlist accepts (``ONLY``: never its inheritance children, G25). Idempotent: a member
    (read first, rolled up to the partition root), a ``FOR ALL TABLES`` publication, or a
    concurrent add that lost the race (SQLSTATE 42710, already a member) issues no further
    write and returns False. A table with no replica identity PostgreSQL accepts for
    UPDATE/DELETE -- NOTHING, DEFAULT with no valid primary key, USING INDEX whose index is
    gone, or such a leaf partition -- is refused BEFORE any write with
    :class:`PgReplicaIdentityError` (:func:`_verify_table_publishable`: publishing it would
    make its UPDATE/DELETE fail on the source), and so is one whose identity cannot be read
    (fail closed). An absent publication -- also one dropped after that read (SQLSTATE
    42704) -- raises :class:`PgPublicationMissingError`; any other failure propagates (the
    caller grades it by SQLSTATE). The audit line is written after the ADD commits
    (AUTOCOMMIT).

    pgoutput decodes a table's changes only from when it is a publication member, so a
    table added for a load must be added BEFORE its copy starts, and its copy may start only
    once every transaction open at the add has ended (:func:`take_xid_after_change`,
    :func:`wait_for_transactions_before`).
    """
    membership = _publication_membership(connection, name)
    if membership is None:
        raise PgPublicationMissingError(f"publication {name} does not exist on the source")
    for_all, members = membership
    if for_all or table in members:
        if on_log is not None:
            on_log(f'{table} is already in publication "{name}"; not added again')
        return False
    _verify_table_publishable(connection, table)
    dialect = dialect_for(SourceType.POSTGRES)
    ident = _sanitize(name)
    try:
        _run_write(
            connection,
            f'ALTER PUBLICATION "{ident}" ADD TABLE ONLY {dialect.quote_table(table)}',
            action=(
                f'added {table} to publication "{ident}" (selected after the Full Load; '
                "its changes stream from now on)"
            ),
            on_log=on_log,
        )
    except Exception as exc:
        code = pg_sqlstate(exc)
        if code == "42710":  # duplicate_object: already a member
            if on_log is not None:
                on_log(f'{table} is already in publication "{name}"; not added again')
            return False
        if code == "42704":  # undefined_object: the publication was dropped meanwhile
            raise PgPublicationMissingError(
                f"publication {name} does not exist on the source"
            ) from exc
        raise
    return True


def take_xid_after_change(
    connection: object, *, on_log: Optional[Callable[[str], None]] = None
) -> str:
    """Take one transaction ID right after a publication change and return it (G17).

    ``SELECT pg_current_xact_id()::text`` (PostgreSQL 13+) in its own AUTOCOMMIT
    transaction: the ID is assigned after the change committed, so it is greater than the ID
    of every transaction that could have written before it -- what
    :func:`wait_for_transactions_before` waits out. It assigns an ID, so it goes through the
    allowlisted write path and is audited like one.
    """
    result = _run_write(
        connection,
        "SELECT pg_current_xact_id()::text",
        action=(
            "took a transaction id after the publication change, to wait for the "
            "transactions open then"
        ),
        on_log=on_log,
    )
    row = result.first()
    if row is None or row[0] is None:
        raise PgReplicationError("pg_current_xact_id() returned no transaction id")
    value = str(row[0]).strip()
    if not value.isdigit():
        raise PgReplicationError("pg_current_xact_id() returned an unexpected value")
    return value


# The wait's outcomes (:func:`wait_for_transactions_before`).
WAIT_DONE = "done"
WAIT_CANCELLED = "cancelled"
WAIT_TIMEOUT = "timeout"


def wait_for_transactions_before(
    connection: object,
    xid: str,
    *,
    timeout_seconds: float = 600.0,
    poll_seconds: float = 5.0,
    heartbeat: Optional[Callable[[], None]] = None,
    cancelled: Optional[Callable[[], bool]] = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> "tuple[str, float]":
    """Wait until every transaction with an ID below ``xid`` has ended (G17).

    Polls ``pg_snapshot_xmin(pg_current_snapshot()) >= xid`` -- the OLDEST transaction
    still running, so a long-running one keeps the wait going -- every ``poll_seconds``,
    heartbeating each time, for at most ``timeout_seconds``. (Not ``pg_snapshot_xmax``: a
    transaction still running can have an ID at or above it.) Returns ``(outcome,
    seconds)``: :data:`WAIT_DONE`, :data:`WAIT_CANCELLED` (``cancelled()`` turned True) or
    :data:`WAIT_TIMEOUT` (an idle-in-transaction session, a long batch, or an uncommitted
    prepared transaction). Reads only -- never the write path. RAISES
    :class:`PgReplicationError` when the read fails: unreadable is not "finished".
    """
    started = clock()
    statement = text("SELECT pg_snapshot_xmin(pg_current_snapshot()) >= CAST(:xid AS xid8)")
    while True:
        try:
            row = connection.execute(statement, {"xid": str(xid)}).first()  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - re-raised as the module's error
            raise PgReplicationError(
                f"could not read the source's running transactions ({type(exc).__name__})"
            ) from exc
        if heartbeat is not None:
            heartbeat()
        if row is not None and bool(row[0]):
            return WAIT_DONE, clock() - started
        if cancelled is not None and cancelled():
            return WAIT_CANCELLED, clock() - started
        if clock() - started >= timeout_seconds:
            return WAIT_TIMEOUT, clock() - started
        sleep(poll_seconds)


def create_publication(
    connection: object,
    *,
    name: str,
    tables: Sequence[str],
    on_log: Optional[Callable[[str], None]] = None,
) -> bool:
    """Create a publication for exactly ``tables``; return whether it was CREATED (vs reused).

    ``FOR TABLE ONLY <t>, ONLY <t2>, ...`` -- exactly the migrated tables: NEVER ``FOR ALL
    TABLES`` (that would make an UPDATE/DELETE on any of the source's no-PK tables ERROR on
    the publisher, a customer write outage), and never a table's inheritance children,
    which a bare ``FOR TABLE <t>`` adds too (G25; a partitioned table's partitions are
    published either way). ``tables`` are qualified ``schema.table`` names, quoted via the
    PG dialect. PostgreSQL has no ``CREATE PUBLICATION IF NOT EXISTS``, so existence is
    checked first, and a ``CREATE`` that loses a race to another run with the same CDC stack
    name (SQLSTATE 42710, or 23505 when simultaneous) reuses what that run created. On
    reuse the existing publication's table set -- rolled up to each partition root, as
    :func:`add_table_to_publication` reads it -- is RECONCILED against ``tables``:
    a requested table it does not list (e.g. a table added before a Re-run over a grown set,
    G17) is ADDED, one audited ``ADD TABLE`` each (:func:`add_table_to_publication`, which
    first refuses -- raises -- a table whose UPDATE/DELETE publishing would break) --
    never left unreplicated (pgoutput only streams a publication's members), and never by
    dropping the publication. The fresh ``CREATE`` has no identity check of its own: its
    one caller, :func:`provision_pg_replication`, grades every table it would publish
    before any write (G20), which also refuses before the first of those ADDs.
    :func:`provision_pg_replication` then creates the new slot,
    whose creation waits out the transactions open at that moment, so its consistent point
    decodes every change after it. One that COVERS every requested table and lists more (a
    Full load + CDC re-run over a set narrowed after its first load, G16) is reused as is,
    with no write: the connector's ``table.include.list`` -- not the publication -- defines
    what CDC captures, so the extra members are decoded and filtered out. A reused
    publication returns False (the compensation in :func:`provision_pg_replication` never
    drops it).

    Created ``WITH (publish_via_partition_root = true)``, which is REQUIRED for a
    partitioned source table and harmless otherwise. PostgreSQL's default is ``false``:
    a publication naming a partitioned PARENT expands to its LEAVES
    (``pg_publication_tables`` returns ``order_events_2026q3`` / ``_2026q4``, not
    ``order_events``) and publishes every change under the LEAF's relation name. This tool
    collapses partitions into the parent, so Debezium's ``table.include.list`` names the
    PARENT -- and every leaf-named change was therefore filtered out and DISCARDED. CDC
    replicated NOTHING for a partitioned table, silently: the connector ran, the slot
    advanced, and no row ever arrived. Verified on a live PostgreSQL 17 (pubviaroot = 'f',
    publication expanded to both leaves). With the option on, changes are published as the
    parent and the include-list matches.
    """
    if not tables:
        raise PgReplicationError("cannot create a publication for zero tables")
    membership = _publication_membership(connection, name)
    if membership is None:
        dialect = dialect_for(SourceType.POSTGRES)
        quoted = ", ".join(f"ONLY {dialect.quote_table(t)}" for t in tables)
        ident = _sanitize(name)  # allowlisted charset; also the literal in the DDL
        try:
            _run_write(
                connection,
                f'CREATE PUBLICATION "{ident}" FOR TABLE {quoted} '
                "WITH (publish_via_partition_root = true)",
                action=f"creating publication {ident}",
                on_log=on_log,
            )
            return True
        except Exception as exc:
            if pg_sqlstate(exc) not in ("42710", "23505"):  # not "it exists now"
                raise
            membership = _publication_membership(connection, name)
            if membership is None:
                raise
    existing = membership[1]
    requested = {str(t) for t in tables}
    for table in sorted(requested - existing):
        # A requested table the publication does not list yet: added, never skipped
        # (pgoutput would stream nothing for it) and never by dropping the publication.
        add_table_to_publication(connection, name=name, table=table, on_log=on_log)
    if on_log is not None:
        extra = sorted(existing - requested)
        on_log(
            f"publication {name} already exists and covers every requested table; "
            f"reusing it (it also lists {', '.join(extra)}, which this run does not "
            "load: CDC's table.include.list filters them out)"
            if extra
            else f"publication {name} already exists with the same tables; reusing"
        )
    return False


def create_replication_slot(
    connection: object,
    *,
    name: str,
    on_log: Optional[Callable[[str], None]] = None,
) -> str:
    """Create a ``pgoutput`` logical slot and return its consistent-point LSN.

    Raises if the slot already exists: its LSN would be an OLD consistency point that no
    longer matches THIS Full Load's snapshot, so reusing it would break the gapless
    guarantee. Callers that intend a fresh handoff drop a stale slot first (see
    :func:`provision_pg_replication`).
    """
    if slot_exists(connection, name):
        raise PgReplicationError(
            f"replication slot {name} already exists; drop it before creating a fresh "
            "one so its LSN matches this Full Load's consistency point"
        )
    result = _run_write(
        connection,
        "SELECT lsn FROM pg_create_logical_replication_slot(:name, 'pgoutput')",
        action=f"creating replication slot {name}",
        on_log=on_log,
        params={"name": name},
    )
    row = result.first()
    if row is None or row[0] is None:
        raise PgReplicationError(f"slot {name} creation returned no LSN")
    return str(row[0])


def drop_replication_slot(
    connection: object,
    *,
    name: str,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """Drop the replication slot ``name`` if it exists (idempotent).

    ``pg_drop_replication_slot`` errors on an ACTIVE slot, so the caller must ensure the
    CDC connector is already gone (teardown drops the slot only after the stack delete).
    """
    if not slot_exists(connection, name):
        if on_log is not None:
            on_log(f"replication slot {name} absent; nothing to drop")
        return
    _run_write(
        connection,
        "SELECT pg_drop_replication_slot(:name)",
        action=f"dropping replication slot {name}",
        on_log=on_log,
        params={"name": name},
    )


def drop_publication(
    connection: object,
    *,
    name: str,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """Drop the publication ``name`` if it exists (idempotent; DROP ... IF EXISTS)."""
    ident = _sanitize(name)
    _run_write(
        connection,
        f'DROP PUBLICATION IF EXISTS "{ident}"',
        action=f"dropping publication {ident}",
        on_log=on_log,
    )


def set_replica_identity_full(
    connection: object,
    tables: Sequence[str],
    *,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """``ALTER TABLE <t> REPLICA IDENTITY FULL`` for each of ``tables`` (allowlisted write).

    Required for a ``COMPOSITE_KEY``-re-keyed table so its UPDATE/DELETE before-image carries
    the prepended leading column a DELETE keyed on the composite key needs -- see
    :func:`rekeyed_tables_needing_full_identity`. Without it the leading column is absent
    from the DEFAULT before-image and the re-keyed DELETE can be silently lost. This is a source
    write, so it lives on this audited path; it is idempotent (re-applying FULL is a no-op)
    and RAISES loudly if the ALTER is refused (e.g. the source role does not own the table),
    letting the caller surface an actionable message instead of losing deletes. ``tables``
    are qualified ``schema.table`` names, quoted via the PG dialect.
    """
    if not tables:
        return
    dialect = dialect_for(SourceType.POSTGRES)
    for table in tables:
        _run_write(
            connection,
            f"ALTER TABLE {dialect.quote_table(table)} REPLICA IDENTITY FULL",
            action=f"setting REPLICA IDENTITY FULL on {table} (re-keyed CDC before-image)",
            on_log=on_log,
        )


_SLOT_STATE_SQL = (
    "SELECT NOT active AND slot_type = 'logical' "
    "       AND plugin IS NOT DISTINCT FROM 'pgoutput' "
    "       AND database IS NOT DISTINCT FROM current_database() AS reusable, "
    "       active, database IS NOT DISTINCT FROM current_database() AS same_database, "
    "       coalesce(database, '') AS database "
    "FROM pg_replication_slots WHERE slot_name = :name"
)


def _stale_slot_or_refuse(
    connection: object, slot_name: str, *, on_log: Optional[Callable[[str], None]]
) -> bool:
    """Does a same-named slot exist that provisioning may drop as a prior run's (G25)?

    Read-only, before any write. ``True`` for an inactive logical pgoutput slot in this
    database (the slot this stack's last Full Load left), ``False`` for none. Anything else
    is refused with :class:`PgReplicationError` rather than dropped: a slot something
    streams from, or one bound to another database or plugin, is not this run's -- the
    slot name derives from the CDC stack name alone, and two migrations of one source
    cluster can share it (the default name) -- and dropping it loses that pipeline's
    changes."""
    row = connection.execute(  # type: ignore[attr-defined]
        text(_SLOT_STATE_SQL), {"name": slot_name}
    ).first()
    if row is None:
        return False
    if row[0]:
        return True
    if row[1]:
        reason = "is in use (another CDC pipeline streams from it)"
        fix = "stop that pipeline, or give this migration its own CDC stack name"
    else:
        reason = (
            f"belongs to database {str(row[3])[:63]}"
            if not row[2] and row[3]
            else "is not a logical pgoutput slot"
        )
        fix = "give this migration its own CDC stack name"
    if on_log is not None:
        on_log(f"refused before any write: replication slot {slot_name} {reason}")
    raise PgReplicationError(
        f"Replication slot {slot_name} {reason}; it was not dropped. Nothing was changed "
        f"on the source: {fix}, then run the Full Load again."
    )


def _repair_reused_publication(
    connection: object,
    *,
    name: str,
    tables: Sequence[str],
    on_log: Optional[Callable[[str], None]],
) -> None:
    """Make a REUSED publication publish under the partition root BEFORE the slot (G25).

    A publication Debezium created (``publication.autocreate.mode=filtered``) or an older
    version made is at ``publish_via_partition_root = false``, and the setting is not
    retroactive: a change committed before the flip decodes under its LEAF partition's name,
    which ``table.include.list`` (the parent) discards. Flipped before the slot's consistent
    point, nothing the slot decodes is affected. If the flip fails, a partitioned requested
    table is refused with the statement to run by hand; otherwise the publication is reused
    as is (nothing partitioned is captured through it)."""
    try:
        ensure_publication_via_partition_root(
            connection, name=name, on_log=on_log, before_slot=True
        )
        return
    except Exception as exc:  # noqa: BLE001 - graded below
        cls = type(exc).__name__[:32]
        failure = exc
    try:
        partitioned = (
            connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT 1 FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind = 'p' AND n.nspname || '.' || c.relname = ANY(:names) "
                    "LIMIT 1"
                ),
                {"names": sorted({str(t) for t in tables})},
            ).first()
            is not None
        )
    except Exception:  # noqa: BLE001 - unreadable: fail closed
        partitioned = True
    if partitioned:
        # The statement leads: the line must fit the JobManager's 300 characters.
        raise PgReplicationError(
            f'Run ALTER PUBLICATION "{name}" SET (publish_via_partition_root = true) as its '
            f"owner, then run the Full Load again: setting it failed ({cls}), and without it "
            "CDC discards the changes of a partitioned table."
        ) from failure
    if on_log is not None:
        on_log(
            f"publication {name} keeps publish_via_partition_root = false (setting it "
            f"failed: {cls}); no table of this run is partitioned, so it is reused as is"
        )


def provision_pg_replication(
    connection: object,
    *,
    slot_name: str,
    publication_name: str,
    tables: Sequence[str],
    full_identity_tables: Sequence[str] = (),
    on_log: Optional[Callable[[str], None]] = None,
) -> PgReplicationHandles:
    """Create the publication + a FRESH slot at the current consistency point.

    Ordering: set REPLICA IDENTITY FULL on the re-keyed tables FIRST (so every change WAL
    from the slot's LSN onward carries the full before-image the re-keyed DELETE needs),
    then the publication (what pgoutput may decode -- the connector's table.include.list
    defines the capture within it, so a reused publication that lists more is fine, G16,
    and a reused one missing a requested table gets it added, one ``ADD TABLE`` each, G17;
    a reused one also gets ``publish_via_partition_root = true``, G25), then the slot (whose
    returned consistent-point LSN pins WAL from here). A stale slot from a prior run is
    dropped first so the new slot's LSN matches THIS Full Load's snapshot -- safe because
    slot creation runs at Full Load time, before any CDC connector of this run exists to
    hold it active. A same-named slot that is in use, or bound to another database or
    plugin, is someone else's and is refused before any write instead (G25).
    Returns the handles (incl. the resume LSN) to record on the watermark. Callers pass a
    connection from :func:`build_pg_source_write_engine` (the un-guarded AUTOCOMMIT path).

    ``full_identity_tables`` are the captured tables whose CDC re-key adds a column outside
    the source PK (:func:`rekeyed_tables_needing_full_identity`); each is set REPLICA
    IDENTITY FULL. A FULL-identity ALTER that fails RAISES (before the slot is created), so
    the handoff never silently starts without the wider before-image.

    Refuses upfront -- BEFORE any write: no ALTER TABLE, no publication, no slot -- if a
    table it would publish has no usable REPLICA IDENTITY, or whether it has one cannot be
    read (:func:`_verify_tables_publishable`, G20: the strict, fail-closed rule the one-table
    ADD applies; a re-keyed plain table about to be set FULL passes, a partitioned one is
    graded by its partitions), if any requested table is REPLICA IDENTITY NOTHING
    (:func:`_verify_tables_replicable`) -- a source-write outage guard -- and if a slot of
    that name is not this stack's stale one (:func:`_stale_slot_or_refuse`). On a reused
    publication the up-front check covers every table it would add, so a refusal never
    leaves some of them added. On AUTOCOMMIT the publication commits immediately, so if
    slot creation then fails, a publication CREATED by this call is dropped (compensating)
    rather than left orphaned on the source. (A REPLICA IDENTITY FULL set here is left in
    place on a later failure -- it is a benign, reversible widening of the before-image,
    not an orphaned object arming a write outage.)
    """
    # Source-safety guards (independent of the advisory prerequisite), all BEFORE the
    # first write: never publish a table whose UPDATE/DELETE would then ERROR on the source
    # publisher. The strict, fail-closed check grades every table this call would add to
    # the publication (G20); the lenient one keeps refusing NOTHING for every requested
    # table, members of a reused publication included. Never drop another pipeline's slot.
    _verify_tables_publishable(
        connection,
        tables,
        publication=publication_name,
        widened=full_identity_tables,
        on_log=on_log,
    )
    _verify_tables_replicable(connection, tables, on_log=on_log)
    stale_slot = _stale_slot_or_refuse(connection, slot_name, on_log=on_log)
    # Re-keyed tables need the full before-image (leading key column) BEFORE the slot's LSN.
    set_replica_identity_full(connection, full_identity_tables, on_log=on_log)
    created_publication = create_publication(
        connection, name=publication_name, tables=tables, on_log=on_log
    )
    try:
        if not created_publication:
            _repair_reused_publication(
                connection, name=publication_name, tables=tables, on_log=on_log
            )
        if stale_slot:
            if on_log is not None:
                on_log(f"dropping stale replication slot {slot_name} from a prior run")
            drop_replication_slot(connection, name=slot_name, on_log=on_log)
        lsn = create_replication_slot(connection, name=slot_name, on_log=on_log)
    except Exception:
        # AUTOCOMMIT already committed the CREATE PUBLICATION; if we created it this call,
        # drop it so a failed provision does not leave an orphaned publication arming a
        # source-write outage. A reused (pre-existing) publication is left as-is.
        if created_publication:
            try:
                drop_publication(connection, name=publication_name, on_log=on_log)
            except Exception:  # noqa: BLE001 - best-effort compensation
                # Swallowed so the ORIGINAL slot failure is what the caller sees, but
                # never silently: the publication this call created is still on the
                # customer's source arming the write outage the compensation exists to
                # prevent, and the operator has to drop it by hand.
                if on_log is not None:
                    on_log(
                        f"publication {publication_name} is STILL ON THE SOURCE -- the "
                        "compensating drop failed; drop it manually with "
                        f'DROP PUBLICATION "{publication_name}"'
                    )
        raise
    return PgReplicationHandles(
        slot_name=slot_name, publication_name=publication_name, consistent_lsn=lsn
    )


def deprovision_pg_replication(
    connection: object,
    *,
    slot_name: str,
    publication_name: str,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """Drop the slot then the publication (idempotent) -- CDC teardown.

    Slot first (frees the pinned WAL immediately), then the publication. Must run AFTER
    the CDC connector is gone (else the slot is active) and BEFORE the source secret is
    deleted (else these writes can no longer authenticate).
    """
    drop_replication_slot(connection, name=slot_name, on_log=on_log)
    drop_publication(connection, name=publication_name, on_log=on_log)


__all__ = [
    "PgReplicationError",
    "PgPublicationMissingError",
    "PgReplicaIdentityError",
    "PgReplicationHandles",
    "pg_slot_name",
    "pg_publication_name",
    "rekeyed_tables_needing_full_identity",
    "RekeyedIdentityRead",
    "read_rekeyed_replica_identity",
    "grade_rekeyed_replica_identity",
    "ReplicaIdentityUsability",
    "read_replica_identity_usability",
    "replica_identity_problem",
    "replica_identity_remedy",
    "REPLICA_IDENTITY_WEAK",
    "REPLICA_IDENTITY_FULL",
    "REPLICA_IDENTITY_UNKNOWN",
    "set_replica_identity_full",
    "build_pg_source_write_engine",
    "publication_exists",
    "slot_exists",
    "publication_tables",
    "publication_publishes_via_partition_root",
    "ensure_publication_via_partition_root",
    "PgReplicationObjects",
    "read_pg_replication_objects",
    "pg_replication_objects_blocker",
    "NO_PRIMARY_KEY_PROBLEM",
    "INDEX_GONE_PROBLEM",
    "REKEYED_FULL_REFUSED_PROBLEM",
    "REKEYED_FULL_REFUSED_REMEDY",
    "SLOT_POSITION_HEADER",
    "SLOT_MISSING_HEADER",
    "SLOT_UNUSABLE_HEADER",
    "PUBLICATION_MISSING_HEADER",
    "PUBLICATION_COVERAGE_HEADER",
    "RESNAPSHOT_OFFERED",
    "RESNAPSHOT_REFUSED",
    "pg_cdc_route",
    "PUBLISH_LIST_HEADER",
    "INHERITANCE_SNAPSHOT_HEADER",
    "INHERITANCE_UNREADABLE_HEADER",
    "BLOCKS_WITH_THEIR_OWN_FIX",
    "publish_list_fix",
    "read_inheritance_children",
    "with_inheritance_children",
    "create_publication",
    "pg_sqlstate",
    "add_table_to_publication",
    "take_xid_after_change",
    "wait_for_transactions_before",
    "WAIT_DONE",
    "WAIT_CANCELLED",
    "WAIT_TIMEOUT",
    "create_replication_slot",
    "drop_replication_slot",
    "drop_publication",
    "provision_pg_replication",
    "deprovision_pg_replication",
]
