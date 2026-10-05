# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ONE audited source-write path: PostgreSQL CDC replication slot + publication.

Everywhere else in this tool the migration source is strictly READ-ONLY (Property 1),
enforced by :func:`~dsql_migrator.core.introspector.install_read_only_guard` on every
source engine. PostgreSQL CDC is the sole, deliberate exception: to hand off from Full
Load to CDC gaplessly the tool must, ON THE SOURCE,

  1. create a **publication** scoped to exactly the migrated tables, and
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
from dataclasses import dataclass
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
# double-quoted schema.table (a doubled quote is the escape), matched on the normalized
# text. ONLY / * / a column list / WHERE / TABLES IN SCHEMA / a second table / a trailing
# statement all fail the anchored match.
_PUBLICATION_ADD_TABLE_RE = re.compile(
    r'^ALTER PUBLICATION "[A-Z0-9_]+" ADD TABLE "(?:[^"]|"")+"\."(?:[^"]|"")+"$'
)
# The ONLY transaction-ID statement (normalized form).
_XID_STATEMENT = "SELECT PG_CURRENT_XACT_ID()::TEXT"


class PgReplicationError(RuntimeError):
    """A replication slot / publication create or drop failed on the source."""


class PgPublicationMissingError(PgReplicationError):
    """The publication a one-table add targets does not exist on the source (G17)."""


class PgReplicaIdentityError(PgReplicationError):
    """A one-table add was refused BEFORE any write (G17): the table has no replica identity
    PostgreSQL accepts for UPDATE/DELETE once it is published (``cause``, the value-free
    phrase of :func:`replica_identity_problem`), or whether it has one could not be read
    (``unreadable``, ``cause`` None) -- fail closed, never "usable"."""

    def __init__(
        self, message: str, *, cause: Optional[str] = None, unreadable: bool = False
    ) -> None:
        super().__init__(message)
        self.cause = cause
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
    at most five of them, sorted. Value-free: names and codes only.
    """

    identity: str
    primary_key: tuple
    weak_partitions: int
    weak_partition_names: tuple


# ONE catalog round trip for every scoped table. Leaves are aggregated SERVER-side (a count
# plus at most five names), never one row per leaf, so a heavily partitioned table costs the
# same result size as a plain one. Catalog only: no table scan, no lock. pg_partition_tree is
# PostgreSQL 12+ (the tool targets 13-18); ``a.attnum = ANY(i.indkey)`` reads the int2vector
# directly. The approved live check runs this on PG 13-17 (see the G2 notes).
_REKEYED_REPLICA_IDENTITY_SQL = (
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
    "       ELSE ARRAY[]::text[] END AS weak_sample "
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
        out[str(row[0])] = RekeyedIdentityRead(
            identity=str(row[1]) if row[1] is not None else "",
            primary_key=tuple(str(v) for v in (row[2] or ()) if v is not None),
            weak_partitions=max(int(row[3] or 0), 0),
            weak_partition_names=sample,
        )
    return out


class ReplicaIdentityUsability(NamedTuple):
    """One table's live facts for "would publishing it make its UPDATE/DELETE fail?" (G17).

    ``kind`` is ``pg_class.relkind`` ('r' a plain table, 'p' a partitioned one); ``identity``
    its own ``relreplident``; ``usable`` whether PostgreSQL has a replica identity for its
    UPDATE/DELETE once published (graded server-side by :func:`_identity_usable_sql`); and,
    for a partitioned table, ``unusable_partitions`` how many LEAF partitions have none, at
    most five of them named (sorted) in ``unusable_partition_names``. Value-free: names and
    codes only.
    """

    kind: str
    identity: str
    usable: bool
    unusable_partitions: int
    unusable_partition_names: tuple


def _identity_usable_sql(alias: str) -> str:
    """Does relation ``alias`` keep a replica identity PostgreSQL accepts for UPDATE/DELETE
    once published? The relcache's own rule (``RelationGetIndexList`` /
    ``CheckCmdReplicaIdentity``): FULL; DEFAULT backed by a valid, immediate (not deferrable)
    primary key; or USING INDEX backed by a valid, unique, immediate, non-partial identity
    index. PostgreSQL does NOT reset ``relreplident`` when that key or index is dropped, so
    the code alone proves nothing. Catalog only."""
    return (
        f"({alias}.relreplident = 'f' "
        f"OR ({alias}.relreplident = 'd' AND EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        f"WHERE i.indrelid = {alias}.oid AND i.indisprimary AND i.indisvalid "
        f"AND i.indimmediate)) "
        f"OR ({alias}.relreplident = 'i' AND EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        f"WHERE i.indrelid = {alias}.oid AND i.indisreplident AND i.indisvalid "
        f"AND i.indisunique AND i.indimmediate AND i.indpred IS NULL)))"
    )


# ONE catalog round trip for every named table, like the re-keyed read above: the leaves of a
# partitioned table are graded and aggregated SERVER-side (a count plus at most five names).
# PostgreSQL skips the check for a partitioned parent itself (the UPDATE/DELETE runs on the
# leaf), so a parent is graded by its leaves -- plus its own NOTHING, which the prerequisite
# also refuses. Catalog only: no table scan, no lock.
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
    "       ELSE ARRAY[]::text[] END AS unusable_sample "
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
        out[str(row[0])] = ReplicaIdentityUsability(
            kind=str(row[1]) if row[1] is not None else "",
            identity=str(row[2]) if row[2] is not None else "",
            usable=bool(row[3]),
            unusable_partitions=max(int(row[4] or 0), 0),
            unusable_partition_names=tuple(
                str(v) for v in (row[5] or ()) if v is not None
            )[:5],
        )
    return out


def replica_identity_problem(read: ReplicaIdentityUsability) -> Optional[str]:
    """Why publishing the table would make its UPDATE/DELETE fail on the source -- a short,
    value-free phrase -- or ``None`` when it would not (G17). Pure.

    A partitioned table is graded by its leaves (PostgreSQL enforces the LEAF's identity:
    ``ALTER TABLE <parent>`` does not reach existing partitions) and by its own NOTHING.
    """
    if read.kind == "p":
        if read.identity == "n":
            return "REPLICA IDENTITY NOTHING"
        count = read.unusable_partitions
        if count <= 0:
            return None
        shown = ", ".join(read.unusable_partition_names)
        more = (
            f" and {count - len(read.unusable_partition_names)} more"
            if count > len(read.unusable_partition_names)
            else ""
        )
        return f"{count} of its partitions {'has' if count == 1 else 'have'} none: {shown}{more}"
    if read.usable:
        return None
    if read.identity == "n":
        return "REPLICA IDENTITY NOTHING"
    if read.identity == "d":
        return "REPLICA IDENTITY DEFAULT, but no valid primary key"
    if read.identity == "i":
        return "REPLICA IDENTITY USING INDEX, but that index is gone or not valid"
    return "a REPLICA IDENTITY that PostgreSQL cannot use"


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
            "(publish_via_partition_root = true), or ADD TABLE of one quoted table): "
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

    ``pg_publication_tables`` is the EFFECTIVE membership, which is what the reconcile has
    to compare against: for a partitioned table it reports the PARENT only because
    :func:`create_publication` sets ``publish_via_partition_root = true``. Without that
    option it expands to the LEAVES, so this set could never equal the requested parent
    name and the reconcile refused every re-run over a partitioned source. Do not "fix"
    that by reading ``pg_publication_rel`` (which always reports the registered parent) --
    that would hide the real defect, which is that leaf-named changes are what pgoutput
    would then stream.
    """
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
            "WHERE pubname = :name"
        ),
        {"name": name},
    ).fetchall()
    return {str(r[0]) for r in rows}


# Kept as the private name the existing provisioning call site uses.
_publication_tables = publication_tables


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
) -> str:
    """Make ``name`` publish under the partition ROOT. Returns what happened.

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
            f'repaired publication "{name}": publish_via_partition_root false -> true, so '
            "changes to a partitioned table stream under the parent the target has (in "
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


def read_pg_replication_objects(
    connection: object, *, publication_name: str, slot_name: str
) -> PgReplicationObjects:
    """Read the CDC publication + slot state from the source (read-only).

    Needs no special privilege: ``pg_publication`` / ``pg_replication_slots`` /
    ``pg_publication_tables`` are readable by an ordinary role (live-verified with
    ``rolsuper=f rolreplication=f`` and no grants, inside ``BEGIN TRANSACTION READ ONLY``),
    so this is strictly cheaper than the publication-privilege check that already runs.

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
        # repair. The raw :func:`publication_tables` reader is left alone: the
        # membership reconcile in :func:`create_publication` needs the unrolled view.
        covered = {_partition_roots(connection).get(name, name) for name in covered}
    return PgReplicationObjects(
        publication_name=publication_name,
        slot_name=slot_name,
        publication_present=pub_n > 0,
        publication_publishes_all_dml=(None if all_dml is None else bool(all_dml)),
        publication_tables=frozenset(covered),
        slot_present_any_database=int(row[2] or 0) > 0,
        slot_usable=int(row[3] or 0) > 0,
        publication_via_partition_root=via_root,
    )


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


def pg_replication_objects_blocker(
    objects: PgReplicationObjects,
    selected_tables: Sequence[str],
    *,
    resumes_from_slot: bool,
    connector_autocreates_publication: bool = False,
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
    """
    pub = objects.publication_name
    if not objects.publication_present and not connector_autocreates_publication:
        return (
            "CDC's publication does not exist on the source",
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
            "CDC's publication does not cover every selected table",
            f"Publication '{pub}' exists but does not include {len(missing)} of the "
            f"selected tables: {shown}. Those tables would replicate NOTHING while the "
            "connector reported RUNNING, which is worse than a visible failure.",
        )
    if objects.publication_present and objects.publication_publishes_all_dml is False:
        return (
            "CDC's publication does not publish every change type",
            f"Publication '{pub}' is restricted to a subset of INSERT/UPDATE/DELETE, so "
            "the change types it omits would never reach the target and the connector "
            "would still report RUNNING. Recreate it with all three, or let this run "
            "re-snapshot and create its own.",
        )
    if resumes_from_slot and not objects.slot_usable:
        return (
            "CDC's replication slot does not exist on the source",
            f"This start is configured to resume from replication slot "
            f"'{objects.slot_name}' (snapshot.mode=never), and the source has no usable "
            "logical pgoutput slot of that name in this database. A slot cannot be created "
            "at a past position, so one created now would start from the CURRENT WAL and "
            "silently skip every change committed since the Full Load.",
        )
    if objects.slot_present_any_database and not objects.slot_usable:
        return (
            "A replication slot of that name is not usable for CDC",
            f"A slot named '{objects.slot_name}' exists in this cluster but is not a "
            "logical pgoutput slot in this database, so the connector cannot use it and "
            "cannot create its own under that name. Drop it, or use a different CDC stack "
            "name so the derived slot name differs.",
        )
    return None


def _verify_tables_replicable(connection: object, tables: Sequence[str]) -> None:
    """Raise if any table lacks a REPLICA IDENTITY usable for UPDATE/DELETE replication.

    A last-line source-safety guard at the write layer (independent of the advisory
    prerequisite check, which can be bypassed): adding a REPLICA IDENTITY NOTHING table to
    a publication ARMS a source-write outage -- every UPDATE/DELETE on it then ERRORs on
    the publisher. relreplident 'n' (nothing) is refused; 'd'/'f'/'i' are usable ('d'
    relies on the primary key, which the prerequisite gate separately requires). Best-effort
    read: if relreplident is unreadable the table is not blocked here (the prereq owns that).

    Only the Full Load's own ``CREATE PUBLICATION`` (:func:`provision_pg_replication`) uses
    this lenient check, whose prerequisite run precedes the load. A one-table ADD after a Full
    Load (:func:`add_table_to_publication`, G17 -- including the ADDs a reused publication
    gets in :func:`create_publication`) cannot lean on that: the primary key the prerequisite
    reads is the Step 1 inventory's, which can predate a dropped key. It runs the strict,
    fail-closed :func:`_verify_table_publishable` instead.
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
    if nothing:
        raise PgReplicationError(
            "cannot publish tables with REPLICA IDENTITY NOTHING (UPDATE/DELETE would "
            f"fail on the source): {', '.join(nothing)}. Set a primary key or "
            "ALTER TABLE ... REPLICA IDENTITY FULL before starting CDC."
        )


def _verify_table_publishable(connection: object, table: str, *, publication: str) -> None:
    """Raise :class:`PgReplicaIdentityError` unless ``table`` keeps a replica identity
    PostgreSQL accepts for UPDATE/DELETE once published (G17) -- FAIL CLOSED.

    The last line before a one-table ADD to a publication that publishes UPDATE and DELETE:
    without a usable identity every UPDATE/DELETE on the table then fails on the source, and
    nothing removes the member afterwards. One catalog read
    (:func:`read_replica_identity_usability`, the relcache's rule: a valid primary key behind
    DEFAULT, a valid identity index behind USING INDEX, FULL, and every leaf of a partitioned
    table). An unreadable answer, or a table not found, refuses too: unreadable is not
    usable.
    """
    try:
        reads = read_replica_identity_usability(connection, [table])
    except Exception as exc:  # noqa: BLE001 - unreadable is not usable
        raise PgReplicaIdentityError(
            f'cannot add {table} to publication "{publication}": whether it has a usable '
            f"REPLICA IDENTITY could not be read ({type(exc).__name__}); it was not added",
            unreadable=True,
        ) from exc
    read = reads.get(table)
    if read is None:
        raise PgReplicaIdentityError(
            f'cannot add {table} to publication "{publication}": it was not found on the '
            "source as a table, so its REPLICA IDENTITY could not be checked; it was not "
            "added",
            unreadable=True,
        )
    problem = replica_identity_problem(read)
    if problem:
        raise PgReplicaIdentityError(
            f'cannot add {table} to publication "{publication}": it has no usable REPLICA '
            f"IDENTITY ({problem}), so every UPDATE and DELETE on it would fail on the "
            "source once published. Give it a primary key, or set REPLICA IDENTITY FULL "
            "(on each partition of a partitioned table), then retry.",
            cause=problem,
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

    ``ALTER PUBLICATION "<name>" ADD TABLE <one quoted table>`` -- the one shape the
    allowlist accepts. Idempotent: a member (read first, rolled up to the partition root),
    a ``FOR ALL TABLES`` publication, or a concurrent add that lost the race (SQLSTATE
    42710, already a member) issues no further write and returns False. A table with no
    replica identity PostgreSQL accepts for UPDATE/DELETE -- NOTHING, DEFAULT with no valid
    primary key, USING INDEX whose index is gone, or such a leaf partition -- is refused
    BEFORE any write with :class:`PgReplicaIdentityError` (:func:`_verify_table_publishable`:
    publishing it would make its UPDATE/DELETE fail on the source), and so is one whose
    identity cannot be read (fail closed). An absent publication raises
    :class:`PgPublicationMissingError`; any other failure propagates (the caller grades it by
    SQLSTATE). The audit line is written after the ADD commits (AUTOCOMMIT).

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
    _verify_table_publishable(connection, table, publication=name)
    dialect = dialect_for(SourceType.POSTGRES)
    ident = _sanitize(name)
    try:
        _run_write(
            connection,
            f'ALTER PUBLICATION "{ident}" ADD TABLE {dialect.quote_table(table)}',
            action=(
                f'added {table} to publication "{ident}" (selected after the Full Load; '
                "its changes stream from now on)"
            ),
            on_log=on_log,
        )
    except Exception as exc:
        if pg_sqlstate(exc) == "42710":  # duplicate_object: already a member
            if on_log is not None:
                on_log(f'{table} is already in publication "{name}"; not added again')
            return False
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

    ``FOR TABLE <exact migrated tables>`` -- NEVER ``FOR ALL TABLES`` (that would make an
    UPDATE/DELETE on any of the source's no-PK tables ERROR on the publisher, a customer
    write outage). ``tables`` are qualified ``schema.table`` names, quoted via the PG
    dialect. PostgreSQL has no ``CREATE PUBLICATION IF NOT EXISTS``, so existence is checked
    first. On reuse the existing publication's table set is RECONCILED against ``tables``:
    a requested table it does not list (e.g. a table added before a Re-run over a grown set,
    G17) is ADDED, one audited ``ADD TABLE`` each (:func:`add_table_to_publication`, which
    first refuses -- raises -- a table whose UPDATE/DELETE publishing would break) --
    never left unreplicated (pgoutput only streams a publication's members), and never by
    dropping the publication. :func:`provision_pg_replication` then creates the new slot,
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
    if publication_exists(connection, name):
        existing = _publication_tables(connection, name)
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
    dialect = dialect_for(SourceType.POSTGRES)
    quoted = ", ".join(dialect.quote_table(t) for t in tables)
    ident = _sanitize(name)  # allowlisted charset; also the literal in the DDL
    _run_write(
        connection,
        f'CREATE PUBLICATION "{ident}" FOR TABLE {quoted} '
        "WITH (publish_via_partition_root = true)",
        action=f"creating publication {ident}",
        on_log=on_log,
    )
    return True


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
    and a reused one missing a requested table gets it added, one ``ADD TABLE`` each, G17),
    then the slot (whose returned consistent-point LSN pins WAL from here). A stale slot
    from a prior run is dropped first so the new slot's LSN matches THIS Full Load's
    snapshot -- safe because slot creation runs at Full Load time, before any CDC connector
    exists to hold it active.
    Returns the handles (incl. the resume LSN) to record on the watermark. Callers pass a
    connection from :func:`build_pg_source_write_engine` (the un-guarded AUTOCOMMIT path).

    ``full_identity_tables`` are the captured tables whose CDC re-key adds a column outside
    the source PK (:func:`rekeyed_tables_needing_full_identity`); each is set REPLICA
    IDENTITY FULL. A FULL-identity ALTER that fails RAISES (before the slot is created), so
    the handoff never silently starts without the wider before-image.

    Refuses upfront if any table lacks a usable REPLICA IDENTITY (a source-write outage
    guard). On AUTOCOMMIT the publication commits immediately, so if slot creation then
    fails, a publication CREATED by this call is dropped (compensating) rather than left
    orphaned on the source. (A REPLICA IDENTITY FULL set here is left in place on a later
    failure -- it is a benign, reversible widening of the before-image, not an orphaned
    object arming a write outage.)
    """
    # Source-safety guard (independent of the advisory prereq): never publish a table
    # whose UPDATE/DELETE would then ERROR on the source publisher.
    _verify_tables_replicable(connection, tables)
    # Re-keyed tables need the full before-image (leading key column) BEFORE the slot's LSN.
    set_replica_identity_full(connection, full_identity_tables, on_log=on_log)
    created_publication = create_publication(
        connection, name=publication_name, tables=tables, on_log=on_log
    )
    try:
        if slot_exists(connection, slot_name):
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
