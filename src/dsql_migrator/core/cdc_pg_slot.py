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

and DROP both on teardown. These are the only writes the tool ever issues to a source,
so they are confined to this module, which:

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
secret is deleted (a slot left behind pins WAL and can fill the source disk). Both are
subsequent Phase-C wiring; this module is the primitive they call.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

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
# perform these four intended operations (create/drop slot, create/drop publication) plus
# the read-only existence checks.
_ALLOWED_WRITE_PREFIXES: tuple[str, ...] = (
    "CREATE PUBLICATION ",
    "DROP PUBLICATION ",
    "SELECT LSN FROM PG_CREATE_LOGICAL_REPLICATION_SLOT",
    "SELECT PG_DROP_REPLICATION_SLOT",
)


class PgReplicationError(RuntimeError):
    """A replication slot / publication create or drop failed on the source."""


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


def build_pg_source_write_engine(
    source_config: SourceConnectionConfig, password: Optional[SecretValue]
) -> Engine:
    """Build the dedicated, NON-read-only-guarded, AUTOCOMMIT PostgreSQL source engine.

    This is the ONLY source engine in the tool without :func:`install_read_only_guard`;
    it exists solely so this module can create/drop the replication slot + publication.
    AUTOCOMMIT is required because ``pg_create_logical_replication_slot`` must not run
    inside a wrapping transaction. Refuses a non-PostgreSQL source (MySQL never writes
    the source). The password is revealed only here (Property 7), mirroring
    ``make_source_engine_factory`` but WITHOUT the guard.
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
    return create_engine(url, **dialect.engine_kwargs()).execution_options(
        isolation_level="AUTOCOMMIT"
    )


def _assert_allowed(statement: str) -> None:
    """Raise unless ``statement`` is one of the fixed allowlisted source writes."""
    normalized = " ".join(statement.strip().upper().split())
    if not any(normalized.startswith(p) for p in _ALLOWED_WRITE_PREFIXES):
        raise PgReplicationError(
            f"refused non-allowlisted source write: {normalized[:60]!r}"
        )


def _run_write(
    connection: object,
    statement: str,
    *,
    action: str,
    on_log: Optional[Callable[[str], None]],
    params: Optional[dict] = None,
):
    """Execute one allowlisted source write, logging it for audit. Returns the result."""
    _assert_allowed(statement)
    if on_log is not None:
        on_log(action)
    return connection.execute(text(statement), params or {})  # type: ignore[attr-defined]


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


def _publication_tables(connection: object, name: str) -> set:
    """Return the set of qualified ``schema.table`` names a publication covers."""
    rows = connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
            "WHERE pubname = :name"
        ),
        {"name": name},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _verify_tables_replicable(connection: object, tables: Sequence[str]) -> None:
    """Raise if any table lacks a REPLICA IDENTITY usable for UPDATE/DELETE replication.

    A last-line source-safety guard at the write layer (independent of the advisory
    prerequisite check, which can be bypassed): adding a REPLICA IDENTITY NOTHING table to
    a publication ARMS a source-write outage -- every UPDATE/DELETE on it then ERRORs on
    the publisher. relreplident 'n' (nothing) is refused; 'd'/'f'/'i' are usable ('d'
    relies on the primary key, which the prerequisite gate separately requires). Best-effort
    read: if relreplident is unreadable the table is not blocked here (the prereq owns that).
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
    a mismatch (e.g. a table added on a re-run) is refused loudly rather than silently
    leaving the new table unreplicated -- pgoutput only streams a publication's members.
    """
    if not tables:
        raise PgReplicationError("cannot create a publication for zero tables")
    if publication_exists(connection, name):
        existing = _publication_tables(connection, name)
        requested = {str(t) for t in tables}
        if existing != requested:
            raise PgReplicationError(
                f"publication {name} already exists but covers {sorted(existing)}, not "
                f"the requested {sorted(requested)}. pgoutput only streams a publication's "
                "member tables, so reusing it would silently skip the difference. Drop it "
                f'(DROP PUBLICATION IF EXISTS "{_sanitize(name)}") and re-run, or align the '
                "table selection."
            )
        if on_log is not None:
            on_log(f"publication {name} already exists with the same tables; reusing")
        return False
    dialect = dialect_for(SourceType.POSTGRES)
    quoted = ", ".join(dialect.quote_table(t) for t in tables)
    ident = _sanitize(name)  # allowlisted charset; also the literal in the DDL
    _run_write(
        connection,
        f'CREATE PUBLICATION "{ident}" FOR TABLE {quoted}',
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


def provision_pg_replication(
    connection: object,
    *,
    slot_name: str,
    publication_name: str,
    tables: Sequence[str],
    on_log: Optional[Callable[[str], None]] = None,
) -> PgReplicationHandles:
    """Create the publication + a FRESH slot at the current consistency point.

    Ordering: publication FIRST (defines the capture set), then the slot (whose returned
    consistent-point LSN pins WAL from here). A stale slot from a prior run is dropped
    first so the new slot's LSN matches THIS Full Load's snapshot -- safe because slot
    creation runs at Full Load time, before any CDC connector exists to hold it active.
    Returns the handles (incl. the resume LSN) to record on the watermark. Callers pass a
    connection from :func:`build_pg_source_write_engine` (the un-guarded AUTOCOMMIT path).

    Refuses upfront if any table lacks a usable REPLICA IDENTITY (a source-write outage
    guard). On AUTOCOMMIT the publication commits immediately, so if slot creation then
    fails, a publication CREATED by this call is dropped (compensating) rather than left
    orphaned on the source.
    """
    # Source-safety guard (independent of the advisory prereq): never publish a table
    # whose UPDATE/DELETE would then ERROR on the source publisher.
    _verify_tables_replicable(connection, tables)
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
                pass
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
    "PgReplicationHandles",
    "pg_slot_name",
    "pg_publication_name",
    "build_pg_source_write_engine",
    "publication_exists",
    "slot_exists",
    "create_publication",
    "create_replication_slot",
    "drop_replication_slot",
    "drop_publication",
    "provision_pg_replication",
    "deprovision_pg_replication",
]
