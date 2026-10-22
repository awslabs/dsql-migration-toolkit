# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The MySQL CDC start-point fence: ``FLUSH BINARY LOGS`` before the Full Load reads (G147).

A Full load + CDC run records its CDC start point (:mod:`dsql_migrator.core.watermark`)
before any table is read, and every reader then opens its own consistent snapshot; CDC
replays the binlog from that point. A transaction the start point already covers has to
be visible to those snapshots. On a source with ``gtid_mode=ON`` it always is: a GTID
enters ``gtid_executed`` only after the transaction's InnoDB commit. A binlog
file:position is not: MySQL writes a transaction to the binlog, syncs it, and only then
commits it in InnoDB, so a position read in between already covers a transaction no
snapshot can see yet. CDC starts after it and the snapshot misses it -- the change is lost,
silently (reproduced on MySQL 8.0 and 8.4).

``FLUSH BINARY LOGS`` closes that window. Before MySQL switches to a new binlog file it
waits until every transaction written to the current one has finished its InnoDB commit
(it holds the binlog lock meanwhile, so new commits wait too -- normally for one group
commit). Run after the start point is read and before any reader opens, it guarantees that
every transaction before the start point is visible to every snapshot of the load. It
locks no table and needs only the RELOAD privilege, which the admin user RDS and Aurora
create with the database holds; it rotates the binlog to a new file.

On Aurora MySQL with enhanced binlog the tool does not rely on the flush (a position there
was seen covering a transaction its snapshot could not see yet, and the flush cannot be
shown to wait for it), so such a source needs ``gtid_mode=ON`` for a Full load + CDC run.

A transaction prepared with ``XA PREPARE`` before the start point and committed after the
readers open is lost with ANY binlog start point, GTID included: its rows sit before the
start point and no snapshot sees them. ``XA RECOVER`` lists such transactions; the fence
waits a bounded time for the ones it found to finish and otherwise refuses the start point.

Everywhere else the MySQL source is strictly read-only (Property 1): the shared guard
(:func:`~dsql_migrator.core.introspector.install_read_only_guard`) refuses ``FLUSH``.
This module is the one exception, kept narrow the way
:mod:`dsql_migrator.core.cdc_pg_slot` keeps PostgreSQL's: its own engine without the
guard, used only for a Full load + CDC run, issuing only the fixed statements in
:data:`FENCE_STATEMENTS` (asserted before each), and reporting through ``on_log`` for
the activity log.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.config import SecretValue
from dsql_migrator.core.cdc_coords import gtid_mode_is_on
from dsql_migrator.core.models import SourceConnectionConfig, SourceType

FLUSH_BINARY_LOGS = "FLUSH BINARY LOGS"
XA_RECOVER = "XA RECOVER"
ENHANCED_BINLOG_STATUS = "SHOW GLOBAL STATUS LIKE 'aurora_enhanced_binlog'"
#: The only statements the fence engine ever runs (normalized upper-case text).
FENCE_STATEMENTS = frozenset(
    " ".join(s.upper().split()) for s in (FLUSH_BINARY_LOGS, XA_RECOVER, ENHANCED_BINLOG_STATUS)
)

# MySQL "Access denied; you need (at least one of) the ... privilege(s)".
_ER_SPECIFIC_ACCESS_DENIED = 1227

GTID_REMEDY = "set gtid_mode=ON and enforce_gtid_consistency=ON on the source"


class StartPointFenceError(RuntimeError):
    """The CDC start point cannot be made exact; the run stops before any table is read."""


@dataclass(frozen=True)
class StartPointFence:
    """What :func:`fence_start_point` did.

    ``method`` is ``"gtid"`` (the GTID set is exact; no FLUSH) or ``"flush"`` (FLUSH BINARY
    LOGS ran). ``xa_checked`` is False when ``XA RECOVER`` could not run (privilege).
    """

    method: str
    flush_seconds: Optional[float] = None
    xa_checked: bool = True


def _assert_fence_statement(statement: str) -> None:
    normalized = " ".join(statement.upper().split())
    if normalized not in FENCE_STATEMENTS:
        raise StartPointFenceError(f"refused non-fence source statement: {normalized[:60]!r}")


def _run(connection: object, statement: str):
    _assert_fence_statement(statement)
    return connection.execute(text(statement))  # type: ignore[attr-defined]


def _mysql_errno(exc: BaseException) -> Optional[int]:
    """The MySQL error number of a driver error wrapped by SQLAlchemy, if any."""
    origin = getattr(exc, "orig", exc)
    args = getattr(origin, "args", ())
    return args[0] if args and isinstance(args[0], int) else None


def start_point_unfenced(watermark: object) -> bool:
    """Is ``watermark`` a MySQL binlog file:position start point that no fence made exact?

    True for a point recorded by a Full load only run or an earlier version (no
    ``start_point_fence``) while the source was not running ``gtid_mode=ON``: it can cover
    a transaction that was still committing when it was read. False for a GTID start
    point (Debezium positions by GTID set only on ``gtid_mode=ON``), a fenced one, a
    PostgreSQL watermark and none at all. A point recorded before ``gtid_mode`` was
    recorded counts as a GTID one when it carries a GTID set, as
    :func:`~dsql_migrator.ui.data_migration._full_load_engine.watermark_coordinate` reads it.
    """
    if watermark is None or getattr(watermark, "wal_lsn", None):
        return False
    if not getattr(watermark, "binlog_file", None):
        return False
    gtid_mode = getattr(watermark, "gtid_mode", None)
    if gtid_mode_is_on(gtid_mode) or (not gtid_mode and getattr(watermark, "gtid_executed", None)):
        return False
    return not getattr(watermark, "start_point_fence", None)


def enhanced_binlog_active(connection: object) -> bool:
    """Does this Aurora MySQL cluster write its binlog with enhanced binlog?

    ``False`` on community MySQL / RDS (no such status variable) and when the status
    cannot be read.
    """
    try:
        rows = _run(connection, ENHANCED_BINLOG_STATUS).fetchall()
    except Exception:  # noqa: BLE001 - not Aurora, or not readable: treat as standard binlog
        return False
    return any(str(row[1]).strip().upper() == "ACTIVE" for row in rows if len(row) > 1)


def _prepared_xids(connection: object) -> Optional[frozenset]:
    """The XIDs of the XA transactions PREPARED now (each XA RECOVER row: formatID,
    gtrid_length, bqual_length, data); ``None`` when XA RECOVER cannot run."""
    try:
        return frozenset(tuple(row) for row in _run(connection, XA_RECOVER).fetchall())
    except Exception:  # noqa: BLE001 - XA_RECOVER_ADMIN missing, or XA unsupported
        return None


def fence_start_point(
    connection: object,
    *,
    gtid_mode: Optional[str],
    on_log: Callable[..., None],
    xa_wait_seconds: float = 30.0,
    poll_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> StartPointFence:
    """Make the start point just recorded exact before any reader opens its snapshot.

    ``connection`` comes from :func:`build_fence_engine` (AUTOCOMMIT, no read-only guard).
    ``on_log(message)`` reports what was done, ``on_log(message, warning=True)`` what could
    not be checked. Raises :class:`StartPointFenceError` with the remedy when the point
    cannot be made exact: FLUSH refused (no RELOAD), Aurora enhanced binlog without
    ``gtid_mode=ON``, or an XA transaction prepared before the wait began and still prepared
    after ``xa_wait_seconds``. Its first line -- all a job error keeps (300 characters) --
    names the cause and the remedy; the lines after it explain.
    """
    method = "gtid"
    flush_seconds: Optional[float] = None
    if not gtid_mode_is_on(gtid_mode):
        if enhanced_binlog_active(connection):
            raise StartPointFenceError(
                "Aurora enhanced binlog without gtid_mode=ON: the CDC start point cannot be "
                "made exact. Set gtid_mode=ON and enforce_gtid_consistency=ON (DB cluster "
                "parameter group), or set aurora_enhanced_binlog=0, then run the Full Load "
                "again.\n"
                f"gtid_mode is {gtid_mode or 'unknown'}. With enhanced binlog a binlog "
                "position can cover a transaction before it is visible to the Full Load's "
                "snapshots, and the tool does not rely on FLUSH BINARY LOGS to wait for it "
                "there, so a change made at the start point could be lost."
            )
        started = monotonic()
        try:
            _run(connection, FLUSH_BINARY_LOGS)
        except Exception as exc:  # noqa: BLE001 - re-raised with the remedy
            if _mysql_errno(exc) == _ER_SPECIFIC_ACCESS_DENIED:
                raise StartPointFenceError(
                    "The source user cannot run FLUSH BINARY LOGS (it needs RELOAD), so the "
                    "CDC start point cannot be made exact. Grant RELOAD to the source user, "
                    f"or {GTID_REMEDY}, then run the Full Load again.\n"
                    "The source does not run gtid_mode=ON, so the Full Load runs FLUSH BINARY "
                    "LOGS right after recording the start point, before any table is read."
                ) from exc
            raise StartPointFenceError(
                f"FLUSH BINARY LOGS failed on the source, so the CDC start point cannot be "
                f"made exact: {str(getattr(exc, 'orig', exc))[:200]}"
            ) from exc
        method = "flush"
        flush_seconds = round(monotonic() - started, 3)
        on_log(
            f"FLUSH BINARY LOGS took {flush_seconds:.3f} s: every transaction before the "
            "start point is committed before the Full Load reads (the binlog moved to a new "
            "file)"
        )
    pending = _prepared_xids(connection)
    if pending is None:
        on_log(
            "XA RECOVER could not run (it needs XA_RECOVER_ADMIN), so prepared XA "
            "transactions were not checked: if the application uses XA, one prepared before "
            "the start point and committed after the load starts is not copied",
            warning=True,
        )
        return StartPointFence(method, flush_seconds, xa_checked=False)
    # Only the transactions already prepared now can sit before the start point; one
    # prepared later follows it, and CDC replays it. So the wait is for these ones only.
    deadline = monotonic() + xa_wait_seconds
    while pending and monotonic() < deadline:
        sleep(poll_seconds)
        now = _prepared_xids(connection)
        if now is None:
            raise StartPointFenceError(
                f"XA RECOVER failed while the run waited for {len(pending)} prepared XA "
                "transaction(s) to finish, so the CDC start point cannot be shown exact. Run "
                "the Full Load again."
            )
        pending = pending & now
    if pending:
        raise StartPointFenceError(
            f"{len(pending)} XA transaction(s) are still prepared on the source after "
            f"{xa_wait_seconds:.0f} s, so the CDC start point cannot be made exact. Run the "
            "Full Load again once their transaction manager has finished them.\n"
            "A transaction prepared before the CDC start point and committed after the Full "
            "Load reads is copied neither by the load nor by CDC. XA RECOVER lists the "
            "prepared transactions of every schema and application on the server; finishing "
            "one by hand (XA COMMIT / XA ROLLBACK) is its owner's decision, as it can break "
            "the atomicity of the global transaction."
        )
    return StartPointFence(method, flush_seconds, xa_checked=True)


def build_fence_engine(
    source_config: SourceConnectionConfig, password: Optional[SecretValue]
) -> Engine:
    """The fence's own MySQL engine: AUTOCOMMIT, WITHOUT the read-only guard (see module doc).

    MySQL only; the password is revealed only here (Property 7).
    """
    if source_config.source_type is not SourceType.MYSQL:
        raise StartPointFenceError(
            f"the start-point fence is MySQL-only (got {source_config.source_type.value})"
        )
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    from dsql_migrator.core.source_dialect import dialect_for

    if password is None and getattr(source_config, "secret", None) is not None:
        # Secrets Manager authentication: resolved like the read path's factory.
        from dsql_migrator.config import resolve_secret

        password = resolve_secret(source_config.secret)
    dialect = dialect_for(SourceType.MYSQL)
    url = URL.create(
        dialect.driver_scheme,
        username=source_config.username,
        password=password.reveal() if password is not None else None,
        host=source_config.host,
        port=source_config.port,
        database=source_config.database,
    )
    # NB: deliberately NO install_read_only_guard -- this is the sanctioned fence path.
    return create_engine(url, **dialect.engine_kwargs()).execution_options(
        isolation_level="AUTOCOMMIT"
    )
