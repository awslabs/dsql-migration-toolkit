# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The :class:`SourceDialect` ABC -- the engine-agnostic source-reading contract.

Each concrete dialect lives in its own module (``mysql.py``, ``postgres.py``) so an
engine's specifics stay in one place; this base carries only the shared interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Sequence

from dsql_migrator.core.models import SourceType

if TYPE_CHECKING:
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts


@dataclass(frozen=True)
class SourceVersions:
    """Best-effort version metadata probed read-only from a source connection.

    Every field is optional: each version is probed independently and any failure
    (a missing variable/function, an engine that has no Aurora version) leaves it
    ``None`` without failing the connection test. Rendered on the overview diagram.

    - ``server_version``: raw server version string in the engine's own format
      (MySQL ``VERSION()`` e.g. ``8.0.mysql_aurora.3.04.0``; PostgreSQL ``version()``).
    - ``engine_version``: the clean base-engine version (MySQL community patch from
      ``@@innodb_version`` e.g. ``8.0.42``; PostgreSQL ``server_version`` e.g. ``16.4``).
    - ``aurora_version``: the Aurora-managed engine version (Aurora MySQL from
      ``@@aurora_version`` e.g. ``3.07.1``; Aurora PostgreSQL from ``aurora_version()``).
      ``None`` for RDS/community/self-managed sources.
    """

    server_version: Optional[str] = None
    engine_version: Optional[str] = None
    aurora_version: Optional[str] = None


def probe_scalar(connection: object, sql: str) -> Optional[str]:
    """Run a scalar query read-only, returning its first column as ``str`` or ``None``.

    Best effort: any failure (a variable/function the engine lacks, a driver error)
    returns ``None`` so an optional version probe never fails the connection test.
    Shared by every dialect's :meth:`SourceDialect.probe_versions` -- engine-neutral,
    so it lives on the base rather than in a per-engine module.
    """
    from sqlalchemy import text

    try:
        row = connection.execute(text(sql)).first()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - optional metadata; never fail the caller
        return None
    return str(row[0]) if row and row[0] is not None else None


def estimate_row_counts_query(
    connection: object,
    tables: list[str],
    *,
    current_schema_sql: str,
    select_from: str,
    schema_column: str,
    table_column: str,
    estimate_column: str,
    extra_filter: str = "",
    parse_estimate=lambda value: int(value) if value is not None else None,
) -> "dict[str, Optional[int]]":
    """Scan-free per-table row ESTIMATE, keyed exactly as ``tables`` was passed.

    The engine-neutral skeleton behind each dialect's :meth:`SourceDialect.
    estimate_row_counts`: resolve the default schema (``current_schema_sql``), split each
    requested name into ``(schema, table)`` (an unqualified name uses the default
    schema), and read the metadata row estimate in ONE query -- never a ``COUNT(*)``
    scan. A name missing from the estimate maps to ``None`` (unknown, distinct from a
    genuine 0). The engine supplies its own catalog source: ``select_from`` (FROM/JOINs),
    the ``schema_column``/``table_column``/``estimate_column`` projections, an optional
    ``extra_filter`` (e.g. PostgreSQL ``c.relkind IN ('r','p')``), and ``parse_estimate``
    (e.g. map PostgreSQL's never-analyzed ``-1`` to ``None``). Column names come from the
    dialect (never user input), so the interpolation is injection-safe; values bind.
    """
    from sqlalchemy import text

    from dsql_migrator.core.introspector import ReadOnlySourceError

    if not tables:
        return {}
    current_schema: Optional[str] = None
    try:
        current_schema = connection.execute(  # type: ignore[attr-defined]
            text(current_schema_sql)
        ).scalar()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - degrade gracefully
        current_schema = None

    wanted: dict[tuple[str, str], str] = {}
    params: dict[str, str] = {}
    clauses: list[str] = []
    for index, name in enumerate(tables):
        schema, separator, obj = name.partition(".")
        if not (separator and schema and obj):
            schema, obj = (current_schema or ""), name
        wanted[(schema, obj)] = name
        params[f"s{index}"] = schema
        params[f"t{index}"] = obj
        clauses.append(f"({schema_column} = :s{index} AND {table_column} = :t{index})")

    out: dict[str, Optional[int]] = {name: None for name in tables}
    if not clauses:
        return out
    where = " OR ".join(clauses)
    if extra_filter:
        where = f"{extra_filter} AND ({where})"
    query = text(
        f"SELECT {schema_column}, {table_column}, {estimate_column} "
        f"{select_from} WHERE {where}"
    )
    try:
        result = connection.execute(query, params)  # type: ignore[attr-defined]
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - estimates are non-critical; leave None
        return out
    for row in result:
        original = wanted.get((str(row[0]), str(row[1])))
        if original is not None:
            out[original] = parse_estimate(row[2])
    return out


class SourceDialect(ABC):
    """Source-engine-specific behavior for reading a migration source (read-only)."""

    #: The ``SourceType`` this dialect serves.
    source_type: SourceType

    #: Whether the ``database`` connection field names a single SCHEMA to reflect
    #: (MySQL: a database IS a schema) or the connection database whose non-system
    #: schemas should ALL be reflected (PostgreSQL: schemas live inside the database).
    #: Drives ``_assemble_inventory``: True -> a set database reflects that one schema
    #: (unqualified); False -> every non-system schema of the connected database is
    #: reflected, ``schema.table``-qualified (so a PG source never silently drops a
    #: non-``public`` schema).
    database_is_schema: bool = True

    @property
    @abstractmethod
    def driver_scheme(self) -> str:
        """SQLAlchemy URL scheme for this source engine (e.g. ``mysql+pymysql``)."""

    @property
    @abstractmethod
    def default_port(self) -> int:
        """Default TCP port for this source engine."""

    @property
    @abstractmethod
    def system_schemas(self) -> frozenset[str]:
        """Schemas never part of a user's migratable inventory (engine internals)."""

    @abstractmethod
    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        """``create_engine`` kwargs shared by every engine for this source."""

    @abstractmethod
    def enrich(
        self, connection: object, enrich_db: str, tables: list
    ) -> tuple[list, list, list]:
        """Enrich reflected ``tables`` and collect (triggers, routines, events).

        Engine-specific catalog reads for one schema (column defaults, index method,
        partitioning) applied to ``tables`` in place, plus the schema's stored
        triggers/routines/events returned as three lists. A dialect with no
        engine-specific enrichment returns three empty lists. Structural reflection
        (tables/columns/views) is dialect-agnostic and done by the caller.
        """

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Quote a bare identifier for this engine (e.g. MySQL backticks)."""

    @abstractmethod
    def quote_table(self, name: str) -> str:
        """Quote a possibly ``schema.table`` name, quoting each part separately.

        Cluster-wide introspection qualifies names as ``schema.table``; each part
        must be quoted independently so the engine reads it as schema + table, not
        one identifier containing a dot.
        """

    @property
    @abstractmethod
    def integer_pk_types(self) -> frozenset[str]:
        """Base type names whose LEADING PK column is range-shardable (integers)."""

    @abstractmethod
    def select_column_sql(self, column: object) -> str:
        """SELECT-list expression to read one source column (quoted, engine-specific).

        MySQL wraps a spatial column as ``ST_AsBinary(col) AS col`` (WKB bytes,
        matching what Debezium delivers) so it can migrate to ``bytea``; an ordinary
        column is just the quoted name.
        """

    @property
    @abstractmethod
    def snapshot_start_sql(self) -> str:
        """SQL that opens the read-only consistent-snapshot transaction for a stream."""

    @abstractmethod
    def value_converter(self, table: object, *, target_types: object = None) -> object:
        """Per-row value converter for reading ``table`` from this source.

        Turns a raw driver row into target-ready values (engine/driver-specific quirks
        -> canonical types); ``target_types`` optionally overrides the target type per
        column. MySQL returns the PyMySQL-aware :class:`~dsql_migrator.core.exporter.
        ValueConverter`.
        """

    @abstractmethod
    def estimate_row_counts(
        self, connection: object, tables: list[str]
    ) -> "dict[str, Optional[int]]":
        """Scan-free per-table row ESTIMATE (``None`` = unknown/missing), keyed as passed.

        Reads the engine's metadata row estimate in ONE query -- never a ``COUNT(*)``
        scan -- so a watermark/consistency baseline adds negligible load even for a
        very large source. MySQL reads ``information_schema.tables.table_rows``;
        PostgreSQL reads ``pg_class.reltuples``. Typically implemented via
        :func:`estimate_row_counts_query`.
        """

    @abstractmethod
    def probe_versions(self, connection: object) -> SourceVersions:
        """Read source version metadata read-only for the overview diagram.

        Best effort: each version is probed independently (via :func:`probe_scalar`)
        and any failure yields ``None`` -- it must never fail the connection test.
        MySQL reads ``VERSION()`` / ``@@innodb_version`` / ``@@aurora_version``;
        PostgreSQL reads ``version()`` / ``server_version`` / ``aurora_version()``.
        """

    @abstractmethod
    def probe_grants(self, connection: object) -> list[str]:
        """Read the source user's privilege grants read-only, as raw text lines.

        Feeds the "Source user has the required privileges" prerequisite, which joins +
        upper-cases the returned blob and looks for the tokens a mode needs (Full Load:
        ``SELECT``; ``ALL PRIVILEGES`` satisfies any). Best effort: any error yields
        ``[]`` (the check then reports a FAIL with remediation rather than crashing).
        The grant surface is engine-specific -- MySQL reads ``SHOW GRANTS``; PostgreSQL
        (which has no ``SHOW GRANTS``) reports ``ALL PRIVILEGES`` for a superuser, else
        the distinct table-privilege types granted to the current role -- so it MUST be
        gathered per dialect, not with one MySQL statement run against every engine.
        """

    @property
    @abstractmethod
    def engine_display_name(self) -> str:
        """Human-facing engine name for operator messages (``MySQL`` / ``PostgreSQL``).

        Used to word the source-failure hints for the right engine so a PostgreSQL
        migration never shows "the source MySQL connection dropped".
        """

    @abstractmethod
    def is_transient_error(self, exc: BaseException) -> bool:
        """True for a SOURCE failure a fresh connection can recover from (retry it).

        The Full Load's source read is where this matters: an Aurora failover / any
        connection drop / stall kills the in-flight read of a large table, but the table
        is NOT broken -- re-reading from a new connection succeeds -- so it is worth an
        automatic retry, whereas a genuine data/schema error would fail identically
        forever and must surface at once. The recoverable shapes are engine-specific
        (MySQL numeric driver codes; PostgreSQL SQLSTATE classes on ``.sqlstate``), so
        each dialect classifies its own -- a MySQL-only classifier silently never fires
        for psycopg (which carries a string SQLSTATE, not an int code), leaving a PG
        failover un-retried. Anything unrecognized is NON-transient (never retried).
        """

    @abstractmethod
    def is_too_many_connections(self, exc: BaseException) -> bool:
        """True when the SOURCE refused a connection for lack of free slots.

        A subset of the transient errors that needs different advice (reduce reader
        concurrency / raise the source limit, not just "wait and re-run"). MySQL uses
        error codes 1040/1203; PostgreSQL uses SQLSTATE ``53300``.
        """

    @abstractmethod
    def capture_resume_lsn(self, connection: object) -> Optional[str]:
        """Read the engine's CDC resume coordinate at the Full Load consistency point.

        The gapless Full Load -> CDC handoff point (CDC resumes streaming from here).
        PostgreSQL returns the current WAL LSN (``pg_current_wal_lsn`` on a primary /
        ``pg_last_wal_replay_lsn`` on a standby); it is captured BEFORE the per-table
        reader snapshots open (which are at or after it), so replaying from it is a
        superset and the idempotent load converges with no gap -- the PG analog of
        capturing MySQL's binlog:pos. MySQL returns ``None`` here: its binlog/GTID
        coordinate is captured by the dedicated ``WatermarkCapturer``, not this seam.
        Best effort: any failure (insufficient privilege, unreadable) returns ``None``.
        """

    @abstractmethod
    def read_active_query_count(self, connection: object) -> Optional[int]:
        """Read the source's live active-query concurrency, or ``None`` on any failure.

        Feeds the opt-in :class:`~dsql_migrator.core.exporter.SourceLoadGovernor`, which
        pauses Full Load reads while this exceeds a ceiling (protecting a live-serving
        source). The metric is engine-specific: MySQL reads global ``Threads_running``
        (``SHOW GLOBAL STATUS``), PostgreSQL counts ``active`` backends in
        ``pg_stat_activity``. It MUST be a valid statement for the engine AND succeed
        inside the export's open snapshot transaction -- a MySQL ``SHOW`` run on a
        PostgreSQL source is a syntax error that would ABORT that transaction and fail
        every subsequent page read. Best effort: any failure returns ``None`` so the
        governor fails open (treats it as "don't throttle") and never stalls the load.
        """

    @abstractmethod
    def probe_cdc_prerequisites(
        self, connection: object, table_names: Sequence[str]
    ) -> "Optional[PostgresCdcFacts]":
        """Read the CDC logical-replication readiness facts (PostgreSQL only).

        Gathers -- read-only, best-effort -- the facts the PostgreSQL CDC prerequisite
        checks need (``wal_level``, whether the user can create a slot, slot/wal-sender
        headroom, whether the source is a writer, and each captured table's REPLICA
        IDENTITY) into a :class:`~dsql_migrator.core.prerequisites_postgres.PostgresCdcFacts`.
        MySQL returns ``None``: its CDC readiness is the binlog/GTID checks, not this
        seam. Every field is best-effort so an under-privileged source degrades to
        "unknown" (a non-blocking INFO) rather than erroring the gate. Read-only, so it
        passes the source read-only guard (only ``SHOW`` / ``SELECT`` on system views).
        """


__all__ = [
    "SourceDialect",
    "SourceVersions",
    "probe_scalar",
    "estimate_row_counts_query",
]
