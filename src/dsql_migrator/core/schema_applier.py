# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema Applier (DDL executor) for the target Aurora DSQL cluster.

The :class:`SchemaApplier` applies one piece of converted DDL to the target and
reports a per-object result, mirroring the AWS Schema Conversion Tool's
"select an object, preview source vs. target DDL, apply" flow (Requirement 10,
"Schema Apply Design"). It is the write counterpart of the read-only
:class:`~dsql_migrator.core.target_introspector.TargetIntrospector`.

What it guarantees (Property 12 -- schema apply safety):

- **Pre-apply existence/conflict check (Requirement 10.3).** Before touching the
  target, the object identity is derived from the target DDL and checked against
  the :class:`TargetIntrospector` (reused, not re-implemented). The result drives
  the conflict decision below.
- **Conflict handling via :class:`~dsql_migrator.core.models.ApplyMode`
  (Requirements 10.4, 10.5).** ``SKIP_IF_EXISTS`` leaves an existing object
  untouched and reports ``SKIPPED``; ``REPLACE`` drops and recreates it.
- **Destructive confirmation (Requirement 10.6).** A ``REPLACE`` of an object
  that already exists is destructive and is refused with a clear ``FAILED``
  result unless the caller passes ``confirmed=True``.
- **Single-DDL transactions, DDL/DML separation (Property 2 / Requirement 3.6).**
  Each DDL statement (the ``DROP`` for a replace and the ``CREATE``) is executed
  on its own as a single statement over an autocommit connection -- never
  batched with another statement.
- **OC001 idempotent retry (Requirement 10.5 / Property 5).** Each DDL execution
  is wrapped in :func:`~dsql_migrator.core.occ.with_occ_retry`, so a
  ``SQLSTATE 40001`` schema conflict (OC001) is retried with backoff. A ``40001``
  means the statement did not commit, so re-running the same DDL is safe.
- **Read-only source (Property 1 / Requirement 10.8).** The applier has no source
  handle whatsoever: it only ever writes to the injected target connection and
  reads existence from the target introspector. There is no code path to the
  source database.

The target connection is obtained through an injectable ``connection_factory``
(default: a :class:`~dsql_migrator.core.target_connection.DsqlConnector`-backed
factory), so unit tests drive it with a fake connection and can simulate OC001
conflicts without any live DSQL cluster or AWS call.
"""

from __future__ import annotations

import random
import re
import time
from typing import Any, Callable, Optional, Protocol, Sequence

from psycopg import sql

from dsql_migrator.core.models import (
    ApplyMode,
    ApplyResult,
    ApplyStatus,
    DdlPreview,
    TargetConnectionConfig,
)
from dsql_migrator.core.occ import (
    DEFAULT_BASE_DELAY_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    JitterFunc,
    SleepFunc,
    is_occ_conflict,
    with_occ_retry,
)
from dsql_migrator.core.target_connection import is_transient_connection_error


def _is_retryable_connect_error(exc: BaseException) -> bool:
    """Retryable while OPENING a DSQL connection for DDL: an OCC ``40001`` or a
    transient connection failure (dropped socket / TLS teardown / connect timeout /
    the new-connection rate limit rejecting a connect under a burst). Lets a
    per-table DROP+recreate connect ride out a connection storm instead of failing
    the table -- the same resilience the batched loader's pool leases already have.
    """
    return is_occ_conflict(exc) or is_transient_connection_error(exc)


def _open_connection_with_retry(
    connection_factory: "ConnectionFactory",
    *,
    occ_max_attempts: int,
    occ_base_delay: float,
    sleep: "SleepFunc",
    jitter: "JitterFunc",
) -> Any:
    """Open a fresh DSQL connection, retrying a transient connect failure.

    Every DDL path here opens a brand-new connection. Opening it must tolerate a
    connection storm (many workers connecting at once when a wave of tables
    finishes, tripping DSQL's new-connection rate limit) exactly as the batched
    loader's pool does -- otherwise the connect fails the whole operation before a
    single statement runs. Backoff/jitter/attempts are the caller's OCC budget.
    """
    return with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
        retryable=_is_retryable_connect_error,
    )(connection_factory)()

# A connection factory opens one new DSQL connection (autocommit + TLS + IAM).
# Injectable so unit tests never reach a real cluster.
ConnectionFactory = Callable[[], Any]

# DDL object kinds the applier recognizes when deriving an object's identity and
# building its DROP statement for a destructive REPLACE.
_TABLE = "TABLE"
_VIEW = "VIEW"
_MATERIALIZED_VIEW = "MATERIALIZED VIEW"
_INDEX = "INDEX"
_SCHEMA = "SCHEMA"

# PostgreSQL/DSQL SQLSTATE raised when a relation already exists. Tables, views,
# and indexes all live in the relation namespace, so a duplicate index surfaces
# this code too. Used to make a SKIP_IF_EXISTS apply idempotently self-heal when
# the pre-apply introspection snapshot was stale (Property 5 spirit).
_DUPLICATE_OBJECT_SQLSTATE = "42P07"

# Parses the leading ``CREATE <kind> [modifiers] <identifier>`` of a converted
# DDL statement to recover the kind and the created object's name. Tolerant of
# the DSQL-specific ``ASYNC`` index modifier and the usual ``IF NOT EXISTS`` /
# ``OR REPLACE`` / ``UNIQUE`` qualifiers. ``CREATE SCHEMA`` is also recognized so
# a schema-qualified table can ensure its schema exists first. Identifiers may be
# schema-qualified and may be double-quoted (each part captured verbatim).
_CREATE_PATTERN = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:UNIQUE\s+)?"
    r"(?P<kind>MATERIALIZED\s+VIEW|TABLE|VIEW|INDEX|SCHEMA)\s+"
    r"(?:ASYNC\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>(?:\"[^\"]+\"|[A-Za-z_][\w$]*)"
    r"(?:\.(?:\"[^\"]+\"|[A-Za-z_][\w$]*))*)",
    re.IGNORECASE,
)


class SchemaApplyError(ValueError):
    """Raised when a target DDL statement cannot be parsed into an object.

    Only ``CREATE TABLE/VIEW/MATERIALIZED VIEW/INDEX`` statements are supported;
    anything else (a malformed or non-CREATE statement) is rejected up front
    rather than being sent to the target.
    """


class _ExistenceOracle(Protocol):
    """The pre-apply existence check contract (a browsed TargetIntrospector)."""

    def object_exists(self, name: str) -> bool: ...


class _ParsedObject:
    """The identity of the object a CREATE statement defines."""

    __slots__ = ("name", "kind", "identifier")

    def __init__(self, name: str, kind: str, identifier: sql.Identifier) -> None:
        self.name = name
        self.kind = kind
        self.identifier = identifier


def parse_create_object(target_ddl: str) -> tuple[str, str]:
    """Return the ``(object_name, kind)`` defined by a ``CREATE`` statement.

    ``object_name`` is the (optionally schema-qualified) name with any double
    quotes stripped, suitable for an existence lookup against the target
    introspector. ``kind`` is one of ``TABLE``/``VIEW``/``MATERIALIZED VIEW``/
    ``INDEX``. Raises :class:`SchemaApplyError` for anything that is not a
    recognized ``CREATE`` statement, so an unparseable DDL never reaches the
    target.
    """
    parsed = _parse(target_ddl)
    return parsed.name, parsed.kind


def _parse(target_ddl: str) -> _ParsedObject:
    """Parse the created object's identity from a ``CREATE`` DDL statement."""
    match = _CREATE_PATTERN.match(target_ddl)
    if match is None:
        raise SchemaApplyError(
            "target DDL must be a CREATE TABLE/VIEW/MATERIALIZED VIEW/INDEX statement"
        )
    kind = re.sub(r"\s+", " ", match.group("kind").strip().upper())
    raw_parts = _split_identifier(match.group("name"))
    object_name = ".".join(raw_parts)
    return _ParsedObject(
        name=object_name, kind=kind, identifier=sql.Identifier(*raw_parts)
    )


def _split_identifier(raw: str) -> list[str]:
    """Split a possibly schema-qualified identifier, unquoting each part.

    A double-quoted part is taken verbatim (minus the quotes); an unquoted part
    is returned as written. The parts are recomposed safely with
    :class:`psycopg.sql.Identifier` when building a DROP statement, so the name
    can never break out into SQL (Requirement 9.4).
    """
    parts: list[str] = []
    for part in re.findall(r'"[^"]+"|[^.]+', raw):
        token = part.strip()
        if token.startswith('"') and token.endswith('"'):
            parts.append(token[1:-1])
        else:
            parts.append(token)
    return parts


class SchemaApplier:
    """Applies converted DDL to the target DSQL cluster and reports the result.

    The pre-apply existence check is delegated to a browsed
    :class:`~dsql_migrator.core.target_introspector.TargetIntrospector` (passed
    as ``introspector``); the target connection is obtained from an injectable
    ``connection_factory``. Both seams let unit tests run without a live cluster
    and simulate OC001 conflicts. The OCC retry budget and the sleep/jitter
    sources are injectable so tests are deterministic and never sleep for real.
    """

    def __init__(
        self,
        introspector: _ExistenceOracle,
        *,
        target: Optional[TargetConnectionConfig] = None,
        connection_factory: Optional[ConnectionFactory] = None,
        occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
        sleep: SleepFunc = time.sleep,
        jitter: JitterFunc = random.random,
    ) -> None:
        """Create an applier.

        ``introspector`` answers the pre-apply existence check (Requirement 10.3)
        and must already have browsed the target. ``connection_factory`` opens one
        new autocommit/TLS DSQL connection per call; when omitted, ``target`` is
        required and a
        :class:`~dsql_migrator.core.target_connection.DsqlConnector`-backed factory
        is built (IAM tokens stay confidential, Property 7). ``sleep`` and
        ``jitter`` are forwarded to
        :func:`~dsql_migrator.core.occ.with_occ_retry`.
        """
        if connection_factory is None and target is None:
            raise SchemaApplyError(
                "either connection_factory or target must be provided"
            )
        self._introspector = introspector
        self._connection_factory = connection_factory or _default_connection_factory(
            target  # type: ignore[arg-type]  # guarded above
        )
        self._occ_max_attempts = occ_max_attempts
        self._occ_base_delay = occ_base_delay
        self._sleep = sleep
        self._jitter = jitter

    def preview(self, source_ddl: str, target_ddl: str) -> DdlPreview:
        """Pair source vs. target DDL and report target existence (Req 10.2/10.3).

        Derives the object identity from ``target_ddl`` and consults the
        introspector for whether the object already exists on the target. This is
        read-only on the target (the introspector issues no SQL once browsed) and
        never touches the source.
        """
        parsed = _parse(target_ddl)
        return DdlPreview(
            object_name=parsed.name,
            source_ddl=source_ddl,
            target_ddl=target_ddl,
            exists=self._introspector.object_exists(parsed.name),
        )

    def apply(
        self,
        target_ddl: str,
        on_conflict: ApplyMode,
        *,
        confirmed: bool = False,
    ) -> ApplyResult:
        """Apply ``target_ddl`` to the target and return a per-object result.

        Flow (Property 12): derive the object identity, check existence via the
        introspector, then:

        - object absent -> execute the ``CREATE`` and report ``CREATED``;
        - object present + :attr:`ApplyMode.SKIP_IF_EXISTS` -> report ``SKIPPED``
          without touching the target;
        - object present + :attr:`ApplyMode.REPLACE` without ``confirmed`` ->
          refuse with ``FAILED`` (destructive confirmation required, Req 10.6);
        - object present + :attr:`ApplyMode.REPLACE` with ``confirmed=True`` ->
          ``DROP`` then ``CREATE`` as two separate single-DDL transactions and
          report ``CREATED``.

        Each DDL runs as a single statement with OC001 idempotent retry. A target
        error is captured into a ``FAILED`` result (with the reason) rather than
        raised, so the caller can aggregate per-object outcomes. The source is
        never accessed (Property 1).
        """
        parsed = _parse(target_ddl)

        # CREATE SCHEMA IF NOT EXISTS is idempotent, and schemas are not tracked
        # by the table/view existence introspector, so ensure it directly with no
        # existence check or destructive drop. This lets a schema-qualified table
        # create its schema first (Requirement 3 / qualified names).
        if parsed.kind == _SCHEMA:
            try:
                self._execute(parsed, target_ddl, drop_first=False)
            except Exception as exc:  # noqa: BLE001 - surfaced as a FAILED result
                # CREATE SCHEMA IF NOT EXISTS is already idempotent, but self-heal
                # a duplicate-object race (SQLSTATE 42P07) the same way the
                # table/view/index path does below, so a concurrent create still
                # converges to CREATED instead of a spurious FAILED.
                if _is_duplicate_object(exc):
                    return ApplyResult(
                        object_name=parsed.name,
                        status=ApplyStatus.CREATED,
                        detail="Schema already present on the target.",
                    )
                return ApplyResult(
                    object_name=parsed.name,
                    status=ApplyStatus.FAILED,
                    detail=f"Apply failed: {exc}",
                )
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.CREATED,
                detail="Schema ensured on the target.",
            )

        exists = self._introspector.object_exists(parsed.name)

        if exists and on_conflict is ApplyMode.SKIP_IF_EXISTS:
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.SKIPPED,
                detail="Object already exists on the target; skipped.",
            )

        if exists and on_conflict is ApplyMode.REPLACE and not confirmed:
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.FAILED,
                detail=(
                    "Destructive REPLACE requires explicit confirmation "
                    "(pass confirmed=True) before dropping the existing object."
                ),
            )

        replace_existing = exists and on_conflict is ApplyMode.REPLACE
        try:
            self._execute(parsed, target_ddl, drop_first=replace_existing)
        except Exception as exc:  # noqa: BLE001 - surfaced as a FAILED result
            # Idempotent self-heal under SKIP_IF_EXISTS (Property 5 spirit): a
            # "relation already exists" (SQLSTATE 42P07) means the object is
            # already on the target even though the (possibly stale) pre-apply
            # introspection snapshot reported it absent -- e.g. an index created
            # by an earlier partial apply. Report SKIPPED so a re-apply converges
            # instead of failing. REPLACE is not self-healed here: it did not drop
            # first (the object looked absent), so the conflict is surfaced.
            if on_conflict is ApplyMode.SKIP_IF_EXISTS and _is_duplicate_object(exc):
                return ApplyResult(
                    object_name=parsed.name,
                    status=ApplyStatus.SKIPPED,
                    detail="Object already exists on the target; skipped.",
                )
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.FAILED,
                detail=f"Apply failed: {exc}",
            )

        detail = (
            "Replaced the existing object on the target."
            if replace_existing
            else "Created on the target."
        )
        return ApplyResult(
            object_name=parsed.name, status=ApplyStatus.CREATED, detail=detail
        )

    def drop(self, target_ddl: str) -> None:
        """Drop the object ``target_ddl`` defines (``DROP <kind> IF EXISTS``).

        A pre-pass helper for a destructive REPLACE: before a dependency target
        (e.g. a table) is dropped and recreated, the dependent objects that block
        its ``DROP`` (e.g. a view selecting from it that an earlier apply created)
        are dropped first, so the table ``DROP`` no longer fails with "other
        objects depend on it". The kind/name are parsed from the CREATE DDL, the
        statement is ``DROP <kind> IF EXISTS <name>`` (idempotent -- a no-op when
        the object is absent), runs as a single autocommit DDL with OC001 retry,
        and never touches the source (Property 1/2/5). The dropped object is
        recreated later by its own apply unit, so this only reorders the drop.
        """
        parsed = _parse(target_ddl)
        connection = _open_connection_with_retry(
            self._connection_factory,
            occ_max_attempts=self._occ_max_attempts,
            occ_base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )
        try:
            self._run_ddl(connection, _build_drop_statement(parsed))
        finally:
            _safe_close(connection)

    def _execute(
        self, parsed: _ParsedObject, target_ddl: str, *, drop_first: bool
    ) -> None:
        """Run the DROP (when replacing) and the CREATE as single-DDL txns.

        A single connection is opened for the call; because it is autocommit, each
        ``execute`` is its own transaction, so the ``DROP`` and the ``CREATE`` are
        never combined into one transaction (Property 2 / DDL separation). Each
        statement is wrapped in OCC retry for OC001 idempotency (Property 5).
        """
        connection = _open_connection_with_retry(
            self._connection_factory,
            occ_max_attempts=self._occ_max_attempts,
            occ_base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )
        try:
            if drop_first:
                self._run_ddl(connection, _build_drop_statement(parsed))
            self._run_ddl(connection, target_ddl)
        finally:
            _safe_close(connection)

    def _run_ddl(self, connection: Any, statement: object) -> None:
        """Execute one DDL statement with OC001 idempotent retry (Property 5)."""
        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )(_execute_single_ddl)
        retried(connection, statement)


def _build_drop_statement(parsed: _ParsedObject) -> sql.Composed:
    """Build a safe ``DROP <kind> IF EXISTS <name>`` for a destructive replace.

    The object name is composed with :class:`psycopg.sql.Identifier` (never
    interpolated), and the kind keyword comes from the fixed parsed set, so the
    statement is injection-safe (Requirement 9.4). ``IF EXISTS`` keeps the drop
    idempotent under OCC retry.
    """
    return sql.SQL("DROP {kind} IF EXISTS {name}").format(
        kind=sql.SQL(parsed.kind), name=parsed.identifier
    )


def _execute_single_ddl(connection: Any, statement: object) -> None:
    """Execute exactly one DDL statement on ``connection`` (one transaction)."""
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
    finally:
        _safe_close(cursor)


def drop_object(
    target_ddl: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> None:
    """Drop the object ``target_ddl`` defines (``DROP <kind> IF EXISTS <name>``).

    Standalone, introspector-free counterpart to :meth:`SchemaApplier.drop`, for
    callers that only need a pre-drop (e.g. the Full Load "drop & reload" path
    dropping a dependent view before recreating a table it depends on). The
    kind/name are parsed from the CREATE DDL and composed injection-safely;
    ``IF EXISTS`` keeps it idempotent under OC001 retry. Runs as a single
    autocommit DDL on a fresh connection, which is closed afterward.
    """
    parsed = _parse(target_ddl)
    retried = with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )(_execute_single_ddl)
    connection = _open_connection_with_retry(
        connection_factory,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )
    try:
        retried(connection, _build_drop_statement(parsed))
    finally:
        _safe_close(connection)


def recreate_table(
    schema_ddls: Sequence[str],
    target_ddl: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> None:
    """Drop ``target_ddl``'s table (if present) and recreate it empty.

    The integrity-first "replace existing data" path for Full Load: Aurora DSQL
    has no ``TRUNCATE`` and a large ``DELETE`` would exceed the per-transaction
    row limit, so a fresh load over a table that already holds data is done by
    ``DROP TABLE IF EXISTS`` + ``CREATE`` -- a metadata-only operation independent
    of row count (the AWS-recommended TRUNCATE replacement). The table's
    schema(s) are ensured first (idempotent ``CREATE SCHEMA IF NOT EXISTS``), then
    the table is dropped and recreated. Each statement runs as its own autocommit
    transaction (never combined) with OC001 idempotent retry (Property 2/5).

    Secondary indexes are NOT created here: the caller (re)creates them *after*
    the data load via the importer's post-load ``CREATE INDEX ASYNC``, so the
    bulk load is not slowed by maintaining indexes. ``connection_factory`` opens a
    fresh autocommit/TLS/IAM connection (DSQL requires a fresh connection after a
    schema/table change), which is closed before the caller loads data on its own
    connections.
    """
    parsed = _parse(target_ddl)
    retried = with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )(_execute_single_ddl)
    # Opening the connection is itself retried on a transient connection failure:
    # when many table workers finish together and the next wave all open fresh DSQL
    # connections at once, DSQL's new-connection rate limit can reject/time-out a
    # connect. Without this, that per-table DROP+recreate connect failed the table
    # outright (0 rows, no batch ever ran); with it, the connect rides out the storm
    # -- matching the batched loader's pool, which already leases inside a retry.
    connection = _open_connection_with_retry(
        connection_factory,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )
    try:
        for schema_ddl in schema_ddls:
            retried(connection, schema_ddl)
        retried(connection, _build_drop_statement(parsed))
        retried(connection, target_ddl)
    finally:
        _safe_close(connection)


def _is_duplicate_object(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is a "relation already exists" error.

    PostgreSQL/DSQL raise ``SQLSTATE 42P07`` when a relation -- a table, view, or
    index, which all share the relation namespace -- already exists. Detection
    uses the exception's ``sqlstate`` attribute (not its type), so it matches both
    real ``psycopg`` errors and test fakes, mirroring
    :func:`~dsql_migrator.core.occ.is_occ_conflict`.
    """
    return getattr(exc, "sqlstate", None) == _DUPLICATE_OBJECT_SQLSTATE


def _default_connection_factory(target: TargetConnectionConfig) -> ConnectionFactory:
    """Build a connection factory backed by :class:`DsqlConnector`.

    Each call opens a new autocommit/TLS connection authenticated with a
    short-lived IAM token; the token is generated and kept confidential by the
    connector (Property 7). Imported lazily so importing this module does not
    require ``boto3``/``psycopg`` to be configured until a real apply runs.
    """
    from dsql_migrator.core.target_connection import DsqlConnector

    connector = DsqlConnector(target)
    return connector.connect


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


__all__ = [
    "SchemaApplier",
    "SchemaApplyError",
    "ConnectionFactory",
    "parse_create_object",
    "recreate_table",
]
