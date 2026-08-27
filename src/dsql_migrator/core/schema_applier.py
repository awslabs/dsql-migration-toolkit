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


# getaddrinfo failures for a host that does not exist (a typo'd or deleted DSQL endpoint).
# These never recover on retry, unlike a transient DNS blip ("temporary failure in name
# resolution", EAI_AGAIN) or a refused connect (a rebooting cluster) -- which stay retryable.
_PERMANENT_CONNECT_SIGNATURES = (
    "could not translate host name",
    "nodename nor servname provided",
    "name or service not known",
    "no address associated with hostname",
)


def _is_permanent_connect_error(exc: BaseException) -> bool:
    """True for a connect failure that will NEVER succeed on retry (host does not resolve).

    :func:`is_transient_connection_error` treats every no-SQLSTATE psycopg error as transient,
    so a typo'd / deleted DSQL endpoint (a getaddrinfo NXDOMAIN) would otherwise be retried
    the full OCC budget before failing. Fail fast on the unambiguous permanent-resolution
    signatures ONLY -- NOT ``temporary failure in name resolution`` (a DNS blip) or a refused
    connect (a rebooting cluster), which stay retryable. Scoped to the applier; the shared
    :func:`is_transient_connection_error` (used by the batched loader's pool) is unchanged.
    """
    message = str(exc).lower()
    return any(sig in message for sig in _PERMANENT_CONNECT_SIGNATURES)


def _is_retryable_connect_error(exc: BaseException) -> bool:
    """Retryable while OPENING a DSQL connection for DDL: an OCC ``40001`` or a
    transient connection failure (dropped socket / TLS teardown / connect timeout /
    the new-connection rate limit rejecting a connect under a burst). Lets a
    per-table DROP+recreate connect ride out a connection storm instead of failing
    the table -- the same resilience the batched loader's pool leases already have.
    A permanently-unresolvable host is excluded so a misconfigured endpoint fails fast.
    """
    if _is_permanent_connect_error(exc):
        return False
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
_DUPLICATE_CONSTRAINT_SQLSTATE = "42710"

# PostgreSQL ``program_limit_exceeded``. Aurora DSQL raises this on ``CREATE SCHEMA``
# when the cluster is already at its hard cap of 10 schemas ("more than 10 schemas
# not allowed"). It is a HARD limit, not a transient/OCC conflict -- retrying never
# clears it -- so it must surface immediately as an actionable message telling the
# user to free a schema, not be swallowed or retried.
_PROGRAM_LIMIT_SQLSTATE = "54000"
_DSQL_MAX_SCHEMAS = 10

# PostgreSQL ``dependent_objects_still_exist``. Raised when a destructive REPLACE
# tries to DROP a table that a VIEW still selects from. The apply already pre-drops
# the views IN THE APPLY SET before recreating tables, but a view created by an
# EARLIER session (and not selected this time) is invisible to that pre-pass, so the
# DROP fails. The raw error names the blocking view and suggests DROP ... CASCADE --
# which is the wrong advice here: it would silently destroy a view this tool may not
# know how to recreate. The actionable fix is to include that view in the selection so
# the apply drops and recreates it in the right order, which is what the translated
# message says.
_DEPENDENT_OBJECTS_SQLSTATE = "2BP01"

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

    A double-quoted part is taken verbatim (minus the quotes); an UNQUOTED part is
    folded to lower case, because PostgreSQL/DSQL case-fold an unquoted identifier
    at ``CREATE`` time -- ``CREATE TABLE Orders`` creates the relation ``orders``.
    Preserving the written case would make the existence key and the ``DROP`` target
    the case-exact ``"Orders"``, which does not match the folded ``orders``: the
    ``DROP ... IF EXISTS`` then silently no-ops and a destructive REPLACE fails to
    replace (or the re-CREATE hits "relation already exists"). Folding here keeps the
    parsed identity consistent with what the server actually stored. The parts are
    recomposed safely with :class:`psycopg.sql.Identifier` when building a DROP
    statement, so the name can never break out into SQL (Requirement 9.4).
    """
    parts: list[str] = []
    for part in re.findall(r'"[^"]+"|[^.]+', raw):
        token = part.strip()
        if token.startswith('"') and token.endswith('"'):
            parts.append(token[1:-1])
        else:
            parts.append(token.lower())
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
        # DROP ... IF EXISTS is idempotent, so the whole unit can reconnect and replay if a
        # transient connection failure hits mid-execute (not just at connect open).
        _run_ddls_reconnecting(
            self._connection_factory,
            [_build_drop_statement(parsed)],
            occ_max_attempts=self._occ_max_attempts,
            occ_base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )

    def _execute(
        self, parsed: _ParsedObject, target_ddl: str, *, drop_first: bool
    ) -> None:
        """Run the DROP (when replacing) and the CREATE as single-DDL txns.

        Each ``execute`` runs on an autocommit connection, so the ``DROP`` and the
        ``CREATE`` are never combined into one transaction (Property 2 / DDL separation),
        and each statement is wrapped in OC001 retry (Property 5).

        A destructive REPLACE (``drop_first``) runs the ``DROP IF EXISTS`` + ``CREATE`` as
        one replay-idempotent unit (:func:`_run_ddls_reconnecting`): a transient connection
        failure reconnects and replays (the DROP makes the CREATE safe to re-run), and a
        committed DROP followed by a NON-transient CREATE failure is surfaced as an explicit
        "dropped but not recreated" error instead of silent data loss. A brand-new object
        (no drop) runs the bare ``CREATE`` on a storm-retried connect but is NOT replayed on
        a mid-execute transient -- a bare CREATE is not idempotent, so a replay could
        spuriously hit "relation already exists".
        """
        if drop_first:
            _run_ddls_reconnecting(
                self._connection_factory,
                [_build_drop_statement(parsed), target_ddl],
                occ_max_attempts=self._occ_max_attempts,
                occ_base_delay=self._occ_base_delay,
                sleep=self._sleep,
                jitter=self._jitter,
                recreated=parsed,
            )
            return
        connection = _open_connection_with_retry(
            self._connection_factory,
            occ_max_attempts=self._occ_max_attempts,
            occ_base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )
        try:
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
    """Execute exactly one DDL statement on ``connection`` (one transaction).

    DSQL's hard 10-schema-per-cluster limit surfaces here (on ``CREATE SCHEMA``) as
    a raw ``program_limit_exceeded``; it is translated into an actionable
    :class:`SchemaApplyError` so the user is told to free a schema rather than
    seeing an opaque driver error. It is NOT retried (a hard limit, not transient),
    so raising here propagates straight out of the (OCC-only) retry wrapper.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
    except Exception as exc:  # noqa: BLE001 - translate specific, actionable failures
        if _is_schema_limit_exceeded(exc):
            raise SchemaApplyError(
                f"Aurora DSQL allows at most {_DSQL_MAX_SCHEMAS} schemas per "
                "cluster, and this one is already at the limit, so the schema this "
                "migration needs could not be created. Remove an unused schema from "
                "the target cluster (DROP SCHEMA ... CASCADE) or use a different "
                "cluster, then retry."
            ) from exc
        if _is_dependent_objects_error(exc):
            # Replace the driver's "Use DROP ... CASCADE" hint with the safe fix
            # (select the dependent view too). Not retried: a dependency is a hard
            # state, not a transient conflict.
            raise SchemaApplyError(dependent_objects_hint(str(exc))) from exc
        raise
    finally:
        _safe_close(cursor)


def _run_ddls_reconnecting(
    connection_factory: ConnectionFactory,
    statements: Sequence[object],
    *,
    occ_max_attempts: int,
    occ_base_delay: float,
    sleep: SleepFunc,
    jitter: JitterFunc,
    recreated: Optional[_ParsedObject] = None,
) -> None:
    """Run an idempotent DDL sequence on a fresh connection, reconnecting and replaying the
    WHOLE sequence on a transient connection failure -- not merely the connect open.

    A statement's connection can drop mid-execute during a connection storm (many workers
    opening fresh DSQL connections at once when a wave of tables finishes -- tripping the
    new-connection rate limit, a TLS teardown, or an expired token). Retrying the execute on
    the now-dead connection is futile, so the unit re-opens a fresh connection and replays
    from the start. Callers pass ONLY replay-safe statements: ``CREATE SCHEMA IF NOT EXISTS``,
    ``DROP ... IF EXISTS``, or a ``CREATE`` that a preceding ``DROP IF EXISTS`` in the same
    sequence makes safe to re-run. Each statement still carries its own OC001 (40001) retry;
    only a connection-level transient (SQLSTATE class ``08`` / no-SQLSTATE) triggers the
    whole-unit reconnect -- a 40001 is absorbed per-statement, exactly as before, so there is
    no double retry.

    When ``recreated`` is given, the LAST statement is the CREATE that follows a committed
    DROP. DSQL commits each DDL immediately, so a NON-transient failure of that CREATE leaves
    the object dropped and gone. It is re-raised as an explicit "dropped but not recreated"
    error rather than a generic apply failure, so the operator knows to restore it (a generic
    message reads as a harmless retryable hiccup and the loss goes unnoticed).
    """
    statements = list(statements)
    run_one = with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )(_execute_single_ddl)

    def _unit() -> None:
        connection = connection_factory()
        try:
            for index, statement in enumerate(statements):
                is_recreate = recreated is not None and index == len(statements) - 1
                try:
                    run_one(connection, statement)
                except Exception as exc:  # noqa: BLE001 - re-raised (loud for a lost object)
                    if (
                        is_recreate
                        and not isinstance(exc, SchemaApplyError)
                        and not is_transient_connection_error(exc)
                    ):
                        raise SchemaApplyError(
                            f"{recreated.kind} {recreated.name} was DROPPED but could not be "
                            "recreated, so it no longer exists on the target. Fix the CREATE "
                            f"DDL and re-apply to restore it. Underlying error: {exc}"
                        ) from exc
                    raise
        finally:
            _safe_close(connection)

    # Only a transient CONNECTION failure replays the unit; a SchemaApplyError (parse/schema
    # limit/dependency/dropped-not-recreated) is terminal and must never be retried.
    with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
        retryable=lambda exc: not isinstance(exc, SchemaApplyError)
        and not _is_permanent_connect_error(exc)
        and is_transient_connection_error(exc),
    )(_unit)()


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
    # DROP ... IF EXISTS is idempotent, so the unit reconnects and replays on a transient
    # connection failure mid-execute, not merely at connect open.
    _run_ddls_reconnecting(
        connection_factory,
        [_build_drop_statement(parsed)],
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )


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
    # The schema ensure(s), the DROP, and the CREATE run as ONE replay-idempotent unit. A
    # transient connection failure at any point -- the connection storm when a wave of table
    # workers open fresh DSQL connections at once and trip the new-connection rate limit --
    # reconnects and replays from the start, not just the connect open (without which the
    # per-table DROP+recreate failed the table outright, 0 rows loaded). A committed DROP
    # followed by a NON-transient CREATE failure is surfaced as an explicit "dropped but not
    # recreated" error (recreated=parsed) instead of leaving the table gone under a generic
    # message. Each statement keeps its own OC001 (40001) retry.
    _run_ddls_reconnecting(
        connection_factory,
        [*schema_ddls, _build_drop_statement(parsed), target_ddl],
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
        recreated=parsed,
    )


def _is_duplicate_object(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is a "relation already exists" error.

    PostgreSQL/DSQL raise ``SQLSTATE 42P07`` when a relation -- a table, view, or
    index, which all share the relation namespace -- already exists. Detection
    uses the exception's ``sqlstate`` attribute (not its type), so it matches both
    real ``psycopg`` errors and test fakes, mirroring
    :func:`~dsql_migrator.core.occ.is_occ_conflict`.
    """
    return getattr(exc, "sqlstate", None) == _DUPLICATE_OBJECT_SQLSTATE


def _is_duplicate_constraint(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` means the constraint already exists.

    PostgreSQL/DSQL raise ``duplicate_object`` (``SQLSTATE 42710``) when
    ``ALTER TABLE ... ADD CONSTRAINT`` names a constraint that is already present.
    Treated as success by :func:`apply_foreign_key` so a reconnect that replays an
    already-committed ADD is idempotent.
    """
    return getattr(exc, "sqlstate", None) == _DUPLICATE_CONSTRAINT_SQLSTATE


def apply_foreign_key(
    add_ddl: object,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> None:
    """Apply one preserved foreign key as a post-load ``ALTER TABLE ADD CONSTRAINT``.

    Aurora DSQL enforces foreign keys, but they must NOT exist during the
    concurrent, cross-table bulk load (a child row can commit before its parent),
    so each FK is added AFTER the load, as its own single autocommit DDL (DSQL
    allows one DDL per transaction) with OC001 (40001) retry and a connection-level
    transient reconnect. ``ADD CONSTRAINT`` is not natively idempotent, so a
    reconnect that replays an already-committed ADD raises ``duplicate_object``
    (SQLSTATE 42710); that is treated as success, because the constraint being
    present is the goal.
    """
    try:
        _run_ddls_reconnecting(
            connection_factory,
            [add_ddl],
            occ_max_attempts=occ_max_attempts,
            occ_base_delay=occ_base_delay,
            sleep=sleep,
            jitter=jitter,
        )
    except Exception as exc:  # noqa: BLE001 - duplicate == already applied (idempotent)
        if _is_duplicate_constraint(exc):
            return
        raise


def validate_foreign_key(
    table_name: str,
    constraint_name: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> None:
    """Validate a ``NOT VALID`` foreign key against existing rows (async DDL).

    The counterpart to :func:`apply_foreign_key`: on Aurora DSQL an ``ADD CONSTRAINT``
    must be ``NOT VALID`` (enforces new writes without scanning existing data), so
    ``ALTER TABLE ASYNC <t> VALIDATE CONSTRAINT <name>`` then marks the constraint
    validated for the already-loaded rows. It runs as an async DDL job (returns
    immediately). Callers run it best-effort after their orphan pre-gate is clean, so
    it is expected to succeed; if it does not, the constraint still enforces every new
    write. Identifiers are composed injection-safely.
    """
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    ddl = sql.SQL("ALTER TABLE ASYNC {tbl} VALIDATE CONSTRAINT {name}").format(
        tbl=_pg_table_identifier(table_name),
        name=sql.Identifier(constraint_name),
    )
    _run_ddls_reconnecting(
        connection_factory,
        [ddl],
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )


def _is_dependent_objects_error(exc: BaseException) -> bool:
    """Return ``True`` if a DROP failed because another object depends on the target.

    PostgreSQL/DSQL raise ``dependent_objects_still_exist`` (SQLSTATE ``2BP01``).
    Matches the SQLSTATE, with a message fallback for a wrapped/re-raised error that
    lost it (mirroring :func:`_is_schema_limit_exceeded`).
    """
    if getattr(exc, "sqlstate", None) == _DEPENDENT_OBJECTS_SQLSTATE:
        return True
    return "depend on it" in str(exc).lower()


def dependent_objects_hint(error_text: str) -> str:
    """Turn a raw ``dependent_objects_still_exist`` error into actionable guidance.

    The database's own HINT is "Use DROP ... CASCADE", which is the WRONG advice for
    this tool: cascading would silently destroy a view the tool may not be able to
    recreate (Property 12 -- destructive work stays under the operator's control).

    The apply already pre-drops every view IN THE SELECTION before recreating tables,
    so a blocking view means it simply was not selected -- typically created by an
    earlier apply. Including it in the selection is the fix: the pre-pass then drops it
    first and its own apply unit recreates it.

    Names the blocking objects when the driver reported them (the DETAIL lines), so the
    user knows exactly what to add. Pure/unit-testable: takes the error text, returns
    the replacement message.
    """
    # DETAIL lines read e.g. "view ecommerce_demo.customer_order_summary depends on
    # table ecommerce_demo.countries" -- pull the dependent object's name.
    blockers: list[str] = []
    for match in re.finditer(
        r"\b(?:view|materialized view)\s+([A-Za-z_][\w.\"$]*)\s+depends on\b",
        error_text,
        re.IGNORECASE,
    ):
        name = match.group(1).strip('"')
        if name not in blockers:
            blockers.append(name)
    if blockers:
        listed = ", ".join(blockers)
        one = len(blockers) == 1
        return (
            f"Cannot replace this table: the {'view' if one else 'views'} {listed} "
            f"still {'depends' if one else 'depend'} on it. Select "
            f"{'that view' if one else 'those views'} in the object browser as well "
            f"and re-run the apply — {'it is' if one else 'they are'} then dropped "
            "before the table is recreated, and recreated afterwards. (Avoid DROP ... "
            "CASCADE, which the database suggests: it would delete the "
            f"{'view' if one else 'views'} outright.)"
        )
    return (
        "Cannot replace this table: another object (usually a view) still depends on "
        "it. Select the dependent object in the object browser as well and re-run the "
        "apply — it is then dropped before the table is recreated, and recreated "
        "afterwards. (Avoid DROP ... CASCADE, which the database suggests: it would "
        "delete the dependent object outright.)"
    )


def _is_schema_limit_exceeded(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is DSQL's "too many schemas" hard-limit error.

    DSQL caps a cluster at ``_DSQL_MAX_SCHEMAS`` schemas and raises
    ``program_limit_exceeded`` (SQLSTATE ``54000``) on a ``CREATE SCHEMA`` that
    would exceed it. Match the SQLSTATE and require the message to mention schemas
    (``54000`` also covers other program limits, e.g. too many columns), and fall
    back to a message signature when the SQLSTATE was lost (wrapped/re-raised).
    """
    state = getattr(exc, "sqlstate", None)
    message = str(exc).lower()
    if state == _PROGRAM_LIMIT_SQLSTATE:
        return "schema" in message
    return "schemas not allowed" in message or "more than 10 schemas" in message


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
    "apply_foreign_key",
    "validate_foreign_key",
]
