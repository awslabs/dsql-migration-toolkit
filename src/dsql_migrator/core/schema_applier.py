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

import logging
import random
import re
import time
from dataclasses import dataclass, field
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

from psycopg import sql

from dsql_migrator.core.batched_import import UNCONFIRMED_INDEX_NOTE
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
from dsql_migrator.core.target_connection import (
    is_server_unavailable,
    is_transient_connection_error,
)

if TYPE_CHECKING:
    from dsql_migrator.core.target_introspector import ColumnNullFacts


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
    """Retryable while OPENING a DSQL connection for DDL: an OCC ``40001``, Aurora DSQL's
    temporary server unavailable (``XX000``), or a transient connection failure (dropped
    socket / TLS teardown / connect timeout / the new-connection rate limit rejecting a
    connect under a burst). Lets a per-table DROP+recreate connect ride out a connection
    storm instead of failing the table -- the same resilience the batched loader's pool
    leases already have. A connect is idempotent. A permanently-unresolvable host is
    excluded so a misconfigured endpoint fails fast.
    """
    if _is_permanent_connect_error(exc):
        return False
    return (
        is_occ_conflict(exc)
        or is_server_unavailable(exc)
        or is_transient_connection_error(exc)
    )


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
    r"(?P<name>(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][\w$]*)"
    r"(?:\.(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][\w$]*))*)",
    re.IGNORECASE,
)
# The ``ON <table>`` of a CREATE INDEX, read right after the index name.
_INDEX_ON_PATTERN = re.compile(
    r"\s+ON\s+(?:ONLY\s+)?(?P<table>(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][\w$]*)"
    r"(?:\.(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][\w$]*))*)",
    re.IGNORECASE,
)


class SchemaApplyError(ValueError):
    """Raised when a target DDL statement cannot be parsed into an object.

    Only ``CREATE TABLE/VIEW/MATERIALIZED VIEW/INDEX`` statements are supported;
    anything else (a malformed or non-CREATE statement) is rejected up front
    rather than being sent to the target.
    """


class _UnconfirmedIndexError(Exception):
    """A bare ``CREATE INDEX`` failed after an attempt to create it answered Aurora DSQL's
    ``XX000``, so that attempt may have created the index -- but the failure cannot show it
    did: index names are per schema on Aurora DSQL and come from the source, where MySQL's
    are per table, so a later "already exists" (``42P07``) can be another table's index.

    Keeps the failure's ``sqlstate`` (a ``42P07`` still takes the SKIP_IF_EXISTS self-heal of
    :meth:`SchemaApplier.apply`) and reads as its first line followed by
    :data:`~dsql_migrator.core.batched_import.UNCONFIRMED_INDEX_NOTE`, the post-load index
    pass's wording.
    """

    def __init__(self, cause: BaseException) -> None:
        lines = str(cause).strip().splitlines()
        super().__init__(
            (lines[0].strip() if lines else type(cause).__name__) + UNCONFIRMED_INDEX_NOTE
        )
        self.sqlstate = getattr(cause, "sqlstate", None)


class _ExistenceOracle(Protocol):
    """The pre-apply existence check contract (a browsed TargetIntrospector)."""

    def object_exists(self, name: str) -> bool: ...


class _ParsedObject:
    """The identity of the object a CREATE statement defines.

    For an INDEX, ``on_table`` holds the ``ON`` table's unquoted parts. An index lives in
    that table's SCHEMA, so it is looked up and dropped schema-qualified: Aurora DSQL keeps
    one relation namespace per schema, and a bare ``DROP INDEX "x"`` resolves through the
    search_path -- so it could drop a same-named index of ANOTHER schema -- while a bare
    existence check matched an index of the same name in any schema and skipped this one.
    """

    __slots__ = ("name", "kind", "parts", "identifier", "on_table")

    def __init__(
        self,
        kind: str,
        parts: "list[str]",
        on_table: "Optional[list[str]]" = None,
    ) -> None:
        self.name = ".".join(parts)
        self.kind = kind
        self.parts = parts
        self.identifier = sql.Identifier(*parts)
        self.on_table = on_table

    @property
    def _index_schema(self) -> Optional[str]:
        if self.kind == _INDEX and self.on_table and len(self.on_table) > 1:
            return self.on_table[0]
        return None

    @property
    def stored_parts(self) -> Optional[tuple[str, str]]:
        """``(schema, name)`` as the catalog stores them, or ``None`` when unqualified.

        Each part cut to 63 bytes like the server does (:func:`~dsql_migrator.core.
        target_introspector.pg_stored_name`); an index's schema is its table's."""
        from dsql_migrator.core.target_introspector import pg_stored_name

        if self.kind == _INDEX:
            schema = self._index_schema
            if schema is None:
                return None
            return pg_stored_name(schema), pg_stored_name(self.name)
        if len(self.parts) != 2:
            return None
        return pg_stored_name(self.parts[0]), pg_stored_name(self.parts[1])

    @property
    def lookup_name(self) -> str:
        """The name to check existence by (an index: ``<its table's schema>.<index>``)."""
        stored = self.stored_parts
        if stored is not None:
            return ".".join(stored)
        schema = self._index_schema
        return f"{schema}.{self.name}" if schema else self.name

    @property
    def drop_identifier(self) -> "sql.Identifier":
        schema = self._index_schema
        return sql.Identifier(schema, self.name) if schema else self.identifier

    @property
    def on_table_name(self) -> Optional[str]:
        return ".".join(self.on_table) if self.on_table else None

    @property
    def stored_on_table_name(self) -> Optional[str]:
        """The ``ON`` table as the catalog stores it (each part cut to 63 bytes)."""
        from dsql_migrator.core.target_introspector import pg_stored_name

        if not self.on_table:
            return None
        return ".".join(pg_stored_name(part) for part in self.on_table)


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
    on_table = None
    if kind == _INDEX:
        on_match = _INDEX_ON_PATTERN.search(target_ddl, match.end())
        if on_match is not None:
            on_table = _split_identifier(on_match.group("table"))
    return _ParsedObject(kind, _split_identifier(match.group("name")), on_table)


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
    for part in re.findall(r'"(?:[^"]|"")+"|[^.]+', raw):
        token = part.strip()
        if token.startswith('"') and token.endswith('"'):
            parts.append(token[1:-1].replace('""', '"'))
        else:
            parts.append(token.lower())
    return parts


_LOGGER = logging.getLogger(__name__)

# The SKIPPED detail of an object that is simply already there.
SKIPPED_EXISTING_DETAIL = "Object already exists on the target; skipped."


@dataclass(frozen=True)
class ConvertedNames:
    """What the conversion emits, by stored ``(schema, name)``, so that what an EARLIER
    version of the tool left on the target under another name is recognized -- to drop it
    on a REPLACE, and to say what it is -- and never taken for the source's own object.
    Nothing here makes an apply SKIP an object: a leftover is not the object.

    ``views``: every source view as the converter emits it (quoted, each part cut to 63
    bytes). Up to 0.1.570 views were emitted unquoted, so ``V_Orders`` is on such a target
    as ``v_orders`` -- unless the source also has a view ``v_orders``, which is then that
    view, not a leftover.

    ``renamed_indexes``: ``(schema, emitted index name)`` -> the source index name the
    converter renamed it from (:func:`~dsql_migrator.core.converter.index_renames`). Up to
    0.1.570 the index kept that name, so a target built then holds it under the old one.
    ``legacy_indexes`` is the reverse: ``(schema, stored source name)`` -> ``(its table,
    emitted name)``, for telling a table or view whose name such an index holds what to do.
    ``relations``: every source table and view -> ``"table"`` / ``"view"``.
    """

    views: frozenset = frozenset()
    renamed_indexes: Mapping = field(default_factory=dict)
    legacy_indexes: Mapping = field(default_factory=dict)
    relations: Mapping = field(default_factory=dict)


def converted_names(
    view_names: "Iterable[str]",
    index_renames: "Mapping[str, Mapping[str, str]]",
    *,
    table_names: "Iterable[str]" = (),
) -> ConvertedNames:
    """:class:`ConvertedNames` from the source's view and table names (``schema.name``,
    split on the first dot like the converter) and the conversion's index renames (keyed
    by table)."""
    from dsql_migrator.core.target_introspector import pg_stored_name

    def _stored(qualified: str) -> "Optional[tuple[str, str]]":
        schema, _, name = qualified.partition(".")
        return (pg_stored_name(schema), pg_stored_name(name)) if schema and name else None

    relations: dict = {}
    for qualified in table_names:
        stored = _stored(qualified)
        if stored is not None:
            relations.setdefault(stored, "table")
    views = set()
    for qualified in view_names:
        stored = _stored(qualified)
        if stored is not None:
            views.add(stored)
            relations.setdefault(stored, "view")
    renamed: dict = {}
    legacy: dict = {}
    for table in sorted(index_renames):
        schema, _, name = table.partition(".")
        if not (schema and name):
            continue
        stored_schema = pg_stored_name(schema)
        owner = f"{stored_schema}.{pg_stored_name(name)}"
        for old, new in sorted(index_renames[table].items()):
            renamed[(stored_schema, pg_stored_name(new))] = old
            legacy.setdefault((stored_schema, pg_stored_name(old)), (owner, new))
    return ConvertedNames(
        views=frozenset(views),
        renamed_indexes=renamed,
        legacy_indexes=legacy,
        relations=relations,
    )


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
        self._converted_names: Optional[ConvertedNames] = None

    def set_converted_names(self, names: Optional[ConvertedNames]) -> None:
        """What the conversion emits (see :class:`ConvertedNames`); ``None`` when unknown."""
        self._converted_names = names

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

        # A name can be held by a DIFFERENT kind of relation, or by the index of another
        # table (Aurora DSQL keeps one namespace per schema; e.g. an earlier version left
        # the index ``currency`` of ``account`` under the name of the table ``currency``).
        # Skipping as "already exists" then reported a table or index that is not there,
        # so the holder is checked when the oracle can tell (a browsed TargetIntrospector).
        # It is matched EXACTLY, as the server stores names: the case-insensitive
        # object_exists made "Currency" and the index currency one name, refusing (or
        # skipping) the object that was not there.
        holder_of = _browsed_holder(self._introspector)
        stored = parsed.stored_parts
        exact = holder_of is not None and stored is not None
        legacy: Optional[_ParsedObject] = None
        if exact:
            holder = holder_of(stored)  # type: ignore[misc,arg-type]  # guarded above
            if holder is not None and not _holder_matches(parsed, holder):
                live = self._live_holder(parsed)
                holder = holder if live is _UNKNOWN else live
            if holder is not None and not _holder_matches(parsed, holder):
                return ApplyResult(
                    object_name=parsed.name,
                    status=ApplyStatus.FAILED,
                    detail=_name_taken_detail(parsed, holder, self._converted_names),
                )
            # What an earlier version left for this object under another name. Never a
            # reason to skip -- it is not this object (a view that dependents read by its
            # quoted name; an index whose old name a table may need) -- but a REPLACE drops
            # it, and a SKIP that creates the object says it is there.
            legacy = _legacy_twin(parsed, holder_of, self._converted_names)  # type: ignore[arg-type]
            exists = holder is not None
        else:
            exists = self._introspector.object_exists(parsed.lookup_name)

        if exists and on_conflict is ApplyMode.SKIP_IF_EXISTS:
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.SKIPPED,
                detail=SKIPPED_EXISTING_DETAIL,
            )

        if on_conflict is ApplyMode.REPLACE and legacy is not None:
            exists = True  # the leftover is dropped with it
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
            self._execute(
                parsed, target_ddl, drop_first=replace_existing, drop_target=legacy
            )
        except Exception as exc:  # noqa: BLE001 - surfaced as a FAILED result
            # Idempotent self-heal under SKIP_IF_EXISTS (Property 5 spirit): a
            # "relation already exists" (SQLSTATE 42P07) means the object is
            # already on the target even though the (possibly stale) pre-apply
            # introspection snapshot reported it absent -- e.g. an index created
            # by an earlier partial apply. Report SKIPPED so a re-apply converges
            # instead of failing. REPLACE is not self-healed here: it did not drop
            # first (the object looked absent), so the conflict is surfaced.
            if on_conflict is ApplyMode.SKIP_IF_EXISTS and _is_duplicate_object(exc):
                # ...unless the live catalog shows ANOTHER object holds the name (another
                # table's index, a table where a view goes): this object is not there.
                live = self._live_holder(parsed) if exact else _UNKNOWN
                if (
                    live is not _UNKNOWN
                    and live is not None
                    and not _holder_matches(parsed, live)  # type: ignore[arg-type]
                ):
                    return ApplyResult(
                        object_name=parsed.name,
                        status=ApplyStatus.FAILED,
                        detail=_name_taken_detail(  # type: ignore[arg-type]
                            parsed, live, self._converted_names
                        ),
                    )
                return ApplyResult(
                    object_name=parsed.name,
                    status=ApplyStatus.SKIPPED,
                    detail=SKIPPED_EXISTING_DETAIL,
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
        if legacy is not None and not replace_existing:
            note = _legacy_copy_note(parsed, legacy, self._converted_names)
            return ApplyResult(
                object_name=parsed.name,
                status=ApplyStatus.CREATED,
                detail=note,
                note=note,
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
        statements = [_build_drop_statement(parsed)]
        holder_of = _browsed_holder(self._introspector)
        legacy = (
            _legacy_folded_view(
                parsed,
                holder_of,
                self._converted_names.views if self._converted_names is not None else None,
            )
            if holder_of is not None
            else None
        )
        if legacy is not None:
            statements.append(_build_drop_statement(legacy))
        # DROP ... IF EXISTS is idempotent, so the whole unit can reconnect and replay if a
        # transient connection failure hits mid-execute (not just at connect open).
        _run_ddls_reconnecting(
            self._connection_factory,
            statements,
            occ_max_attempts=self._occ_max_attempts,
            occ_base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )

    def _live_holder(self, parsed: _ParsedObject) -> object:
        """What holds ``parsed``'s stored name on the live target now (see
        :func:`~dsql_migrator.core.target_introspector.live_relation_holder`), or
        :data:`_UNKNOWN` when it cannot be read -- the caller then keeps what it knew."""
        from dsql_migrator.core.target_introspector import live_relation_holder

        stored = parsed.stored_parts
        if stored is None:
            return _UNKNOWN
        try:
            connection = _open_connection_with_retry(
                self._connection_factory,
                occ_max_attempts=self._occ_max_attempts,
                occ_base_delay=self._occ_base_delay,
                sleep=self._sleep,
                jitter=self._jitter,
            )
        except Exception:  # noqa: BLE001 - unknown: the caller keeps the browse's answer
            return _UNKNOWN
        try:
            holder = live_relation_holder(connection, *stored)
        except Exception:  # noqa: BLE001 - unknown, as above
            return _UNKNOWN
        finally:
            _safe_close(connection)
        if holder is not None and not (
            isinstance(holder, tuple) and len(holder) == 2 and isinstance(holder[0], str)
        ):
            return _UNKNOWN
        return holder

    def _execute(
        self,
        parsed: _ParsedObject,
        target_ddl: str,
        *,
        drop_first: bool,
        drop_target: Optional[_ParsedObject] = None,
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
        spuriously hit "relation already exists". An ``XX000`` on the bare CREATE is
        retried on the same connection (see :meth:`_run_ddl`). ``drop_target`` is what an
        earlier version left under another name (:func:`_legacy_twin`): the unit drops it
        AND the exact name, so a replay after the CREATE committed drops what it created
        before re-creating it instead of failing "already exists".
        """
        if drop_first:
            drops = [_build_drop_statement(parsed)]
            if drop_target is not None:
                drops.insert(0, _build_drop_statement(drop_target))
            _run_ddls_reconnecting(
                self._connection_factory,
                [*drops, target_ddl],
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
            self._run_ddl(connection, target_ddl, kind=parsed.kind)
        finally:
            _safe_close(connection)

    def _run_ddl(self, connection: Any, statement: object, *, kind: str) -> None:
        """Execute one DDL statement with OC001 idempotent retry (Property 5).

        Aurora DSQL's XX000 (server unavailable) is retried too, on the same connection,
        which Aurora DSQL keeps; only the bare CREATE path runs here, never inside a
        reconnecting unit, so nothing nests. A bare CREATE is not idempotent: an attempt
        that answered XX000 may still have created the object. For a table, view,
        materialized view or schema (``kind``), a later "already exists" (42P07) means the
        earlier attempt did -- the name is that object's in its schema, so it is there and
        the apply succeeds. Not for an index: index names are per schema on Aurora DSQL but
        come from the source, where MySQL's are per table, so the index found may be another
        table's. Its failure keeps its meaning and is raised as
        :class:`_UnconfirmedIndexError` (it may have been created anyway), as the post-load
        index pass reports it. A 42P07 with no earlier XX000 keeps its meaning.
        """
        unconfirmed = [False]  # an earlier attempt answered XX000: its CREATE may have landed

        def _attempt(conn: Any, stmt: object) -> None:
            try:
                _execute_single_ddl(conn, stmt)
            except Exception as exc:  # noqa: BLE001 - classified, then re-raised
                if unconfirmed[0] and _is_duplicate_object(exc) and kind != _INDEX:
                    return  # the earlier attempt's CREATE landed
                if is_server_unavailable(exc):
                    unconfirmed[0] = True
                raise

        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
            retryable=lambda exc: is_occ_conflict(exc) or is_server_unavailable(exc),
        )(_attempt)
        try:
            retried(connection, statement)
        except Exception as exc:  # noqa: BLE001 - an unconfirmed index is noted, then raised
            if kind == _INDEX and unconfirmed[0]:
                raise _UnconfirmedIndexError(exc) from exc
            raise


# "The live catalog could not be read" (distinct from ``None``, "the name is free").
_UNKNOWN = object()


def _holder_matches(parsed: _ParsedObject, holder: "tuple[str, Optional[str]]") -> bool:
    """Whether the relation that holds ``parsed``'s name on the target IS that object.

    Exact, on stored names: the holder's names come from the catalog (cut to 63 bytes)."""
    kind, owner = holder
    if parsed.kind == _INDEX:
        on_table = parsed.stored_on_table_name
        return kind == "INDEX" and (not on_table or owner == on_table)
    if parsed.kind in ("TABLE", "VIEW"):
        return kind == parsed.kind
    return True


def _ascii_fold(name: str) -> str:
    """``name`` as PostgreSQL folds an unquoted identifier (ASCII letters only)."""
    from dsql_migrator.core.target_introspector import pg_fold_unquoted

    return pg_fold_unquoted(name)


_HolderLookup = Callable[[tuple[str, str]], Optional[tuple[str, Optional[str]]]]


def _legacy_folded_view(
    parsed: _ParsedObject,
    holder_of: _HolderLookup,
    own_views: "Optional[frozenset]" = None,
) -> Optional[_ParsedObject]:
    """The copy an earlier version created of view ``parsed`` under its FOLDED name, if any.

    Up to 0.1.570 views were emitted unquoted (``CREATE VIEW shop.V_Orders``), which the
    server stored as ``shop.v_orders``; they are quoted now. A view under the folded name
    is that copy -- unless it is itself a source view (``own_views``: stored names of the
    source's views, see :class:`ConvertedNames`; ``None`` when unknown). A REPLACE and a
    pre-drop drop it with the view: it would still read, and block the DROP of, its
    tables. ``holder_of`` answers for stored ``(schema, name)`` parts."""
    stored = parsed.stored_parts
    if parsed.kind != "VIEW" or stored is None:
        return None
    folded = (_ascii_fold(stored[0]), _ascii_fold(stored[1]))
    if folded == stored or (own_views is not None and folded in own_views):
        return None
    if holder_of(folded) != ("VIEW", None):
        return None
    return _ParsedObject("VIEW", list(folded))


def _legacy_renamed_index(
    parsed: _ParsedObject, holder_of: _HolderLookup, renamed: "Mapping"
) -> Optional[_ParsedObject]:
    """The copy an earlier version created of index ``parsed`` under its SOURCE name.

    The converter renames an index whose name another relation of the schema uses
    (``renamed``: see :class:`ConvertedNames`); up to 0.1.570 the first table applied kept
    the source name. An index of that name on the same table is that copy (or another
    index the converter now also renames: names that agree in 63 bytes). A REPLACE drops
    it with the index."""
    stored = parsed.stored_parts
    if parsed.kind != _INDEX or stored is None or not parsed.on_table:
        return None
    old = renamed.get(stored)
    if old is None:
        return None
    from dsql_migrator.core.target_introspector import pg_stored_name

    old_stored = (stored[0], pg_stored_name(old))
    if old_stored == stored:
        return None
    if holder_of(old_stored) != ("INDEX", parsed.stored_on_table_name):
        return None
    return _ParsedObject(_INDEX, [old], parsed.on_table)


def _legacy_twin(
    parsed: _ParsedObject, holder_of: _HolderLookup, names: Optional[ConvertedNames]
) -> Optional[_ParsedObject]:
    """What an earlier version left on the target for ``parsed`` under another name."""
    return _legacy_folded_view(
        parsed, holder_of, names.views if names is not None else None
    ) or _legacy_renamed_index(
        parsed, holder_of, names.renamed_indexes if names is not None else {}
    )


def _browsed_holder(introspector: object) -> Optional[_HolderLookup]:
    """The introspector's exact holder lookup over stored parts, if it has one."""
    holder_of = getattr(introspector, "relation_holder", None)
    if holder_of is None:
        return None
    return lambda parts: holder_of(".".join(parts))


def _legacy_copy_note(
    parsed: _ParsedObject, legacy: _ParsedObject, names: Optional[ConvertedNames]
) -> str:
    """What a SKIP apply that created ``parsed`` says about the copy an earlier version
    left under another name (it is not dropped: SKIP never drops)."""
    schema = (parsed.stored_parts or ("", ""))[0]
    if parsed.kind == _INDEX:
        blocks = ""
        needs = names.relations.get((schema, legacy.name)) if names is not None else None
        if needs is not None:
            blocks = (
                f" It also holds the name of the {needs} {schema}.{legacy.name}, which "
                "cannot be created until it is dropped."
            )
        return (
            f"Created as {parsed.name}. The copy an earlier version created under the "
            f"source name, {legacy.name}, is still on {parsed.on_table_name} and now "
            f'duplicates it; drop it (DROP INDEX "{schema}"."{legacy.name}").' + blocks
        )
    return (
        f"Created as {parsed.name}. The copy an earlier version created without quotes, "
        f"{legacy.name}, is still on the target and no longer kept in step with the "
        "source; drop it once nothing reads that name (a Replace of this view, or a Drop "
        "& reload of a table it reads, drops it)."
    )


def _name_taken_detail(
    parsed: _ParsedObject,
    holder: "tuple[str, Optional[str]]",
    names: Optional[ConvertedNames] = None,
) -> str:
    kind, owner = holder
    subject = (
        f"Index {parsed.name} on {parsed.on_table_name}"
        if parsed.kind == _INDEX and parsed.on_table_name
        else f"{parsed.kind.title()} {parsed.name}"
    )
    stored = parsed.stored_parts
    copy_of = (
        names.legacy_indexes.get(stored)
        if names is not None and stored is not None and kind == "INDEX"
        else None
    )
    if copy_of is not None and copy_of[0] == owner:
        # Up to 0.1.570 the colliding index kept this name; the conversion now creates it
        # as ``copy_of[1]``, so the old copy can go.
        return (
            f"{subject} was not created: its name is held on the target by the index "
            f"{stored[1]} on {owner}, the copy an earlier version made of the index this "
            f"migration creates as {copy_of[1]}. Drop that index "
            f'(DROP INDEX "{stored[0]}"."{stored[1]}") once {copy_of[1]} exists (this apply '
            "creates it), then apply again."
        )
    what = f"an index on {owner}" if kind == "INDEX" and owner else f"a {kind.lower()}"
    return (
        f"{subject} was not created: the name is already used on the target by {what} "
        "(Aurora DSQL keeps a schema's tables, views and indexes in one namespace; an "
        "earlier apply may have left it). Drop or rename that object on the target if it "
        "is left over, then apply again."
    )


def _build_drop_statement(parsed: _ParsedObject) -> sql.Composed:
    """Build a safe ``DROP <kind> IF EXISTS <name>`` for a destructive replace.

    The object name is composed with :class:`psycopg.sql.Identifier` (never
    interpolated), and the kind keyword comes from the fixed parsed set, so the
    statement is injection-safe (Requirement 9.4). ``IF EXISTS`` keeps the drop
    idempotent under OCC retry.
    """
    return sql.SQL("DROP {kind} IF EXISTS {name}").format(
        kind=sql.SQL(parsed.kind), name=parsed.drop_identifier
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
            # Aurora DSQL checks the limit BEFORE the existence check (live-verified on a
            # disposable cluster, 2026-10-16): once the database holds 10 user schemas,
            # EVERY CREATE SCHEMA fails -- an IF NOT EXISTS for a schema that is already
            # there, and even for ``public``. The converter emits that idempotent ensure with
            # every table, so a migration that filled the 10th schema (or any migration into
            # a cluster already at 10) failed on every later table. An ensure for a schema
            # the catalog already has is therefore complete, not failed.
            schema = _ensured_schema_name(statement)
            if schema is not None and _schema_present(connection, schema):
                return
            named = f' "{schema}"' if schema else ""
            raise SchemaApplyError(
                f"Schema{named} could not be created: the target already holds "
                f"{_DSQL_MAX_SCHEMAS} schemas, the Aurora DSQL limit (public not counted). "
                "Free schemas it no longer uses, or use another cluster."
            ) from exc
        if _is_dependent_objects_error(exc):
            # Replace the driver's "Use DROP ... CASCADE" hint with the safe fix
            # (select the dependent view too). Not retried: a dependency is a hard
            # state, not a transient conflict.
            raise SchemaApplyError(dependent_objects_hint(str(exc))) from exc
        raise
    finally:
        _safe_close(cursor)


# The idempotent schema ensure the converter emits: ``CREATE SCHEMA IF NOT EXISTS <name>``.
_ENSURE_SCHEMA_PATTERN = re.compile(
    r'^\s*CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(?P<name>"(?:[^"]|"")+"|[A-Za-z_][\w$]*)\s*;?\s*$',
    re.IGNORECASE,
)


def _ensured_schema_name(statement: object) -> Optional[str]:
    """The schema a ``CREATE SCHEMA IF NOT EXISTS`` statement ensures, as the catalog stores
    it (a quoted name exactly, an unquoted one folded to lower case), else ``None``."""
    if not isinstance(statement, str):
        return None
    match = _ENSURE_SCHEMA_PATTERN.match(statement)
    if match is None:
        return None
    raw = match.group("name")
    if raw.startswith('"'):
        return raw[1:-1].replace('""', '"')
    return raw.lower()


def _schema_present(connection: Any, schema: str) -> bool:
    """Whether ``schema`` exists on the target (``False`` when the catalog read fails)."""
    from dsql_migrator.core.target_introspector import pg_stored_name

    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s",
            (pg_stored_name(schema),),
        )
        return cursor.fetchone() is not None
    except Exception as exc:  # noqa: BLE001 - unknown means "not confirmed"...
        # ...except a transient read failure, which the caller's retry should repeat
        # instead of turning a schema that IS there into a terminal limit error.
        if (
            is_occ_conflict(exc)
            or is_server_unavailable(exc)
            or is_transient_connection_error(exc)
        ):
            raise
        return False
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
    only a connection-level transient (SQLSTATE class ``08`` / no-SQLSTATE) or Aurora DSQL's
    XX000 (its statement may have committed; the sequence is replay-safe either way) triggers
    the whole-unit reconnect -- a 40001 is absorbed per-statement, exactly as before, so there
    is no double retry.

    When ``recreated`` is given, the LAST statement is the CREATE that follows a committed
    DROP. DSQL commits each DDL immediately, so a NON-transient failure of that CREATE (not
    class 08, no SQLSTATE or XX000) leaves the object dropped and gone. It is re-raised as an
    explicit "dropped but not recreated" error rather than a generic apply failure, so the
    operator knows to restore it (a generic message reads as a harmless retryable hiccup and
    the loss goes unnoticed).
    """
    statements = list(statements)

    def _body(connection, run_one, _retry_read) -> None:
        for index, statement in enumerate(statements):
            is_recreate = recreated is not None and index == len(statements) - 1
            try:
                run_one(connection, statement)
            except Exception as exc:  # noqa: BLE001 - re-raised (loud for a lost object)
                if (
                    is_recreate
                    and not isinstance(exc, SchemaApplyError)
                    and not is_transient_connection_error(exc)
                    and not is_server_unavailable(exc)
                ):
                    raise SchemaApplyError(
                        f"{recreated.kind} {recreated.name} was DROPPED but could not be "
                        "recreated, so it no longer exists on the target. Fix the CREATE "
                        f"DDL and re-apply to restore it. Underlying error: {exc}"
                    ) from exc
                raise

    _run_unit_reconnecting(
        connection_factory,
        _body,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )


def _run_unit_reconnecting(
    connection_factory: ConnectionFactory,
    body,
    *,
    occ_max_attempts: int,
    occ_base_delay: float,
    sleep: SleepFunc,
    jitter: JitterFunc,
):
    """Run ``body(connection, run_one, retry_read)`` as ONE reconnecting unit; return its value.

    Each attempt opens a FRESH connection (closed in a ``finally``). ``run_one(connection,
    statement)`` executes one DDL with its own 40001 (OC000/OC001) retry; ``retry_read(fn)``
    runs ``fn(connection)`` under the same 40001 retry (for the catalog reads the overlay
    and the restore do inside the unit). Only a transient CONNECTION failure (SQLSTATE class
    ``08`` / no SQLSTATE, not a permanent connect error) or Aurora DSQL's XX000 (server
    unavailable; its statement may have committed) replays the whole unit on a new
    connection -- so the body must be replay-safe; a :class:`SchemaApplyError` is terminal
    and never replayed. The same nested budgets :func:`_run_ddls_reconnecting` always used;
    an XX000 is retried at this unit level only, never per statement as well.
    """
    run_one = with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )(_execute_single_ddl)

    def _unit():
        connection = connection_factory()
        try:
            def retry_read(fn):
                return with_occ_retry(
                    max_attempts=occ_max_attempts,
                    base_delay=occ_base_delay,
                    sleep=sleep,
                    jitter=jitter,
                )(fn)(connection)

            return body(connection, run_one, retry_read)
        finally:
            _safe_close(connection)

    # Only a transient CONNECTION failure or Aurora DSQL's XX000 replays the unit; a
    # SchemaApplyError (parse/schema limit/dependency/dropped-not-recreated) is terminal and
    # must never be retried.
    return with_occ_retry(
        max_attempts=occ_max_attempts,
        base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
        retryable=lambda exc: not isinstance(exc, SchemaApplyError)
        and not _is_permanent_connect_error(exc)
        and (is_transient_connection_error(exc) or is_server_unavailable(exc)),
    )(_unit)()


def drop_object(
    target_ddl: str,
    *,
    connection_factory: ConnectionFactory,
    own_views: "Optional[frozenset]" = None,
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
    autocommit DDL on a fresh connection, which is closed afterward. A view an earlier
    version left under its folded name is dropped too (see :func:`_legacy_folded_view`,
    read from the live catalog here; ``own_views`` as there).
    """
    parsed = _parse(target_ddl)
    statements = [_build_drop_statement(parsed)]
    legacy = _live_legacy_folded_view(
        parsed,
        connection_factory,
        own_views,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )
    if legacy is not None:
        statements.append(_build_drop_statement(legacy))
    # DROP ... IF EXISTS is idempotent, so the unit reconnects and replays on a transient
    # connection failure mid-execute, not merely at connect open.
    _run_ddls_reconnecting(
        connection_factory,
        statements,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )


def _live_legacy_folded_view(
    parsed: _ParsedObject,
    connection_factory: ConnectionFactory,
    own_views: "Optional[frozenset]",
    *,
    occ_max_attempts: int,
    occ_base_delay: float,
    sleep: SleepFunc,
    jitter: JitterFunc,
) -> Optional[_ParsedObject]:
    """:func:`_legacy_folded_view` on the live catalog, read as resiliently as the DROP that
    follows it: a transient connect or read failure is replayed on a fresh connection, as
    :func:`_run_ddls_reconnecting` does.

    No query for a view whose name is already lower case (it has no folded twin). A catalog
    that still cannot be read is logged and treated as "no twin": the exact DROP runs, and
    a twin that is left blocks its table's DROP with the dependent-view hint."""
    from dsql_migrator.core.target_introspector import live_relation_holder

    stored = parsed.stored_parts
    if parsed.kind != "VIEW" or stored is None:
        return None
    folded = (_ascii_fold(stored[0]), _ascii_fold(stored[1]))
    if folded == stored or (own_views is not None and folded in own_views):
        return None

    def _read() -> Optional[_ParsedObject]:
        connection = connection_factory()
        try:
            return _legacy_folded_view(
                parsed, lambda parts: live_relation_holder(connection, *parts), own_views
            )
        finally:
            _safe_close(connection)

    try:
        return with_occ_retry(
            max_attempts=occ_max_attempts,
            base_delay=occ_base_delay,
            sleep=sleep,
            jitter=jitter,
            retryable=lambda exc: not _is_permanent_connect_error(exc)
            and (is_transient_connection_error(exc) or is_server_unavailable(exc)),
        )(_read)()
    except Exception:  # noqa: BLE001 - logged; the exact DROP still runs
        _LOGGER.warning(
            "Could not read the target catalog for an earlier version's lower-case copy "
            "of view %s; only the exact name is dropped",
            parsed.name,
            exc_info=True,
        )
        return None


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
    replay (after a dropped connection or an XX000) of an already-committed ADD raises
    ``duplicate_object`` (SQLSTATE 42710); that is treated as success, because the
    constraint being present is the goal.
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


def drop_foreign_key(
    table_name: str,
    constraint_name: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> None:
    """Drop one enforced foreign key so CDC can stream without 23503 dead-letters.

    The inverse of :func:`apply_foreign_key`, needed because an enforced FK and a live
    CDC stream are mutually exclusive: the sink applies change records across several
    tasks with NO parent-before-child ordering, so a child row can arrive first and be
    rejected with SQLSTATE 23503 -- which the sink treats as a poison row and
    dead-letters permanently (no retry, offsets advance, task survives => silent data
    loss). A preceding ``Full load only`` run leaves FKs applied AND validated, so a
    session that then switches to CDC has to remove them before streaming; cut over
    re-creates them with :func:`apply_foreign_key` once the stream has drained.

    Runs as its own single autocommit DDL (DSQL allows one DDL per transaction) with
    OCC (40001) retry and a connection-level transient reconnect, exactly like its
    inverse. ``IF EXISTS`` makes it idempotent, so a reconnect that replays an
    already-committed drop does not raise ``undefined_object``. Identifiers are
    composed injection-safely. Live-verified against Aurora DSQL, which accepts
    ``ALTER TABLE ... DROP CONSTRAINT IF EXISTS``.
    """
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    ddl = sql.SQL("ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS {name}").format(
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
    # A FOREIGN KEY blocker reads "constraint fk_order_items_orders on table order_items
    # depends on table orders". It used to fall through to the view fallback below, which
    # blamed a view and told the user to select the dependent object in the object browser
    # -- impossible for a foreign key, which is not an item there. So name the constraint
    # and give an action that exists.
    #
    # It must NOT assert whose the constraint is. This text used to say "This migration did
    # not create it, so the tool will not remove it" on the strength of the reload path
    # pre-dropping the constraints this migration owns -- but Schema Conversion's REPLACE did
    # NOT, so an operator who had just used the tool's own "Apply foreign keys" was told the
    # tool did not create the constraint it had created minutes earlier. Both destructive
    # paths pre-drop now, yet this function cannot see the preserved set, so it states only
    # what it knows: the constraint blocks, it was not dropped, and here is the way out.
    constraints: list[str] = []
    for match in re.finditer(
        r"\bconstraint\s+([A-Za-z_][\w.\"$]*)\s+on\s+table\s+([A-Za-z_][\w.\"$]*)\s+"
        r"depends on\b",
        error_text,
        re.IGNORECASE,
    ):
        entry = f"{match.group(2).strip(chr(34))}.{match.group(1).strip(chr(34))}"
        if entry not in constraints:
            constraints.append(entry)
    if constraints:
        listed = ", ".join(constraints)
        one = len(constraints) == 1
        # Kept SHORT on purpose: the durable Full Load record truncates a detail at 200
        # characters, so a long explanation was cut mid-clause and the action was the part
        # lost. It also must not claim who owns the constraint (this function cannot know)
        # nor that anything re-creates foreign keys automatically (nothing has since
        # v0.1.461 -- they are the explicit "Apply foreign keys" action).
        it_them = "it" if one else "them"
        return (
            f"Cannot replace this table: the foreign {'key' if one else 'keys'} {listed} "
            f"still {'depends' if one else 'depend'} on it and {'was' if one else 'were'} "
            f"not dropped. Drop {it_them} and re-run, or use Append. (Not DROP ... CASCADE "
            f"— it deletes dependents.) Re-apply foreign keys after the next load."
        )
    return (
        "Cannot replace this table: another object (a view or a foreign key) still "
        "depends on it. If it is a view, select it in the object browser as well and "
        "re-run the apply — it is then dropped before the table is recreated, and "
        "recreated afterwards. If it is a foreign key, drop it and re-run, or choose "
        "Append instead of Drop & reload; re-apply the foreign keys after the next load "
        '("Apply foreign keys" on the Data Migration step). (Avoid '
        "DROP ... CASCADE, which the database suggests: it would delete the dependent "
        "object outright.)"
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


# ---------------------------------------------------------------------------
# Column-exclusion overlay (P2): DROP NOT NULL, then DROP DEFAULT; and the cut-over restore
# ---------------------------------------------------------------------------
#
# Settled live on Aurora DSQL (P0 F1-F12, P2 LIVE 1): both ALTERs are synchronous O(1)
# metadata changes (no sys.jobs row, no poll), accepted while any index build -- a
# multi-column UNIQUE one included -- is submitted or processing; they are idempotent (F10),
# so a 40001 retry or a whole-unit replay after a lost reply is safe; concurrent DDL on
# different tables fails 40001 (F8), so callers run the units SERIALLY; and DROP NOT NULL
# must come BEFORE DROP DEFAULT (F9: the reverse order made concurrent writers that omit the
# column store the DEFAULT into a still-NOT-NULL column, 67 x 23502).


class OverlayRefusedError(SchemaApplyError):
    """The overlay (or a restore) refused a column before any statement ran."""


class OverlayVerifyError(SchemaApplyError):
    """The read-back after the overlay (or a restore) does not show the intended state."""


_COMPUTED_REFUSAL = "Computed by Aurora DSQL; never written."


class OverlayStatement:
    """One overlay ALTER: ``action`` is ``DROP NOT NULL`` or ``DROP DEFAULT``."""

    __slots__ = ("column", "action", "statement")

    def __init__(self, column: str, action: str, statement: "sql.Composed") -> None:
        self.column = column
        self.action = action
        self.statement = statement

    def text(self) -> str:
        """The statement as shown to the operator (identifiers quoted)."""
        return self.statement.as_string(None)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"OverlayStatement({self.column!r}, {self.action!r})"


def build_overlay_statements(
    table_name: str, facts: "Mapping[str, Optional[ColumnNullFacts]]"
) -> "list[OverlayStatement]":
    """THE statement builder: per column (sorted), DROP NOT NULL, then DROP DEFAULT.

    Only what the facts still need: a nullable column gets no DROP NOT NULL, a column with
    no DEFAULT no DROP DEFAULT; an absent (``None``) column nothing. A column Aurora DSQL
    computes raises :class:`OverlayRefusedError` (the rule's B4 backstop). Fed live facts by
    the overlay and both Start dialogs, and the applied DDL's facts (:func:`ddl_column_facts`)
    by the exclusion popup, so every surface shows the statements that would run.
    """
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    table = _pg_table_identifier(table_name)
    out: list[OverlayStatement] = []
    for column in sorted(facts):
        column_facts = facts[column]
        if column_facts is None:
            continue
        if column_facts.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")
        ident = sql.Identifier(column)
        if column_facts.not_null:
            out.append(OverlayStatement(column, "DROP NOT NULL", sql.SQL(
                "ALTER TABLE {t} ALTER COLUMN {c} DROP NOT NULL").format(t=table, c=ident)))
        if column_facts.default_expr is not None:
            out.append(OverlayStatement(column, "DROP DEFAULT", sql.SQL(
                "ALTER TABLE {t} ALTER COLUMN {c} DROP DEFAULT").format(t=table, c=ident)))
    return out


@lru_cache(maxsize=32768)  # = converter.DDL_CACHE_SIZE
def _generated_columns_folded(create_ddl: str) -> frozenset:
    """The applied DDL's GENERATED columns, cached by text: the popup previews every
    matched table on each render, and the converter's reader is uncached."""
    from dsql_migrator.core.converter import parse_target_generated_columns

    return frozenset(parse_target_generated_columns(create_ddl, folded=True))


def ddl_column_facts(
    create_ddl: str, columns: "Sequence[str]"
) -> "Optional[dict[str, Optional[ColumnNullFacts]]]":
    """The facts :func:`build_overlay_statements` needs, read from an applied DDL.

    Marks identity and generated columns as the live read does (an identity column is
    implicitly NOT NULL and has no DEFAULT), so the preview refuses exactly what the live
    overlay refuses. ``None`` when the DDL cannot be read ("unknown", never "nothing to
    do"); an inner ``None`` is a column the DDL does not declare.
    """
    from dsql_migrator.core.converter import (
        parse_target_column_constraints,
        parse_target_identity_columns,
    )
    from dsql_migrator.core.target_introspector import ColumnNullFacts

    constraints = parse_target_column_constraints(create_ddl)
    if not constraints:
        return None
    generated = _generated_columns_folded(create_ddl)
    identity = parse_target_identity_columns(create_ddl)
    out: dict[str, Optional[ColumnNullFacts]] = {}
    for column in columns:
        if column not in constraints:
            out[column] = None
            continue
        not_null, default_sql = constraints[column]
        is_identity = column in identity
        is_generated = column in generated
        out[column] = ColumnNullFacts(
            not_null=not_null or is_identity,
            default_expr=None if (is_generated or is_identity) else default_sql,
            identity=is_identity,
            generated=is_generated,
        )
    return out


class OverlayOutcome:
    """What one table's overlay unit did.

    ``dropped_defaults`` maps each column whose DEFAULT this unit dropped to the pre-ALTER
    expression (what the ledger records and cut over restores).
    """

    __slots__ = ("dropped_not_null", "dropped_defaults", "already_ok")

    def __init__(self, dropped_not_null, dropped_defaults, already_ok) -> None:
        self.dropped_not_null: tuple[str, ...] = tuple(dropped_not_null)
        self.dropped_defaults: dict[str, str] = dict(dropped_defaults)
        self.already_ok: tuple[str, ...] = tuple(already_ok)

    def __eq__(self, other) -> bool:
        return isinstance(other, OverlayOutcome) and (
            self.dropped_not_null, self.dropped_defaults, self.already_ok
        ) == (other.dropped_not_null, other.dropped_defaults, other.already_ok)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"OverlayOutcome(dropped_not_null={self.dropped_not_null}, "
            f"dropped_defaults={self.dropped_defaults}, already_ok={self.already_ok})"
        )


def _backstop(table_name: str, facts) -> None:
    for column, column_facts in facts.items():
        if column_facts is not None and column_facts.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")


def apply_exclusion_overlay(
    table_name: str,
    planned: "Mapping[str, Optional[ColumnNullFacts]]",
    *,
    expected_defaults: "Optional[Mapping[str, Optional[str]]]" = None,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> OverlayOutcome:
    """Make ``planned``'s present columns nullable with no DEFAULT, as ONE unit.

    ``planned`` is the caller's pre-ALTER facts (``None`` = absent live: skipped).
    ``expected_defaults`` maps a column to the DEFAULT the ledger recorded for it (``None``
    = recorded as having none); ``None`` skips the check. Steps:

    1. the backstop, before any connect -- a computed column raises
       :class:`OverlayRefusedError`;
    2. nothing to issue by ``planned`` (every present column already nullable with no
       DEFAULT): return at once, with NO connection;
    3. otherwise one reconnecting unit on a fresh connection. EVERY attempt first re-reads
       the live facts on its own connection (a lost reply replays safely, F10) and refuses
       before ANY ALTER of that attempt when a live DEFAULT is not the recorded one -- the
       DEFAULT it would drop was never recorded, so cut over could not put it back. It then
       issues what the live catalog still needs, one statement per autocommit transaction
       with 40001 retried per statement, and does ONE read-back on the SAME connection: any
       present column still NOT NULL, still with a DEFAULT, or gone raises
       :class:`OverlayVerifyError`. No poll, no sleep, no sys.jobs.

    Every other error propagates (the caller fails the table).
    """
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    _backstop(table_name, planned)
    present = sorted(c for c, f in planned.items() if f is not None)
    if not build_overlay_statements(table_name, planned):
        return OverlayOutcome((), {}, present)

    issued_not_null: list[str] = []
    issued_defaults: dict[str, str] = {}

    def _read(conn):
        return read_overlay_column_facts(conn, table_name, present)

    def _body(connection, run_one, retry_read):
        _exists, facts = retry_read(_read)
        _backstop(table_name, facts)
        if expected_defaults is not None:
            _refuse_unrecorded_defaults(table_name, facts, expected_defaults, issued_not_null,
                                        issued_defaults)
        for statement in build_overlay_statements(table_name, facts):
            # Counted BEFORE it runs: a lost reply may still have committed it, and the
            # replay's re-read then finds nothing left to issue for it.
            if statement.action == "DROP NOT NULL":
                issued_not_null.append(statement.column)
            else:
                # What THIS statement drops: the live facts it was built from.
                issued_defaults[statement.column] = facts[statement.column].default_expr  # type: ignore[union-attr]
            run_one(connection, statement.statement)
        exists, after = retry_read(_read)
        remaining = []
        for column in present:
            live = after.get(column)
            if not exists or live is None:
                remaining.append(f"{column} (gone)")
            elif live.not_null:
                remaining.append(f"{column} (still NOT NULL)")
            elif live.default_expr is not None:
                remaining.append(f"{column} (still DEFAULT {live.default_expr})")
        if remaining:
            raise OverlayVerifyError(
                f"{table_name}: the overlay did not take effect for "
                + ", ".join(remaining)
            )
        return None

    _run_unit_reconnecting(
        connection_factory,
        _body,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )
    done = set(issued_not_null) | set(issued_defaults)
    return OverlayOutcome(
        sorted(dict.fromkeys(issued_not_null)),
        issued_defaults,
        [c for c in present if c not in done],
    )


def _refuse_unrecorded_defaults(table_name, facts, expected, issued_not_null, issued_defaults):
    """Raise before any ALTER when a live DEFAULT is not the one the ledger recorded."""
    mismatched = [
        f"{column} (live DEFAULT {live.default_expr}, recorded "
        f"{'none' if expected.get(column) is None else expected.get(column)})"
        for column, live in sorted(facts.items())
        if live is not None
        and live.default_expr is not None
        and live.default_expr != expected.get(column)
    ]
    if not mismatched:
        return
    earlier = [f"{c} DROP NOT NULL" for c in dict.fromkeys(issued_not_null)] + [
        f"{c} DROP DEFAULT" for c in issued_defaults
    ]
    changed = (
        "nothing was changed on this table"
        if not earlier
        else "an earlier attempt already issued " + ", ".join(earlier)
    )
    raise OverlayRefusedError(
        f"{table_name}: the DEFAULT to drop is not the recorded one for "
        + ", ".join(mismatched)
        + f"; {changed}. Re-run to record the current DEFAULT first."
    )


def is_single_sql_expression(expr: str) -> bool:
    """True when ``expr`` is ONE SQL expression, safe to splice into a SET DEFAULT.

    The text comes from ``pg_get_expr`` (the ledger) and is still checked, because the
    restore executes it and the ledger is a file anyone with the bucket or the state file
    can edit. The scan is exact because it refuses everything that could make its quote
    tracking wrong, none of which ``pg_get_expr`` emits:

    * a backslash anywhere but inside a PLAIN '...' literal: outside quotes, in a "..."
      identifier, and in a prefixed literal (E'...', U&'...', B'...', X'...') it could
      start an escape. Inside a plain literal it is an ordinary character under
      ``standard_conforming_strings`` (which :func:`restore_column_default` checks on its
      own connection before running such a text), and ``pg_get_expr`` prints every bytea
      constant that way ('\\x00'::bytea);
    * any ``$`` outside quotes (a dollar-quoted body, a ``$1``);
    * ``;``, ``--`` or ``/*`` outside quotes;
    * a ``,`` outside every bracket (a second ALTER TABLE subcommand), and an unbalanced
      ``(`` / ``[``;
    * an empty text or an unterminated quote.

    Quotes are '...' and "..." with the doubled-quote escape.
    """
    text = (expr or "").strip()
    if not text:
        return False
    quote: Optional[str] = None
    plain = False  # the open quote is a '...' literal with no E / U& / B / X prefix
    # Where the last '...' literal closed, and whether it was plain: a quote after
    # whitespace holding a newline CONTINUES that literal in PostgreSQL's lexer, in the
    # same (escape) mode, so it inherits instead of being judged by the char before it.
    closed_at, closed_plain = -1, False
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\" and not plain:
                return False
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 2
                    continue
                if quote == "'":
                    closed_at, closed_plain = i + 1, plain
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            gap = text[closed_at:i] if closed_at >= 0 else None
            if ch == "'" and gap is not None and gap.strip() == "" and ("\n" in gap or "\r" in gap):
                plain = closed_plain
            else:
                before = text[i - 1] if i else " "
                plain = ch == "'" and not (before.isalnum() or before in "_&$")
        elif ch == "\\":
            return False
        elif ch in ("(", "["):
            depth += 1
        elif ch in (")", "]"):
            depth -= 1
            if depth < 0:
                return False
        elif ch in (";", "$") or (ch == "," and depth == 0):
            return False
        elif text.startswith("--", i) or text.startswith("/*", i):
            return False
        i += 1
    return quote is None and depth == 0


def restore_default_statement(table_name: str, column: str, expr: str) -> sql.Composed:
    """THE ``ALTER TABLE .. ALTER COLUMN .. SET DEFAULT (expr)`` a cut-over restore runs
    (and shows). Parenthesized, so any text the single-expression scan missed is a syntax
    error rather than a second subcommand; PostgreSQL does not keep the parentheses."""
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    return (
        sql.SQL("ALTER TABLE {t} ALTER COLUMN {c} SET DEFAULT (").format(
            t=_pg_table_identifier(table_name), c=sql.Identifier(column)
        )
        + sql.SQL(expr)
        + sql.SQL(")")
    )


class RestoreOutcome:
    """``status``: restored (this unit set it) | done (already equal) | diverged | gone."""

    __slots__ = ("status", "live_default")

    def __init__(self, status: str, live_default: Optional[str]) -> None:
        self.status = status
        self.live_default = live_default

    def __eq__(self, other) -> bool:
        return isinstance(other, RestoreOutcome) and (
            self.status, self.live_default) == (other.status, other.live_default)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"RestoreOutcome({self.status!r}, {self.live_default!r})"


def restore_column_default(
    table_name: str,
    column: str,
    expr: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> RestoreOutcome:
    """Put back ONE dropped DEFAULT at cut over (``SET DEFAULT`` affects new rows only, F12).

    Refused before any connect unless ``expr`` is a single expression. Then one
    reconnecting unit whose EVERY attempt first re-reads the live column: absent -> gone;
    computed -> refused; the same DEFAULT live -> done (or restored, if an earlier attempt of
    THIS unit set it and only the reply was lost); a different DEFAULT live -> diverged, never
    overwritten; no DEFAULT -> ``SET DEFAULT expr`` and a read-back on the same connection
    that must round-trip byte-identically (F4), else :class:`OverlayVerifyError`.
    """
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    if not is_single_sql_expression(expr):
        raise OverlayRefusedError(
            f"{table_name}.{column}: the recorded DEFAULT is not a single expression, so "
            "it is not restored automatically; run SET DEFAULT by hand."
        )
    issued = {"set": False}

    def _read(conn):
        return read_overlay_column_facts(conn, table_name, [column])

    def _standard_strings(conn):
        cur = conn.cursor()
        try:
            cur.execute("SHOW standard_conforming_strings")
            row = cur.fetchone()
        finally:
            cur.close()
        return str(row[0]).strip().lower() if row else ""

    def _body(connection, run_one, retry_read):
        exists, facts = retry_read(_read)
        live = facts.get(column) if exists else None
        if live is None:
            return RestoreOutcome("gone", None)
        if live.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")
        if live.default_expr is not None:
            if live.default_expr == expr:
                return RestoreOutcome("restored" if issued["set"] else "done", expr)
            return RestoreOutcome("diverged", live.default_expr)
        if "\\" in expr and retry_read(_standard_strings) != "on":
            # Off, a backslash in a plain '...' opens an escape: the scan's quote tracking
            # no longer holds, so the text is not run.
            raise OverlayRefusedError(
                f"{table_name}.{column}: the recorded DEFAULT holds a backslash and "
                "standard_conforming_strings is off on this connection, so it is not "
                "restored automatically; run SET DEFAULT by hand."
            )
        issued["set"] = True
        # The read-back below still compares byte-identically (PostgreSQL drops the
        # parentheses the statement adds).
        run_one(connection, restore_default_statement(table_name, column, expr))
        exists, facts = retry_read(_read)
        after = facts.get(column) if exists else None
        if after is None or after.default_expr != expr:
            raise OverlayVerifyError(
                f"{table_name}.{column}: the restored DEFAULT does not round-trip "
                f"(set {expr!r}, read back "
                f"{None if after is None else after.default_expr!r})."
            )
        return RestoreOutcome("restored", expr)

    return _run_unit_reconnecting(
        connection_factory,
        _body,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )


__all__ = [
    "SchemaApplier",
    "SchemaApplyError",
    "OverlayRefusedError",
    "OverlayVerifyError",
    "OverlayStatement",
    "OverlayOutcome",
    "RestoreOutcome",
    "build_overlay_statements",
    "ddl_column_facts",
    "apply_exclusion_overlay",
    "is_single_sql_expression",
    "restore_column_default",
    "restore_default_statement",
    "ConnectionFactory",
    "parse_create_object",
    "recreate_table",
    "apply_foreign_key",
    "validate_foreign_key",
]
