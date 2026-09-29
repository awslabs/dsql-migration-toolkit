# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The record of what the column-exclusion overlay changed on the TARGET.

The overlay (``core.schema_applier.apply_exclusion_overlay``) makes an excluded column
nullable with no DEFAULT, so every row the migration writes stores NULL in it. The
original DEFAULT must come back at cut over (D3), and a later run must know which tables
already hold NULL under which column set -- neither can be re-derived from the target once
the ALTERs ran. This ledger is that record.

* Keyed by the TARGET (``overlay_ledger_key``: cluster endpoint + database), not the
  session: it survives a session restore and Start over, because the target keeps what
  the overlay did whatever the session does.
* Reads are tri-state (``present`` / ``absent`` / ``unreadable``) and NEVER raise for a
  store error: an unreadable ledger must not look like "no record" (that would under-force
  a reload or skip a restore). Callers fail closed on ``unreadable``.
* Writes RAISE (:class:`OverlayLedgerError`) unless they durably landed: every
  ``merge_tables`` (``put_tables`` is its constant form) reads the current records fresh,
  evaluates each table's merge against THAT read (never a cached view), writes the batch as
  one unit and re-reads the touched tables to verify, under a process-wide per-key lock
  (the control plane is a single task; workers never write).
* Three stores: in memory (tests), SQLite (its own table in the session-state file) and S3
  (``overlay-ledger/<key>.json`` in the session bucket, outside ``sessions/`` and
  ``jobs/`` so neither store's prune can touch it).

The pure rules below (``plan_ledger_record`` and the shared predicates) are the ONE
definition every writer and gate uses. A DEFAULT's text is compared as ``pg_get_expr``
output (P0 F4) and a recorded DDL DEFAULT as sqlglot's rendering on both sides; a sqlglot
upgrade that renders differently can only turn an automatic restore into a manual one.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Literal, Mapping, Optional, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.column_exclusion import set_digest

if TYPE_CHECKING:
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.core.target_introspector import ColumnNullFacts

LEDGER_SCHEMA_VERSION = 1
LEDGER_PREFIX = "overlay-ledger/"

RecordedDdlState = Literal["declared", "none", "unreadable"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class OverlayLedgerColumn(BaseModel):
    """One overlaid column: its dropped DEFAULT and what the applied DDL declared then."""

    model_config = ConfigDict(extra="forbid")

    column: str = Field(min_length=1)
    # pg_get_expr of the DEFAULT the overlay dropped; None when it only dropped NOT NULL.
    original_default: Optional[str] = None
    # The effective DDL at drop time (digest + what it declared for this column): cut over
    # restores automatically only while the DDL is unchanged or declares the same DEFAULT.
    recorded_ddl_digest: Optional[str] = None
    recorded_ddl_state: RecordedDdlState = "unreadable"
    recorded_ddl_default: Optional[str] = None
    dropped_not_null: bool = False
    restored_at: Optional[datetime] = None
    restore_error: Optional[str] = None


class OverlayLedgerTable(BaseModel):
    """One table's record. There is deliberately NO table-level DDL digest."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    set_digest: str = Field(min_length=1)
    set_columns: list[str] = Field(default_factory=list)
    # True when every row in the table was written under THIS column set (it was empty or
    # recreated when the set was applied): an append is then safe (append_accepted).
    empty_when_applied: bool = False
    applied_at: datetime
    columns: list[OverlayLedgerColumn] = Field(default_factory=list)

    def column(self, name: str) -> Optional[OverlayLedgerColumn]:
        return next((c for c in self.columns if c.column == name), None)


class OverlayLedgerError(RuntimeError):
    """A ledger write did not durably land (after its bounded retries)."""


class OverlayLedgerUnavailableError(RuntimeError):
    """The ledger could not be read, so a table that needs it is not touched."""

    MESSAGE = "cannot read the record of dropped DEFAULTs; nothing was changed on this table"

    def __init__(self, message: str = MESSAGE) -> None:
        super().__init__(message)


@dataclass(frozen=True)
class LedgerRead:
    """``present`` exactly when at least one table record exists; ``unreadable`` never
    means "none" -- the caller fails closed."""

    status: Literal["present", "absent", "unreadable"]
    tables: Mapping[str, OverlayLedgerTable] = field(default_factory=dict)
    error: Optional[str] = None


def overlay_ledger_key(target: "TargetConnectionConfig") -> str:
    """The ledger key: the TARGET cluster + database (filename-safe, 64 hex chars)."""
    endpoint = (target.cluster_endpoint or "").strip().lower().rstrip(".")
    return hashlib.sha256(f"v1|{endpoint}|{target.database}".encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def ddl_digest(text: Optional[str]) -> Optional[str]:
    """sha256 of the exact effective DDL text; ``None`` for no text."""
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def recorded_ddl_fields(
    effective_ddl: Optional[str], column: str
) -> "tuple[Optional[str], RecordedDdlState, Optional[str]]":
    """``(recorded_ddl_digest, recorded_ddl_state, recorded_ddl_default)`` for a column."""
    from dsql_migrator.core.converter import parse_target_column_constraints

    if not effective_ddl:
        return None, "unreadable", None
    digest = ddl_digest(effective_ddl)
    constraints = parse_target_column_constraints(effective_ddl)
    if column not in constraints:
        return digest, "unreadable", None
    _not_null, default_sql = constraints[column]
    if default_sql is None:
        return digest, "none", None
    return digest, "declared", default_sql


@dataclass(frozen=True)
class LedgerDecision:
    kind: Literal["write", "delete", "unchanged"]
    record: Optional[OverlayLedgerTable]


def plan_ledger_record(
    existing: Optional[OverlayLedgerTable],
    *,
    table: str,
    overlay_facts: "Mapping[str, ColumnNullFacts]",
    has_rows: bool,
    just_recreated: bool,
    effective_ddl: Optional[str],
    now: datetime,
    reincluded: "Sequence[str]" = (),
) -> LedgerDecision:
    """THE rule for what a table's record becomes when the overlay (or a reconcile) runs.

    ``overlay_facts`` holds the PRESENT live overlay columns and their PRE-ALTER facts
    (empty for a recorded-only table). The one rule for the Full Load pre-pass batch, the
    thread-path writer and the Start CDC batch:

    (a) a recorded-only table that holds rows and was not recreated is left UNCHANGED (never
        batched, so a failing batch never fails it);
    (b) a column whose DEFAULT is dropped now is rewritten as ONE unit (its original
        DEFAULT and what the DDL declared, restore state cleared); a column with no live
        DEFAULT keeps its record (its DEFAULT is still owed), only ``dropped_not_null`` is
        OR-ed in -- a NOT-NULL-only column gets an entry with no DEFAULT (never restored);
    (c) an existing entry for a column no longer overlaid is REMOVED when the table was
        recreated (the DEFAULT came back with the table) and kept otherwise;
    (d) recreated or empty: the current set with ``empty_when_applied``; an append with the
        same set digest keeps ``empty_when_applied`` (never downgraded); a different or new
        set on an append (only under the cdc_coexisting / legacy exemption) is not empty;
    (e) no entries and an empty current set: DELETE the record.

    ``reincluded`` (:func:`reincluded_columns`) names the recorded set's columns this run
    WRITES values into on a table that keeps its rows (the exempt append under an empty
    set): the table no longer holds NULL there in every row, so the recorded-only path of
    (a) clears ``empty_when_applied`` instead of leaving the record unchanged, and keeps
    the set, so a later non-exempt run is still refused. (With overlay columns the set
    differs from the recorded one, and (d) already records it as not empty.)
    """
    downgrade = bool(reincluded) and has_rows and not just_recreated
    if not overlay_facts and existing is not None and has_rows and not just_recreated:
        if downgrade and existing.empty_when_applied:
            return LedgerDecision(
                "write",
                existing.model_copy(update={"empty_when_applied": False, "applied_at": now}),
            )
        return LedgerDecision("unchanged", existing)
    old = {c.column: c for c in (existing.columns if existing else [])}
    columns: dict[str, OverlayLedgerColumn] = {}
    for name, entry in old.items():
        if name not in overlay_facts and not just_recreated:
            columns[name] = entry  # (c) still dropped, still owed at cut over
    for name in sorted(overlay_facts):
        facts = overlay_facts[name]
        # A recreated table starts over: the dropped table's entry says nothing about it.
        prior = None if just_recreated else old.get(name)
        if facts.default_expr is not None:
            digest, state, default = recorded_ddl_fields(effective_ddl, name)
            columns[name] = OverlayLedgerColumn(
                column=name,
                original_default=facts.default_expr,
                recorded_ddl_digest=digest,
                recorded_ddl_state=state,
                recorded_ddl_default=default,
                dropped_not_null=(prior.dropped_not_null if prior else False) or facts.not_null,
            )
        elif prior is not None:
            columns[name] = prior.model_copy(
                update={"dropped_not_null": prior.dropped_not_null or facts.not_null}
            )
        elif facts.not_null:
            columns[name] = OverlayLedgerColumn(column=name, dropped_not_null=True)
    current = sorted(overlay_facts)
    digest_now = set_digest(current)
    if just_recreated or not has_rows:
        set_columns, digest, empty = current, digest_now, True
    elif existing is not None and existing.set_digest == digest_now:
        set_columns, digest, empty = existing.set_columns, existing.set_digest, existing.empty_when_applied
    else:
        set_columns, digest, empty = current, digest_now, False
    if not columns and not current:
        return LedgerDecision("delete", None)
    return LedgerDecision(
        "write",
        OverlayLedgerTable(
            table=table,
            set_digest=digest,
            set_columns=list(set_columns),
            empty_when_applied=empty,
            applied_at=now,
            columns=[columns[c] for c in sorted(columns)],
        ),
    )


def append_accepted(record: Optional[OverlayLedgerTable], overlay_present: "Sequence[str]") -> bool:
    """An append into a table holding rows keeps the NULL contract only when every row was
    written under exactly this column set."""
    return (
        record is not None
        and record.set_digest == set_digest(overlay_present)
        and record.empty_when_applied
    )


def reincluded_columns(
    record: Optional[OverlayLedgerTable],
    *,
    present_live: "Sequence[str]",
    omitted: "Sequence[str]",
) -> "tuple[str, ...]":
    """Columns the last set excluded that are present live and no longer omitted: their
    existing rows hold NULL where the next load would write values."""
    if record is None:
        return ()
    live, gone = set(present_live), set(omitted)
    return tuple(sorted(c for c in record.set_columns if c in live and c not in gone))


APPEND_REFUSAL_TEXT = "needs Drop & reload: {columns} hold NULL from an earlier exclusion set"
APPEND_OVERLAY_REFUSAL_TEXT = (
    "needs Drop & reload: the rows already in this table were not all loaded with "
    "{columns} excluded, so appending would mix NULL with stored values"
)


@dataclass(frozen=True)
class AppendRefusal:
    """``overlay``: the current set's rows would not all hold NULL; ``reincluded``: the
    last set's columns hold NULL where this run would write values."""

    kind: Literal["overlay", "reincluded"]
    columns: "tuple[str, ...]"

    def text(self) -> str:
        template = APPEND_OVERLAY_REFUSAL_TEXT if self.kind == "overlay" else APPEND_REFUSAL_TEXT
        return template.format(columns=", ".join(self.columns))


def append_refusal(
    record: Optional[OverlayLedgerTable],
    *,
    has_rows: bool,
    exempt: bool,
    overlay_present: "Sequence[str]",
    present_live: "Sequence[str]",
    omitted: "Sequence[str]",
) -> Optional[AppendRefusal]:
    """Why appending into this table would break the NULL contract, or ``None``.

    The ONE predicate for the Start dialog's forced Drop & reload and the engine's
    fail-closed check, so both exempt and refuse the same tables. ``exempt`` is
    cdc_coexisting (Drop & reload does not exist while the sink streams): the caller
    appends and warns instead. A legacy entry is no exemption -- a refusal never names a
    legacy column, so exempting on one would only hide a refusal for another column.
    """
    if not has_rows or exempt:
        return None
    if overlay_present and not append_accepted(record, overlay_present):
        return AppendRefusal("overlay", tuple(sorted(overlay_present)))
    back = reincluded_columns(record, present_live=present_live, omitted=omitted)
    if back:
        return AppendRefusal("reincluded", back)
    return None


def recorded_default_mismatches(
    facts: "Mapping[str, Optional[ColumnNullFacts]]",
    overlay_columns: "Sequence[str]",
    recorded: "Mapping[str, str]",
) -> "tuple[str, ...]":
    """Overlay columns whose LIVE DEFAULT is not what the ledger recorded (compare first,
    before any ALTER). A live ``None`` counts as a match (already dropped)."""
    out = []
    for column in overlay_columns:
        live = facts.get(column)
        if live is not None and live.default_expr is not None and live.default_expr != recorded.get(column):
            out.append(column)
    return tuple(sorted(out))


def apply_restore_results(
    record: OverlayLedgerTable,
    results: "Mapping[str, tuple[str, Optional[str]]]",
    now: datetime,
) -> OverlayLedgerTable:
    """The record after a cut-over restore: restored/done stamp ``restored_at``; a failure
    records ``restore_error``; ``gone`` prunes the entry. The table record is kept."""
    columns = []
    for entry in record.columns:
        outcome = results.get(entry.column)
        if outcome is None:
            columns.append(entry)
            continue
        status, error = outcome
        if status == "gone":
            continue
        if status in ("restored", "done"):
            columns.append(entry.model_copy(update={"restored_at": now, "restore_error": None}))
        else:
            columns.append(entry.model_copy(update={"restore_error": error or status}))
    return record.model_copy(update={"columns": columns})


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


LedgerMerge = Callable[[Optional[OverlayLedgerTable]], LedgerDecision]


class OverlayLedgerStore(Protocol):
    def read(self, key: str) -> LedgerRead: ...

    def put_tables(
        self, key: str, updates: "Mapping[str, Optional[OverlayLedgerTable]]"
    ) -> "dict[str, Optional[OverlayLedgerTable]]": ...

    def merge_tables(
        self, key: str, merges: "Mapping[str, LedgerMerge]"
    ) -> "dict[str, Optional[OverlayLedgerTable]]": ...


_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


def ledger_lock(key: str) -> threading.RLock:
    """The process-wide re-entrant lock for one ledger key (the control plane is one task;
    workers never write). Every read-modify-write-verify holds it."""
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = _LOCKS[key] = threading.RLock()
        return lock


def _decode_payload(raw: str) -> "tuple[str, dict[str, OverlayLedgerTable]]":
    """``("ok"|"newer", tables)`` of a stored JSON payload; raises when invalid."""
    data = json.loads(raw)
    version = int(data.get("schema_version", 0))
    if version > LEDGER_SCHEMA_VERSION:
        return "newer", {}
    tables = {
        name: OverlayLedgerTable.model_validate(record)
        for name, record in (data.get("tables") or {}).items()
    }
    return "ok", tables


def _encode_payload(tables: "Mapping[str, OverlayLedgerTable]") -> str:
    return json.dumps(
        {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "tables": {name: json.loads(rec.model_dump_json()) for name, rec in sorted(tables.items())},
        },
        sort_keys=True,
    )


def _constant_merges(updates) -> "dict[str, LedgerMerge]":
    """``put_tables``' records as merges that ignore the fresh record (None deletes)."""
    return {
        name: (lambda _existing, r=record: LedgerDecision("write", r) if r is not None
               else LedgerDecision("delete", None))
        for name, record in updates.items()
    }


def _resolve(current: "Mapping[str, OverlayLedgerTable]", merges) -> "dict[str, Optional[OverlayLedgerTable]]":
    """Each merge evaluated against the FRESH record: the writes and deletes to apply
    (an ``unchanged`` decision writes nothing)."""
    updates: dict[str, Optional[OverlayLedgerTable]] = {}
    for name, merge in merges.items():
        decision = merge(current.get(name))
        if decision.kind == "write":
            updates[name] = decision.record
        elif decision.kind == "delete":
            updates[name] = None
    return updates


def _apply(current: "Mapping[str, OverlayLedgerTable]", updates) -> "dict[str, OverlayLedgerTable]":
    merged = dict(current)
    for name, record in updates.items():
        if record is None:
            merged.pop(name, None)
        else:
            merged[name] = record
    return merged


def _verify(updates, readback: "Mapping[str, OverlayLedgerTable]") -> "dict[str, Optional[OverlayLedgerTable]]":
    out: dict[str, Optional[OverlayLedgerTable]] = {}
    for name, record in updates.items():
        got = readback.get(name)
        if record is None:
            if got is not None:
                raise OverlayLedgerError(f"ledger delete of {name} did not land")
        elif got != record:
            raise OverlayLedgerError(f"ledger write of {name} did not read back as written")
        out[name] = got
    return out


class _BlobLedger:
    """A store that keeps one JSON document per key (in-memory and S3 share this)."""

    def __init__(self, *, max_attempts: int = 3, sleep: Callable[[float], None] = time.sleep) -> None:
        self._max_attempts = max(1, max_attempts)
        self._sleep = sleep

    # Subclasses: _get(key) -> Optional[str] (None = absent; raise on error); _put(key, raw).
    def _get(self, key: str) -> Optional[str]:
        raise NotImplementedError

    def _put(self, key: str, raw: str) -> None:
        raise NotImplementedError

    def _get_retrying(self, key: str) -> Optional[str]:
        last: Optional[BaseException] = None
        for attempt in range(self._max_attempts):
            try:
                return self._get(key)
            except Exception as exc:  # noqa: BLE001 - bounded retry, then the caller decides
                last = exc
                if attempt + 1 < self._max_attempts:
                    self._sleep(0.2 * (attempt + 1))
        raise last  # type: ignore[misc]

    def read(self, key: str) -> LedgerRead:
        try:
            raw = self._get_retrying(key)
        except Exception as exc:  # noqa: BLE001 - never raises: unreadable
            return LedgerRead("unreadable", {}, f"{type(exc).__name__}: {exc}")
        if raw is None:
            return LedgerRead("absent")
        try:
            status, tables = _decode_payload(raw)
        except Exception as exc:  # noqa: BLE001 - a corrupt payload is unreadable, not absent
            return LedgerRead("unreadable", {}, f"invalid ledger payload: {exc}")
        if status == "newer":
            return LedgerRead("unreadable", {}, "the ledger was written by a newer version")
        return LedgerRead("present" if tables else "absent", MappingProxyType(tables))

    def put_tables(self, key, updates):
        return self.merge_tables(key, _constant_merges(updates))

    def merge_tables(self, key, merges):
        """Under the key's lock: a FRESH read, each merge evaluated against it, one write,
        a verifying re-read. Returns every named table's record after the call."""
        if not merges:
            return {}
        with ledger_lock(key):
            last: Optional[BaseException] = None
            for attempt in range(self._max_attempts):
                try:
                    raw = self._get(key)
                    if raw is None:
                        current: dict = {}
                    else:
                        status, current = _decode_payload(raw)
                        if status == "newer":
                            raise OverlayLedgerError(
                                "the ledger was written by a newer version; refusing to overwrite it"
                            )
                    updates = _resolve(current, merges)
                    if not updates:
                        return {name: current.get(name) for name in merges}
                    self._put(key, _encode_payload(_apply(current, updates)))
                    back = self._get(key)
                    _status, readback = _decode_payload(back) if back else ("ok", {})
                    verified = _verify(updates, readback)
                    return {name: verified[name] if name in updates else current.get(name)
                            for name in merges}
                except OverlayLedgerError:
                    raise
                except Exception as exc:  # noqa: BLE001 - bounded retry
                    last = exc
                    if attempt + 1 < self._max_attempts:
                        self._sleep(0.2 * (attempt + 1))
            raise OverlayLedgerError(f"ledger write failed: {type(last).__name__}: {last}") from last


class InMemoryOverlayLedger(_BlobLedger):
    """For tests and doubles."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.blobs: dict[str, str] = {}

    def _get(self, key):
        return self.blobs.get(key)

    def _put(self, key, raw):
        self.blobs[key] = raw


class S3OverlayLedger(_BlobLedger):
    """``overlay-ledger/<key>.json`` in the session bucket (outside sessions/ and jobs/).

    ``absent`` ONLY for ``NoSuchKey``: NoSuchBucket, AccessDenied, throttling, 5xx and
    network errors are retried and then read ``unreadable`` (a write raises). The client
    is built lazily (no network at construction); the bucket is ensured by the session
    store at startup, never here.
    """

    def __init__(self, bucket: str, *, region: Optional[str] = None, aws_profile: Optional[str] = None,
                 prefix: str = LEDGER_PREFIX, s3_client=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._bucket = bucket
        self._region = region
        self._aws_profile = aws_profile
        self._prefix = prefix
        self._client = s3_client

    def _s3(self):
        if self._client is None:
            from dsql_migrator.core.aws_session import build_session

            self._client = build_session(self._aws_profile).client("s3", region_name=self._region)
        return self._client

    def _object_key(self, key: str) -> str:
        return f"{self._prefix}{key}.json"

    def _get(self, key):
        try:
            obj = self._s3().get_object(Bucket=self._bucket, Key=self._object_key(key))
        except Exception as exc:  # noqa: BLE001 - classify below
            code = (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")
            if code == "NoSuchKey":
                return None
            raise
        body = obj["Body"].read()
        return body.decode("utf-8") if isinstance(body, bytes) else str(body)

    def _put(self, key, raw):
        self._s3().put_object(
            Bucket=self._bucket, Key=self._object_key(key),
            Body=raw.encode("utf-8"), ContentType="application/json",
        )


class SqliteOverlayLedger:
    """Its own ``overlay_ledger`` table in the session-state SQLite file.

    One row per (target key, table); a batch is ONE transaction (``BEGIN IMMEDIATE``, the
    upserts and deletes, ``COMMIT``; rollback on any error), then a read-back. The session
    store's delete / prune never touch this table.
    """

    def __init__(self, path: str, *, max_attempts: int = 3, sleep: Callable[[float], None] = time.sleep) -> None:
        import sqlite3

        self._max_attempts = max(1, max_attempts)
        self._sleep = sleep
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=5, isolation_level=None)
        with self._lock:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS overlay_ledger ("
                "target_key TEXT NOT NULL, table_name TEXT NOT NULL, "
                "schema_version INTEGER NOT NULL, payload TEXT NOT NULL, "
                "updated_at TEXT NOT NULL, PRIMARY KEY (target_key, table_name))"
            )

    def _select(self, key: str):
        return self._conn.execute(
            "SELECT table_name, schema_version, payload FROM overlay_ledger WHERE target_key = ?",
            (key,),
        ).fetchall()

    def _rows(self, key: str):
        with self._lock:
            return self._select(key)

    def _decode_rows(self, rows):
        tables: dict[str, OverlayLedgerTable] = {}
        for name, version, payload in rows:
            if int(version) > LEDGER_SCHEMA_VERSION:
                return "newer", {}
            tables[name] = OverlayLedgerTable.model_validate_json(payload)
        return "ok", tables

    def read(self, key: str) -> LedgerRead:
        last: Optional[BaseException] = None
        for attempt in range(self._max_attempts):
            try:
                status, tables = self._decode_rows(self._rows(key))
                if status == "newer":
                    return LedgerRead("unreadable", {}, "the ledger was written by a newer version")
                return LedgerRead("present" if tables else "absent", MappingProxyType(tables))
            except Exception as exc:  # noqa: BLE001 - bounded retry, then unreadable
                last = exc
                if attempt + 1 < self._max_attempts:
                    self._sleep(0.2 * (attempt + 1))
        return LedgerRead("unreadable", {}, f"{type(last).__name__}: {last}")

    def put_tables(self, key, updates):
        return self.merge_tables(key, _constant_merges(updates))

    def merge_tables(self, key, merges):
        """One ``BEGIN IMMEDIATE`` transaction: read the key's rows inside it, evaluate each
        merge against them, upsert / delete, ``COMMIT`` (rollback on any error); then a
        verifying read-back."""
        if not merges:
            return {}
        with ledger_lock(key):
            last: Optional[BaseException] = None
            for attempt in range(self._max_attempts):
                try:
                    now = datetime.now(timezone.utc).isoformat()
                    with self._lock:
                        self._conn.execute("BEGIN IMMEDIATE")
                        try:
                            status, current = self._decode_rows(self._select(key))
                            if status == "newer":
                                raise OverlayLedgerError(
                                    "the ledger was written by a newer version; refusing to overwrite it"
                                )
                            updates = _resolve(current, merges)
                            for name, record in updates.items():
                                if record is None:
                                    self._conn.execute(
                                        "DELETE FROM overlay_ledger WHERE target_key = ? AND table_name = ?",
                                        (key, name),
                                    )
                                else:
                                    self._conn.execute(
                                        "INSERT OR REPLACE INTO overlay_ledger "
                                        "(target_key, table_name, schema_version, payload, updated_at) "
                                        "VALUES (?, ?, ?, ?, ?)",
                                        (key, name, LEDGER_SCHEMA_VERSION, record.model_dump_json(), now),
                                    )
                            self._conn.execute("COMMIT")
                        except BaseException:
                            # SQLite rolls back by itself on some errors (disk full, I/O):
                            # a second ROLLBACK would then raise and hide the real error.
                            if self._conn.in_transaction:
                                try:
                                    self._conn.execute("ROLLBACK")
                                except Exception:  # noqa: BLE001 - keep the original error
                                    pass
                            raise
                    if not updates:
                        return {name: current.get(name) for name in merges}
                    _status, readback = self._decode_rows(self._rows(key))
                    verified = _verify(updates, readback)
                    return {name: verified[name] if name in updates else current.get(name)
                            for name in merges}
                except OverlayLedgerError:
                    raise
                except Exception as exc:  # noqa: BLE001 - bounded retry ('database is locked')
                    last = exc
                    if attempt + 1 < self._max_attempts:
                        self._sleep(0.2 * (attempt + 1))
            raise OverlayLedgerError(f"ledger write failed: {type(last).__name__}: {last}") from last


class OverlayLedgerView:
    """One run's view of the ledger: read ONCE, then kept current by each write's read-back.

    ``write`` refuses on an unreadable view, and on success swaps in a NEW mapping (the
    current one plus the verified read-back); a raise leaves the view unchanged.
    """

    def __init__(self, store: OverlayLedgerStore, key: str, initial: LedgerRead) -> None:
        self._store = store
        self._key = key
        self.status = initial.status
        self.error = initial.error
        self._tables: Mapping[str, OverlayLedgerTable] = MappingProxyType(dict(initial.tables))

    @property
    def tables(self) -> Mapping[str, OverlayLedgerTable]:
        return self._tables

    def record(self, table: str) -> Optional[OverlayLedgerTable]:
        return self._tables.get(table)

    def write(self, updates: "Mapping[str, Optional[OverlayLedgerTable]]") -> None:
        """Put these records as they are (``None`` deletes). A writer whose record depends
        on the current one uses :meth:`merge` instead."""
        self.merge(_constant_merges(updates))

    def merge(self, merges: "Mapping[str, LedgerMerge]") -> None:
        """Each table's merge is evaluated under the key's lock against a FRESH store read,
        never this view's cached record, so a concurrent writer's entries are not lost."""
        if not merges:
            return
        if self.status == "unreadable":
            raise OverlayLedgerError(OverlayLedgerUnavailableError.MESSAGE)
        with ledger_lock(self._key):
            after = self._store.merge_tables(self._key, merges)
            merged = dict(self._tables)
            for name, record in after.items():
                if record is None:
                    merged.pop(name, None)
                else:
                    merged[name] = record
            self._tables = MappingProxyType(merged)
            self.status = "present" if merged else "absent"


__all__ = [
    "APPEND_OVERLAY_REFUSAL_TEXT",
    "APPEND_REFUSAL_TEXT",
    "AppendRefusal",
    "InMemoryOverlayLedger",
    "LEDGER_PREFIX",
    "LEDGER_SCHEMA_VERSION",
    "LedgerDecision",
    "LedgerMerge",
    "LedgerRead",
    "OverlayLedgerColumn",
    "OverlayLedgerError",
    "OverlayLedgerStore",
    "OverlayLedgerTable",
    "OverlayLedgerUnavailableError",
    "OverlayLedgerView",
    "S3OverlayLedger",
    "SqliteOverlayLedger",
    "append_accepted",
    "append_refusal",
    "apply_restore_results",
    "ddl_digest",
    "ledger_lock",
    "overlay_ledger_key",
    "plan_ledger_record",
    "recorded_ddl_fields",
    "recorded_default_mismatches",
    "reincluded_columns",
]
