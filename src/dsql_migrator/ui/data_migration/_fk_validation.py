# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Aurora DSQL's verdict on the foreign keys "Apply foreign keys" puts in place (G142).

"Add first, check after": the pass
(:func:`~dsql_migrator.ui.data_migration._full_load_engine.apply_preserved_foreign_keys`)
adds every preserved key ``NOT VALID`` -- seconds, no row read, and from then on Aurora
DSQL refuses every write that would break it -- and then submits ``ALTER TABLE ASYNC ...
VALIDATE CONSTRAINT``, a server-side job that checks the rows already loaded. This module
reads that verdict back and acts on it:

* each key's state, from ``pg_constraint.convalidated`` and ``sys.jobs`` BY the job id the
  pass recorded (:func:`~dsql_migrator.core.target_introspector.foreign_key_validation_states`)
  -- never ``CALL sys.wait_for_job``, which blocks for as long as an id is unknown to it;
* a background watch that polls it with backoff and heartbeats and records what it saw on
  the session (``DataMigrationState``, persisted: Aurora DSQL drops a finished job after
  about 30 minutes, and the verdict must outlive a re-render, a Validation re-run and an
  app restart) -- and, only for a key whose validation FAILED, runs the page-resumable
  orphan count (G139) and builds the literal-free query that lists the rows;
* "Validate again", which submits the validation anew once the rows are fixed;
* the one renderer both screens show it with (Data Migration for Full load only, Cut over).

A key is "validated" only when ``convalidated`` is true.
"""

from __future__ import annotations

import logging
import re
import time as _time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Collection, Mapping, Optional, Sequence

from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus, log_activity
from dsql_migrator.core.models import ForeignKeyDef
from dsql_migrator.core.schema_applier import validate_foreign_key
from dsql_migrator.core.target_introspector import (
    foreign_key_validation_states,
    target_primary_key_columns,
)
from dsql_migrator.core.validation_sql import build_orphan_listing_sql

_LOGGER = logging.getLogger(__name__)

# One key's state, read from Aurora DSQL (fk_validation_state).
FK_VALIDATED = "validated"  # convalidated is true
FK_VALIDATING = "validating"  # its job is submitted / processing
FK_FAILED = "failed"  # its job failed: rows already in the child break it
FK_NOT_VALIDATED = "not-validated"  # in place, no job running (never submitted, or gone)
FK_ABSENT = "absent"  # not on the child table (dropped since)
FK_UNKNOWN = "unknown"  # not read yet, or the catalog could not be read

# The states a watch stops polling a key in.
_SETTLED = frozenset({FK_VALIDATED, FK_FAILED, FK_NOT_VALIDATED, FK_ABSENT})
_RUNNING = ("submitted", "processing")

# Aurora DSQL's per-transaction row limit, which a repair of the reported rows must keep to.
REPAIR_BATCH_ROWS = 3000

# One watch job polls for at most this long; the screens start another while a key is still
# being validated (a large child takes Aurora DSQL a while: 330 s for 3M rows, measured).
_WATCH_BUDGET_SECONDS = 600.0
_POLL_FIRST_SECONDS = 2.0
_POLL_MAX_SECONDS = 30.0

# A failed job's ``details`` is the constraint error ("insert or update on table ... violates
# foreign key constraint ..."), with no key value on Aurora DSQL; PostgreSQL's own message adds
# "Key (col)=(value) is not present", which is dropped so no row value is kept (Property 7).
_KEY_VALUE_RE = re.compile(r"\bKey\s*\(.*?\)\s*=\s*\(.*?\).*", re.IGNORECASE | re.DOTALL)
_DETAILS_MAX = 300


def sanitize_job_details(text: object) -> Optional[str]:
    """The first line of a job's ``details``, with any ``Key (...)=(...)`` part removed."""
    if not text:
        return None
    lines = str(text).strip().splitlines()
    first = _KEY_VALUE_RE.sub("", lines[0] if lines else "").strip().rstrip(".,;: ")
    return first[:_DETAILS_MAX] or None


def fk_validation_state(read: object, record: object = None) -> str:
    """One key's state from its catalog ``read`` (a ``ForeignKeyValidationRead`` or
    ``None``) and its ``record`` (a ``ForeignKeyValidationJob``). Pure.

    ``convalidated`` decides "validated"; the job row decides "validating" / "failed". Once
    Aurora DSQL dropped the row (about 30 minutes after a job ends), the status the record
    last saw stands in: a failure stays a failure. A job that reports ``completed`` while
    ``convalidated`` is not true yet is still validating (the catalog catches up).
    """
    if read is None:
        return FK_UNKNOWN
    if not getattr(read, "present", False):
        return FK_ABSENT
    if getattr(read, "validated", False):
        return FK_VALIDATED
    status = getattr(read, "job_status", None)
    if status in _RUNNING or status == "completed":
        return FK_VALIDATING
    if status == "failed":
        return FK_FAILED
    if getattr(record, "status", None) == "failed":
        return FK_FAILED
    if getattr(record, "job_id", None) and not getattr(read, "jobs_read", True):
        return FK_VALIDATING  # sys.jobs unreadable this time; the job may still run
    return FK_NOT_VALIDATED


@dataclass(frozen=True)
class FkValidationItem:
    """One key as the screens show it (value-free)."""

    table: str
    constraint: str
    state: str
    details: Optional[str] = None
    orphan_count: Optional[int] = None
    listing_sql: Optional[str] = None
    count_error: Optional[str] = None
    # The listing reads a key window whose start was found by a count in another process
    # (before a restart): its placeholders cannot be filled in here.
    listing_start_unknown: bool = False

    @property
    def key(self) -> "tuple[str, str]":
        return (self.table, self.constraint)

    @property
    def label(self) -> str:
        return f"{self.table}.{self.constraint}"

    @property
    def counting(self) -> bool:
        """A failure whose explaining count has not reported yet."""
        return self.state == FK_FAILED and self.orphan_count is None and not self.count_error


@dataclass(frozen=True)
class FkValidationView:
    """The recorded keys' states, as last read from Aurora DSQL (``read_at`` None: not read
    in this process yet -- the screens then say they are reading it)."""

    items: "tuple[FkValidationItem, ...]" = ()
    read_at: Optional[datetime] = None

    def _count(self, *states: str) -> int:
        return sum(1 for item in self.items if item.state in states)

    @property
    def total(self) -> int:
        return len(self.items)

    @property
    def in_place(self) -> int:
        """Keys not known to be gone (an unread one counts: the pass put it in place)."""
        return self.total - self._count(FK_ABSENT)

    @property
    def confirmed_in_place(self) -> int:
        """Keys the last read found on the target."""
        return self._count(FK_VALIDATED, FK_VALIDATING, FK_FAILED, FK_NOT_VALIDATED)

    @property
    def validated(self) -> int:
        return self._count(FK_VALIDATED)

    @property
    def validating(self) -> int:
        return self._count(FK_VALIDATING)

    @property
    def absent(self) -> int:
        return self._count(FK_ABSENT)

    @property
    def unknown(self) -> int:
        return self._count(FK_UNKNOWN)

    @property
    def checking(self) -> bool:
        return self.read_at is None

    @property
    def settled(self) -> bool:
        return not self.checking and all(item.state in _SETTLED for item in self.items)

    def state_of(self, key: "tuple[str, str]") -> Optional[str]:
        for item in self.items:
            if item.key == key:
                return item.state
        return None

    def to_revalidate(self) -> "list[FkValidationItem]":
        """The keys "Validate again" resubmits: failed, or in place with no job running."""
        return [item for item in self.items if item.state in (FK_FAILED, FK_NOT_VALIDATED)]


def fk_validation_view(
    records: "Mapping[tuple[str, str], object]",
    reads: "Mapping[tuple[str, str], object]",
    read_at: Optional[datetime],
    count_errors: "Optional[Mapping[tuple[str, str], str]]" = None,
    listings: "Optional[Mapping[tuple[str, str], str]]" = None,
) -> Optional[FkValidationView]:
    """The view of ``records`` (``ForeignKeyValidationJob`` by key) under the last catalog
    ``reads``; ``None`` with nothing recorded. ``listings``: a failed key's listing query
    with its window's start filled in (this process only), shown instead of the recorded
    literal-free one. Pure."""
    if not records:
        return None
    items = []
    for key in sorted(records):
        record = records[key]
        state = fk_validation_state(reads.get(key), record) if read_at is not None else FK_UNKNOWN
        failed = state == FK_FAILED
        recorded_listing = getattr(record, "listing_sql", None) if failed else None
        runnable = (listings or {}).get(key) if failed else None
        items.append(
            FkValidationItem(
                table=key[0],
                constraint=key[1],
                state=state,
                details=getattr(record, "details", None) if failed else None,
                orphan_count=getattr(record, "orphan_count", None) if failed else None,
                listing_sql=runnable or recorded_listing,
                count_error=(count_errors or {}).get(key) if failed else None,
                listing_start_unknown=bool(
                    not runnable
                    and recorded_listing
                    and LISTING_START_PLACEHOLDER in recorded_listing
                ),
            )
        )
    return FkValidationView(items=tuple(items), read_at=read_at)


def foreign_key_defs(table_conversions) -> "dict[tuple[str, str], ForeignKeyDef]":
    """``(child, constraint) -> ForeignKeyDef`` for every rendered preserved key. Pure."""
    from dsql_migrator.ui.data_migration._full_load_engine import _constraint_name_from_ddl

    defs: "dict[tuple[str, str], ForeignKeyDef]" = {}
    for child, conv in (table_conversions or {}).items():
        by_name = {fk.name: fk for fk in getattr(conv, "preserved_foreign_keys", ()) or ()}
        for ddl in getattr(conv, "foreign_key_ddls", ()) or ():
            name = _constraint_name_from_ddl(ddl)
            fk = by_name.get(name) if name else None
            if fk is not None:
                defs[(child, name)] = fk
    return defs


def pending_foreign_keys(table_conversions) -> "list[tuple[str, Optional[str]]]":
    """``(child, constraint)`` of EVERY rendered foreign-key DDL, in order -- a key the
    operator added or renamed in Schema Conversion too, which has no source metadata, and
    one added without a name under the name the pass gives it. The constraint is ``None``
    when its name cannot be read from the DDL: such a key can never be confirmed in place
    by a catalog read. Pure."""
    from dsql_migrator.ui.data_migration._full_load_engine import _named_foreign_key_ddls

    return [
        (child, name)
        for child, conv in (table_conversions or {}).items()
        for name, _ddl in _named_foreign_key_ddls(getattr(conv, "foreign_key_ddls", ()) or ())
    ]


# One identifier as a DDL writes it: double-quoted (as rendered) or bare (typed by hand).
_IDENT = r'(?:"(?:[^"]|"")+"|[^\s(),."]+)'
_FK_SHAPE_RE = re.compile(
    rf"FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+({_IDENT}(?:\s*\.\s*{_IDENT})?)\s*\(([^)]*)\)",
    re.IGNORECASE,
)
_FK_ACTION_RE = re.compile(
    r"\bON\s+(?:DELETE|UPDATE)\s+(?:CASCADE|SET\s+NULL|SET\s+DEFAULT)\b", re.IGNORECASE
)


def _unquote(identifier: str) -> str:
    token = identifier.strip()
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"')
    return token.lower()  # a bare identifier folds to lower case


def _fk_ddl_shape(ddl: object) -> "Optional[tuple[list[str], str, list[str]]]":
    """``(child columns, parent table, parent columns)`` read from an ``ADD CONSTRAINT ...
    FOREIGN KEY`` DDL, for a key without source metadata; ``None`` when unreadable."""
    match = _FK_SHAPE_RE.search(str(ddl))
    if match is None:
        return None

    def _columns(text: str) -> "list[str]":
        return [_unquote(token) for token in re.findall(_IDENT, text)]

    parent = ".".join(_unquote(part) for part in re.findall(_IDENT, match.group(2)))
    return _columns(match.group(1)), parent, _columns(match.group(3))


def _matches_loosely_on_mysql(column: object) -> bool:
    """Whether MySQL matches this text key column's values more loosely than Aurora DSQL,
    which compares a ``varchar`` byte for byte: a case-insensitive (``*_ci``) collation, or
    a PAD SPACE one (every collation but the ``*_0900_*`` / NO PAD ones and ``binary`` --
    ``utf8mb4_bin`` and ``latin1_bin`` included), which ignores trailing spaces, so
    ``'abc '`` matches ``'abc'``. A ``CHAR(n)`` column keeps that on Aurora DSQL (its
    ``char(n)`` ignores trailing spaces too); a ``VARCHAR`` does not."""
    from dsql_migrator.core.assessor import _is_case_insensitive_collation

    collation = str(getattr(column, "collation", None) or "").strip().lower()
    if not collation:
        return False
    if _is_case_insensitive_collation(collation):
        return True
    if collation == "binary" or "_0900_" in collation or "nopad" in collation:
        return False
    return not str(getattr(column, "mysql_type", "") or "").strip().lower().startswith("char")


def foreign_keys_cdc_can_break(table_conversions, tables) -> "frozenset[tuple[str, str]]":
    """The keys a MySQL CDC stream can leave orphans behind for, so Cut over holds Finish on
    them until Aurora DSQL validates them (G142). Pure; the caller applies it to a MySQL
    source on a Full load + CDC migration only.

    * An ``ON DELETE`` / ``ON UPDATE`` ``CASCADE``, ``SET NULL`` or ``SET DEFAULT`` (the
      cascade gap): InnoDB applies it inside the engine and never writes it to the binary
      log, so CDC replicates the parent change and not the child rows it changed.
    * A text key MySQL matches more loosely than Aurora DSQL -- on the child's columns or
      the parent's: a case-insensitive (``*_ci``) collation (Evaluation's ``CI_COLLATION``
      finding), or a PAD SPACE one on a ``VARCHAR`` key (``utf8mb4_bin`` too), which
      ignores trailing spaces. Aurora DSQL compares the bytes, so a child written during
      CDC can match no parent there.

    Every rendered key counts, by its constraint name (one added without a name by the name
    the pass gives it): a key with source metadata by that metadata (the source's own
    action, which InnoDB applies), and one the operator renamed or added in Schema
    Conversion by its DDL (its ``ON DELETE`` / ``ON UPDATE`` action and the columns it
    names). The final Validation's defaults (row counts, the key reconcile) see none of
    these: the rows are there, only their keys point nowhere.
    """
    from dsql_migrator.ui.data_migration._full_load_engine import _named_foreign_key_ddls

    by_table = {getattr(table, "name", None): table for table in (tables or ())}

    def _loose(table_name: Optional[str], columns: "Sequence[str]") -> bool:
        table = by_table.get(table_name)
        wanted = set(columns)
        return table is not None and any(
            column.name in wanted and _matches_loosely_on_mysql(column)
            for column in table.columns
        )

    breakable: "set[tuple[str, str]]" = set()
    for child, conv in (table_conversions or {}).items():
        by_name = {fk.name: fk for fk in getattr(conv, "preserved_foreign_keys", ()) or ()}
        for name, ddl in _named_foreign_key_ddls(getattr(conv, "foreign_key_ddls", ()) or ()):
            if not name:
                continue
            fk = by_name.get(name)
            if fk is not None:
                cascades = fk.has_cascade_action
                columns, parent, parent_columns = (
                    fk.columns, fk.referenced_table, fk.referenced_columns
                )
            else:
                cascades = bool(_FK_ACTION_RE.search(str(ddl)))
                columns, parent, parent_columns = _fk_ddl_shape(ddl) or ([], None, [])
            if cascades or _loose(child, columns) or _loose(parent, parent_columns):
                breakable.add((child, name))
    return frozenset(breakable)


def watch_foreign_key_validation(
    connection_factory: Callable[[], Any],
    records: "Callable[[], Mapping[tuple[str, str], object]]",
    *,
    on_read: "Optional[Callable[[Mapping, Mapping, datetime], Optional[Mapping]]]" = None,
    on_failed: "Optional[Callable[[tuple[str, str]], None]]" = None,
    heartbeat: Optional[Callable[[], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    budget_seconds: float = _WATCH_BUDGET_SECONDS,
    first_delay: float = _POLL_FIRST_SECONDS,
    max_delay: float = _POLL_MAX_SECONDS,
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
    read: Optional[Callable[..., Mapping]] = None,
    now: Optional[Callable[[], datetime]] = None,
) -> Mapping:
    """Poll Aurora DSQL for the recorded keys' validation until each one settles (G142).

    ``records()`` is read again before every poll, so a "Validate again" that replaces a
    job id is picked up. Each poll is ONE catalog read
    (:func:`~dsql_migrator.core.target_introspector.foreign_key_validation_states`: every
    key's ``convalidated`` and ``sys.jobs`` by the recorded job ids), handed to
    ``on_read(reads, job ids asked for, when the read started)``, which returns the reads
    it stored (``None``: it stores nothing of its own). A key is judged only by a read
    taken for the job it still has: one whose job changed while the read was in flight
    ("Validate again"), or one the caller did not store, is read again -- never judged by
    the old job's answer. A key whose job failed is handed to ``on_failed`` once per job
    id. Between polls the wait doubles from ``first_delay`` to ``max_delay``, with a
    ``heartbeat`` before each poll. Returns the last read when every key is validated,
    failed, not validated (no job running) or gone, when ``should_cancel()`` asks, or after
    ``budget_seconds`` -- the screens start another watch while a key is still being
    validated, and Aurora DSQL's job runs on whether or not anyone watches it.
    """
    reader = read or foreign_key_validation_states
    clock = now if callable(now) else (lambda: datetime.now(timezone.utc))
    beat = heartbeat if callable(heartbeat) else (lambda: None)
    stopped = should_cancel if callable(should_cancel) else (lambda: False)
    delay = max(0.0, first_delay)
    deadline = monotonic() + max(0.0, budget_seconds)
    reported: "set[tuple[tuple[str, str], object]]" = set()
    reads: Mapping = {}
    while True:
        beat()
        current = dict(records() or {})
        if not current:
            return {}
        keys = sorted(current)
        jobs = {key: rec.job_id for key, rec in current.items() if getattr(rec, "job_id", None)}
        started = clock()
        reads = reader(keys, jobs, connection_factory=connection_factory)
        stored = on_read(reads, jobs, started) if callable(on_read) else None
        current = dict(records() or {})
        judged = reads if stored is None else stored
        states = {
            key: fk_validation_state(judged.get(key), current[key])
            if getattr(current[key], "job_id", None) == jobs.get(key)
            else FK_UNKNOWN
            for key in keys
            if key in current
        }
        for key in states:
            marker = (key, getattr(current.get(key), "job_id", None))
            if states[key] == FK_FAILED and marker not in reported:
                reported.add(marker)
                if callable(on_failed):
                    on_failed(key)
        if all(state in _SETTLED for state in states.values()):
            return reads
        if stopped() or monotonic() >= deadline:
            return reads
        sleep(delay)
        delay = min(max(max_delay, 0.0), max(delay * 2, first_delay))


def orphan_listing_query(
    connection_factory: Callable[[], Any], table: str, fk: ForeignKeyDef
) -> str:
    """The literal-free query listing ``fk``'s orphans over the whole child, keyed by the
    live child key -- the fallback when the count that finds their window did not finish."""
    key = target_primary_key_columns(table, connection_factory=connection_factory) or []
    return build_orphan_listing_sql(table, fk, key).as_string(None)


# What a listing query's window start is called while its values are not filled in.
LISTING_START_PLACEHOLDER = "%(after_"


def explain_foreign_key_failure(
    connection_factory: Callable[[], Any],
    table: str,
    fk: ForeignKeyDef,
    *,
    on_page: Optional[Callable[[], None]] = None,
) -> "tuple[int, Optional[str], Optional[str]]":
    """``(orphans, listing query, listing query to run)`` for a key Aurora DSQL's
    validation failed on (G142).

    The page-resumable count (G139, :func:`_count_orphans`: keyset pages of the live key,
    each replayed alone after a dropped connection or a conflict) runs only now, after a
    failure, to say how many rows break the key, and notes the page where it found the
    first one. The listing reads THAT window -- the count's page after its cursor -- never
    the whole child in one statement. The first query is literal-free: the window's start
    is ``%(after_0)s``... (no key value: it is kept on the session and logged). The second
    has those values filled in, to run as it stands; it holds key values, so it is only
    shown on the screen (``None`` when the window starts at the first row, where the two
    are the same). No orphan found: no listing. Raises when the count cannot finish.
    """
    from psycopg import sql

    from dsql_migrator.ui.data_migration._full_load_engine import (
        _ORPHAN_PAGE_SIZE,
        _count_orphans,
    )

    first: dict = {}
    count = int(
        _count_orphans(
            connection_factory, table, fk, on_page=on_page,
            on_orphans=lambda cursor: first.setdefault("cursor", cursor),
        )
    )
    if not count:
        return 0, None, None
    key = target_primary_key_columns(table, connection_factory=connection_factory) or []
    cursor = first.get("cursor")
    if "cursor" not in first or (cursor is not None and len(cursor) != len(key)):
        # No window (a child without a key), or the key changed under the count.
        return count, build_orphan_listing_sql(table, fk, key).as_string(None), None
    template = build_orphan_listing_sql(
        table, fk, key, page_size=_ORPHAN_PAGE_SIZE,
        after=[sql.Placeholder(f"after_{i}") for i in range(len(key))] if cursor else None,
    ).as_string(None)
    runnable = (
        build_orphan_listing_sql(
            table, fk, key, page_size=_ORPHAN_PAGE_SIZE,
            after=[sql.Literal(value) for value in cursor],
        ).as_string(None)
        if cursor
        else None
    )
    return count, template, runnable


def _repair_guidance() -> str:
    return (
        f"Fix them on Aurora DSQL in batches of at most {REPAIR_BATCH_ROWS:,} rows per "
        "transaction (its limit): add the missing parent rows, point each row at an "
        "existing parent or NULL, or delete it. Then click Validate again."
    )


def _call_safely(callback: Optional[Callable[[], None]]) -> None:
    if not callable(callback):
        return
    try:
        callback()
    except Exception:  # noqa: BLE001 - a refresh of a page that went away
        _LOGGER.debug("Foreign-key validation refresh failed", exc_info=True)


def _job_live(job_manager: object, job_id: Optional[str]) -> bool:
    if not job_id or job_manager is None:
        return False
    try:
        status = getattr(job_manager.get_status(job_id), "status", None)  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - an unknown id is not a live job
        return False
    return status in ("PENDING", "RUNNING")


def _defs_provider(fk_defs) -> "Callable[[], Mapping[tuple[str, str], ForeignKeyDef]]":
    """Resolve ``fk_defs`` (a mapping or a callable returning one) once, lazily."""
    cache: dict = {}

    def _defs() -> "Mapping[tuple[str, str], ForeignKeyDef]":
        if "defs" not in cache:
            try:
                cache["defs"] = dict((fk_defs() if callable(fk_defs) else fk_defs) or {})
            except Exception:  # noqa: BLE001 - no metadata: a failure is still reported
                _LOGGER.warning("Foreign-key metadata unavailable", exc_info=True)
                cache["defs"] = {}
        return cache["defs"]

    return _defs


def run_fk_validation_watch(
    migration_state,
    *,
    connection_factory: Callable[[], Any],
    fk_defs=None,
    category: ActivityCategory = ActivityCategory.VALIDATION,
    refresh: Optional[Callable[[], None]] = None,
    handle: object = None,
    budget_seconds: float = _WATCH_BUDGET_SECONDS,
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
) -> Mapping:
    """The watch job's work: poll, record what Aurora DSQL reports, explain a failure.

    Every read is folded into the session's record (``set_fk_validation_reads``), which
    keeps a failed job's status and details past the ~30 minutes Aurora DSQL keeps the job.
    A key whose validation failed is counted (page-resumable, heartbeat per page) and logged
    ONCE per job: the details, the count, the listing query and the repair steps. The run
    ends with one SUCCESS line when the last key validates.
    """
    defs = _defs_provider(fk_defs)
    beat = getattr(handle, "heartbeat", None)

    def _on_read(reads: Mapping, jobs: Mapping, started_at: Optional[datetime] = None):
        newly_validated, all_validated, changed, stored = (
            migration_state.set_fk_validation_reads(reads, jobs, now=started_at)
        )
        if newly_validated and all_validated:
            total = len(migration_state.fk_validation_records())
            log_activity(
                category,
                "foreign keys validated",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"Aurora DSQL validated all {total} foreign key(s) in place: the rows "
                    "already loaded satisfy every key, and each one enforces every new write."
                ),
            )
        if changed:
            _call_safely(refresh)
        # The watch judges each key by what the session stored ({} when it stored none:
        # the read started before the record changed, so it is taken again).
        return stored if stored is not None else {}

    def _on_failed(key: "tuple[str, str]") -> None:
        record = migration_state.fk_validation_record(key)
        if record is None or record.orphan_count is not None:
            return  # already explained (the record persists across restarts)
        job_id = record.job_id
        if migration_state.fk_explain_error(key, job_id) is not None:
            return  # this job's count already failed once in this process
        fk = defs().get(key)
        details = record.details or "no details reported"
        count: Optional[int] = None
        listing: Optional[str] = None
        runnable: Optional[str] = None
        error: Optional[str] = None
        if fk is None:
            error = "the foreign key's definition is not in this session's schema"
        else:
            try:
                count, listing, runnable = explain_foreign_key_failure(
                    connection_factory, key[0], fk, on_page=beat
                )
            except Exception as exc:  # noqa: BLE001 - reported with the failure
                _LOGGER.warning("Orphan count failed for %s.%s", *key, exc_info=True)
                sqlstate = getattr(exc, "sqlstate", None)
                error = f"{type(exc).__name__}" + (f", SQLSTATE {sqlstate}" if sqlstate else "")
                try:
                    listing = orphan_listing_query(connection_factory, key[0], fk)
                except Exception:  # noqa: BLE001 - the listing is a convenience
                    listing = None
        # The literal-free query is kept and logged; the one with its window's start filled
        # in holds key values, so it stays in this process for the screen only.
        migration_state.note_fk_explanation(
            key, job_id, count, listing, error=error, listing_runnable=runnable
        )
        if count is None:
            found = f"The rows that break it could not be counted ({error})."
        elif count:
            found = (
                f"{count:,} child row(s) reference a parent row that is not on Aurora DSQL."
            )
        else:
            found = "No such row is found now (it may have been fixed since)."
        log_activity(
            category,
            "foreign key not validated",
            status=ActivityStatus.WARNING,
            target=f"{key[0]}.{key[1]}",
            detail=(
                f"Aurora DSQL's validation of the rows already loaded failed: {details}. "
                f"{found} The key stays in place, NOT VALID: it refuses every new write "
                "that would add such a row, but the rows already there are not proven."
                + (
                    f" List them with: {listing}"
                    + (
                        " (it reads the key window where the count found the first one; "
                        "the screen shows it with that window's start filled in)."
                        if runnable
                        else "."
                    )
                    if listing
                    else ""
                )
                + " "
                + _repair_guidance()
            ),
        )
        _call_safely(refresh)

    return watch_foreign_key_validation(
        connection_factory,
        migration_state.fk_validation_records,
        on_read=_on_read,
        on_failed=_on_failed,
        heartbeat=beat,
        should_cancel=lambda: bool(getattr(handle, "cancelled", False)),
        budget_seconds=budget_seconds,
        sleep=sleep,
        monotonic=monotonic,
    )


def start_fk_validation_watch(
    migration_state,
    job_manager: object,
    *,
    connection_factory: Optional[Callable[[], Any]],
    fk_defs=None,
    category: ActivityCategory = ActivityCategory.VALIDATION,
    refresh: Optional[Callable[[], None]] = None,
) -> Optional[str]:
    """Start the background watch of the recorded keys -- one per session at a time.

    Returns the new job id, or ``None`` when nothing is recorded, a watch is already
    running (it reads the record on every poll, so it covers new job ids too), or there is
    no job manager / connection. Best-effort: the watch only reads and reports.
    """
    if job_manager is None or connection_factory is None or migration_state is None:
        return None
    if not migration_state.fk_validation_records():
        return None

    def _work(handle: object = None) -> None:
        migration_state.mark_fk_validation_watch_active()
        try:
            run_fk_validation_watch(
                migration_state,
                connection_factory=connection_factory,
                fk_defs=fk_defs,
                category=category,
                refresh=refresh,
                handle=handle,
            )
        finally:
            migration_state.note_fk_validation_watch_ended()
            # The screens re-arm their own return while a key is still being validated
            # and no watch reads it, so a watch that ran out of time is started again.
            _call_safely(refresh)

    def _submit() -> str:
        return job_manager.submit(_work, durable=False)  # type: ignore[attr-defined]

    try:
        return migration_state.start_fk_validation_watch(
            _submit, lambda job_id: _job_live(job_manager, job_id)
        )
    except Exception:  # noqa: BLE001 - the watch is a report; never break the caller
        _LOGGER.warning("Could not start the foreign-key validation watch", exc_info=True)
        return None


def submit_fk_revalidation(
    migration_state,
    job_manager: object,
    *,
    connection_factory: Optional[Callable[[], Any]],
    keys: "Sequence[tuple[str, str]]",
    fk_defs=None,
    category: ActivityCategory = ActivityCategory.VALIDATION,
    refresh: Optional[Callable[[], None]] = None,
) -> Optional[str]:
    """"Validate again": submit ``ALTER TABLE ASYNC ... VALIDATE CONSTRAINT`` anew for
    ``keys`` (their rows fixed), record the new job ids and watch them.

    Read-only on the data and asynchronous on Aurora DSQL, so it needs no drained
    attestation: every key in place already enforces each new write. A key with no record
    is skipped. One resubmission at a time per session; without a job manager it runs
    inline (library callers, tests).
    """
    wanted = [key for key in keys if migration_state.fk_validation_record(key) is not None]
    if not wanted or connection_factory is None:
        return None

    def _work(handle: object = None) -> None:
        beat = getattr(handle, "heartbeat", None)
        submitted: "dict[tuple[str, str], Optional[str]]" = {}
        failed: "dict[tuple[str, str], str]" = {}
        for key in wanted:
            _call_safely(beat)
            try:
                submitted[key] = validate_foreign_key(
                    key[0], key[1], connection_factory=connection_factory
                )
            except Exception as exc:  # noqa: BLE001 - reported per key
                _LOGGER.warning("VALIDATE CONSTRAINT not submitted for %s.%s", *key, exc_info=True)
                sqlstate = getattr(exc, "sqlstate", None)
                failed[key] = type(exc).__name__ + (f", SQLSTATE {sqlstate}" if sqlstate else "")
        migration_state.record_fk_revalidation(submitted)
        if submitted:
            log_activity(
                category,
                "foreign key validation submitted again",
                status=ActivityStatus.INFO,
                detail=(
                    f"Aurora DSQL validates {len(submitted)} foreign key(s) again: "
                    + ", ".join(f"{t}.{c}" for t, c in sorted(submitted))
                ),
            )
        for (table, constraint), reason in sorted(failed.items()):
            log_activity(
                category,
                "foreign key validation not submitted",
                status=ActivityStatus.WARNING,
                target=f"{table}.{constraint}",
                detail=f"Validate again could not start Aurora DSQL's check ({reason})",
            )
        _call_safely(refresh)
        start_fk_validation_watch(
            migration_state, job_manager, connection_factory=connection_factory,
            fk_defs=fk_defs, category=category, refresh=refresh,
        )

    if job_manager is None:
        _work()
        return None

    def _submit() -> str:
        return job_manager.submit(_work, durable=False)  # type: ignore[attr-defined]

    try:
        return migration_state.start_fk_revalidation(
            _submit, lambda job_id: _job_live(job_manager, job_id)
        )
    except Exception:  # noqa: BLE001 - reported by the click handler's refresh
        _LOGGER.warning("Could not submit Validate again", exc_info=True)
        return None


def fk_watch_due(migration_state, view: Optional[FkValidationView], job_manager: object) -> bool:
    """Whether a screen should start a watch now: something is recorded, not every key has
    settled (or it was never read in this process), no watch or resubmission runs, and the
    last watch did not end a moment ago. No I/O."""
    if view is None or migration_state is None or job_manager is None:
        return False
    if view.settled:
        return False
    if migration_state.fk_validation_watch_active():
        return False
    if _job_live(job_manager, getattr(migration_state, "fk_revalidation_job_id", None)):
        return False
    return migration_state.fk_validation_watch_rest_over()


def _keys(count: int) -> str:
    """``"1 foreign key"`` / ``"2 foreign keys"``."""
    return f"{count} foreign key" + ("" if count == 1 else "s")


def _names(labels: "Sequence[str]", limit: int = 6) -> str:
    shown = ", ".join(labels[:limit])
    return shown + (f" (+{len(labels) - limit} more)" if len(labels) > limit else "")


def _finish_waits(labels: "Sequence[str]") -> str:
    """The sentence naming the keys Cut over's Finish waits for (empty without any)."""
    if not labels:
        return ""
    return (
        f"Finish waits for Aurora DSQL's check of {_names(labels)}: a MySQL CDC stream can "
        f"leave orphan rows for {'it' if len(labels) == 1 else 'them'} that the final "
        "Validation does not see."
    )


def render_fk_validation(
    ui,
    view: Optional[FkValidationView],
    *,
    on_validate_again: Optional[Callable[[Sequence[tuple[str, str]]], None]] = None,
    revalidating: bool = False,
    holding: "Collection[tuple[str, str]]" = (),
) -> None:
    """The "validated by Aurora DSQL" block under "Apply foreign keys" (both screens). No I/O.

    Before the first read in this process it says it is reading; then: every key validated
    (success), validation still running (info, live), or keys whose validation failed or
    has no job (warning) -- each with Aurora DSQL's details, the orphan count, the query
    that lists the rows, the repair steps, and Validate again -- with the keys still being
    validated named beside them. Keys no longer on the target are named so a reload that
    dropped them is not read as "validated". ``holding``: the keys Cut over's Finish waits
    for (on a MySQL CDC migration, the keys the stream can break, until they validate); the
    block says so for each of them instead of "does not hold the next step".
    """
    if view is None or not view.items:
        return
    from dsql_migrator.ui.design import (
        CODE_SURFACE_CLASSES,
        NOTICE_STYLE,
        inline_hint,
        notice_container,
        render_notice,
    )

    held = set(holding or ())
    in_place = view.in_place
    if view.checking:
        render_notice(
            ui,
            tone="info",
            busy=True,
            header=f"Reading Aurora DSQL's validation of {_keys(in_place)}",
            body=(
                "Each key in place refuses every new write that would break it. Whether the "
                "rows already loaded satisfy it is Aurora DSQL's own check, read back now."
                + (
                    " " + _finish_waits([i.label for i in view.items if i.key in held])
                    if held
                    else ""
                )
            ),
        )
        return
    if view.absent:
        gone = [i.label for i in view.items if i.state == FK_ABSENT]
        render_notice(
            ui,
            tone="warning",
            header=(
                f"{_keys(view.absent)} {'is' if view.absent == 1 else 'are'} no longer on "
                "Aurora DSQL"
            ),
            body=(
                f"{'It was' if view.absent == 1 else 'They were'} put in place, but Aurora "
                f"DSQL no longer has {'it' if view.absent == 1 else 'them'}: {_names(gone)} "
                "(dropping or replacing a table drops its keys). Apply foreign keys again to "
                "add them back."
            ),
        )
    if not in_place:
        return
    problems = view.to_revalidate()
    # Still being validated (or read): beside the failures too, so a mixed outcome does
    # not hide a check that is still running and may still fail.
    running = [item for item in view.items if item.state in (FK_VALIDATING, FK_UNKNOWN)]
    running_held = [item.label for item in running if item.key in held]
    if view.validated == in_place:
        render_notice(
            ui,
            tone="success",
            header=f"All {_keys(in_place)} validated by Aurora DSQL",
            body="The rows already loaded satisfy every key, and each one enforces every new write.",
        )
    elif not problems:
        render_notice(
            ui,
            tone="info",
            busy=bool(view.validating),
            header=(
                f"Aurora DSQL is validating the loaded rows: {view.validated} of "
                f"{_keys(in_place)} validated"
            ),
            body=(
                "Every key already refuses a new write that would break it. Checking the rows "
                "already loaded runs on Aurora DSQL in the background"
                + (
                    ". " + _finish_waits(running_held) + " You can leave this page; the "
                    "result is read back from Aurora DSQL when you return."
                    if running_held
                    else " and does not hold the next step; you can leave this page, the "
                    "result is read back from Aurora DSQL when you return."
                )
                + (
                    f" {view.unknown} could not be read just now; "
                    f"{'it is' if view.unknown == 1 else 'they are'} read again."
                    if view.unknown
                    else ""
                )
            ),
        )
    if not problems:
        return
    failed = [item for item in problems if item.state == FK_FAILED]
    with notice_container(
        ui,
        tone="warning",
        header=(
            f"{len(failed)} of {_keys(in_place)} failed Aurora DSQL's validation"
            if failed
            else f"{len(problems)} of {_keys(in_place)} "
            f"{'is' if len(problems) == 1 else 'are'} not validated"
        ),
        body=(
            "Rows already on Aurora DSQL reference a parent row that is not there. The key "
            "stays in place, NOT VALID: it refuses every new write that would add such a "
            "row, but the existing rows are not proven until it validates."
            if failed
            else f"No validation job runs for {'it' if len(problems) == 1 else 'them'}: it "
            "could not be submitted, or Aurora DSQL no longer reports it (it keeps a "
            "finished job about 30 minutes)."
        ),
    ):
        _border = NOTICE_STYLE["warning"][1]
        for item in problems:
            with ui.column().classes(f"w-full gap-1 pt-2 border-t {_border}"):  # type: ignore[attr-defined]
                ui.label(item.label).classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-900 wrap-anywhere"
                )
                if item.key in held:
                    inline_hint(
                        ui,
                        "Finish waits for this key to validate: a MySQL CDC stream can leave "
                        "orphan rows for it that the final Validation does not see.",
                        tone="warning",
                    )
                if item.state == FK_NOT_VALIDATED:
                    inline_hint(ui, "Not validated: no validation job runs for it.", tone="warning")
                    continue
                ui.label(  # type: ignore[attr-defined]
                    f"Aurora DSQL: {item.details or 'the validation failed (no details reported)'}"
                ).classes("text-xs text-gray-700 wrap-anywhere")
                if item.counting:
                    inline_hint(ui, "Counting the rows that break it.", tone="info")
                elif item.count_error:
                    inline_hint(
                        ui, f"The rows could not be counted ({item.count_error}).", tone="warning"
                    )
                elif item.orphan_count:
                    inline_hint(
                        ui,
                        f"{item.orphan_count:,} child row(s) reference a parent row that is "
                        "not on Aurora DSQL.",
                        tone="warning",
                    )
                else:
                    inline_hint(
                        ui,
                        "No such row is found now (it may have been fixed since).",
                        tone="info",
                    )
                if item.listing_sql:
                    ui.label("List them with:").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-600"
                    )
                    with ui.column().classes(  # type: ignore[attr-defined]
                        f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"
                    ):
                        ui.label(item.listing_sql).classes(  # type: ignore[attr-defined]
                            "text-xs font-mono wrap-anywhere text-gray-800"
                        )
                    if item.listing_start_unknown:
                        inline_hint(
                            ui,
                            "It reads the key window where the count found the first such "
                            "row; that window's start (after_0, ...) is not kept across a "
                            "restart. Validate again counts the rows again and fills it in "
                            "if the validation still fails.",
                            tone="info",
                        )
        if running:
            validating = [item.label for item in running if item.state == FK_VALIDATING]
            unread = [item.label for item in running if item.state == FK_UNKNOWN]
            inline_hint(
                ui,
                " ".join(
                    part
                    for part in (
                        f"Still being validated by Aurora DSQL: {_names(validating)}."
                        if validating
                        else "",
                        f"Not read just now, read again: {_names(unread)}." if unread else "",
                        _finish_waits(running_held),
                    )
                    if part
                ),
                tone="info",
            )
        if failed:
            ui.label(_repair_guidance()).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-700 leading-snug"
            )
        if on_validate_again is not None:
            keys = [item.key for item in problems]
            with ui.row().classes("items-center gap-3 w-full justify-end"):  # type: ignore[attr-defined]
                if revalidating:
                    inline_hint(ui, "Submitting the validation again.", tone="info")
                button = ui.button(  # type: ignore[attr-defined]
                    "Validate again",
                    icon="fact_check",
                    on_click=lambda: on_validate_again(keys),
                ).props("color=primary no-caps")
                if revalidating:
                    button.props("disable")


__all__ = [
    "FK_ABSENT",
    "FK_FAILED",
    "FK_NOT_VALIDATED",
    "FK_UNKNOWN",
    "FK_VALIDATED",
    "FK_VALIDATING",
    "LISTING_START_PLACEHOLDER",
    "REPAIR_BATCH_ROWS",
    "FkValidationItem",
    "FkValidationView",
    "explain_foreign_key_failure",
    "fk_validation_state",
    "fk_validation_view",
    "fk_watch_due",
    "foreign_key_defs",
    "foreign_keys_cdc_can_break",
    "orphan_listing_query",
    "pending_foreign_keys",
    "render_fk_validation",
    "run_fk_validation_watch",
    "sanitize_job_details",
    "start_fk_validation_watch",
    "submit_fk_revalidation",
    "watch_foreign_key_validation",
]
