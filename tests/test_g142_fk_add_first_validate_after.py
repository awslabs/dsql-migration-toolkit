# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G142: the foreign-key pass adds first and lets Aurora DSQL check after.

- Phase B: each preserved key is added ``NOT VALID`` (no row read, seconds inside the write
  freeze; 42710 = already there) after its parent's unique index is valid. No orphan count
  runs before the ADD -- except for a key whose parent side is incomplete on Aurora DSQL by
  design (an excluded referenced column, an unticked parent, an unselected child), which
  keeps the count before its ADD and today's message.
- Right after, every key in place gets ``ALTER TABLE ASYNC ... VALIDATE CONSTRAINT``; the
  job id it returns is recorded on the session (persisted), a key whose recorded job still
  runs keeps it, and 0 rows (already valid) is success.
- The verdict is Aurora DSQL's: a background watch polls ``sys.jobs`` BY job id and
  ``pg_constraint.convalidated`` with backoff and heartbeats, never ``sys.wait_for_job``. A
  failed job shows its details; only then the page-resumable orphan count explains it (the
  count and a literal-free listing query) and "Validate again" resubmits.
- Finish waits only for the keys to be in place, except for the keys a MySQL CDC stream can
  break (a cascade action, a case-insensitive text key): a soft-block "orphans" with the
  existing waiver while their validation failed or still runs.

Doubles only: an in-memory Aurora DSQL that answers the tool's own statements; no AWS.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._fk_validation as fkv
import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    ForeignKeyValidationJob,
    TableDef,
)
from dsql_migrator.core.schema_applier import validate_foreign_key
from dsql_migrator.core.session_state_store import SessionSnapshot
from dsql_migrator.core.target_introspector import (
    ForeignKeyValidationRead,
    foreign_key_validation_states,
)
from dsql_migrator.core.validation_sql import build_orphan_listing_sql
from dsql_migrator.ui.data_migration._state import DataMigrationState

ORDERS = "shop.orders"
LINES = "shop.order_lines"
KEY_ORDERS = (ORDERS, "fk_orders_customer")
KEY_LINES = (LINES, "fk_lines_order")


class _SqlState(Exception):
    def __init__(self, sqlstate: str, message: str = "") -> None:
        super().__init__(message or sqlstate)
        self.sqlstate = sqlstate


def _text(statement) -> str:
    return statement if isinstance(statement, str) else statement.as_string(None)


class _Dsql:
    """An in-memory Aurora DSQL: child rows, parent keys, FK constraints and VALIDATE jobs.

    ``children`` maps a child table to ``(fk column, parent table, {pk: fk value})``. A
    VALIDATE job is ``submitted``, then ``processing``, then ``completed`` (the constraint
    turns convalidated) or ``failed`` (an orphan exists) after ``polls`` reads of
    ``sys.jobs``. ``CALL sys.wait_for_job`` is refused outright: the tool must never wait.
    """

    def __init__(self, *, polls: int = 2) -> None:
        self.parents = {"shop.customers": {1, 2, 3}, ORDERS: {10, 11, 12}}
        self.children = {
            ORDERS: ("customer_id", "shop.customers", {10: 1, 11: 2, 12: 3}),
            LINES: ("order_id", ORDERS, {1: 10, 2: 11, 3: 12}),
        }
        self.constraints: dict = {}  # (table, name) -> convalidated
        self.jobs: dict = {}  # job id -> [key, status, details, reads]
        self.polls = polls
        self.log: list = []
        self.seq = 0

    def connect(self):
        return _Conn(self)

    def orphans(self, table: str) -> int:
        column, parent, rows = self.children[table]
        return sum(1 for value in rows.values() if value is not None and value not in self.parents[parent])

    def statements(self, needle: str) -> list:
        return [s for s in self.log if needle in s]


class _Conn:
    def __init__(self, db: _Dsql) -> None:
        self.db = db

    def cursor(self):
        return _Cur(self.db)

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


_ADD_RE = re.compile(r'ALTER TABLE "([^"]+)"\."([^"]+)" ADD CONSTRAINT "([^"]+)"')
_VALIDATE_RE = re.compile(r'ALTER TABLE ASYNC "([^"]+)"\."([^"]+)" VALIDATE CONSTRAINT "([^"]+)"')
_FROM_RE = re.compile(r'FROM "([^"]+)"\."([^"]+)"')


class _Cur:
    def __init__(self, db: _Dsql) -> None:
        self.db = db
        self.rows: list = []
        self.description = None

    def execute(self, statement, params=None) -> None:
        db, text = self.db, _text(statement)
        db.log.append(text)
        assert "wait_for_job" not in text, "the tool must never wait on a job"
        self.rows, self.description = [], None
        if (match := _ADD_RE.search(text)) is not None:
            key = (f"{match.group(1)}.{match.group(2)}", match.group(3))
            if key in db.constraints:
                raise _SqlState("42710", "constraint already exists")
            db.constraints[key] = False
        elif (match := _VALIDATE_RE.search(text)) is not None:
            key = (f"{match.group(1)}.{match.group(2)}", match.group(3))
            self.description = [("job_id",)]
            if db.constraints.get(key):
                return  # already valid: no job, 0 rows
            db.seq += 1
            job = f"job{db.seq:02d}"
            db.jobs[job] = [key, "submitted", None, 0]
            self.rows = [(job,)]
        elif "FROM sys.jobs" in text:
            for job in params["ids"]:
                entry = db.jobs.get(job)
                if entry is None:
                    continue  # an id Aurora DSQL does not know: no row
                entry[3] += 1
                if entry[1] in ("submitted", "processing"):
                    if entry[3] >= db.polls:
                        if db.orphans(entry[0][0]):
                            entry[1] = "failed"
                            entry[2] = (
                                f'insert or update on table "{entry[0][0].split(".")[1]}" '
                                f'violates foreign key constraint "{entry[0][1]}"'
                            )
                        else:
                            entry[1] = "completed"
                            db.constraints[entry[0]] = True
                    else:
                        entry[1] = "processing"
                self.rows.append((job, entry[1], entry[2]))
        elif "con.convalidated" in text:
            table = f"{params['schema']}.{params['table']}"
            self.rows = [
                (params["schema"], name, valid)
                for (owner, name), valid in db.constraints.items()
                if owner == table
            ]
        elif "ix.indisunique" in text:
            self.rows = [("pk", True, ["id"])]
        elif "indisprimary" in text:
            self.rows = [("id",)]
        elif "array_agg" in text:  # one orphan page of the count (the whole child)
            table = "{}.{}".format(*_FROM_RE.search(text).groups())
            rows = db.children[table][2]
            self.rows = [(db.orphans(table), max(rows), len(rows))]
        else:
            raise AssertionError(f"unexpected statement: {text}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self) -> None:
        pass


def _fk(name, column, parent, *, on_delete=None) -> ForeignKeyDef:
    return ForeignKeyDef(
        name=name, columns=[column], referenced_table=parent, referenced_columns=["id"],
        on_delete=on_delete,
    )


def _ddl(table, fk: ForeignKeyDef) -> str:
    schema, name = table.split(".")
    pschema, pname = fk.referenced_table.split(".")
    return (
        f'ALTER TABLE "{schema}"."{name}" ADD CONSTRAINT "{fk.name}" FOREIGN KEY '
        f'("{fk.columns[0]}") REFERENCES "{pschema}"."{pname}" ("id") NOT VALID'
    )


def _conversions(*, cascade: bool = False) -> dict:
    orders_fk = _fk("fk_orders_customer", "customer_id", "shop.customers",
                    on_delete="CASCADE" if cascade else None)
    lines_fk = _fk("fk_lines_order", "order_id", ORDERS)
    return {
        ORDERS: TableConversion(table=ORDERS, target_ddl="CREATE TABLE x (id int)",
                                foreign_key_ddls=[_ddl(ORDERS, orders_fk)],
                                preserved_foreign_keys=[orders_fk]),
        LINES: TableConversion(table=LINES, target_ddl="CREATE TABLE y (id int)",
                               foreign_key_ddls=[_ddl(LINES, lines_fk)],
                               preserved_foreign_keys=[lines_fk]),
    }


def _pass(db: _Dsql, **kwargs):
    events: list = []
    kwargs.setdefault("on_validation", None)
    original = engine.log_activity
    engine.log_activity = lambda category, action, **kw: events.append((category, action, kw))
    try:
        result = engine.apply_preserved_foreign_keys(_conversions(), db.connect, **kwargs)
    finally:
        engine.log_activity = original
    return result, events


# ---------------------------------------------------------------------------
# 1. Phase B: add first, in seconds, without a count
# ---------------------------------------------------------------------------


def test_phase_b_adds_every_key_without_reading_a_row_and_submits_validate() -> None:
    db = _Dsql()
    db.children[ORDERS][2][13] = 99  # an orphan: it no longer holds the ADD back
    submitted: list = []
    result, events = _pass(db, on_validation=submitted.append)
    assert result == (2, 0, 0)
    assert db.constraints == {KEY_ORDERS: False, KEY_LINES: False}
    # No orphan count, no primary-key read, no scan of a child: per key one unique-index
    # read, one ADD and one VALIDATE -- O(keys) statements, seconds on Aurora DSQL.
    assert not db.statements("array_agg") and not db.statements("indisprimary")
    assert not db.statements("NOT EXISTS")
    assert len(db.statements("ADD CONSTRAINT")) == 2
    assert len(db.statements("VALIDATE CONSTRAINT")) == 2
    # Every ADD comes before the first VALIDATE (the keys enforce before anything is read).
    first_validate = next(i for i, s in enumerate(db.log) if "VALIDATE CONSTRAINT" in s)
    assert all(i < first_validate for i, s in enumerate(db.log) if "ADD CONSTRAINT" in s)
    (sub,) = submitted
    assert set(sub.in_place) == {KEY_ORDERS, KEY_LINES}
    assert sub.jobs == {KEY_ORDERS: "job01", KEY_LINES: "job02"}
    assert not sub.already_valid and not sub.unsubmitted
    outcome = events[-1][2]
    assert outcome["status"] is engine.ActivityStatus.SUCCESS
    assert "2 validation job(s) running" in outcome["detail"]


def test_an_existing_key_is_success_and_an_already_valid_one_needs_no_job() -> None:
    db = _Dsql()
    db.constraints[KEY_ORDERS] = True  # added and validated by an earlier pass
    submitted: list = []
    result, _events = _pass(db, on_validation=submitted.append)
    # 42710 is success; its VALIDATE returns the job_id column with 0 rows: valid already.
    assert result == (2, 0, 0)
    (sub,) = submitted
    assert sub.already_valid == frozenset({KEY_ORDERS})
    assert set(sub.jobs) == {KEY_LINES}
    state = DataMigrationState()
    state.record_fk_validation(sub)
    assert state.fk_validation_record(KEY_ORDERS).status == "completed"
    assert state.fk_validation_record(KEY_ORDERS).job_id is None


def test_a_validate_returning_no_row_is_success_when_the_catalog_could_not_say(
    monkeypatch,
) -> None:
    # The catalog read before the submission failed, so the valid key's VALIDATE is sent:
    # Aurora DSQL answers the job_id column with 0 rows -- valid already, no job, no error.
    db = _Dsql()
    db.constraints[KEY_ORDERS] = True

    def _unreadable(*_a, **_k):
        raise OSError("catalog unreadable")

    monkeypatch.setattr(
        "dsql_migrator.core.target_introspector.foreign_key_validation_states", _unreadable
    )
    submitted: list = []
    result, events = _pass(db, on_validation=submitted.append)
    assert result == (2, 0, 0)
    assert submitted[0].already_valid == frozenset({KEY_ORDERS})
    assert len(db.statements("VALIDATE CONSTRAINT")) == 2  # both sent, one job created
    assert set(submitted[0].jobs) == {KEY_LINES}
    assert events[-1][2]["status"] is engine.ActivityStatus.SUCCESS


def test_a_recorded_job_still_running_is_kept_never_submitted_twice() -> None:
    db = _Dsql(polls=99)
    first: list = []
    _pass(db, on_validation=first.append)
    jobs = dict(first[0].jobs)
    again: list = []
    _pass(db, validation_jobs=jobs, on_validation=again.append)
    # Aurora DSQL does not de-duplicate VALIDATE jobs: the running ones are kept.
    assert again[0].jobs == jobs
    assert len(db.statements("VALIDATE CONSTRAINT")) == 2
    # sys.jobs was read BY the recorded ids, nothing else.
    (read,) = db.statements("FROM sys.jobs")
    assert "job_id = ANY" in read


def test_a_recorded_job_unknown_to_aurora_dsql_is_submitted_again_not_waited_on() -> None:
    db = _Dsql()
    submitted: list = []
    _pass(db, validation_jobs={KEY_ORDERS: "gone-id", KEY_LINES: "gone-id-2"},
          on_validation=submitted.append)
    assert set(submitted[0].jobs.values()) == {"job01", "job02"}
    assert not db.statements("wait_for_job")


def test_a_validate_that_cannot_be_submitted_is_reported_and_the_key_stays() -> None:
    db = _Dsql()
    original = engine.validate_foreign_key

    def _refuse(table, name, *, connection_factory):
        raise _SqlState("42501", "permission denied")

    engine.validate_foreign_key = _refuse
    try:
        submitted: list = []
        result, events = _pass(db, on_validation=submitted.append)
    finally:
        engine.validate_foreign_key = original
    assert result == (2, 0, 0)
    assert set(submitted[0].unsubmitted) == {KEY_ORDERS, KEY_LINES}
    warned = [e for e in events if e[1] == "foreign key validation not submitted"]
    assert len(warned) == 2 and "SQLSTATE 42501" in warned[0][2]["detail"]
    assert events[-1][2]["status"] is engine.ActivityStatus.WARNING


# ---------------------------------------------------------------------------
# 2. The incomplete-parent exception: counted before the ADD
# ---------------------------------------------------------------------------


def test_the_keys_counted_before_their_add_are_the_three_incomplete_parent_cases() -> None:
    conv = _conversions()
    # No selection recorded and no exclusion: every key is added first.
    assert engine.foreign_keys_counted_before_add(conv, None) == {}
    # An unticked parent: the selected child's key into it.
    unticked = engine.foreign_keys_counted_before_add(conv, [ORDERS, LINES])
    assert set(unticked) == {KEY_ORDERS}
    assert "shop.customers is not part of this migration" in unticked[KEY_ORDERS]
    # An unselected child kept for a key into the migration (foreign_keys_pointing_in).
    scoped = engine.foreign_keys_in_scope(conv, [ORDERS, "shop.customers"])
    pointing = engine.foreign_keys_counted_before_add(scoped, [ORDERS, "shop.customers"])
    assert pointing == {KEY_LINES: f"its table {LINES} is not part of this migration"}
    # A B3 referenced column excluded on the parent (baked in once data moved).
    b3 = engine.foreign_keys_counted_before_add(conv, None, {"shop.customers": {"id"}})
    assert set(b3) == {KEY_ORDERS} and "shop.customers.id is excluded" in b3[KEY_ORDERS]


def test_a_counted_first_key_with_orphans_is_not_added_with_todays_message() -> None:
    db = _Dsql()
    db.children[ORDERS][2][13] = 99
    reason = "its parent table shop.customers is not part of this migration"
    result, events = _pass(db, check_before_add={KEY_ORDERS: reason})
    assert result == (1, 1, 0)
    assert KEY_ORDERS not in db.constraints and KEY_LINES in db.constraints
    (line,) = [e for e in events if e[1] == "foreign key not applied"]
    detail = line[2]["detail"]
    assert detail.startswith("1 child row(s) reference a missing parent, so Aurora DSQL "
                             "cannot enforce this foreign key.")
    assert reason in detail
    # Counted, then (being dirty) never added; the other key was added without a count.
    assert len(db.statements("array_agg")) == 1
    assert "fk_orders_customer" not in " ".join(db.statements("ADD CONSTRAINT"))


def test_a_clean_counted_first_key_is_counted_then_added() -> None:
    db = _Dsql()
    _pass(db, check_before_add={KEY_ORDERS: "its parent table is outside"})
    count_at = next(i for i, s in enumerate(db.log) if "array_agg" in s)
    add_at = next(i for i, s in enumerate(db.log) if '"fk_orders_customer" FOREIGN' in s)
    assert count_at < add_at and db.constraints[KEY_ORDERS] is False


# ---------------------------------------------------------------------------
# 3. validate_foreign_key, the catalog read, the listing query
# ---------------------------------------------------------------------------


def test_validate_foreign_key_returns_the_job_id_or_none_when_already_valid() -> None:
    db = _Dsql()
    db.constraints[KEY_ORDERS] = False
    assert validate_foreign_key(ORDERS, "fk_orders_customer", connection_factory=db.connect) == "job01"
    db.constraints[KEY_ORDERS] = True
    assert validate_foreign_key(ORDERS, "fk_orders_customer", connection_factory=db.connect) is None

    class _NoResult:
        description = None

        def execute(self, *_a):
            pass

        def close(self):
            pass

    # PostgreSQL validates synchronously and returns no result: no job, nothing to wait on.
    conn = SimpleNamespace(cursor=lambda: _NoResult(), close=lambda: None)
    assert validate_foreign_key("t", "fk", connection_factory=lambda: conn) is None


def test_the_catalog_read_answers_by_job_id_and_says_what_it_could_not_read() -> None:
    db = _Dsql(polls=1)
    db.constraints[KEY_ORDERS] = False
    db.jobs["j1"] = [KEY_ORDERS, "submitted", None, 0]
    reads = foreign_key_validation_states(
        [KEY_ORDERS, KEY_LINES], {KEY_ORDERS: "j1"}, connection_factory=db.connect
    )
    # The catalog is read before sys.jobs: the job finished between the two reads.
    assert reads[KEY_ORDERS] == ForeignKeyValidationRead(
        present=True, validated=False, job_status="completed", job_details=None, jobs_read=True
    )
    assert reads[KEY_LINES] == ForeignKeyValidationRead(present=False, validated=False)
    # Asked BY id only.
    (jobs_read,) = db.statements("FROM sys.jobs")
    assert "ANY(" in jobs_read
    # A connection that cannot be opened: every key unknown, never "absent".
    assert foreign_key_validation_states(
        [KEY_ORDERS], connection_factory=lambda: (_ for _ in ()).throw(OSError("down"))
    ) == {KEY_ORDERS: None}


def test_an_unreadable_sys_jobs_keeps_the_catalog_answer() -> None:
    db = _Dsql()
    db.constraints[KEY_ORDERS] = False

    class _Conn2(_Conn):
        def cursor(self):
            cur = _Cur(self.db)
            real = cur.execute

            def execute(statement, params=None):
                if "sys.jobs" in _text(statement):
                    raise _SqlState("42501", "permission denied for sys.jobs")
                return real(statement, params)

            cur.execute = execute
            return cur

    reads = foreign_key_validation_states(
        [KEY_ORDERS], {KEY_ORDERS: "j1"}, connection_factory=lambda: _Conn2(db)
    )
    assert reads[KEY_ORDERS].present and not reads[KEY_ORDERS].jobs_read
    record = ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer", job_id="j1",
                                     status="submitted")
    assert fkv.fk_validation_state(reads[KEY_ORDERS], record) == fkv.FK_VALIDATING


def test_the_listing_query_names_tables_and_columns_only() -> None:
    fk = _fk("fk_orders_customer", "customer_id", "shop.customers")
    sql = build_orphan_listing_sql(ORDERS, fk, ["id"]).as_string(None)
    assert sql == (
        'SELECT c."id", c."customer_id" FROM "shop"."orders" AS c LEFT JOIN '
        '"shop"."customers" AS p ON p."id" = c."customer_id" WHERE c."customer_id" IS NOT '
        'NULL AND p."id" IS NULL ORDER BY c."id" LIMIT 100'
    )
    # No value from the data, ever: the only literal is the LIMIT.
    assert "'" not in sql and re.findall(r"\b\d+\b", sql) == ["100"]


def test_job_details_keep_no_key_value() -> None:
    text = ('insert or update on table "orders" violates foreign key constraint "fk"\n'
            "DETAIL:  Key (customer_id)=(12345) is not present in table \"customers\".")
    assert fkv.sanitize_job_details(text) == (
        'insert or update on table "orders" violates foreign key constraint "fk"'
    )
    assert "12345" not in fkv.sanitize_job_details(
        'violates "fk": Key (customer_id)=(12345) is not present'
    )


# ---------------------------------------------------------------------------
# 4. The watch: polled by job id with backoff and heartbeats; a failure explained
# ---------------------------------------------------------------------------


def _recorded(db: _Dsql, state: DataMigrationState) -> None:
    submitted: list = []
    _pass(db, on_validation=submitted.append)
    state.record_fk_validation(submitted[0])


class _Handle:
    def __init__(self) -> None:
        self.beats = 0
        self.cancelled = False

    def heartbeat(self) -> None:
        self.beats += 1


def _watch(db: _Dsql, state: DataMigrationState, events: list, sleeps: list, refreshes=None):
    original = fkv.log_activity
    fkv.log_activity = lambda category, action, **kw: events.append((category, action, kw))
    handle = _Handle()
    try:
        fkv.run_fk_validation_watch(
            state, connection_factory=db.connect,
            fk_defs=lambda: fkv.foreign_key_defs(_conversions()),
            handle=handle, sleep=sleeps.append,
            refresh=(lambda: refreshes.append(1)) if refreshes is not None else None,
        )
    finally:
        fkv.log_activity = original
    return handle


def test_the_watch_polls_by_job_id_with_backoff_until_every_key_validates() -> None:
    db = _Dsql(polls=4)
    state = DataMigrationState()
    _recorded(db, state)
    events, sleeps, refreshes = [], [], []
    handle = _watch(db, state, events, sleeps, refreshes)
    view = state.fk_validation_view()
    assert view.validated == 2 and view.settled and not view.checking
    # Backoff: 2 s, then doubling; one heartbeat per poll. The read that sees the jobs
    # "completed" read convalidated just before they finished, so it waits one more poll
    # for the catalog (a key is validated only when convalidated is true).
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert handle.beats == 5
    # Every sys.jobs read named exactly the recorded job ids.
    ids = {rec.job_id for rec in state.fk_validation_records().values()}
    for read in db.statements("FROM sys.jobs"):
        assert "ANY(" in read
    assert ids == {"job01", "job02"}
    (done,) = [e for e in events if e[1] == "foreign keys validated"]
    assert done[2]["status"] is fkv.ActivityStatus.SUCCESS
    # Refreshed on a change only (the first read, the last), not on every poll.
    assert 1 <= len(refreshes) < 4
    assert not db.statements("wait_for_job")


def test_a_failed_job_is_explained_by_the_orphan_count_and_validated_again() -> None:
    db = _Dsql(polls=1)
    db.children[ORDERS][2][13] = 987654321  # an orphan only the validation finds now
    state = DataMigrationState()
    _recorded(db, state)
    events, sleeps = [], []
    _watch(db, state, events, sleeps)
    record = state.fk_validation_record(KEY_ORDERS)
    assert record.status == "failed"
    assert record.details == ('insert or update on table "orders" violates foreign key '
                              'constraint "fk_orders_customer"')
    assert record.orphan_count == 1
    # The listing reads the count's window holding the first orphan (here the first page,
    # so no window start is needed): one bounded page, never a scan of the whole child.
    assert record.listing_sql.startswith(
        'WITH pg AS MATERIALIZED (SELECT "id", "customer_id" FROM "shop"."orders" ORDER BY'
    )
    assert "LIMIT 5000" in record.listing_sql and "%(after_" not in record.listing_sql
    # The count ran only after the failure, for that key only.
    assert len(db.statements("array_agg")) == 1
    (line,) = [e for e in events if e[1] == "foreign key not validated"]
    detail = line[2]["detail"]
    assert line[2]["target"] == "shop.orders.fk_orders_customer"
    assert "1 child row(s) reference a parent row that is not on Aurora DSQL" in detail
    assert "List them with: WITH pg AS MATERIALIZED" in detail and "3,000 rows" in detail
    assert "Validate again" in detail and "987654321" not in detail
    view = state.fk_validation_view()
    assert view.state_of(KEY_ORDERS) == fkv.FK_FAILED and view.state_of(KEY_LINES) == fkv.FK_VALIDATED
    assert [item.key for item in view.to_revalidate()] == [KEY_ORDERS]

    # The rows are fixed; Validate again resubmits (inline here: no job manager).
    db.parents["shop.customers"].add(987654321)
    fkv.submit_fk_revalidation(state, None, connection_factory=db.connect, keys=[KEY_ORDERS])
    fresh = state.fk_validation_record(KEY_ORDERS)
    assert fresh.job_id == "job03" and fresh.status == "submitted"
    assert fresh.orphan_count is None and fresh.details is None
    # Its last read belonged to the old job: the screen reads again before it says anything.
    assert state.fk_validation_view().checking
    _watch(db, state, [], [])
    assert state.fk_validation_view().validated == 2


def test_the_failure_is_explained_once_per_job_even_across_watches() -> None:
    db = _Dsql(polls=1)
    db.children[ORDERS][2][13] = 99
    state = DataMigrationState()
    _recorded(db, state)
    events: list = []
    _watch(db, state, events, [])
    _watch(db, state, events, [])
    assert len([e for e in events if e[1] == "foreign key not validated"]) == 1
    assert len(db.statements("array_agg")) == 1


def test_a_job_aurora_dsql_no_longer_reports_settles_the_watch_at_once() -> None:
    # Never wait on an id Aurora DSQL does not know: no row -> not validated, the watch
    # stops (Validate again resubmits), and nothing ever calls sys.wait_for_job.
    db = _Dsql()
    db.constraints[KEY_ORDERS] = False
    state = DataMigrationState()
    state.record_fk_validation(engine.ForeignKeyValidationSubmission(
        in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "unknown-id"}))
    sleeps: list = []
    _watch(db, state, [], sleeps)
    assert sleeps == []  # one read, settled
    assert state.fk_validation_view().state_of(KEY_ORDERS) == fkv.FK_NOT_VALIDATED


def test_a_read_in_flight_for_the_old_job_never_lands_on_validate_again() -> None:
    # A watch read asked sys.jobs for the job that failed; meanwhile Validate again recorded
    # a new job. The old failure must not be folded into the new record, nor shown.
    state = DataMigrationState()
    state.record_fk_validation(engine.ForeignKeyValidationSubmission(
        in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j-old"}))
    state.record_fk_revalidation({KEY_ORDERS: "j-new"})
    failed_old = ForeignKeyValidationRead(present=True, validated=False, job_status="failed",
                                          job_details="violates")
    state.set_fk_validation_reads({KEY_ORDERS: failed_old}, {KEY_ORDERS: "j-old"})
    record = state.fk_validation_record(KEY_ORDERS)
    assert record.job_id == "j-new" and record.status == "submitted" and record.details is None
    assert state.fk_validation_view().state_of(KEY_ORDERS) == fkv.FK_UNKNOWN
    # ...and a count explaining the old job is dropped too.
    state.note_fk_explanation(KEY_ORDERS, "j-old", 7, "SELECT 1")
    assert state.fk_validation_record(KEY_ORDERS).orphan_count is None


def test_a_failure_seen_once_stays_failed_after_aurora_dsql_drops_the_job() -> None:
    record = ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer",
                                     job_id="j1", status="failed", details="violates")
    gone = ForeignKeyValidationRead(present=True, validated=False)
    assert fkv.fk_validation_state(gone, record) == fkv.FK_FAILED
    assert fkv.fk_validation_state(None, record) == fkv.FK_UNKNOWN
    assert fkv.fk_validation_state(ForeignKeyValidationRead(present=False, validated=False),
                                   record) == fkv.FK_ABSENT
    completing = ForeignKeyValidationRead(present=True, validated=False, job_status="completed")
    assert fkv.fk_validation_state(completing, record) == fkv.FK_VALIDATING
    valid = ForeignKeyValidationRead(present=True, validated=True, job_status="failed")
    assert fkv.fk_validation_state(valid, record) == fkv.FK_VALIDATED


# ---------------------------------------------------------------------------
# 5. Persisted and read back from the catalog after a restart
# ---------------------------------------------------------------------------


def test_the_record_survives_a_restart_and_the_screen_reads_the_catalog_again() -> None:
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    db = _Dsql(polls=1)
    db.children[ORDERS][2][13] = 987654321
    state = DataMigrationState()
    _recorded(db, state)
    _watch(db, state, [], [])
    snapshot = capture_session_snapshot("s1", SessionConnectionState(), EvaluationState(),
                                        SchemaConversionState(), state)
    payload = snapshot.model_dump_json()
    assert "foreign_key_validation" in payload and "job01" in payload
    assert "987654321" not in payload  # no row value is persisted (Property 7)
    restored = DataMigrationState()
    apply_session_snapshot(SessionSnapshot.model_validate_json(payload),
                           SessionConnectionState(), EvaluationState(), SchemaConversionState(),
                           restored)
    assert restored.fk_validation_records() == state.fk_validation_records()
    view = restored.fk_validation_view()
    # Never trusted from the snapshot: not read yet in this process.
    assert view.checking and not view.settled
    jobs = SimpleNamespace(submitted=[], get_status=lambda job_id: SimpleNamespace(status="DONE"))

    def _submit(work, **_kw):
        jobs.submitted.append(work)
        return "watch-1"

    jobs.submit = _submit
    assert fkv.fk_watch_due(restored, view, jobs)
    fkv.start_fk_validation_watch(restored, jobs, connection_factory=db.connect,
                                  fk_defs=lambda: fkv.foreign_key_defs(_conversions()))
    assert restored.fk_validation_job_id == "watch-1"
    jobs.submitted[0](_Handle())  # the watch reads Aurora DSQL
    view = restored.fk_validation_view()
    assert view.state_of(KEY_ORDERS) == fkv.FK_FAILED and view.state_of(KEY_LINES) == fkv.FK_VALIDATED
    # A failure explained before the restart is not counted again.
    assert len(db.statements("array_agg")) == 1


def test_an_empty_record_is_left_out_of_the_payload() -> None:
    assert "foreign_key_validation" not in SessionSnapshot(session_id="s").model_dump_json()
    assert "orphan_count" not in ForeignKeyValidationJob(
        table="t", constraint="c", job_id="j").model_dump_json()


def test_a_schema_replace_forgets_the_record_and_start_over_resets_it() -> None:
    state = DataMigrationState()
    state.record_fk_validation(engine.ForeignKeyValidationSubmission(
        in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j"}))
    state.clear_fk_validation()
    assert state.fk_validation_view() is None
    import inspect

    import dsql_migrator.ui.app as app

    assert "_dm.clear_fk_validation()" in inspect.getsource(app._forget_applied_foreign_keys)


# ---------------------------------------------------------------------------
# 6. Finish: in place is enough, except the keys a MySQL CDC stream can break
# ---------------------------------------------------------------------------


def test_the_gate_waits_for_the_keys_in_place_not_for_their_validation() -> None:
    from dsql_migrator.ui.validation import cutover_finish_fk_gate

    def gate(**kw):
        base = dict(cdc_in_use=True, fk_pending_count=3, fk_apply_result=None,
                    proceed_without_fks=False)
        base.update(kw)
        return cutover_finish_fk_gate(**base)

    assert gate() == "pending"
    assert gate(fk_apply_result=(2, 1, 0)) == "incomplete"
    assert gate(fk_apply_result=(3, 0, 0)) is None  # validation still running: not held
    # A catalog read showing every key present opens it after a Validation re-run or a
    # restart cleared this session's result.
    assert gate(fks_in_place=True) is None
    assert gate(proceed_without_fks=True) is None and gate(cdc_in_use=False) is None
    # The keys a MySQL CDC stream can break are a second hold, on the keys in place: held
    # while not validated (a key not recorded or gone is the first hold's).
    from dsql_migrator.ui.validation import cutover_fk_held

    view = _view({KEY_ORDERS: "failed", KEY_LINES: "absent"})
    extra = (ORDERS, "fk_never_added")
    assert cutover_fk_held({KEY_ORDERS, KEY_LINES, extra}, view) == {KEY_ORDERS: "failed"}
    assert cutover_fk_held({KEY_LINES}, _view({KEY_LINES: "validated"})) == {}
    assert cutover_fk_held({KEY_ORDERS}, None) == {}


def test_the_keys_cdc_can_break_are_cascades_and_case_insensitive_text_keys() -> None:
    conv = _conversions(cascade=True)
    tables = [
        TableDef(name=ORDERS, columns=[ColumnDef(name="id", mysql_type="int"),
                                        ColumnDef(name="customer_id", mysql_type="int")],
                 primary_key=["id"]),
        TableDef(name=LINES, columns=[ColumnDef(name="id", mysql_type="int"),
                                       ColumnDef(name="order_id", mysql_type="int")],
                 primary_key=["id"]),
    ]
    assert fkv.foreign_keys_cdc_can_break(conv, tables) == frozenset({KEY_ORDERS})
    # A text key under a *_ci collation, on the parent's side.
    tables.append(TableDef(name="shop.customers", columns=[
        ColumnDef(name="id", mysql_type="varchar(10)", collation="utf8mb4_0900_ai_ci")],
        primary_key=["id"]))
    plain = _conversions()
    assert fkv.foreign_keys_cdc_can_break(plain, tables) == frozenset({KEY_ORDERS})
    # ...or the child's.
    tables[1] = TableDef(name=LINES, columns=[
        ColumnDef(name="id", mysql_type="int"),
        ColumnDef(name="order_id", mysql_type="varchar(10)", collation="latin1_swedish_ci")],
        primary_key=["id"])
    assert fkv.foreign_keys_cdc_can_break(plain, tables) == frozenset({KEY_ORDERS, KEY_LINES})
    # Without the table definitions there is no collation to read: nothing is held.
    assert fkv.foreign_keys_cdc_can_break(plain, [tables[0]]) == frozenset()


def _cutover_inputs(*, source_type, cascade=True):
    from dsql_migrator.core.models import (
        SourceConnectionConfig,
        SourceInventory,
        TargetConnectionConfig,
    )

    fk = ForeignKeyDef(name="fk_o_c", columns=["cid"], referenced_table="shop.customers",
                       referenced_columns=["id"], on_delete="CASCADE" if cascade else None)
    inv = SourceInventory(tables=[
        TableDef(name="shop.customers", columns=[ColumnDef(name="id", mysql_type="int")],
                 primary_key=["id"]),
        TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="int"),
                                               ColumnDef(name="cid", mysql_type="int")],
                 primary_key=["id"], foreign_keys=[fk]),
    ])
    session = SimpleNamespace(
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        aws_profile=None,
        source_config=SourceConnectionConfig(host="h", database="d", source_type=source_type),
    )
    eval_store = SimpleNamespace(get_or_create=lambda _sid: SimpleNamespace(
        result=SimpleNamespace(inventory=inv)))
    conv_store = SimpleNamespace(get=lambda _sid: SimpleNamespace(
        edited_target_ddls={}, preserve_foreign_keys=True))
    return session, eval_store, conv_store


def test_only_a_mysql_cdc_migration_holds_finish_on_those_keys() -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.validation import _cutover_fk_keys

    session, eval_store, conv_store = _cutover_inputs(source_type=SourceType.MYSQL)
    keys, breakable, defs = _cutover_fk_keys(session, eval_store, conv_store, "s",
                                             cdc_in_use=True)
    assert keys == [("shop.orders", "fk_o_c")] and breakable == frozenset(keys)
    assert set(defs) == set(keys)
    assert _cutover_fk_keys(session, eval_store, conv_store, "s", cdc_in_use=False)[1] == frozenset()
    pg, eval_pg, conv_pg = _cutover_inputs(source_type=SourceType.POSTGRES)
    # PostgreSQL logical decoding carries a cascade's child changes.
    assert _cutover_fk_keys(pg, eval_pg, conv_pg, "s", cdc_in_use=True)[1] == frozenset()


def test_the_ack_line_names_the_keys_aurora_dsql_has_not_validated() -> None:
    from dsql_migrator.ui.validation import ValidationState, _cutover_integrity_detail

    db = _Dsql(polls=1)
    db.children[ORDERS][2][13] = 99
    state = DataMigrationState()
    _recorded(db, state)
    view = state.fk_validation_view()
    vstate = ValidationState()
    vstate.set_cutover_fk_apply((2, 0, 0))
    assert "validated by Aurora DSQL: NOT KNOWN" in _cutover_integrity_detail(vstate, 2, fk_view=view)
    _watch(db, state, [], [])
    detail = _cutover_integrity_detail(vstate, 2, fk_view=state.fk_validation_view())
    assert "foreign keys 2/2 applied; validated by Aurora DSQL: 1 of 2" in detail
    assert "1 NOT validated: shop.orders.fk_orders_customer" in detail
    # A Validation re-run cleared this session's result: the catalog still says in place.
    detail = _cutover_integrity_detail(ValidationState(), 2, fk_view=state.fk_validation_view())
    assert "foreign keys 2/2 in place" in detail and "NOT applied" not in detail
    waived = ValidationState()
    waived.waive_fk_validation([KEY_ORDERS])
    detail = _cutover_integrity_detail(waived, 2, fk_view=state.fk_validation_view())
    assert "foreign keys 2/2 in place, validation check WAIVED" in detail
    assert "fk_orders_customer" in detail
    # Waiving the keys not in place, with every key in place, waives nothing.
    waived = ValidationState()
    waived.waive_foreign_keys()
    detail = _cutover_integrity_detail(waived, 2, fk_view=state.fk_validation_view())
    assert "WAIVED" not in detail


def test_the_cut_over_screen_holds_finish_on_an_unvalidated_cascade_key(monkeypatch) -> None:
    import sys
    import types

    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import (
        SourceType,
        TableValidationResult,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen
    from tests.test_cutover_defaults_ui import _Ui

    session_obj, eval_store, conv_store = _cutover_inputs(source_type=SourceType.MYSQL)
    ui = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: True)
    notices: list = []
    monkeypatch.setattr(uiv, "render_notice", lambda _ui, **kw: (
        notices.append(kw), ui.texts.append(kw.get("header", "")), ui.texts.append(kw.get("body", ""))))
    logged: list = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw)))
    sessions = SessionStore()
    live = sessions.get_or_create("s1")
    live.set_target(session_obj.target_config)
    live.set_source(session_obj.source_config, None)
    vstore, mstore = ValidationStore(), DataMigrationStore()
    content, _runner = build_cutover_screen(
        sessions, "s1", validation_store=vstore, migration_store=mstore,
        eval_store=eval_store, conversion_store=conv_store,
    )
    vstate = vstore.get_or_create("s1")
    vstate.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[
        TableValidationResult(table="shop.orders", source_row_count=1, target_row_count=1,
                              row_count_match=True, matched=True)]))
    vstate.set_cutover_fk_apply((1, 0, 0))
    mstate = mstore.get_or_create("s1")
    mstate.record_fk_validation(engine.ForeignKeyValidationSubmission(
        in_place=(("shop.orders", "fk_o_c"),), jobs={("shop.orders", "fk_o_c"): "j1"}))
    mstate.set_fk_validation_reads({("shop.orders", "fk_o_c"): ForeignKeyValidationRead(
        present=True, validated=False, job_status="processing")},
        {("shop.orders", "fk_o_c"): "j1"})
    content(lambda: None)
    body = ui.body()
    assert "Waiting for Aurora DSQL to validate the foreign keys CDC can break" in body
    assert "shop.orders.fk_o_c" in body and "still running" in body
    ack = [b for b in ui.buttons if b.text == "I've cut over to DSQL"]
    assert ack and ack[0].disabled
    (waive,) = [b for b in ui.buttons if b.text == "Cut over without this check"]
    waive.on_click()
    (line,) = [kw for action, kw in logged if action == "foreign keys waived at cut over"]
    assert "1 foreign key(s) Aurora DSQL has NOT validated (shop.orders.fk_o_c)" in line["detail"]
    assert vstate.proceed_without_fk_validation == frozenset({("shop.orders", "fk_o_c")})
    assert not vstate.proceed_without_foreign_keys  # the other hold's waiver is its own

    # Validated: Finish opens without any waiver.
    vstate.proceed_without_fk_validation = frozenset()
    mstate.set_fk_validation_reads({("shop.orders", "fk_o_c"): ForeignKeyValidationRead(
        present=True, validated=True)}, {("shop.orders", "fk_o_c"): "j1"})
    ui.texts.clear()
    ui.buttons.clear()
    content(lambda: None)
    ack = [b for b in ui.buttons if b.text == "I've cut over to DSQL"]
    assert ack and not ack[0].disabled
    assert "All 1 foreign key validated by Aurora DSQL" in ui.body()


# ---------------------------------------------------------------------------
# 7. The screens
# ---------------------------------------------------------------------------


class _RenderUi:
    def __init__(self) -> None:
        self.texts: list = []
        self.buttons: list = []
        self.spinners = 0

    class _El:
        def __init__(self, ui, text="", on_click=None):
            self.ui, self.text, self.on_click, self.disabled = ui, text, on_click, False

        def props(self, value=None, remove=None, **_k):
            if value and "disable" in str(value).split():
                self.disabled = True
            return self

        def classes(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return _RenderUi._El(self, str(text))

    def button(self, text="", *_a, on_click=None, **_k):
        el = _RenderUi._El(self, str(text), on_click)
        self.buttons.append(el)
        return el

    def spinner(self, *_a, **_k):
        self.spinners += 1
        return _RenderUi._El(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _RenderUi._El(self)

    def body(self) -> str:
        return " ".join(self.texts)


def _view(states: dict, records: dict | None = None):
    records = records or {key: ForeignKeyValidationJob(table=key[0], constraint=key[1],
                                                       job_id="j", status="submitted")
                          for key in states}
    reads = {
        key: {
            "validated": ForeignKeyValidationRead(present=True, validated=True),
            "validating": ForeignKeyValidationRead(present=True, validated=False,
                                                   job_status="processing"),
            "failed": ForeignKeyValidationRead(present=True, validated=False, job_status="failed"),
            "absent": ForeignKeyValidationRead(present=False, validated=False),
        }[state]
        for key, state in states.items()
    }
    from datetime import datetime, timezone

    return fkv.fk_validation_view(records, reads, datetime.now(timezone.utc))


def test_the_block_before_during_and_after() -> None:
    ui = _RenderUi()
    checking = fkv.fk_validation_view(
        {KEY_ORDERS: ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer")},
        {}, None)
    fkv.render_fk_validation(ui, checking)
    assert "Reading Aurora DSQL's validation of 1 foreign key" in ui.body() and ui.spinners == 1

    ui = _RenderUi()
    fkv.render_fk_validation(ui, _view({KEY_ORDERS: "validating", KEY_LINES: "validated"}))
    assert "validating the loaded rows: 1 of 2 foreign keys validated" in ui.body()
    assert "does not hold the next step" in ui.body() and ui.spinners == 1
    assert not ui.buttons

    ui = _RenderUi()
    fkv.render_fk_validation(ui, _view({KEY_ORDERS: "validated", KEY_LINES: "validated"}))
    assert "All 2 foreign keys validated by Aurora DSQL" in ui.body()


def test_a_failed_key_shows_details_count_listing_and_validate_again() -> None:
    record = ForeignKeyValidationJob(
        table=ORDERS, constraint="fk_orders_customer", job_id="j", status="failed",
        details='violates foreign key constraint "fk_orders_customer"', orphan_count=1234,
        listing_sql='SELECT c."id" FROM "shop"."orders" AS c LIMIT 100')
    view = _view({KEY_ORDERS: "failed", KEY_LINES: "validated"},
                 {KEY_ORDERS: record, KEY_LINES: ForeignKeyValidationJob(
                     table=LINES, constraint="fk_lines_order", status="completed")})
    clicked: list = []
    ui = _RenderUi()
    fkv.render_fk_validation(ui, view, on_validate_again=clicked.append)
    body = ui.body()
    assert "1 of 2 foreign keys failed Aurora DSQL's validation" in body
    assert "Aurora DSQL: violates foreign key constraint" in body
    assert "1,234 child row(s) reference a parent row that is not on Aurora DSQL." in body
    assert 'SELECT c."id" FROM "shop"."orders" AS c LIMIT 100' in body
    assert "batches of at most 3,000 rows" in body
    (again,) = [b for b in ui.buttons if b.text == "Validate again"]
    again.on_click()
    assert clicked == [[KEY_ORDERS]]
    ui = _RenderUi()
    fkv.render_fk_validation(ui, view, on_validate_again=clicked.append, revalidating=True)
    assert [b for b in ui.buttons if b.text == "Validate again"][0].disabled
    # Counting still running: said so, never "0 rows".
    counting = _view({KEY_ORDERS: "failed"}, {KEY_ORDERS: record.model_copy(
        update={"orphan_count": None})})
    ui = _RenderUi()
    fkv.render_fk_validation(ui, counting)
    assert "Counting the rows that break it" in ui.body()


def test_a_key_gone_from_the_target_is_named_not_counted_as_validated() -> None:
    ui = _RenderUi()
    fkv.render_fk_validation(ui, _view({KEY_ORDERS: "absent", KEY_LINES: "validated"}))
    assert "1 foreign key is no longer on Aurora DSQL" in ui.body()
    # Worded without guessing why it went (the tool's own "Remove foreign keys" leaves the
    # record instead, so it is never reported here).
    assert "A reload or a schema replace dropped them" not in ui.body()
    assert "All 1 foreign key validated by Aurora DSQL" in ui.body()


def test_the_cut_over_block_shows_the_pass_running_then_the_validation() -> None:
    from dsql_migrator.ui.validation import _render_cutover_section
    from tests.test_ui_validation import _cutover_summary, _no_drift

    ui = _RenderUi()
    _render_cutover_section(
        ui, _cutover_summary(), _no_drift(), cdc_in_use=True,
        fk_apply_provider=lambda: None, fk_pending_count=3,
        fk_apply_running=True, fk_apply_progress=(1, 3), cdc_drained_confirmed=True,
    )
    assert "Adding foreign keys: 1 of 3" in ui.body()
    assert [b for b in ui.buttons if b.text == "Apply foreign keys"][0].disabled
    assert "Each key is added at once, NOT VALID, so this takes seconds" in ui.body()

    ui = _RenderUi()
    _render_cutover_section(
        ui, _cutover_summary(), _no_drift(), cdc_in_use=True,
        fk_apply_provider=lambda: None, fk_pending_count=2, fk_apply_result=(2, 0, 0),
        cdc_drained_confirmed=True,
        fk_validation=_view({KEY_ORDERS: "validating", KEY_LINES: "validated"}),
        on_validate_again=lambda keys: None,
    )
    assert "Applied 2 foreign key(s): in place, enforcing every new write." in ui.body()
    assert "1 of 2 foreign keys validated" in ui.body()


def test_the_full_load_card_wires_the_same_engine() -> None:
    import inspect

    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as flu

    src = inspect.getsource(dm.build_data_migration_screen)
    action = src[src.index("def apply_foreign_keys_now("):]
    action = action[:action.index("def _fk_apply_running(")]
    assert "on_validation=migration_state.record_fk_validation" in action
    assert "watch_foreign_key_validation(force=True)" in action
    assert "foreign_keys_validation=migration_state.fk_validation_view," in src
    assert "validate_foreign_keys_again=validate_foreign_keys_again," in src
    assert "fk_counted_before_add=foreign_keys_counted_before_add(" in src
    card = inspect.getsource(flu._render_full_load_step)
    assert "render_fk_validation(" in card


def test_no_code_path_waits_on_a_job_or_claims_a_failure_is_unobservable() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "unobservable" not in text, path
        for line in text.splitlines():
            stripped = line.strip()
            if "wait_for_job" in stripped and not stripped.startswith("#"):
                # Only prose (docstrings) may name it, to say it is never called.
                assert "never" in stripped or "blocks" in stripped, (path, stripped)


# ---------------------------------------------------------------------------
# 8. The manual, in three languages
# ---------------------------------------------------------------------------


def test_the_manual_says_add_first_validate_after_in_three_languages() -> None:
    # Chapter 2 is a user guide (2026-10-25): the job and constraint columns Aurora DSQL
    # reports (sys.jobs, convalidated) are in the CHANGELOG, not the manual.
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual"
    needles = {
        "en": {
            "02-evaluation-and-schema-conversion.md": (
                "**Added first, validated after.**", "without reading a row", "**When a validation fails.**", "at most 3,000 rows",
                "**Validate again**", "**Keys counted before they are added.**",
                "explicit **Apply foreign keys** action"),
            "06-limitations.md": ("stays `NOT VALID`", "**Validate again**"),
            "10-conclusion.md": ("**Apply foreign keys, then repoint the application**",
                                 "text column MySQL matches more loosely"),
            "05-validation.md": ("**early warning**",),
            "04-cdc-and-dsql-constraints.md": ("**Foreign keys (applied at cut over, after the drain)**",
                                               "`NOT VALID` at once"),
        },
        "ko": {
            "02-evaluation-and-schema-conversion.md": (
                "**먼저 추가하고, 나중에 검증합니다.**", "행을 하나도 읽지 않고", "**검증이 실패하면.**", "최대 3,000행", "**Validate again**",
                "**추가하기 전에 세는 키.**"),
            "06-limitations.md": ("`NOT VALID`로 남음", "**Validate again**"),
            "10-conclusion.md": ("**외래 키를 적용한 뒤 애플리케이션을 DSQL 엔드포인트로 전환**",),
            "05-validation.md": ("**조기 경고**",),
            "04-cdc-and-dsql-constraints.md": ("**외래 키(드레인 후 cut over에서 적용)**",),
        },
        "ja": {
            "02-evaluation-and-schema-conversion.md": (
                "**先に追加し、後で検証する。**", "行を 1 行も読まずに", "**検証が失敗したとき。**", "最大 3,000 行",
                "**Validate again**", "**追加する前に数えるキー。**"),
            "06-limitations.md": ("`NOT VALID` のまま残ります", "**Validate again**"),
            "10-conclusion.md": ("**外部キーを適用してから、アプリケーションを再ポイント**",),
            "05-validation.md": ("**早期警告**",),
            "04-cdc-and-dsql-constraints.md": ("**外部キー (ドレイン後のカットオーバーで適用)**",),
        },
    }
    stale = ("Orphan pre-gate", "pre-apply", "사전 게이트", "적용 전\n게이트", "事前ゲート",
             "適用前ゲート", "automatically, as a run-level pass", "block the cut-over foreign-key",
             "re-checks every key")
    for lang, files in needles.items():
        for name, phrases in files.items():
            text = (manual / lang / name).read_text(encoding="utf-8")
            flat = " ".join(text.split())
            for phrase in phrases:
                assert " ".join(phrase.split()) in flat, (lang, name, phrase)
        for path in (manual / lang).glob("*.md"):
            text = path.read_text(encoding="utf-8")
            for phrase in stale:
                assert phrase not in text, (lang, path.name, phrase)
