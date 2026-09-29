# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The overlay ledger (core/overlay_ledger.py): the record rules every writer shares, the
append verdict, the three stores' tri-state reads and verified writes, the per-key lock
under concurrent writers, the run view, and isolation from the session / job prunes."""

import hashlib
import json
import sqlite3
import threading
import time
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dsql_migrator.core.column_exclusion import set_digest
from dsql_migrator.core.models import TargetConnectionConfig
from dsql_migrator.core.overlay_ledger import (
    APPEND_REFUSAL_TEXT,
    LEDGER_PREFIX,
    InMemoryOverlayLedger,
    LedgerDecision,
    LedgerRead,
    OverlayLedgerColumn,
    OverlayLedgerError,
    OverlayLedgerTable,
    OverlayLedgerUnavailableError,
    OverlayLedgerView,
    S3OverlayLedger,
    SqliteOverlayLedger,
    append_accepted,
    append_refusal,
    apply_restore_results,
    ddl_digest,
    ledger_lock,
    overlay_ledger_key,
    plan_ledger_record,
    recorded_ddl_fields,
    recorded_default_mismatches,
    reincluded_columns,
)
from dsql_migrator.core.target_introspector import ColumnNullFacts as F

from tests.test_job_store import _S3Error
from tests.test_job_store import _FakeS3 as _JobFakeS3

T0 = datetime(2026, 9, 29, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 9, 29, 1, 0, tzinfo=timezone.utc)
DDL1 = "CREATE TABLE shop.docs (id INT PRIMARY KEY, a TEXT NOT NULL DEFAULT 'x', b TEXT DEFAULT 'y', n INT NOT NULL)"
DDL2 = "CREATE TABLE shop.docs (id INT PRIMARY KEY, a TEXT NOT NULL, b TEXT DEFAULT 'y', n INT NOT NULL)"
NO_SLEEP = {"sleep": lambda _s: None}


def _plan(existing, facts, *, has_rows=True, just_recreated=False, ddl=DDL1, now=T1):
    return plan_ledger_record(existing, table="shop.docs", overlay_facts=facts, has_rows=has_rows,
                              just_recreated=just_recreated, effective_ddl=ddl, now=now)


def _rec(**cols):
    """A record for shop.docs with ``cols`` = {name: original_default or None}."""
    names = sorted(cols)
    return OverlayLedgerTable(
        table="shop.docs", set_digest=set_digest(names), set_columns=names, empty_when_applied=True,
        applied_at=T0,
        columns=[OverlayLedgerColumn(column=c, original_default=d, recorded_ddl_digest=ddl_digest(DDL1),
                                     recorded_ddl_state="declared" if d else "none",
                                     recorded_ddl_default=d, dropped_not_null=True) for c, d in sorted(cols.items())])


# ---------------------------------------------------------------------------
# Models, key, pure helpers
# ---------------------------------------------------------------------------


def test_models_forbid_extra_fields_so_no_table_level_ddl_digest_can_creep_in():
    with pytest.raises(ValidationError):
        OverlayLedgerTable.model_validate({"table": "t", "set_digest": "d", "applied_at": T0, "ddl_digest": "x"})
    with pytest.raises(ValidationError):
        OverlayLedgerColumn.model_validate({"column": "c", "surprise": 1})
    assert OverlayLedgerUnavailableError().args[0] == OverlayLedgerUnavailableError.MESSAGE


def test_ledger_key_is_the_normalized_target():
    def key(endpoint, database="postgres"):
        return overlay_ledger_key(TargetConnectionConfig(cluster_endpoint=endpoint, region="us-east-1",
                                                         database=database))

    k = key("abc.dsql.us-east-1.on.aws")
    assert k == key("  ABC.dsql.us-east-1.on.aws. ") and len(k) == 64 and int(k, 16) >= 0
    assert key("abc.dsql.us-east-1.on.aws", "other") != k


def test_recorded_ddl_fields_states():
    digest = hashlib.sha256(DDL1.encode()).hexdigest()
    assert recorded_ddl_fields(DDL1, "a") == (digest, "declared", "'x'")
    assert recorded_ddl_fields(DDL1, "n") == (digest, "none", None)
    assert recorded_ddl_fields(DDL1, "missing") == (digest, "unreadable", None)
    assert recorded_ddl_fields("garbage(", "a") == (ddl_digest("garbage("), "unreadable", None)
    assert recorded_ddl_fields(None, "a") == (None, "unreadable", None)


# ---------------------------------------------------------------------------
# plan_ledger_record, rules (a)-(e)
# ---------------------------------------------------------------------------


def test_first_drop_records_default_ddl_and_set():
    d = _plan(None, {"a": F(True, "'x'::text"), "n": F(True, None)}, has_rows=False)
    assert d.kind == "write"
    r = d.record
    assert r.set_columns == ["a", "n"] and r.set_digest == set_digest(["a", "n"]) and r.empty_when_applied
    a, n = r.column("a"), r.column("n")
    assert (a.original_default, a.recorded_ddl_state, a.recorded_ddl_default, a.dropped_not_null) == \
        ("'x'::text", "declared", "'x'", True)
    # A NOT-NULL-only column: an entry with no DEFAULT (never restored), dropped_not_null True.
    assert n.original_default is None and n.dropped_not_null is True


def test_empty_when_applied_is_never_downgraded_for_the_same_digest():
    existing = _rec(a="'x'::text")
    d = _plan(existing, {"a": F(False, None)}, has_rows=True)  # an append, same set
    assert d.record.empty_when_applied is True and d.record.set_digest == existing.set_digest
    other = _plan(existing, {"a": F(False, None), "b": F(False, "'y'::text")}, has_rows=True)
    assert other.record.empty_when_applied is False  # a different set on an append (exempt path)


def test_a_re_drop_rewrites_only_that_column_as_one_unit():
    existing = _rec(a="'x'::text", b="'y'::text")
    existing = existing.model_copy(update={"columns": [
        existing.column("a").model_copy(update={"restored_at": T0, "restore_error": "boom"}),
        existing.column("b")]})
    d = _plan(existing, {"a": F(True, "'z'::text"), "b": F(False, None)}, just_recreated=False, ddl=DDL2)
    a = d.record.column("a")
    assert a.original_default == "'z'::text" and a.restored_at is None and a.restore_error is None
    assert (a.recorded_ddl_digest, a.recorded_ddl_state, a.recorded_ddl_default) == (ddl_digest(DDL2), "none", None)
    # The finding-5 sibling: a live column with no DEFAULT keeps its record intact.
    assert d.record.column("b") == existing.column("b")


def test_no_live_default_keeps_the_owed_record_and_never_touches_recorded_ddl():
    existing = _rec(a="'x'::text")
    d = _plan(existing, {"a": F(False, None)}, ddl=DDL2)
    before, after = existing.column("a"), d.record.column("a")
    assert after == before  # recorded_ddl_* untouched by a reconcile under a new DDL


def test_recreated_prunes_entries_no_longer_overlaid_and_delete_when_nothing_is_left():
    existing = _rec(a="'x'::text", b="'y'::text")
    d = _plan(existing, {"b": F(True, "'y'::text")}, just_recreated=True)
    assert [c.column for c in d.record.columns] == ["b"] and d.record.set_columns == ["b"]
    assert _plan(existing, {}, just_recreated=True).kind == "delete"  # recreated, empty set


def test_recorded_only_table_with_rows_is_unchanged():
    existing = _rec(a="'x'::text")
    d = _plan(existing, {}, has_rows=True, just_recreated=False)
    assert d == LedgerDecision("unchanged", existing)


def test_empty_not_recreated_table_with_an_owed_entry_is_rewritten_to_the_empty_set():
    existing = _rec(a="'x'::text")
    d = _plan(existing, {}, has_rows=False, just_recreated=False)
    assert d.kind == "write"
    assert d.record.set_columns == [] and d.record.set_digest == hashlib.sha256(b"v1|").hexdigest()
    assert d.record.empty_when_applied is True and d.record.column("a") == existing.column("a")


# ---------------------------------------------------------------------------
# append_refusal matrix + the other shared predicates
# ---------------------------------------------------------------------------


def _refusal(record, overlay_present=(), present_live=(), omitted=(), has_rows=True, exempt=False):
    return append_refusal(record, has_rows=has_rows, exempt=exempt, overlay_present=list(overlay_present),
                          present_live=list(present_live), omitted=list(omitted))


def test_append_refusal_matrix():
    same = _rec(a="'x'::text")
    assert _refusal(None, ["a"]).kind == "overlay"                       # no record
    assert _refusal(same, ["a"], ["a"], ["a"]) is None                   # same digest, all rows under it
    assert _refusal(same, ["a", "b"], ["a", "b"], ["a", "b"]).columns == ("a", "b")  # different digest
    assert _refusal(same.model_copy(update={"empty_when_applied": False}), ["a"]).kind == "overlay"
    back = _refusal(same, [], present_live=["a"], omitted=[])            # empty set, 'a' re-included
    assert back.kind == "reincluded" and back.text() == APPEND_REFUSAL_TEXT.format(columns="a")
    assert _refusal(same, [], present_live=[], omitted=[]) is None       # 'a' gone live
    assert _refusal(same, [], present_live=["a"], omitted=["a"]) is None  # still omitted (legacy)
    assert _refusal(None, ["a"], exempt=True) is None
    assert _refusal(None, ["a"], has_rows=False) is None
    assert append_accepted(same, ["a"]) and not append_accepted(None, [])
    assert reincluded_columns(None, present_live=["a"], omitted=[]) == ()


def test_recorded_default_mismatches_and_restore_results():
    facts = {"a": F(False, "'x'::text"), "b": F(False, "'new'::text"), "c": F(False, None), "d": None}
    assert recorded_default_mismatches(facts, ["a", "b", "c", "d"], {"a": "'x'::text", "b": "'old'::text"}) == ("b",)
    record = _rec(a="'x'::text", b="'y'::text", c="'z'::text")
    out = apply_restore_results(record, {"a": ("restored", None), "b": ("failed", "42501 denied"),
                                         "c": ("gone", None)}, T1)
    assert out.column("a").restored_at == T1 and out.column("a").restore_error is None
    assert out.column("b").restore_error == "42501 denied" and out.column("b").restored_at is None
    assert out.column("c") is None and out.table == record.table  # gone is pruned, record kept


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


class _S3:
    """An S3 client double with per-call fault injection."""

    def __init__(self):
        self.objects = {}
        self.get_errors, self.put_errors, self.puts = [], [], 0
        self.tamper = None

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if self.get_errors:
            raise self.get_errors.pop(0)
        if Key not in self.objects:
            raise _S3Error("NoSuchKey")
        body = self.objects[Key]
        return {"Body": type("B", (), {"read": lambda _self: body})()}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.puts += 1
        if self.put_errors:
            raise self.put_errors.pop(0)
        self.objects[Key] = self.tamper(Body) if self.tamper else Body


def _stores(tmp_path):
    return [InMemoryOverlayLedger(**NO_SLEEP), SqliteOverlayLedger(str(tmp_path / "s.sqlite"), **NO_SLEEP),
            S3OverlayLedger("b", s3_client=_S3(), **NO_SLEEP)]


def test_every_store_round_trips_deletes_and_reads_zero_tables_as_absent(tmp_path):
    for store in _stores(tmp_path):
        assert store.read("k").status == "absent"
        back = store.put_tables("k", {"shop.docs": _rec(a="'x'::text")})
        assert back == {"shop.docs": _rec(a="'x'::text")}
        read = store.read("k")
        assert read.status == "present" and read.tables["shop.docs"] == _rec(a="'x'::text")
        assert store.put_tables("k", {"shop.docs": None}) == {"shop.docs": None}
        assert store.read("k") == LedgerRead("absent", store.read("k").tables) and not store.read("k").tables
        assert store.put_tables("k", {}) == {}


def test_s3_only_no_such_key_is_absent():
    for code in ("AccessDenied", "NoSuchBucket", "InternalError", "SlowDown"):
        s3 = _S3()
        s3.get_errors = [_S3Error(code)] * 3
        read = S3OverlayLedger("b", s3_client=s3, **NO_SLEEP).read("k")
        assert read.status == "unreadable" and code in read.error, code
    s3 = _S3()
    s3.get_errors = [TimeoutError("read timed out")] * 3
    assert S3OverlayLedger("b", s3_client=s3, **NO_SLEEP).read("k").status == "unreadable"
    s3 = _S3()
    s3.get_errors = [_S3Error("SlowDown")]  # one throttle, then the retry reads absent
    assert S3OverlayLedger("b", s3_client=s3, **NO_SLEEP).read("k").status == "absent"
    ledger = S3OverlayLedger("b", s3_client=_S3(), **NO_SLEEP)
    ledger.put_tables("k", {"shop.docs": _rec(a="1")})
    assert list(ledger._s3().objects) == [f"{LEDGER_PREFIX}k.json"]


def test_s3_put_rejected_raises_after_exactly_max_attempts_and_a_bad_readback_raises():
    s3 = _S3()
    s3.put_errors = [_S3Error("AccessDenied")] * 5
    with pytest.raises(OverlayLedgerError, match="ledger write failed"):
        S3OverlayLedger("b", s3_client=s3, max_attempts=3, **NO_SLEEP).put_tables("k", {"shop.docs": _rec(a="1")})
    assert s3.puts == 3
    s3 = _S3()
    s3.tamper = lambda body: body.replace(b'"1"', b'"2"')
    with pytest.raises(OverlayLedgerError, match="did not read back as written"):
        S3OverlayLedger("b", s3_client=s3, **NO_SLEEP).put_tables("k", {"shop.docs": _rec(a="1")})


def test_corrupt_and_newer_payloads_are_unreadable_and_never_overwritten(tmp_path):
    mem = InMemoryOverlayLedger(**NO_SLEEP)
    mem.blobs["k"] = "{not json"
    assert mem.read("k").status == "unreadable"
    mem.blobs["k"] = json.dumps({"schema_version": 2, "tables": {}})
    assert mem.read("k").status == "unreadable"
    with pytest.raises(OverlayLedgerError, match="newer version"):
        mem.put_tables("k", {"shop.docs": _rec(a="1")})
    assert json.loads(mem.blobs["k"])["schema_version"] == 2
    mem.blobs["k"] = json.dumps({"schema_version": 1, "tables": {}})
    assert mem.read("k").status == "absent"
    lite = SqliteOverlayLedger(str(tmp_path / "s.sqlite"), **NO_SLEEP)
    lite.put_tables("k", {"shop.docs": _rec(a="1")})
    lite._conn.execute("UPDATE overlay_ledger SET schema_version = 2")
    assert lite.read("k").status == "unreadable"
    with pytest.raises(OverlayLedgerError, match="newer version"):
        lite.put_tables("k", {"shop.other": _rec(a="1")})
    lite._conn.execute("UPDATE overlay_ledger SET schema_version = 1, payload = '{bad'")
    assert lite.read("k").status == "unreadable"


def test_sqlite_batch_is_one_transaction(tmp_path):
    lite = SqliteOverlayLedger(str(tmp_path / "s.sqlite"), max_attempts=1, **NO_SLEEP)
    lite._conn.execute(
        "CREATE TRIGGER fail_second BEFORE INSERT ON overlay_ledger WHEN NEW.table_name = 'shop.b' "
        "BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    with pytest.raises(OverlayLedgerError):
        lite.put_tables("k", {"shop.a": _rec(a="1"), "shop.b": _rec(a="2")})
    assert lite.read("k").status == "absent"  # the first row was rolled back too
    assert lite._conn.execute("SELECT count(*) FROM overlay_ledger").fetchone() == (0,)


def test_sqlite_retries_a_locked_database(tmp_path):
    path = str(tmp_path / "s.sqlite")
    lite = SqliteOverlayLedger(path, max_attempts=3, **NO_SLEEP)
    other = sqlite3.connect(path, isolation_level=None, timeout=0)
    other.execute("BEGIN IMMEDIATE")  # another writer holds the lock ...
    lite._conn.execute("PRAGMA busy_timeout = 0")
    released = []

    def _sleep(_s):
        if not released:
            other.execute("COMMIT")  # ... and releases it before the retry
            released.append(True)

    lite._sleep = _sleep
    assert lite.put_tables("k", {"shop.a": _rec(a="1")}) == {"shop.a": _rec(a="1")}
    assert released


# ---------------------------------------------------------------------------
# Concurrency (decision 10) and the run view
# ---------------------------------------------------------------------------


class _SlowLedger(InMemoryOverlayLedger):
    """Sleeps between the fresh read and the write, widening any lost-update window."""

    def _put(self, key, raw):
        time.sleep(0.02)
        super()._put(key, raw)


def _add_column(name, default):
    def merge(existing):
        base = existing or OverlayLedgerTable(table="shop.docs", set_digest=set_digest([]), applied_at=T0)
        cols = [c for c in base.columns if c.column != name] + [OverlayLedgerColumn(column=name, original_default=default)]
        return LedgerDecision("write", base.model_copy(update={"columns": sorted(cols, key=lambda c: c.column)}))
    return merge


def test_three_concurrent_writers_lose_no_entry():
    store = _SlowLedger(**NO_SLEEP)
    store.put_tables("k", {"shop.docs": _rec(c="'z'::text")})
    stale = [OverlayLedgerView(store, "k", store.read("k")) for _ in range(3)]  # all read BEFORE any write

    def _stamp(existing):
        cols = [c.model_copy(update={"restored_at": T1}) if c.column == "c" else c for c in existing.columns]
        return LedgerDecision("write", existing.model_copy(update={"columns": cols}))

    work = [lambda: stale[0].merge({"shop.docs": _add_column("a", "'x'::text")}),
            lambda: stale[1].merge({"shop.docs": _add_column("b", "'y'::text")}),
            lambda: stale[2].merge({"shop.docs": _stamp})]
    threads = [threading.Thread(target=w) for w in work]
    [t.start() for t in threads]
    [t.join() for t in threads]
    final = store.read("k").tables["shop.docs"]
    assert {c.column: c.original_default for c in final.columns} == {"a": "'x'::text", "b": "'y'::text", "c": "'z'::text"}
    assert final.column("c").restored_at == T1


def test_ledger_lock_is_one_reentrant_lock_per_key():
    assert ledger_lock("k1") is ledger_lock("k1") and ledger_lock("k1") is not ledger_lock("k2")
    with ledger_lock("k1"):
        with ledger_lock("k1"):  # re-entrant: the view holds it around the store's own hold
            pass


def test_view_adopts_the_readback_stays_unchanged_on_a_raise_and_refuses_when_unreadable():
    store = InMemoryOverlayLedger(**NO_SLEEP)
    view = OverlayLedgerView(store, "k", store.read("k"))
    old = view.tables
    view.write({"shop.docs": _rec(a="1")})
    assert view.status == "present" and view.record("shop.docs") == _rec(a="1")
    assert dict(old) == {}  # the old mapping is not mutated; the view swapped in a new one
    with pytest.raises(TypeError):
        view.tables["x"] = None  # read-only mapping
    store.blobs["k"] = json.dumps({"schema_version": 9, "tables": {}})  # a newer writer
    before = view.tables
    with pytest.raises(OverlayLedgerError):
        view.write({"shop.docs": None})
    assert view.tables is before and view.status == "present"
    view.write({})  # nothing to write: no store call

    class _Boom(InMemoryOverlayLedger):
        def merge_tables(self, *_a, **_kw):
            raise AssertionError("must not be called")

    dead = OverlayLedgerView(_Boom(), "k", LedgerRead("unreadable", {}, "AccessDenied"))
    with pytest.raises(OverlayLedgerError, match="cannot read the record of dropped DEFAULTs"):
        dead.write({"shop.docs": _rec(a="1")})


def test_an_unchanged_merge_writes_nothing():
    store = InMemoryOverlayLedger(**NO_SLEEP)
    store.put_tables("k", {"shop.docs": _rec(a="1")})
    raw = store.blobs["k"]
    calls = []
    store._put = lambda *a: calls.append(a)
    out = store.merge_tables("k", {"shop.docs": lambda existing: LedgerDecision("unchanged", existing)})
    assert out == {"shop.docs": _rec(a="1")} and calls == [] and store.blobs["k"] == raw


# ---------------------------------------------------------------------------
# Prune isolation: the session and job stores never touch the ledger
# ---------------------------------------------------------------------------


def test_s3_session_and_job_prunes_never_delete_the_ledger():
    from dsql_migrator.core.job_store import S3JobStore
    from dsql_migrator.core.session_state_store import S3SessionStateStore

    fake = _JobFakeS3(buckets=("b",))
    ledger = S3OverlayLedger("b", s3_client=fake, **NO_SLEEP)
    ledger.put_tables("k", {"shop.docs": _rec(a="1")})
    fake.put_object(Bucket="b", Key="sessions/s1.json", Body=b"{}")
    fake.put_object(Bucket="b", Key="jobs/j1.json", Body=b"{}", Metadata={"status": "DONE"})
    assert S3SessionStateStore("b", s3_client=fake).prune(0) == ["s1"]
    assert S3JobStore("b", s3_client=fake).prune_terminal(0) == ["j1"]
    assert f"{LEDGER_PREFIX}k.json" in fake._objects
    assert ledger.read("k").status == "present"


def test_sqlite_session_delete_and_prune_leave_the_ledger_rows(tmp_path):
    from dsql_migrator.core.session_state_store import SessionSnapshot, SqliteSessionStateStore

    path = str(tmp_path / "state.sqlite")
    sessions = SqliteSessionStateStore(path)
    ledger = SqliteOverlayLedger(path, **NO_SLEEP)
    ledger.put_tables("k", {"shop.docs": _rec(a="1")})
    for sid in ("s1", "s2"):
        sessions.save(SessionSnapshot(session_id=sid))
    sessions.delete("s1")
    sessions.prune(0)
    assert ledger.read("k").tables["shop.docs"] == _rec(a="1")
