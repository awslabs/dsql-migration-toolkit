# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G139: the FK orphan count before each ``ADD CONSTRAINT`` resumes page by page.

The orphan pre-gate of :func:`apply_preserved_foreign_keys` (the cut-over foreign-key pass,
and the Data Migration step's for Full load only) counted a child's orphans on a plain
connection. A 40001 or an XX000 re-ran the whole count -- the live key read (G138) and every
page -- from the start, and a dropped connection failed it: the concurrent count, then the
shared probe, then one reconnect, each from page 1. Aurora DSQL closes every connection after
60 minutes, so at the measured 7-9k rows/s a child above about 25-33M rows failed all three
attempts and its foreign key was reported "orphan pre-check failed".

The count now opens its own connection from the factory behind Validation's reconnecting
proxy and reads the child's live primary key once on it: a dropped connection is replaced
and only the statement it cut off runs again, a 40001 (or an XX000 the proxy did not absorb)
rolls back and replays the same statement -- the key read or one page, of a single-column
key or a composite one -- and every retry budget is per statement. The order of the pass
(count, ADD ... NOT VALID, VALIDATE) is unchanged. Its per-FK activity lines now take the
origin's category (Validation at cut over, Full Load after the load), like its own two lines.

Doubles only: no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import functools
from pathlib import Path

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.models import ForeignKeyDef
from dsql_migrator.core.occ import DEFAULT_MAX_ATTEMPTS
from dsql_migrator.core.validator import _ReconnectingTargetConnection

PAGE = 5
_COMPOSITE = ("order_no", "line_no")


class OperationalError(Exception):
    """A connection Aurora DSQL closed, as psycopg reports it: no SQLSTATE."""

    sqlstate = None


class _Conflict(Exception):
    sqlstate = "40001"


class _Unavailable(Exception):
    sqlstate = "XX000"


class AdminShutdown(Exception):
    """A session the server ended, as psycopg reports PostgreSQL's FATAL (G141)."""

    sqlstate = "57P01"


class _Child:
    """An in-memory child table (``(key, fk value)`` rows) and parent keys, plus one fault
    plan shared by every connection opened on it.

    ``key`` is the primary key the live catalog reports: one column (a row's key is a
    number), a composite one (a row's key is a tuple and the page cursor comes back as
    text), or none (the single scan). Each connection serves ``lifetime`` statements and
    then drops on the next one, as Aurora DSQL closes a connection at its 60-minute limit.
    ``drop_at`` / ``conflict_at`` / ``unavailable_at`` are global attempt numbers (1-based;
    every execute counts, the key read first) that drop the connection / raise a 40001 /
    raise an XX000; ``terminate_at`` ends the session as the server does (``sqlstate``, by
    default 57P01, G141); every attempt of the page whose keyset bound is
    ``always_conflict`` raises a 40001.
    """

    _NEVER = object()

    def __init__(self, rows, parents, *, key=("id",), lifetime=None, drop_at=(),
                 conflict_at=(), unavailable_at=(), always_conflict=_NEVER,
                 terminate_at=(), terminate_sqlstate="57P01"):
        self.rows = sorted(rows)
        self.parents = set(parents)
        self.key = tuple(key)
        self.lifetime = lifetime
        self.drop_at = set(drop_at)
        self.conflict_at = set(conflict_at)
        self.unavailable_at = set(unavailable_at)
        self.always_conflict = always_conflict
        self.terminate_at = set(terminate_at)
        self.terminate_sqlstate = terminate_sqlstate
        self.attempts: list = []      # (connection index, page bound / "key" / "scan", outcome)
        self.connections: list = []

    def connect(self):
        conn = _Conn(self, len(self.connections))
        self.connections.append(conn)
        return conn

    def _is_orphan(self, ref) -> bool:
        return ref is not None and ref not in self.parents

    @property
    def truth(self) -> int:
        return sum(1 for _key, ref in self.rows if self._is_orphan(ref))

    def window(self, bound) -> list:
        return [r for r in self.rows if bound is None or r[0] > bound][:PAGE]

    def page(self, bound):
        window = self.window(bound)
        orphans = sum(1 for _key, ref in window if self._is_orphan(ref))
        last = window[-1][0] if window else None
        if len(self.key) > 1:  # (orphans, rows, the last key as text per column)
            texts = [str(part) for part in last] if last else [None] * len(self.key)
            return (orphans, len(window), *texts)
        return (orphans, last, len(window))

    def bounds(self) -> list:
        """Every page's bound, in order, as a correct keyset walk visits them."""
        out, bound = [], None
        while True:
            out.append(bound)
            window = self.window(bound)
            if len(window) < PAGE:
                return out
            bound = window[-1][0]

    def outcomes(self, outcome) -> list:
        """The bound of every page / scan attempt (not the key read) that ended ``outcome``."""
        return [bound for _i, bound, got in self.attempts if got == outcome and bound != "key"]

    def key_reads(self) -> list:
        return [got for _i, bound, got in self.attempts if bound == "key"]

    def attempted_in_order(self) -> list:
        """Attempted page bounds with back-to-back repeats (replays of one page) collapsed."""
        out: list = []
        for _i, bound, _got in self.attempts:
            if bound != "key" and (not out or out[-1] != bound):
                out.append(bound)
        return out


class _Conn:
    def __init__(self, child: _Child, index: int) -> None:
        self.child = child
        self.index = index
        self.served = 0
        self.closed = False
        self.rollbacks = 0

    def cursor(self):
        return _Cur(self)

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class _Cur:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn
        self.result = None

    def execute(self, statement, parameters=None) -> None:
        conn, child = self.conn, self.conn.child
        text = statement if isinstance(statement, str) else statement.as_string(None)
        if "indisprimary" in text:  # the live primary-key read (G138)
            bound = "key"
        elif "array_agg" in text:  # a keyset page
            assert f"LIMIT {PAGE}" in text, "the fake serves pages of PAGE rows"
            assert ("ck_k0" in text) == (len(child.key) > 1), "paged by the live key"
            if len(child.key) > 1:
                bound = None if not parameters else tuple(
                    int(parameters[f"last_{i}"]) for i in range(len(child.key)))
            else:
                bound = (parameters or {}).get("last")
        else:
            assert "NOT EXISTS" in text and not child.key, "only a keyless child is one scan"
            bound = "scan"
        attempt = len(child.attempts) + 1

        def record(outcome):
            child.attempts.append((conn.index, bound, outcome))

        if conn.closed or attempt in child.drop_at or (
            child.lifetime is not None and conn.served >= child.lifetime
        ):
            conn.closed = True
            record("dropped")
            raise OperationalError("the connection is closed")
        if attempt in child.terminate_at:
            conn.closed = True
            record("terminated")
            error = AdminShutdown("terminating connection due to administrator command")
            error.sqlstate = child.terminate_sqlstate
            raise error
        conn.served += 1
        if attempt in child.conflict_at or (
            bound not in ("key", "scan") and bound == child.always_conflict
        ):
            record("40001")
            raise _Conflict("OC001: schema has been updated by another transaction")
        if attempt in child.unavailable_at:
            record("XX000")
            raise _Unavailable("server unavailable")
        record("ok")
        if bound == "key":
            self.result = [(column,) for column in child.key]
        elif bound == "scan":
            self.result = (child.truth,)
        else:
            self.result = child.page(bound)

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result

    def close(self) -> None:
        pass


def _orders(n: int = 103, orphan_pks=(3, 4, 50, 51, 96, 102, 103)) -> list:
    """``n`` orders keyed 1..n on customers 1..50, with orphans at ``orphan_pks`` and one
    NULL key (pk 10), which is not an orphan."""
    rows = []
    for pk in range(1, n + 1):
        ref = None if pk == 10 else 9000 + pk if pk in orphan_pks else 1 + pk % 50
        rows.append((pk, ref))
    return rows


def _lines(**kwargs) -> list:
    """The same rows keyed by a composite ``(order_no, line_no)``, in the same order."""
    return [((pk // 4, pk % 4), ref) for pk, ref in _orders(**kwargs)]


_KEYS = pytest.mark.parametrize(
    ("key", "rows"), [(("id",), _orders), (_COMPOSITE, _lines)], ids=["single", "composite"]
)
_PARENTS = range(1, 51)
_FK = ForeignKeyDef(name="fk_orders_customer", columns=["customer_id"],
                    referenced_table="shop.customers", referenced_columns=["id"])
_DDL = ('ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_orders_customer" FOREIGN KEY '
        '("customer_id") REFERENCES "shop"."customers" ("id") NOT VALID')


def _count(child: _Child, beats=None) -> int:
    return engine._count_orphans(
        child.connect, "shop.orders", _FK, page_size=PAGE,
        on_page=(lambda: beats.append(1)) if beats is not None else None,
        sleep=lambda _s: None,
    )


# ---------------------------------------------------------------------------
# 1. The count itself
# ---------------------------------------------------------------------------


@_KEYS
def test_the_count_survives_more_connection_closes_than_the_old_three_attempts(key, rows) -> None:
    # 21 pages over connections that each serve 4 statements: the old count failed at the
    # first close, and its two fallbacks (each from the key read and page 1) at their own
    # fourth statement. A single-column key and a composite one resume alike.
    child = _Child(rows(), _PARENTS, key=key, lifetime=4, conflict_at={12})
    beats: list = []
    assert _count(child, beats=beats) == child.truth == 7
    pages = child.bounds()
    assert len(pages) == 21
    # The live key read once, then every page counted exactly once, in keyset order ...
    assert child.key_reads() == ["ok"]
    assert child.outcomes("ok") == pages
    # ... and a failed attempt is followed only by a replay of THAT page: no page before
    # it is read again (a restart from page 1 would put None back into this walk).
    assert child.attempted_in_order() == pages
    assert child.outcomes("40001") and child.outcomes("dropped")
    # One connection per 4 statements (the key read, 21 pages and 1 conflict): 6, where the
    # old count gave up after 3; each closed (the proxy discards a dropped one, the count
    # closes the last).
    assert len(child.connections) == 6
    assert all(conn.closed for conn in child.connections)
    # The 40001 rolled back before its replay; a reconnect needs none.
    assert sum(conn.rollbacks for conn in child.connections) == 1
    # One liveness report per page, not per attempt.
    assert len(beats) == len(pages)


@_KEYS
@pytest.mark.parametrize(
    ("conflict_at", "page"),
    # Page 4, twice in a row; page 1, right after the key read (G141: the first page goes
    # through the same replay, so a conflict there replays it alone too).
    [({5, 6}, 3), ({2}, 0), ({2, 3}, 0)],
    ids=["page4", "page1", "page1-twice"],
)
def test_a_conflict_replays_only_its_page_on_the_same_connection(
    key, rows, conflict_at, page
) -> None:
    child = _Child(rows(), _PARENTS, key=key, conflict_at=conflict_at)
    assert _count(child) == 7
    assert child.attempted_in_order() == child.bounds()
    bound = child.bounds()[page]
    assert child.outcomes("40001") == [bound] * len(conflict_at)
    assert child.key_reads() == ["ok"]
    assert len(child.connections) == 1
    assert child.connections[0].rollbacks == len(conflict_at)


@_KEYS
@pytest.mark.parametrize(
    ("unavailable_at", "page"),
    [({4, 5, 6, 7}, 2), ({2, 3, 4, 5}, 0)],  # page 3; page 1, right after the key read
    ids=["page3", "page1"],
)
def test_an_xx000_the_proxy_did_not_absorb_replays_the_same_page(
    key, rows, unavailable_at, page
) -> None:
    # On an XX000 the proxy tries a statement up to 4 times on the same connection; when all
    # 4 fail, the page replay rolls back and runs the same page again (it succeeds here).
    child = _Child(rows(), _PARENTS, key=key, unavailable_at=unavailable_at)
    assert _count(child) == 7
    bound = child.bounds()[page]
    assert child.outcomes("XX000") == [bound] * 4
    assert child.attempted_in_order() == child.bounds()
    assert len(child.connections) == 1
    assert child.connections[0].rollbacks == 1


@_KEYS
@pytest.mark.parametrize("sqlstate", ["57P01", "57P02", "57P03"])
@pytest.mark.parametrize("attempt", [2, 6], ids=["page1", "page5"])
def test_a_session_the_server_ends_is_replaced_and_its_page_runs_again(
    key, rows, sqlstate, attempt
) -> None:
    # G141: a FATAL 57P01 (pg_terminate_backend, an admin shutdown), 57P02 or 57P03 is a
    # lost connection, as the sink treats it: the proxy opens a new one and replays only the
    # page it cut off. It used to fail the count at once ("orphan pre-check failed").
    child = _Child(rows(), _PARENTS, key=key, terminate_at={attempt},
                   terminate_sqlstate=sqlstate)
    assert _count(child) == 7
    assert child.outcomes("terminated") == [child.bounds()[attempt - 2]]
    assert child.outcomes("ok") == child.bounds() == child.attempted_in_order()
    assert child.key_reads() == ["ok"]
    assert len(child.connections) == 2 and child.connections[0].closed


def test_a_query_cancel_is_not_a_lost_connection() -> None:
    # 57014 (a statement timeout or a cancel) is no ended session: it is not replayed.
    child = _Child(_orders(), _PARENTS, terminate_at={3}, terminate_sqlstate="57014")
    with pytest.raises(AdminShutdown):
        _count(child)
    assert len(child.connections) == 1


@_KEYS
def test_a_page_that_keeps_conflicting_fails_the_count_without_rereading_earlier_pages(
    key, rows
) -> None:
    pages = _Child(rows(), _PARENTS, key=key).bounds()
    child = _Child(rows(), _PARENTS, key=key, always_conflict=pages[2])
    with pytest.raises(_Conflict):
        _count(child)
    # The budget is the page's own: DEFAULT_MAX_ATTEMPTS tries of page 3, pages 1 and 2
    # read once, nothing after page 3, the key read once.
    assert child.outcomes("40001") == [pages[2]] * DEFAULT_MAX_ATTEMPTS
    assert child.outcomes("ok") == pages[:2]
    assert child.key_reads() == ["ok"]
    assert child.connections[-1].closed


@pytest.mark.parametrize(
    ("fault", "connections"),
    [({"conflict_at": {1}}, 1), ({"unavailable_at": {1}}, 1), ({"drop_at": {1}}, 2)],
    ids=["40001", "XX000", "dropped"],
)
def test_a_fault_on_the_live_key_read_replays_only_the_key_read(fault, connections) -> None:
    # G138 re-read the key inside the retry of the whole count; the key is read once here,
    # so a fault on it replays the key read, and every page then runs exactly once.
    child = _Child(_orders(), _PARENTS, **fault)
    assert _count(child) == 7
    assert len(child.key_reads()) == 2 and child.key_reads()[-1] == "ok"
    assert child.outcomes("ok") == child.bounds() == child.attempted_in_order()
    assert len(child.connections) == connections


def test_the_single_scan_of_a_child_without_a_primary_key_resumes_too() -> None:
    # A child with no primary key on Aurora DSQL keeps the one-statement scan: a conflict
    # replays it, a drop replaces the connection and runs it again.
    child = _Child(_orders(), _PARENTS, key=(), conflict_at={2}, drop_at={3})
    beats: list = []
    assert _count(child, beats=beats) == 7
    assert [got for _i, _b, got in child.attempts] == ["ok", "40001", "dropped", "ok"]
    assert child.outcomes("ok") == ["scan"]
    assert len(child.connections) == 2
    assert child.connections[0].rollbacks == 1
    assert len(beats) == 1


def test_a_real_query_error_is_not_retried() -> None:
    class _Missing(Exception):
        sqlstate = "42P01"

    calls = {"pages": 0}

    class _Cur:
        def execute(self, statement, *_a):
            if isinstance(statement, str) and "indisprimary" in statement:
                return  # the live key read: an ``id`` key
            calls["pages"] += 1
            raise _Missing("relation does not exist")

        def fetchall(self):
            return [("id",)]

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    with pytest.raises(_Missing):
        engine._count_orphans(lambda: _Conn(), "shop.orders", _FK,
                              page_size=PAGE, sleep=lambda _s: None)
    assert calls["pages"] == 1


def test_the_reconnecting_proxy_rolls_back_its_live_connection_only() -> None:
    made: list = []

    def factory():
        conn = _Child([], ()).connect()
        made.append(conn)
        return conn

    proxy = _ReconnectingTargetConnection(factory, sleep=lambda _s: None)
    proxy.rollback()
    assert made[0].rollbacks == 1
    proxy.close()
    proxy.rollback()  # nothing live: a no-op, no reconnect
    assert len(made) == 1 and made[0].rollbacks == 1


# ---------------------------------------------------------------------------
# 2. Through the foreign-key pass
# ---------------------------------------------------------------------------


def _conv() -> TableConversion:
    return TableConversion(
        table="shop.orders",
        target_ddl='CREATE TABLE "shop"."orders" ("id" bigint PRIMARY KEY, "customer_id" int)',
        foreign_key_ddls=[_DDL],
        preserved_foreign_keys=[_FK],
    )


def _pass(monkeypatch, child: _Child, origin: str = "cut-over"):
    added: list = []
    validated: list = []
    events: list = []
    # The real count, on PAGE-row pages and without sleeping between retries.
    monkeypatch.setattr(engine, "_count_orphans", functools.partial(
        engine._count_orphans, page_size=PAGE, sleep=lambda _s: None))
    monkeypatch.setattr(engine, "_wait_for_referenced_unique_index",
                        lambda *a, **k: ("valid", 0.0))
    monkeypatch.setattr(engine, "apply_foreign_key",
                        lambda ddl, *, connection_factory: added.append(ddl))
    monkeypatch.setattr(engine, "validate_foreign_key",
                        lambda table, name, *, connection_factory: validated.append(name))
    monkeypatch.setattr(engine, "log_activity",
                        lambda category, action, **kw: events.append((category, action, kw)))
    beats: list = []
    result = engine.apply_preserved_foreign_keys(
        {"shop.orders": _conv()}, child.connect, origin=origin,
        heartbeat=lambda: beats.append(1),
    )
    return result, added, validated, events, beats


@_KEYS
def test_a_large_clean_child_is_counted_across_closes_and_its_key_added(
    monkeypatch, key, rows
) -> None:
    # The old pass reported (0, 0, 1) "orphan pre-check failed" here: 21 pages over
    # connections of 4 statements outlived its three attempts.
    child = _Child(rows(orphan_pks=()), _PARENTS, key=key, lifetime=4, conflict_at={7})
    result, added, validated, events, beats = _pass(monkeypatch, child)
    assert result == (1, 0, 0)
    assert added == [_DDL] and validated == ["fk_orders_customer"]
    assert child.attempted_in_order() == child.bounds()
    assert child.key_reads() == ["ok"]
    assert len(child.connections) > 3
    # The cut-over job's heartbeat reached every page of the count (plus the per-FK beats).
    assert len(beats) >= len(child.bounds())
    outcome = events[-1]
    assert outcome[1] == "apply foreign keys (cut-over)"
    assert outcome[2]["status"] is engine.ActivityStatus.SUCCESS


def test_orphans_counted_across_closes_still_keep_the_key_off(monkeypatch) -> None:
    # The verdict is unchanged: a count above 0 skips the ADD (NO semantic change to the
    # cut-over order yet) -- only the count now reaches its end.
    child = _Child(_orders(), _PARENTS, lifetime=4)
    result, added, validated, events, _beats = _pass(monkeypatch, child)
    assert result == (0, 1, 0)
    assert added == [] and validated == []
    lines = [e for e in events if e[1] == "foreign key not applied"]
    assert len(lines) == 1
    assert "7 child row(s) reference a missing parent" in lines[0][2]["detail"]
    assert lines[0][2]["target"] == "shop.orders.fk_orders_customer"


def test_a_page_that_never_succeeds_fails_the_key_and_never_reads_as_clean(monkeypatch) -> None:
    pages = _Child(_orders(orphan_pks=()), _PARENTS).bounds()
    child = _Child(_orders(orphan_pks=()), _PARENTS, always_conflict=pages[4])
    result, added, validated, events, _beats = _pass(monkeypatch, child)
    assert result == (0, 0, 1)
    assert added == [] and validated == []
    lines = [e for e in events if e[1] == "foreign key not applied"]
    assert len(lines) == 1 and "orphan pre-check failed" in lines[0][2]["detail"]


@pytest.mark.parametrize(
    ("origin", "category"),
    [("cut-over", "VALIDATION"), ("post-load", "FULL_LOAD")],
)
def test_every_per_fk_line_takes_the_origin_category(monkeypatch, origin, category) -> None:
    """At cut over the per-FK lines were filed under Full Load while the pass's own two lines
    sat under Validation, so filtering the Validation step hid which key was not added."""
    from tests.test_ui_data_migration import _three_fk_conv

    _ddls, conv = _three_fk_conv()
    # fk_a: orphans (skipped); fk_b: the pre-check fails (concurrent and serial);
    # fk_c: its parent's index had to be waited for, then the ADD is refused.
    monkeypatch.setattr(engine, "_pregate_orphans",
                        lambda pending, *a, **k: {0: 4, 1: RuntimeError("boom"), 2: 0})

    def _serial(*_a, **_k):
        raise RuntimeError("catalog unreadable")

    monkeypatch.setattr(engine, "_count_orphans", _serial)

    def _wait(fk, *_a, **_k):
        return ("valid", 2.0) if fk.name == "fk_c" else ("valid", 0.0)

    monkeypatch.setattr(engine, "_wait_for_referenced_unique_index", _wait)

    def _add(ddl, *, connection_factory):
        raise RuntimeError("ALTER rejected")

    monkeypatch.setattr(engine, "apply_foreign_key", _add)
    monkeypatch.setattr(engine, "validate_foreign_key", lambda *a, **k: None)
    events: list = []
    monkeypatch.setattr(engine, "log_activity",
                        lambda cat, action, **kw: events.append((cat, action, kw)))

    result = engine.apply_preserved_foreign_keys(
        {"child": conv}, lambda: _Child([], ()).connect(), origin=origin
    )
    assert result == (0, 1, 2)
    per_fk = [e for e in events if not e[1].startswith("apply foreign keys")]
    assert sorted((e[2]["target"], e[1]) for e in per_fk) == [
        ("child.fk_a", "foreign key not applied"),
        ("child.fk_b", "foreign key not applied"),
        ("child.fk_c", "foreign key not applied"),
        ("child.fk_c", "waited for referenced unique index"),
    ]
    expected = getattr(engine.ActivityCategory, category)
    assert {e[0] for e in events} == {expected}, events


# ---------------------------------------------------------------------------
# 3. The manual
# ---------------------------------------------------------------------------


def test_the_manual_says_the_count_resumes_in_three_languages() -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual"
    needles = {
        "en": ("**Orphan pre-gate.**", "re-runs only the page it interrupted",
               "60-minute connection limit", "~300 s per-transaction limit",
               "a composite key too", "no primary key"),
        "ko": ("**Orphan(고아) 사전 게이트.**", "중단된 그 페이지만 다시 실행",
               "60분 연결 한도", "~300초 트랜잭션 한도", "복합 키도", "기본 키가 없는"),
        "ja": ("**孤児(orphan)事前ゲート。**", "中断されたそのページだけを再実行",
               "60 分の接続上限", "~300 秒トランザクション上限", "複合キーも", "主キーのない"),
    }
    for lang, (heading, *phrases) in needles.items():
        text = (manual / lang / "02-evaluation-and-schema-conversion.md").read_text(
            encoding="utf-8")
        block = next(b for b in text.split("\n\n") if heading in b)
        flat = " ".join(block.split())
        for needle in (*phrases, "`40001`", "keyset"):
            assert needle in flat, (lang, needle)
        # G138 pages a composite key: only a table without one is counted in one query.
        for stale in ("composite or missing primary key", "복합 PK이거나", "複合 PK や"):
            assert stale not in flat, (lang, stale)
