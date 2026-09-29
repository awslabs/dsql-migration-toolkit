# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2 core overlay pieces: the bulk target facts, the reconnecting unit, the one statement
builder, apply_exclusion_overlay and restore_column_default -- against a fake catalog that records every statement and connect.

Behaviour pinned here is what P0 settled live (F1-F12) and P2 LIVE 1: DROP NOT NULL before
DROP DEFAULT, one statement per transaction with 40001 retried, whole-unit replay on a lost
connection that re-reads first (idempotent, F10), one read-back on the same connection, no
poll.
"""

import re

import pytest

from dsql_migrator.core.schema_applier import (
    OverlayRefusedError,
    OverlayVerifyError,
    SchemaApplyError,
    _run_unit_reconnecting,
    apply_exclusion_overlay,
    build_overlay_statements,
    ddl_column_facts,
    restore_column_default,
)
from dsql_migrator.core.target_introspector import ColumnNullFacts as F
from dsql_migrator.core.target_introspector import target_overlay_facts


# ---------------------------------------------------------------------------
# A fake DSQL catalog + DDL executor
# ---------------------------------------------------------------------------


class Occ(Exception):
    def __init__(self, msg="schema has been updated by another transaction (OC001)"):
        super().__init__(msg)
        self.sqlstate = "40001"


class Conn08(Exception):
    sqlstate = "08006"


class OperationalError(Exception):  # the name is what the transient check matches
    sqlstate = None


class Denied(Exception):
    sqlstate = "42501"


def _stored(name):
    """The catalog's form of an identifier: UTF-8 cut to 63 bytes (NAMEDATALEN)."""
    return name.encode("utf-8")[:63].decode("utf-8", errors="ignore")


class FakeDb:
    def __init__(self, tables, rows=None):
        # tables: {"shop.t": {"col": {"notnull": bool, "default": str|None, "identity": "", "generated": ""}}}
        # Column names are stored as the catalog stores them (cut to 63 bytes).
        self.tables = {t: {_stored(c): dict({"identity": "", "generated": ""}, **v) for c, v in cols.items()}
                       for t, cols in tables.items()}
        self.rows = rows or {}
        self.pks = {}
        self.log = []  # (connection id, statement text)
        self.connects = 0
        self.faults = []  # [pattern, exception, remaining, when ('before'|'after'), skip]
        self.ignore_alters = False  # an ALTER that "succeeds" but changes nothing
        self.store_default_as = None  # a SET DEFAULT that stores a different text

    def fault(self, pattern, exc, times=1, when="before", skip=0):
        self.faults.append([re.compile(pattern), exc, times, when, skip])

    def connect(self):
        self.connects += 1
        return FakeConn(self, self.connects)


class FakeConn:
    def __init__(self, db, cid):
        self.db, self.cid, self.rollbacks, self.closed = db, cid, 0, False

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _text(stmt):
    return stmt.as_string(None) if hasattr(stmt, "as_string") else str(stmt)


class FakeCursor:
    def __init__(self, conn):
        self.conn, self._rows = conn, []

    def close(self):
        pass

    def _fire(self, text, when):
        for f in self.conn.db.faults:
            if f[2] > 0 and f[3] == when and f[0].search(text):
                if f[4] > 0:  # let the first ``skip`` matches through
                    f[4] -= 1
                    continue
                f[2] -= 1
                raise f[1]

    def execute(self, stmt, params=None):
        db, text = self.conn.db, _text(stmt)
        db.log.append((self.conn.cid, text))
        self._fire(text, "before")
        if "ix.indisprimary" in text:
            name = f"{params['schema']}.{params['table']}" if "schema" in params else params["table"]
            self._rows = [(c,) for c in db.pks.get(name, [])]
        elif "pg_catalog.pg_attribute a ON" in text:
            name = f"{params['schema']}.{params['table']}" if "schema" in params else params["table"]
            table = db.tables.get(name)
            if table is None:
                self._rows = []
            else:
                # attname = ANY(name[]): each parameter is cut to 63 bytes like the catalog.
                asked = {_stored(c) for c in params["columns"]}
                hits = [(c, table[c]["notnull"], table[c]["identity"], table[c]["generated"],
                         table[c]["default"] if not table[c]["generated"] else "(gen expr)")
                        for c in table if c in asked]
                self._rows = hits or [(None, None, None, None, None)]
        elif text.startswith("SELECT 1 FROM"):
            name = ".".join(re.findall(r'"([^"]+)"', text))
            self._rows = [(1,)] if db.rows.get(name) else []
        elif text.startswith("ALTER TABLE"):
            m = re.match(r'ALTER TABLE ((?:"[^"]+"\.)?"[^"]+") ALTER COLUMN "([^"]+)" (DROP NOT NULL|DROP DEFAULT|SET DEFAULT (.*))$', text)
            assert m, text
            name = ".".join(re.findall(r'"([^"]+)"', m.group(1)))
            col = db.tables[name][_stored(m.group(2))]
            if not db.ignore_alters:
                if m.group(3) == "DROP NOT NULL":
                    col["notnull"] = False
                elif m.group(3) == "DROP DEFAULT":
                    col["default"] = None
                else:
                    # PostgreSQL keeps the expression, not the parentheses around it.
                    expr = m.group(4)
                    if expr.startswith("(") and expr.endswith(")"):
                        expr = expr[1:-1]
                    col["default"] = db.store_default_as or expr
        self._fire(text, "after")

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


NO_SLEEP = {"sleep": lambda _s: None, "jitter": lambda: 0.0}


def _alters(db):
    return [t.split(" ALTER COLUMN ")[1] for _c, t in db.log if t.startswith("ALTER TABLE")]


def _db(**cols):
    return FakeDb({"shop.docs": cols})


# ---------------------------------------------------------------------------
# S2: bulk target facts
# ---------------------------------------------------------------------------


def test_facts_one_connection_params_only_and_the_three_absences():
    db = FakeDb({"shop.a": {"c": {"notnull": True, "default": "'x'::text"},
                            "g": {"notnull": False, "default": None, "generated": "s"},
                            "i": {"notnull": True, "default": None, "identity": "d"}},
                 "shop.b": {"id": {"notnull": True, "default": None}}}, rows={"shop.a": True})
    db.pks["shop.a"] = ["c"]
    facts = target_overlay_facts(["shop.a", "shop.b", "shop.gone"],
                                 {"shop.a": ["c", "g", "i", "zz"], "shop.b": ["x"], "shop.gone": ["y"]},
                                 connection_factory=db.connect, primary_key_tables={"shop.a", "shop.b"})
    assert db.connects == 1
    catalog = [t for _c, t in db.log if "pg_attribute" in t]
    assert all("shop" not in t and "docs" not in t for t in catalog)  # identifiers only in params
    a = facts["shop.a"]
    assert a.exists and a.has_rows is True and a.primary_key == ("c",)
    assert a.columns["c"] == F(True, "'x'::text", False, False)
    assert a.columns["g"].generated and a.columns["g"].default_expr is None  # gen expr is not a DEFAULT
    assert a.columns["i"].identity and a.columns["i"].computed
    assert a.columns["zz"] is None                         # absent column: inner None
    assert facts["shop.b"].exists and facts["shop.b"].columns == {"x": None} and facts["shop.b"].primary_key == ()
    assert facts["shop.gone"].exists is False and facts["shop.gone"].columns == {"y": None}


def test_facts_identity_kinds_and_the_columns_list_param():
    db = FakeDb({"shop.a": {"x": {"notnull": True, "default": None, "identity": "a"},
                            "y": {"notnull": False, "default": "nextval('s'::regclass)"}}})
    seen = []
    orig = FakeCursor.execute

    def _spy(self, stmt, params=None):
        seen.append(params)
        return orig(self, stmt, params)

    FakeCursor.execute = _spy
    try:
        facts = target_overlay_facts(["shop.a"], {"shop.a": ["x", "y"]}, connection_factory=db.connect,
                                     probe_rows=False)
    finally:
        FakeCursor.execute = orig
    assert facts["shop.a"].columns["x"].identity  # 'a' (ALWAYS) as well as 'd'
    assert facts["shop.a"].columns["y"] == F(False, "nextval('s'::regclass)")  # pg_get_expr text as is
    assert [p["columns"] for p in seen if p and "columns" in p] == [["x", "y"]]


def test_facts_errors_are_scoped_and_row_probe_is_optional():
    db = FakeDb({"shop.a": {"c": {"notnull": False, "default": None}}, "shop.b": {"c": {"notnull": False, "default": None}}})
    db.fault(r"pg_attribute", Denied(), times=1)
    conns = []
    orig = db.connect
    db.connect = lambda: conns.append(orig()) or conns[-1]
    facts = target_overlay_facts(["shop.a", "shop.b"], {"shop.a": ["c"], "shop.b": ["c"]},
                                 connection_factory=db.connect, probe_rows=False)
    assert facts["shop.a"] is None and facts["shop.b"] is not None  # only the failing table unknown
    assert conns[0].rollbacks == 1 and conns[0].closed  # rolled back, then the next table read
    assert not any(t.startswith("SELECT 1") for _c, t in db.log)
    assert facts["shop.b"].has_rows is None

    def _down():
        raise Conn08()

    assert target_overlay_facts(["shop.a", "shop.b"], {}, connection_factory=_down) == {"shop.a": None, "shop.b": None}


# ---------------------------------------------------------------------------
# S3: the reconnecting unit
# ---------------------------------------------------------------------------


def test_unit_returns_the_body_value_and_replays_only_connection_failures():
    db = _db()
    calls = []

    def body(conn, run_one, retry_read):
        calls.append(conn.cid)
        if len(calls) == 1:
            raise Conn08()
        return "done"

    assert _run_unit_reconnecting(db.connect, body, occ_max_attempts=3, occ_base_delay=0, **NO_SLEEP) == "done"
    assert calls == [1, 2]

    def terminal(conn, run_one, retry_read):
        raise SchemaApplyError("no")

    db2 = _db()
    with pytest.raises(SchemaApplyError):
        _run_unit_reconnecting(db2.connect, terminal, occ_max_attempts=3, occ_base_delay=0, **NO_SLEEP)
    assert db2.connects == 1

    def unresolvable():
        raise OperationalError("could not translate host name: nodename nor servname provided, or not known")

    with pytest.raises(OperationalError):
        _run_unit_reconnecting(unresolvable, lambda *a: None, occ_max_attempts=3, occ_base_delay=0, **NO_SLEEP)


# ---------------------------------------------------------------------------
# S4: the statement builder + apply_exclusion_overlay
# ---------------------------------------------------------------------------


def test_order_pin_drop_not_null_before_drop_default_per_column():
    stmts = build_overlay_statements("shop.docs", {
        "b": F(True, "'x'::text"), "a": F(True, "0"), "c": F(True, None), "d": F(False, "now()"), "e": None})
    assert [(s.column, s.action) for s in stmts] == [
        ("a", "DROP NOT NULL"), ("a", "DROP DEFAULT"), ("b", "DROP NOT NULL"), ("b", "DROP DEFAULT"),
        ("c", "DROP NOT NULL"), ("d", "DROP DEFAULT")]
    assert stmts[0].text() == 'ALTER TABLE "shop"."docs" ALTER COLUMN "a" DROP NOT NULL'
    with pytest.raises(OverlayRefusedError, match="Computed by Aurora DSQL"):
        build_overlay_statements("shop.docs", {"g": F(False, None, generated=True)})


def test_one_builder_for_live_facts_and_the_applied_ddl():
    ddl = 'CREATE TABLE "shop"."docs" ("id" INT PRIMARY KEY, "body" TEXT NOT NULL DEFAULT \'x\', "n" INT NOT NULL)'
    from_ddl = ddl_column_facts(ddl, ["body", "n", "missing"])
    live = {"body": F(True, "'x'"), "n": F(True, None), "missing": None}
    assert [s.text() for s in build_overlay_statements("shop.docs", from_ddl)] == \
        [s.text() for s in build_overlay_statements("shop.docs", live)]
    # The P1 moot-gate case: NOT NULL with no DEFAULT -> exactly DROP NOT NULL.
    assert [s.action for s in build_overlay_statements("shop.docs", {"n": F(True, None)})] == ["DROP NOT NULL"]


def test_overlay_issues_only_what_is_needed_on_one_connection_and_reads_back():
    db = _db(body={"notnull": True, "default": "'x'::text"}, note={"notnull": False, "default": None})
    out = apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text"), "note": F(False, None)},
                                  connection_factory=db.connect, **NO_SLEEP)
    assert db.connects == 1
    assert _alters(db) == ['"body" DROP NOT NULL', '"body" DROP DEFAULT']
    assert out.dropped_not_null == ("body",) and out.dropped_defaults == {"body": "'x'::text"}
    assert out.already_ok == ("note",)
    assert "pg_attribute" in db.log[-1][1] and db.log[-1][0] == 1  # read-back, same connection
    assert not any("sys.jobs" in t or "wait_for_job" in t for _c, t in db.log)  # no poll


@pytest.mark.parametrize("exc", [Conn08(), OperationalError("connection closed")])
def test_a_lost_connection_after_drop_not_null_replays_from_a_re_read(exc):
    db = _db(body={"notnull": True, "default": "'x'::text"})
    db.fault(r"DROP NOT NULL", exc, when="after")  # the ALTER committed, the reply was lost
    out = apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text")}, connection_factory=db.connect, **NO_SLEEP)
    by_conn = {}
    for cid, text in db.log:
        by_conn.setdefault(cid, []).append("facts" if "pg_attribute" in text else text.split(" ALTER COLUMN ")[-1])
    assert by_conn[1] == ["facts", '"body" DROP NOT NULL']
    assert by_conn[2] == ["facts", '"body" DROP DEFAULT', "facts"]
    assert out.dropped_defaults == {"body": "'x'::text"}
    assert out.dropped_not_null == ("body",)  # committed on connection 1, its reply lost


def test_verify_fails_fast_with_no_second_read_and_computed_is_refused_before_connect():
    db = _db(body={"notnull": True, "default": "'x'::text"})
    db.ignore_alters = True
    with pytest.raises(OverlayVerifyError, match="still NOT NULL"):
        apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text")}, connection_factory=db.connect, **NO_SLEEP)
    assert sum("pg_attribute" in t for _c, t in db.log) == 2  # the pre-read + ONE read-back
    db2 = _db()
    with pytest.raises(OverlayRefusedError):
        apply_exclusion_overlay("shop.docs", {"g": F(False, None, identity=True)}, connection_factory=db2.connect, **NO_SLEEP)
    assert db2.connects == 0
    # A replay whose re-read newly shows a computed column is refused too.
    db3 = _db(body={"notnull": True, "default": "'x'::text"})
    db3.fault(r"DROP NOT NULL", Conn08(), when="after")
    db3.tables["shop.docs"]["body"]["generated"] = ""  # set below after the first ALTER

    def _flip(*_a):
        db3.tables["shop.docs"]["body"]["identity"] = "a"

    orig = db3.connect
    db3.connect = lambda: (_flip() if db3.connects == 1 else None) or orig()
    with pytest.raises(OverlayRefusedError):
        apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text")}, connection_factory=db3.connect, **NO_SLEEP)


def test_partial_success_and_empty_plans():
    db = _db(body={"notnull": False, "default": "'x'::text"})
    apply_exclusion_overlay("shop.docs", {"body": F(False, "'x'::text")}, connection_factory=db.connect, **NO_SLEEP)
    assert _alters(db) == ['"body" DROP DEFAULT']
    db2 = _db()
    out = apply_exclusion_overlay("shop.docs", {"body": F(False, None), "x": None}, connection_factory=db2.connect, **NO_SLEEP)
    assert db2.connects == 0 and out.already_ok == ("body",)


def test_attempt_one_builds_from_the_live_read_not_the_callers_plan():
    # The plan saw a DEFAULT that is gone and no NOT NULL that is now there: the live read wins.
    db = _db(body={"notnull": True, "default": None})
    out = apply_exclusion_overlay("shop.docs", {"body": F(False, "'x'::text")}, connection_factory=db.connect, **NO_SLEEP)
    assert _alters(db) == ['"body" DROP NOT NULL'] and out.dropped_defaults == {}
    assert [("facts" if "pg_attribute" in t else "alter") for _c, t in db.log] == ["facts", "alter", "facts"]


def test_an_unrecorded_live_default_is_refused_before_any_alter():
    for expected in ({"body": "'old'::text"}, {"body": None}, {}):
        db = _db(body={"notnull": True, "default": "'new'::text"}, n={"notnull": True, "default": None})
        with pytest.raises(OverlayRefusedError, match="nothing was changed on this table") as err:
            apply_exclusion_overlay("shop.docs", {"body": F(True, "'new'::text"), "n": F(True, None)},
                                    expected_defaults=expected, connection_factory=db.connect, **NO_SLEEP)
        assert _alters(db) == [], expected  # not even the other column's DROP NOT NULL
        assert "body (live DEFAULT 'new'::text, recorded " in str(err.value)
        assert db.tables["shop.docs"]["body"]["notnull"] is True
    # The recorded DEFAULT matches -> the overlay runs; an already-dropped one is no mismatch.
    db = _db(body={"notnull": True, "default": "'x'::text"}, gone={"notnull": True, "default": None})
    out = apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text"), "gone": F(True, "'y'::text")},
                                  expected_defaults={"body": "'x'::text", "gone": "'y'::text"},
                                  connection_factory=db.connect, **NO_SLEEP)
    assert out.dropped_defaults == {"body": "'x'::text"} and out.dropped_not_null == ("body", "gone")


def test_a_default_changed_between_attempts_is_refused_and_names_what_already_ran():
    db = _db(body={"notnull": True, "default": "'x'::text"})
    db.fault(r"DROP NOT NULL", Conn08(), when="after")
    orig = db.connect

    def _connect():
        if db.connects == 1:  # a concurrent writer changes the DEFAULT before the replay
            db.tables["shop.docs"]["body"]["default"] = "'z'::text"
        return orig()

    with pytest.raises(OverlayRefusedError, match="an earlier attempt already issued body DROP NOT NULL\\."):
        apply_exclusion_overlay("shop.docs", {"body": F(True, "'x'::text")}, expected_defaults={"body": "'x'::text"},
                                connection_factory=_connect, **NO_SLEEP)
    assert _alters(db) == ['"body" DROP NOT NULL']
    assert db.tables["shop.docs"]["body"]["default"] == "'z'::text"  # never dropped


def test_40001_is_retried_per_statement_and_on_the_read_back_but_42501_is_not():
    for msg in ("change conflicts with another transaction (OC000)", "schema has been updated (OC001)"):
        db = _db(a={"notnull": True, "default": "0"})
        db.fault(r"DROP NOT NULL", Occ(msg))
        apply_exclusion_overlay("shop.docs", {"a": F(True, "0")}, connection_factory=db.connect, **NO_SLEEP)
        assert _alters(db) == ['"a" DROP NOT NULL', '"a" DROP NOT NULL', '"a" DROP DEFAULT'], msg
        assert db.connects == 1
    for skip in (0, 1):  # a 40001 on the pre-read, then on the read-back
        db = _db(a={"notnull": True, "default": None})
        db.fault(r"pg_attribute", Occ(), skip=skip)
        apply_exclusion_overlay("shop.docs", {"a": F(True, None)}, connection_factory=db.connect, **NO_SLEEP)
        assert sum("pg_attribute" in t for _c, t in db.log) == 3 and db.connects == 1, skip
        assert _alters(db) == ['"a" DROP NOT NULL'], skip
    db = _db(a={"notnull": True, "default": None})
    db.fault(r"DROP NOT NULL", Denied())
    with pytest.raises(Denied):
        apply_exclusion_overlay("shop.docs", {"a": F(True, None)}, connection_factory=db.connect, **NO_SLEEP)
    assert db.connects == 1


# ---------------------------------------------------------------------------
# S5: restore_column_default
# ---------------------------------------------------------------------------


def test_restore_is_idempotent_and_verified():
    db = _db(body={"notnull": False, "default": None})
    first = restore_column_default("shop.docs", "body", "'x'::text", connection_factory=db.connect, **NO_SLEEP)
    assert first.status == "restored" and db.tables["shop.docs"]["body"]["default"] == "'x'::text"
    sets = sum(t.startswith("ALTER TABLE") for _c, t in db.log)
    second = restore_column_default("shop.docs", "body", "'x'::text", connection_factory=db.connect, **NO_SLEEP)
    assert second.status == "done" and sum(t.startswith("ALTER TABLE") for _c, t in db.log) == sets
    assert db.connects == 2  # one per call, the read-back on the SET's connection


def test_restore_replays_after_a_lost_reply_without_a_second_set():
    db = _db(body={"notnull": False, "default": None})
    db.fault(r"SET DEFAULT", Conn08(), when="after")
    out = restore_column_default("shop.docs", "body", "now()", connection_factory=db.connect, **NO_SLEEP)
    assert out.status == "restored" and sum("SET DEFAULT" in t for _c, t in db.log) == 1


def test_restore_never_overwrites_refuses_unsafe_text_and_detects_a_bad_round_trip():
    db = _db(body={"notnull": False, "default": "'other'::text"})
    assert restore_column_default("shop.docs", "body", "'x'::text", connection_factory=db.connect, **NO_SLEEP).status == "diverged"
    assert db.tables["shop.docs"]["body"]["default"] == "'other'::text"
    assert restore_column_default("shop.docs", "gone", "1", connection_factory=db.connect, **NO_SLEEP).status == "gone"
    none = _db()
    with pytest.raises(OverlayRefusedError):
        restore_column_default("shop.docs", "body", "1; DROP TABLE x", connection_factory=none.connect, **NO_SLEEP)
    assert none.connects == 0
    ok = _db(body={"notnull": False, "default": None})
    assert restore_column_default("shop.docs", "body", "'a;b'::text", connection_factory=ok.connect, **NO_SLEEP).status == "restored"
    bad = _db(body={"notnull": False, "default": None})
    bad.store_default_as = "'y'::text"
    with pytest.raises(OverlayVerifyError, match="does not round-trip"):
        restore_column_default("shop.docs", "body", "'x'::text", connection_factory=bad.connect, **NO_SLEEP)
    comp = _db(body={"notnull": False, "default": None, "generated": "s"})
    with pytest.raises(OverlayRefusedError):
        restore_column_default("shop.docs", "body", "1", connection_factory=comp.connect, **NO_SLEEP)


# ---------------------------------------------------------------------------
# Review fixes (P2-01..04 verification)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("expr", [
    "'x'::text, DROP COLUMN secret",                       # a second ALTER TABLE subcommand
    "E'\\'';DROP TABLE t;SELECT E'\\''",                   # a backslash escape shifts the quote parity
    "E'\\'';COMMIT;DROP TABLE t;SELECT E'\\''",
    "$a1$'$a1$;DROP TABLE t;SELECT $a1$'$a1$",             # a dollar tag with a digit
    "x) , DROP COLUMN s, ALTER c SET DEFAULT (1",          # closes the wrapping parenthesis
    "'a\\b'::text", "$1", "(1", "1)", "'open",
])
def test_restore_refuses_every_splice_before_connecting(expr):
    db = _db(body={"notnull": False, "default": None})
    with pytest.raises(OverlayRefusedError, match="not a single expression"):
        restore_column_default("shop.docs", "body", expr, connection_factory=db.connect, **NO_SLEEP)
    assert db.connects == 0


@pytest.mark.parametrize("expr", [
    "'a;b'::text", "now()", "nextval('s'::regclass)", "'{1,2}'::integer[]", "ROW(1, 2)",
    "'x'::character varying", "ARRAY[1, 2]", "(0)::numeric(10,2)", "'it''s'::text", "'$5'::text",
])
def test_real_pg_get_expr_text_is_restored_inside_parentheses(expr):
    db = _db(body={"notnull": False, "default": None})
    assert restore_column_default("shop.docs", "body", expr, connection_factory=db.connect, **NO_SLEEP).status == "restored"
    sets = [t for _c, t in db.log if "SET DEFAULT" in t]
    assert sets == [f'ALTER TABLE "shop"."docs" ALTER COLUMN "body" SET DEFAULT ({expr})']


LONG = "customer_shipping_address_line_two_normalized_for_geo_lookup_v22"  # 64 bytes
WIDE = "é" * 32  # 64 bytes; the cut falls inside a character


def test_a_name_over_63_bytes_is_keyed_by_the_asked_name():
    assert len(LONG.encode()) == 64 and len(WIDE.encode()) == 64
    db = _db(**{LONG: {"notnull": True, "default": "'x'::text"}, WIDE: {"notnull": False, "default": "0"}})
    facts = target_overlay_facts(["shop.docs"], {"shop.docs": [LONG, WIDE]}, connection_factory=db.connect,
                                 probe_rows=False)["shop.docs"]
    assert set(facts.columns) == {LONG, WIDE}  # no stray truncated key
    assert facts.columns[LONG] == F(True, "'x'::text") and facts.columns[WIDE] == F(False, "0")
    out = apply_exclusion_overlay("shop.docs", facts.columns, connection_factory=db.connect, **NO_SLEEP)
    assert out.dropped_defaults == {LONG: "'x'::text", WIDE: "0"} and out.dropped_not_null == (LONG,)
    assert restore_column_default("shop.docs", LONG, "'x'::text", connection_factory=db.connect, **NO_SLEEP).status == "restored"


def test_two_asked_names_the_catalog_stores_as_one_make_the_table_unknown():
    twin = LONG[:-1] + "3"  # the same first 63 bytes
    db = _db(**{LONG: {"notnull": True, "default": None}})
    assert target_overlay_facts(["shop.docs"], {"shop.docs": [LONG, twin]}, connection_factory=db.connect,
                                probe_rows=False) == {"shop.docs": None}


def test_facts_retry_one_occ_and_reconnect_a_lost_connection():
    tables = {f"shop.t{i}": {"c": {"notnull": False, "default": None}} for i in range(4)}
    db = FakeDb(tables, rows={name: True for name in tables})
    db.fault(r'SELECT 1 FROM "shop"."t1"', Occ())
    facts = target_overlay_facts(list(tables), {n: ["c"] for n in tables}, connection_factory=db.connect,
                                 sleep=lambda _s: None)
    assert all(facts[n] is not None for n in tables) and db.connects == 1

    db = FakeDb(tables, rows={name: True for name in tables})
    db.fault(r'SELECT 1 FROM "shop"."t1"', Conn08())
    facts = target_overlay_facts(list(tables), {n: ["c"] for n in tables}, connection_factory=db.connect,
                                 sleep=lambda _s: None)
    assert all(facts[n] is not None for n in tables) and db.connects == 2

    db = FakeDb(tables, rows={name: True for name in tables})
    db.fault(r'SELECT 1 FROM "shop"."t[123]"', Conn08(), times=99)
    facts = target_overlay_facts(list(tables), {n: ["c"] for n in tables}, connection_factory=db.connect,
                                 sleep=lambda _s: None)
    assert facts["shop.t0"] is not None and [facts[f"shop.t{i}"] for i in (1, 2, 3)] == [None, None, None]
    assert db.connects == 3  # the first connection + the two bounded reconnects


def test_ddl_facts_refuse_identity_and_unquoted_generated_like_the_live_read():
    for column_sql in ('"seq" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)',
                       '"seq" BIGINT GENERATED ALWAYS AS IDENTITY (CACHE 1)'):
        ddl = f'CREATE TABLE "shop"."t" ("id" BIGINT PRIMARY KEY, {column_sql}, "x" INT)'
        facts = ddl_column_facts(ddl, ["seq"])
        assert facts["seq"].identity and facts["seq"].not_null and facts["seq"].computed
        with pytest.raises(OverlayRefusedError):
            build_overlay_statements("shop.t", facts)
    ddl = 'CREATE TABLE "app"."t" ("id" INT PRIMARY KEY, "a" INT, Total INT GENERATED ALWAYS AS (a * 2) STORED)'
    assert ddl_column_facts(ddl, ["total"])["total"].generated
    with pytest.raises(OverlayRefusedError):
        build_overlay_statements("app.t", ddl_column_facts(ddl, ["total"]))
    assert ddl_column_facts("garbage(", ["a"]) is None  # unknown, never "nothing to do"
