# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-15: the cut-over DEFAULT restore -- classification, the background status cache,
the restore job (settle check, serial units, one ledger batch), the click-time gates, and
the reverse gate on every job that would drop the DEFAULTs again."""

import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.overlay_ledger import (
    InMemoryOverlayLedger,
    LedgerRead,
    OverlayLedgerColumn,
    OverlayLedgerError,
    OverlayLedgerTable,
    overlay_ledger_key,
    recorded_ddl_fields,
)
from dsql_migrator.core.schema_applier import RestoreOutcome
from dsql_migrator.core.target_introspector import ColumnNullFacts, TableOverlayFacts
from dsql_migrator.ui import cutover_defaults as cd
from dsql_migrator.ui.cutover_defaults import (
    MANUAL_REASONS,
    NOT_EXCLUDED_REASON,
    CutoverDefaultsSnapshot,
    classify_cutover_defaults,
    cutover_settle_probe,
    read_cutover_defaults,
    run_cutover_default_restore,
)

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
D1 = ('CREATE TABLE "app"."t" ("id" INT NOT NULL PRIMARY KEY, "a" VARCHAR(16) NOT NULL DEFAULT \'x\', '
      '"b" TEXT DEFAULT \'y\', "n" TEXT)')
TARGET = SimpleNamespace(cluster_endpoint="c.dsql.us-east-1.on.aws", database="postgres", region="us-east-1")
KEY = overlay_ledger_key(TARGET)


def _col(not_null=False, default=None, identity=False, generated=False):
    return ColumnNullFacts(not_null=not_null, default_expr=default, identity=identity, generated=generated)


def _facts(columns, exists=True):
    return TableOverlayFacts(exists=exists, has_rows=None, primary_key=None, columns=columns)


def _entry(column, original, ddl=D1, *, dropped_not_null=True, fields=None):
    digest, state, default = fields if fields is not None else recorded_ddl_fields(ddl, column)
    return OverlayLedgerColumn(column=column, original_default=original, recorded_ddl_digest=digest,
                               recorded_ddl_state=state, recorded_ddl_default=default,
                               dropped_not_null=dropped_not_null)


def _record(*entries, table="app.t"):
    return OverlayLedgerTable(table=table, set_digest="d", set_columns=[e.column for e in entries],
                              empty_when_applied=True, applied_at=T0, columns=list(entries))


def _classify(entries, live=None, *, ddls=None, excluded=None, table="app.t"):
    live = live if live is not None else {e.column: _col() for e in entries}
    items, nullable = classify_cutover_defaults(
        {table: _record(*entries, table=table)}, {table: _facts(live)},
        current_ddls={table: D1} if ddls is None else ddls,
        still_excluded={table: [e.column for e in entries]} if excluded is None else excluded)
    return {i.column: i for i in items}, nullable


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_status_comes_from_the_live_catalog():
    entries = [_entry("a", "'x'::character varying"), _entry("b", "'y'::text")]
    by, _n = _classify(entries, {"a": _col(), "b": _col(default="'y'::text")})
    assert by["a"].status == "pending" and by["b"].status == "done"
    by, _n = _classify(entries, {"a": _col(default="'other'::text"), "b": None})
    assert by["a"].status == "diverged" and not by["a"].auto and by["b"].status == "gone"
    items, _n = classify_cutover_defaults({"app.t": _record(*entries)}, {"app.t": _facts({}, exists=False)},
                                          current_ddls={"app.t": D1}, still_excluded={"app.t": ["a", "b"]})
    assert {i.status for i in items} == {"gone"} and all(i.table_absent for i in items)
    items, _n = classify_cutover_defaults({"app.t": _record(*entries)}, {"app.t": None},
                                          current_ddls={"app.t": D1}, still_excluded={"app.t": ["a", "b"]})
    assert {i.status for i in items} == {"unknown"} and not any(i.auto for i in items)


def test_auto_through_A_or_B_and_only_while_still_excluded():
    by, _n = _classify([_entry("a", "'x'::character varying")])
    assert by["a"].auto and by["a"].reason is None  # (A): the DDL is unchanged
    edited = D1.replace('"n" TEXT', '"n" VARCHAR(9)')  # another column's type edited
    by, _n = _classify([_entry("a", "'x'::character varying")], ddls={"app.t": edited})
    assert by["a"].auto  # (B): the same declared DEFAULT
    by, _n = _classify([_entry("a", "'x'::character varying")], excluded={"app.t": []})
    assert not by["a"].auto and by["a"].reason == NOT_EXCLUDED_REASON.format(o="'x'::character varying")


def test_a_sibling_re_drop_leaves_the_untouched_column_manual():
    # a and b recorded under D1; a Step-2 REPLACE to D2 removes only a's DEFAULT and b is
    # re-dropped (its entry rewritten under D2). No table-level digest is consulted.
    d2 = D1.replace(" DEFAULT 'x'", "")
    a = _entry("a", "'x'::character varying")
    b = _entry("b", "'y'::text", ddl=d2)
    by, _n = _classify([a, b], ddls={"app.t": d2})
    assert not by["a"].auto and by["a"].reason == MANUAL_REASONS[8].format(o="'x'::character varying", x="'x'")
    assert [c for c, i in by.items() if i.auto] == ["b"]


def test_the_variants_of_an_edited_ddl():
    o = "'x'::character varying"
    d_z = D1.replace("DEFAULT 'x'", "DEFAULT 'z'")
    by, _n = _classify([_entry("a", "'z'::character varying", ddl=d_z)], ddls={"app.t": d_z})
    assert by["a"].auto  # 'x' -> 'z' and the table recreated: restores 'z'
    by, _n = _classify([_entry("a", o)], ddls={"app.t": d_z})
    assert by["a"].reason == MANUAL_REASONS[9].format(o=o, x="'x'", y="'z'")  # edited, not recreated
    bad = "garbage ("
    by, _n = _classify([_entry("a", o, ddl=bad)], ddls={"app.t": bad})
    assert by["a"].auto  # an unparseable DDL left unchanged: (A)
    by, _n = _classify([_entry("a", o, ddl=bad)], ddls={"app.t": bad + " "})
    assert by["a"].reason == MANUAL_REASONS[4].format(o=o)
    by, _n = _classify([_entry("n", "'hand'::text")])
    assert by["n"].auto  # a hand-set DEFAULT, DDL unchanged: (A)
    by, _n = _classify([_entry("n", "'hand'::text")], ddls={"app.t": D1 + " "})
    assert by["n"].reason == MANUAL_REASONS[7].format(o="'hand'::text")


@pytest.mark.parametrize("case,reason", [
    ("no_ddls", 1), ("not_in_rebuild", 2), ("digest_none", 3), ("unparseable_now", 4), ("unreadable", 5),
    ("column_gone", 6), ("state_none", 7), ("declared_now_none", 8), ("declared_now_other", 9)])
def test_each_manual_reason_in_order(case, reason):
    o = "'x'::character varying"
    other = D1 + " "
    entry, ddls = _entry("a", o), {"app.t": other}
    if case == "no_ddls":
        ddls = None
    elif case == "not_in_rebuild":
        ddls = {"app.other": D1}
    elif case == "digest_none":
        entry = _entry("a", o, fields=(None, "unreadable", None))
    elif case == "unparseable_now":
        ddls = {"app.t": "garbage ("}
    elif case == "unreadable":
        entry = _entry("a", o, fields=("old", "unreadable", None))
    elif case == "column_gone":
        ddls = {"app.t": other.replace('"a" VARCHAR(16) NOT NULL DEFAULT \'x\', ', "")}
    elif case == "state_none":
        entry = _entry("a", o, fields=("old", "none", None))
    elif case == "declared_now_none":
        ddls = {"app.t": other.replace(" DEFAULT 'x'", "")}
    else:
        ddls = {"app.t": other.replace("DEFAULT 'x'", "DEFAULT 'q'")}
    items, _n = classify_cutover_defaults(
        {"app.t": _record(entry)}, {"app.t": _facts({"a": _col()})},
        current_ddls=ddls, still_excluded={"app.t": ["a"]})
    item = items[0]
    assert not item.auto
    expected = MANUAL_REASONS[reason].format(o=o, c="a", x="'x'", y="'q'")
    assert item.reason == expected
    assert "None" not in item.reason


def test_a_semicolon_outside_quotes_is_never_executed_inside_quotes_is():
    by, _n = _classify([_entry("a", "'x'::text; DROP TABLE t")])
    assert by["a"].executable is False and not by["a"].auto
    assert "run the statement yourself" in by["a"].reason
    by, _n = _classify([_entry("a", "'a;b'::text")], ddls={"app.t": D1.replace("'x'", "'a;b'")})
    assert by["a"].executable is True


def test_the_statement_and_the_nullable_columns():
    entries = [_entry("a", "'x'::character varying"), _entry("n", None, dropped_not_null=True),
               _entry("b", "'y'::text", dropped_not_null=False)]
    by, nullable = _classify(entries, {"a": _col(), "n": _col(), "b": _col()})
    assert by["a"].statement == 'ALTER TABLE "app"."t" ALTER COLUMN "a" SET DEFAULT (\'x\'::character varying);'
    assert "n" not in by  # nothing to restore: NOT NULL only
    assert nullable == ("app.t.a", "app.t.n")


def test_the_counts():
    snap = CutoverDefaultsSnapshot(state="loaded", items=tuple(_classify(
        [_entry("a", "'x'::character varying"), _entry("b", "'y'::text")],
        {"a": _col(), "b": _col(default="'y'::text")})[0].values()))
    assert (snap.pending, snap.restored, snap.total, len(snap.auto_items), snap.outstanding) == (1, 1, 2, 1, 1)


# ---------------------------------------------------------------------------
# The background fill
# ---------------------------------------------------------------------------


def _fill(ledger, *, facts=None, ddls=D1, excluded=("a",), found=None, raise_facts=False):
    calls = []

    def _read(names, columns):
        calls.append(list(names))
        if raise_facts:
            raise RuntimeError("connect failed")
        return facts if facts is not None else {n: _facts({c: _col() for c in columns[n]}) for n in names}

    snap = read_cutover_defaults(
        ledger=ledger, ledger_key=KEY, read_facts=_read, current_ddls=lambda: {"app.t": ddls},
        still_excluded=lambda: {"app.t": list(excluded)}, on_record_found=found, fill_key="k", generation=3)
    return snap, calls


def test_the_fill_reads_the_ledger_once_and_the_catalog_once():
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", "'x'::character varying"))})
    found = []
    snap, calls = _fill(ledger, found=lambda: found.append(1))
    assert snap.state == "loaded" and calls == [["app.t"]] and found == [1]
    assert snap.fill_key == "k" and snap.generation == 3 and [i.auto for i in snap.items] == [True]


def test_the_fill_states():
    empty, calls = _fill(InMemoryOverlayLedger())
    assert empty.state == "loaded" and empty.items == () and calls == []
    bad = InMemoryOverlayLedger()
    bad.read = lambda k: LedgerRead("unreadable", {}, "AccessDenied")
    snap, calls = _fill(bad)
    assert snap.state == "ledger_unreadable" and snap.error == "AccessDenied" and calls == []
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", "'x'::character varying"))})
    assert _fill(ledger, raise_facts=True)[0].state == "target_unreadable"
    assert _fill(ledger, facts={"app.t": None})[0].state == "target_unreadable"


# ---------------------------------------------------------------------------
# The settle check
# ---------------------------------------------------------------------------


class _Deployer:
    def __init__(self, stack=None, states=None, raises=None):
        self._stack, self._states, self._raises = stack, states or {}, raises

    def describe_stack_or_none(self, name):
        return self._stack

    def connector_state(self, name):
        if self._raises:
            raise self._raises
        return self._states.get(name.rsplit("-", 2)[-2] if False else name)


def _probe(**kw):
    deployer = _Deployer(**kw)
    return cutover_settle_probe(region="us-east-1", stack_name="cdc", deployer_factory=lambda *a, **k: deployer)


def test_the_settle_check():
    assert _probe() is None  # no stack
    assert "UPDATE_IN_PROGRESS" in _probe(stack=SimpleNamespace(stack_status="UPDATE_IN_PROGRESS"))
    assert _probe(stack=SimpleNamespace(stack_status="UPDATE_ROLLBACK_FAILED")) is None  # stuck: never blocks
    running = SimpleNamespace(stack_status="UPDATE_COMPLETE")
    assert "CREATING" in _probe(stack=running, states={"cdc-debezium-source": "CREATING"})
    assert "UPDATING" in _probe(stack=running, states={"cdc-dsql-sink": "UPDATING"})
    for state in ("RUNNING", "FAILED", "DELETING", None):
        assert _probe(stack=running, states={"cdc-debezium-source": state, "cdc-dsql-sink": state}) is None
    with pytest.raises(RuntimeError):
        _probe(stack=running, raises=RuntimeError("AccessDenied"))


# ---------------------------------------------------------------------------
# The restore job body
# ---------------------------------------------------------------------------


def _items(ledger, *columns, live=None):
    ledger.put_tables(KEY, {"app.t": _record(*[_entry(c, o) for c, o in columns])})
    items, _n = classify_cutover_defaults(
        ledger.read(KEY).tables, {"app.t": _facts(live or {c: _col() for c, _o in columns})},
        current_ddls={"app.t": D1}, still_excluded={"app.t": [c for c, _o in columns]})
    return items


class _Restore:
    def __init__(self, outcomes=None, fail=(), sleep=0.0):
        self.calls, self.inflight, self.max = [], 0, 0
        self._outcomes, self._fail, self._sleep = outcomes or {}, set(fail), sleep
        self._lock = threading.Lock()

    def __call__(self, table, column, expr, *, connection_factory):
        with self._lock:
            self.inflight += 1
            self.max = max(self.max, self.inflight)
        try:
            time.sleep(self._sleep)
            self.calls.append((table, column, expr))
            if column in self._fail:
                raise type("E", (Exception,), {"sqlstate": "42501"})("permission denied")
            status = self._outcomes.get(f"{table}.{column}", self._outcomes.get(column, "restored"))
            return RestoreOutcome(status, expr)
        finally:
            with self._lock:
                self.inflight -= 1


class _Handle:
    def __init__(self):
        self.cancelled, self.beats = False, 0

    def heartbeat(self):
        self.beats += 1


def _job(ledger, items, restore, *, probe=None):
    return run_cutover_default_restore(_Handle(), items=items, ledger=ledger, ledger_key=KEY, restore=restore,
                                       connection_factory=lambda: None, settle_probe=probe, now=lambda: T0)


@pytest.mark.parametrize("probe", [lambda: "the cdc-stack is UPDATE_IN_PROGRESS", lambda: "cdc-debezium-source is CREATING",
                                   lambda: (_ for _ in ()).throw(RuntimeError("AccessDenied"))])
def test_a_settle_refusal_restores_nothing(probe):
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"))
    restore = _Restore()
    outcome = _job(ledger, items, restore, probe=probe)
    assert restore.calls == [] and outcome.refused_by_settle and outcome.refused.startswith("not restored: ")
    assert "Wait until both CDC connectors are RUNNING" in outcome.refused


def test_the_units_run_serially_and_the_ledger_is_written_once():
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"), ("b", "'y'::text"), ("n", "'z'::text"))
    writes = []
    real = ledger.merge_tables
    ledger.merge_tables = lambda k, m: writes.append(sorted(m)) or real(k, m)
    restore = _Restore(outcomes={"b": "done"}, fail={"n"}, sleep=0.01)
    outcome = _job(ledger, items, restore)
    assert restore.max == 1 and [c for _t, c, _e in restore.calls] == ["a", "b", "n"]
    assert writes == [["app.t"]]
    assert outcome.restored == ("app.t.a",) and outcome.already_done == ("app.t.b",)
    assert list(outcome.failed) == ["app.t.n"] and "42501" in outcome.failed["app.t.n"]
    rec = ledger.read(KEY).tables["app.t"]
    assert rec.column("a").restored_at == T0 and rec.column("b").restored_at == T0
    assert rec.column("n").restored_at is None and "permission denied" in rec.column("n").restore_error
    assert outcome.statements == ('ALTER TABLE "app"."t" ALTER COLUMN "a" SET DEFAULT (\'x\'::character varying);',)


def test_a_diverged_column_is_never_overwritten_and_a_changed_original_is_skipped():
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"), ("b", "'y'::text"))
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", "'x'::character varying"), _entry("b", "'CHANGED'::text"))})
    restore = _Restore(outcomes={"a": "diverged"})
    outcome = _job(ledger, items, restore)
    assert outcome.diverged == ("app.t.a",) and outcome.skipped == ("app.t.b",)
    assert [c for _t, c, _e in restore.calls] == ["a"]


def test_gone_columns_are_pruned_and_a_gone_table_is_deleted():
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", "'x'::character varying"), _entry("b", "'y'::text")),
                            "app.g": _record(_entry("a", "'x'::character varying"), table="app.g")})
    tables = ledger.read(KEY).tables
    items, _n = classify_cutover_defaults(
        tables, {"app.t": _facts({"a": _col(), "b": None}), "app.g": _facts({}, exists=False)},
        current_ddls={"app.t": D1}, still_excluded={"app.t": ["a", "b"], "app.g": ["a"]})
    outcome = _job(ledger, [i for i in items if i.status in ("pending", "gone")], _Restore(outcomes={"app.t.b": "gone", "app.g.a": "gone"}))
    after = ledger.read(KEY).tables
    assert "app.g" not in after and after["app.t"].column("b") is None and after["app.t"].column("a") is not None
    assert set(outcome.gone) == {"app.t.b", "app.g.a"}


def test_a_failing_ledger_write_is_an_honest_warning():
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"))
    ledger.merge_tables = lambda k, m: (_ for _ in ()).throw(OverlayLedgerError("SlowDown"))
    outcome = _job(ledger, items, _Restore())
    assert outcome.restored == ("app.t.a",) and outcome.ledger_warning == cd.LEDGER_WRITE_WARNING


def test_an_unreadable_ledger_at_job_time_restores_nothing():
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"))
    ledger.read = lambda k: LedgerRead("unreadable", {}, "x")
    restore = _Restore()
    outcome = _job(ledger, items, restore)
    assert restore.calls == [] and outcome.refused == cd.LEDGER_UNREADABLE_REFUSAL and not outcome.refused_by_settle


# ---------------------------------------------------------------------------
# ValidationState: the cache, the job slot, the clearers
# ---------------------------------------------------------------------------


def _vstate():
    from dsql_migrator.ui.validation import ValidationState

    return ValidationState()


def test_a_fill_that_raced_an_invalidation_is_dropped_and_fills_are_single_flight():
    state = _vstate()
    gen = state.begin_cutover_defaults_fill()
    assert state.begin_cutover_defaults_fill() is None  # single flight
    state.invalidate_cutover_defaults()
    assert state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded"), gen) is False
    assert state.cutover_defaults is None and state.cutover_defaults_filling is False
    gen = state.begin_cutover_defaults_fill()
    assert state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded", fill_key="k",
                                                                      filled_monotonic=100.0), gen)
    assert not state.cutover_defaults_stale("k", 120.0) and state.cutover_defaults_stale("k", 200.0)
    assert state.cutover_defaults_stale("other", 120.0)


def test_note_cdc_start_clears_the_tick_and_the_cache():
    state = _vstate()
    gen = state.begin_cutover_defaults_fill()
    state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded"), gen)
    state.set_cutover_cdc_drained_confirmed(True)
    state.note_cdc_start()
    assert state.cutover_cdc_drained_confirmed is False and state.cutover_defaults is None


@pytest.mark.parametrize("clear", ["clear_cutover_outcomes", "set_result", "clear_outputs"])
def test_the_clearers_drop_the_outcome_and_the_cache(clear):
    from dsql_migrator.core.models import ValidationMode, ValidationReport

    state = _vstate()
    gen = state.begin_cutover_defaults_fill()
    state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded"), gen)
    state.set_cutover_default_restore_outcome(cd.CutoverRestoreOutcome(restored=("a",)))
    state.set_cutover_default_restore_job("job-r")
    if clear == "set_result":
        state.set_result(ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[]))
    else:
        getattr(state, clear)()
    assert state.cutover_defaults is None and state.cutover_default_restore_outcome is None
    if clear == "clear_cutover_outcomes":
        assert state.cutover_default_restore_job_id == "job-r"  # the reverse gate still sees it


def test_the_new_state_is_never_snapshotted_and_start_over_resets_it():
    import pathlib

    from dsql_migrator.ui.validation import ValidationStore

    root = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
    for rel in ("ui/session_persistence.py", "core/session_state_store.py"):
        text = (root / rel).read_text(encoding="utf-8")
        assert "cutover_defaults" not in text and "cutover_default_restore" not in text
    store = ValidationStore()
    state = store.get_or_create("s")
    state.set_cutover_default_restore_job("j")
    state.set_cutover_default_restore_outcome(cd.CutoverRestoreOutcome())
    store.reset_in_place("s")
    assert state.cutover_default_restore_job_id is None and state.cutover_default_restore_outcome is None


# ---------------------------------------------------------------------------
# The click-time gates and the job wrapper
# ---------------------------------------------------------------------------


class _Jobs:
    def __init__(self, live=()):
        self.live, self.submitted = set(live), []

    def get_status(self, job_id):
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id is None:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(status="RUNNING" if job_id in self.live else "COMPLETED")

    def submit(self, work):
        self.submitted.append(work)
        return f"job-{len(self.submitted)}"


def _click(*, cdc=False, drained=True, jobs=None, snap=None, request=None, mstate=None, restore=None,
           probe=None, ledger=None):
    import dsql_migrator.ui.validation as uiv

    state = _vstate()
    ledger = ledger or InMemoryOverlayLedger()
    if snap is None:
        items = _items(ledger, ("a", "'x'::character varying"))
        snap = CutoverDefaultsSnapshot(state="loaded", items=tuple(items))
    gen = state.begin_cutover_defaults_fill()
    state.finish_cutover_defaults_fill(snap, gen)
    state.set_cutover_cdc_drained_confirmed(drained)
    notes, refreshes = [], []
    session = SimpleNamespace(target_config=TARGET, aws_profile=None,
                              source_config=SimpleNamespace(), cdc_in_use=cdc)
    mstate = mstate or SimpleNamespace(job_id=None, cdc_action_kind=None, cdc_deploy_job_id=None,
                                       cdc_stack_name="cdc", cdc_deploy_role_arn=None)
    jobs = jobs if jobs is not None else _Jobs()
    orig = uiv._cdc_in_use
    uiv._cdc_in_use = lambda _s: cdc
    try:
        uiv._run_cutover_default_restore(
            session, state, mstate, request=request, overlay_ledger=ledger, job_manager=jobs,
            refresh=lambda: refreshes.append(1), notify=lambda m, **k: notes.append(m),
            settle_probe=probe, restore=restore or _Restore(), connection_factory=lambda: None)
    finally:
        uiv._cdc_in_use = orig
    return state, jobs, notes, ledger


@pytest.mark.parametrize("case", ["full_load", "start_cdc", "restoring", "not_drained", "not_loaded", "row_gone"])
def test_the_click_time_gates(case):
    kwargs = {}
    if case == "full_load":
        kwargs["jobs"] = _Jobs(live={"fl"})
        kwargs["mstate"] = SimpleNamespace(job_id="fl", cdc_action_kind=None, cdc_deploy_job_id=None,
                                           cdc_stack_name="cdc", cdc_deploy_role_arn=None)
    elif case == "start_cdc":
        kwargs["jobs"] = _Jobs(live={"sc"})
        kwargs["mstate"] = SimpleNamespace(job_id=None, cdc_action_kind="start", cdc_deploy_job_id="sc",
                                           cdc_stack_name="cdc", cdc_deploy_role_arn=None)
    elif case == "not_drained":
        kwargs.update(cdc=True, drained=False)
    elif case == "not_loaded":
        kwargs["snap"] = CutoverDefaultsSnapshot(state="ledger_unreadable")
    elif case == "row_gone":
        kwargs["request"] = ("app.t", "nope")
    if case == "restoring":
        import dsql_migrator.ui.validation as uiv

        jobs = _Jobs(live={"job-1"})
        state, jobs, notes, _l = _click(jobs=jobs)
        assert len(jobs.submitted) == 1
        state2 = state
        state2.set_cutover_default_restore_job("job-1")
        notes2 = []
        uiv._run_cutover_default_restore(
            SimpleNamespace(target_config=TARGET, aws_profile=None), state2, None, request=None,
            overlay_ledger=InMemoryOverlayLedger(), job_manager=jobs, refresh=lambda: None,
            notify=lambda m, **k: notes2.append(m))
        assert len(jobs.submitted) == 1 and notes2 == ["The DEFAULTs are already being restored."]
        return
    state, jobs, notes, _l = _click(**kwargs)
    assert jobs.submitted == [] and len(notes) == 1


def test_a_click_submits_one_job_that_records_the_outcome(monkeypatch):
    import dsql_migrator.ui.validation as uiv

    logged = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw.get("status"))))
    state, jobs, notes, ledger = _click()
    assert notes == [] and len(jobs.submitted) == 1 and state.cutover_default_restore_job_id == "job-1"
    gen_before = state.cutover_defaults
    jobs.submitted[0](_Handle())
    assert state.cutover_default_restore_outcome.restored == ("app.t.a",)
    assert state.cutover_defaults is None and gen_before is not None  # re-read after the job
    assert ("excluded-column DEFAULTs restored (cut-over)", uiv.ActivityStatus.SUCCESS) in logged


def test_a_settle_refusal_clears_the_attestation_and_logs_a_warning(monkeypatch):
    import dsql_migrator.ui.validation as uiv

    logged = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw.get("status"))))
    restore = _Restore()
    state, jobs, _n, _l = _click(cdc=True, drained=True, restore=restore,
                                 probe=lambda: "cdc-debezium-source is CREATING")
    jobs.submitted[0](_Handle())
    assert restore.calls == [] and state.cutover_cdc_drained_confirmed is False
    assert state.cutover_default_restore_outcome.refused_by_settle
    assert ("excluded-column DEFAULTs not restored (cut-over)", uiv.ActivityStatus.WARNING) in logged


def test_a_per_row_click_restores_only_that_manual_row():
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"), ("b", "'y'::text"))
    manual = tuple(i if i.column == "a" else i.__class__(**{**i.__dict__, "auto": False, "reason": "r"})
                   for i in items)
    restore = _Restore()
    state, jobs, notes, _l = _click(snap=CutoverDefaultsSnapshot(state="loaded", items=manual),
                                    request=("app.t", "b"), restore=restore, ledger=ledger)
    jobs.submitted[0](_Handle())
    assert [c for _t, c, _e in restore.calls] == ["b"]


# ---------------------------------------------------------------------------
# The reverse gate and the builder wiring
# ---------------------------------------------------------------------------


def test_the_reverse_gate_reason():
    from dsql_migrator.ui.data_migration._models import CUTOVER_RESTORE_BLOCK_TEXT, cutover_restore_block_reason

    assert cutover_restore_block_reason(SimpleNamespace(cutover_restore_in_flight=lambda: True)) == \
        CUTOVER_RESTORE_BLOCK_TEXT
    assert cutover_restore_block_reason(SimpleNamespace(cutover_restore_in_flight=lambda: False)) is None
    assert cutover_restore_block_reason(SimpleNamespace(cutover_restore_in_flight=None)) is None
    boom = SimpleNamespace(cutover_restore_in_flight=lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert cutover_restore_block_reason(boom) is None


def test_start_cdc_refuses_while_cut_over_restores(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration import DataMigrationState

    notices = []
    monkeypatch.setattr(cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    state = DataMigrationState()
    state.cutover_restore_in_flight = lambda: True
    jobs = _Jobs()
    cdc_ui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), state, jobs, lambda: None)
    assert jobs.submitted == [] and notices[0]["header"] == "Start CDC is waiting for cut over"


def test_the_builder_wires_the_restore_in_flight_callable():
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import DataMigrationStore, build_data_migration_screen
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore

    for with_store in (True, False):
        migration_store = DataMigrationStore()
        build_data_migration_screen(
            SessionStore(), "s1", job_manager=JobManager(), eval_store=EvaluationStore(),
            migration_store=migration_store, conv_store=SchemaConversionStore(),
            validation_store=ValidationStore() if with_store else None)
        fn = migration_store.get_or_create("s1").cutover_restore_in_flight
        assert (fn is not None) is with_store
        if with_store:
            assert fn() is False


def test_effective_target_ddls_follows_the_session_source_engine():
    from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
    from dsql_migrator.ui.data_migration._models import effective_target_ddls

    inv = SourceInventory(tables=[TableDef(name="app.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="c", mysql_type="text")])])
    conv = SimpleNamespace(edited_target_ddls={}, preserve_foreign_keys=True)
    assert effective_target_ddls(None, conv, SourceType.MYSQL) is None
    assert effective_target_ddls(inv, None, SourceType.MYSQL) is None
    mysql = effective_target_ddls(inv, conv, SourceType.MYSQL)
    no_fk = effective_target_ddls(inv, SimpleNamespace(edited_target_ddls={}, preserve_foreign_keys=False),
                                  SourceType.MYSQL)
    assert mysql == no_fk and "app.t" in mysql
    # The builder's provider uses the session's engine, also for a restored session with no
    # source connection (it returned None there before).
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import DataMigrationStore, build_data_migration_screen
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    sessions, store = SessionStore(), DataMigrationStore()
    session = sessions.get_or_create("s1")
    session.restored_source_type = SourceType.POSTGRES
    build_data_migration_screen(sessions, "s1", job_manager=JobManager(), eval_store=EvaluationStore(),
                                migration_store=store, conv_store=SchemaConversionStore())
    provider = store.get_or_create("s1").effective_target_ddls
    # A type the two engines convert differently, so a MySQL fallback cannot pass.
    inv2 = SourceInventory(tables=[TableDef(name="app.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="c", mysql_type="text[]")])])
    fresh = SchemaConversionStore().get_or_create("s1")
    assert effective_target_ddls(inv2, fresh, SourceType.POSTGRES) != effective_target_ddls(
        inv2, fresh, SourceType.MYSQL)
    assert provider(inv2) == effective_target_ddls(inv2, fresh, SourceType.POSTGRES)


def test_the_runner_and_every_retry_check_the_reverse_gate_before_submitting():
    import ast
    import inspect

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    for name in ("runner", "_run_retry_for"):
        fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name]
        assert fns, name
        fn = fns[0]
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)]
        gate = min(c.lineno for c in calls if getattr(c.func, "id", None) == "cutover_restore_block_reason")
        submits = [c.lineno for c in calls if getattr(c.func, "attr", None) == "submit"]
        assert submits and gate < min(submits), name
        (guard,) = [n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == "_restore_block"]
        assert any(isinstance(b, ast.Return) for b in guard.body), name


# ---------------------------------------------------------------------------
# The cut-over screen: the background fill, the FK click-time re-check
# ---------------------------------------------------------------------------


def _screen(monkeypatch, *, ledger, cdc=False, fk_pending=0):
    import asyncio
    import sys
    import types

    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import TableValidationResult, ValidationMode, ValidationReport
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen

    from tests.test_ui_validation import _ScreenUi

    class _Ui(_ScreenUi):
        def __init__(self):
            super().__init__()
            self.timers, self.notes = [], []

        def timer(self, _interval, callback, once=False):
            self.timers.append(callback)
            return self._El(self)

        def notify(self, message, **_k):
            self.notes.append(message)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    recorder = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui, fake.run = recorder, _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: cdc)
    monkeypatch.setattr(uiv, "_cutover_pending_foreign_keys", lambda *a, **k: fk_pending)
    applied = []
    monkeypatch.setattr(uiv, "_run_cutover_foreign_keys", lambda *a, **k: applied.append(1))
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_target(TARGET)
    vstore, mstore = ValidationStore(), DataMigrationStore()
    content, _runner = build_cutover_screen(
        sessions, "s1", validation_store=vstore, migration_store=mstore, overlay_ledger=ledger,
        overlay_facts_reader=lambda names, cols: {n: _facts({c: _col() for c in cols[n]}) for n in names})
    state = vstore.get_or_create("s1")
    state.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[TableValidationResult(
        table="app.t", source_row_count=1, target_row_count=1, row_count_match=True, matched=True)]))
    refreshes = []

    def render():
        recorder.timers.clear()
        content(lambda: refreshes.append(1))

    return SimpleNamespace(render=render, ui=recorder, state=state, refreshes=refreshes, applied=applied,
                           mstate=mstore.get_or_create("s1"), run=lambda cb: asyncio.run(cb()))


class _GuardedLedger(InMemoryOverlayLedger):
    """Raises on any read while ``armed`` (a render must do no store I/O)."""

    armed = True

    def read(self, key):
        if self.armed:
            raise AssertionError("store I/O during render")
        return super().read(key)


def test_the_render_does_no_store_io_and_one_fill_lands_the_cache(monkeypatch):
    ledger = _GuardedLedger()
    ledger.armed = False
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", "'x'::character varying"))})
    ledger.armed = True
    s = _screen(monkeypatch, ledger=ledger)
    s.render()
    (callback,) = s.ui.timers
    assert s.state.cutover_defaults is None
    s.render()  # before the first timer fires: arms another (the claim is in the fill)
    (late,) = s.ui.timers
    ledger.armed = False  # the fill itself runs off the render
    reads = []
    real = ledger.read
    ledger.read = lambda k: reads.append(k) or real(k)
    s.run(callback)
    s.run(late)  # the fill already landed: no second read
    assert s.state.cutover_defaults.state == "loaded" and s.refreshes == [1] and len(reads) == 1
    assert s.mstate.overlay_ledger_in_use is True  # a record marks the session
    ledger.armed = True
    s.render()
    assert s.ui.timers == []  # fresh: no second fill


def test_a_fill_key_change_schedules_one_new_fill(monkeypatch):
    ledger = InMemoryOverlayLedger()
    s = _screen(monkeypatch, ledger=ledger)
    s.render()
    s.run(s.ui.timers[0])
    s.render()
    assert s.ui.timers == []
    s.mstate.set_lob_exclusion("app.t", "c", True)  # the exclusion set changed
    s.render()
    assert len(s.ui.timers) == 1


def test_no_ledger_handle_means_no_fill(monkeypatch):
    s = _screen(monkeypatch, ledger=None)
    s.render()
    assert s.ui.timers == []


def test_a_stale_tab_cannot_apply_foreign_keys_after_a_cdc_start(monkeypatch):
    s = _screen(monkeypatch, ledger=None, cdc=True, fk_pending=2)
    s.state.set_cutover_cdc_drained_confirmed(True)
    s.render()
    clicks = [cb for text, cb in s.ui.clicks if "Apply" in text and "foreign key" in text.lower()]
    assert clicks, [t for t, _c in s.ui.clicks]
    s.state.note_cdc_start()  # a CDC start after this render
    clicks[0]()
    assert s.applied == [] and s.ui.notes and "drained" in s.ui.notes[-1]
    s.state.set_cutover_cdc_drained_confirmed(True)
    clicks[0]()
    assert s.applied == [1]


# ---------------------------------------------------------------------------
# Review fixes (P2-15/16 adversarial review)
# ---------------------------------------------------------------------------


def test_a_timer_dropped_before_it_fires_never_leaks_the_claim(monkeypatch):
    ledger = InMemoryOverlayLedger()
    s = _screen(monkeypatch, ledger=ledger)
    s.render()
    assert len(s.ui.timers) == 1  # dropped: a refresh deleted it before it fired
    s.render()
    (callback,) = s.ui.timers  # a replacement is armed
    s.run(callback)
    assert s.state.cutover_defaults is not None and s.state.cutover_defaults.state == "loaded"


def test_an_invalidation_during_a_fill_arms_a_fill_for_the_new_inputs(monkeypatch):
    state = _vstate()
    old = state.begin_cutover_defaults_fill()
    state.invalidate_cutover_defaults()  # e.g. Check again while the read runs
    assert state.cutover_defaults_filling is False  # the running fill is for old inputs
    new = state.begin_cutover_defaults_fill()
    assert new is not None and new != old
    assert state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded"), old) is False
    assert state.cutover_defaults_filling is True  # the old fill did not release the new claim
    assert state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded"), new) is True


def test_a_dropped_result_re_renders_so_a_new_fill_is_armed(monkeypatch):
    ledger = InMemoryOverlayLedger()
    s = _screen(monkeypatch, ledger=ledger)
    s.render()
    (callback,) = s.ui.timers
    real = ledger.read

    def _read_then_invalidate(key):
        s.state.invalidate_cutover_defaults()  # the inputs moved during the read
        return real(key)

    ledger.read = _read_then_invalidate
    s.run(callback)
    assert s.state.cutover_defaults is None and s.refreshes == [1]  # re-rendered
    ledger.read = real
    s.render()
    (again,) = s.ui.timers
    s.run(again)
    assert s.state.cutover_defaults.state == "loaded"


def test_a_hung_fill_claim_is_re_taken_after_the_watchdog_bound():
    state = _vstate()
    assert state.begin_cutover_defaults_fill(now_monotonic=100.0) is not None
    assert state.begin_cutover_defaults_fill(now_monotonic=110.0) is None
    assert state.begin_cutover_defaults_fill(now_monotonic=100.0 + 2 * cd.CUTOVER_DEFAULTS_TTL_SECONDS + 1) \
        is not None


def test_exclude_and_reload_checks_the_reverse_gate_before_writing_anything():
    import ast
    import inspect

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    (fn,) = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "exclude_lob_and_reload"]
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)]
    gate = min(c.lineno for c in calls if getattr(c.func, "id", None) == "cutover_restore_block_reason")
    writes = [c.lineno for c in calls if getattr(c.func, "attr", None) in ("set_lob_exclusion", "set_replace_targets")]
    assert writes and gate < min(writes)


def test_the_not_single_expression_reason_is_stated_once():
    o = "f(1), 2"
    by, _n = _classify([_entry("a", o)])  # (A) holds, but it is not one expression
    assert by["a"].reason == cd.NOT_EXECUTABLE_REASON.format(o=o)
    by, _n = _classify([_entry("a", o)], excluded={"app.t": []})
    assert by["a"].reason.count(cd.NOT_EXECUTABLE_REASON.format(o=o)) == 1
    assert by["a"].reason.startswith(NOT_EXCLUDED_REASON.format(o=o))


def test_a_stale_tab_click_after_the_inputs_changed_restores_nothing():
    import dsql_migrator.ui.validation as uiv

    state = _vstate()
    ledger = InMemoryOverlayLedger()
    items = _items(ledger, ("a", "'x'::character varying"))
    gen = state.begin_cutover_defaults_fill()
    state.finish_cutover_defaults_fill(CutoverDefaultsSnapshot(state="loaded", items=tuple(items), fill_key="k1"), gen)
    jobs, notes = _Jobs(), []
    uiv._run_cutover_default_restore(
        SimpleNamespace(target_config=TARGET, aws_profile=None), state,
        SimpleNamespace(job_id=None, cdc_action_kind=None, cdc_deploy_job_id=None),
        request=None, overlay_ledger=ledger, job_manager=jobs, refresh=lambda: None,
        notify=lambda m, **k: notes.append(m), restore=_Restore(), fill_key_now=lambda: "k2")
    assert jobs.submitted == [] and "changed since this screen was drawn" in notes[0]
    assert state.cutover_defaults is None  # re-read
