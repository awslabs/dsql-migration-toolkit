# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-12: the Start Full Load dialog and the column-exclusion overlay.

The dialog makes its own ledger read (before the target probe), one facts read, and
works out which tables the NULL contract forces to Drop & reload -- with the engine's
own verdict -- lists the exact ALTERs, and consumes or drops the forced set on close.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.column_exclusion import set_digest
from dsql_migrator.core.models import ColumnDef, StepStatus, TableDef
from dsql_migrator.core.overlay_ledger import LedgerRead, OverlayLedgerColumn, OverlayLedgerTable
from dsql_migrator.core.target_introspector import ColumnNullFacts as F
from dsql_migrator.core.target_introspector import TableOverlayFacts as T
from dsql_migrator.ui import data_migration as dm
from dsql_migrator.ui.data_migration import DataMigrationState, OverlayDialogView
from dsql_migrator.ui.data_migration._models import exclusion_forced_replace

from tests.test_ui_data_migration import _ConfirmDialogUi, _StubConnector, _StubSession

PAGES = "shop.pages"


def _record(columns=("summary",), empty=True, table=PAGES):
    return OverlayLedgerTable(
        table=table, set_digest=set_digest(list(columns)), set_columns=list(columns), empty_when_applied=empty,
        applied_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        columns=[OverlayLedgerColumn(column=c, original_default="'x'::text", dropped_not_null=True) for c in columns])


def _facts(rows=True, **cols):
    return T(True, rows, None, {c: f for c, f in cols.items()})


def _view(overlay=None, *, excluded=None, source=("id", "summary", "body"), unfinished=(), ddl=None, indexes=()):
    overlay = {PAGES: frozenset({"summary"})} if overlay is None else overlay
    return OverlayDialogView(
        overlay_by_table=overlay,
        excluded_by_table=overlay if excluded is None else excluded,
        source_columns_for=lambda _t: list(source),
        ddl_facts_for=lambda _t: ddl if ddl is not None else {"summary": F(True, "'x'::text")},
        index_ddls_for=lambda _t: list(indexes),
        recreated_unfinished=frozenset(unfinished),
    )


def _forced(view, facts, ledger=None, *, cdc=False, tables=(PAGES,)):
    return exclusion_forced_replace(
        list(tables), overlay_by_table=view.overlay_by_table, excluded_by_table=view.excluded_by_table,
        source_columns_for=view.source_columns_for, facts=facts, ledger_tables=ledger or {},
        cdc_coexisting=cdc, recreated_unfinished=view.recreated_unfinished, index_ddls_for=view.index_ddls_for)


# ---------------------------------------------------------------------------
# The forced-set matrix (pure) and its agreement with the engine
# ---------------------------------------------------------------------------


SUMMARY = F(True, "'x'::text")
MATRIX = [
    # (id, view kwargs, facts, ledger, cdc, forced?, reason fragment)
    ("no record, rows", {}, {PAGES: _facts(summary=SUMMARY)}, {}, False, True, "different exclusion set"),
    ("same digest, all rows under it", {}, {PAGES: _facts(summary=SUMMARY)}, {PAGES: _record()}, False, False, None),
    ("different digest", {"overlay": {PAGES: frozenset({"summary"})}}, {PAGES: _facts(summary=SUMMARY, body=F(False, None))},
     {PAGES: _record(("body",))}, False, True, "different exclusion set"),
    ("empty set, re-included column", {"overlay": {}, "excluded": {}}, {PAGES: _facts(summary=F(False, None))},
     {PAGES: _record()}, False, True, "hold NULL from an earlier exclusion set"),
    ("re-included under CDC", {"overlay": {}, "excluded": {}}, {PAGES: _facts(summary=F(False, None))},
     {PAGES: _record()}, True, False, None),
    ("recorded column gone from the source", {"overlay": {}, "excluded": {}, "source": ("id", "body")},
     {PAGES: _facts(summary=F(False, None))}, {PAGES: _record()}, False, False, None),
    ("overlay columns absent live", {}, {PAGES: _facts()}, {}, False, False, None),
    ("empty table", {}, {PAGES: _facts(rows=False, summary=SUMMARY)}, {}, False, False, None),
    ("live computed overlay column", {}, {PAGES: _facts(summary=F(False, None, generated=True))}, {}, False, False, None),
    ("recreated and unfinished", {"unfinished": (PAGES,)}, {PAGES: _facts(rows=False, summary=SUMMARY)}, {}, False,
     True, "recreated by the last run and not finished"),
]


@pytest.mark.parametrize("case", MATRIX, ids=[m[0] for m in MATRIX])
def test_the_forced_matrix(case):
    _id, view_kw, facts, ledger, cdc, want_forced, fragment = case
    outcome = _forced(_view(**view_kw), facts, ledger, cdc=cdc)
    assert (PAGES in outcome.forced) is want_forced, outcome
    if fragment:
        assert fragment in outcome.forced[PAGES]


def test_unknown_facts_make_a_table_undecided_never_forced():
    outcome = _forced(_view(), {PAGES: None})
    assert outcome.forced == {} and outcome.undecided == (PAGES,)


def test_under_cdc_nothing_is_forced_and_each_candidate_is_warned():
    outcome = _forced(_view(), {PAGES: _facts(summary=SUMMARY)}, cdc=True)
    assert outcome.forced == {} and "cannot be dropped while CDC streams" in outcome.appended_warnings[PAGES]
    unfinished = _forced(_view(unfinished=(PAGES,), indexes=["CREATE INDEX ASYNC i ON t (a)",
                                                             "CREATE UNIQUE INDEX ASYNC u ON t (b)"]),
                         {PAGES: _facts(rows=False, summary=SUMMARY)}, cdc=True)
    warning = unfinished.appended_warnings[PAGES]
    assert "CREATE INDEX ASYNC i" in warning and "CREATE UNIQUE INDEX ASYNC u" in warning


@pytest.mark.parametrize("case", MATRIX, ids=[m[0] for m in MATRIX])
def test_the_forced_predicate_matches_the_engine_check(case):
    """The dialog and the engine's fail-closed check give the same verdict for every row."""
    from dsql_migrator.ui.data_migration._full_load_engine import DataMigrationInputs, _append_verdict, _live_overlay

    _id, view_kw, facts, ledger, cdc, _want, _fragment = case
    view = _view(**view_kw)
    if view.recreated_unfinished:
        pytest.skip("clause 3 (recreated_unfinished) is the retry's replace set, not the append verdict")
    table = TableDef(name=PAGES, primary_key=["id"], columns=[ColumnDef(name=c, mysql_type="text")
                                                           for c in view.source_columns_for(PAGES)])
    inputs = SimpleNamespace(excluded_lob_columns=view.excluded_by_table, cdc_coexisting=cdc)
    live, computed = _live_overlay(facts[PAGES], view.overlay_by_table.get(PAGES, ()))
    refusal, exemption, _re = _append_verdict(inputs, table, facts[PAGES], ledger.get(PAGES), live, computed)
    engine_refuses = refusal is not None and exemption is None
    assert engine_refuses is (PAGES in _forced(view, facts, ledger, cdc=cdc).forced)
    assert DataMigrationInputs  # the real type the engine is built on


# ---------------------------------------------------------------------------
# Driving the real dialog
# ---------------------------------------------------------------------------


class _Btn:
    def __init__(self, ui, text):
        self.ui, self.text, self.enabled, self.color = ui, text, True, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def props(self, value=""):
        if "color=" in value:
            self.color = value.split("color=")[1].split()[0]
        return self

    def set_text(self, text):
        self.text = text

    def disable(self):
        self.enabled = False

    def enable(self):
        self.enabled = True

    def __getattr__(self, _name):
        return lambda *_a, **_k: self


class _Dialog(_ConfirmDialogUi._El):
    def __init__(self, ui):
        super().__init__(ui)
        self.value_handlers = []

    def on_value_change(self, handler):
        self.value_handlers.append(handler)
        return self

    def close(self):
        for handler in self.value_handlers:
            handler(SimpleNamespace(value=False))
        return self


class _Radio(_ConfirmDialogUi._El):
    def __init__(self, ui, on_change):
        super().__init__(ui)
        self.on_change, self.value_handlers = on_change, []

    def on_value_change(self, handler):
        self.value_handlers.append(handler)
        return self

    def pick(self, value):
        self.on_change(SimpleNamespace(value=value))
        for handler in self.value_handlers:
            handler(SimpleNamespace(value=value))


class _DialogUi(_ConfirmDialogUi):
    def __init__(self):
        super().__init__()
        self.btns = []
        self.dialogs = []
        self.radios = []
        self.order = []

    def button(self, text="", *_a, on_click=None, **_k):
        super().button(text, on_click=on_click)
        btn = _Btn(self, str(text))
        self.btns.append(btn)
        return btn

    def dialog(self, *_a, **_k):
        d = _Dialog(self)
        self.dialogs.append(d)
        return d

    def radio(self, options=None, *_a, on_change=None, **_k):
        super().radio(options)
        r = _Radio(self, on_change)
        self.radios.append(r)
        return r

    def start(self):
        return self.btns[-1]  # the confirm button is created last


def _open(monkeypatch, *, state=None, view=None, ledger=None, facts=None, rows=(PAGES,), probe_raises=False,
          cdc=False, tables=(PAGES,), order=None, job=None, status=None, click="Start", retry_tables=None,
          present=None):
    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()), raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    order = order if order is not None else []
    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())

    def _rows(names, **_k):
        order.append("probe")
        if probe_raises:
            raise RuntimeError("could not connect")
        return [n for n in names if n in rows]

    monkeypatch.setattr(fl, "tables_with_rows", _rows)
    monkeypatch.setattr(fl, "target_primary_keys", lambda names, **k: {n: ["id"] for n in names})
    monkeypatch.setattr(fl, "tables_present",
                        lambda names, **k: [n for n in names if present is None or n in present])
    facts_calls = []

    def _facts_read(names, cols, **_k):
        facts_calls.append((list(names), {t: sorted(c) for t, c in cols.items()}))
        return {n: (facts or {}).get(n) for n in names}

    monkeypatch.setattr(fl, "target_overlay_facts", _facts_read)
    monkeypatch.setattr(fl, "cdc_streaming_started", lambda *a, **k: cdc)
    reads = []

    def _ledger_read():
        order.append("ledger")
        reads.append(1)
        if isinstance(ledger, Exception):
            raise ledger
        return ledger

    ui = _DialogUi()
    state = state if state is not None else DataMigrationState()
    started = []

    class _NoJobs:
        def get_status(self, job_id):
            from dsql_migrator.core.job_manager import JobNotFoundError

            raise JobNotFoundError(job_id)

        def get_error(self, job_id):
            return None

    dm._render_full_load_step(
        ui, state, _NoJobs(), _StubSession(), job=job, status=status or StepStatus.NOT_STARTED,
        selected_names=list(tables), retry_tables=retry_tables,
        guard_reason=None, start_full_load=lambda: started.append(state.take_replace_targets()),
        retry_failed_load=lambda *a: None, reload_table=lambda *a: None,
        accept_quarantine_and_continue=lambda *a: None, stop_full_load=lambda *a: None, refresh=lambda *a: None,
        schema_recreate_candidates=lambda: [],
        overlay_ledger_read=_ledger_read if ledger is not None or ledger is None and view is not None else None,
        exclusion_overlay_view=(lambda _t: view) if view is not None else None,
    )
    start = [h for (label, h) in ui.handlers if click in label]
    asyncio.run(start[0]())
    return SimpleNamespace(ui=ui, state=state, reads=reads, facts_calls=facts_calls, started=started, order=order)


def _body(ui):
    return " ".join(ui.texts)


def _confirm(ui):
    handler = [h for (label, h) in ui.handlers if label == ui.start().text or label.startswith(ui.start().text[:4])][-1]
    handler()


def test_a_forced_table_gets_the_negative_confirm_and_its_reason(monkeypatch):
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.state.exclusion_forced_replace.keys() == {PAGES}
    assert ctx.ui.start().text == "Drop 1 table, recreate and load" and ctx.ui.start().color == "negative"
    body = _body(ctx.ui)
    assert "1 table will be dropped and reloaded" in body and "different exclusion set" in body
    assert ctx.ui.radios == []  # the only populated table is forced: no choice to draw


def test_the_label_counts_forced_plus_the_drop_choice(monkeypatch):
    other = "shop.other"
    view = _view()
    ctx = _open(monkeypatch, view=view, ledger=LedgerRead("absent"), rows=(PAGES, other), tables=(PAGES, other),
                facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.ui.start().text == "Drop 1 table, recreate and load"
    (radio,) = ctx.ui.radios
    radio.pick("drop")
    assert ctx.ui.start().text == "Drop 2 tables, recreate and load" and ctx.ui.start().color == "negative"
    radio.pick("append")
    assert ctx.ui.start().text == "Drop 1 table, recreate and load" and ctx.ui.start().color == "negative"


def test_the_dialog_lists_the_exact_alters_in_run_order(monkeypatch):
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("present", {PAGES: _record()}), rows=(),
                facts={PAGES: _facts(rows=False, summary=SUMMARY)})
    body = _body(ctx.ui)
    assert "1 x DROP NOT NULL (one-way), 1 x DROP DEFAULT (restored at Cut over)" in body
    # "(DSQL)" labels each statement as run on the target.
    first = '(DSQL) ALTER TABLE "shop"."pages" ALTER COLUMN "summary" DROP NOT NULL'
    second = '(DSQL) ALTER TABLE "shop"."pages" ALTER COLUMN "summary" DROP DEFAULT'
    assert first in ctx.ui.texts and second in ctx.ui.texts
    assert ctx.ui.texts.index(first) < ctx.ui.texts.index(second)
    import re
    assert not any(t.startswith("ALTER TABLE") for t in ctx.ui.texts)
    for line in (t for t in ctx.ui.texts if t.startswith("(DSQL) ALTER TABLE")):
        assert re.match(r"^\(DSQL\) ALTER TABLE .+ ALTER COLUMN .+ DROP (NOT NULL|DEFAULT)$", line)
    # A live catalog whose DEFAULT is already dropped lists only DROP NOT NULL.
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("present", {PAGES: _record()}), rows=(),
                facts={PAGES: _facts(rows=False, summary=F(True, None))})
    alters = [t for t in ctx.ui.texts if t.startswith("(DSQL) ALTER TABLE")]
    assert alters == ['(DSQL) ALTER TABLE "shop"."pages" ALTER COLUMN "summary" DROP NOT NULL']
    assert "1 column in 1 table will load as NULL" in _body(ctx.ui)


@pytest.mark.parametrize("probe_raises", [False, True])
@pytest.mark.parametrize("ledger", [LedgerRead("unreadable", {}, "AccessDenied"), RuntimeError("boom")])
def test_an_unknown_ledger_read_disables_start_with_retry(monkeypatch, ledger, probe_raises):
    ctx = _open(monkeypatch, view=_view(), ledger=ledger, probe_raises=probe_raises,
                facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.ui.start().enabled is False
    assert "Could not read the record of excluded-column changes" in _body(ctx.ui)
    assert ctx.state.exclusion_forced_replace == {}
    retry = [h for (label, h) in ctx.ui.handlers if label == "Retry"]
    assert retry and len(ctx.reads) == 1
    asyncio.run(retry[0]())
    assert len(ctx.reads) == 2  # Retry re-reads the ledger


def test_a_present_record_sets_in_use_before_the_probe_and_absent_does_not(monkeypatch):
    order = []
    ctx = _open(monkeypatch, view=_view(overlay={}, excluded={}), order=order,
                ledger=LedgerRead("present", {"shop.elsewhere": _record(table="shop.elsewhere")}))
    assert ctx.state.overlay_ledger_in_use and order[:2] == ["ledger", "probe"]
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.state.overlay_ledger_in_use is False


def test_one_facts_read_covers_overlay_and_recorded_columns_and_none_without_them(monkeypatch):
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("present", {PAGES: _record(("body",))}),
                facts={PAGES: _facts(summary=SUMMARY, body=F(False, None))})
    assert ctx.facts_calls == [([PAGES], {PAGES: ["body", "summary"]})]
    ctx = _open(monkeypatch, view=_view(overlay={}, excluded={}), ledger=LedgerRead("absent"))
    assert ctx.facts_calls == [] and ctx.ui.start().text == "Append and load"  # the previous dialog


def test_a_whole_probe_failure_warns_and_forces_nothing(monkeypatch):
    # Finding 3: the record is still read, so the session is marked as using the ledger.
    ctx = _open(monkeypatch, view=_view(overlay={}, excluded={}), probe_raises=True,
                ledger=LedgerRead("present", {PAGES: _record()}))
    assert ctx.state.overlay_ledger_in_use
    assert "Could not read the Aurora DSQL target" in _body(ctx.ui)
    assert ctx.state.exclusion_forced_replace == {} and ctx.ui.start().enabled


def test_no_ledger_handle_keeps_the_previous_dialog(monkeypatch):
    ctx = _open(monkeypatch, view=None, ledger=None)
    assert ctx.reads == [] and ctx.facts_calls == []
    assert ctx.ui.start().text == "Append and load" and ctx.state.exclusion_forced_replace == {}


def test_cancel_or_a_backdrop_close_drops_the_forced_set_and_confirm_consumes_it(monkeypatch):
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.state.exclusion_forced_replace
    ctx.ui.dialogs[-1].close()  # Cancel / Esc / backdrop: the dialog's value goes False
    assert ctx.state.exclusion_forced_replace == {}
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    confirm = [h for (label, h) in ctx.ui.handlers if label.startswith("Drop 1 table")]
    confirm[-1]()
    assert ctx.started == [frozenset({PAGES})]  # the run took the forced table
    assert ctx.state.exclusion_forced_replace == {}


def test_under_cdc_the_dialog_forces_nothing_and_describes_no_append(monkeypatch):
    # DELIBERATE G132 UPDATE: the dialog used to warn "Existing rows keep their values while CDC
    # streams" -- an append beside the sink that it cannot start: no Full Load runs while CDC is
    # live (G129), so its confirm is disabled and its notice says why, and nothing else may
    # describe that load as happening.
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), cdc=True,
                facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.state.exclusion_forced_replace == {}
    body = _body(ctx.ui)
    assert "Existing rows keep their values while CDC streams" not in body
    assert "cannot be dropped while CDC streams" not in body
    assert "CDC is currently streaming" in body and ctx.ui.start().enabled is False


# ---------------------------------------------------------------------------
# Decision 2: the page-header Run goes through the Start dialog when the overlay is in play
# ---------------------------------------------------------------------------


def test_the_header_run_asks_for_the_dialog_only_when_the_overlay_is_in_play():
    import inspect

    from dsql_migrator.core.models import SourceInventory

    src = " ".join(inspect.getsource(dm.build_data_migration_screen).split())
    assert "return content, header_runner" in src
    assert "if overlay_in_play(migration_state, _inventory()): migration_state.set_active_substep(\"full_load\") " \
           "migration_state.pending_full_load_confirm = True return" in src
    assert "migration_state.clear_exclusion_forced_replace() runner() return content, header_runner" in src
    inventory = SourceInventory(tables=[TableDef(name=PAGES, primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="summary", mysql_type="text")])])
    state = DataMigrationState()
    assert dm.overlay_in_play(state, inventory) is False  # no exclusions, never used: direct run
    state.set_lob_exclusion(PAGES, "summary", True)
    assert dm.overlay_in_play(state, inventory) is True
    state.set_lob_exclusion(PAGES, "summary", False)
    assert dm.overlay_in_play(state, inventory) is False  # a tick alone leaves nothing behind
    state.mark_overlay_ledger_in_use()  # a run overlaid (or a ledger read found a record)
    assert dm.overlay_in_play(state, inventory) is True


def test_a_pending_confirm_opens_the_start_dialog_once(monkeypatch):
    import nicegui.context as ctxobj
    import nicegui.run as nrun

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()), raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)

    class _AsyncTimerUi(_DialogUi):
        def timer(self, _interval, callback=None, *_a, **_k):
            if callable(callback):
                result = callback()
                if asyncio.iscoroutine(result):
                    asyncio.run(result)
            return _ConfirmDialogUi._El(self)

    state = DataMigrationState()
    state.pending_full_load_confirm = True
    ui = _AsyncTimerUi()
    dm._render_full_load_step(
        ui, state, SimpleNamespace(get_status=lambda j: None), None, job=None, status=StepStatus.NOT_STARTED,
        selected_names=[PAGES], guard_reason=None, start_full_load=lambda: None, retry_failed_load=lambda *a: None,
        reload_table=lambda *a: None, accept_quarantine_and_continue=lambda *a: None,
        stop_full_load=lambda *a: None, refresh=lambda *a: None, schema_recreate_candidates=lambda: [])
    assert ui.opened == 1 and state.pending_full_load_confirm is False



# ---------------------------------------------------------------------------
# Review fixes (P2-10..12 verification)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure", ["probe", "facts"])
def test_a_recreated_unfinished_table_is_forced_even_when_the_target_cannot_be_read(monkeypatch, failure):
    ctx = _open(monkeypatch, view=_view(unfinished=(PAGES,)), ledger=LedgerRead("present", {PAGES: _record()}),
                probe_raises=failure == "probe", facts={PAGES: None} if failure == "facts" else None)
    assert PAGES in ctx.state.exclusion_forced_replace
    assert "recreated by the last run and not finished" in ctx.state.exclusion_forced_replace[PAGES]
    assert ctx.ui.start().text == "Drop 1 table, recreate and load" and ctx.ui.start().color == "negative"


def test_the_checklist_scopes_the_forced_label_to_the_ticked_tables(monkeypatch):
    from dsql_migrator.core.models import ChunkState, MigrationJob

    other = "shop.other"
    job = MigrationJob(job_id="j1", status="DONE", progress_pct=100.0, chunks=[
        ChunkState(chunk_id=other, status="DONE", rows_loaded=5, attempts=1),
        ChunkState(chunk_id=PAGES, status="DONE", rows_loaded=3, attempts=1)])
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), rows=(PAGES, other), tables=(other, PAGES),
                facts={PAGES: _facts(summary=SUMMARY)}, job=job, status=StepStatus.DONE,
                retry_tables=lambda names: None, click="Reload specific tables")
    assert ctx.state.exclusion_forced_replace.keys() == {PAGES}
    # Nothing ticked: no table is dropped, so the label is not a red "Drop 0 tables".
    assert ctx.ui.start().text == "Append and load" and ctx.ui.start().color == "primary"
    assert "will be dropped and reloaded" not in _body(ctx.ui)  # the forced notice follows the ticks
    toggles_by_order = ctx.ui.checkbox_handlers
    toggles_by_order[0](SimpleNamespace(value=True))  # tick shop.other only
    assert ctx.ui.start().text == "Append and load"
    toggles_by_order[1](SimpleNamespace(value=True))  # tick the forced table too
    assert ctx.ui.start().text == "Drop 1 table, recreate and load" and ctx.ui.start().color == "negative"


def test_a_missing_table_notice_does_not_point_at_a_radio_that_is_not_drawn(monkeypatch):
    missing = "shop.missing"
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), rows=(PAGES,), tables=(PAGES, missing),
                present={PAGES}, facts={PAGES: _facts(summary=SUMMARY)})
    assert ctx.ui.radios == [] and "choose 'Drop & reload' below" not in _body(ctx.ui)
    assert "1 selected table do not exist on the target" in _body(ctx.ui)


def test_the_dialog_pins_what_it_disclosed_and_only_a_confirm_hands_it_over(monkeypatch):
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    state = ctx.state
    assert state.take_run_choice() == (frozenset(), None)  # not confirmed: nothing counts
    ctx = _open(monkeypatch, view=_view(), ledger=LedgerRead("absent"), facts={PAGES: _facts(summary=SUMMARY)})
    ctx.state.confirm_exclusion_forced_replace()
    assert ctx.state.take_run_choice() == (frozenset({PAGES}), {PAGES: frozenset({"summary"})})
    assert ctx.state.take_run_choice() == (frozenset(), None)  # consumed


def test_a_different_overlay_than_the_dialog_disclosed_stops_the_run():
    assert dm._disclosed_differs({PAGES: {"summary"}}, {PAGES: frozenset({"summary"})}, [PAGES]) is False
    assert dm._disclosed_differs({}, {PAGES: frozenset({"summary"})}, [PAGES]) is True  # settled after the dialog
    assert dm._disclosed_differs(None, {PAGES: frozenset({"summary"})}, [PAGES]) is False  # no dialog: nothing pinned
    assert dm._disclosed_differs({"shop.elsewhere": {"x"}}, {}, [PAGES]) is False  # outside the run


def test_every_type_counts_the_overlay_in_the_prerequisites():
    # P2-13: Start CDC overlays a CDC-only migration before any connector, like the Full
    # Load pre-pass, so an excluded NOT NULL column is loadable for every type.
    import inspect

    src = " ".join(inspect.getsource(dm.build_data_migration_screen).split())
    assert "_prereq_overlay = overlay_lob_exclusions(migration_state, inventory)" in src


class _ClassRecorder:
    """Wraps a double's element and records the classes set on it (per label text)."""

    def __init__(self, el, sink, text):
        self._el, self._sink, self._text = el, sink, text

    def classes(self, c=""):
        self._sink.setdefault(self._text, []).append(c)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __getattr__(self, name):
        return getattr(self._el, name)


def test_the_full_load_dialog_statements_wrap_at_word_boundaries(monkeypatch):
    """LIVE 8: break-all split the ALTER list mid-identifier ("not|e"). The statements wrap
    at spaces (wrap-anywhere only splits a token wider than the line); text unchanged."""
    recorded = {}
    orig = _DialogUi.label

    def label(self, text="", *a, **k):
        return _ClassRecorder(orig(self, text, *a, **k), recorded, str(text))

    monkeypatch.setattr(_DialogUi, "label", label)
    _open(monkeypatch, view=_view(), ledger=LedgerRead("present", {PAGES: _record()}), rows=(),
          facts={PAGES: _facts(rows=False, summary=SUMMARY)})
    alters = {t: c for t, c in recorded.items() if t.startswith("(DSQL) ALTER TABLE")}
    assert len(alters) == 2
    for text, cls in alters.items():
        joined = " ".join(cls)
        assert "wrap-anywhere" in joined and "break-all" not in joined, (text, cls)
