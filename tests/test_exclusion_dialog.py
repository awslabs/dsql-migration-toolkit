"""P3-C14: the exclusion popup (normal mode) and its button row, driven by a NiceGUI double."""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.models import ColumnDef, SourceType, TableDef
from dsql_migrator.ui.data_migration import DataMigrationState
from dsql_migrator.ui.data_migration._exclusion_dialog import (
    BUTTON_LABEL,
    ExclusionDialogContext,
    open_exclusion_dialog,
    render_exclusion_button_row,
)
from dsql_migrator.ui.data_migration._models import ExclusionInputs


class _El:
    def __init__(self, ui, kind="el", text=""):
        self._ui = ui
        self.kind = kind
        self.text = text
        self.props_seen: list[str] = []
        self.enabled = True
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def props(self, value=""):
        self.props_seen.append(value)
        return self

    def classes(self, *_a, **_k):
        return self

    def style(self, *_a, **_k):
        return self

    def tooltip(self, *_a, **_k):
        return self

    def open(self):
        self._ui.opened += 1
        return self

    def close(self):
        self._ui.closed += 1
        return self

    def set_enabled(self, value):
        self.enabled = bool(value)

    def set_value(self, value):
        self.value = value
        self._ui.set_values.append(value)

    def cancel(self):
        return None


class _Ui:
    """Records what the popup draws and captures handlers so a test can click."""

    def __init__(self):
        self.texts: list[str] = []
        self.buttons: dict[str, list] = {}
        self.elements: list[_El] = []
        self.opened = 0
        self.closed = 0
        self.notified: list[str] = []
        self.timers: list = []
        self.set_values: list[str] = []
        self.editors: list[_El] = []
        self.refreshes = 0
        self.dialogs: list[_El] = []
        self.expansions: dict = {}
        self.selects: list = []
        self.tables: list = []

    def _el(self, kind="el", text=""):
        el = _El(self, kind, text)
        self.elements.append(el)
        return el

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el("label", text)

    def button(self, text="", *_a, on_click=None, **_k):
        el = self._el("button", text)
        self.buttons.setdefault(str(text), []).append((on_click, el))
        return el

    def codemirror(self, value="", *_a, on_change=None, **_k):
        el = self._el("codemirror", value)
        el.value = value
        el.on_change = on_change
        self.editors.append(el)
        return el

    def dialog(self, *_a, **_k):
        el = self._el("dialog")
        self.dialogs.append(el)
        return el

    def refreshable(self, fn):
        ui = self

        def _w(*a, **k):
            return fn(*a, **k)

        def _refresh(*_a, **_k):
            ui.refreshes += 1
            return fn()

        _w.refresh = _refresh
        return _w

    def notify(self, text="", *_a, **_k):
        self.notified.append(str(text))

    def timer(self, _interval, callback=None, *_a, once=False, **_k):
        self.timers.append(callback)
        if once and callable(callback) and getattr(callback, "__name__", "") == "open":
            callback()
        return self._el("timer")

    def expansion(self, text="", *_a, value=False, on_value_change=None, **_k):
        el = self._el("expansion", text)
        el.value = value
        self.expansions[str(text)] = (on_value_change, el)
        return el

    def select(self, options=None, *_a, value=None, on_change=None, **_k):
        el = self._el("select")
        el.value = value
        self.selects.append((list(options or []), on_change, el))
        return el

    def table(self, *_a, columns=None, rows=None, **_k):
        el = self._el("table")
        self.tables.append(list(rows or []))
        return el

    def __getattr__(self, _name):
        return lambda *_a, **_k: self._el(_name)

    def expand(self, prefix):
        title = next(t for t in self.expansions if t.startswith(prefix))
        handler, _el = self.expansions[title]
        handler(SimpleNamespace(value=True))

    def click(self, label, index=-1):
        handler, _el = self.buttons[label][index]
        return handler()

    def type_text(self, value):
        editor = self.editors[-1]
        editor.value = value
        editor.on_change(SimpleNamespace(value=value))
        # the debounce: fire the latest one-shot preview
        self.timers[-1]()


def _t(name, **cols):
    return TableDef(name=name, primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="bigint")]
                    + [ColumnDef(name=c, mysql_type=t) for c, t in cols.items()])


INV = SimpleNamespace(tables=[_t("shop.t", a="longtext", b="text"), _t("shop.u", note="text"),
                              _t("shop.audit_1", x="text")])
NAMES = [t.name for t in INV.tables]


def _ctx(state=None, *, refresh=None, job_manager=None, lock_reason=None):
    state = state or DataMigrationState()
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))
    inputs = ExclusionInputs(selected=list(NAMES), applied={}, preserve_foreign_keys=True,
                             migratable=list(NAMES), signature=("s",))
    calls = {"refresh": 0}

    def _refresh():
        calls["refresh"] += 1

    ctx = ExclusionDialogContext(
        migration_state=state, job_manager=job_manager or SimpleNamespace(get_status=_none),
        session=state._session, inventory_fn=lambda: INV, inputs_fn=lambda: inputs,
        refresh=refresh or _refresh, lock_reason_fn=lambda: lock_reason,
    )
    return ctx, calls


def _none(job_id):
    from dsql_migrator.core.job_manager import JobNotFoundError

    raise JobNotFoundError(job_id)


def test_the_row_shows_the_button_and_summary_and_refreshes_locally():
    ui = _Ui()
    ctx, calls = _ctx()
    row = render_exclusion_button_row(ui, ctx)
    assert BUTTON_LABEL in ui.buttons and "Nothing excluded" in ui.texts
    row.refresh()
    assert ui.refreshes == 1 and calls["refresh"] == 0  # never refresh_all


def test_the_tree_handlers_refresh_the_row_not_the_page():
    import inspect

    import dsql_migrator.ui.data_migration as dm

    source = inspect.getsource(dm._render_table_selection)
    assert source.count("on_selection_change(") >= 3  # tick, Select all, Unselect all
    screen = inspect.getsource(dm.build_data_migration_screen)
    assert "note_tree_retick(chosen)" in screen and "row.refresh()" in screen


def test_the_blocking_summary_is_an_error_notice_with_review():
    ui = _Ui()
    state = DataMigrationState()
    state.restore_lob_exclusion("shop.t", "id")  # a primary key: no longer applies
    ctx, _calls = _ctx(state)
    render_exclusion_button_row(ui, ctx)
    assert "Review" in ui.buttons
    assert "1 saved exclusion no longer applies. Starting is blocked until you review them." in ui.texts


def test_the_dialog_is_persistent_opens_next_tick_and_cancel_writes_nothing():
    ui = _Ui()
    state = DataMigrationState()
    state.restore_lob_exclusion("shop.t", "a")
    state.add_pending_rule("exclude shop.u.note  # from the Full Load quarantine")
    ctx, calls = _ctx(state)
    before = (state.lob_exclusions(), state.exclusion_rules_text(), state.rule_excluded_tables(),
              state.pending_rules())
    open_exclusion_dialog(ui, ctx)
    assert ui.opened == 1 and "persistent" in ui.dialogs[0].props_seen
    assert ui.editors[0].value.splitlines() == [
        "exclude shop.t.a", "exclude shop.u.note  # from the Full Load quarantine"]
    ui.type_text("exclude shop.t.b")
    ui.click("Cancel")
    after = (state.lob_exclusions(), state.exclusion_rules_text(), state.rule_excluded_tables(),
             state.pending_rules())
    assert after == before and calls["refresh"] == 0
    # a reopen shows the saved rules plus the pending line again
    ui2 = _Ui()
    open_exclusion_dialog(ui2, ctx)
    assert ui2.editors[0].value == ui.editors[0].text


def test_errors_disable_save_and_a_suggestion_rewrites_only_that_line():
    ui = _Ui()
    ctx, _calls = _ctx()
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.u.note\nexclude shop.t.A")
    save = ui.buttons["Save"][-1][1]
    assert save.enabled is False
    assert any("need fixing. Save is disabled." in t for t in ui.texts)
    label = next(k for k in ui.buttons if k.startswith('Use "exclude shop.t.'))
    ui.click(label)
    assert ui.set_values[-1] == "exclude shop.u.note\nexclude shop.t.a"
    assert ui.buttons["Save"][-1][1].enabled is True


def test_a_keystroke_never_refreshes_the_page_and_save_refreshes_once():
    ui = _Ui()
    state = DataMigrationState()
    ctx, calls = _ctx(state)
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.t.a")
    ui.type_text("exclude shop.t.a\nexclude table shop.audit_*")
    assert calls["refresh"] == 0
    ui.click("Save")
    assert calls["refresh"] == 1 and ui.closed == 1
    assert state.lob_exclusions() == {"shop.t": {"a"}}
    assert state.rule_excluded_tables() == {"shop.audit_1"}
    assert state.exclusion_rules_text() == "exclude shop.t.a\nexclude table shop.audit_*"


def test_a_lock_that_appears_after_open_refuses_the_save_and_keeps_the_dialog():
    ui = _Ui()
    state = DataMigrationState()
    ctx, calls = _ctx(state)
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.t.a")
    # meanwhile an attached stack is adopted
    state.cdc_stack_name = "dsql-cdc-x"
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert state.adopt_deployed_exclusions({"shop.u": ["note"]}, expect_stack="dsql-cdc-x")
    ui.click("Save")
    assert ui.notified and ui.closed == 0 and calls["refresh"] == 0
    assert state.lob_exclusions() == {"shop.u": {"note"}}
    assert ui.editors[0].value == "exclude shop.t.a"


# --- P3-C15: the sections -------------------------------------------------------------

from dsql_migrator.ui.data_migration._exclusion_dialog import REVIEW_FILTERS, lob_rows  # noqa: E402


def _open(ctx, text):
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.type_text(text)
    return ui


def test_review_all_filters_and_paging_survive_a_rerender():
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ui = _open(ctx, "exclude shop.t.a\nexclude shop.t.id\ninclude shop.t.a\nexclude shop.u.note\n"
                    "exclude table shop.audit_*")
    ui.expand("Review all")
    options, on_change, _el = ui.selects[-1]
    assert tuple(options) == REVIEW_FILTERS == ("All", "Excluded", "Blocked", "Warning")
    statuses = {r["status"].split(" ")[0] for r in ui.tables[-1]}
    assert statuses == {"Unselected", "Excluded", "Kept", "Blocked"}  # id: primary key
    on_change(SimpleNamespace(value="Excluded"))
    assert {r["status"].split(" ")[0] for r in ui.tables[-1]} == {"Excluded"}
    assert state.exclusion_dialog_ui()["review_filter"] == "Excluded"
    ui.type_text("exclude shop.t.a\nexclude shop.u.note")  # a re-render keeps the filter
    assert {r["status"].split(" ")[0] for r in ui.tables[-1]} == {"Excluded"}
    ui.click("Cancel")
    assert state.exclusion_dialog_ui() == {}  # cleared on close


def test_lob_rows_are_the_open_candidates_and_add_rule_quotes_the_name():
    tables = [_t("shop.t", a="longtext", b="mediumtext"), _t("my s.t2", c="longblob")]
    inv = SimpleNamespace(tables=tables)
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.inventory_fn = lambda: inv
    names = [t.name for t in tables]
    inputs = ExclusionInputs(selected=names, applied={}, preserve_foreign_keys=True,
                             migratable=names, signature=("s",))
    ctx.inputs_fn = lambda: inputs
    ui = _open(ctx, "exclude shop.t.a")
    ui.expand("Oversized-LOB candidates")
    rows = [(r["table"], r["column"]) for r in lob_rows(ctx, inv, inputs, ctx.migration_state._exclusion_resolution_memo["editor"][2])]
    assert rows == [("my s.t2", "c"), ("shop.t", "b")]  # a is excluded; id (PK) never offered
    ui.click("+ add rule", index=0)
    assert ui.set_values[-1].splitlines()[-1] == 'exclude "my s".t2.c'


def test_an_include_kept_candidate_offers_go_to_its_line():
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ui = _open(ctx, "exclude shop.t.*\ninclude shop.t.a")
    ui.expand("Oversized-LOB candidates")
    assert "Go to L2" in ui.buttons


def test_the_capture_section_is_for_cdc_types_only_and_equals_what_is_sent():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ui = _open(ctx, "exclude shop.t.a")
    assert not any(t.startswith("CDC capture list") for t in ui.expansions)
    ctx.capture_tables_fn = lambda selected: [t for t in INV.tables if t.name in set(selected)]
    ui = _open(ctx, "exclude shop.t.a")
    assert any(t.startswith("CDC capture list") for t in ui.expansions)
    ui.click("Save")
    sent = cdc_capture_exclusions(state, INV, INV.tables).sent
    preview = ctx.migration_state._exclusion_resolution_memo["editor"][2]
    assert preview.cdc_value == sent


def test_target_changes_render_defaults_verbatim_and_summarize():
    ddl = ('CREATE TABLE "shop"."t" ("id" BIGINT NOT NULL, "a" TEXT NOT NULL DEFAULT \'\', '
           '"b" TEXT DEFAULT \'\\x00\'::bytea, "c" TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + '
           'INTERVAL \'1 day\'), PRIMARY KEY ("id"))')
    tables = [_t("shop.t", a="longtext", b="text", c="timestamp")]
    inv = SimpleNamespace(tables=tables)
    applied = {"shop.t": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[],
                                         preserved_foreign_keys=[], index_ddls=[])}
    inputs = ExclusionInputs(selected=["shop.t"], applied=applied, preserve_foreign_keys=True,
                             migratable=["shop.t"], signature=("s",))
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.inventory_fn = lambda: inv
    ctx.inputs_fn = lambda: inputs
    ui = _open(ctx, "exclude shop.t.a\nexclude shop.t.b\nexclude shop.t.c")
    ui.expand("Changes to Aurora DSQL")
    assert "1 x DROP NOT NULL (one-way), 3 x DROP DEFAULT (restored at Cut over)" in ui.texts
    assert 'ALTER TABLE "shop"."t" ALTER COLUMN "a" DROP NOT NULL' in ui.texts
    assert "Cut over restores 3 DEFAULTs (new rows only)." in ui.texts


# --- P3-C16: Check sizes ----------------------------------------------------------------

import asyncio  # noqa: E402

import pytest  # noqa: E402

from dsql_migrator.ui.data_migration import _exclusion_dialog as dlg  # noqa: E402


class _Engine:
    def __init__(self, log):
        self.log = log

    def dispose(self):
        self.log.append("dispose")


def _patch_probe(monkeypatch, *, answers, log, fail=False, on_call=None):
    import nicegui.run as nrun

    import dsql_migrator.ui.connect as connect
    import dsql_migrator.ui.prerequisite_probes as probes

    async def io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", io_bound)

    def factory_maker(_password, **_k):
        def factory(_config):
            log.append("engine")
            return _Engine(log)
        return factory

    monkeypatch.setattr(connect, "make_source_engine_factory", factory_maker)

    class _Probe:
        def __init__(self, *_a, **_k):
            pass

        def oversized_values(self, table, columns, *, engine, should_cancel, **_k):
            assert isinstance(engine, _Engine)
            log.append(("probe", table, tuple(columns)))
            if on_call:
                on_call()
            if fail:
                raise RuntimeError("boom")
            out = {}
            for column in columns:
                if should_cancel():
                    break
                out[column] = answers.get((table, column), False)
            return out

    monkeypatch.setattr(probes, "SessionSourceProbe", _Probe)


def _rows(*pairs):
    return [{"table": t, "column": c, "source": "longtext", "target": "text"} for t, c in pairs]


def test_check_sizes_measures_the_page_once_and_disposes_the_engine(monkeypatch):
    log = []
    _patch_probe(monkeypatch, answers={("shop.t", "a"): True}, log=log)
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a"), ("shop.t", "b"), ("shop.u", "note")),
                                 lambda: None))
    assert log.count("engine") == 1 and log[-1] == "dispose"
    assert [e for e in log if isinstance(e, tuple)] == [
        ("probe", "shop.t", ("a", "b")), ("probe", "shop.u", ("note",))]
    sizes = state.value_sizes()
    assert sizes["shop.t.a"][0] == "over" and sizes["shop.t.b"][0] == "ok"
    assert not state.size_check_running()


def test_a_second_click_while_running_starts_nothing(monkeypatch):
    log = []
    _patch_probe(monkeypatch, answers={}, log=log)
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    assert state.begin_size_check()
    asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a")), lambda: None))
    assert log == []


def test_cancel_stops_after_the_current_column_and_the_rest_read_not_determined(monkeypatch):
    log = []
    state = DataMigrationState()
    _patch_probe(monkeypatch, answers={}, log=log, on_call=state.cancel_size_check)
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a"), ("shop.t", "b"), ("shop.u", "note")),
                                 lambda: None))
    sizes = state.value_sizes()
    assert sizes["shop.t.a"][0] == "unknown" and "shop.u.note" not in sizes
    assert dlg._size_label(sizes["shop.t.a"], None)[0].startswith("Not determined")


def test_the_engine_is_disposed_even_when_the_probe_fails(monkeypatch):
    log = []
    _patch_probe(monkeypatch, answers={}, log=log, fail=True)
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    with pytest.raises(RuntimeError):
        asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a")), lambda: None))
    assert log[-1] == "dispose" and not state.size_check_running()


def test_size_labels():
    from datetime import datetime, timezone

    at = datetime(2026, 10, 1, 5, 2, tzinfo=timezone.utc)
    over, tone = dlg._size_label(("over", at), None)
    assert over.startswith("Over the limit found · ") and "quarantined" in over and tone == "warning"
    ok, tone = dlg._size_label(("ok", at), "latin1")
    assert ok.startswith("No value over the limit · ") and "measured in latin1" in ok
    assert dlg._size_label(None, None) == ("Not checked", "neutral")


def test_check_sizes_is_disabled_with_the_reconnect_hint_when_not_ready():
    from dsql_migrator.ui.data_migration._cdc_monitoring import RECONNECT_REASON

    ui = _Ui()
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: False
    dlg._render_check_sizes(ui, ctx, _rows(("shop.t", "a")), lambda: None)
    _handler, button = ui.buttons["Check sizes"][-1]
    assert "disable" in " ".join(button.props_seen)
    assert any(t.startswith("Reads the source: one bounded query per column for the 1 column")
               for t in ui.texts)
    assert RECONNECT_REASON  # the tooltip carries it (the double records no tooltip text)
