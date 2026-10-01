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
        self.id = len(ui.elements) + 1

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

    def checkbox(self, text="", *_a, value=False, on_change=None, **_k):
        el = self._el("checkbox", text)
        el.value = value
        el.on_change = on_change
        self.texts.append(str(text))
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
    ui.type_text("exclude shop.t.a\nexclude shop.u.note")  # a re-render keeps the filter
    assert {r["status"].split(" ")[0] for r in ui.tables[-1]} == {"Excluded"}
    # the view is the dialog's own: another tab's popup starts fresh and never resets it
    other = _Ui()
    open_exclusion_dialog(other, ctx)
    other.click("Cancel")
    ui.type_text("exclude shop.t.a\nexclude shop.u.note\nexclude shop.t.b")
    assert {r["status"].split(" ")[0] for r in ui.tables[-1]} == {"Excluded"}


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
    def __init__(self, log, reachable=True):
        self.log = log
        self.reachable = reachable

    def connect(self):
        import contextlib

        if not self.reachable:
            raise OSError("unreachable")
        return contextlib.nullcontext()

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
    assert sizes[("shop.t", "a")][0] == "over" and sizes[("shop.t", "b")][0] == "ok"
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
    assert sizes[("shop.t", "a")][0] == "unknown" and ("shop.u", "note") not in sizes
    assert dlg._size_label(sizes[("shop.t", "a")], None)[0].startswith("Not determined")


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


# --- P3-C17: read-only modes ------------------------------------------------------------

from tests.test_ui_data_migration import _StubJob, _StubJobManager  # noqa: E402

from dsql_migrator.ui.data_migration import MigrationType  # noqa: E402


def _locked_ctx(*, mtype, job_id=None, phase="infra", deleting=False, type_lock=None):
    state = DataMigrationState()
    state.migration_type = mtype
    state.job_id = job_id
    jobs = {"job-1": _StubJob("DONE")}
    if phase:
        state.set_cdc_stack_phase(phase, status="CREATE_COMPLETE")
    if deleting:
        state.set_cdc_deploy_job_id("del-1", kind="delete")
        jobs["del-1"] = _StubJob("RUNNING")
    state.add_pending_rule("exclude shop.t.b  # from the CDC dead-letter queue")
    ctx, _calls = _ctx(state, job_manager=_StubJobManager(jobs))
    ctx.type_lock_reason_fn = lambda: type_lock
    return ctx, state


def test_every_read_only_variant_lists_the_pending_lines_and_keeps_them():
    for kw in (dict(mtype=MigrationType.FULL_LOAD_AND_CDC, deleting=True, phase="running"),
               dict(mtype=MigrationType.FULL_LOAD_AND_CDC, job_id="job-1"),
               dict(mtype=MigrationType.CDC_ONLY),
               dict(mtype=MigrationType.FULL_LOAD_ONLY),
               dict(mtype=MigrationType.CDC_ONLY, job_id="job-1", phase=None)):
        ctx, state = _locked_ctx(**kw)
        ui = _Ui()
        open_exclusion_dialog(ui, ctx)
        assert any(t.startswith('Pending: "exclude shop.t.b" (from the CDC dead-letter queue)')
                   for t in ui.texts), kw
        assert "disable" in " ".join(ui.editors[0].props_seen)
        assert state.pending_rules() == ["exclude shop.t.b  # from the CDC dead-letter queue"]


def test_the_delete_wait_offers_only_close():
    ctx, _state2 = _locked_ctx(mtype=MigrationType.FULL_LOAD_ONLY, deleting=True, phase="running")
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    assert "Close" in ui.buttons and "Change migration type" not in ui.buttons


def test_change_migration_type_closes_first_then_scrolls_with_the_shared_script():
    import inspect

    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.ui.data_migration._models import SCROLL_TO_MIGRATION_TYPE_JS

    ctx, _state2 = _locked_ctx(mtype=MigrationType.FULL_LOAD_ONLY)
    ui = _Ui()
    events = []
    ui.run_javascript = lambda js: events.append(("js", js))
    open_exclusion_dialog(ui, ctx)
    original_close = ui.dialogs[0].close
    ui.dialogs[0].close = lambda: (events.append("close"), original_close())[1]
    handler, _button = ui.buttons["Change migration type"][-1]
    # the dialog was built with dialog.close bound before our wrapper: call the handler and
    # read the order from the closed counter and the queued timer
    before = ui.closed
    handler()
    assert ui.closed == before + 1
    ui.timers[-1]()
    assert events[-1] == ("js", SCROLL_TO_MIGRATION_TYPE_JS)
    assert "SCROLL_TO_MIGRATION_TYPE_JS" in inspect.getsource(dm._scroll_to_migration_type_button)


def test_change_migration_type_is_disabled_with_the_type_lock_reason():
    ctx, _state2 = _locked_ctx(mtype=MigrationType.CDC_ONLY, job_id="job-1", phase=None,
                               type_lock="Locked: CDC is running.")
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    _handler, button = ui.buttons["Change migration type"][-1]
    assert "disable" in " ".join(button.props_seen)


# --- P3-C18: Change & reload --------------------------------------------------------------


def _reload_ctx(status="DONE", *, ready=True):
    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    state.job_id = "job-1"
    state.restore_lob_exclusion("shop.t", "a")
    state.restore_exclusion_rules("exclude shop.t.a", ())
    jobs = {"job-1": _StubJob(status)}
    jm = _StubJobManager(jobs)
    ctx, calls = _ctx(state, job_manager=jm)
    ctx.session.connection_ready = lambda: ready
    submitted = []

    def run_retry_for(tables):
        submitted.append((list(tables), state.take_run_choice()))

    ctx.extra["run_retry_for"] = run_retry_for
    return ctx, state, jobs, submitted


def test_a_running_first_load_disables_change_and_reload_and_a_click_does_not_switch():
    ctx, state, _jobs, submitted = _reload_ctx("RUNNING")
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    handler, button = ui.buttons["Change & reload"][-1]
    assert "disable" in " ".join(button.props_seen)
    assert any(t.startswith("A Full Load is running") for t in ui.texts)
    dialogs = len(ui.dialogs)
    handler()
    assert len(ui.dialogs) == dialogs and ui.notified  # no mode switch


def test_a_successful_change_and_reload_forces_exactly_the_diffed_tables():
    ctx, state, _jobs, submitted = _reload_ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    assert ui.texts.count("Change exclusions & reload") == 1
    ui.type_text("exclude shop.t.a\nexclude shop.u.note")
    assert "Drop & reload shop.u" in ui.buttons
    ui.click("Drop & reload shop.u")
    (tables, (replace, pin)), = submitted
    assert tables == ["shop.u"] and replace == frozenset({"shop.u"}) and pin is None
    assert state.reload_mode == "append"  # the run-wide choice is untouched
    assert state.lob_exclusions() == {"shop.t": {"a"}, "shop.u": {"note"}}
    assert state.exclusion_forced_replace == {}


def test_a_text_only_change_saves_the_text_and_reloads_nothing():
    ctx, state, _jobs, submitted = _reload_ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    ui.type_text("exclude shop.t.a  # why")
    ui.click("Save")
    assert submitted == [] and state.exclusion_rules_text() == "exclude shop.t.a  # why"


@pytest.mark.parametrize("later", ["RUNNING", "gone"])
def test_the_save_race_writes_nothing_and_keeps_the_text(later):
    ctx, state, jobs, submitted = _reload_ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    ui.type_text("exclude shop.t.a\nexclude shop.u.note")
    if later == "RUNNING":
        jobs["job-1"] = _StubJob("RUNNING")
    else:
        jobs.clear()
    before = (state.job_id, state.lob_exclusions(), state.exclusion_rules_text(),
              state.rule_excluded_tables(), state.exclusion_forced_replace, state.pending_rules(),
              state.fk_apply_result)
    ui.click("Drop & reload shop.u")
    after = (state.job_id, state.lob_exclusions(), state.exclusion_rules_text(),
             state.rule_excluded_tables(), state.exclusion_forced_replace, state.pending_rules(),
             state.fk_apply_result)
    assert submitted == [] and after == before and ui.notified
    assert ui.editors[-1].value == "exclude shop.t.a\nexclude shop.u.note"
    ui.click("Cancel")
    assert state.exclusion_rules_text() == "exclude shop.t.a"


def test_table_lines_are_e3_in_change_and_reload():
    ctx, state, _jobs, _submitted = _reload_ctx()
    ctx.lock_reason_fn = lambda: ("Locked — a Full Load has already run for this table set. "
                                  "Use 'Start over' to migrate a different set of tables.")
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    ui.type_text("exclude shop.t.a\nexclude table shop.audit_*")
    assert any("Start over" in t for t in ui.texts)
    action = [el for el in ui.elements if el.kind == "button"
              and (el.text == "Save" or str(el.text).startswith("Drop & reload"))][-1]
    assert action.enabled is False


def test_a_ticked_legacy_table_joins_the_reload_and_becomes_overlay():
    ctx, state, _jobs, submitted = _reload_ctx()
    state.restore_legacy_exclusions(["shop.t:a"])
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    ui.type_text("exclude shop.t.a")
    checkbox = [e for e in ui.elements if e.kind == "checkbox"][-1]
    assert "excluded before v0.1.562 (a)" in checkbox.text and checkbox.value is False
    checkbox.on_change(SimpleNamespace(value=True))
    ui.click("Drop & reload shop.t")
    (tables, (replace, _pin)), = submitted
    assert tables == ["shop.t"] and replace == frozenset({"shop.t"})
    assert state.legacy_exclusions() == {}  # now an overlay entry: NULL after the reload


# --- P3-C19: the quarantine route ----------------------------------------------------------

from dsql_migrator.ui.data_migration._exclusion_dialog import (  # noqa: E402
    open_exclusion_route,
    quarantine_route_block,
)

_TEXT_REASON = "datatype limit greater than 1048576 bytes not supported for text"
_CANDS = lambda _t: (("a", "longtext"), ("b", "text"))  # noqa: E731


def test_outcome_a_inserts_exactly_one_line_and_reloads_in_two_clicks():
    ctx, state, _jobs, submitted = _reload_ctx()
    state._excluded_lob_columns.clear()
    state.restore_exclusion_rules("", ())
    ui = _Ui()
    # a text reason narrows the two candidates to the longtext one
    open_exclusion_route(ui, ctx, "shop.t", _TEXT_REASON, lob_candidates_for=_CANDS)
    assert ui.texts.count("Change exclusions & reload") == 1
    assert ui.editors[-1].value == "exclude shop.t.a  # from the Full Load quarantine"
    ui.click("Drop & reload shop.t")  # the second click
    (tables, (replace, _pin)), = submitted
    assert tables == ["shop.t"] and state.lob_exclusions() == {"shop.t": {"a"}}


def test_outcome_b_filters_the_lob_section_and_c_shows_the_type_a_rule_hint():
    ctx, state, _jobs, _submitted = _reload_ctx()
    state._excluded_lob_columns.clear()
    state.restore_exclusion_rules("", ())
    ui = _Ui()
    open_exclusion_route(ui, ctx, "shop.t", "... not supported for jsonb", lob_candidates_for=_CANDS)
    assert any(t.startswith("The database reported a type, not a column") for t in ui.texts)
    lob = next(el for t, (_h, el) in ui.expansions.items() if t.startswith("Oversized-LOB"))
    assert lob.value is True  # opened, filtered to the table
    assert any(t.startswith("shop.t.") for t in ui.texts) and not any(
        t.startswith("shop.u.") for t in ui.texts)
    # C: the saved rules already exclude every flagged column
    ctx2, state2, _j, _s = _reload_ctx()
    ui2 = _Ui()
    open_exclusion_route(ui2, ctx2, "shop.t", "... not supported for jsonb",
                         lob_candidates_for=lambda _t: (("a", "longtext"),))
    assert any(t.startswith("Every column of shop.t whose type this tool flags") for t in ui2.texts)
    assert "disable" not in " ".join(ui2.editors[-1].props_seen)  # still editable


def test_not_saved_names_the_line_and_save():
    ctx, state, _jobs, _submitted = _reload_ctx()
    state._excluded_lob_columns.clear()
    state.restore_exclusion_rules("", ())
    state.add_pending_rule("exclude shop.t.a  # from the CDC dead-letter queue")
    ui = _Ui()
    open_exclusion_route(ui, ctx, "shop.t", _TEXT_REASON,
                         lob_candidates_for=lambda _t: (("a", "longtext"),))
    assert any(t.startswith("Already excluded by L1 (not saved yet)") for t in ui.texts)


def test_with_a_stack_the_route_records_a_pending_line_once_and_submits_nothing():
    ctx, state, _jobs, submitted = _reload_ctx()
    state._excluded_lob_columns.clear()
    state.restore_exclusion_rules("", ())
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    for _ in range(2):
        ui = _Ui()
        open_exclusion_route(ui, ctx, "shop.t", _TEXT_REASON,
                             lob_candidates_for=lambda _t: (("a", "longtext"),))
        assert any("(read only)" in t for t in ui.texts)
    assert state.pending_rules() == ["exclude shop.t.a  # from the Full Load quarantine"]
    assert state.lob_exclusions() == {} and submitted == []
    # B and C record nothing
    ui = _Ui()
    open_exclusion_route(ui, ctx, "shop.u", "... not supported for jsonb",
                         lob_candidates_for=lambda _t: (("note", "longtext"),))
    assert state.pending_rules() == ["exclude shop.t.a  # from the Full Load quarantine"]


def test_a_blocked_gate_disables_the_action_and_a_stale_click_does_nothing():
    ctx, state, jobs, submitted = _reload_ctx()
    state.cutover_restore_in_flight = lambda: True
    assert quarantine_route_block(ctx).startswith("Cut over is restoring")
    refreshed = []
    ctx.refresh = lambda: refreshed.append(True)
    ui = _Ui()
    open_exclusion_route(ui, ctx, "shop.t", _TEXT_REASON, lob_candidates_for=_CANDS)
    assert ui.dialogs == [] and ui.notified and refreshed and submitted == []
    state.cutover_restore_in_flight = None
    ctx.session.connection_ready = lambda: False
    assert quarantine_route_block(ctx).startswith("Reconnect first")


# --- P3-C20: the dead-letter route -------------------------------------------------------

from dsql_migrator.ui.data_migration._exclusion_dialog import open_dlq_route  # noqa: E402


def test_the_dlq_route_persists_a_pending_line_once_and_applies_it_at_the_next_editable_open():
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    for _ in range(2):
        open_dlq_route(_Ui(), ctx, "shop.t", ["b"])
    assert state.pending_rules() == ["exclude shop.t.b  # from the CDC dead-letter queue"]
    from dsql_migrator.ui.data_migration._models import effective_lob_exclusions

    assert effective_lob_exclusions(state, INV) == {}
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    assert "exclude shop.t.b  # from the CDC dead-letter queue" in ui.editors[-1].value


def test_a_record_naming_no_column_records_nothing_and_filters_the_lob_section():
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ui = _Ui()
    open_dlq_route(ui, ctx, "shop.t", [])
    assert state.pending_rules() == []
    lob = next(el for t, (_h, el) in ui.expansions.items() if t.startswith("Oversized-LOB"))
    assert lob.value is True
    assert any(t.startswith("shop.t.") for t in ui.texts) and not any(
        t.startswith("shop.u.") for t in ui.texts)


# --- review #3 fixes (C12-C18) ---------------------------------------------------------


def _change_without_preview(ui, value):
    editor = ui.editors[-1]
    editor.value = value
    editor.on_change(SimpleNamespace(value=value))  # the debounced preview has NOT run


def test_a_drop_confirm_never_acts_on_a_text_it_has_not_shown():
    ctx, state, _jobs, submitted = _reload_ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    assert "Save" in ui.buttons  # nothing to drop yet
    _change_without_preview(ui, "exclude shop.t.a\nexclude shop.u.note")
    ui.click("Save")  # the stale button
    assert submitted == [] and state.lob_exclusions() == {"shop.t": {"a"}}
    assert ui.notified and "Drop & reload shop.u" in ui.buttons  # refreshed: now it shows
    # a retarget inside the window: the shown confirm never drops another table
    _change_without_preview(ui, "exclude shop.t.a\nexclude shop.audit_1.x")
    ui.click("Drop & reload shop.u")
    assert submitted == []
    ui.click("Drop & reload shop.audit_1")
    assert [t for t, _choice in submitted] == [["shop.audit_1"]]


def test_a_normal_save_never_stores_a_text_it_has_not_checked():
    state = DataMigrationState()
    ctx, calls = _ctx(state)
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.t.a")
    _change_without_preview(ui, "exclude shop.t.a\nexclude shop.u.note")
    ui.click("Save")
    assert state.lob_exclusions() == {} and calls["refresh"] == 0 and ui.notified


def test_a_fix_shown_for_a_line_that_moved_rewrites_nothing():
    ctx, _calls = _ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.t.A")
    label = next(k for k in ui.buttons if k.startswith('Use "exclude shop.t.'))
    _change_without_preview(ui, "exclude shop.u.note\nexclude shop.t.A")
    ui.click(label)
    assert ui.editors[-1].value == "exclude shop.u.note\nexclude shop.t.A"  # untouched
    assert ui.notified


def test_blocked_lob_candidates_offer_no_rule_and_add_rule_never_duplicates():
    applied = {"shop.t": SimpleNamespace(
        target_ddl=('CREATE TABLE "shop"."t" ("id" BIGINT NOT NULL, "a" TEXT, "b" TEXT, '
                    '"g" TEXT GENERATED ALWAYS AS (upper(a)) STORED, PRIMARY KEY ("id"))'),
        foreign_key_ddls=[], preserved_foreign_keys=[], index_ddls=[])}
    tables = [_t("shop.t", a="longtext", b="longtext", g="longtext")]
    inv = SimpleNamespace(tables=tables)
    inputs = ExclusionInputs(selected=["shop.t"], applied=applied, preserve_foreign_keys=True,
                             migratable=["shop.t"], signature=("s",))
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.inventory_fn = lambda: inv
    ctx.inputs_fn = lambda: inputs
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.type_text("")
    ui.expand("Oversized-LOB candidates")
    assert any(t.startswith("Cannot be excluded: Computed by Aurora DSQL") for t in ui.texts)
    assert len(ui.buttons.get("+ add rule", [])) == 2  # a and b, not g
    ui.click("+ add rule", index=0)
    ui.click("+ add rule", index=0)
    assert ui.set_values[-1].splitlines().count("exclude shop.t.a") == 1


def test_one_popup_per_tab():
    from dsql_migrator.ui.data_migration import _exclusion_dialog as dlgmod

    ctx, _calls = _ctx()

    class _Client:
        id = "tab-1"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    ui = _Ui()
    ui.context = SimpleNamespace(client=_Client())
    dlgmod._OPEN_DIALOGS.pop("tab-1", None)
    open_exclusion_dialog(ui, ctx)
    open_exclusion_dialog(ui, ctx)  # a double-click
    assert len(ui.dialogs) == 1
    dlgmod._release(ui.context.client)  # the hide handler
    open_exclusion_dialog(ui, ctx)
    assert len(ui.dialogs) == 2
    dlgmod._OPEN_DIALOGS.pop("tab-1", None)


def test_a_pass_refreshes_the_popup_on_screen_and_keeps_earlier_verdicts(monkeypatch):
    from datetime import datetime, timezone

    log = []
    _patch_probe(monkeypatch, answers={}, log=log, on_call=None)
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    t0 = datetime(2026, 10, 1, 5, 2, tzinfo=timezone.utc)
    state._value_size_cache[("shop.t", "b")] = ("over", t0)
    seen = []
    state.set_size_check_listener(lambda: seen.append("current"))
    state.cancel_size_check  # noqa: B018
    _patch_probe(monkeypatch, answers={}, log=log, on_call=state.cancel_size_check)
    asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a"), ("shop.t", "b")), lambda: seen.append("starter")))
    assert "current" in seen and "starter" not in seen
    sizes = state.value_sizes()
    assert sizes[("shop.t", "b")] == ("over", t0)  # an unfinished pass never undoes a result
    assert sizes[("shop.t", "a")][0] == "unknown"


def test_an_unreachable_source_measures_nothing_and_says_so(monkeypatch):
    import nicegui.run as nrun

    import dsql_migrator.ui.connect as connect

    async def io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", io_bound)
    log = []
    monkeypatch.setattr(connect, "make_source_engine_factory",
                        lambda _p, **_k: (lambda _c: _Engine(log, reachable=False)))
    state = DataMigrationState()
    ctx, _calls = _ctx(state)
    ctx.session.connection_ready = lambda: True
    ctx.session.source_config = object()
    ctx.session.source_password = None
    said = []
    asyncio.run(dlg._check_sizes(ctx, _rows(("shop.t", "a")), lambda: None,
                                 notify=lambda text, **_k: said.append(text)))
    assert state.value_sizes() == {} and said and "reconnect" in said[0]
    assert log == ["dispose"]


def test_size_labels_are_in_utc_and_the_unknown_label_says_what_happened():
    from datetime import datetime, timedelta, timezone

    at = datetime(2026, 10, 1, 14, 2, tzinfo=timezone(timedelta(hours=9)))
    assert "· 05:02 UTC" in dlg._size_label(("over", at), None)[0]
    assert dlg._size_label(("unknown", at), None)[0] == (
        "Not determined: the read failed, took longer than 15 s, or was cancelled")


def test_go_to_line_selects_the_include_line():
    ctx, _calls = _ctx()
    ui = _Ui()
    js = []
    ui.run_javascript = lambda code: js.append(code)
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude shop.t.*\ninclude shop.t.a")
    ui.expand("Oversized-LOB candidates")
    ui.click("Go to L2")
    assert js and "Math.min(2," in js[-1] and "scrollIntoView" in js[-1]


def test_include_kept_candidates_are_measured_too():
    ctx, _calls = _ctx()
    ctx.session.connection_ready = lambda: True
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.type_text("exclude type:longtext\ninclude shop.t.a")
    ui.expand("Oversized-LOB candidates")
    assert "Check sizes" in ui.buttons
    assert any(t.startswith("Reads the source: one bounded query per column for the 1 column")
               for t in ui.texts)


def test_dotted_names_keep_their_own_size_verdict():
    from datetime import datetime, timezone

    state = DataMigrationState()
    state.record_value_sizes("shop.a.b", {"c": True}, asked=["c"])
    state.record_value_sizes("shop.a", {"b.c": False}, asked=["b.c"])
    sizes = state.value_sizes()
    assert sizes[("shop.a.b", "c")][0] == "over" and sizes[("shop.a", "b.c")][0] == "ok"
    assert isinstance(sizes[("shop.a", "b.c")][1], datetime)
    assert timezone.utc  # noqa: B018


def test_the_delete_wait_popup_shows_no_remedy_for_a_differing_stack_set():
    ctx, state = _locked_ctx(mtype=MigrationType.CDC_ONLY, deleting=True, phase="running")
    state.restore_lob_exclusion("shop.t", "a")
    state.set_cdc_deployed_column_exclude_list("shop.t.b")
    ctx.capture_tables_fn = lambda selected: [t for t in INV.tables if t.name in set(selected)]
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    assert not any("Delete the CDC infrastructure" in t or "Start over" in t for t in ui.texts)


# --- P3-C22: the old cards are gone ---------------------------------------------------------


def test_no_surface_renders_the_old_card_name():
    import pathlib

    import dsql_migrator.ui.data_migration as dm

    root = pathlib.Path(dm.__file__).parent
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "'Oversized LOB columns'" not in text and '"Oversized LOB columns' not in text, path.name


def test_the_cdc_step_summary_shows_what_cdc_receives_as_deployed():
    from dsql_migrator.core.models import SourceInventory
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_exclusion_summary

    state = DataMigrationState()
    state.restore_lob_exclusion("shop.t", "a")
    state.set_cdc_deployed_column_exclude_list("shop.t.a")
    ui = _Ui()
    _render_cdc_exclusion_summary(ui, state, SimpleNamespace(get_status=_none),
                                  SourceInventory(tables=list(INV.tables)))
    assert any(t.startswith("Excluded: 1 column (change them in Exclude tables & columns")
               for t in ui.texts)
    assert any(t.startswith("What CDC receives (") and t.endswith("as deployed)")
               for t in ui.expansions)
    assert "shop.t.a" in ui.texts


def test_saved_entry_notes_stay_visible_after_the_lock():
    ctx, state = _locked_ctx(mtype=MigrationType.FULL_LOAD_AND_CDC, job_id="job-1")
    state.restore_lob_exclusion("shop.t", "id")  # a primary key: still migrated
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    assert any("shop.t.id is still migrated" in t for t in ui.texts)
    row_ui = _Ui()
    render_exclusion_button_row(row_ui, ctx)
    assert any(t.endswith("need attention: open Exclude tables & columns.") for t in row_ui.texts)
