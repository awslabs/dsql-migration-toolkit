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

    def __getattr__(self, _name):
        return lambda *_a, **_k: self._el(_name)

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
