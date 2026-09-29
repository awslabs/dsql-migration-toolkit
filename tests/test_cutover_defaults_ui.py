# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-16: the cut-over DEFAULT block -- ONE drained checkbox shared by every drained-gated
button (Finding 5.1), the block copy, the finish-area warning and the acknowledgement
WARNING."""

from types import SimpleNamespace

import pytest

from dsql_migrator.ui.cutover_defaults import CutoverDefaultItem, CutoverDefaultsSnapshot, CutoverRestoreOutcome


class _El:
    def __init__(self, ui, kind, text="", on_click=None, on_change=None):
        self.ui, self.kind, self.text = ui, kind, text
        self.on_click, self.on_change = on_click, on_change
        self.disabled = False

    def props(self, value=None, remove=None, **_k):
        if remove and "disable" in remove:
            self.disabled = False
        elif value and "disable" in str(value).split():
            self.disabled = True
        return self

    def __getattr__(self, _name):
        return lambda *_a, **_k: self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Ui:
    def __init__(self):
        self.texts, self.buttons, self.checkboxes, self.notes = [], [], [], []

    def _el(self, kind, text="", **kw):
        if text:
            self.texts.append(str(text))
        return _El(self, kind, str(text), **kw)

    def label(self, text="", *_a, **_k):
        return self._el("label", text)

    def button(self, text="", *_a, on_click=None, **_k):
        el = self._el("button", text, on_click=on_click)
        self.buttons.append(el)
        return el

    def checkbox(self, text="", *_a, value=None, on_change=None, **_k):
        el = self._el("checkbox", text, on_change=on_change)
        el.value = value
        self.checkboxes.append(el)
        return el

    def notify(self, message, **_k):
        self.notes.append(message)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _El(self, "x")

    def by(self, text):
        return [b for b in self.buttons if b.text == text]

    def body(self):
        return " ".join(self.texts)


def _item(column, status="pending", auto=True, executable=True, reason=None, table="app.t"):
    return CutoverDefaultItem(
        table=table, column=column, original_default="'x'::text", status=status, live_default=None,
        auto=auto, reason=reason, executable=executable, dropped_not_null=True,
        statement=f'ALTER TABLE "app"."t" ALTER COLUMN "{column}" SET DEFAULT (\'x\'::text);')


def _snap(*items, nullable=("app.t.a",)):
    return CutoverDefaultsSnapshot(state="loaded", items=tuple(items), nullable=nullable)


def _render(ui, *, cdc=True, snap=None, fk=0, drained=False, block=None, checking=False, provider=None,
            fk_provider=None, outcome=None, gated=None):
    from dsql_migrator.ui.validation import _render_cutover_section

    from tests.test_ui_validation import _cutover_summary, _no_drift

    latched = []
    notices = []
    import dsql_migrator.ui.validation as uiv

    orig = uiv.render_notice
    uiv.render_notice = lambda _ui, **kw: (notices.append(kw), _ui.texts.append(kw.get("header", "")),
                                           _ui.texts.append(kw.get("body", "")))
    try:
        _render_cutover_section(
            ui, _cutover_summary(), _no_drift(), cdc_in_use=cdc,
            fk_apply_provider=fk_provider or (lambda: None), fk_pending_count=fk,
            cdc_drained_confirmed=drained, on_cdc_drained_change=latched.append,
            default_restore=snap, defaults_checking=checking,
            restore_provider=provider or (lambda request: None), restore_job_block=block,
            restore_outcome=outcome, drained_gated=gated)
    finally:
        uiv.render_notice = orig
    return latched, notices


def test_finding_5_1_one_checkbox_enables_the_restores_without_foreign_keys():
    ui = _Ui()
    calls = []
    manual = _item("m", auto=False, reason="Your Schema Conversion DDL changed.")
    latched, _n = _render(ui, snap=_snap(_item("a"), manual), fk=0, provider=calls.append)
    assert len(ui.checkboxes) == 1
    restore_all, = ui.by("Restore DEFAULTs")
    row, = ui.by("Restore")
    assert restore_all.disabled and row.disabled
    ui.checkboxes[0].on_change(SimpleNamespace(value=True))
    assert not restore_all.disabled and not row.disabled and latched == [True]
    restore_all.on_click()
    row.on_click()
    assert calls == [None, ("app.t", "m")]
    ui.checkboxes[0].on_change(SimpleNamespace(value=False))
    assert restore_all.disabled and row.disabled and latched == [True, False]


def test_with_foreign_keys_pending_there_is_still_one_checkbox_for_all_three():
    ui = _Ui()
    applied = []
    _render(ui, snap=_snap(_item("a"), _item("m", auto=False, reason="r")), fk=2,
            fk_provider=lambda: applied.append(1))
    assert len(ui.checkboxes) == 1
    fk, = ui.by("Apply foreign keys")
    buttons = [fk, *ui.by("Restore DEFAULTs"), *ui.by("Restore")]
    assert all(b.disabled for b in buttons)
    ui.checkboxes[0].on_change(SimpleNamespace(value=True))
    assert not any(b.disabled for b in buttons)
    fk.on_click()
    assert applied == [1]
    assert "CDC has drained" in ui.checkboxes[0].text and "frozen source writes" in ui.checkboxes[0].text


def test_a_running_load_keeps_restore_disabled_after_a_tick_while_fk_apply_enables():
    ui = _Ui()
    _render(ui, snap=_snap(_item("a"), _item("m", auto=False, reason="r")), fk=1, block="off while a Full Load runs")
    ui.checkboxes[0].on_change(SimpleNamespace(value=True))
    assert not ui.by("Apply foreign keys")[0].disabled
    assert ui.by("Restore DEFAULTs")[0].disabled and ui.by("Restore")[0].disabled
    assert "Restore is off while a Full Load runs." in ui.body()


def test_the_checkbox_appears_once_the_fill_finds_a_pending_restore():
    ui = _Ui()
    _render(ui, snap=None, checking=True)
    assert ui.checkboxes == [] and "checking Aurora DSQL" in ui.body()
    ui = _Ui()
    _render(ui, snap=_snap(_item("a")))
    assert len(ui.checkboxes) == 1


def test_the_block_copy():
    ui = _Ui()
    _render(ui, snap=_snap(_item("a"), _item("b", status="pending", auto=False, executable=False,
                                                reason="DEFAULT is not a single expression.")))
    body = ui.body()
    assert "Excluded-column DEFAULTs restored: 0 of 2" in body
    assert "Before you repoint: restore DEFAULTs on 2 excluded columns" in body
    assert "Restoring affects new inserts only; migrated rows stay NULL." in body
    assert "gives those rows the DEFAULT" in body  # the stated lag limit (CDC)
    assert "stay nullable (Aurora DSQL cannot add NOT NULL back in place): app.t.a" in body
    assert "Needs your decision (1):" in body and "Run this statement yourself." in body
    assert ui.by("Restore") == []  # the non-executable row gets no button
    assert 'ALTER TABLE "app"."t" ALTER COLUMN "a" SET DEFAULT' in body
    assert "Change & reload" not in body
    full = _Ui()
    _render(full, cdc=False, snap=_snap(_item("a")))
    assert "gives those rows the DEFAULT" not in full.body() and full.checkboxes == []
    assert not full.by("Restore DEFAULTs")[0].disabled  # not drained-gated without CDC


def test_the_unreadable_states_offer_check_again():
    for state, text in (("ledger_unreadable", "record of dropped DEFAULTs could not be read"),
                        ("target_unreadable", "Could not read Aurora DSQL's catalog")):
        ui = _Ui()
        from dsql_migrator.ui.validation import _render_cutover_defaults_block

        rechecked = []
        _render_cutover_defaults_block(
            ui, CutoverDefaultsSnapshot(state=state), checking=False, cdc_in_use=True, drained={"ok": False},
            gated=[], restore_provider=None, restore_job_block=None, restore_running=False, restore_outcome=None,
            on_recheck=lambda: rechecked.append(1))
        assert text in ui.body()
        ui.by("Check again")[0].on_click()
        assert rechecked == [1]


def test_the_outcome_shows_the_refusal_and_each_failed_statement():
    ui = _Ui()
    outcome = CutoverRestoreOutcome(failed={"app.t.a": "42501: denied"},
                                    failed_statements={"app.t.a": "ALTER TABLE x;"}, refused=None)
    _l, notices = _render(ui, snap=_snap(_item("a")), outcome=outcome)
    failed = [n for n in notices if "could not be restored" in n["header"]]
    assert failed and "42501: denied" in failed[0]["body"] and "ALTER TABLE x;" in failed[0]["body"]
    ui = _Ui()
    _l, notices = _render(ui, snap=_snap(_item("a")), outcome=CutoverRestoreOutcome(refused="not restored: busy"))
    assert any(n["header"] == "DEFAULTs not restored" and n["body"] == "not restored: busy" for n in notices)


# ---------------------------------------------------------------------------
# The screen: the finish warning and the acknowledgement WARNING
# ---------------------------------------------------------------------------


def _screen(monkeypatch, *, snap, cdc=True, drained=False, in_use=True, exclude=False):
    import sys
    import types

    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import TableValidationResult, ValidationMode, ValidationReport
    from dsql_migrator.core.overlay_ledger import InMemoryOverlayLedger
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen

    ui = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: cdc)
    monkeypatch.setattr(uiv, "render_notice", lambda _ui, **kw: (ui.texts.append(kw.get("header", "")),
                                                                 ui.texts.append(kw.get("body", ""))))
    logged = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw)))
    sessions = SessionStore()
    sessions.get_or_create("s1").set_target(SimpleNamespace(
        cluster_endpoint="c.dsql.us-east-1.on.aws", database="postgres", region="us-east-1"))
    vstore, mstore = ValidationStore(), DataMigrationStore()
    content, runner = build_cutover_screen(sessions, "s1", validation_store=vstore, migration_store=mstore,
                                           overlay_ledger=InMemoryOverlayLedger())
    state = vstore.get_or_create("s1")
    state.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[TableValidationResult(
        table="app.t", source_row_count=1, target_row_count=1, row_count_match=True, matched=True)]))
    if in_use:
        mstore.get_or_create("s1").mark_overlay_ledger_in_use()
    if exclude:
        mstore.get_or_create("s1").set_lob_exclusion("app.t", "c", True)
    if snap is not None:
        import time

        gen = state.begin_cutover_defaults_fill()
        from dataclasses import replace

        from dsql_migrator.ui.validation import _cutover_fill_key

        key = _cutover_fill_key(sessions.get_or_create("s1"), mstore.get_or_create("s1"), state, None, None)
        state.finish_cutover_defaults_fill(replace(snap, fill_key=key, filled_monotonic=time.monotonic()), gen)
    state.set_cutover_cdc_drained_confirmed(drained)
    content(lambda: None)
    return ui, runner, logged


def test_the_finish_warning_offers_restore_and_never_disables_the_acknowledgement(monkeypatch):
    ui, _runner, _logged = _screen(monkeypatch, snap=_snap(_item("a"), _item("b")))
    assert "2 excluded column(s) still have no DEFAULT on Aurora DSQL" in ui.body()
    restores = ui.by("Restore DEFAULTs")
    assert len(restores) == 2 and all(b.disabled for b in restores)  # drained-gated under CDC
    ui.checkboxes[0].on_change(SimpleNamespace(value=True))
    assert not any(b.disabled for b in restores)  # the ONE checkbox enables the finish button too
    # Never blocking (the FK gate aside, which a Full-Load-only render does not have).
    full, _r, _l = _screen(monkeypatch, snap=_snap(_item("a"), _item("b")), cdc=False)
    ack, = full.by("I've cut over to DSQL")
    assert not ack.disabled and "still have no DEFAULT" in full.body()
    done, _r, _l = _screen(monkeypatch, snap=_snap(_item("a", status="done", auto=False)))
    assert "still have no DEFAULT" not in done.body()


@pytest.mark.parametrize("snap,expect", [
    (_snap(_item("a"), _item("b")), "cut over with 2 DEFAULTs not restored"),
    (_snap(_item("a", status="done", auto=False)), None),
    (CutoverDefaultsSnapshot(state="ledger_unreadable"), "could confirm the excluded columns' DEFAULTs (ledger_unreadable)"),
])
def test_the_acknowledgement_logs_owed_defaults_as_one_warning(monkeypatch, snap, expect):
    import dsql_migrator.ui.validation as uiv

    ui, runner, logged = _screen(monkeypatch, snap=snap, cdc=False)
    runner()
    lines = [kw for action, kw in logged if action == "excluded-column DEFAULTs not restored at cut over"]
    if expect is None:
        assert lines == []
    else:
        assert len(lines) == 1 and expect in lines[0]["detail"]
        assert lines[0]["status"] is uiv.ActivityStatus.WARNING


def test_a_session_that_never_used_the_record_logs_nothing(monkeypatch):
    _ui, runner, logged = _screen(monkeypatch, snap=None, cdc=False, in_use=False)
    runner()
    assert not [a for a, _kw in logged if a == "excluded-column DEFAULTs not restored at cut over"]


# ---------------------------------------------------------------------------
# Review fixes (P2-15/16 adversarial review)
# ---------------------------------------------------------------------------


def test_the_checking_state_offers_check_again():
    from dsql_migrator.ui.validation import _render_cutover_defaults_block

    ui = _Ui()
    rechecked = []
    _render_cutover_defaults_block(
        ui, None, checking=True, cdc_in_use=False, drained={"ok": True}, gated=[], restore_provider=None,
        restore_job_block=None, restore_running=False, restore_outcome=None,
        on_recheck=lambda: rechecked.append(1))
    ui.by("Check again")[0].on_click()
    assert rechecked == [1]


def test_an_all_skipped_restore_is_reported(monkeypatch):
    import dsql_migrator.ui.validation as uiv

    logged = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw)))
    outcome = CutoverRestoreOutcome(skipped=("app.t.a",))
    uiv._log_cutover_restore(outcome)
    assert [(a, kw["status"]) for a, kw in logged] == [
        ("excluded-column DEFAULTs not restored (cut-over)", uiv.ActivityStatus.WARNING)]
    assert "app.t.a" in logged[0][1]["detail"]
    ui = _Ui()
    _l, notices = _render(ui, snap=_snap(_item("a")), outcome=outcome)
    assert any(n["header"] == "1 DEFAULT(s) not restored" and "record changed" in n["body"] for n in notices)


def test_the_restored_line_names_each_statement_once_terminated(monkeypatch):
    """Each statement carries its own ';' (the block shows them so): the activity line
    must not double it before the next clause (live: '...::text);; migrated rows')."""
    import dsql_migrator.ui.validation as uiv

    logged = []
    monkeypatch.setattr(uiv, "log_activity", lambda cat, action, **kw: logged.append((action, kw)))
    uiv._log_cutover_restore(CutoverRestoreOutcome(
        restored=("app.t.a", "app.t.b"),
        statements=('ALTER TABLE "app"."t" ALTER COLUMN "a" SET DEFAULT (0);',
                    'ALTER TABLE "app"."t" ALTER COLUMN "b" SET DEFAULT (1);')))
    detail = logged[0][1]["detail"]
    assert ";;" not in detail
    assert ('ran: ALTER TABLE "app"."t" ALTER COLUMN "a" SET DEFAULT (0); '
            'ALTER TABLE "app"."t" ALTER COLUMN "b" SET DEFAULT (1); migrated rows stay NULL') in detail


def test_a_session_that_never_excluded_sees_nothing_of_an_unreadable_record(monkeypatch):
    for in_use, excluded in ((False, False), (True, False), (False, True)):
        ui, runner, logged = _screen(monkeypatch, snap=CutoverDefaultsSnapshot(state="ledger_unreadable"),
                                     cdc=False, in_use=in_use, exclude=excluded)
        shown = "record of dropped DEFAULTs could not be read" in ui.body()
        runner()
        warned = [a for a, _kw in logged if a == "excluded-column DEFAULTs not restored at cut over"]
        uses = in_use or excluded
        assert shown is uses and bool(warned) is uses, (in_use, excluded)
    ui, _runner, _logged = _screen(monkeypatch, snap=None, cdc=False, in_use=False)
    assert "checking Aurora DSQL" not in ui.body() and "Checking whether" not in ui.body()


def test_the_finish_body_names_only_what_the_block_shows(monkeypatch):
    unknown = _item("a", status="unknown", auto=False)
    ui, _r, _l = _screen(monkeypatch, snap=_snap(unknown), cdc=False)
    body = ui.body()
    assert "1 excluded column(s) could not be checked on Aurora DSQL" in body
    assert "Needs your decision" not in body and "will store NULL" not in body and "Check again" in body
    ui, _r, _l = _screen(monkeypatch, snap=_snap(_item("a")), cdc=False)
    body = ui.body()
    assert "Restore them now." in body and "Needs your decision" not in body and "will store NULL" in body
    ui, _r, _l = _screen(monkeypatch, snap=_snap(_item("m", auto=False, reason="r")), cdc=False)
    assert 'Decide on the columns listed under "Needs your decision" above.' in ui.body()
