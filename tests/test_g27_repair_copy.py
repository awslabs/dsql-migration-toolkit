# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 repair R-COPY (rule E, copy only): a re-run of the Full Load for a NEW copy names
'Drop & reload', because an Append keeps the rows already on the target as they were.

* [a] The Start CDC dialog's "The start point's binary log has been purged" warning (the Full
  Load branch of ``core.watermark.binlog_resume_gap_reason``) said "Re-run the Full Load to take
  a fresh snapshot". The Full Load dialog opens on Append, so an operator who followed that
  remedy appended over the earlier copy: a row updated or deleted on the source before the new
  watermark stayed stale (or extra) on the target, and CDC from that watermark never replays it.
  The remedy now names Drop & reload and says why.
* [b] That dialog (Start, Re-run Full Load, Re-run all tables) labelled Append "(idempotent;
  recommended)" in Full load + CDC too, where the run takes CDC's new start point. For a CDC
  migration type and a run that takes a new start point, Append is no longer called
  recommended, and one line under the choice names Drop & reload. The default selection is
  still Append (the destructive option is never preselected). Full load only, and the actions
  that keep the start point (Retry unfinished tables, Reload specific tables), are unchanged.

These tests drive the real dialog builders through NiceGUI doubles. Only the source and target
boundary is faked: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType, StepStatus
from dsql_migrator.core.watermark import binlog_resume_gap_reason
from dsql_migrator.ui.data_migration import MigrationType
from dsql_migrator.ui.design import INLINE_HINT_TEXT

from tests.test_g12_table_set_after_load import _Jobs, _loaded
from tests.test_g17_full_load_and_cdc_additions import _G17PgSession, _G17Session
from tests.test_g25_fl_r6 import NEVER_STARTED, _Ui, _failed_b, _state
from tests.test_ui_data_migration import _ConfirmDialogUi, _StubConnector

MY, PG = SourceType.MYSQL, SourceType.POSTGRES
FLC, FLO = MigrationType.FULL_LOAD_AND_CDC, MigrationType.FULL_LOAD_ONLY

# ---------------------------------------------------------------------------
# [a] The binlog-purged remedy
# ---------------------------------------------------------------------------

_PURGED_HEADER = "The start point's binary log has been purged"
_RERUN_DROP = "Re-run the Full Load choosing 'Drop & reload' to take a fresh snapshot"
_WHY = (
    "an Append keeps the rows already on the target as they were, and CDC from the new "
    "watermark never replays the changes made before it."
)
# The remedy before the fix (no Drop & reload).
_OLD_REMEDY = "Re-run the Full Load to take a fresh snapshot"
# The Manual branch's text, unchanged by this repair (its remedy is a retained position).
_MANUAL_TEXT = (
    "The Manual start point is in binary log 'mysql-bin.000090', which the source no longer "
    "retains (oldest kept: 'mysql-bin.000095'), so CDC cannot start from it — the source "
    "connector fails to find that log. Enter a position in a log the source still retains "
    "(Fetch current position fills the current one); changes before that position are not "
    "replicated."
)


def test_the_purged_binlog_remedy_names_drop_and_reload() -> None:
    # FAILS WITHOUT THE FIX: "Re-run the Full Load to take a fresh snapshot", with no Drop &
    # reload, and the re-run dialog opens on Append.
    reason = binlog_resume_gap_reason(
        "mysql-bin.000090", ["mysql-bin.000095", "mysql-bin.000096"]
    )
    assert reason is not None
    assert _RERUN_DROP in reason
    assert _WHY in reason
    assert _OLD_REMEDY not in reason
    # The rest of the remedy stays: which log is gone, raising retention first, and the Manual
    # alternative that accepts the gap.
    assert "'mysql-bin.000090'" in reason and "(oldest kept: 'mysql-bin.000095')" in reason
    assert "CALL mysql.rds_set_configuration('binlog retention hours', 168)" in reason
    assert reason.endswith("Or start CDC from a manual position and accept the gap.")


def test_the_manual_start_point_remedy_is_unchanged() -> None:
    # Guard: a Manual start point's remedy is a position the source still retains, not a
    # re-run of the Full Load.
    manual = binlog_resume_gap_reason("mysql-bin.000090", ["mysql-bin.000095"], manual=True)
    assert manual == _MANUAL_TEXT
    assert "Drop & reload" not in manual


def test_the_start_cdc_dialog_purge_warning_names_drop_and_reload(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX. The real read-only probe and the real Start CDC dialog run here.
    # Only the source engine (SHOW BINARY LOGS) is faked, and the dialog's other reads are
    # stubbed because they are not what this test checks. The setup is MySQL Full load + CDC
    # on a stack that never started, with Automatic from the Full Load watermark (W1).
    import nicegui

    import dsql_migrator.core.watermark as wm
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_g25_cdc_r7 import _JM, _W1, _never_started
    from tests.test_ui_data_migration import _DialogUi

    class _Engine:
        def connect(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def dispose(self):
            return None

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda _pw, **_k: (lambda _cfg: _Engine())
    )
    monkeypatch.setattr(wm, "list_binary_logs", lambda _conn: ["mysql-bin.000095"])
    for name in (
        "_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_cdc_blocking_foreign_keys",
        "_pg_objects_gate_block",
    ):
        monkeypatch.setattr(_cdc_ui, name, lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    ui = _DialogUi()
    bodies: dict = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        bodies[header] = body

    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=MY, database="app", port=3306),
        source_password="pw", target_config=None, aws_profile=None, source_secret_id=None,
    )
    state = _never_started()
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(
            ui, state, lambda: None, session=session, job_manager=_JM(_W1), inventory=None,
        )
    )
    # Still a warning, not a block, and its remedy names Drop & reload.
    assert ("warning", _PURGED_HEADER) in ui.notices
    body = bodies[_PURGED_HEADER]
    assert "'mysql-bin.000090'" in body
    assert _RERUN_DROP in body and _WHY in body
    assert _OLD_REMEDY not in body


# ---------------------------------------------------------------------------
# [b] The Full Load dialog's Append / Drop & reload choice
# ---------------------------------------------------------------------------

_APPEND_RECOMMENDED = (
    "Append — keep existing rows, load only the missing ones (idempotent; recommended)"
)
_APPEND_PLAIN = "Append — keep existing rows, load only the missing ones (idempotent)"
_DROP = (
    "Drop & reload — DROP and recreate these tables first for a clean load (existing rows are "
    "permanently lost; DSQL has no TRUNCATE)"
)
_LINE = (
    "CDC from this run's new start point needs 'Drop & reload': an Append keeps the rows "
    "already on the target as they were, and CDC from the new watermark never replays the "
    "changes made before it."
)
_RECREATES = (
    "Drop & reload recreates each table from your applied Schema Conversion (including any "
    "edits you made there), then rebuilds its secondary indexes once every table's data has "
    "loaded."
)

# The actions that take a NEW start point: (job record, step status).
_NEW_START_POINT = {
    "Start Full Load": (lambda: None, StepStatus.NOT_STARTED),
    "Re-run Full Load": (_loaded, StepStatus.DONE),
    "Re-run all tables": (_failed_b, StepStatus.FAILED),
}
# The actions that keep the original start point (the retry engine, original watermark).
_KEEPS_START_POINT = {
    "Retry unfinished tables": (_failed_b, StepStatus.FAILED),
    "Reload specific tables": (_loaded, StepStatus.DONE),
}


class _Label(_ConfirmDialogUi._El):
    """A label that records the classes it is given (an inline hint's tone)."""

    def __init__(self, ui, text):
        super().__init__(ui)
        self._text = text

    def classes(self, spec="", *_a, **_k):
        self._ui.label_classes.setdefault(self._text, []).append(str(spec))
        return self


class _ChoiceUi(_Ui):
    """The G25 Full Load step double, plus each radio's options and initial value, and each
    label's classes."""

    def __init__(self):
        super().__init__()
        self.choices: list = []
        self.label_classes: dict = {}

    def radio(self, options=None, *_a, value=None, on_change=None, **_k):
        self.choices.append((dict(options or {}), value))
        return super().radio(options, on_change=on_change)

    def label(self, text="", *a, **k):
        super().label(text, *a, **k)
        return _Label(self, str(text))


def _open(monkeypatch, *, engine, mtype, action, reload_mode=None):
    """Render the real Full Load step and open ``action``'s confirm dialog, with both selected
    tables holding rows on the target. In Full load + CDC the real probe reads a stack that
    never started, so the new-start-point gate does not hold the action. ``reload_mode`` is
    the choice left by an earlier dialog. Returns the double, the state and the reload choice
    ``(options, initial value)``."""
    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    make_job, status = {**_NEW_START_POINT, **_KEEPS_START_POINT}[action]
    job = make_job()
    state = _state(
        monkeypatch, NEVER_STARTED if mtype is FLC else None, mtype=mtype,
        job_id="job-1" if job is not None else None,
    )
    if reload_mode is not None:
        state.set_reload_mode(reload_mode)

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())
    monkeypatch.setattr(fl, "tables_with_rows", lambda names, **_k: list(names))
    monkeypatch.setattr(fl, "target_primary_keys", lambda names, **k: {n: ["id"] for n in names})
    monkeypatch.setattr(fl, "tables_present", lambda names, **k: list(names))
    ui = _ChoiceUi()
    dm._render_full_load_step(
        ui, state, _Jobs({"job-1": job} if job is not None else {}),
        _G17PgSession() if engine is PG else _G17Session(), job=job, status=status,
        selected_names=["app.a", "app.b"], retry_tables=lambda names: None, guard_reason=None,
        start_full_load=lambda: None, retry_failed_load=lambda *a: None,
        reload_table=lambda *a: None, accept_quarantine_and_continue=lambda *a: None,
        stop_full_load=lambda *a: None, refresh=lambda *a: None,
        schema_recreate_candidates=lambda: [],
    )
    (handler,) = [h for (text, h) in ui.handlers if text.startswith(action)]
    asyncio.run(handler())
    assert ui.opened == 1, f"the {action!r} dialog did not open"
    (choice,) = [c for c in ui.choices if set(c[0]) == {"append", "drop"}]
    return ui, state, choice


def _assert_append_is_the_default(ui, state, value) -> None:
    """The dialog opens on Append, and its confirm button says so."""
    assert value == "append"
    assert state.reload_mode == "append"
    assert ui.start().text == "Append and load" and ui.start().color == "primary"


@pytest.mark.parametrize("engine", [MY, PG], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("action", list(_NEW_START_POINT))
def test_a_new_start_point_in_full_load_and_cdc_names_drop_and_reload(
    monkeypatch, engine, action
) -> None:
    # FAILS WITHOUT THE FIX: "Append — ... (idempotent; recommended)" with nothing tying the
    # choice to CDC, though CDC starts at this run's watermark and an Append keeps the earlier
    # copy's rows as they were.
    ui, state, (options, value) = _open(monkeypatch, engine=engine, mtype=FLC, action=action)
    assert options == {"append": _APPEND_PLAIN, "drop": _DROP}
    assert not [t for t in ui.texts if "recommended" in t and t.startswith("Append")]
    # One line, directly under the choice, as an amber inline hint.
    assert ui.texts.count(_LINE) == 1
    at = ui.texts.index(_LINE)
    assert ui.texts[at - 2: at] == [_APPEND_PLAIN, _DROP]
    assert ui.texts[at + 1] == _RECREATES
    assert ui.label_classes[_LINE] == [f"text-xs {INLINE_HINT_TEXT['warning']}"]
    # The default selection did not change: the dialog still opens on Append.
    _assert_append_is_the_default(ui, state, value)


@pytest.mark.parametrize("engine", [MY, PG], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("action", list(_NEW_START_POINT))
def test_full_load_only_keeps_the_choice_as_it_was(monkeypatch, engine, action) -> None:
    # Guard: Full load only has no CDC step, so its choice is unchanged.
    ui, state, (options, value) = _open(monkeypatch, engine=engine, mtype=FLO, action=action)
    assert options == {"append": _APPEND_RECOMMENDED, "drop": _DROP}
    assert _LINE not in ui.texts
    _assert_append_is_the_default(ui, state, value)


@pytest.mark.parametrize("engine", [MY, PG], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("mtype", [FLC, FLO], ids=["full_load_and_cdc", "full_load_only"])
@pytest.mark.parametrize("action", list(_KEEPS_START_POINT))
def test_a_retry_or_reload_keeps_the_choice_as_it_was(monkeypatch, engine, mtype, action) -> None:
    # Guard: Retry unfinished tables and Reload specific tables run under the original
    # watermark, so an Append there converges and its choice is unchanged.
    ui, state, (options, value) = _open(monkeypatch, engine=engine, mtype=mtype, action=action)
    assert options == {"append": _APPEND_RECOMMENDED, "drop": _DROP}
    assert _LINE not in ui.texts
    assert value == "append" and state.reload_mode == "append"


@pytest.mark.parametrize("mtype", [FLC, FLO], ids=["full_load_and_cdc", "full_load_only"])
@pytest.mark.parametrize("action", list(_NEW_START_POINT))
def test_the_dialog_still_opens_on_append_after_an_earlier_drop_choice(
    monkeypatch, mtype, action
) -> None:
    # Guard: Drop & reload is never preselected, even when an earlier dialog chose it. The
    # operator picks it; the confirm button then reads as destructive.
    ui, state, (_options, value) = _open(
        monkeypatch, engine=MY, mtype=mtype, action=action, reload_mode="drop"
    )
    _assert_append_is_the_default(ui, state, value)
    (radio,) = ui.radios
    radio.pick("drop")
    assert state.reload_mode == "drop"
    assert ui.start().text == "Drop, recreate and load" and ui.start().color == "negative"


# --- [G27 repair review R3] the purged-binlog remedy in CDC only names the type switch -----------


def test_the_purged_binlog_remedy_in_cdc_only_names_the_type_switch(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: CDC only has no Full Load step, yet the remedy said only "Re-run the
    # Full Load choosing 'Drop & reload'" -- every other CDC-only Drop & reload route names the
    # switch to Full load only for that run.
    import dsql_migrator.core.watermark as wm_mod
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.ui.cdc_start_point_gate import drop_and_reload_rerun
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_g25_cdc_r7 import _JM, _W1, _never_started

    class _Engine:
        def connect(self):
            class _C:
                def __enter__(self):
                    return object()

                def __exit__(self, *_a):
                    return False

            return _C()

        def dispose(self):
            return None

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine())
    )
    monkeypatch.setattr(wm_mod, "list_binary_logs", lambda _c: ["mysql-bin.000095"])
    session = SimpleNamespace(source_config=SimpleNamespace(), source_password="pw")
    note = drop_and_reload_rerun(MigrationType.CDC_ONLY)[1].strip()
    assert note.startswith("(CDC only has no Full Load step")
    cdc_only = _cdc_ui._probe_binlog_resume_gap(
        _never_started(migration_type=MigrationType.CDC_ONLY), _JM(_W1), session
    )
    assert cdc_only and "choosing 'Drop & reload'" in cdc_only and note in cdc_only
    # Full load + CDC has the step: unchanged.
    flc = _cdc_ui._probe_binlog_resume_gap(_never_started(), _JM(_W1), session)
    assert flc and "CDC only has no Full Load step" not in flc
