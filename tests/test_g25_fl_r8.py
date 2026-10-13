# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 8 (last), package FL: rules D, E and F on the Full Load side.

* Rule E -- a Full Load re-run that makes a NEW copy (before CDC from a new start point, or a
  fresh copy before cut over) is a Drop & reload: an append keeps the rows already on the
  target as they were, so the source's updates and deletes since the earlier load are lost.
  The Full load only completion notice (MySQL), Validation's Full load only cut-over step 2
  and Validation's never-streamed recovery runbook for a CDC type say so.
* Rule F -- Full load only has no CDC step: its completion notice switches the type first,
  then deletes and deploys on the CDC step.
* PostgreSQL's Full load only notice no longer promises "no re-snapshot": its CDC only Start
  sends snapshot.mode=initial, which snapshots first when the stack's source connector never
  completed its snapshot.
* CDC only with no Full Load record has no Reload specific tables: its extra-row remedy is a
  Full Load from a new start point, behind new CDC topics on a stack that has streamed.
* Rule D -- the CDC-live Retry / Reload notice states the CDC-topics condition.

Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import inspect

import pytest

from dsql_migrator.core.models import (
    ReconcileResult,
    StepStatus,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.ui.data_migration import MigrationType

from tests.test_g12_table_set_after_load import _Jobs, _loaded
from tests.test_g12_table_set_after_load import _screen as _flo_screen
from tests.test_g25_fl_r6 import (
    MY,
    NEVER_STARTED,
    PG,
    ROLLED_BACK,
    STOPPED,
    _click,
    _failed_b,
    _live_notice,
    _probe,
    _render,
    _state,
)
from tests.test_g25_fl_r7 import _deployed, _in_order, _runbook, _steps, _wm_job
from tests.test_ui_validation import (
    _CopyUi,
    _CutoverUi,
    _cutover_summary,
    _drift_na,
    _no_drift,
    _release_summary,
)

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
CDC_ONLY = MigrationType.CDC_ONLY

_HEADER = "Full Load complete — want continuous replication (CDC) next?"
_DROP_RELOAD = "Re-run Full Load choosing 'Drop & reload'"
_SWITCH_CDC_ONLY = 'set the migration type to "CDC only"'
_DELETE = "Delete CDC infrastructure and deploy it again"
# The copy rule E retires: an Append re-run sold as idempotent / safe for a fresh copy.
_APPEND_IS_SAFE = (
    "it's idempotent",
    "only fills the unfinished",
    "safe and duplicate-free",
    "it only fills missing rows and never creates duplicates",
)


def _notice(monkeypatch, *, job, stack, pg=False) -> str:
    """The Full load only completion notice's body, from the REAL screen over the REAL
    stack-phase probe (``stack`` None: never probed)."""
    s = _flo_screen(monkeypatch, job=job, selected=["app.a", "app.b"], pg=pg)
    if stack is not None:
        _probe(monkeypatch, s.state, stack)
    s.content(lambda: None)
    texts = s.ui.texts
    return texts[texts.index(_HEADER) + 1]


# ---------------------------------------------------------------------------
# 1. The Full load only completion notice (rules E and F; PostgreSQL's re-snapshot)
# ---------------------------------------------------------------------------

_NEWER = {
    "stopped, newer watermark": (_deployed("mysql-bin.000003", 100), "mysql-bin.000009", 4),
    "rolled back": (ROLLED_BACK, "mysql-bin.000009", 4),
}


@pytest.mark.parametrize("case", list(_NEWER), ids=list(_NEWER))
def test_mysql_cdc_only_from_a_rerun_on_a_streamed_stack_needs_a_drop_and_reload_copy(
    monkeypatch, case
) -> None:
    # FAILS WITHOUT THE FIX (#0): "Delete CDC infrastructure and deploy it again, then set the
    # migration type to CDC only and Start CDC: the new stack streams from this watermark onto
    # the loaded target" -- after the default Append re-run, the rows the source updated or
    # deleted while CDC was stopped keep their old values, and CDC from that watermark never
    # re-delivers them.
    stack, file, pos = _NEWER[case]
    body = _notice(monkeypatch, job=_wm_job(file, pos), stack=stack)
    assert "needs a new stack" in body and "a copy made with Drop & reload" in body
    assert f"if this run appended, first {_DROP_RELOAD}" in body
    assert "an append keeps the rows already on the target as they were" in body
    # The Drop & reload copy comes before CDC from its watermark.
    assert _in_order(body, _DROP_RELOAD, _SWITCH_CDC_ONLY, "then Start CDC",
                     "the new stack streams from the Full Load's watermark")
    for phrase in _APPEND_IS_SAFE:
        assert phrase not in body, phrase


@pytest.mark.parametrize("case", list(_NEWER), ids=list(_NEWER))
def test_mysql_streamed_notice_switches_the_type_before_the_cdc_steps_delete(
    monkeypatch, case
) -> None:
    # FAILS WITHOUT THE FIX (#13): "Delete CDC infrastructure and deploy it again, then set the
    # migration type to CDC only" -- Full load only has no CDC step, so Delete is not on the
    # screen until the type is switched.
    stack, file, pos = _NEWER[case]
    body = _notice(monkeypatch, job=_wm_job(file, pos), stack=stack)
    assert _in_order(body, _SWITCH_CDC_ONLY, f"on its CDC step {_DELETE}", "then Start CDC")
    assert f"{_DELETE}, then set the migration type" not in body
    assert body.index(_SWITCH_CDC_ONLY) < body.index("Delete CDC infrastructure")


def test_mysql_never_streamed_notice_names_drop_and_reload_after_an_append(monkeypatch):
    # Rule E on the same seed (CDC only from this Full Load's watermark): a run that appended
    # to an earlier load's rows needs a Drop & reload re-run first. A first load is unchanged.
    for stack in (None, NEVER_STARTED):
        body = _notice(monkeypatch, job=_wm_job("mysql-bin.000009", 4), stack=stack)
        assert "streams from this Full Load's watermark onto the already-loaded target" in body
        assert "(no re-snapshot)" in body  # true on MySQL: the binlog retains it
        assert (f"If this run appended to rows an earlier load left on the target, first "
                f"{_DROP_RELOAD}") in body
        assert _in_order(body, _DROP_RELOAD, "Use the link below")
        assert "Delete CDC infrastructure" not in body


def test_mysql_a_start_point_not_newer_keeps_the_continuing_notice(monkeypatch) -> None:
    # Guard: a run that kept the original start point (Retry, Reload specific tables) lets
    # CDC only continue from where streaming stopped -- no Drop & reload, no new stack.
    body = _notice(monkeypatch, job=_wm_job("mysql-bin.000003", 100),
                   stack=_deployed("mysql-bin.000003", 100))
    assert "CDC continues from where streaming stopped" in body
    assert "Drop & reload" not in body and "Delete CDC infrastructure" not in body


@pytest.mark.parametrize("stack", [STOPPED, ROLLED_BACK], ids=["stopped", "rolled-back"])
def test_postgresql_notice_does_not_promise_no_re_snapshot(monkeypatch, stack) -> None:
    # FAILS WITHOUT THE FIX (#12): '"CDC only" continues from its replication slot (no
    # re-snapshot)' -- this run recorded no slot, so the Start sends snapshot.mode=initial,
    # which snapshots every selected table first when the stack's source connector never
    # completed its snapshot (a task that failed before then, or a rolled-back Start).
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route
    from dsql_migrator.core.cdc_postgres import pg_snapshot_mode

    job = _loaded()
    assert pg_snapshot_mode(job.watermark) == "initial"  # the premise: no slot recorded
    body = _notice(monkeypatch, job=job, stack=stack, pg=True)
    assert "no re-snapshot" not in body
    assert ("continues from that stack's replication slot if its source connector streamed "
            "(otherwise Debezium snapshots every selected table first)") in body
    # The retention bound and the route rule stay.
    assert "gapless only while the CDC topics still retain what the sink had not applied" in body
    route = pg_cdc_route(None, started=True, cdc_only=True)
    assert route in body
    # Rule F: the type switch first, then the CDC step's route.
    assert _in_order(body, _SWITCH_CDC_ONLY, "Start CDC on its CDC step", route)
    assert body.index(_SWITCH_CDC_ONLY) < body.index("Delete CDC infrastructure")


def test_postgresql_never_started_notice_is_unchanged(monkeypatch) -> None:
    # Guard: a stack never started keeps the re-snapshot notice (no slot, no CDC step yet).
    body = _notice(monkeypatch, job=_loaded(), stack=NEVER_STARTED, pg=True)
    assert body.startswith("This run created no replication slot")
    assert "Delete CDC infrastructure" not in body


# ---------------------------------------------------------------------------
# 2. Validation's Full load only cut-over step 2 (rule E)
# ---------------------------------------------------------------------------


def _cutover_steps(cdc_in_use: bool) -> dict:
    from dsql_migrator.ui.validation import _render_cutover_section

    ui = _CutoverUi()
    _render_cutover_section(ui, _cutover_summary(), _no_drift(), cdc_in_use=cdc_in_use)
    heading = "Steps to cut over (with CDC)" if cdc_in_use else "Steps to cut over (Full Load)"
    texts = ui.texts[ui.texts.index(heading) + 1:]
    steps: dict = {}
    for num, text in zip(texts[0::2], texts[1::2]):
        if not num.isdigit():
            break
        steps[num] = text
    return steps


def test_full_load_only_cut_over_takes_a_fresh_copy_with_drop_and_reload() -> None:
    # FAILS WITHOUT THE FIX (#1, #8): "re-run Full Load once more (it's idempotent -- only
    # fills the unfinished work, never duplicates)" -- the default Append applies no update or
    # delete made since the snapshot, and a row-count MATCH cannot see a stale value, so the
    # application went live on stale rows.
    steps = _cutover_steps(cdc_in_use=False)
    assert list(steps) == ["1", "2", "3", "4", "5"]
    step2 = steps["2"]
    assert step2.startswith("If the source took writes since the snapshot, " + _DROP_RELOAD)
    assert ("the default Append only adds missing rows and applies no update or delete made "
            "since") in step2
    assert "re-run validation here in CHECKSUM mode" in step2
    for phrase in _APPEND_IS_SAFE + ("once more",):
        assert phrase not in step2, phrase
    # The neighbours are unchanged.
    assert steps["1"].startswith("Freeze writes on the source")
    assert steps["3"].startswith("Confirm a clean MATCH")


def test_the_cut_over_step_matches_the_manual() -> None:
    # The docs twin (10-conclusion §10.3) already says so: one voice.
    from pathlib import Path

    manual = Path(__file__).resolve().parents[1] / "docs" / "manual" / "en" / "10-conclusion.md"
    text = " ".join(manual.read_text(encoding="utf-8").split())
    assert "the default *Append* only adds missing rows and applies no update or delete" in text
    step2 = _cutover_steps(cdc_in_use=False)["2"]
    assert "the default Append only adds missing rows and applies no update or delete" in step2


def test_the_cdc_cut_over_steps_are_unchanged() -> None:
    steps = _cutover_steps(cdc_in_use=True)
    assert list(steps) == ["1", "2", "3", "4", "5", "6"]
    assert "Drop & reload" not in " ".join(steps.values())


# ---------------------------------------------------------------------------
# 3. Validation's never-streamed recovery runbook, CDC types (rule E)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY, None], ids=["flc", "cdc_only", "unknown"])
def test_a_never_streamed_cdc_type_backfills_with_drop_and_reload(mtype) -> None:
    # FAILS WITHOUT THE FIX (#2, #7): "click Start/Re-run to backfill the missing rows (safe
    # and duplicate-free)", then "Start CDC: it starts from the new snapshot's start point" --
    # the re-run defaults to Append, so a row updated or deleted on the source before the new
    # start point keeps its old value, and CDC from that point never replays the change. The
    # same card renders right after the streamed runbook's own Delete-and-redeploy step.
    ui = _runbook(migration_type=mtype)  # cdc_stack_streamed=False, state read
    body = ui.body()
    assert "Re-run Full Load + CDC to backfill the gap" in body
    assert "so that re-run is a Drop & reload" in body
    joined = " ".join(_steps(ui))
    assert _in_order(joined, "click Re-run Full Load", "choose 'Drop & reload'",
                     "an append would keep rows older than it",
                     "Start CDC: it starts from the new Full Load's start point",
                     "Re-run validation")
    for phrase in _APPEND_IS_SAFE + ("Start/Re-run", "new snapshot's start point"):
        assert phrase not in body, phrase
    # A stack that never streamed cannot be running: still no Stop CDC, no redeploy.
    assert "Stop CDC" not in joined and "Delete CDC infrastructure" not in joined


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY], ids=["flc", "cdc_only"])
def test_the_never_streamed_re_run_step_is_the_streamed_branchs_own(mtype) -> None:
    # One click-path: after the streamed runbook deletes and redeploys, the never-streamed card
    # it re-renders as continues with the very same Full Load and Start CDC steps.
    streamed = _steps(_runbook(cdc_stack_streamed=True, migration_type=mtype))
    never = _steps(_runbook(migration_type=mtype))
    assert streamed[-3:] == never[-3:]
    assert len(streamed) == len(never) + 1  # only the Stop/Delete step differs


def test_the_screen_after_a_redeploy_names_drop_and_reload(monkeypatch) -> None:
    # The REAL validation screen as right after Delete CDC infrastructure and the new deploy:
    # the new stack has never streamed, so the never-streamed card renders.
    from tests.test_g25_fl_r7 import _validation_screen

    body = _validation_screen(monkeypatch, mtype=FLC, probe=NEVER_STARTED)
    assert "Steps to recover" in body
    assert "choose 'Drop & reload'" in body and "Start/Re-run" not in body
    assert _in_order(body, "choose 'Drop & reload'", "Continue to CDC and Start CDC")


def test_full_load_only_keeps_its_fill_the_gap_runbook() -> None:
    # Guard: Full load only has no CDC start point to protect; its backfill fills the missing
    # rows, and its cut-over step 2 (above) takes the fresh copy before cut over.
    ui = _runbook(migration_type=FLO)
    joined = " ".join(_steps(ui))
    assert "Start/Re-run to backfill the missing rows" in joined
    assert "Drop & reload" not in ui.body()


# ---------------------------------------------------------------------------
# 4. CDC only extra rows with no Full Load record
# ---------------------------------------------------------------------------


def _extra_rows(**kwargs) -> str:
    from dsql_migrator.ui.validation import _render_recovery_section

    ui = _CopyUi()
    _render_recovery_section(
        ui, _release_summary(is_match=False, mismatched=1, extra=1), _drift_na(), **kwargs
    )
    return ui.body()


@pytest.mark.parametrize("probe", ["streamed", "unread"])
def test_cdc_only_extra_rows_without_a_full_load_record_puts_new_topics_first(probe) -> None:
    # FAILS WITHOUT THE FIX (#5, #11): "reload that table on the Full Load step ('Reload
    # specific tables…', with CDC stopped) ... keeps the original start point. With CDC only,
    # switch the migration type to Full load + CDC first" -- without a Full Load record that
    # button is never offered, there is no original start point, and the only Full Load left
    # takes a new start point, refused on the streamed stack.
    flags = {"cdc_stack_streamed": True} if probe == "streamed" else {"cdc_state_unread": True}
    body = _extra_rows(migration_type=CDC_ONLY, full_load_record=False, **flags)
    assert "a delete never landed" in body  # the diagnosis is unchanged
    assert "Reload specific tables" not in body
    assert "keeps the original start point" not in body
    assert "this session holds no Full Load whose tables could be reloaded" in body
    assert _in_order(body, "Stop CDC if it is running", _DELETE,
                     "switch the migration type to Full load + CDC",
                     "run its Full Load choosing Drop & reload", "and Start CDC.")


def test_cdc_only_extra_rows_on_a_never_streamed_stack_skips_the_redeploy() -> None:
    body = _extra_rows(migration_type=CDC_ONLY, full_load_record=False)
    assert "Delete CDC infrastructure" not in body and "Reload specific tables" not in body
    assert _in_order(body, "switch the migration type to Full load + CDC",
                     "run its Full Load choosing Drop & reload", "and Start CDC.")


@pytest.mark.parametrize("mtype", [CDC_ONLY, FLC, FLO, None])
def test_a_session_with_a_full_load_record_keeps_the_scoped_reload(mtype) -> None:
    # Guard: with a record the scoped reload exists and keeps the original start point.
    body = _extra_rows(migration_type=mtype, cdc_stack_streamed=True)
    assert "'Reload specific tables…', with CDC stopped" in body
    assert "keeps the original start point" in body
    assert "this session holds no Full Load" not in body


def test_full_load_record_is_read_from_the_session_job(monkeypatch) -> None:
    from dsql_migrator.ui.validation import _has_full_load_record

    state = _state(mtype=CDC_ONLY, job_id=None)
    assert _has_full_load_record(state, _Jobs({})) is False
    state.job_id = "job-1"
    assert _has_full_load_record(state, _Jobs({})) is False  # record gone
    assert _has_full_load_record(state, _Jobs({"job-1": _loaded()})) is True

    class _Broken:
        def get_status(self, _job_id):
            raise RuntimeError("boom")

    assert _has_full_load_record(state, _Broken()) is True  # the usual remedy


def test_the_screen_threads_the_record_to_the_extra_row_remedy(monkeypatch) -> None:
    # The REAL validation screen: a CDC only migration whose target was loaded outside the
    # tool (no Full Load job), on a stack that has streamed, finds an extra row.
    from tests.test_ui_validation import _g1_screen, _ModeAwareValidator, _TimerUi

    ui = _TimerUi()
    s = _g1_screen(monkeypatch, _ModeAwareValidator(), ledger_tables=(), ui=ui)
    s.session.set_migration_type(CDC_ONLY)
    assert s.mstate.job_id is None
    _probe(monkeypatch, s.mstate, STOPPED)
    s.vstate.set_result(ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[
        TableValidationResult(
            table="orders", source_row_count=9, target_row_count=10, row_count_match=False,
            matched=False,
            reconcile=ReconcileResult(pk_column="id", source_count=9, target_count=10,
                                      missing_on_target=0, extra_on_target=1,
                                      consistent=False),
        ),
    ]))
    s.content(lambda: None)
    body = ui.body()
    assert "a delete never landed" in body
    assert "Reload specific tables" not in body
    assert _in_order(body, _DELETE, "switch the migration type to Full load + CDC",
                     "run its Full Load choosing Drop & reload")


# ---------------------------------------------------------------------------
# 5. The CDC-live Retry / Reload notice (rule D)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_live_retry_notice_states_the_cdc_topics_condition(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX (#6, #10): "this run keeps the original start point, so CDC
    # continues from where it stopped." -- Stop CDC removes the sink without a drain, so what it
    # had not applied reaches the target on the restart only from the CDC topics.
    state = _state(monkeypatch, NEVER_STARTED)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED, cdc_live=True)
    _click(ui, "Retry unfinished tables")
    body = _live_notice(ui)
    assert ("keeps the original start point, so CDC continues from where it stopped, gapless "
            "only while the CDC topics still retain what the sink had not applied (their "
            "retention period on MSK).") in body
    assert "Delete CDC infrastructure" not in body


def _copy_strings(module) -> list:
    """Every user-facing string literal of ``module`` (adjacent literals joined by the
    parser), docstrings excluded."""
    import ast

    tree = ast.parse(inspect.getsource(module))
    skip = {id(n.value) for n in ast.walk(tree)
            if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
    texts = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            skip.update(id(v) for v in node.values)
            texts.append("".join(v.value if isinstance(v, ast.Constant) else "{}"
                                 for v in node.values))
    texts += [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and id(n) not in skip]
    return texts


def test_every_keeps_the_start_point_continuity_claim_in_the_step_is_conditioned() -> None:
    # Rule D sweep over the Full Load step's own copy: each "continues from where ... stopped"
    # carries the CDC-topics condition in the same string.
    from dsql_migrator.ui.data_migration import _full_load_ui

    assert "gapless only while the CDC topics still retain what the sink had not applied" in (
        _full_load_ui._CDC_LIVE_KEEPS_START_POINT
    )
    claims = [t for t in _copy_strings(_full_load_ui)
              if "continues from where it stopped" in t
              or "continues from where streaming stopped" in t]
    assert len(claims) >= 2, claims  # the live notice and the Reload checklist hint
    for text in claims:
        assert "gapless only while the CDC topics" in text, text
