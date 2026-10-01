# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The exclusion lock's strict order and TRUE remedies (P1 of the column-exclusion work).

0 being deleted > 1 being created > 2 streaming or a stack > 3 Full Load committed, and no
state offers "Stop CDC" as a complete remedy, a Manual start point, or an action this
version does not have.
"""

from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.data_migration._cdc_monitoring import (
    exclude_and_reload_block_reason,
    exclusion_lock_state,
    lob_exclusion_lock,
)

from tests.test_ui_data_migration import _StubJob, _StubJobManager


def _state(mtype=MigrationType.FULL_LOAD_AND_CDC, engine=SourceType.MYSQL, **kw):
    state = DataMigrationState()
    state.migration_type = mtype
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=engine))
    for key, value in kw.items():
        setattr(state, key, value)
    return state


def _deleting(state):
    state.set_cdc_deploy_job_id("del-1", kind="delete")
    return _StubJobManager({"del-1": _StubJob("RUNNING")})


def test_a_running_delete_is_a_wait_not_a_remedy_and_beats_every_other_tier():
    state = _state(job_id="job-1")
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    jm = _deleting(state)
    lock = exclusion_lock_state(state, jm)
    assert lock.tier == 0 and lock.tone == "info"
    assert "being deleted" in lock.reason and "not stuck" in lock.reason
    assert "~15-25 min" in lock.reason  # MySQL: the seeder Lambda's ENI wait
    # Committed or not, tier 0 is a wait: never Start over while the stack goes away.
    assert lock.reason.endswith("Wait for the deletion to finish before changing these exclusions.")
    assert "Start over" not in lock.reason
    # A restored session sees the delete only as DELETE_IN_PROGRESS.
    restored = _state(engine=SourceType.POSTGRES)
    restored.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
    lock = exclusion_lock_state(restored, _StubJobManager({}))
    assert lock.tier == 0 and "~2-5 min" in lock.reason  # PostgreSQL: no seeder
    # The legacy locks say the same wait instead of offering the running Delete.
    assert "being deleted" in lob_exclusion_lock(restored, _StubJobManager({}))[1]
    assert "being deleted" in exclude_and_reload_block_reason(restored, _StubJobManager({}))


def test_infra_being_created_is_tier_1():
    state = _state()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    lock = exclusion_lock_state(state, _StubJobManager({"infra-1": _StubJob("RUNNING")}))
    assert lock.tier == 1 and "being created" in lock.reason


def test_a_stack_with_a_committed_full_load_names_delete_then_start_over():
    state = _state(job_id="job-1")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    lock = exclusion_lock_state(state, _StubJobManager({"job-1": _StubJob("COMPLETED")}))
    assert lock.tier == 2
    assert "delete the CDC infrastructure on the CDC step" in lock.reason
    assert "Stop CDC alone is not enough" in lock.reason
    # P3-C18: with the load's record, the remedy is Change & reload (never Start over);
    # the MySQL line names the watermark re-seed.
    assert "2 use Change & reload for the affected tables" in lock.reason
    assert "Start over" not in lock.reason and "re-seeds from the Full Load watermark" in lock.reason
    pruned = exclusion_lock_state(state, _StubJobManager({}))
    assert pruned.cell == "R1b" and "Start over" in pruned.reason
    assert "Change & reload" not in pruned.reason and "watermark" not in pruned.reason
    # Streaming: Stop first, THEN delete -- never Stop as the whole remedy.
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    lock = exclusion_lock_state(state, _StubJobManager({"job-1": _StubJob("COMPLETED")}))
    assert "Stop CDC, then delete the CDC infrastructure" in lock.reason


@pytest.mark.parametrize(
    "engine, offset, expected",
    [
        (SourceType.MYSQL, True, "are NOT replicated"),
        (SourceType.MYSQL, False, "as the first Start would have"),
        (SourceType.POSTGRES, True, "snapshots the captured tables again"),
    ],
)
def test_cdc_only_without_a_full_load_states_the_engine_cost(engine, offset, expected):
    state = _state(MigrationType.CDC_ONLY, engine=engine)
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.set_cdc_has_committed_offset(offset)
    lock = exclusion_lock_state(state, _StubJobManager({}))
    assert lock.tier == 2 and expected in lock.reason
    step = ("change the column rules (changing the table set needs Start over)" if offset
            else "change the excluded columns here")
    assert f"{step}, then deploy it again and Start CDC" in lock.reason
    assert "keep their values in newly excluded columns" in lock.reason
    if engine is SourceType.MYSQL:
        assert "Start CDC on Automatic" in lock.reason


def test_a_full_load_type_without_a_load_needs_no_reload():
    state = _state()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    lock = exclusion_lock_state(state, _StubJobManager({}))
    assert lock.tier == 2 and "it can overlap the load" in lock.reason
    assert "nothing needs reloading" in lock.reason
    only = _state(MigrationType.FULL_LOAD_ONLY)
    only.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    lock = exclusion_lock_state(only, _StubJobManager({}))
    assert "switch the migration type to Full load + CDC" in lock.reason
    assert "switch back to Full load only" in lock.reason


def test_no_stack_and_a_committed_full_load_is_tier_3():
    state = _state(job_id="job-1")
    lock = exclusion_lock_state(state, _StubJobManager({"job-1": _StubJob("COMPLETED")}))
    assert lock.tier == 3 and "use Change & reload" in lock.reason
    assert "Changing the table set needs Start over." in lock.reason
    pruned = exclusion_lock_state(state, _StubJobManager({}))
    assert pruned.tier == 3 and "no longer available" in pruned.reason
    assert "Change & reload" not in pruned.reason and "Start over" in pruned.reason
    assert exclusion_lock_state(_state(), _StubJobManager({})).locked is False


def test_no_state_offers_a_partial_or_unavailable_remedy():
    """Every reachable state's text: never Stop alone or a Manual start point; Change &
    reload is named only when a Full Load committed and its record is found."""
    reasons = []
    for mtype in MigrationType:
        for engine in (SourceType.MYSQL, SourceType.POSTGRES):
            for phase in (None, "infra", "running", "unstable"):
                for job_id in (None, "job-1"):
                    state = _state(mtype, engine=engine, job_id=job_id)
                    if phase:
                        state.set_cdc_stack_phase(phase, status="UPDATE_COMPLETE")
                    for jobs in ({}, {"job-1": _StubJob("COMPLETED")}):
                        lock = exclusion_lock_state(state, _StubJobManager(jobs))
                        if lock.reason:
                            reasons.append(lock.reason)
                            found = job_id is not None and "job-1" in jobs
                            if "Change & reload" in lock.reason:
                                assert found, (mtype, phase, job_id, jobs)
                        legacy = lob_exclusion_lock(state, _StubJobManager(jobs))[1]
                        if legacy:
                            reasons.append(legacy)
    assert reasons
    for text in reasons:
        assert "Stop CDC to change" not in text and "stop CDC first" not in text, text
        assert "Manual" not in text, text


def test_the_dead_letter_route_shows_the_lock_states_own_steps():
    # P3-C20: the DLQ "Exclude column…" records a pending rule and opens the exclusion
    # popup read-only while a stack holds the set; its text IS exclusion_lock_state's.
    from tests.test_exclusion_dialog import _Ui, _ctx

    from dsql_migrator.ui.data_migration._exclusion_dialog import open_dlq_route

    state = _state(MigrationType.CDC_ONLY)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    state.set_cdc_has_committed_offset(True)
    ctx, _calls = _ctx(state, job_manager=_StubJobManager({}))
    ui = _Ui()
    open_dlq_route(ui, ctx, "shop.t", ["a"])
    lock = exclusion_lock_state(state, _StubJobManager({}))
    assert lock.cell == "R2"  # a streamed CDC-only session: never R1b
    assert lock.reason in ui.texts
    assert state.pending_rules() == ["exclude shop.t.a  # from the CDC dead-letter queue"]
    assert state.lob_exclusions() == {}

def test_a_23502_on_a_table_with_saved_exclusions_names_the_exclusion():
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_schema_drift_banner

    from tests.test_ui_data_migration import _RecordingUi

    view = SimpleNamespace(schema_drift=[SimpleNamespace(kind="drop-column", table="shop.docs", count=3)])
    ui = _RecordingUi()
    _render_cdc_schema_drift_banner(ui, view, saved_exclusions={"shop.docs": {"body"}})
    joined = " ".join(ui.texts)
    assert "saved column exclusions (body)" in joined and "not sent" in joined
    plain = _RecordingUi()
    _render_cdc_schema_drift_banner(plain, view, saved_exclusions={})
    assert "saved column exclusions" not in " ".join(plain.texts)


def test_the_table_picker_lock_says_wait_during_a_delete_and_delete_after_stop():
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = _state(MigrationType.CDC_ONLY)
    jm = _deleting(state)  # the delete job makes cdc_streaming_started True
    reason = selection_lock_reason(state, jm, status=StepStatus.NOT_STARTED,
                                   migration_type=MigrationType.CDC_ONLY, has_job=False)
    assert "being deleted" in reason and "stop CDC" not in reason
    live = _state(MigrationType.CDC_ONLY)
    live.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    reason = selection_lock_reason(live, _StubJobManager({}), status=StepStatus.NOT_STARTED,
                                   migration_type=MigrationType.CDC_ONLY, has_job=False)
    assert "stop CDC, then delete the CDC infrastructure on the CDC step" in reason


# --- P3-C12: structured cells, the Change & reload gate, the seeder answer ------------

from dsql_migrator.ui.data_migration._cdc_monitoring import (  # noqa: E402
    RECONNECT_REASON,
    change_and_reload_block_reason,
)


def _gate(state, jm, *, ready=True):
    return change_and_reload_block_reason(state, jm, connections_ready=ready)


@pytest.mark.parametrize("status", ["DONE", "FAILED", "CANCELLED", "COMPLETED"])
def test_change_and_reload_is_open_for_a_finished_job(status):
    state = _state(job_id="job-1")
    assert _gate(state, _StubJobManager({"job-1": _StubJob(status)})) is None
    lock = exclusion_lock_state(state, _StubJobManager({"job-1": _StubJob(status)}))
    assert (lock.cell, lock.offers_change_and_reload, lock.offers_change_type) == ("R4", True, False)


@pytest.mark.parametrize("status", ["PENDING", "RUNNING"])
def test_a_running_load_holds_change_and_reload(status):
    state = _state(job_id="job-1")
    assert _gate(state, _StubJobManager({"job-1": _StubJob(status)})).startswith(
        "A Full Load is running with the current exclusions.")


def test_no_job_record_gives_the_no_run_or_start_over_reason():
    from dsql_migrator.core.models import StepStatus  # noqa: F401 (the CDC-promoted step is not read)

    state = _state()
    assert _gate(state, _StubJobManager({})).startswith("No Full Load run is recorded")
    assert "Start over" not in _gate(state, _StubJobManager({}))
    gone = _state(job_id="job-1")
    assert _gate(gone, _StubJobManager({})).endswith("Use Start over to change exclusions.")
    assert exclusion_lock_state(gone, _StubJobManager({})).cell == "R4"
    assert not exclusion_lock_state(gone, _StubJobManager({})).offers_change_and_reload


def test_the_fk_pass_restore_and_reconnect_clauses():
    state = _state(job_id="job-1", fk_apply_job_id="fk-1", fk_apply_result=None)
    jm = _StubJobManager({"job-1": _StubJob("DONE"), "fk-1": _StubJob("RUNNING")})
    assert _gate(state, jm).startswith("Foreign keys are being applied")
    state.fk_apply_result = object()
    assert _gate(state, jm) is None
    state.cutover_restore_in_flight = lambda: True
    assert _gate(state, jm).startswith("Cut over is restoring")
    state.cutover_restore_in_flight = None
    assert _gate(state, jm) is None
    assert _gate(state, jm, ready=False) == RECONNECT_REASON


def test_tiers_0_to_2_answer_with_their_own_reason_and_cdc_only_with_a_type_switch():
    state = _state(job_id="job-1")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    jm = _StubJobManager({"job-1": _StubJob("DONE")})
    assert _gate(state, jm) == exclusion_lock_state(state, jm).reason
    cdc_only = _state(mtype=MigrationType.CDC_ONLY, job_id="job-1")
    reason = _gate(cdc_only, jm)
    assert "Full load + CDC" in reason and "Change & reload" in reason
    lock = exclusion_lock_state(cdc_only, jm)
    assert (lock.cell, lock.offers_change_and_reload, lock.offers_change_type) == ("R4", False, True)


def test_the_stack_cells():
    jm = _StubJobManager({"job-1": _StubJob("DONE")})
    for mtype, job_id, cell, change_type in [
        (MigrationType.FULL_LOAD_AND_CDC, "job-1", "R1", False),
        (MigrationType.FULL_LOAD_ONLY, "job-1", "R1", True),
        (MigrationType.CDC_ONLY, None, "R2", False),
        (MigrationType.FULL_LOAD_AND_CDC, None, "R3", False),
        (MigrationType.FULL_LOAD_ONLY, None, "R3", True),
    ]:
        state = _state(mtype=mtype, job_id=job_id)
        state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
        lock = exclusion_lock_state(state, jm)
        assert (lock.cell, lock.offers_change_type, lock.offers_change_and_reload) == (
            cell, change_type, False), mtype
    missing = _state(job_id="job-gone")
    missing.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert exclusion_lock_state(missing, jm).cell == "R1b"


def test_the_card_lock_is_the_lock_state_and_matches_the_old_predicate():
    from dsql_migrator.ui.data_migration import exclusion_card_locked

    cases = []
    for job_id in (None, "job-1"):
        for phase in (None, "absent", "infra", "running", "provisioning", "partial", "unstable"):
            for job_kind in (None, "infra", "delete", "start"):
                cases.append((job_id, phase, job_kind))
    for job_id, phase, job_kind in cases:
        state = _state(job_id=job_id)
        jobs = {"job-1": _StubJob("DONE")}
        if phase:
            state.set_cdc_stack_phase(phase, status="CREATE_COMPLETE")
        if job_kind:
            state.set_cdc_deploy_job_id("op-1", kind=job_kind)
            jobs["op-1"] = _StubJob("RUNNING")
        jm = _StubJobManager(jobs)
        old = lob_exclusion_lock(state, jm, full_load_committed=job_id is not None)[0]
        assert exclusion_card_locked(state, jm) is old, (job_id, phase, job_kind)


def test_seeder_answer_honours_the_restored_engine():
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_stack_has_seeder_lambda
    from dsql_migrator.ui.data_migration._models import session_has_seeder_lambda

    restored = SimpleNamespace(source_config=None, restored_source_type=SourceType.POSTGRES)
    live = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))
    assert session_has_seeder_lambda(restored) is False
    assert session_has_seeder_lambda(live) is True
    assert session_has_seeder_lambda(None) is None
    for session, expected in ((restored, False), (live, True), (None, None)):
        state = DataMigrationState()
        state._session = session
        assert _cdc_stack_has_seeder_lambda(state) is expected


# --- P3-C17: read-only popups, the tier-0 wait everywhere, R2/R3 wording ---------------


@pytest.mark.parametrize("engine", [SourceType.MYSQL, SourceType.POSTGRES])
@pytest.mark.parametrize("mtype", list(MigrationType))
@pytest.mark.parametrize("job_id", [None, "job-1"])
@pytest.mark.parametrize("restored", [False, True])
def test_no_lock_offers_delete_or_stop_while_a_delete_runs(engine, mtype, job_id, restored):
    from dsql_migrator.ui.data_migration._cdc_monitoring import cdc_delete_eta

    state = _state(mtype=mtype, engine=engine, job_id=job_id)
    jobs = {"job-1": _StubJob("DONE")}
    if restored:
        state.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
        jm = _StubJobManager(jobs)
    else:
        state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
        state.set_cdc_deploy_job_id("del-1", kind="delete")
        jobs["del-1"] = _StubJob("RUNNING")
        jm = _StubJobManager(jobs)
    lock = exclusion_lock_state(state, jm)
    assert lock.tier == 0
    assert lock.reason.endswith("Wait for the deletion to finish before changing these exclusions.")
    for banned in ("Stop CDC", "delete the CDC infrastructure", "Start over", "Change & reload"):
        assert banned not in lock.reason
    assert cdc_delete_eta(state)[0] in lock.reason
    assert change_and_reload_block_reason(state, jm, connections_ready=True) == lock.reason


@pytest.mark.parametrize("offset", [False, True])
@pytest.mark.parametrize("mtype", [MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC,
                                   MigrationType.FULL_LOAD_ONLY])
def test_r2_and_r3_name_no_reload_watermark_or_manual_start(mtype, offset):
    state = _state(mtype=mtype)
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.set_cdc_has_committed_offset(offset)
    lock = exclusion_lock_state(state, _StubJobManager({}))
    assert lock.cell == ("R2" if mtype is MigrationType.CDC_ONLY else "R3")
    for banned in ("Drop & reload", "Change & reload", "watermark", "Manual", "Fetch current position"):
        assert banned not in lock.reason, banned


def test_cdc_promotion_is_r2_never_r1b():
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration._cdc_state import cdc_pipeline_live
    from dsql_migrator.ui.data_migration._full_load_engine import data_migration_step_after_cdc

    state = _state(mtype=MigrationType.CDC_ONLY)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    state.set_cdc_has_committed_offset(True)
    step = data_migration_step_after_cdc(StepStatus.NOT_STARTED, cdc_streaming=cdc_pipeline_live(state))
    assert step is StepStatus.DONE and state.job_id is None
    lock = exclusion_lock_state(state, _StubJobManager({}))
    assert lock.cell == "R2" and "Start over" not in lock.reason.split("(changing")[0]
    assert "change the column rules (changing the table set needs Start over)" in lock.reason
