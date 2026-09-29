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
    dlq_exclusion_runbook,
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
    assert "Stop CDC alone is not enough" in lock.reason and "Start over" in lock.reason
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
    assert "change the excluded columns here, then deploy it again and Start CDC" in lock.reason
    assert "keep their values in newly excluded columns" in lock.reason


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
    assert lock.tier == 3 and "Start over" in lock.reason and "Exclude column & reload" in lock.reason
    pruned = exclusion_lock_state(state, _StubJobManager({}))
    assert pruned.tier == 3 and "no longer available" in pruned.reason
    assert exclusion_lock_state(_state(), _StubJobManager({})).locked is False


def test_no_state_offers_a_partial_or_unavailable_remedy():
    """Every reachable state's text: never Stop alone, a Manual start point, or P3's
    Change & reload (not in this version)."""
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
                        legacy = lob_exclusion_lock(state, _StubJobManager(jobs))[1]
                        if legacy:
                            reasons.append(legacy)
    assert reasons
    for text in reasons:
        assert "Stop CDC to change" not in text and "stop CDC first" not in text, text
        assert "Manual" not in text and "Change & reload" not in text, text


def test_the_dead_letter_runbook_follows_the_real_state():
    fl = _state(job_id="job-1")
    steps = dlq_exclusion_runbook(fl, _StubJobManager({"job-1": _StubJob("COMPLETED")}))
    assert steps[0] == "Exclude the column (this dialog)."
    assert "Drop & reload" in steps[1] and "empty (NULL) in every row" in steps[1]
    assert steps[-2:] == ["Deploy the CDC infrastructure again.", "Start CDC."]
    assert not any("Stop CDC" in s for s in steps)  # the dialog opens only with no stack
    pruned = dlq_exclusion_runbook(fl, _StubJobManager({}))
    assert "Start over" in pruned[1]
    cdc_only = dlq_exclusion_runbook(_state(MigrationType.CDC_ONLY), _StubJobManager({}))
    assert not any("Full Load step" in s for s in cdc_only)  # CDC only has none
    assert "current position" in cdc_only[-1]
    pg = dlq_exclusion_runbook(_state(MigrationType.CDC_ONLY, engine=SourceType.POSTGRES),
                               _StubJobManager({}))
    assert "snapshots the captured tables again" in pg[-1]


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
