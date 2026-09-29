# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-09: what a stored column exclusion MEANS across a restore.

A session written before the NULL overlay (0.1.561) carries no semantics stamp; its
entries become legacy (omitted only, never ALTERed) exactly when rows already moved under
the old semantics, judged on evidence of writes. Every new session is stamped, so it can
never restore as legacy. Also pinned: the overlay-ledger in-use flag, the transient
forced-reload set, the builder-time handles across Start over, and the overlay / legacy
sets the engine and CDC paths consume.
"""

from datetime import datetime, timezone

import pytest

from dsql_migrator.core.column_exclusion import EXCLUSION_SEMANTICS_VERSION
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import ChunkState, ColumnDef, MigrationJob, SourceInventory, TableDef
from dsql_migrator.core.session_state_store import SessionSnapshot
from dsql_migrator.ui.data_migration import (
    DataMigrationState,
    classify_restored_exclusions,
    legacy_lob_exclusions,
    overlay_lob_exclusions,
)
from dsql_migrator.ui.data_migration._state import DataMigrationStore
from dsql_migrator.ui.evaluation import EvaluationState
from dsql_migrator.ui.schema_conversion import SchemaConversionState
from dsql_migrator.ui.session import SessionConnectionState
from dsql_migrator.ui.session_persistence import (
    apply_session_snapshot,
    capture_session_snapshot,
    session_signature,
)


class _Jobs:
    def __init__(self, jobs=None):
        self.jobs = jobs or {}

    def get_status(self, job_id):
        if job_id not in self.jobs:
            raise JobNotFoundError(job_id)
        return self.jobs[job_id]


def _job(*chunks):
    return MigrationJob(job_id="J1", chunks=[ChunkState(chunk_id="app.t", status=s, rows_loaded=r) for s, r in chunks])


def _capture(migration_state, session=None):
    return capture_session_snapshot(
        "s1", session or SessionConnectionState(), EvaluationState(), SchemaConversionState(), migration_state
    )


def _restore(snapshot, jobs=None):
    state = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(), SchemaConversionState(), state,
                           job_manager=jobs)
    return state


def _old(**kw):
    """A 0.1.561-shaped snapshot: exclusions, no stamp."""
    return SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c"], **kw)


# ---------------------------------------------------------------------------
# The stamp
# ---------------------------------------------------------------------------


def test_the_new_fields_are_additive():
    snap = SessionSnapshot(session_id="s")
    assert (snap.exclusion_semantics, snap.legacy_column_exclusions, snap.overlay_ledger_in_use) == (None, [], False)
    legacy_payload = SessionSnapshot(session_id="s", cdc_lob_exclusions=["a:b"]).model_dump()
    for key in ("exclusion_semantics", "legacy_column_exclusions", "overlay_ledger_in_use"):
        legacy_payload.pop(key)
    assert SessionSnapshot.model_validate(legacy_payload).cdc_lob_exclusions == ["a:b"]


def test_a_new_session_always_writes_the_stamp_and_never_restores_as_legacy():
    assert _capture(DataMigrationState()).exclusion_semantics == EXCLUSION_SEMANTICS_VERSION
    state = DataMigrationState()
    state.set_lob_exclusion("app.t", "c", True)
    state.job_id = "J1"
    snap = _capture(state)
    assert snap.exclusion_semantics == 1 and snap.legacy_column_exclusions == []
    assert snap.cdc_lob_exclusions == ["app.t:c"]
    restored = _restore(snap, _Jobs({"J1": _job(("DONE", 10))}))  # rows DID move -- under the overlay
    assert restored.legacy_exclusions() == {} and restored.lob_exclusions() == {"app.t": {"c"}}


def test_a_stamped_snapshot_restores_its_legacy_list_verbatim_and_never_reclassifies():
    snap = SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c", "app.t:d"], exclusion_semantics=1,
                           legacy_column_exclusions=["app.t:c"])
    restored = _restore(snap, _Jobs())  # "no evidence" would upgrade -- it must not be asked
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}
    assert not restored.exclusion_classification_pending()
    assert _capture(restored).legacy_column_exclusions == ["app.t:c"]


def test_a_future_stamp_is_read_conservatively_as_legacy():
    restored = _restore(SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c"], exclusion_semantics=9))
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}


# ---------------------------------------------------------------------------
# Evidence-based classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("snapshot_kw, jobs", [
    ({"migration_job_id": "J1"}, _Jobs({"J1": _job(("DONE", 5))})),
    ({"migration_job_id": "J1"}, _Jobs({"J1": _job(("FAILED", 0))})),       # a restart-reconciled IN_PROGRESS
    ({"migration_job_id": "J1"}, _Jobs({"J1": _job(("PENDING", 3))})),
    ({"migration_job_id": "J1"}, _Jobs()),                                  # the record is gone: not proof of nothing
    ({"cdc_connector_names": ["src", "sink"], "cdc_action_kind": "stop"}, _Jobs()),  # the teardown skip must not hide them
    ({"cdc_ops_window_start": datetime(2026, 9, 1, tzinfo=timezone.utc)}, _Jobs()),
])
def test_an_unstamped_snapshot_with_evidence_of_writes_is_legacy(snapshot_kw, jobs):
    restored = _restore(_old(**snapshot_kw), jobs)
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}
    assert not restored.exclusion_classification_pending()
    snap = _capture(restored)
    assert snap.exclusion_semantics == 1 and snap.legacy_column_exclusions == ["app.t:c"]
    assert snap.cdc_lob_exclusions == ["app.t:c"]  # the stored set is byte-identical


def test_only_infra_inputs_upgrade_a_full_load_only_session_at_restore():
    restored = _restore(_old(migration_type="full_load_only", migration_job_id=None,
                             cdc_infra_inputs={"VpcId": "vpc-1"}), _Jobs())
    assert restored.legacy_exclusions() == {} and not restored.exclusion_classification_pending()
    # Upgraded entries are overlay entries; the first run that overlays them marks the
    # ledger in use (nothing was recorded yet).
    assert restored.overlay_ledger_in_use is False


def test_a_possible_cdc_stack_waits_for_the_probe_and_pending_behaves_as_legacy():
    restored = _restore(_old(migration_type="cdc_only"), _Jobs())
    assert restored.exclusion_classification_pending()
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}  # pending = legacy
    snap = _capture(restored)
    assert snap.exclusion_semantics is None and snap.legacy_column_exclusions == []  # not frozen yet
    # Unstable or not yet probed: still pending.
    restored.set_cdc_stack_phase("unstable")
    assert classify_restored_exclusions(restored) is None
    # Infra only, no committed offset: nothing was written -> upgrade.
    restored.set_cdc_stack_phase("infra")
    assert classify_restored_exclusions(restored) is False
    assert restored.legacy_exclusions() == {}


@pytest.mark.parametrize("evidence", ["committed", "running", "cdc_started", "new_job"])
def test_stage_two_evidence_makes_a_pending_session_legacy(evidence):
    restored = _restore(_old(migration_type="full_load_and_cdc"), _Jobs())
    assert restored.exclusion_classification_pending()
    if evidence == "committed":
        restored.set_cdc_has_committed_offset(True)
        restored.set_cdc_stack_phase("infra")
    elif evidence == "running":
        restored.set_cdc_stack_phase("running")
    elif evidence == "cdc_started":
        restored.set_cdc_ops_window_start(datetime(2026, 9, 1, tzinfo=timezone.utc))
        restored.set_cdc_stack_phase("infra")  # the race: a start while pending, then "infra"
    else:
        restored.job_id = "J-new"  # a run started while pending wrote under the old semantics
        restored.set_cdc_stack_phase("absent")
    assert classify_restored_exclusions(restored) is True
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}


def test_stage_two_never_runs_before_the_job_evidence_is_judged():
    # Restored without a job manager (a harness): a probe saying "absent" must not upgrade
    # a session whose job may hold rows.
    restored = _restore(_old(migration_type="full_load_and_cdc", migration_job_id="J1"), None)
    restored.set_cdc_stack_phase("absent")
    assert classify_restored_exclusions(restored) is None and restored.exclusion_classification_pending()
    assert classify_restored_exclusions(restored, _Jobs({"J1": _job(("DONE", 1))})) is True


def test_the_cdc_probe_settles_a_pending_classification(monkeypatch):
    from dsql_migrator.ui.data_migration import _cdc_status

    restored = _restore(_old(migration_type="cdc_only"), _Jobs())
    monkeypatch.setattr(_cdc_status, "_discover_cdc_controller",
                        lambda state, session: state.set_cdc_stack_phase("infra"))
    _cdc_status._ensure_cdc_controller(restored, SessionConnectionState())
    assert not restored.exclusion_classification_pending() and restored.legacy_exclusions() == {}
    # A probe that raises leaves it pending and the classification never raises out.
    pending = _restore(_old(migration_type="cdc_only"), _Jobs())

    def boom(state, session):
        raise RuntimeError("describe failed")

    monkeypatch.setattr(_cdc_status, "_discover_cdc_controller", boom)
    with pytest.raises(RuntimeError):
        _cdc_status._ensure_cdc_controller(pending, SessionConnectionState())
    assert pending.exclusion_classification_pending()


# ---------------------------------------------------------------------------
# State: the in-use flag, operator edits, the forced set, Start over
# ---------------------------------------------------------------------------


def test_the_in_use_flag_is_set_by_a_run_that_overlays_not_by_a_tick():
    import dsql_migrator.ui.data_migration as dm

    inventory = _inventory()
    state = DataMigrationState()
    state.restore_lob_exclusion("app.t", "c")
    assert state.overlay_ledger_in_use is False  # a restore is not a use
    state.set_lob_exclusion("app.t", "d", True)
    state.set_lob_exclusion("app.t", "d", False)
    assert state.overlay_ledger_in_use is False  # a tick and an untick: nothing recorded
    state.set_lob_exclusion("app.t", "d", True)
    fields = dm._overlay_inputs(state, inventory, None, True)  # a run that overlays d is built...
    assert state.overlay_ledger_in_use is False
    dm._mark_overlay_use(state, fields["overlay_columns"])  # ...and submitted
    assert state.overlay_ledger_in_use
    state.set_lob_exclusion("app.t", "d", False)
    state.set_lob_exclusion("app.t", "c", False)
    assert state.overlay_ledger_in_use  # that run may have left a record
    snap = _capture(state)
    assert snap.overlay_ledger_in_use and _restore(snap).overlay_ledger_in_use
    store = DataMigrationStore()
    live = store.get_or_create("s1")
    live.mark_overlay_ledger_in_use()
    store.reset_in_place("s1")
    assert live.overlay_ledger_in_use is False


def test_unticking_a_legacy_entry_drops_its_mark_and_a_retick_makes_it_an_overlay_entry():
    restored = _restore(_old(migration_job_id="J1"), _Jobs({"J1": _job(("DONE", 1))}))
    restored.set_lob_exclusion("app.t", "c", False)
    assert restored.legacy_exclusions() == {} and restored.lob_exclusions() == {}
    restored.set_lob_exclusion("app.t", "c", True)
    assert restored.legacy_exclusions() == {} and restored.lob_exclusions() == {"app.t": {"c"}}


def test_the_signature_moves_on_finalization_legacy_and_the_flag():
    parts = (SessionConnectionState(), EvaluationState(), SchemaConversionState())
    restored = _restore(_old(migration_type="cdc_only"), _Jobs())
    before = session_signature(parts[0], parts[1], parts[2], restored)
    restored.set_cdc_stack_phase("infra")
    classify_restored_exclusions(restored)
    assert session_signature(parts[0], parts[1], parts[2], restored) != before


def test_the_forced_set_joins_the_replace_set_and_is_consumed_alone():
    state = DataMigrationState()
    state.set_tables_with_data(frozenset({"a"}))
    state.set_exclusion_forced_replace({"b": "needs Drop & reload"})
    assert state.replace_targets == frozenset({"b"})  # append mode: only the forced one
    state.set_reload_mode("drop")
    assert state.replace_targets == frozenset({"a", "b"})
    # Unconfirmed (a dialog lost to a reload or disconnect): the forced set is dropped.
    assert state.take_replace_targets() == frozenset({"a"})
    assert state.exclusion_forced_replace == {}
    state.set_exclusion_forced_replace({"b": "needs Drop & reload"})
    state.confirm_exclusion_forced_replace()
    assert state.take_replace_targets() == frozenset({"a", "b"})
    assert state.exclusion_forced_replace == {} and state.tables_with_data == frozenset({"a"})
    assert state.reload_mode == "drop" and state.replace_targets == frozenset({"a"})
    state.set_exclusion_forced_replace({"forced_tbl_zz": "r"})
    assert "forced_tbl_zz" not in _capture(state).model_dump_json()  # never persisted
    state.clear_exclusion_forced_replace()
    assert state.replace_targets == frozenset({"a"})


def test_start_over_clears_the_forced_set_and_keeps_the_builder_time_handles():
    store = DataMigrationStore()
    live = store.get_or_create("s1")
    handles = {"overlay_ledger": object(), "effective_target_ddls": lambda: {},
               "on_cdc_start": lambda: None, "cutover_restore_in_flight": lambda: False}
    for name, value in handles.items():
        setattr(live, name, value)
    live.set_exclusion_forced_replace({"b": "r"})
    store.reset_in_place("s1")
    assert live.exclusion_forced_replace == {}
    assert all(getattr(live, name) is value for name, value in handles.items())


# ---------------------------------------------------------------------------
# The sets the engine and CDC paths consume
# ---------------------------------------------------------------------------


def _inventory():
    return SourceInventory(tables=[TableDef(
        name="app.t", primary_key=["id"],
        columns=[ColumnDef(name=c, mysql_type="text") for c in ("id", "c", "d", "g")] + [
            ColumnDef(name="doubled", mysql_type="integer", generated=True, generated_kind="STORED",
                      generated_expression="(id * 2)")])])


def test_overlay_and_legacy_sets_split_the_effective_set():
    state = _restore(SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c", "app.t:d", "app.t:id", "app.t:doubled"],
                                     exclusion_semantics=1, legacy_column_exclusions=["app.t:c"]))
    inventory = _inventory()
    assert overlay_lob_exclusions(state, inventory) == {"app.t": frozenset({"d"})}  # minus legacy, key, computed
    assert legacy_lob_exclusions(state, inventory) == {"app.t": frozenset({"c"})}
    pending = _restore(SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:d"], migration_type="cdc_only"), _Jobs())
    assert overlay_lob_exclusions(pending, inventory) == {}  # pending entries are never overlaid
    assert legacy_lob_exclusions(pending, inventory) == {"app.t": frozenset({"d"})}


def test_the_capture_list_is_byte_identical_whatever_the_classification():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    inventory = _inventory()
    values = []
    for snap in (SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c"], exclusion_semantics=1,
                                 legacy_column_exclusions=["app.t:c"]),
                 SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c"], migration_type="cdc_only"),
                 SessionSnapshot(session_id="s1", cdc_lob_exclusions=["app.t:c"], exclusion_semantics=1)):
        capture = cdc_capture_exclusions(_restore(snap, _Jobs()), inventory, inventory.tables)
        values.append((capture.value, capture.sent, tuple(sorted(capture.structured.items()))))
    assert values[0] == values[1] == values[2]



def test_a_pending_session_saved_again_keeps_only_its_restored_candidates():
    # Review fix: a re-save while pending must not turn the operator's NEW ticks into
    # legacy candidates on the next restore.
    restored = _restore(_old(migration_type="full_load_and_cdc"), _Jobs())
    assert restored.exclusion_classification_pending()
    restored.set_lob_exclusion("app.t", "d", True)
    snap = SessionSnapshot.model_validate_json(_capture(restored).model_dump_json())
    assert snap.exclusion_semantics is None and snap.pending_legacy_candidates == ["app.t:c"]
    again = _restore(snap, _Jobs())
    assert again.legacy_exclusions() == {"app.t": frozenset({"c"})}  # d stays an overlay entry
    again.set_cdc_stack_phase("running")
    assert classify_restored_exclusions(again) is True
    assert again.final_legacy_exclusions() == {"app.t": frozenset({"c"})}


def test_an_earlier_run_of_the_lineage_is_evidence_even_when_the_newest_job_is_all_pending():
    j1 = MigrationJob(job_id="J1", chunks=[ChunkState(chunk_id="app.t", status="DONE", rows_loaded=100)])
    j2 = MigrationJob(job_id="J2", chunks=[ChunkState(chunk_id="app.t", status="PENDING")])

    class _Listing(_Jobs):
        def list_jobs(self):
            return list(self.jobs.values())

    restored = _restore(_old(migration_type="full_load_only", migration_job_id="J2"), _Listing({"J1": j1, "J2": j2}))
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}
    # A retry-seeded chunk (PENDING, rows 0, attempts kept) is evidence on its own.
    j3 = MigrationJob(job_id="J3", chunks=[ChunkState(chunk_id="app.t", status="PENDING", attempts=1)])
    restored = _restore(_old(migration_type="full_load_only", migration_job_id="J3"), _Jobs({"J3": j3}))
    assert restored.legacy_exclusions() == {"app.t": frozenset({"c"})}
