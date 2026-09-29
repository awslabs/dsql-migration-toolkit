# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pins the fixes from the adversarial review of P1 chunks 2-6 (column exclusion): the
kept-legacy composite gate, capture-size remedies, the job_id lock, the live exclusion
inputs, the 42703 gate's live target, both Attach surfaces, the dead-letter recovery after
a Delete, and the lock copy for Stop / delete / create in flight."""

from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import (
    CdcStackParams,
    legacy_column_exclude_key_conflicts,
)
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.data_migration._cdc_monitoring import (
    exclusion_after_lock_note,
    exclusion_lock_state,
    lob_exclusion_lock,
)

from tests.test_ui_data_migration import (
    _RecordingUi,
    _StubJob,
    _StubJobManager,
)


def _t(name, cols, pk=("id",)):
    return TableDef(name=name, columns=[ColumnDef(name=c, mysql_type="longtext" if c != "id" else "int")
                                        for c in cols], primary_key=list(pk))


def _state(mtype=MigrationType.FULL_LOAD_AND_CDC, entries=(), **kw):
    state = DataMigrationState()
    state.migration_type = mtype
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))
    for table, col in entries:
        state.set_lob_exclusion(table, col, True)
    for key, value in kw.items():
        setattr(state, key, value)
    return state


# ---------------------------------------------------------------------------
# A kept 0.1.561 value that excludes a composite key column: refused, like 0.1.561
# ---------------------------------------------------------------------------


def _params(value, legacy, mkc="shop\\.docs:tenant,id"):
    return CdcStackParams(
        filled=[("ColumnExcludeList", value), ("MessageKeyColumns", mkc), ("TableIncludeList", "shop.docs")],
        placeholders=[], stack_name="s", topic_prefix="p", column_exclude_legacy=legacy)


def test_the_deployer_refuses_a_kept_legacy_value_that_excludes_a_key_column():
    legacy = "shop.docs.body,shop.docs.tenant"
    assert legacy_column_exclude_key_conflicts(_params(legacy, legacy)) == ["shop.docs.tenant"]
    # The exact form was chosen (a new stack): no conflict possible.
    assert legacy_column_exclude_key_conflicts(_params("shop.docs.body", legacy)) == []
    assert legacy_column_exclude_key_conflicts(_params(legacy, None)) == []

    from tests.test_cdc_deploy_stages import SINK, SRC, _FakeDeployer, _FakeHandle, run_cdc_start
    from dsql_migrator.core.cdc_deployer import CdcDeployError

    deployer = _FakeDeployer(connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
                             discovery_params={"ColumnExcludeList": legacy})
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(_FakeHandle(), stack_name="mysql-dsql-cdc-stack",
                      params=_params("shop.docs.body", legacy), deployer=deployer,
                      on_log=lambda *_a: None, sleep=lambda _s: None,
                      connector_timeout_seconds=1.0, poll_interval_seconds=0.0)
    assert "shop.docs.tenant" in str(exc.value) and "Delete the CDC infrastructure" in str(exc.value)
    assert deployer.updates == []


def test_start_refuses_a_kept_legacy_value_that_excludes_a_key_column(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_ui as cdcui
    from dsql_migrator.core.table_selection import TableSelection

    inv = SourceInventory(tables=[TableDef(
        name="shop.docs", primary_key=["id"],
        columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name="tenant", mysql_type="varchar(64)"),
                 ColumnDef(name="body", mysql_type="longtext")])])
    state = _state(entries=[("shop.docs", "tenant"), ("shop.docs", "body")])
    state.set_selection(TableSelection(selected_tables=["shop.docs"]))
    state.set_cdc_message_key_columns({"shop.docs": ["tenant", "id"]})
    state.set_cdc_deployed_column_exclude_list("shop.docs.body,shop.docs.tenant")  # 0.1.561
    notices: list = []
    monkeypatch.setattr(cdcui, "render_notice", lambda ui, **kw: notices.append(kw))
    submitted: list = []
    jm = SimpleNamespace(get_status=lambda _j: (_ for _ in ()).throw(KeyError(_j)),
                         submit=lambda w: submitted.append(w) or "job")
    cdcui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), state, jm, lambda: None,
                            inventory=inv, session=SimpleNamespace(source_config=None, target_config=None,
                                                                   aws_profile=None, restored_source_type=None,
                                                                   source_password=None))
    (hit,) = [n for n in notices if n.get("header") == "The deployed capture list excludes a composite key column"]
    assert "shop.docs.tenant" in hit["body"] and "a new stack gets a capture list without key columns" in hit["body"]
    assert not submitted


# ---------------------------------------------------------------------------
# Capture size: held before the load for Full load + CDC; true remedy once locked
# ---------------------------------------------------------------------------


def _wide(n=150):
    tables = [_t(f"shop.table_{i:04d}", ["id", f"payload_document_body_{i:04d}"]) for i in range(n)]
    return SourceInventory(tables=tables), [(t.name, t.columns[1].name) for t in tables]


def test_a_capture_list_over_4_kib_holds_the_load_while_it_can_still_be_fixed():
    from dsql_migrator.ui.data_migration import data_migration_exclusion_block

    inv, entries = _wide()
    state = _state(entries=entries)
    block = data_migration_exclusion_block(state, _StubJobManager({}), None, inv)
    assert block and "4,096-byte" in block and "Exclude fewer columns" in block
    # Full load only captures nothing: no such hold.
    only = _state(MigrationType.FULL_LOAD_ONLY, entries=entries)
    assert data_migration_exclusion_block(only, _StubJobManager({}), None, inv) is None


def test_once_locked_the_capture_refusal_names_the_lock_remedy_not_exclude_fewer():
    from dsql_migrator.ui.data_migration._cdc_ui import _stored_exclusion_deploy_block

    inv, entries = _wide()
    state = _state(entries=entries, job_id="job-1")
    block = _stored_exclusion_deploy_block(state, _StubJobManager({"job-1": _StubJob("COMPLETED")}), inv, None)
    assert "4,096-byte" in block and "Start over" in block and "Exclude fewer" not in block
    # Attach does not judge capture size (the adopted stack's value decides at Start).
    assert _stored_exclusion_deploy_block(
        state, _StubJobManager({}), inv, None, include_capture=False) is None


# ---------------------------------------------------------------------------
# The lock keys on job_id; a CDC-only session that streamed unlocks after the delete
# ---------------------------------------------------------------------------


def test_a_cdc_only_session_that_streamed_can_change_its_columns_after_the_delete():
    from dsql_migrator.ui.data_migration import exclusion_card_locked

    state = _state(MigrationType.CDC_ONLY)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    assert exclusion_card_locked(state, _StubJobManager({}))
    assert "change the excluded columns here" in exclusion_lock_state(state, _StubJobManager({})).reason
    # Stop + Delete: the stack is gone; the FULL_LOAD step CDC promoted to DONE is not a load.
    state.set_cdc_stack_phase("absent", status=None)
    assert exclusion_card_locked(state, _StubJobManager({})) is False
    assert lob_exclusion_lock(state, _StubJobManager({}), full_load_committed=False) == (False, None)


def test_the_screen_and_the_cdc_card_use_job_id_not_the_step():
    import inspect

    import dsql_migrator.ui.data_migration as dm
    import dsql_migrator.ui.data_migration._cdc_ui as cdcui

    screen = " ".join(inspect.getsource(dm.build_data_migration_screen).split())
    assert "full_load_committed=getattr(migration_state, \"job_id\", None) is not None" in screen
    assert "status is StepStatus.DONE ) _lob" not in screen
    step = " ".join(inspect.getsource(cdcui._render_cdc_step).split())
    assert "full_load_committed = getattr(migration_state, \"job_id\", None) is not None" in step


# ---------------------------------------------------------------------------
# Live exclusion inputs: a tick / an edit reaches the gates with no re-render
# ---------------------------------------------------------------------------


def test_the_live_inputs_see_a_tick_and_an_edit_and_convert_only_on_change(monkeypatch):
    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.core.table_selection import TableSelection

    calls: list = []
    real = dm.SchemaConverter

    class _Counting(real):  # type: ignore[misc,valid-type]
        def convert(self, inventory, options=None):
            calls.append(1)
            return super().convert(inventory, options)

    monkeypatch.setattr(dm, "SchemaConverter", _Counting)
    monkeypatch.setattr(dm, "migratable_table_names", lambda inv_, *_a: [t.name for t in inv_.tables])
    monkeypatch.setattr(dm, "default_migration_selection", lambda *_a: [])
    inv = SourceInventory(tables=[_t("shop.a", ["id", "x"]), _t("shop.b", ["id", "y"])])
    conv_state = SimpleNamespace(generated_node_ids=None, ticked_node_ids=None,
                                 edited_target_ddls={}, preserve_foreign_keys=True)
    state = _state()
    state.set_exclusion_inputs_resolver(dm.make_exclusion_inputs_resolver(
        state, inventory_fn=lambda: inv, target_inventory_fn=lambda: None, conv_state=conv_state,
        session=SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))))
    state.set_selection(TableSelection(selected_tables=["shop.a"]))
    assert state.resolved_selection() == ["shop.a"]
    state.set_selection(TableSelection(selected_tables=["shop.a", "shop.b"]))  # a tick, no render
    assert state.resolved_selection() == ["shop.a", "shop.b"]
    for _ in range(5):
        state.applied_conversions()
    assert len(calls) == 1  # memoized across calls
    conv_state.edited_target_ddls = {"shop.a": 'CREATE TABLE "shop"."a" ("id" INT PRIMARY KEY)'}
    applied, _ = state.applied_conversions()  # an edit made elsewhere, no render in between
    assert len(calls) == 2 and applied["shop.a"].target_ddl.endswith("PRIMARY KEY)")


def test_the_screen_registers_and_seeds_the_live_inputs():
    import ast
    import inspect

    import dsql_migrator.ui.data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    tree = ast.parse(src)
    calls = {getattr(n.func, "attr", getattr(n.func, "id", None)) for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert {"make_exclusion_inputs_resolver", "set_exclusion_inputs_resolver", "seed",
            "resolve_migration_selection"} <= calls


# ---------------------------------------------------------------------------
# 42703: only a CERTAIN failure -- the live table still having the column lets it through
# ---------------------------------------------------------------------------


def test_an_unapplied_edit_does_not_block_while_the_live_table_has_the_column():
    from dsql_migrator.ui.data_migration._models import ExclusionInputs, missing_column_block_reason

    inv = SourceInventory(tables=[_t("shop.t", ["id", "c"])])
    edited = {"shop.t": SimpleNamespace(target_ddl='CREATE TABLE "shop"."t" ("id" INT PRIMARY KEY)',
                                        foreign_key_ddls=[], preserved_foreign_keys=[])}
    for live, blocked in (({"shop.t": frozenset({"id", "c"})}, False),
                          ({"shop.t": frozenset({"id"})}, True), (None, True)):
        state = _state()
        state.set_exclusion_inputs_resolver(
            lambda live=live: ExclusionInputs(["shop.t"], edited, True, live))
        got = missing_column_block_reason(state, inv)
        assert bool(got) is blocked, live


# ---------------------------------------------------------------------------
# Both Attach surfaces are gated; the dead-letter recovery is reachable after a Delete
# ---------------------------------------------------------------------------


def test_the_plan_level_attach_banner_is_gated_too():
    from dsql_migrator.ui.data_migration import _render_cdc_existing_infra_banner

    inv = SourceInventory(tables=[_t("shop.docs", ["id", "body"])])
    state = _state(entries=[("shop.docs", "id")])
    state.set_cdc_other_stacks([("dsql-cdc-old", "CREATE_COMPLETE")])
    ui = _RecordingUi()
    _render_cdc_existing_infra_banner(ui, state, lambda: None, job_manager=_StubJobManager({}),
                                      inventory=inv, session=None)
    (attach,) = [b for b in ui.buttons if b.text == "Attach to dsql-cdc-old"]
    assert "disable" in attach.props_seen
    attach.on_click()
    assert state.cdc_stack_name != "dsql-cdc-old"


def test_the_dead_letter_recovery_is_offered_after_stop_and_works_after_delete(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_status as status_mod
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_CODE
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _render_cdc_oversized_recovery_without_monitoring,
    )

    record = SimpleNamespace(error_code=OVERSIZED_VALUE_CODE, table="shop.docs", pk="7",
                             message="value too large for column body")
    monkeypatch.setattr(status_mod, "cdc_dlq_records", lambda *_a: [record])

    def _render(state):
        ui = _RecordingUi()
        _render_cdc_oversized_recovery_without_monitoring(
            ui, state, _StubJobManager({}),
            lob_candidates_for=lambda _t: (("body", "longtext"),),
            exclude_columns=lambda *_a: None)
        return ui

    stopped = _state(MigrationType.CDC_ONLY)
    stopped.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")  # Stop keeps the stack
    ui = _render(stopped)
    (button,) = [b for b in ui.buttons if b.text == "Exclude column…"]
    assert "disable" in " ".join(button.props_seen)
    deleted = _state(MigrationType.CDC_ONLY)
    deleted.set_cdc_stack_phase("absent", status=None)
    (button,) = [b for b in _render(deleted).buttons if b.text == "Exclude column…"]
    assert "disable" not in " ".join(button.props_seen)
    # While CDC is live the panel inside monitoring owns it: nothing here.
    live = _state(MigrationType.CDC_ONLY)
    live.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    assert not [b for b in _render(live).buttons if b.text == "Exclude column…"]


# ---------------------------------------------------------------------------
# Lock copy: committed Full Load in tiers 0/1, Stop in flight, pruned record, cause notes
# ---------------------------------------------------------------------------


def test_delete_or_create_in_flight_after_a_committed_load_names_start_over():
    state = _state(job_id="job-1")
    state.set_cdc_deploy_job_id("del", kind="delete")
    lock = exclusion_lock_state(state, _StubJobManager({"del": _StubJob("RUNNING")}))
    assert lock.tier == 0 and "use Start over" in lock.reason
    state.set_cdc_deploy_job_id("inf", kind="infra")
    lock = exclusion_lock_state(state, _StubJobManager({"inf": _StubJob("RUNNING")}))
    assert lock.tier == 1 and "then use Start over" in lock.reason


def test_a_stop_in_flight_is_named_as_a_wait():
    state = _state(MigrationType.CDC_ONLY)
    state.set_cdc_deploy_job_id("stop", kind="stop")
    jm = _StubJobManager({"stop": _StubJob("RUNNING")})
    lock = exclusion_lock_state(state, jm)
    assert "CDC is being stopped" in lock.reason and "wait for the Stop CDC that is running" in lock.reason
    assert "wait for the Stop CDC that is running" in lob_exclusion_lock(state, jm)[1]


def test_the_reload_hint_needs_the_job_record():
    state = _state(job_id="job-1")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert "Exclude column & reload" in exclusion_lock_state(
        state, _StubJobManager({"job-1": _StubJob("COMPLETED")})).reason
    assert "Exclude column & reload" not in exclusion_lock_state(state, _StubJobManager({})).reason


def test_the_after_lock_note_matches_the_cause():
    jm = _StubJobManager({})
    assert "already moved" in exclusion_after_lock_note(_state(job_id="job-1"), jm)
    deleting = _state()
    deleting.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
    assert "being deleted" in exclusion_after_lock_note(deleting, jm)
    creating = _state()
    creating.set_cdc_deploy_job_id("inf", kind="infra")
    assert "being created" in exclusion_after_lock_note(creating, _StubJobManager({"inf": _StubJob("RUNNING")}))
    deployed = _state()
    deployed.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert "delete the CDC infrastructure on the CDC step first" in exclusion_after_lock_note(deployed, jm)
