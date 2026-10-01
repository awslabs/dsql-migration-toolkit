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

    from dsql_migrator.core.table_selection import TableSelection

    inv, entries = _wide()
    state = _state(entries=entries, job_id="job-1")
    state.set_selection(TableSelection(selected_tables=[t.name for t in inv.tables]))
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
    # It streamed, so CDC promoted the FULL_LOAD step: the table set stays locked after the
    # Delete, and only the column rules can change (P3-C17 wording).
    assert "change the column rules (changing the table set needs Start over)" in (
        exclusion_lock_state(state, _StubJobManager({})).reason)
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
    # P3-C20: enabled in every state -- it records a pending rule and opens the popup
    # read-only with the lock's own steps while the stack holds the set.
    assert "disable" not in " ".join(button.props_seen)
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
    # P3-C17: tier 0 is a wait whether or not a load committed (never Start over while the
    # stack goes away); tier 1 keeps the committed Start-over remedy.
    assert lock.tier == 0 and "Start over" not in lock.reason
    assert lock.reason.endswith("Wait for the deletion to finish before changing these exclusions.")
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
    # P3-C18: the remedy that reloads is Change & reload, named only with the job record.
    assert "Change & reload" in exclusion_lock_state(
        state, _StubJobManager({"job-1": _StubJob("COMPLETED")})).reason
    assert "Change & reload" not in exclusion_lock_state(state, _StubJobManager({})).reason


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


# ---------------------------------------------------------------------------
# Round 3 (verification of the review fixes)
# ---------------------------------------------------------------------------


def _pg_gen_tables(n=100):
    return SourceInventory(tables=[TableDef(
        name=f"public.customer_order_line_{i:04d}", primary_key=["id"],
        columns=[ColumnDef(name="id", mysql_type="integer"), ColumnDef(name="amount", mysql_type="numeric"),
                 ColumnDef(name=f"amount_with_tax_{i:04d}", mysql_type="numeric", generated=True,
                           generated_kind="STORED", generated_expression="(amount * 1.1)")])
        for i in range(n)])


def test_generated_columns_alone_over_the_limit_do_not_hold_the_load_and_say_why():
    from dsql_migrator.ui.data_migration import capture_pre_load_notices, data_migration_exclusion_block
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    inv = _pg_gen_tables()
    state = _state()  # Full load + CDC, NO saved exclusions
    # 0.1.561 let this load run; no exclusion change could fix it -> no hold ...
    assert data_migration_exclusion_block(state, _StubJobManager({}), None, inv) is None
    # ... but the card warns before the load, with a remedy that can be followed.
    (note,) = capture_pre_load_notices(state, _StubJobManager({}), inv)
    assert "generated columns Aurora DSQL computes" in note and "capture fewer tables" in note
    assert "Exclude fewer" not in note
    from dsql_migrator.core.table_selection import TableSelection

    state.set_selection(TableSelection(selected_tables=[t.name for t in inv.tables]))
    header, body = _deploy_block_notice(state, _StubJobManager({}), inv, None)
    assert header == "The excluded columns do not fit the CDC capture configuration"
    assert "capture fewer tables" in body and "Exclude fewer" not in body


def test_exclusions_on_deselected_tables_are_not_counted_in_the_capture_list():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    inv, entries = _wide()
    state = _state(entries=entries)
    captured = inv.tables[:1]
    cx = cdc_capture_exclusions(state, inv, captured)
    assert cx.error is None and list(cx.structured) == [captured[0].name]
    # The legacy (0.1.561) value is untouched: it still carries every stored entry.
    assert cx.legacy.count(",") == len(entries) - 1


def test_a_stack_lock_before_the_load_still_holds_an_oversized_capture_list():
    """Attaching (or deploying early) locked the card before the load; the load is still
    the last point to fix the list, with the lock's own remedy."""
    from dsql_migrator.ui.data_migration import data_migration_exclusion_block

    inv, entries = _wide()
    state = _state(entries=entries)
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    block = data_migration_exclusion_block(state, _StubJobManager({}), None, inv)
    assert block and "4,096-byte" in block and "delete the CDC infrastructure" in block
    assert "Exclude fewer" not in block


def test_attach_is_not_held_by_a_missing_column_and_deploy_names_each_reason():
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice, _render_cdc_attach_row
    from dsql_migrator.ui.data_migration._models import ExclusionInputs

    inv = SourceInventory(tables=[_t("shop.docs", ["id", "body", "note"])])
    ddl = {"shop.docs": SimpleNamespace(target_ddl='CREATE TABLE "shop"."docs" ("id" INT PRIMARY KEY, "body" TEXT)',
                                        foreign_key_ddls=[], preserved_foreign_keys=[])}
    state = _state(MigrationType.CDC_ONLY)
    state.set_exclusion_inputs_resolver(lambda: ExclusionInputs(["shop.docs"], ddl, True, None))
    ui = _RecordingUi()
    _render_cdc_attach_row(ui, state, _StubJobManager({}), lambda: None, [("dsql-cdc-old", "UPDATE_COMPLETE")],
                           inventory=inv)
    (attach,) = [b for b in ui.buttons if b.text == "Attach to dsql-cdc-old"]
    assert "disable" not in attach.props_seen
    header, body = _deploy_block_notice(state, _StubJobManager({}), inv, None)
    assert header == "A source column is missing from the Aurora DSQL table"
    assert "CDC would dead-letter every change to it" in body and "the load would fail" not in body


def test_the_banner_attach_gate_points_where_the_card_really_is():
    from dsql_migrator.ui.data_migration import _render_cdc_existing_infra_banner

    inv = SourceInventory(tables=[_t("shop.docs", ["id", "body"])])
    for mtype, where in ((MigrationType.CDC_ONLY, "on the CDC step"), (MigrationType.FULL_LOAD_AND_CDC, "below")):
        state = _state(mtype, entries=[("shop.docs", "id")])
        state.set_cdc_other_stacks([("dsql-cdc-old", "CREATE_COMPLETE")])
        ui = _RecordingUi()
        _render_cdc_existing_infra_banner(ui, state, lambda: None, job_manager=_StubJobManager({}),
                                          inventory=inv, session=None)
        joined = " ".join(ui.texts)
        assert f"in 'Oversized LOB columns' {where} first" in joined and "above first" not in joined, mtype


def test_full_load_only_after_lock_note_names_the_type_switch():
    only = _state(MigrationType.FULL_LOAD_ONLY)
    only.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    note = exclusion_after_lock_note(only, _StubJobManager({}))
    assert "switch the migration type to Full load + CDC" in note


def test_the_reload_tooltip_does_not_send_the_full_load_route_to_the_cdc_step():
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclude_and_reload_block_reason

    state = _state()
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    reason = exclude_and_reload_block_reason(state, _StubJobManager({}))
    assert "this action is available again" in reason and "on the CDC step." not in reason


def test_preview_equals_sent_for_a_legacy_value_with_an_empty_entry():
    from dsql_migrator.core.cdc import split_column_exclude_value

    for value in ("shop.t.a,,shop.t.b", r"shop\.t\.a\,b,shop.t.c", "", "shop.t.a"):
        assert ",".join(split_column_exclude_value(value)) == value, value


def test_non_ascii_letters_are_not_case_twins_for_debezium():
    from dsql_migrator.core.cdc import column_exclude_inexact_entries, format_column_exclude_value

    captured = [TableDef(name="shop.t", primary_key=[], columns=[ColumnDef(name=c, mysql_type="text")
                                                                 for c in ("É", "é", "k", "K")])]
    assert format_column_exclude_value({"shop.t": ["É"]}, captured) == "shop.t.É"
    assert column_exclude_inexact_entries({"shop.t": ["É"]}, captured) == []
    assert format_column_exclude_value({"shop.t": ["k"]}, captured) == "(?-i)shop.t.k"  # ASCII twin


def test_names_debezium_cannot_separate_are_warned_about():
    from dsql_migrator.ui.data_migration._models import capture_ambiguity_warning, cdc_capture_exclusions

    inv = SourceInventory(tables=[TableDef(name="shop.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int"), ColumnDef(name="a.b", mysql_type="text")]),
        TableDef(name="shop.t.a", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="int"), ColumnDef(name="b", mysql_type="text")])])
    state = _state(entries=[("shop.t", "a.b")])
    warning = capture_ambiguity_warning(cdc_capture_exclusions(state, inv, inv.tables))
    assert warning and "shop.t.a.b (also drops shop.t.a / b)" in warning
    assert capture_ambiguity_warning(cdc_capture_exclusions(_state(), inv, inv.tables)) is None


def test_a_quoted_referenced_column_with_a_paren_still_blocks_b3():
    from dsql_migrator.core.column_exclusion import B3, build_exclusion_context, column_exclusion_verdict
    from dsql_migrator.core.models import ForeignKeyDef

    parent = TableDef(name="shop.parent", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int"), ColumnDef(name="code)x", mysql_type="text")])
    child = TableDef(name="shop.child", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int"), ColumnDef(name="pc", mysql_type="text")],
        foreign_keys=[ForeignKeyDef(name="fk_c", columns=["pc"], referenced_table="shop.parent",
                                    referenced_columns=["code)x"])])
    ddl = 'ALTER TABLE "shop"."child" ADD CONSTRAINT "fk_c" FOREIGN KEY ("pc") REFERENCES "shop"."parent" ("code)x") NOT VALID'
    ctx = build_exclusion_context(SourceInventory(tables=[parent, child]), {"shop.child": SimpleNamespace(
        target_ddl="CREATE TABLE x (id INT)", foreign_key_ddls=[ddl], preserved_foreign_keys=[])})
    assert {f.code for f in column_exclusion_verdict("shop.parent", "code)x", ctx).findings} == {B3}


def test_a_named_pk_does_not_change_the_cdc_rekey_of_a_deployed_pipeline():
    from dsql_migrator.core.cdc import composite_key_columns_for_cdc

    orders = TableDef(name="shop.orders", primary_key=["id"], columns=[
        ColumnDef(name="tenant_id", mysql_type="bigint"), ColumnDef(name="id", mysql_type="bigint")])
    applied = {"shop.orders": SimpleNamespace(target_ddl=(
        'CREATE TABLE "shop"."orders" ("tenant_id" BIGINT NOT NULL, "id" BIGINT NOT NULL, '
        'CONSTRAINT orders_pk PRIMARY KEY ("tenant_id", "id"))'))}
    assert composite_key_columns_for_cdc([orders], applied) == {}  # as 0.1.561


# ---------------------------------------------------------------------------
# Round 4 (verification of 435a94b)
# ---------------------------------------------------------------------------


def _missing_col_state(mtype=MigrationType.CDC_ONLY):
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration._models import ExclusionInputs

    inv = SourceInventory(tables=[_t("shop.docs", ["id", "body", "note"])])
    ddl = {"shop.docs": SimpleNamespace(target_ddl='CREATE TABLE "shop"."docs" ("id" INT PRIMARY KEY, "body" TEXT)',
                                        foreign_key_ddls=[], preserved_foreign_keys=[])}
    state = _state(mtype)
    state.set_exclusion_inputs_resolver(lambda: ExclusionInputs(["shop.docs"], ddl, True, None))
    state.set_prereq_gated_mode(MigrationMode.CDC)
    return state, inv


def test_start_cdc_holds_on_a_missing_target_column_whatever_route_led_there():
    """Attach does not check 42703 (it moves no data), so Start CDC must."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    state, inv = _missing_col_state()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")  # e.g. just attached
    # READY otherwise (it resumes from its committed offset), so only 42703 can hold it.
    state.set_cdc_has_committed_offset(True)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui, state, _StubJobManager({}), lambda: None, inventory=inv, session=None)
    joined = " ".join(ui.texts)
    assert "A source column is missing from the Aurora DSQL table" in joined
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in " ".join(start.props_seen)
    assert any("42703" in t for t in ui.tooltips)
    # Control: with the column present the same ready state is startable.
    ok, inv_ok = _missing_col_state()
    ok.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    ok.set_cdc_has_committed_offset(True)
    ui_ok = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui_ok, ok, _StubJobManager({}), lambda: None,
                                     inventory=SourceInventory(tables=[_t("shop.docs", ["id", "body"])]), session=None)
    (start_ok,) = [b for b in ui_ok.buttons if b.text == "Start CDC"]
    assert "disable" not in " ".join(start_ok.props_seen)
    # Once CDC streams the check is skipped (Schema Conversion Apply is held then anyway).
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    from dsql_migrator.ui.data_migration._models import missing_column_block_reason

    assert missing_column_block_reason(state, inv, job_manager=_StubJobManager({})) is None


def test_start_cdc_refuses_a_missing_target_column_at_click_time(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_ui as cdcui
    from dsql_migrator.core.table_selection import TableSelection

    state, inv = _missing_col_state()
    state.set_selection(TableSelection(selected_tables=["shop.docs"]))
    notices: list = []
    monkeypatch.setattr(cdcui, "render_notice", lambda ui, **kw: notices.append(kw))
    submitted: list = []
    jm = SimpleNamespace(get_status=lambda _j: (_ for _ in ()).throw(KeyError(_j)),
                         submit=lambda w: submitted.append(w) or "job")
    cdcui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), state, jm, lambda: None,
                            inventory=inv, session=SimpleNamespace(source_config=None, target_config=None,
                                                                   aws_profile=None, restored_source_type=None,
                                                                   source_password=None))
    assert any(n.get("header") == "A source column is missing from the Aurora DSQL table" for n in notices)
    assert not submitted


def test_the_generated_overflow_remedy_follows_the_table_pickers_lock():
    from dsql_migrator.core.table_selection import TableSelection
    from dsql_migrator.ui.data_migration import capture_pre_load_notices
    from dsql_migrator.ui.data_migration._cdc_monitoring import capture_error_with_remedy
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    inv = _pg_gen_tables()
    state = _state()
    state.set_selection(TableSelection(selected_tables=[t.name for t in inv.tables]))
    capture = cdc_capture_exclusions(state, inv, inv.tables)
    free = capture_error_with_remedy(state, _StubJobManager({}), capture)
    assert "Deselect tables in the table picker." in free
    locked = capture_error_with_remedy(state, _StubJobManager({}), capture, table_lock="Locked — X.")
    assert "The table picker is locked. Locked — X." in locked and "Deselect tables" not in locked
    # Real lock: a stack deployed early holds the picker -> its own remedy (delete the stack).
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    (note,) = capture_pre_load_notices(state, _StubJobManager({}), inv, session=SimpleNamespace(workflow=None))
    assert "The table picker is locked." in note and "delete the CDC infrastructure" in note
    assert "Deselect tables in the table picker." not in note


def test_no_ambiguity_warning_when_both_same_named_columns_are_excluded():
    from dsql_migrator.ui.data_migration._models import capture_ambiguity_warning, cdc_capture_exclusions

    inv = SourceInventory(tables=[TableDef(name="shop.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int"), ColumnDef(name="a.b", mysql_type="text")]),
        TableDef(name="shop.t.a", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="int"), ColumnDef(name="b", mysql_type="text")])])
    both = _state(entries=[("shop.t", "a.b"), ("shop.t.a", "b")])
    assert capture_ambiguity_warning(cdc_capture_exclusions(both, inv, inv.tables)) is None


def test_full_load_only_reload_tooltip_names_the_type_switch():
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclude_and_reload_block_reason

    only = _state(MigrationType.FULL_LOAD_ONLY)
    only.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert "switch the migration type to Full load + CDC" in exclude_and_reload_block_reason(only, _StubJobManager({}))
    both = _state()
    both.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert "switch the migration type" not in exclude_and_reload_block_reason(both, _StubJobManager({}))


def test_the_ascii_fold_matches_the_reference_and_the_ambiguity_map_is_skipped_without_dots():
    from dsql_migrator.core import cdc

    for text in ("Shop.T.Note", "ÉTAT.Kelvin", "abc", "", "Ωmega.X", "MiXeD_1.Ä"):
        ref = "".join(chr(ord(ch) + 32) if "A" <= ch <= "Z" else ch for ch in text)
        assert cdc._cdc_ascii_fold(text) == ref, text
    plain = [TableDef(name="shop.t", primary_key=[], columns=[ColumnDef(name="a", mysql_type="text")])]
    assert cdc.column_exclude_ambiguous_entries({"shop.t": ["a"]}, plain) == {}
    assert cdc.column_exclude_ambiguous_entries({}, plain) == {}


def test_the_picker_lock_names_cdc_when_cdc_alone_promoted_the_step():
    """Round 5: the lock stays (the set was committed); only its false cause changes."""
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import selection_lock_reason, table_set_lock_reason

    cdc_only = _state(MigrationType.CDC_ONLY)
    reason = selection_lock_reason(cdc_only, _StubJobManager({}), status=StepStatus.DONE,
                                   migration_type=MigrationType.CDC_ONLY, has_job=False)
    assert reason.startswith("Locked — CDC has already streamed this table set.")
    assert "a Full Load has already run" not in reason
    # A recorded Full Load keeps the Full Load text (live job or pruned record alike).
    loaded = _state(job_id="job-1")
    for has_job in (True, False):
        text = selection_lock_reason(loaded, _StubJobManager({}), status=StepStatus.DONE,
                                     migration_type=MigrationType.FULL_LOAD_AND_CDC, has_job=has_job)
        assert "a Full Load has already run" in text
    # The generated-overflow remedy reuses it through table_set_lock_reason.
    from dsql_migrator.ui.workflow import WorkflowState, WorkflowStep, with_status

    session = SimpleNamespace(workflow=with_status(WorkflowState(), WorkflowStep.FULL_LOAD, StepStatus.DONE))
    assert table_set_lock_reason(cdc_only, _StubJobManager({}), session).startswith(
        "Locked — CDC has already streamed")
