# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G16: Full load + CDC can drop tables after its Full Load, until CDC first starts.

The table-set lock lifts through the SAME predicate as G12 (``table_set_editable_after_load``):
an unticked table is left as is on Aurora DSQL and is never captured, deployed, gated,
monitored or validated as CDC (ONE capture formula, ``cdc_capture_names``: the watermark's
covered tables still selected). Since G17 (tests/test_g17_full_load_and_cdc_additions.py) a
table the Full Load did not load may be ticked too -- it is loaded by a guarded Retry before
CDC captures it -- so the per-table add-lock (``table_set_add_lock``) refuses only a table an
exclude table rule holds; the tests below that pinned the older "not_loaded" refusal were
changed deliberately. Once CDC has started for the migration -- even if stopped since -- the
lock is exactly today's. A restored session behaves like a live one (the user's report,
2026-10-05). Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    MigrationJob,
    SourceInventory,
    SourceType,
    StepStatus,
    TableDef,
    TableSelection,
    Watermark,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

from tests.test_g12_table_set_after_load import (
    TODAY,
    _c,
    _flo,
    _inv,
    _job,
    _Jobs,
    _loaded,
    _reload_screen,
    _screen,
    _session,
)
from tests.test_ui_data_migration import _PickerUi, _RecordingUi, _StubJob, _StubJobManager

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
DONE = StepStatus.DONE
_TS = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _fc(selected=("app.a", "app.b"), *, touched=True, phase="absent", status=None,
        job_id="job-1"):
    """A Full load + CDC state after a Full Load; ``phase`` is what the probe reported
    (``None`` = not probed yet)."""
    state = _flo(selected, touched=touched, mtype=FLC, job_id=job_id)
    if phase is not None:
        state.set_cdc_stack_phase(phase, status=status)
    return state


def _reason(state, jobs, *, mtype=FLC, has_job=True, status=DONE):
    from dsql_migrator.ui.data_migration import selection_lock_reason

    return selection_lock_reason(
        state, jobs, status=status, migration_type=mtype, has_job=has_job
    )


def _with_session(state, engine=SourceType.MYSQL):
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=engine),
                                     restored_source_type=None)
    return state


# ---------------------------------------------------------------------------
# 1. The one predicate: Full load + CDC opens until CDC first starts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("phase", ["absent", "infra"])
def test_full_load_and_cdc_unlocks_after_a_load_once_cdc_is_known_not_started(phase) -> None:
    # FAILS WITHOUT THE FIX: this exact state read "Locked — a Full Load has already run".
    from dsql_migrator.ui.data_migration import (
        table_set_editable_after_load,
        table_set_lock_reason,
    )

    state = _fc(phase=phase, status="CREATE_COMPLETE" if phase == "infra" else None)
    jobs = _Jobs({"job-1": _loaded()})
    assert table_set_editable_after_load(state, jobs) is True
    assert _reason(state, jobs) is None
    assert table_set_lock_reason(state, jobs, _session(DONE)) is None


def _ops_window(state, jobs):
    state.set_cdc_ops_window_start(_TS)
    return state, jobs


def _connectors(state, jobs):
    state.cdc_connector_names = ["dsql-cdc-x-source"]
    return state, jobs


def _committed(state, jobs):
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_cdc_has_committed_offset(True)
    return state, jobs


def _phase(phase, status=None):
    def build(state, jobs):
        state.set_cdc_stack_phase(phase, status=status)
        return state, jobs
    return build


def _cdc_job(kind, *, ops_window=False):
    def build(state, jobs):
        state.set_cdc_deploy_job_id(f"{kind}-1", kind=kind)
        jobs.jobs[f"{kind}-1"] = _job("RUNNING", job_id=f"{kind}-1")
        if ops_window:
            state.set_cdc_ops_window_start(_TS)
        return state, jobs
    return build


def _pruned(state, _jobs):
    return state, _Jobs({})


def _no_counts(state, _jobs):
    return state, _Jobs({"job-1": _job("DONE", [_c("app.a"), _c("app.b")], counts={})})


def _cdc_only(state, jobs):
    state.migration_type = MigrationType.CDC_ONLY
    return state, jobs


@pytest.mark.parametrize("evidence", [
    _ops_window, _connectors, _committed, _phase("running", "UPDATE_COMPLETE"),
    _phase("partial"), _phase("provisioning"), _cdc_job("start"), _cdc_job("stop"),
    _phase("unstable", "UPDATE_IN_PROGRESS"), _phase("unstable", "UPDATE_ROLLBACK_FAILED"),
    _phase("unstable", "DELETE_FAILED"), _cdc_job("delete", ops_window=True), _pruned,
    _no_counts,
], ids=lambda f: getattr(f, "__name__", "x"))
def test_cdc_evidence_keeps_todays_lock_byte_for_byte(evidence) -> None:
    from dsql_migrator.ui.data_migration import (
        table_set_add_lock,
        table_set_editable_after_load,
    )

    state, jobs = evidence(_fc(), _Jobs({"job-1": _loaded()}))
    assert table_set_editable_after_load(state, jobs) is False
    assert table_set_add_lock(state, jobs) is None
    has_job = "job-1" in jobs.jobs
    assert _reason(state, jobs, mtype=state.migration_type, has_job=has_job) == TODAY


# ---------------------------------------------------------------------------
# 2. Waits: never "Start over"
# ---------------------------------------------------------------------------


def test_full_load_and_cdc_waits_never_say_start_over() -> None:
    # FAILS WITHOUT THE FIX: every one of these read "... Use 'Start over' ...".
    from dsql_migrator.ui.data_migration import (
        TABLE_SET_WAIT_CDC_CHECK,
        TABLE_SET_WAIT_CDC_UNREAD,
        TABLE_SET_WAIT_FK,
        TABLE_SET_WAIT_INFRA,
        TABLE_SET_WAIT_LOAD,
        cdc_delete_eta,
        table_set_failed_create_wait,
    )

    jobs = _Jobs({"job-1": _loaded()})
    fresh = _fc(phase=None)
    assert _reason(fresh, jobs) == TABLE_SET_WAIT_CDC_CHECK
    unread = _fc(phase=None)
    unread.set_cdc_probe_error("AccessDenied: cloudformation:DescribeStacks")
    assert _reason(unread, jobs) == TABLE_SET_WAIT_CDC_UNREAD
    infra = _fc(phase="absent")
    infra.set_cdc_deploy_job_id("infra-1", kind="infra")
    infra_jobs = _Jobs({"job-1": _loaded(), "infra-1": _job("RUNNING", job_id="infra-1")})
    assert _reason(infra, infra_jobs) == TABLE_SET_WAIT_INFRA
    # A restored session sees the create only as the probed CREATE_IN_PROGRESS status.
    creating = _fc(phase="unstable", status="CREATE_IN_PROGRESS")
    assert _reason(creating, jobs) == TABLE_SET_WAIT_INFRA
    rolled = _fc(phase="unstable", status="ROLLBACK_COMPLETE")
    failed = _reason(rolled, jobs)
    assert failed == table_set_failed_create_wait("ROLLBACK_COMPLETE")
    assert "(ROLLBACK_COMPLETE), so it never streamed" in failed
    estimate, _why = cdc_delete_eta(fresh)
    delete_text = (
        f"Locked while the CDC infrastructure is being deleted (a cdc-stack delete takes "
        f"{estimate}). Wait for the deletion to finish before changing the table set."
    )
    deleting_job = _fc(phase=None)
    deleting_job.set_cdc_deploy_job_id("delete-1", kind="delete")
    delete_jobs = _Jobs({"job-1": _loaded(), "delete-1": _job("RUNNING", job_id="delete-1")})
    assert _reason(deleting_job, delete_jobs) == delete_text
    deleting = _fc(phase="unstable", status="DELETE_IN_PROGRESS")
    assert _reason(deleting, jobs) == delete_text
    running = _fc()
    assert _reason(running, _Jobs({"job-1": _job("RUNNING", [_c("app.a", "PENDING")])})) == (
        TABLE_SET_WAIT_LOAD)
    fk = _fc()
    fk.set_fk_apply_job("fk-1")
    fk_jobs = _Jobs({"job-1": _loaded(), "fk-1": _job("RUNNING", job_id="fk-1")})
    assert _reason(fk, fk_jobs) == TABLE_SET_WAIT_FK
    for text in (TABLE_SET_WAIT_CDC_CHECK, TABLE_SET_WAIT_CDC_UNREAD, TABLE_SET_WAIT_INFRA,
                 failed, delete_text):
        assert text.startswith("Locked") and "Start over" not in text


def test_full_load_only_stays_independent_of_the_probe() -> None:
    from dsql_migrator.ui.data_migration import (
        table_set_add_lock,
        table_set_editable_after_load,
    )

    jobs = _Jobs({"job-1": _loaded()})
    unprobed = _flo()
    assert table_set_editable_after_load(unprobed, jobs) is True
    assert _reason(unprobed, jobs, mtype=FLO) is None  # G12: no CDC state to wait for
    assert table_set_add_lock(unprobed, jobs) is None  # G12 adds freely
    infra = _flo()
    infra.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert table_set_editable_after_load(infra, jobs) is False
    assert _reason(infra, jobs, mtype=FLO) == TODAY


# ---------------------------------------------------------------------------
# 3. ONE capture formula: the covered tables still selected
# ---------------------------------------------------------------------------


def test_cdc_capture_is_the_covered_tables_still_selected() -> None:
    # FAILS WITHOUT THE FIX: the watermark's whole set was captured, whatever was unticked.
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config

    inv = _inv("app.a", "app.b", "app.c")
    wm = _job(counts={"app.c": 1, "app.a": 1, "app.b": 1}).watermark

    def names(state, watermark=wm):
        return [t.name for t in _cdc_tables_for_config(state, inv, watermark)]

    assert names(_fc(["app.c", "app.a"])) == ["app.a", "app.c"]  # inventory order
    assert names(_fc(["app.a"], touched=False)) == ["app.a", "app.b", "app.c"]
    assert names(_fc(["app.a", "app.x"])) == ["app.a"]  # never an uncovered table
    assert names(_fc([])) == []
    from dsql_migrator.ui.data_migration import cdc_capture_names

    assert cdc_capture_names(_fc([]), wm) == frozenset()
    # Without per-table counts: the selection, then the deployed list -- unchanged.
    assert cdc_capture_names(_fc(["app.a"]), None) is None
    assert names(_fc(["app.b"]), None) == ["app.b"]
    adopted = _fc([], touched=False)
    adopted.set_cdc_reconciled_table_names(["app.c"])
    assert names(adopted, None) == ["app.c"]


def _orders_inventory():
    def table(name):
        return TableDef(name=name, primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="note", mysql_type="text", nullable=True)])

    return SourceInventory(tables=[table("orders"), table("customers")])


def _orders_watermark():
    return Watermark(
        binlog_file="mysql-bin.000042", binlog_position=120, snapshot_timestamp=_TS,
        table_row_counts={"orders": 10, "customers": 5},
    )


def test_start_cdc_sends_only_the_captured_tables(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: TableIncludeList / SinkTopics named customers too.
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    from tests.test_ui_data_migration import _run_start_cdc_capture

    state = DataMigrationState()
    state.migration_type = FLC
    state.set_selection(TableSelection(selected_tables=["orders"]))
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, job_watermark=_orders_watermark(),
        state=state, inventory=_orders_inventory(),
    )
    assert run["submitted"]
    filled = dict(run["kwargs"]["params"].filled)
    assert filled["TableIncludeList"] == "orders"
    assert "orders" in filled["SinkTopics"] and "customers" not in filled["SinkTopics"]
    # The ColumnExcludeList Start sends is computed over the same capture (ONE computation
    # for the preview, Start and Deploy): an unticked table's exclusions are not sent.
    state.restore_lob_exclusion("orders", "note")
    state.restore_lob_exclusion("customers", "note")
    inventory = _orders_inventory()
    sent = cdc_capture_exclusions(
        state, inventory, _cdc_tables_for_config(state, inventory, _orders_watermark())).sent
    assert "orders" in sent and "customers" not in sent


def test_infra_partition_rows_and_will_stream_follow_the_capture() -> None:
    # FAILS WITHOUT THE FIX: the plan and "Will stream" both counted customers.
    from dsql_migrator.ui.data_migration._cdc_ui import (
        _cdc_row_counts_from_watermark,
        _cdc_tables_for_config,
        _render_cdc_start_button,
    )

    inv = _orders_inventory()
    wm = _orders_watermark()
    state = DataMigrationState()
    state.migration_type = FLC
    state.job_id = "job-1"
    state.set_selection(TableSelection(selected_tables=["orders"]))
    assert _cdc_row_counts_from_watermark(wm, _cdc_tables_for_config(state, inv, wm)) == {
        "orders": 10}
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    job = MigrationJob(job_id="job-1", status="DONE", watermark=wm,
                       chunks=[ChunkState(chunk_id=n, status="DONE") for n in ("orders",
                                                                              "customers")])
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": job}), lambda: None, inventory=inv,
                             session=None)
    assert "Will stream 1 table: orders" in ui.texts


def _fc_screen(monkeypatch, *, job, selected, phase="absent", pg=False, render=True):
    """The real Data Migration screen for a Full load + CDC session after a Full Load
    (G12's ``_screen``), with the popup context and the tree's kwargs captured. The probe
    never runs (the timer double does not fire it): ``phase`` is what it reported."""
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _exclusion_dialog as xd

    captured: dict = {}
    real_row = xd.render_exclusion_button_row
    monkeypatch.setattr(xd, "render_exclusion_button_row",
                        lambda ui, ctx: captured.__setitem__("ctx", ctx) or real_row(ui, ctx))
    tree: dict = {}
    real_tree = dm._render_table_selection

    def _tree(*a, **k):
        tree.clear()
        tree.update(k)
        return real_tree(*a, **k)

    monkeypatch.setattr(dm, "_render_table_selection", _tree)
    s = _screen(monkeypatch, job=job, selected=selected, pg=pg)
    s.session.set_migration_type(FLC)
    if phase is not None:
        s.state.set_cdc_stack_phase(phase, status="CREATE_COMPLETE" if phase == "infra" else None)
    s.ui.timer = lambda interval, callback=None, *a, **k: s.ui._El(s.ui)
    s.captured = captured
    s.tree = tree
    if render:
        s.content(lambda: None)
    return s


def test_every_derived_capture_follows_the_narrowing(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the drift baseline, the popup's capture preview and the monitor
    # all kept the unticked app.b.
    from dsql_migrator.ui.data_migration._cdc_status import _migration_status_tables

    def derived(s, after):
        baseline = s.state.cdc_catalog_baseline()
        return (
            [t.name for t in baseline.captured],
            set(baseline.capture_names),
            [t.name for t in s.captured["ctx"].capture_tables_fn(after)],
            _migration_status_tables(s.state, s.jobs),
        )

    narrowed = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a"])
    assert derived(narrowed, ["app.a"]) == (["app.a"], {"app.a"}, ["app.a"], ["app.a"])
    # A popup Save that would unselect app.b previews the capture without it.
    both = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    assert [t.name for t in both.captured["ctx"].capture_tables_fn(["app.a"])] == ["app.a"]
    # Nothing narrowed, or an untouched selection: today's sets.
    whole = (["app.a", "app.b"], {"app.a", "app.b"}, ["app.a", "app.b"], ["app.a", "app.b"])
    assert derived(both, ["app.a", "app.b"]) == whole
    untouched = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a"], render=False)
    untouched.state.selection_touched = False
    untouched.content(lambda: None)
    assert derived(untouched, ["app.a", "app.b"])[:2] == whole[:2]
    assert _migration_status_tables(untouched.state, untouched.jobs) == ["app.a", "app.b"]


# ---------------------------------------------------------------------------
# 4. The per-table add-lock
# ---------------------------------------------------------------------------


def test_the_add_lock_codes_and_refusals() -> None:
    from dsql_migrator.ui.data_migration import table_set_add_lock

    jobs = _Jobs({"job-1": _loaded()})  # covers app.a, app.b
    lock = table_set_add_lock(_fc(["app.a"]), jobs)
    assert lock.code("app.b", tree=True) is None
    # G17 (deliberately changed): a table the Full Load did not load may be ticked -- its
    # guarded load decides whether CDC can capture it.
    assert lock.code("app.c", tree=True) is None
    assert lock.code("app.c", tree=False) is None
    assert lock.refusal(["app.c"], tree=True) is None
    assert lock.refusal(["app.d", "app.c"], tree=False) is None
    assert lock.refusal(["app.b"], tree=True) is None
    assert lock.covered == frozenset({"app.a", "app.b"})
    # A never-started stack deployed without app.b: a covered table may still be ticked
    # again -- Start CDC creates its topic from the deployed partition plan (below).
    infra = _fc(["app.a"], phase="infra", status="CREATE_COMPLETE")
    infra.set_cdc_reconciled_table_names(["app.a"])
    assert table_set_add_lock(infra, jobs).code("app.b", tree=True) is None
    # An exclude table rule's unselect: refused in the tree only.
    ruled = _fc(["app.a"])
    ruled.restore_exclusion_rules("exclude table app.b", ["app.b"])
    rule_lock = table_set_add_lock(ruled, jobs)
    assert rule_lock.code("app.b", tree=True) == "rule"
    assert rule_lock.code("app.b", tree=False) is None
    assert rule_lock.refusal(["app.b"], tree=True) == (
        "An exclude table rule unselected app.b, and that Save dropped its excluded-column "
        "entries: ticked here, CDC would stream values for columns its loaded rows hold as "
        "NULL. To select it again, delete that rule in Exclude tables & columns (Change & "
        "reload), which applies its column rules again.")
    # G17: a rule-held table the Full Load did not load gets the truthful variant (it has
    # no loaded rows holding NULL), covered ones first, the tables in input order.
    ruled.restore_exclusion_rules("exclude table app.b\nexclude table app.c",
                                  ["app.b", "app.c"])
    both = table_set_add_lock(ruled, jobs).refusal(["app.c", "app.b"], tree=True)
    assert both.index("unselected app.b, and") < both.index("unselected app.c, and")
    assert "loaded rows hold as NULL" not in both.split("unselected app.c")[1]
    # Full load only, or CDC evidence: no add-lock.
    assert table_set_add_lock(_flo(), jobs) is None
    started = _fc()
    started.set_cdc_ops_window_start(_TS)
    assert table_set_add_lock(started, jobs) is None


def test_the_add_lock_order_is_deterministic() -> None:
    from dsql_migrator.ui.data_migration import TableSetAddLock

    lock = TableSetAddLock(covered=frozenset({"app.b", "app.a"}),
                           rule_held=frozenset({"app.a", "app.y"}))
    names = ["app.z", "app.b", "app.y", "app.a", "app.b"]
    # G17 (deliberately changed): only rule-held tables, and only in the tree.
    assert lock.blocked(names, tree=True) == ["app.y", "app.a"]
    assert lock.blocked(names, tree=False) == []
    assert lock.node_flags(names, ["app.y"]) == {"app.a": "rule"}
    assert lock.refusal(names, tree=False) is None
    assert lock.refusal(names, tree=True).startswith(
        "An exclude table rule unselected app.a, and")
    assert hash(lock) == hash(TableSetAddLock(
        covered=frozenset({"app.a", "app.b"}), rule_held=frozenset({"app.y", "app.a"})))


# ---------------------------------------------------------------------------
# 5. The tree: lock icons, a whole-or-nothing tick, Select all, kept unlisted names
# ---------------------------------------------------------------------------


def test_locked_leaves_are_not_tickable_and_carry_a_lock_code() -> None:
    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.ui.data_migration import (
        LOCK_TIP_RULE,
        TableSetAddLock,
        _header_table_slot_with_locks,
        build_migration_table_tree,
    )

    inv = _inv("app.a", "app.b", "app.c")
    listed = ["app.a", "app.b", "app.c"]
    plain = build_migration_table_tree(inv, listed)
    assert build_migration_table_tree(inv, listed, add_locked=None) == plain
    flagged = build_migration_table_tree(inv, listed, add_locked={"app.c": "rule"})
    leaves = {leaf["id"]: leaf for leaf in flagged[0]["children"][0]["children"]}
    assert leaves["table:app.c"]["tickable"] is False
    assert leaves["table:app.c"]["add_locked"] == "rule"
    assert "disabled" not in leaves["table:app.c"]  # disabled would kill the tooltip
    assert "tickable" not in leaves["table:app.a"] and "add_locked" not in leaves["table:app.a"]
    lock = TableSetAddLock(covered=frozenset({"app.a", "app.b"}), rule_held=frozenset({"app.c"}))
    assert lock.node_flags(listed, ["app.a", "app.c"]) == {}  # a ticked leaf: always untickable
    assert lock.node_flags(listed, ["app.a"]) == {"app.c": "rule"}
    # G17 (deliberately changed): an uncovered table is no longer flagged at all.
    assert TableSetAddLock(covered=frozenset({"app.a"})).node_flags(listed, ["app.a"]) == {}
    slot = _header_table_slot_with_locks()
    assert "props.node.add_locked === 'rule'" in slot
    assert f"<q-tooltip>{LOCK_TIP_RULE}</q-tooltip>" in slot
    assert "not_loaded" not in slot and not hasattr(dm, "LOCK_TIP_NOT_LOADED")
    assert "not_deployed" not in slot


class _TreeUi(_PickerUi):
    """The picker double, keeping the tree's tick handler, its tick/untick calls, the
    leaf slot, the bulk buttons' handlers and every notification."""

    def __init__(self):
        super().__init__()
        self.ticks: list = []
        self.unticks: list = []
        self.slots: list = []
        self.trees: list = []
        self.nodes: list = []
        self.clicks: dict = {}
        self.notes: list = []

    class _Tree:
        def __init__(self, ui):
            self._ui = ui

        def tick(self, ids=None):
            self._ui.ticks.append(ids)
            return self

        def untick(self, ids=None):
            self._ui.unticks.append(ids)
            return self

        def add_slot(self, name, template=None):
            self._ui.slots.append((name, template))
            return self

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def tree(self, nodes=None, *_a, on_tick=None, **_k):
        self.trees.append(on_tick)
        self.nodes.append(nodes)
        return self._Tree(self)

    def button(self, text="", *_a, on_click=None, **_k):
        if text:
            self.buttons.append(str(text))
            self.clicks[str(text)] = on_click
        return self._El(self)

    def notify(self, text="", *_a, **k):
        self.notes.append((str(text), k.get("type")))


def _render_tree(state, jobs, listed, *, with_lock=True):
    from dsql_migrator.ui.data_migration import _render_table_selection, table_set_add_lock

    ui = _TreeUi()
    changes: list = []
    refreshed: list = []
    _render_table_selection(
        ui, _inv(*listed, "app.c"), state, listed, target_existing=listed,
        default_selection=listed, lock_check=lambda: None,
        on_locked=lambda: refreshed.append(1), on_selection_change=changes.append,
        add_lock_fn=(lambda: table_set_add_lock(state, jobs)) if with_lock else None,
    )
    return ui, changes, refreshed


def test_the_tree_unticks_and_refuses_a_rule_held_tick_whole() -> None:
    # FAILS WITHOUT THE FIX: there was no add_lock_fn (and the tree was locked). G17
    # (deliberately changed): an uncovered table ticks and is stored (pending its guarded
    # load); only a rule-held one refuses the whole tick.
    state = _fc(["app.a", "app.b"])
    state.restore_exclusion_rules("exclude table app.d", ["app.d"])
    jobs = _Jobs({"job-1": _loaded()})
    ui, changes, refreshed = _render_tree(state, jobs, ["app.a", "app.b", "app.c", "app.d"])
    (on_tick,) = ui.trees
    leaves = {leaf["id"]: leaf for leaf in ui.nodes[0][0]["children"][0]["children"]}
    assert "add_locked" not in leaves["table:app.c"]
    assert leaves["table:app.d"]["add_locked"] == "rule"
    (template,) = [t for name, t in ui.slots if name == "header-table"]
    assert "props.node.add_locked" in template
    on_tick(SimpleNamespace(value=["table:app.a"]))  # untick app.b
    assert state.selection.selected_tables == ["app.a"] and changes == [["app.a"]]
    on_tick(SimpleNamespace(value=["table:app.a", "table:app.d", "table:app.b"]))
    (note, tone), = ui.notes
    assert tone == "warning" and note.startswith(
        "An exclude table rule unselected app.d, and that Save dropped its excluded-column "
        "entries: ticked here, it would load and stream every column")
    assert note.endswith("This change was not applied.")
    assert refreshed == [1]
    assert state.selection.selected_tables == ["app.a"]  # all or nothing: app.b not added
    on_tick(SimpleNamespace(value=["table:app.a", "table:app.c", "table:app.b"]))
    assert state.selection.selected_tables == ["app.a", "app.b", "app.c"]  # stored, pending
    assert len(ui.notes) == 1


def test_select_all_selects_only_addable_tables_and_reports_the_skip() -> None:
    # G17 (deliberately changed): Select all ticks the tables the Full Load did not load too
    # (each pending its guarded load) and skips only a rule-held one.
    state = _fc(["app.a"])
    state.restore_exclusion_rules("exclude table app.d", ["app.d"])
    jobs = _Jobs({"job-1": _loaded()})
    ui, changes, _refreshed = _render_tree(state, jobs, ["app.a", "app.b", "app.c", "app.d"])
    ui.clicks["Select all"]()
    # Explicit ids, never tick() all.
    assert ui.ticks[-1] == ["table:app.a", "table:app.b", "table:app.c"]
    assert state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert changes == [["app.a", "app.b", "app.c"]]
    assert ui.notes == [("1 table cannot be selected now and stays unselected (a lock marks "
                         "it in the list).", "info")]
    # Without an add-lock: today's Select all.
    flo = _flo(["app.a"])
    plain, _c2, _r2 = _render_tree(flo, jobs, ["app.a", "app.b", "app.c"], with_lock=False)
    plain.clicks["Select all"]()
    assert plain.ticks[-1] is None and plain.notes == []
    assert flo.selection.selected_tables == ["app.a", "app.b", "app.c"]


def test_unselect_all_stays_allowed_and_an_empty_capture_is_refused_downstream(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the capture stayed the whole watermark set after Unselect all.
    import asyncio
    import sys
    import types

    from dsql_migrator.ui.data_migration import (
        DEPLOY_EMPTY_CAPTURE,
        WILL_STREAM_NONE,
        cdc_capture_names,
    )
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fc(["app.a", "app.b"])
    jobs = _Jobs({"job-1": _loaded()})
    ui, changes, _refreshed = _render_tree(state, jobs, ["app.a", "app.b"])
    ui.clicks["Unselect all"]()
    assert ui.unticks and state.selection.selected_tables == [] and changes == [[]]
    assert cdc_capture_names(state, _loaded().watermark) == frozenset()
    io_calls: list = []

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            io_calls.append(fn)
            raise AssertionError("no AWS / io_bound call may run before the guard")

    fake = types.ModuleType("nicegui")
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    notes: list = []
    deploy_ui = SimpleNamespace(notify=lambda text="", **k: notes.append((text, k.get("type"))))
    asyncio.run(_cdc_ui._start_cdc_infra_deploy(
        deploy_ui, state, jobs, lambda: None, inventory=_inv("app.a", "app.b"),
        session=SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"))))
    assert notes == [(DEPLOY_EMPTY_CAPTURE, "warning")] and io_calls == []
    # The Start button says nothing will stream (not "all tables covered...").
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    start = _RecordingUi()
    _cdc_ui._render_cdc_start_button(start, state, jobs, lambda: None,
                                     inventory=_inv("app.a", "app.b"), session=None)
    assert WILL_STREAM_NONE in start.texts
    assert not any(t.startswith("Will stream all tables") for t in start.texts)


def test_tree_writes_keep_a_loaded_table_the_tree_cannot_list() -> None:
    # FAILS WITHOUT THE FIX: a tick stored (listed AND ticked), silently dropping app.h --
    # a loaded table a stale target catalog no longer lists -- from the CDC capture.
    state = _fc(["app.a", "app.b", "app.h"])
    jobs = _Jobs({"job-1": _job("DONE", [_c("app.a"), _c("app.b"), _c("app.h")],
                                counts={"app.a": 1, "app.b": 1, "app.h": 1})})
    ui, _changes, _refreshed = _render_tree(state, jobs, ["app.a", "app.b"])
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:app.a"]))
    assert state.selection.selected_tables == ["app.a", "app.h"]
    ui.clicks["Select all"]()
    assert state.selection.selected_tables == ["app.a", "app.b", "app.h"]
    ui.clicks["Unselect all"]()
    assert state.selection.selected_tables == ["app.h"]


# ---------------------------------------------------------------------------
# 6. Exclude tables & columns: table lines drop, never add
# ---------------------------------------------------------------------------


def _open_reload(s):
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    ui = _Ui()
    open_exclusion_dialog(ui, s.ctx)
    ui.click("Change & reload")
    return ui


def _action(ui):
    return [b for b in ui.elements if b.kind == "button"
            and (b.text == "Save" or str(b.text).startswith("Drop & reload"))][-1]


def test_change_and_reload_unselects_a_table_in_full_load_and_cdc(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the table line was E3 ("... Start over").
    s = _reload_screen(monkeypatch, mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    ui = _open_reload(s)
    assert any(t.startswith("Column and table rules are editable.") for t in ui.texts)
    ui.type_text("exclude table app.c\nexclude table app.b")
    assert not any("E3" in t or "Start over" in t for t in ui.texts if "rule" in t.lower()
                   and "would" in t)
    assert any("Unselects app.b: left as is on Aurora DSQL (not dropped) and not captured by "
               "CDC; its excluded-column entries are not kept." in t for t in ui.texts)
    assert _action(ui).enabled
    ui.click("Save")
    assert s.calls == []  # nothing dropped or reloaded
    assert s.state.selection.selected_tables == ["app.a"]
    assert s.state.rule_excluded_tables() == frozenset({"app.b", "app.c"})
    from dsql_migrator.ui.data_migration._exclusion_dialog import FL_CDC_RELOAD_LABEL

    assert FL_CDC_RELOAD_LABEL in ui.texts


def test_change_and_reload_selects_an_uncovered_table_for_retry(monkeypatch) -> None:
    # A loaded table a rule unselected after the load may be selected again (the popup
    # applies its column rules again) and is kept as is. G17 (deliberately changed from a
    # per-line E3): app.c (not in the snapshot) may be selected too -- listed under Retry
    # unfinished tables, and captured once loaded.
    s = _reload_screen(monkeypatch, record=("app.a", "app.b"),
                       rules="exclude table app.b\nexclude table app.c",
                       rule_excluded=("app.b", "app.c"), selection=("app.a",), mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    ui = _open_reload(s)
    ui.type_text("exclude table app.c")  # deletes the app.b rule
    assert _action(ui).enabled
    assert any("Selects app.b again: already loaded by the last Full Load, kept as is" in t
               for t in ui.texts)
    ui.type_text("exclude table app.b\nexclude table app.c\ninclude table app.c")
    assert not any("This rule would select app.c again" in t for t in ui.texts)
    assert _action(ui).enabled
    # G17 review (deliberately changed): this watermark records no MySQL start point for
    # app.c, so Retry cannot add it -- the line names Re-run all tables too.
    assert any("Selects app.c: listed under Retry unfinished tables on the Full Load step "
               "until loaded (Re-run all tables loads it where that step says Retry cannot "
               "add it); CDC then captures it with the other tables." in t for t in ui.texts)


def test_an_unprobed_change_and_reload_waits(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the label said tables are fixed (use Start over).
    from dsql_migrator.ui.data_migration import TABLE_SET_WAIT_CDC_CHECK

    s = _reload_screen(monkeypatch, mtype=FLC)  # the probe has not reported
    ui = _open_reload(s)
    assert f"Column rules are editable. {TABLE_SET_WAIT_CDC_CHECK}" in ui.texts
    ui.type_text("exclude app.*.note")
    assert any("Save would re-select app.c" in t and TABLE_SET_WAIT_CDC_CHECK in t
               for t in ui.texts)
    assert not any("Selects app.c" in t for t in ui.texts)
    assert not _action(ui).enabled


def test_the_popup_memo_follows_the_add_lock(monkeypatch) -> None:
    # The memo key carries the add-lock: a Re-run Full Load over the smaller set meanwhile
    # changes the lock (its fresh watermark no longer covers app.b), so a cached resolution
    # is not served for it. G17 (deliberately changed): re-selecting app.b is no E3 any more
    # -- it is a pending addition -- and once CDC has started the popup's table set is fixed
    # again (today's E3 with today's lock text).
    from dsql_migrator.ui.data_migration._exclusion_dialog import _resolve

    s = _reload_screen(monkeypatch, record=("app.a", "app.b"), rules="exclude table app.b",
                       rule_excluded=("app.b",), selection=("app.a",), mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    inventory, inputs = s.ctx.inventory_fn(), s.ctx.inputs_fn()
    first = _resolve(s.ctx, inventory, inputs, "", slot="editor")
    assert first.can_save and first.tables_to_reselect == ("app.b",)
    lock_before = s.ctx.add_lock_fn()
    s.jobs.jobs["job-2"] = _job("DONE", [_c("app.a")], counts={"app.a": 1}, job_id="job-2")
    s.state.job_id = "job-2"
    assert s.ctx.add_lock_fn() != lock_before  # a different memo key
    second = _resolve(s.ctx, inventory, inputs, "", slot="editor")
    assert second is not first
    assert second.can_save and second.tables_to_reselect == ("app.b",)
    s.state.set_cdc_ops_window_start(_TS)
    third = _resolve(s.ctx, inventory, inputs, "", slot="editor")
    assert not third.can_save
    (e3,) = third.global_errors
    assert e3.code == "E3" and TODAY in e3.message


def test_r4_and_r1_name_the_narrowing() -> None:
    # FAILS WITHOUT THE FIX: R4 ended "Changing the table set needs Start over." and R1 had
    # no word about the table list.
    from dsql_migrator.ui.data_migration import R1_FL_CDC_SENTENCE, R4_FL_CDC_NARROWING
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclusion_lock_state

    jobs = _Jobs({"job-1": _loaded()})
    r4 = exclusion_lock_state(_fc(), jobs)
    assert (r4.tier, r4.cell, r4.offers_change_and_reload) == (3, "R4", True)
    assert r4.reason == R4_FL_CDC_NARROWING
    assert "Changing the table set needs Start over" not in r4.reason
    infra = _with_session(_fc(phase="infra", status="CREATE_COMPLETE"))
    r1 = exclusion_lock_state(infra, jobs)
    assert r1.cell == "R1" and r1.reason.endswith(R1_FL_CDC_SENTENCE)
    # Once CDC was started: today's texts exactly (R1 byte-identical but for the sentence).
    started = _fc()
    started.set_cdc_ops_window_start(_TS)
    assert exclusion_lock_state(started, jobs).reason.endswith(
        "Changing the table set needs Start over.")
    started_infra = _with_session(_fc(phase="infra", status="CREATE_COMPLETE"))
    started_infra.set_cdc_ops_window_start(_TS)
    assert exclusion_lock_state(started_infra, jobs).reason == r1.reason[
        : -len(R1_FL_CDC_SENTENCE)]


# ---------------------------------------------------------------------------
# 7. The scoped Full Load view, the step and Retry
# ---------------------------------------------------------------------------


def test_full_load_scope_names_in_full_load_and_cdc_narrows_and_outlives_the_start(
) -> None:
    # FAILS WITHOUT THE FIX: Full load + CDC never had a scoped view.
    from dsql_migrator.ui.data_migration._models import (
        full_load_scope_names,
        scope_full_load_job,
    )

    jobs = _Jobs({"job-1": _loaded()})
    assert full_load_scope_names(_fc(["app.a"]), jobs, ["ignored"]) == ["app.a"]
    # G17 (deliberately changed): while CDC never started, a selected table the watermark
    # does not cover is in the view, PENDING until its guarded load.
    with_new = full_load_scope_names(_fc(["app.a", "app.c"]), jobs, ["app.a", "app.c"])
    assert with_new == ["app.a", "app.c"]
    view = scope_full_load_job(jobs.jobs["job-1"], with_new)
    assert [(c.chunk_id, c.status) for c in view.chunks] == [
        ("app.a", "DONE"), ("app.c", "PENDING")]
    # ...and once CDC has started, the view is removal-only again.
    started_new = _fc(["app.a", "app.c"])
    started_new.set_cdc_ops_window_start(_TS)
    assert full_load_scope_names(started_new, jobs, ["app.a", "app.c"]) == ["app.a"]
    started = _fc(["app.a"])
    started.set_cdc_ops_window_start(_TS)
    started.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    assert full_load_scope_names(started, jobs, []) == ["app.a"]  # the start keeps it
    assert full_load_scope_names(_fc(["app.a"], touched=False), jobs, []) is None
    assert full_load_scope_names(_fc([]), jobs, []) is None
    assert full_load_scope_names(_fc(["app.a", "app.b"]), jobs, []) is None  # nothing changed
    assert full_load_scope_names(
        _fc(["app.a"]), _StubJobManager({"job-1": _StubJob("COMPLETED")}), []) is None
    assert full_load_scope_names(_fc(["app.a"]), None, []) is None


def test_unticking_the_only_failed_table_completes_the_step_and_enables_continue_to_cdc(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the step stayed Failed ("Retry the failed tables before
    # starting CDC") although app.b had been unticked.
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    synced: list = []
    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables",
                        lambda _cfg, names, **_k: synced.append(list(names)) or {})
    notices: list = []
    real_notice = fl.render_notice

    def _notice(ui, *, tone, header, body="", **k):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    monkeypatch.setattr(fl, "render_notice", _notice)
    job = _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")], counts={"app.a": 1, "app.b": 1})
    s = _fc_screen(monkeypatch, job=job, selected=["app.a"], render=False)
    s.jobs.errors["job-1"] = "load failed for app.b"
    s.session.set_workflow(
        with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    s.state.set_error("load failed for app.b")
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    assert s.state.error is None
    (work,) = s.jobs.submitted  # the identity sync the failed run's finalize skipped
    work(SimpleNamespace())
    assert synced == [["app.a"]]
    (scope,) = [n for n in notices if n[1] == "Table set changed after the last Full Load"]
    assert scope[0] == "info" and "CDC does not capture it" in scope[2]
    assert not any(n[1] == "Load failed" for n in notices)
    buttons = [b for b in s.ui.buttons if b.text == "Continue to CDC"]
    assert buttons and all("disable" not in b.props_seen for b in buttons)
    assert not any(b.text.startswith("Retry unfinished") for b in s.ui.buttons)
    # With app.b still selected the step stays Failed, and the hint names the other way out.
    from dsql_migrator.ui.data_migration import CONTINUE_HINT_FL_CDC

    held = _fc_screen(monkeypatch, job=job, selected=["app.a", "app.b"], render=False)
    held.session.set_workflow(
        with_status(held.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    held.content(lambda: None)
    assert any(t.endswith(CONTINUE_HINT_FL_CDC) for t in held.ui.texts)
    # ...but not once CDC was started (the set is fixed again).
    started = _fc_screen(monkeypatch, job=job, selected=["app.a", "app.b"], render=False)
    started.session.set_workflow(
        with_status(started.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    started.state.set_cdc_ops_window_start(_TS)
    started.content(lambda: None)
    assert not any(t.endswith(CONTINUE_HINT_FL_CDC) for t in started.ui.texts)
    assert any(t.startswith("Retry the failed tables before starting CDC")
               for t in started.ui.texts)


def test_a_retry_in_full_load_and_cdc_never_reloads_an_unticked_table(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: Retry reloaded app.b, which this migration no longer captures.
    from dsql_migrator.ui import data_migration as dm

    calls: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, prior, tables, **kw: calls.append(
                            (list(prior), [t.name for t in tables], kw)))
    job = _job("FAILED", [_c("app.a", "FAILED"), _c("app.b", "FAILED")],
               counts={"app.a": 1, "app.b": 1})
    s = _fc_screen(monkeypatch, job=job, selected=["app.a"])
    s.jobs.submit = lambda fn: fn(SimpleNamespace()) or "job-2"
    retry = s.captured["ctx"].extra["run_retry_for"]
    retry(["app.a", "app.b"])
    (prior, tables, kw), = calls
    assert tables == ["app.a"] and kw["run_table_names"] == ["app.a"]
    assert [c.chunk_id for c in prior] == ["app.a", "app.b"]  # the record keeps history
    assert any(m.startswith("app.b is no longer selected") for m, _k in s.ui.notified)
    calls.clear()
    s.state.job_id = "job-1"  # the inline "retry" above recorded job-2
    s.state.set_selection(TableSelection(selected_tables=[]))
    retry(["app.a"])
    assert calls == [] and any("No table is selected" in m for m, _k in s.ui.notified)


# ---------------------------------------------------------------------------
# 8. PostgreSQL: the load's slot is honoured when only unticked tables did not load
# ---------------------------------------------------------------------------


def _pg_job(status, chunks):
    return MigrationJob(
        job_id="job-1", status=status, chunks=list(chunks),
        watermark=Watermark(snapshot_timestamp=_TS, wal_lsn="0/16B3748",
                            slot_name="dsqlmig_s", table_row_counts={"app.a": 1, "app.b": 1}),
    )


def test_the_slot_is_honoured_when_only_unticked_tables_did_not_load() -> None:
    # FAILS WITHOUT THE FIX: the slot was dropped (a full re-snapshot) and the remedy said
    # "Retry the failed tables first" for a table no longer selected.
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_watermark, pg_resnapshot_reason

    pg = SourceType.POSTGRES
    narrowed = _fc(["app.a"])
    failed_b = _pg_job("FAILED", [_c("app.a"), _c("app.b", "FAILED")])
    assert _cdc_watermark(failed_b, narrowed).slot_name == "dsqlmig_s"
    assert pg_resnapshot_reason(failed_b, source_type=pg, start_mode="auto",
                                migration_state=narrowed) is None
    cancelled = _pg_job("CANCELLED", [_c("app.a"), _c("app.b", "PENDING")])
    assert _cdc_watermark(cancelled, narrowed).slot_name == "dsqlmig_s"
    still_dropped = [
        (_fc(["app.a", "app.b"]), failed_b),  # nothing narrowed
        (narrowed, _pg_job("FAILED", [_c("app.a", "FAILED"), _c("app.b", "FAILED")])),
        (narrowed, _pg_job("FAILED", [_c("app.a", rows_quarantined=2), _c("app.b", "FAILED")])),
        (narrowed, _pg_job("FAILED", [_c("app.a"), _c("app.b")])),  # a run-level failure
        (_fc([]), failed_b),  # nothing captured
    ]
    for state, job in still_dropped:
        assert _cdc_watermark(job, state).slot_name is None
        assert pg_resnapshot_reason(job, source_type=pg, start_mode="auto",
                                    migration_state=state) == "unfinished_load"
    # No state: today's record-wide rule, exactly.
    assert _cdc_watermark(failed_b).slot_name is None
    assert pg_resnapshot_reason(failed_b, source_type=pg, start_mode="auto") == (
        "unfinished_load")


def test_pg_resnapshot_reason_and_pg_start_resnapshots_agree_for_a_narrowed_load() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import (
        _cdc_watermark,
        pg_resnapshot_reason,
        pg_start_resnapshots,
    )

    pg = SourceType.POSTGRES
    jobs = [
        _pg_job("DONE", [_c("app.a"), _c("app.b")]),
        _pg_job("FAILED", [_c("app.a"), _c("app.b", "FAILED")]),
        _pg_job("FAILED", [_c("app.a", "FAILED"), _c("app.b")]),
        _pg_job("CANCELLED", [_c("app.a"), _c("app.b", "PENDING")]),
        _pg_job("FAILED", [_c("app.a"), _c("app.b")]),
    ]
    states = [_fc(["app.a"]), _fc(["app.b"]), _fc(["app.a", "app.b"]),
              _fc(["app.a"], touched=False), None]
    for job in jobs:
        for state in states:
            for mode in ("auto", "manual"):
                reason = pg_resnapshot_reason(job, source_type=pg, start_mode=mode,
                                              migration_state=state)
                resnapshots = pg_start_resnapshots(
                    _cdc_watermark(job, state), source_type=pg, start_mode=mode)
                assert (reason is None) is (not resnapshots), (job.status, mode, reason)


def test_every_cdc_watermark_call_passes_the_migration_state() -> None:
    # The slot rule is only as good as its weakest call site: one that forgets the state
    # re-snapshots where Start would resume (or the card disagrees with the button).
    import ast
    import pathlib

    import dsql_migrator

    seen = {"_cdc_watermark": 0, "pg_resnapshot_reason": 0}
    for path in sorted(pathlib.Path(dsql_migrator.__file__).parent.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name not in seen:
                continue
            seen[name] += 1
            keyword = any(k.arg == "migration_state" for k in node.keywords)
            where = f"{path.name}:{node.lineno}"
            if name == "_cdc_watermark":
                assert len(node.args) == 2 or keyword, where
            else:
                assert keyword, where
    assert seen["_cdc_watermark"] >= 16 and seen["pg_resnapshot_reason"] >= 2


# ---------------------------------------------------------------------------
# 9. Spec 6, the prerequisite pin, a restored session, the copy
# ---------------------------------------------------------------------------


def test_a_removal_never_locks_the_type_switch_and_keeps_todays_completion_notice(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: unticking app.b in Full load only locked the switch to a CDC
    # type ("unselected since: app.b") and dropped the CDC recommendation.
    from dsql_migrator.ui.data_migration import migration_type_lock_reason
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config

    s = _screen(monkeypatch, job=_loaded(), selected=["app.a"])
    s.content(lambda: None)
    assert migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs) is None
    assert any("streams from this Full Load's watermark" in t for t in s.ui.texts)
    s.session.set_migration_type(FLC)
    watermark = s.jobs.jobs["job-1"].watermark
    assert [t.name for t in _cdc_tables_for_config(
        s.state, _inv("app.a", "app.b", "app.c"), watermark)] == ["app.a"]
    # Adding a table still locks, on that table only.
    s.session.set_migration_type(FLO)
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.c"]))
    reason = migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs)
    assert "(selected since: app.c)" in reason and "unselected since" not in reason
    assert "re-run Full Load over the current set" in reason


def test_prerequisite_checks_never_rewrite_a_cdc_selection_after_a_load() -> None:
    # FAILS WITHOUT THE FIX: the post-load re-check pinned the migratable-filtered names,
    # silently dropping a loaded table from the CDC capture.
    for mtype in (FLC, MigrationType.CDC_ONLY):
        state = _flo(["app.a", "app.b"], mtype=mtype)
        assert state.pin_checked_selection(["app.a"], expected=state.selection_snapshot()) is (
            False)
        assert state.selection.selected_tables == ["app.a", "app.b"]
    for state in (_flo(["app.a", "app.b"]),  # Full load only: as before
                  _flo(["app.a", "app.b"], mtype=FLC, job_id=None),  # before any load
                  _flo(["app.a", "app.b"], mtype=FLC, touched=False)):  # nothing explicit
        assert state.pin_checked_selection(["app.a"], expected=state.selection_snapshot())
        assert state.selection.selected_tables == ["app.a"]


def test_a_restored_full_load_and_cdc_session_narrows_like_a_live_one() -> None:
    # The user's report: a restored Full load + CDC session could not untick a table
    # although CDC had never run. FAILS WITHOUT THE FIX (today's lock after the probe).
    from dsql_migrator.ui.data_migration._models import full_load_scope_names
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    def restore(prepare):
        live_session = SessionConnectionState()
        live = DataMigrationState()
        live.bind_session(live_session)
        live.migration_type = FLC
        live.job_id = "job-1"
        live.set_selection(TableSelection(selected_tables=["app.a"]))
        prepare(live)
        snapshot = capture_session_snapshot(
            "s1", live_session, EvaluationState(), SchemaConversionState(), live)
        session, state = SessionConnectionState(), DataMigrationState()
        apply_session_snapshot(snapshot, session, EvaluationState(), SchemaConversionState(),
                               state)
        return state

    jobs = _Jobs({"job-1": _loaded()})
    restored = restore(lambda _live: None)
    assert restored.migration_type is FLC
    assert restored.selection.selected_tables == ["app.a"] and restored.selection_touched
    unprobed = _reason(restored, jobs)
    restored.set_cdc_stack_phase("absent", status=None)  # the probe reported: no stack
    assert _reason(restored, jobs) is None
    from dsql_migrator.ui.data_migration import TABLE_SET_WAIT_CDC_CHECK, cdc_capture_names

    assert unprobed == TABLE_SET_WAIT_CDC_CHECK  # until the probe reports: a wait
    assert cdc_capture_names(restored, jobs.jobs["job-1"].watermark) == {"app.a"}
    assert full_load_scope_names(restored, jobs, []) == ["app.a"]

    # A session that ever pressed Start CDC restores its ops window -- even after a Stop,
    # whose connector names the restore skips: today's lock.
    def stopped(live):
        live.set_cdc_ops_window_start(_TS)
        live.set_cdc_connector_names(["dsql-cdc-x-source"])
        live.set_cdc_deploy_job_id("stop-1", kind="stop")

    after_stop = restore(stopped)
    assert after_stop.cdc_connector_names == [] and after_stop.cdc_ops_window_start == _TS
    after_stop.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    assert _reason(after_stop, jobs) == TODAY


def test_g16_tree_hints(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: no hint (the tree was locked) in Full load + CDC.
    from dsql_migrator.ui.data_migration import TREE_HINT_FL_CDC

    absent = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    assert absent.tree["hint"] == TREE_HINT_FL_CDC and absent.tree["locked"] is False
    assert absent.tree["add_lock_fn"]() is not None
    infra = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], phase="infra")
    assert infra.tree["hint"] == TREE_HINT_FL_CDC and infra.tree["locked"] is False
    assert "table list" not in TREE_HINT_FL_CDC  # no deployed-list restriction
    unprobed = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], phase=None)
    assert unprobed.tree["locked"] is True and unprobed.tree["hint"] is None
    started = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"],
                         render=False)
    started.state.set_cdc_ops_window_start(_TS)
    started.content(lambda: None)
    assert started.tree["locked"] is True and started.tree["lock_reason"] == TODAY
    assert started.tree["add_lock_fn"]() is None
    # Full load only keeps the G12 hint.
    flo = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    flo_tree: dict = {}
    from dsql_migrator.ui import data_migration as dm

    real = dm._render_table_selection
    monkeypatch.setattr(dm, "_render_table_selection",
                        lambda *a, **k: flo_tree.update(k) or real(*a, **k))
    flo.content(lambda: None)
    assert flo_tree["hint"].startswith("A Full Load has run. A table you untick is left as is")
    # Re-read at every click (another tab may switch the type); None for Full load only, so
    # its tree is today's, byte for byte.
    assert flo_tree["add_lock_fn"]() is None


@pytest.mark.parametrize("engine", [SourceType.MYSQL, SourceType.POSTGRES])
def test_the_start_tooltip_names_the_narrowing_until_cdc_starts(engine) -> None:
    # FAILS WITHOUT THE FIX: "This set is fixed" while the table list could still drop.
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_button

    def hover(*, started):
        state = _fc(["app.a"], phase="infra", status="CREATE_COMPLETE")
        if started:
            state.set_cdc_ops_window_start(_TS)
        session = SimpleNamespace(source_config=SimpleNamespace(source_type=engine,
                                                                database="app"))
        ui = _RecordingUi()
        # A PostgreSQL watermark without a slot re-snapshots: the tail must not claim
        # the handoff is gapless there.
        _render_cdc_start_button(ui, state, _Jobs({"job-1": _loaded()}), lambda: None,
                                 inventory=_inv("app.a", "app.b"), session=session)
        return " ".join(ui.tooltips)

    from dsql_migrator.ui.data_migration import START_TOOLTIP_NARROWING

    narrowing = hover(started=False)
    assert narrowing.startswith(START_TOOLTIP_NARROWING)
    # G17 (deliberately changed): no Start over while the set can still change -- a table
    # the snapshot does not hold is loaded on the Full Load step first.
    assert "This set is fixed" not in narrowing and "Start over" not in narrowing
    # G17 review (deliberately changed): not "by Retry unfinished tables" -- a MySQL table
    # the Full Load recorded no start point for is loaded by Re-run all tables instead.
    assert "a table you tick is loaded on the Full Load step first" in narrowing
    assert "the tables loaded under its start point since" in narrowing
    assert ("which is what makes the handoff gapless" in narrowing) is (
        engine is SourceType.MYSQL)
    fixed = hover(started=True)
    assert "This set is fixed" in fixed and "Until you start CDC" not in fixed
    # Started after app.b was unticked: the set is a subset of the snapshot, not equal to
    # it (FAILS WITHOUT THE FIX: "It matches the Full Load snapshot").
    assert "drawn from the Full Load snapshot" in fixed
    assert "matches the Full Load snapshot" not in fixed


def test_the_g16_helpers_stay_linear() -> None:
    import time

    from dsql_migrator.ui.data_migration import (
        TableSetAddLock,
        build_migration_table_tree,
        capture_load_complete,
        cdc_capture_names,
        keep_unlisted_covered,
        narrowed_record_names,
    )

    names = [f"app.t{i}" for i in range(20000)]
    job = _job("FAILED", [_c(n) for n in names[:-1]] + [_c(names[-1], "FAILED")],
               counts={n: 1 for n in names})
    state = _fc(names[:-1])
    inventory = _inv(*names)
    started = time.perf_counter()
    captured = cdc_capture_names(state, job.watermark)
    assert len(captured) == 19999
    assert narrowed_record_names(state, job) == names[:-1]
    assert capture_load_complete(job, captured) is True
    assert keep_unlisted_covered(names, names[:10000], set(names)) == names[10000:]
    # G17 (deliberately changed): the flags come from the rule-held tables.
    lock = TableSetAddLock(covered=frozenset(names[:10000]), rule_held=frozenset(names[10000:]))
    flags = lock.node_flags(names, names[:5000])
    assert len(flags) == 10000
    assert lock.refusal(names[10000:], tree=True).startswith(
        "An exclude table rule unselected app.t10000, app.t10001")
    nodes = build_migration_table_tree(inventory, names, add_locked=flags)
    assert sum(1 for leaf in nodes[0]["children"][0]["children"] if "add_locked" in leaf) == (
        10000)
    assert time.perf_counter() - started < 2.0


# ---------------------------------------------------------------------------
# 10. Review fixes: the step after a re-tick, a detour after a start, a stale tab, a re-tick
#     into a never-started stack, an empty capture, the hints, the copy and the job reads
# ---------------------------------------------------------------------------


def _wire_tree(s):
    """Keep the REAL tree's tick handler of a screen double (``s.ui.tree``)."""
    held: dict = {}

    def tree(nodes=None, *_a, on_tick=None, **_k):
        if on_tick is not None:
            held["on_tick"] = on_tick
        return s.ui._El(s.ui)

    s.ui.tree = tree
    return held


def _rerender(s):
    """A fresh render of a screen double (its texts, buttons and tooltips reset first)."""
    for attr in ("texts", "buttons", "tooltips"):
        getattr(s.ui, attr).clear()
    s.content(lambda: None)


def _step(s):
    return get_status(s.session.workflow, WorkflowStep.FULL_LOAD)


def _continue_enabled(s):
    buttons = [b for b in s.ui.buttons if b.text == "Continue to CDC"]
    return bool(buttons) and all("disable" not in b.props_seen for b in buttons)


def _failed_b_job():
    return _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")], counts={"app.a": 1, "app.b": 1})


def _failed_b_screen(monkeypatch, *, job=None, step=StepStatus.FAILED):
    """Full load + CDC after a FAILED Full Load (app.b did not load), both tables selected,
    the probe reporting no stack -- the real tree's tick handler kept."""
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables", lambda *_a, **_k: {})
    s = _fc_screen(monkeypatch, job=job or _failed_b_job(), selected=["app.a", "app.b"],
                   render=False)
    held = _wire_tree(s)
    s.jobs.errors["job-1"] = "load failed for app.b"
    s.session.set_workflow(with_status(s.session.workflow, WorkflowStep.FULL_LOAD, step))
    if step is StepStatus.FAILED:
        s.state.set_error("load failed for app.b")
    _rerender(s)
    return s, held


@pytest.mark.parametrize("how", ["tick", "select_all", "popup_save"])
def test_ticking_a_failed_table_again_makes_the_step_failed_again(monkeypatch, how) -> None:
    # FAILS WITHOUT THE FIX: the re-tick kept the DONE its untick earned -- Continue to CDC
    # enabled over a table whose load failed (on MySQL, CDC would then seed from the
    # watermark and app.b's unloaded rows would never reach Aurora DSQL).
    from dsql_migrator.ui.data_migration import CONTINUE_HINT_FL_CDC

    s, held = _failed_b_screen(monkeypatch)
    assert _step(s) is StepStatus.FAILED and not _continue_enabled(s)
    held["on_tick"](SimpleNamespace(value=["table:app.a"]))  # untick app.b
    _rerender(s)
    assert _step(s) is DONE and s.state.error is None and _continue_enabled(s)
    if how == "tick":
        held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b"]))
    elif how == "select_all":
        (select_all,) = [b for b in s.ui.buttons if b.text == "Select all"]
        select_all.on_click()
    else:  # what a Change & reload Save that selects app.b again ends with
        s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b"]))
        s.captured["ctx"].refresh()
    # G17 (deliberately changed): Select all also ticks app.c, which the Full Load did not
    # load -- pending its guarded load (the step is Failed for it too).
    assert s.state.selection.selected_tables == (
        ["app.a", "app.b", "app.c"] if how == "select_all" else ["app.a", "app.b"])
    assert _step(s) is StepStatus.FAILED and s.state.error == "load failed for app.b"
    _rerender(s)
    assert _step(s) is StepStatus.FAILED and not _continue_enabled(s)
    assert any(t.startswith("Retry the failed tables before starting CDC")
               and t.endswith(CONTINUE_HINT_FL_CDC) for t in s.ui.texts)
    before = s.session.workflow
    _rerender(s)
    assert s.session.workflow is before  # a steady render writes nothing


def test_a_stale_done_over_a_table_that_did_not_load_is_failed_again_on_render(
        monkeypatch) -> None:
    # A session saved in that state (both tables selected, the step DONE) restores it: its
    # first render re-derives the step from the record. FAILS WITHOUT THE FIX.
    s, _held = _failed_b_screen(monkeypatch, step=DONE)
    assert _step(s) is StepStatus.FAILED and s.state.error == "load failed for app.b"
    assert not _continue_enabled(s)
    # A complete record with every table selected: nothing to re-derive, nothing written.
    whole = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    before = whole.session.workflow
    whole.content(lambda: None)
    assert _step(whole) is DONE and whole.session.workflow is before


def _detour_lock(monkeypatch, *, started):
    """Full load + CDC after a Full Load over app.a/app.b -- Start CDC once submitted (then
    Stop + Delete) when ``started`` -- then the Full load only tile and an untick of app.b
    through the real tree. Returns the screen and the type-switch lock."""
    from dsql_migrator.ui.data_migration import migration_type_lock_reason

    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], render=False)
    held = _wire_tree(s)
    if started:
        s.state.set_cdc_ops_window_start(_TS)
    s.content(lambda: None)
    assert migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs) is None
    s.state.set_migration_type(FLO)  # the Full load only tile
    s.state.set_active_substep(None)
    _rerender(s)
    assert s.tree["locked"] is False  # G12: Full load only, no stack -- unchanged
    held["on_tick"](SimpleNamespace(value=["table:app.a"]))  # untick app.b
    assert s.state.selection.selected_tables == ["app.a"]
    return s, held, migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs)


def test_once_cdc_has_started_a_full_load_only_detour_cannot_narrow_it(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: after Start CDC (then Stop + Delete), the Full load only tile
    # and an untick unlocked the type, and a CDC type then captured app.a alone -- though
    # the set is fixed once CDC has started (a removal locked the switch before G16 too).
    from dsql_migrator.ui.data_migration import (
        COVERAGE_GAP_NOTICE_BODY_STARTED,
        migration_type_lock_reason,
    )

    s, held, reason = _detour_lock(monkeypatch, started=True)
    assert reason == (
        "Locked — the table set changed after this Full Load (unselected since: app.b), and "
        "CDC has already started for this migration: a CDC type cannot continue a set that "
        "differs from the one that run started with. To add CDC, re-run Full Load over the "
        "current set (it takes a fresh watermark), or restore the original table set. Before "
        "that re-run, if this migration's CDC stack still exists, Delete CDC infrastructure and "
        "deploy it again: a stack that has streamed only continues from where it stopped, never "
        "from a newer start point.")  # G25 round 7 (decision A2)
    _rerender(s)  # the completion notice says the same, not "Tables were selected..."
    assert any(t.startswith(COVERAGE_GAP_NOTICE_BODY_STARTED) for t in s.ui.texts)
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b"]))  # restored
    assert migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs) is None
    # Before any Start, the same untick never locks the switch (G16, spec 6).
    _s2, _held2, never_started = _detour_lock(monkeypatch, started=False)
    assert never_started is None


def test_a_restored_session_that_started_cdc_keeps_the_detour_locked() -> None:
    # The same in a restored session: the Stop's connector names are not restored, the
    # Start's ops window is. FAILS WITHOUT THE FIX.
    from dsql_migrator.ui.data_migration import migration_type_lock_reason
    from dsql_migrator.ui.data_migration._models import ExclusionInputs
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    live_session, live = SessionConnectionState(), DataMigrationState()
    live.bind_session(live_session)
    live.migration_type = FLC
    live.job_id = "job-1"
    live.set_selection(TableSelection(selected_tables=["app.a", "app.b"]))
    live.set_cdc_ops_window_start(_TS)
    live.set_cdc_connector_names(["dsql-cdc-x-source"])
    live.set_cdc_deploy_job_id("stop-1", kind="stop")
    snapshot = capture_session_snapshot(
        "s1", live_session, EvaluationState(), SchemaConversionState(), live)
    session, state = SessionConnectionState(), DataMigrationState()
    apply_session_snapshot(snapshot, session, EvaluationState(), SchemaConversionState(), state)
    state.bind_session(session)
    state.set_cdc_stack_phase("absent")  # after Delete CDC infrastructure
    jobs = _Jobs({"job-1": _loaded()})
    selected = {"now": list(state.selection.selected_tables)}
    state.set_exclusion_inputs_resolver(lambda: ExclusionInputs(
        selected=list(selected["now"]), applied={}, preserve_foreign_keys=True,
        migratable=["app.a", "app.b"], signature=tuple(selected["now"])))
    state.set_migration_type(FLO)
    state.set_selection(TableSelection(selected_tables=["app.a"]))
    selected["now"] = ["app.a"]
    reason = migration_type_lock_reason(state, status=DONE, job_manager=jobs)
    assert reason is not None and "(unselected since: app.b)" in reason
    assert "CDC has already started for this migration" in reason


def test_a_stale_full_load_only_tab_reads_the_lock_of_the_current_type(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the add-lock was wired only into a tree RENDERED as Full load +
    # CDC, so a tab rendered as Full load only skipped the Full load + CDC rules after
    # another tab switched the session. G17 (deliberately changed): there, app.c (never
    # loaded) is stored as a pending addition; once CDC has started in the other tab, the
    # stale tab's tick is refused with today's lock.
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    s.state.set_cdc_stack_phase("absent")
    held = _wire_tree(s)
    s.ui.timer = lambda interval, callback=None, *a, **k: s.ui._El(s.ui)
    s.content(lambda: None)  # rendered as Full load only
    (select_all,) = [b for b in s.ui.buttons if b.text == "Select all"]
    s.session.set_migration_type(FLC)  # another tab
    s.ui.notified.clear()
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b", "table:app.c"]))
    assert s.state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert s.ui.notified == []
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b"]))
    assert s.state.selection.selected_tables == ["app.a", "app.b"]
    s.state.set_cdc_ops_window_start(_TS)  # the other tab started CDC
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b", "table:app.c"]))
    assert s.state.selection.selected_tables == ["app.a", "app.b"]
    ((note, kw),) = s.ui.notified
    assert kw.get("type") == "warning"
    assert note == f"{TODAY} This change was not applied."
    select_all.on_click()
    assert s.state.selection.selected_tables == ["app.a", "app.b"]


def _three_loaded():
    return _job("DONE", [_c("app.a"), _c("app.b"), _c("app.c")],
                counts={"app.a": 1, "app.b": 1, "app.c": 1})


def test_a_covered_table_ticks_back_into_a_never_started_stack() -> None:
    # FAILS WITHOUT THE FIX: refused ("it would stream on a single Kafka partition ...
    # delete the CDC infrastructure"), which is false -- a never-started stack has no
    # topics yet, and Start creates each from the deployed plan (the premise test below).
    from dsql_migrator.ui.data_migration import cdc_capture_names

    jobs = _Jobs({"job-1": _three_loaded()})
    state = _fc(["app.a", "app.b"], phase="infra", status="CREATE_COMPLETE")
    state.set_cdc_reconciled_table_names(["app.a", "app.b"])  # deployed after app.c's untick
    ui, _changes, refreshed = _render_tree(state, jobs, ["app.a", "app.b", "app.c"])
    leaves = {leaf["id"]: leaf for leaf in ui.nodes[0][0]["children"][0]["children"]}
    assert "add_locked" not in leaves["table:app.c"]
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:app.a", "table:app.b", "table:app.c"]))
    assert ui.notes == [] and refreshed == []
    assert state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert cdc_capture_names(state, jobs.jobs["job-1"].watermark) == {
        "app.a", "app.b", "app.c"}


def test_start_creates_a_ticked_back_topic_from_the_deployed_partition_plan() -> None:
    # Why the re-tick above is safe (a premise, no fix): the first Start pre-creates every
    # SinkTopic from the plan the never-started stack was deployed with.
    from dsql_migrator.core.cdc import cdc_scaling_params
    from dsql_migrator.core.cdc_kafka_prep import parse_partitions_map, plan_topics

    plan = dict(cdc_scaling_params(["app.a", "app.b"], "pfx", env={}))
    specs = plan_topics(
        offset_topic="off", sink_topics=["pfx.app.a", "pfx.app.b", "pfx.app.c"],
        default_partitions=plan["TopicDefaultPartitions"],
        partitions_map=parse_partitions_map(plan["SinkTopicPartitions"]))
    assert {spec.name: spec.partitions for spec in specs if spec.name != "off"} == {
        "pfx.app.a": 4, "pfx.app.b": 4, "pfx.app.c": 4}


def _seedable(job):
    """``job`` with a MySQL binlog start point on its watermark (Start CDC is ready)."""
    job.watermark = job.watermark.model_copy(
        update={"binlog_file": "mysql-bin.000001", "binlog_position": 4})
    return job


_MYSQL = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL,
                                                       database="app"))


@pytest.mark.parametrize("selected, empty", [(["app.a"], False), ([], True)])
def test_an_empty_narrowed_capture_disables_start_cdc_before_the_click(selected, empty) -> None:
    # FAILS WITHOUT THE FIX: "Ready to start CDC" and an enabled primary Start CDC beside
    # "Will stream no tables" -- the click ran the dialog's probes, then refused in a
    # notice the closing dialog took with it.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import WILL_STREAM_NONE
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_button

    state = _fc(selected, phase="infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)  # the run passed the CDC checks
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": _seedable(_loaded())}), lambda: None,
                             inventory=_inv("app.a", "app.b"), session=_MYSQL)
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert ("Ready to start CDC" in ui.texts) is (not empty)
    assert ("disable" in start.props_seen) is empty
    assert (WILL_STREAM_NONE in ui.tooltips) is empty
    assert (WILL_STREAM_NONE in ui.texts) is empty


def test_an_empty_capture_whose_table_list_is_locked_names_the_route_that_works() -> None:
    # FAILS WITHOUT THE FIX: CDC only (reached by unticking everything in Full load only and
    # switching the type -- a removal no longer locks that switch) showed "Tick at least one
    # in the table list" beside a LOCKED table list: a dead end.
    from dsql_migrator.ui.data_migration import (
        WILL_STREAM_NONE,
        WILL_STREAM_NONE_LOCKED_LIST,
        will_stream_none,
    )

    jobs = _Jobs({"job-1": _seedable(_loaded())})
    # Full load + CDC, nothing selected, CDC never started: the list is the remedy.
    assert will_stream_none(_fc([], phase="infra", status="CREATE_COMPLETE"), jobs) == (
        WILL_STREAM_NONE
    )
    # CDC only over the same record: its list cannot change, so name the type switch.
    cdc_only = _flo([], touched=True, mtype=MigrationType.CDC_ONLY, job_id="job-1")
    cdc_only.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert will_stream_none(cdc_only, jobs) == WILL_STREAM_NONE_LOCKED_LIST
    assert "Full load + CDC" in WILL_STREAM_NONE_LOCKED_LIST
    # Full load + CDC once CDC has started: the set is fixed again, so not the list either.
    started = _fc([], phase="running", status="UPDATE_COMPLETE")
    assert will_stream_none(started, jobs) == WILL_STREAM_NONE_LOCKED_LIST


def test_the_deployed_infra_lock_does_not_claim_a_single_partition() -> None:
    # Pre-existing copy, found while fixing G16's own version of it: a topic absent from
    # SinkTopicPartitions takes TopicDefaultPartitions, which in the common UNIFORM plan is
    # every table's count -- so "would stream on a single partition" was false there. It is
    # only the plan's LOWEST count (compute_cdc_partition_plan gives some tables more).
    state = _fc(["app.a"], phase="infra", status="CREATE_COMPLETE")
    reason = _reason(state, _Jobs({}), has_job=False, status=StepStatus.NOT_STARTED)
    assert reason is not None
    assert "single partition" not in reason
    assert "lowest partition count" in reason
    assert "sized for the tables selected then" in reason
    assert "delete the CDC infrastructure on the CDC step first" in reason


def test_the_source_config_card_points_an_empty_capture_at_the_table_list() -> None:
    # FAILS WITHOUT THE FIX: it sent the user to Schema Conversion, which cannot help after
    # a Full Load (only the snapshot's tables can be captured).
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_source_config_card

    from tests.test_ui_data_migration import _ScreenUi

    def texts(job):
        ui = _ScreenUi()
        _render_cdc_source_config_card(
            ui, _fc([], phase="infra", status="CREATE_COMPLETE"), _Jobs({"job-1": job}),
            lambda: None, inventory=_inv("app.a", "app.b"), migration_type=FLC,
            session=_MYSQL)
        return ui.texts

    narrowed = texts(_seedable(_loaded()))
    assert "Select at least one table before starting CDC" in narrowed
    assert ("None of the tables the Full Load loaded is selected, so there is nothing for "
            "the CDC sink to write. Tick at least one of them in the table list, then "
            "return here to start CDC.") in narrowed
    # Without per-table counts nothing was narrowed: today's text.
    plain = texts(_seedable(_job("DONE", [_c("app.a"), _c("app.b")], counts={})))
    assert any(t.startswith("No tables are selected for replication yet") for t in plain)


def test_deploy_cdc_infrastructure_is_disabled_before_the_click_for_an_empty_capture(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: Deploy stayed the enabled primary action after Unselect all;
    # only the dialog's final click answered, with a toast.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import DEPLOY_EMPTY_CAPTURE

    s = _fc_screen(monkeypatch, job=_seedable(_loaded()), selected=["app.a", "app.b"],
                   render=False)
    s.state.set_prereq_gated_mode(MigrationMode.CDC)
    _rerender(s)

    def deploy():
        return [b for b in s.ui.buttons if b.text == "Deploy CDC infrastructure"]

    assert deploy() and all("disable" not in b.props_seen for b in deploy())
    assert DEPLOY_EMPTY_CAPTURE not in s.ui.texts
    (unselect_all,) = [b for b in s.ui.buttons if b.text == "Unselect all"]
    unselect_all.on_click()
    _rerender(s)
    assert deploy() and all("disable" in b.props_seen for b in deploy())
    assert DEPLOY_EMPTY_CAPTURE in s.ui.tooltips and DEPLOY_EMPTY_CAPTURE in s.ui.texts


_RETRY_BASE = ("Retry the failed tables before starting CDC -- streaming resumes from the "
               "snapshot, so it must not start on partial data.")


@pytest.mark.parametrize("job_status", ["FAILED", "CANCELLED"])
@pytest.mark.parametrize("selected", [["app.a", "app.b"], ["app.a"]])
def test_a_run_level_failure_never_offers_the_untick_route(monkeypatch, job_status,
                                                           selected) -> None:
    # FAILS WITHOUT THE FIX: every table loaded (a post-load pass raised, or a Stop or a
    # restart after the last one), yet the hint offered "untick them" -- which leaves the
    # step Failed and only drops the table from CDC.
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables", lambda *_a, **_k: {})
    job = _job(job_status, [_c("app.a"), _c("app.b")], counts={"app.a": 1, "app.b": 1})
    s = _fc_screen(monkeypatch, job=job, selected=selected, render=False)
    s.jobs.errors["job-1"] = "RuntimeError: a post-load pass raised"
    s.session.set_workflow(
        with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    _rerender(s)
    assert _step(s) is StepStatus.FAILED  # whatever is unticked
    assert [t for t in s.ui.texts if t.startswith(_RETRY_BASE)] == [_RETRY_BASE]


@pytest.mark.parametrize("chunk", [_c("app.b", "FAILED"), _c("app.b", rows_quarantined=2)],
                         ids=["failed", "quarantined"])
def test_the_untick_route_is_offered_where_unticking_settles_the_step(monkeypatch,
                                                                      chunk) -> None:
    from dsql_migrator.ui.data_migration import CONTINUE_HINT_FL_CDC

    job = _job("FAILED", [_c("app.a"), chunk], counts={"app.a": 1, "app.b": 3})
    s, _held = _failed_b_screen(monkeypatch, job=job)
    assert [t for t in s.ui.texts if t.startswith(_RETRY_BASE)] == [
        _RETRY_BASE + CONTINUE_HINT_FL_CDC]
    unticked, _held2 = _failed_b_screen(monkeypatch, job=job)
    unticked.state.set_selection(TableSelection(selected_tables=["app.a"]))
    _rerender(unticked)
    assert _step(unticked) is DONE  # and it is true


def _unprobed(state, jobs):
    state.cdc_stack_phase, state.cdc_stack_phase_checked = None, False
    return state, jobs


@pytest.mark.parametrize("evidence", [
    _ops_window, _connectors, _committed, _cdc_job("start"), _no_counts, _pruned, _unprobed,
], ids=lambda f: getattr(f, "__name__", "x"))
def test_cdc_only_keeps_todays_lock_text_where_full_load_and_cdc_could_not_drop_tables(
        evidence) -> None:
    from dsql_migrator.ui.data_migration import table_set_editable_after_load

    state, jobs = evidence(*_cdc_only(_fc(), _Jobs({"job-1": _loaded()})))
    assert table_set_editable_after_load(state, jobs) is False
    has_job = "job-1" in jobs.jobs
    assert _reason(state, jobs, mtype=MigrationType.CDC_ONLY, has_job=has_job) == TODAY


def test_cdc_only_names_the_full_load_and_cdc_route_while_it_can_still_drop_tables(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: CDC only named only Start over (which discards Evaluation, the
    # conversions and the CDC inputs), though Full load + CDC can still drop the tables.
    from dsql_migrator.ui.data_migration import (
        CDC_ONLY_NARROWING_SENTENCE,
        table_set_add_lock,
        table_set_editable_after_load,
    )

    state, jobs = _cdc_only(_fc(), _Jobs({"job-1": _loaded()}))
    assert table_set_editable_after_load(state, jobs) is False  # CDC only keeps its lock
    assert table_set_add_lock(state, jobs) is None
    assert _reason(state, jobs, mtype=MigrationType.CDC_ONLY) == (
        TODAY + CDC_ONLY_NARROWING_SENTENCE)
    # Not while the type is locked (an infra create in flight).
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    jobs.jobs["infra-1"] = _job("RUNNING", job_id="infra-1")
    assert _reason(state, jobs, mtype=MigrationType.CDC_ONLY) == TODAY
    # The table list's lock icon says it, and the route works.
    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], render=False)
    s.session.set_migration_type(MigrationType.CDC_ONLY)
    s.content(lambda: None)
    assert s.tree["locked"] is True
    assert s.tree["lock_reason"] == TODAY + CDC_ONLY_NARROWING_SENTENCE
    s.session.set_migration_type(FLC)
    s.content(lambda: None)
    assert s.tree["locked"] is False


@pytest.mark.parametrize("unread", [False, True], ids=["check", "unread"])
def test_a_waiting_table_line_never_says_what_save_stores(monkeypatch, unread) -> None:
    # FAILS WITHOUT THE FIX: under an E3 that disabled Save, "Save stores the rules and the
    # table selection. Unselects app.b: ... not captured by CDC" still promised the Save.
    s = _reload_screen(monkeypatch, mtype=FLC)  # the probe has not reported
    if unread:
        s.state.set_cdc_probe_error("AccessDenied: cloudformation:DescribeStacks")
    ui = _open_reload(s)
    start = len(ui.texts)
    ui.type_text("exclude table app.c\nexclude table app.b")
    shown = ui.texts[start:]
    assert any("This rule would change which tables migrate (app.b)" in t for t in shown)
    assert not _action(ui).enabled
    assert not any("Unselects app.b" in t for t in shown)
    assert not any("Save stores the rules and the table selection" in t for t in shown)


def test_the_cdc_started_change_and_reload_sentence_is_todays(monkeypatch) -> None:
    s = _reload_screen(monkeypatch, mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    s.state.set_cdc_ops_window_start(_TS)
    ui = _open_reload(s)
    ui.type_text("exclude table app.c\nexclude table app.b")
    assert any("Nothing to drop and reload: Save stores the rules and the table selection. "
               "Unselects app.b: left as is on Aurora DSQL (not dropped), and its "
               "excluded-column entries are not kept." in t for t in ui.texts)


def _job_reads(s, run):
    """How many times ``run()`` reads the Full Load record (each read deep-copies it in the
    real JobManager.get_status)."""
    reads: list = []
    real = s.jobs.get_status

    def counting(job_id):
        if job_id == "job-1":
            reads.append(job_id)
        return real(job_id)

    s.jobs.get_status = counting
    try:
        run()
    finally:
        s.jobs.get_status = real
    return len(reads)


@pytest.mark.parametrize("state, bound", [
    ("streaming", 10), ("stopped", 9), ("absent", 20), ("infra", 19)])
def test_a_full_load_and_cdc_render_reads_the_job_record_a_bounded_number_of_times(
        monkeypatch, state, bound) -> None:
    # FAILS WITHOUT THE FIX (14 / 13 / 24 / 23): once CDC started, the narrowing predicate
    # still read the record first -- every render copying it for nothing; HEAD read it 10
    # and 9 times there. The open states stay at or under Full load only's (23).
    phase, status = {"streaming": ("running", "UPDATE_COMPLETE"),
                     "stopped": ("infra", "UPDATE_COMPLETE"), "absent": ("absent", None),
                     "infra": ("infra", "CREATE_COMPLETE")}[state]
    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], render=False)
    s.state.set_cdc_stack_phase(phase, status=status)
    if state in ("streaming", "stopped"):
        s.state.set_cdc_ops_window_start(_TS)
    if state == "streaming":
        s.state.cdc_connector_names = ["dsql-cdc-x-source", "dsql-cdc-x-sink"]
    s.content(lambda: None)  # warm: the first render may sync the step
    assert _job_reads(s, lambda: s.content(lambda: None)) <= bound


def test_a_change_and_reload_preview_reads_the_job_record_a_bounded_number_of_times(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (8 / 12): the add-lock read the record twice, and two resolves of
    # one preview each asked for the lock.
    s = _reload_screen(monkeypatch, mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    ui = _open_reload(s)
    assert _job_reads(s, lambda: ui.type_text("exclude table app.b")) <= 6
    assert _job_reads(s, lambda: ui.type_text("exclude app.*.note")) <= 10


def test_change_and_reload_save_keeps_a_loaded_table_the_popup_cannot_list(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (keep_selected dropped anywhere on the Save path): app.h -- loaded
    # by the Full Load but no longer listed (no generated DDL) -- left the stored selection,
    # and so the CDC capture, though only app.b was unselected.
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config

    s = _reload_screen(monkeypatch, record=("app.a", "app.b", "app.h"),
                       selection=("app.a", "app.b", "app.h"), unlisted=("app.h",), mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    assert "app.h" not in s.ctx.inputs_fn().migratable
    assert "app.h" in s.ctx.add_lock_fn().covered
    assert [t.name for t in s.ctx.capture_tables_fn(["app.a"])] == ["app.a", "app.h"]
    ui = _open_reload(s)
    ui.type_text("exclude table app.c\nexclude table app.b")
    ui.click("Save")
    assert s.calls == []  # nothing dropped or reloaded
    assert s.state.selection.selected_tables == ["app.a", "app.h"]
    inventory = s.ctx.inventory_fn()
    assert [t.name for t in _cdc_tables_for_config(
        s.state, inventory, s.jobs.jobs["job-1"].watermark)] == ["app.a", "app.h"]


def test_an_untick_and_select_all_keep_a_ticked_leaf_that_carries_a_lock_code() -> None:
    # A ticked table the lock would refuse (app.x: stored, listed, not in the snapshot) is
    # never flagged, so an untick elsewhere and Select all both keep it.
    jobs = _Jobs({"job-1": _loaded()})

    def render():
        state = _fc(["app.a", "app.b", "app.x"])
        return state, _render_tree(state, jobs, ["app.a", "app.b", "app.x"])

    state, (ui, _changes, refreshed) = render()
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:app.a", "table:app.x"]))  # untick app.b
    assert state.selection.selected_tables == ["app.a", "app.x"]
    assert ui.notes == [] and refreshed == []
    state, (ui, _changes, _refreshed) = render()
    ui.clicks["Select all"]()
    assert state.selection.selected_tables == ["app.a", "app.b", "app.x"] and ui.notes == []


def test_the_g16_guards_hold_at_their_edges() -> None:
    from dsql_migrator.ui.data_migration import (
        R1_FL_CDC_SENTENCE,
        capture_load_complete,
        keep_unlisted_covered,
    )
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclusion_lock_state
    from dsql_migrator.ui.data_migration._models import full_load_scope_names

    jobs = _Jobs({"job-1": _loaded()})
    # A failed create shows its own wait in the table list: R1 must not invite an untick.
    failed = exclusion_lock_state(
        _with_session(_fc(phase="unstable", status="ROLLBACK_COMPLETE")), jobs)
    assert failed.cell == "R1" and R1_FL_CDC_SENTENCE not in failed.reason
    # The slot is honoured only for a settled record whose captured tables all loaded.
    assert capture_load_complete(
        _job("RUNNING", [_c("app.a"), _c("app.b", "FAILED")]), {"app.a"}) is False
    assert capture_load_complete(_job("FAILED", [_c("app.b", "FAILED")]), {"app.a"}) is False
    # No per-table counts: the views read the record as is.
    no_counts = _Jobs({"job-1": _job("DONE", [_c("app.a"), _c("app.b")], counts={})})
    assert full_load_scope_names(_fc(["app.a"]), no_counts, []) is None
    # Only covered names are kept past a write.
    assert keep_unlisted_covered(["app.a", "app.x", "app.h"], ["app.a"], {"app.a", "app.h"}) == [
        "app.h"]
