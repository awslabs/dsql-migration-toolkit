# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G131: the CDC table-list ceiling is a warning before a Full load + CDC load and a hold at
Deploy CDC infrastructure and Start CDC; a deploy without recorded row estimates is judged on
the estimates it reads at the click.

G128 held Start Full Load whenever the whole load selection, taken as the CDC capture, did not
fit one CloudFormation parameter. That blocked the documented route (manual §13.7, G16): load
every table, then untick before CDC starts the tables CDC does not need. And with no row
estimates recorded, G128 judged the deploy at its largest partition plan. Evenly sized
captures between about 4,097 / (L + 11) and 4,097 / (L + 9) tables were therefore refused,
although the deploy would send no partition map; for CDC only the refusal never lifted.
Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import sys
import types
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import (
    list_parameter_size_refusal,
    oversized_table_list,
)
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType, StepStatus, Watermark
from dsql_migrator.core.table_selection import TableSelection
from dsql_migrator.ui.data_migration import (
    MigrationType,
    data_migration_exclusion_block,
    pre_load_table_list_warning,
)
from dsql_migrator.ui.data_migration._models import (
    CDC_TABLE_LIST_HEADER,
    CDC_TABLE_LIST_WORST_CASE_HEADER,
    cdc_partition_map_worst_case,
    cdc_pre_load_table_list_warning,
    cdc_table_list_refusal,
)

from tests.test_g12_table_set_after_load import _Jobs
from tests.test_g128_deploy_review import (
    _band,
    _infra,
    _inventory,
    _skewed,
    _start,
    _state,
)

FLC = MigrationType.FULL_LOAD_AND_CDC
_ROUTES = (
    "The Full Load can still load every selected table: after it, until CDC starts, untick "
    "in the table list the tables CDC does not need to capture (they stay on Aurora DSQL as "
    "loaded; CDC and Validation skip them), or use Full load only. Or capture fewer before "
    "the load: "
)


def _even(names):
    return {name: 1_000 for name in names}


def _loaded(names, counts):
    """A finished Full load + CDC record whose watermark covers ``names`` with ``counts``."""
    from datetime import datetime, timezone

    return MigrationJob(
        job_id="job-1",
        status="DONE",
        chunks=[ChunkState(chunk_id=n, status="DONE", attempts=1) for n in names],
        watermark=Watermark(
            binlog_file="mysql-bin.000042",
            binlog_position=120,
            snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
            table_row_counts=dict(counts),
        ),
    )


# ---------------------------------------------------------------------------
# 1. The judgement: exactly what the builders send, and the worst case said as such
# ---------------------------------------------------------------------------


def test_the_largest_oversized_list_is_named_with_its_size() -> None:
    assert oversized_table_list([]) is None
    assert oversized_table_list([("SinkTopics", "s" * 4096)]) is None
    assert oversized_table_list([
        ("TableIncludeList", "t" * 4097), ("SinkTopics", "s" * 4500),
        ("ColumnExcludeList", "c" * 9000),  # not a per-table list
    ]) == ("SinkTopics", 4500)
    assert oversized_table_list([("SinkTopicPartitions", "é" * 2049)]) == (
        "SinkTopicPartitions", 4098)


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_a_deploy_is_judged_on_the_counts_it_plans_from_like_the_builders(engine) -> None:
    # FAILS WITHOUT THE FIX: no counts meant the largest plan, so the 165-table band was
    # refused although the builders send no partition map for it.
    names = _band()
    for counts in (None, _even(names), _skewed(names)):
        assert cdc_table_list_refusal(names, deploy=True, row_counts_by_table=counts) == (
            list_parameter_size_refusal(_infra(engine, names, counts).filled)
        ), counts
    assert cdc_table_list_refusal(names, deploy=True) is None
    assert cdc_table_list_refusal(names, deploy=True, row_counts_by_table=_even(names)) is None
    assert "(SinkTopicPartitions)" in cdc_table_list_refusal(
        names, deploy=True, row_counts_by_table=_skewed(names))
    # Start sends the two lists only.
    assert cdc_table_list_refusal(names, deploy=False) == (
        list_parameter_size_refusal(_start(engine, names).filled)) is None


def test_the_worst_case_names_the_partition_map_and_how_many_tables_fit(monkeypatch) -> None:
    monkeypatch.delenv("DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS", raising=False)
    names = _band()
    worst = cdc_partition_map_worst_case(names)
    assert worst == (
        "If one of these tables holds at least twice their average row count, the deploy "
        "gives it more partitions and sends a partition count for every table: 4,289 bytes in "
        "the CDC configuration (SinkTopicPartitions), over CloudFormation's 4,096-byte limit "
        "for one parameter (about 157 of these tables fit)."
    )
    # "about 157" is what the builders really carry for such names, skewed.
    assert list_parameter_size_refusal(
        _infra("mysql", names[:157], _skewed(names[:157])).filled) is None
    assert list_parameter_size_refusal(
        _infra("mysql", names[:158], _skewed(names[:158])).filled) is not None
    # Lists every deploy sends that do not fit: a refusal, not a worst case.
    assert cdc_partition_map_worst_case(_band(300)) is None
    assert cdc_partition_map_worst_case(names[:100]) is None
    # A fixed partition count: the deploy never sends a map.
    monkeypatch.setenv("DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS", "2")
    assert cdc_partition_map_worst_case(names) is None


def test_the_pre_load_warning_names_the_ceiling_and_the_routes(monkeypatch) -> None:
    monkeypatch.delenv("DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS", raising=False)
    many = _band(300)
    assert cdc_pre_load_table_list_warning(many, planned=False, table_lock=None) == (
        "The 300 selected tables need 7,199 bytes in the CDC configuration (SinkTopics), over "
        "CloudFormation's 4,096-byte limit for one parameter: one CDC pipeline captures about "
        "157 of them (170 if none holds twice their average row count), so Deploy CDC "
        "infrastructure and Start CDC refuse this set. " + _ROUTES
        + "Deselect tables in the table picker."
    )
    # The two figures are what the builders carry: 157 skewed, 170 even (or with the map
    # already deployed, Start's two lists).
    for n, counts, fits in ((157, _skewed, True), (158, _skewed, False),
                            (170, _even, True), (171, _even, False)):
        refusal = list_parameter_size_refusal(_infra("mysql", many[:n], counts(many[:n])).filled)
        assert (refusal is None) is fits, (n, counts)
    # A stack that already owns its plan: only Start's two lists count.
    planned = cdc_pre_load_table_list_warning(many, planned=True, table_lock="Locked — X.")
    assert "captures about 170 of them, so Start CDC refuses this set." in planned
    assert planned.endswith("Or capture fewer before the load: The table picker is locked. "
                            "Locked — X.")
    # The band: only the worst case of the deploy's plan can be said before the load.
    band = cdc_pre_load_table_list_warning(_band(), planned=False, table_lock=None)
    assert band.startswith(cdc_partition_map_worst_case(_band()) + " Deploy CDC "
                           "infrastructure would then refuse this set. " + _ROUTES)
    assert cdc_pre_load_table_list_warning(_band(), planned=True, table_lock=None) is None
    assert cdc_pre_load_table_list_warning(_band()[:100], planned=False, table_lock=None) is None


# ---------------------------------------------------------------------------
# 2. Before a Full load + CDC load: a warning, never a hold
# ---------------------------------------------------------------------------


def test_start_full_load_is_not_held_for_a_selection_one_pipeline_cannot_capture() -> None:
    # FAILS WITHOUT THE FIX: Continue, Start Full Load, the header Run and runner() all
    # refused this load ("Too many tables for one CDC pipeline").
    from dsql_migrator.ui.data_migration import full_load_run_guard_reason

    names = _band(300)
    state = _state(names)
    jobs = _Jobs({})
    assert data_migration_exclusion_block(state, jobs, None, _inventory(names)) is None
    assert full_load_run_guard_reason(
        state, _inventory(names), has_run=True, exclusion_block=None) is None
    # It is said instead, as a warning with the ceiling and the routes.
    warning = pre_load_table_list_warning(state, jobs, None, _inventory(names))
    assert warning.startswith("The 300 selected tables need 7,199 bytes")
    assert warning.endswith(_ROUTES + "Deselect tables in the table picker.")


def test_the_pre_load_warning_follows_the_state() -> None:
    names = _band(300)
    jobs = _Jobs({})
    inv = _inventory(names)
    # A stack deployed before the load owns the plan: Start's lists; the picker is locked.
    deployed = pre_load_table_list_warning(_state(names, phase="infra"), jobs, None, inv)
    assert "so Start CDC refuses this set." in deployed
    assert "Or capture fewer before the load: The table picker is locked. Locked — CDC " \
           "infrastructure is deployed" in deployed
    assert pre_load_table_list_warning(
        _state(_band(), phase="infra"), jobs, None, _inventory(_band())) is None
    # Not before a Full load + CDC load: nothing to say here.
    for mtype in (MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY):
        assert pre_load_table_list_warning(_state(names, mtype), jobs, None, inv) is None, mtype
    loaded = _state(names)
    loaded.job_id = "job-1"
    assert pre_load_table_list_warning(loaded, jobs, None, inv) is None
    # CDC already started for this migration: its set is fixed, the CDC step's holds say so.
    from datetime import datetime, timezone

    started = _state(names)
    started.set_cdc_ops_window_start(datetime(2026, 10, 1, tzinfo=timezone.utc))
    assert pre_load_table_list_warning(started, jobs, None, inv) is None
    assert pre_load_table_list_warning(_state(names), jobs, None, None) is None


def test_the_documented_route_loads_every_table_and_narrows_before_cdc_starts() -> None:
    # FAILS WITHOUT THE FIX at its first step: the 300-table load was refused.
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    names = _band(300)
    inv = _inventory(names)
    assert data_migration_exclusion_block(_state(names), _Jobs({}), None, inv) is None
    # After the load, the capture is the covered tables still selected: all 300 are held at
    # Deploy (the recorded estimates are skewed), with the untick remedy G16 allows ...
    jobs = _Jobs({"job-1": _loaded(names, _skewed(names))})
    after = _state(names)
    after.job_id = "job-1"
    held = _deploy_block_notice(after, jobs, inv, None)
    assert held is not None and held[0] == CDC_TABLE_LIST_HEADER
    assert held[1].endswith("Deselect tables in the table picker.")
    # ... and the 100 that change during the migration window fit.
    narrowed = _state(names[:100])
    narrowed.job_id = "job-1"
    assert _deploy_block_notice(narrowed, jobs, inv, None) is None
    assert data_migration_exclusion_block(narrowed, jobs, None, inv) is None
    assert pre_load_table_list_warning(narrowed, jobs, None, inv) is None


def _render_full_load_card(monkeypatch, *, job, warning):
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_g12_table_set_after_load import _VerifiedSession
    from tests.test_ui_data_migration import _ConfirmDialogUi

    notices: list = []
    real_notice = fl.render_notice

    def _notice(ui, *, tone, header, body="", **k):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    monkeypatch.setattr(fl, "render_notice", _notice)
    monkeypatch.setattr(fl, "_render_full_load_progress", lambda *a, **k: None)
    names = _band(300)
    state = _state(names)
    if job is not None:
        state.job_id = job.job_id
    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, _Jobs({} if job is None else {job.job_id: job}), _VerifiedSession(),
        job=job, status=StepStatus.NOT_STARTED if job is None else StepStatus.DONE,
        selected_names=names, guard_reason=None, pre_load_warning=warning,
        start_full_load=noop, retry_failed_load=noop, reload_table=noop,
        accept_quarantine_and_continue=noop, stop_full_load=noop, refresh=noop,
        retry_tables=noop, schema_recreate_candidates=lambda: [],
    )
    return ui, notices


def test_the_full_load_card_warns_above_start_full_load(monkeypatch) -> None:
    ui, notices = _render_full_load_card(monkeypatch, job=None, warning="W")
    assert ("warning", CDC_TABLE_LIST_HEADER, "W") in notices
    assert "Start Full Load" in ui.buttons
    # Once a load ran the CDC step's holds speak instead.
    names = _band(300)
    _ui, after = _render_full_load_card(
        monkeypatch, job=_loaded(names, _skewed(names)), warning="W")
    assert not [n for n in after if n[1] == CDC_TABLE_LIST_HEADER]


def test_the_screen_hands_the_warning_to_the_full_load_card() -> None:
    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    (step,) = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "_render_full_load_step"]
    (kw,) = [k for k in step.keywords if k.arg == "pre_load_warning"]
    assert ast.unparse(kw.value) == "_table_list_warning"
    (assign,) = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                 and any(getattr(t, "id", None) == "_table_list_warning" for t in n.targets)]
    assert ast.unparse(assign.value.func) == "pre_load_table_list_warning"
    # The gate every entry point reads no longer judges the table lists.
    assert "cdc_table_list_refusal" not in inspect.getsource(dm.data_migration_exclusion_block)


# ---------------------------------------------------------------------------
# 3. Deploy CDC infrastructure: the button holds what is certain, warns of the worst case
# ---------------------------------------------------------------------------


def _deploy_ready(names, mtype=MigrationType.CDC_ONLY, job_id=None):
    from tests.test_ui_data_migration import _cdc_state_ready_for_deploy

    state = _cdc_state_ready_for_deploy()
    state.migration_type = mtype
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))
    state.set_selection(TableSelection(selected_tables=list(names)))
    state.set_cdc_stack_phase("absent")
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0123456789abcdef0"})
    state.job_id = job_id
    return state


def _render_deploy(monkeypatch, state, jobs, inv):
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_ui_data_migration import _GateUi

    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice",
        lambda ui, **kw: notices.append((kw.get("tone"), kw.get("header"), kw.get("body")))
        or real(ui, **kw),
    )
    ui = _GateUi()
    _cdc_ui._render_cdc_infra_deploy_action(
        ui, state, jobs, lambda: None, inventory=inv, session=None)
    return ui, notices


@pytest.mark.parametrize("mtype", [MigrationType.CDC_ONLY, FLC])
def test_an_even_capture_in_the_band_is_not_held_without_estimates(monkeypatch, mtype) -> None:
    # FAILS WITHOUT THE FIX: Deploy was disabled with "need 4,289 bytes ...
    # (SinkTopicPartitions)" -- for CDC only for good, since no Full Load records estimates.
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    monkeypatch.delenv("DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS", raising=False)
    names = _band()
    inv = _inventory(names)
    state = _deploy_ready(names, mtype)
    assert _deploy_block_notice(state, _Jobs({}), inv, None) is None
    ui, notices = _render_deploy(monkeypatch, state, _Jobs({}), inv)
    assert ui.deploy_button().enabled is True
    (hit,) = [n for n in notices if n[1] == CDC_TABLE_LIST_WORST_CASE_HEADER]
    assert hit[0] == "warning"
    assert hit[2] == cdc_partition_map_worst_case(names) + (
        " No row estimates are recorded for them yet, so Deploy CDC infrastructure reads the "
        "source's when you click it, and refuses before it creates anything if its plan does "
        "not fit.")
    # The sysbench-style capture of the verifier: 160 equal tables.
    sysbench = [f"sbtest.sbtest{i}" for i in range(1, 161)]
    assert _deploy_block_notice(
        _deploy_ready(sysbench), _Jobs({}), _inventory(sysbench), None) is None


def test_recorded_estimates_are_judged_exactly_and_a_certain_overflow_is_held(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    names = _band()
    inv = _inventory(names)
    # Skewed estimates recorded by the Full Load: the map is certain, so the button holds.
    state = _deploy_ready(names, FLC, job_id="job-1")
    skewed = _Jobs({"job-1": _loaded(names, _skewed(names))})
    held = _deploy_block_notice(state, skewed, inv, None)
    assert held[0] == CDC_TABLE_LIST_HEADER and "(SinkTopicPartitions)" in held[1]
    ui, notices = _render_deploy(monkeypatch, state, skewed, inv)
    assert ui.deploy_button().enabled is False
    assert not [n for n in notices if n[1] == CDC_TABLE_LIST_WORST_CASE_HEADER]
    # Even estimates recorded: nothing to say.
    even = _Jobs({"job-1": _loaded(names, _even(names))})
    assert _deploy_block_notice(state, even, inv, None) is None
    _ui, quiet = _render_deploy(monkeypatch, state, even, inv)
    assert not [n for n in quiet if n[1] in (CDC_TABLE_LIST_HEADER,
                                             CDC_TABLE_LIST_WORST_CASE_HEADER)]
    # The lists every deploy sends over the limit: held whatever the estimates.
    many = _band(300)
    held = _deploy_block_notice(_deploy_ready(many), _Jobs({}), _inventory(many), None)
    assert held[0] == CDC_TABLE_LIST_HEADER and "(SinkTopics)" in held[1]


# ---------------------------------------------------------------------------
# 4. The Deploy click: the estimates it plans from are read first and judged exactly
# ---------------------------------------------------------------------------


def _fake_nicegui(monkeypatch, estimates):
    """``run.io_bound`` that answers only the source's row estimates, recording each call;
    any other call (an AWS one) fails the test."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    calls: list = []

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            calls.append(fn)
            if fn is _cdc_ui._estimate_cdc_table_rows:
                return estimates
            raise AssertionError(f"no AWS call may run before the refusal: {fn}")

    fake = types.ModuleType("nicegui")
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    return calls


def _click_deploy(state, inv):
    from dsql_migrator.ui.data_migration import _cdc_ui

    notes: list = []
    asyncio.run(_cdc_ui._start_cdc_infra_deploy(
        SimpleNamespace(notify=lambda text="", **k: notes.append((text, k.get("type")))),
        state, _Jobs({}), lambda: None, inventory=inv,
        session=SimpleNamespace(target_config=SimpleNamespace(region="us-east-1")),
    ))
    return notes


def test_a_certain_overflow_is_refused_at_the_click_before_any_read(monkeypatch) -> None:
    calls = _fake_nicegui(monkeypatch, None)
    many = _band(300)
    ((text, tone),) = _click_deploy(_deploy_ready(many), _inventory(many))
    assert tone == "negative" and "(SinkTopics), over CloudFormation's" in text
    assert text.endswith("Deselect tables in the table picker.")
    assert calls == []


def test_skewed_estimates_read_at_the_click_refuse_before_any_aws_call(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    names = _band()
    calls = _fake_nicegui(monkeypatch, _skewed(names))
    ((text, tone),) = _click_deploy(_deploy_ready(names), _inventory(names))
    assert tone == "negative"
    assert "need 4,289 bytes in the CDC configuration (SinkTopicPartitions)" in text
    assert calls == [_cdc_ui._estimate_cdc_table_rows]


@pytest.mark.parametrize("estimates", ["even", None])
def test_the_deploy_sends_the_counts_it_was_judged_on(monkeypatch, estimates) -> None:
    # FAILS WITHOUT THE FIX: the band was refused before any read (judged at its largest).
    import nicegui.run as nrun

    import dsql_migrator.core.ec2_metadata as _em
    from dsql_migrator.core.cdc import MskSeedAdmission
    from dsql_migrator.ui.data_migration import _cdc_ui

    names = _band()
    counts = _even(names) if estimates == "even" else None
    reads: list = []

    def _estimate(_session, table_names):
        reads.append(list(table_names))
        return counts

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    class _Stop(Exception):
        pass

    sent: dict = {}

    def _dispatch(*_a, **kw):
        sent["row_counts_by_table"] = kw.get("row_counts_by_table")
        raise _Stop()

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(_cdc_ui, "_estimate_cdc_table_rows", _estimate)
    monkeypatch.setattr(_em, "build_ec2_client", lambda *_a, **_k: object())
    monkeypatch.setattr(_em, "verify_subnet_egress", lambda *_a, **_k: (True, ""))
    monkeypatch.setattr(_cdc_ui, "_cdc_target_region",
                        lambda *_a, **_k: (SimpleNamespace(region="us-east-1"), "us-east-1"))
    monkeypatch.setattr(_cdc_ui, "_cdc_infra_prefill", lambda *_a, **_k: {
        "vpc_id": "vpc-app", "dsql_cluster_arn": "arn:aws:dsql:us-east-1:1:cluster/c",
        "connector_subnet_ids": "subnet-a,subnet-b"})
    monkeypatch.setattr(_cdc_ui, "_resolve_cdc_source_secret", lambda *_a, **_k: SimpleNamespace(
        ok=True, arn="arn:secret", name="sec", error=None, error_type=None))
    monkeypatch.setattr(_cdc_ui, "_msk_seed_admission", lambda *_a, **_k: MskSeedAdmission())
    monkeypatch.setattr(_cdc_ui, "dispatch_cdc_infra_params", _dispatch)
    notes: list = []
    session = SimpleNamespace(
        aws_profile=None, target_config=SimpleNamespace(region="us-east-1"),
        source_config=SimpleNamespace(source_type=SourceType.MYSQL, database="shop"),
    )
    with pytest.raises(_Stop):
        asyncio.run(_cdc_ui._start_cdc_infra_deploy(
            SimpleNamespace(notify=lambda text="", **k: notes.append((text, k.get("type")))),
            _deploy_ready(names), _Jobs({}), lambda: None, inventory=_inventory(names),
            session=session,
        ))
    assert notes == []
    assert reads == [names]  # read once, at the top of the click
    assert sent["row_counts_by_table"] == counts
