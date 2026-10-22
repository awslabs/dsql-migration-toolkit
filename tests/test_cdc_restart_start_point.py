"""Scratch trial tests (NOT in the repo): where a Start after Stop actually begins."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from dsql_migrator.core.cdc import Watermark
from dsql_migrator.core.models import MigrationMode, SourceType
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType, _cdc_ui
from dsql_migrator.ui.data_migration import _cdc_status as _status

from tests.test_ui_data_migration import _RecordingUi

_NOW = datetime.now(timezone.utc)
# A Full load + CDC run's watermark: its binlog position was fenced (G147).
_WM = Watermark(
    snapshot_timestamp=_NOW,
    binlog_file="mysql-bin-changelog.000007",
    binlog_position=108063011,
    start_point_fence="flush",
)


class _CardUi(_RecordingUi):
    """_RecordingUi + a radio that records its option labels as visible text."""

    def radio(self, options=None, *_a, **_k):
        if isinstance(options, dict):
            self.texts.extend(str(v) for v in options.values())
        return _RecordingUi._El(self)


class _JM:
    def __init__(self, job):
        self._job = job

    def get_status(self, _job_id):
        return self._job


def _state(*, committed=True, deployed=None):
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_cdc_has_committed_offset(committed)
    if deployed is not None:
        state.set_cdc_deployed_watermark(deployed)
    state.job_id = "job-1"
    return state


def _job(wm=_WM):
    return SimpleNamespace(watermark=wm, status="DONE")


def test_probe_records_the_watermark_the_stack_last_started_from():
    class _D:
        stack_status = "UPDATE_COMPLETE"
        is_stable = True

        def __init__(self, p):
            self.current_parameters = p

    class _Dep:
        def __init__(self, p):
            self._p = p

        def describe_stack_or_none(self, _n):
            return _D(self._p)

        def list_cdc_stacks(self):
            return []

    import dsql_migrator.core.cdc_deployer as dmod

    def probe(p):
        st = DataMigrationState()
        orig = dmod.build_cdc_stack_deployer
        dmod.build_cdc_stack_deployer = lambda *_a, **_k: _Dep(p)
        try:
            _status._probe_cdc_stack_phase(
                st, SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
            )
        finally:
            dmod.build_cdc_stack_deployer = orig
        return st

    seeded = probe({"MskBootstrapServers": "", "DeploySink": "true",
                    "WatermarkBinlogFile": "mysql-bin-changelog.000007",
                    "WatermarkBinlogPos": "108063011"})
    assert seeded.cdc_deployed_watermark == ("mysql-bin-changelog.000007", "108063011")
    # G19: with the GTID set it was offered ("" -- none), from the same describe.
    assert seeded.cdc_deployed_watermark_gtids == ""
    with_set = probe({"MskBootstrapServers": "", "DeploySink": "true",
                      "WatermarkBinlogFile": "mysql-bin-changelog.000007",
                      "WatermarkBinlogPos": "108063011",
                      "WatermarkGtids": "u:1-5,\n v:1-3"})
    assert with_set.cdc_deployed_watermark_gtids == "u:1-5,v:1-3"
    cdc_only = probe({"MskBootstrapServers": "", "DeploySink": "true",
                      "WatermarkBinlogFile": "", "WatermarkBinlogPos": "",
                      "WatermarkGtids": "u:1-5"})
    assert cdc_only.cdc_deployed_watermark is None
    assert cdc_only.cdc_deployed_watermark_gtids is None  # no recorded position: no record
    # Adopting another stack drops it (it belongs to the previous stack).
    seeded.adopt_cdc_stack("dsql-cdc-other")
    assert seeded.cdc_deployed_watermark is None
    with_set.adopt_cdc_stack("dsql-cdc-other")
    assert with_set.cdc_deployed_watermark_gtids is None


def test_cdc_start_resumes_truth_table():
    f = _cdc_ui.cdc_start_resumes
    my = SourceType.MYSQL
    # Never streamed -> not a resume (first start), regardless of watermark.
    assert f(_state(committed=False), _WM, my) is False
    # Streamed, nothing to seed -> the committed offset decides: resume.
    assert f(_state(), None, my) is True
    gtid_only = Watermark(snapshot_timestamp=_NOW, gtid_executed="uuid:1-9")
    assert f(_state(), gtid_only, my) is True
    # Streamed FROM this same watermark -> offset is at/past it -> seeder skips: resume.
    assert f(_state(deployed=("mysql-bin-changelog.000007", "108063011")), _WM, my) is True
    # CDC-only history (no watermark seeded) + a NEW Full Load watermark -> seeded: not a resume.
    assert f(_state(), _WM, my) is False
    # A different (newer) Full Load than the one last seeded -> seeded: not a resume.
    assert f(_state(deployed=("mysql-bin-changelog.000005", "4")), _WM, my) is False
    # PostgreSQL is unchanged.
    assert f(_state(), _WM, SourceType.POSTGRES) is True
    # The seeders' own at/past comparison, not string equality: a seed OLDER than the
    # watermark the stack last started from (e.g. a Manual point) is skipped by the
    # no-clobber guard -> resume. Same file: position decides.
    assert f(_state(deployed=("mysql-bin-changelog.000009", "4")), _WM, my) is True
    same_file = ("mysql-bin-changelog.000007", "108063012")
    assert f(_state(deployed=same_file), _WM, my) is True  # seed 1 byte earlier
    assert f(_state(deployed=("mysql-bin-changelog.000007", "108063010")), _WM, my) is False
    # Numeric binlog sequence across the .999999 -> .1000000 width rollover.
    rolled = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.999999",
                       binlog_position=4)
    assert f(_state(deployed=("mysql-bin.1000000", "4")), rolled, my) is True
    # A malformed deployed parameter is not provably a resume.
    assert f(_state(deployed=("mysql-bin-changelog.000007", "x")), _WM, my) is False


_MANUAL_OLD = ("mysql-bin-changelog.000005", 4)


def _manual_state(point, **kw):
    state = _state(**kw)
    state.set_cdc_start_mode("manual")
    if isinstance(point, tuple):
        state.set_cdc_start_position(binlog_file=point[0], binlog_pos=point[1])
    else:
        state.set_cdc_start_position(gtid=point)
    return state


def _card(state, job=None, inventory=None):
    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(job or _job()), lambda: None, inventory=inventory,
        migration_type=MigrationType.CDC_ONLY, session=None,
    )
    return ui


def test_a_manual_point_before_where_the_stack_streamed_is_disclosed_as_not_applied():
    # Committed offset + a Manual point at/before the watermark the stack started from:
    # the seeder (no-clobber) skips it and streaming resumes. The card must say the entry
    # is NOT applied -- and how to replay from it -- instead of letting it look honoured.
    state = _manual_state(
        _MANUAL_OLD, deployed=("mysql-bin-changelog.000007", "108063011")
    )
    assert _cdc_ui.cdc_start_resumes(
        state, _cdc_ui._cdc_start_seed(state, _WM, SourceType.MYSQL)[0], SourceType.MYSQL
    ) is True
    blob = " ".join(_card(state).texts)
    assert "Resuming from the last streamed position" in blob
    assert (
        "Your manual point mysql-bin-changelog.000005:4 is earlier than where this stack "
        "already streamed and is not applied."
    ) in blob
    assert "delete the CDC infrastructure and deploy again" in blob
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui, state, _JM(_job()), lambda: None,
                                     inventory=None, session=None)
    assert "Ready to resume CDC" in " ".join(ui.texts)


def test_a_manual_point_on_a_stack_whose_start_is_unknown_is_refused():
    # Committed offset but no recorded deployed watermark (a CDC-only history): the point
    # cannot be shown to be at or before where streaming reached. DELIBERATE G25 UPDATE (A2):
    # it used to be offered ("applied only if it is past"), which moves the stream forward
    # under the sink's unapplied tail; Start CDC now refuses it, naming the route.
    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER

    state = _manual_state(("mysql-bin-changelog.000009", 4))
    blob = " ".join(_card(state).texts)
    assert "Resuming from the last streamed position" not in blob
    assert NEWER_POINT_HEADER in blob and "its last Start offered no start offset" in blob
    assert "is applied only if it is past" not in blob
    assert "Start point set" not in blob and "Action needed" in blob


def test_a_gtid_only_manual_entry_is_not_a_start_point():
    # A GTID set seeds nothing (the offset is keyed on file:pos): the card must not say
    # "Start point set", must say why, and Start must stay disabled -- even though a
    # usable Full Load watermark exists (Manual never falls back to it).
    state = _manual_state(
        "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100", committed=False
    )
    card = _card(state)
    blob = " ".join(card.texts)
    assert "Start point set" not in blob
    assert "Action needed" in blob
    assert (
        "A GTID set alone cannot set where this pipeline starts: the start offset is keyed "
        "on the binlog file:position, so enter it too (Fetch current position fills both)."
    ) in blob
    assert "(preferred)" not in blob

    from tests.test_ui_data_migration import _StartButtonUi

    button = _StartButtonUi()
    _cdc_ui._render_cdc_start_button(button, state, _JM(_job()), lambda: None,
                                     inventory=None, session=None)
    assert button.start_disabled is True
    assert "Set the CDC start point above first." in " ".join(button.texts)

    # The same state with a binlog file:position is a start point, and Start is enabled.
    from dsql_migrator.core.models import TableSelection
    from tests.test_ui_data_migration import _inventory

    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000009", binlog_pos=4)
    state.set_selection(TableSelection(selected_tables=["orders"]))
    blob = " ".join(_card(state, inventory=_inventory()).texts)
    assert "Start point set — binlog mysql-bin-changelog.000009:4" in blob
    assert "binlog (start) = mysql-bin-changelog.000009:4" in blob  # the preview agrees
    assert "snapshot.mode = recovery" in blob  # a seeded offset needs recovery
    button = _StartButtonUi()
    _cdc_ui._render_cdc_start_button(button, state, _JM(_job()), lambda: None,
                                     inventory=None, session=None)
    assert button.start_disabled is False


def test_the_locked_badge_does_not_promise_a_stop_can_rewind_the_start_point():
    ui = _CardUi()
    _cdc_ui._render_cdc_start_point_card(
        ui, _state(), lambda: None, wm_resume=None, wm_usable=False,
        effective_resume=None, mode="auto", locked=True, session=None,
        source_type=SourceType.MYSQL,
    )
    tips = " ".join(ui.tooltips)
    assert "To change it, stop CDC first." not in tips
    # G25 (A2): after a Stop Start CDC continues; ANY other start point needs a new stack.
    assert "is applied only if it is past" not in tips
    assert (
        "After a Stop, Start CDC continues from the committed position; a different start "
        "point (earlier or later) needs Delete CDC infrastructure and deploy it again first."
    ) in tips


def test_start_button_after_a_new_full_load_on_a_streamed_stack_is_refused():
    # DELIBERATE G25 UPDATE (A2): a Full Load watermark the stack's start cannot be compared
    # with (it recorded none) is not a resume -- it used to be a "Ready to start CDC" seeded
    # start, which moves the stream forward under the sink's unapplied tail.
    from tests.test_ui_data_migration import _StartButtonUi

    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER

    ui = _StartButtonUi()
    _cdc_ui._render_cdc_start_button(ui, _state(), _JM(_job()), lambda: None,
                                     inventory=None, session=None)
    blob = " ".join(ui.texts)
    assert "Ready to resume CDC" not in blob
    assert "continues from exactly where streaming stopped" not in blob
    assert "Ready to start CDC" not in blob and NEWER_POINT_HEADER in blob
    assert ui.start_disabled is True


def test_start_point_card_after_a_new_full_load_on_a_streamed_stack_refuses_it():
    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER

    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, _state(), _JM(_job()), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    blob = " ".join(ui.texts)
    assert "Resuming from the last streamed position" not in blob
    assert "Automatic — gapless from Full Load (recommended)" not in blob
    assert "Automatic — from the Full Load watermark" in blob and NEWER_POINT_HEADER in blob
    # Never started: the gapless Automatic handoff, as before.
    first = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        first, _state(committed=False), _JM(_job()), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    assert "Automatic — gapless from Full Load (recommended)" in " ".join(first.texts)


def test_plain_restart_from_the_same_watermark_keeps_the_resume_copy():
    deployed = ("mysql-bin-changelog.000007", "108063011")
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui, _state(deployed=deployed), _JM(_job()),
                                     lambda: None, inventory=None, session=None)
    assert "Ready to resume CDC" in " ".join(ui.texts)
    card = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        card, _state(deployed=deployed), _JM(_job()), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    assert "Resuming from the last streamed position" in " ".join(card.texts)


def test_start_detail_on_a_restart_names_the_committed_position():
    d = _cdc_ui._cdc_start_detail
    resumed = d("dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL, resumes=True)
    assert "gapless from the Full Load watermark" not in resumed
    assert "committed stream position" in resumed
    no_wm = d("dsql-cdc-x", None, mode="auto", source_type=SourceType.MYSQL, resumes=True)
    assert "CURRENT position" not in no_wm
    assert "committed stream position" in no_wm
    # A seeded start keeps today's exact line.
    assert d("dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL) == d(
        "dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL, resumes=False
    )
    assert "gapless from the Full Load watermark" in d(
        "dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL
    )


def test_start_detail_names_a_manual_start_point():
    d = _cdc_ui._cdc_start_detail
    manual = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000042",
                       binlog_position=1234)
    line = d("dsql-cdc-x", manual, mode="manual", source_type=SourceType.MYSQL,
             seed_origin="manual")
    assert (
        "start point binlog mysql-bin.000042:1234 (Manual start point; changes from this "
        "position on are replicated)"
    ) in line
    assert "gapless from the Full Load watermark" not in line
    assert "CURRENT position" not in line
    # G19 review: a Manual point seeded WITH a GTID set is still named by its binlog
    # file:position -- the card's, the deploy log's and the positioning line's coordinate --
    # plus the set (capped like every other line naming one), never by the set alone.
    with_set = manual.model_copy(update={
        "gtid_executed": "1DFF02DB-0000-0000-0000-000000000001:1-900,\n"
                         "1DFF02DB-0000-0000-0000-000000000002:1-77"
    })
    line = d("dsql-cdc-x", with_set, mode="manual", source_type=SourceType.MYSQL,
             seed_origin="manual")
    assert (
        "start point binlog mysql-bin.000042:1234 with GTID set "
        "1DFF02DB-0000-0000-0000-000000000001:1-900,1DFF02DB-0000-0000-0000-000000000002:1-77 "
        "(Manual start point; changes from this position on are replicated)"
    ) in line
    assert "start point GTID" not in line
    long_set = ",".join(f"{i:08d}-0000-0000-0000-000000000000:1-9" for i in range(10))
    capped = d("dsql-cdc-x", manual.model_copy(update={"gtid_executed": long_set}),
               mode="manual", source_type=SourceType.MYSQL, seed_origin="manual")
    assert " … (Manual start point" in capped and long_set not in capped
    # A GTID-only Manual entry seeds nothing: say so, not "there is no Full Load watermark".
    none = d("dsql-cdc-x", None, mode="manual", source_type=SourceType.MYSQL,
             seed_origin="manual_unseedable")
    assert "CURRENT position" in none
    assert "no binlog file:position to seed" in none
    assert "there is no Full Load watermark" not in none


# --- review of 824a208 ------------------------------------------------------------


def _card_text(state, job=None):
    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(job or _job()), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    return " ".join(ui.texts)


def test_a_manual_point_after_the_full_load_watermark_says_the_window_is_not_replicated():
    state = _state(committed=False)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000009", binlog_pos=4)
    blob = _card_text(state)
    assert "NOT replicated" in blob
    assert "mysql-bin-changelog.000007:108063011" in blob and "mysql-bin-changelog.000009:4" in blob


def test_a_manual_point_before_the_full_load_watermark_says_changes_are_re_applied():
    state = _state(committed=False)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000005", binlog_pos=4)
    blob = _card_text(state)
    assert "re-applied" in blob and "NOT replicated" not in blob


def test_a_manual_point_equal_to_the_watermark_needs_no_disclosure():
    state = _state(committed=False)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000007", binlog_pos=108063011)
    blob = _card_text(state)
    assert "NOT replicated" not in blob and "re-applied" not in blob


def test_the_mysql_manual_option_asks_for_a_binlog_position_not_a_gtid():
    state = _state(committed=False)
    blob = _card_text(state)
    assert "Manual — enter a binlog file:position" in blob
    assert "Manual — enter a GTID or binlog position" not in blob


def test_a_manual_point_on_a_streamed_stack_is_refused_with_the_route():
    # DELIBERATE G25 UPDATE (A2): it used to be "applied only if it is past ... not
    # replicated" -- the forward seed under the sink's unapplied tail Start CDC now refuses.
    state = _state(committed=True)  # streamed; its start watermark unknown
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000009", binlog_pos=4)
    blob = _card_text(state, job=SimpleNamespace(watermark=None, status="DONE"))
    assert "past the position this stack already streamed to" not in blob
    assert (
        "To start from a new start point, use Delete CDC infrastructure and deploy it again "
        "(the new stack starts with empty topics), then Re-run Full Load"
    ) in blob


def test_a_new_full_load_resets_a_stale_manual_start_point():
    """A Manual point entered before a Full load + CDC run's Full Load no longer applies:
    the new watermark is the gapless handoff (review of 824a208: it silently beat it)."""
    from dsql_migrator.ui.data_migration import reset_stale_manual_start_point

    logged = []
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin-changelog.000003", binlog_pos=154)
    assert reset_stale_manual_start_point(state, MigrationType.FULL_LOAD_AND_CDC, SourceType.MYSQL,
                                          log=lambda **kw: logged.append(kw)) is True
    assert state.cdc_start_mode() == "auto" and logged and "mysql-bin-changelog.000003:154" in logged[0]["detail"]
    # Untouched: auto already, a Full-load-only run, and PostgreSQL (Manual = re-snapshot there).
    for mode, mtype, st in (("auto", MigrationType.FULL_LOAD_AND_CDC, SourceType.MYSQL),
                            ("manual", MigrationType.FULL_LOAD_ONLY, SourceType.MYSQL),
                            ("manual", MigrationType.FULL_LOAD_AND_CDC, SourceType.POSTGRES)):
        s = DataMigrationState()
        s.set_cdc_start_mode(mode)
        assert reset_stale_manual_start_point(s, mtype, st, log=lambda **kw: None) is False
        assert s.cdc_start_mode() == mode


def test_the_full_load_start_and_retry_reset_a_stale_manual_point() -> None:
    import inspect

    import dsql_migrator.ui.data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    assert src.count("reset_stale_manual_start_point(") >= 2


def test_a_start_earlier_than_a_seed_a_failed_start_left_behind_is_refused():
    """Review of 824a208 (high): a first Start that FAILED after seeding S leaves S in the
    offsets topic, and the seeder never rewinds -- so a later, EARLIER start point would be
    silently not applied (the stack never streamed, so the card treats it as a first start)."""
    from dsql_migrator.ui.data_migration._cdc_ui import seed_refused_by_an_earlier_offer

    state = _state(committed=False)
    state.note_cdc_offered_seed(Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000120", binlog_position=999))
    earlier = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000100", binlog_position=4)
    later = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000130", binlog_position=4)
    same = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000120", binlog_position=999)
    reason = seed_refused_by_an_earlier_offer(state, earlier, SourceType.MYSQL)
    assert reason and "mysql-bin.000120:999" in reason and "delete the CDC infrastructure" in reason
    assert seed_refused_by_an_earlier_offer(state, later, SourceType.MYSQL) is None
    assert seed_refused_by_an_earlier_offer(state, same, SourceType.MYSQL) is None
    assert seed_refused_by_an_earlier_offer(state, earlier, SourceType.POSTGRES) is None
    # once the stack streamed, the committed-offset logic (cdc_start_resumes) decides
    state.set_cdc_has_committed_offset(True)
    assert seed_refused_by_an_earlier_offer(state, earlier, SourceType.MYSQL) is None


def test_the_offered_seed_keeps_the_latest_and_clears_with_the_stack():
    state = DataMigrationState()
    a = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000120", binlog_position=999)
    b = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000100", binlog_position=4)
    state.note_cdc_offered_seed(a)
    state.note_cdc_offered_seed(b)
    assert state.cdc_offered_seed == ("mysql-bin.000120", 999)
    state.set_cdc_stack_phase("absent")
    assert state.cdc_offered_seed is None


def test_start_cdc_checks_and_records_the_offered_seed():
    import inspect

    src = inspect.getsource(_cdc_ui._start_cdc_deploy)
    assert "seed_refused_by_an_earlier_offer(" in src and "note_cdc_offered_seed(" in src
    assert src.index("seed_refused_by_an_earlier_offer(") < src.index("note_cdc_offered_seed(")


def test_the_start_cdc_fk_gate_only_checks_keys_the_stream_can_reach() -> None:
    """Live (GTID check, 2026-10-01): a 5-table migration_schema stream had Start CDC
    disabled by ecommerce_demo FKs -- owned by the same conversion (the whole evaluated
    inventory) but unreachable by any streamed row. Kept: streamed tables, and children
    whose FK references a streamed table (a streamed parent DELETE still trips them)."""
    from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, SourceInventory, TableDef

    def table(name, refs=()):
        return TableDef(
            name=name, columns=[ColumnDef(name="id", mysql_type="int")], primary_key=["id"],
            foreign_keys=[ForeignKeyDef(name=f"fk_{name.split('.')[1]}_{i}", columns=["id"],
                                        referenced_table=r, referenced_columns=["id"])
                          for i, r in enumerate(refs)],
        )

    inventory = SourceInventory(tables=[
        table("s.parent"), table("s.child", ["s.parent"]), table("s.grandchild", ["s.child"]),
        table("o.a"), table("o.b", ["o.a"]),
    ])
    preserved = {"s.child": ["fk_child_0"], "s.grandchild": ["fk_grandchild_0"], "o.b": ["fk_b_0"]}
    scoped = _cdc_ui.cdc_foreign_keys_in_stream_scope(preserved, ["s.parent"], inventory)
    assert scoped == {"s.child": ["fk_child_0"]}  # direct child of a streamed parent only
    assert _cdc_ui.cdc_foreign_keys_in_stream_scope(preserved, ["s.child"], inventory) == {
        "s.child": ["fk_child_0"], "s.grandchild": ["fk_grandchild_0"]}
    # a preserved table the inventory does not describe is kept (fail-closed)
    assert "x.unknown" in _cdc_ui.cdc_foreign_keys_in_stream_scope(
        {**preserved, "x.unknown": ["fk_u"]}, ["s.parent"], inventory)
    # no inventory: nothing narrowed (the previous, fail-closed behaviour)
    assert _cdc_ui.cdc_foreign_keys_in_stream_scope(preserved, ["s.parent"], None) == preserved

    import inspect

    src = inspect.getsource(_cdc_ui._probe_cdc_blocking_foreign_keys)
    assert "cdc_foreign_keys_in_stream_scope(preserved, streamed, inventory)" in src
    assert src.index("cdc_foreign_keys_in_stream_scope(") < src.index("table_names = sorted(")


# ---------------------------------------------------------------------------
# G5: the preview and Start send ONE snapshot mode; the stack facts it reads
# ---------------------------------------------------------------------------


def test_the_preview_of_a_resuming_stack_prints_schema_only_and_start_sends_it(monkeypatch):
    from dsql_migrator.core.models import TableSelection
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    state = _state(deployed=("mysql-bin-changelog.000007", "108063011"))
    state.set_selection(TableSelection(selected_tables=["orders"]))
    blob = " ".join(_card(state, inventory=_inventory()).texts)
    assert "snapshot.mode = schema_only" in blob
    assert "snapshot.mode = recovery" not in blob
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, job_watermark=_WM, state=state,
    )
    assert dict(run["kwargs"]["params"].filled)["SnapshotMode"] == "schema_only"
    # A first start from the same watermark (never streamed) is still recovery.
    first = _state(committed=False)
    first.set_selection(TableSelection(selected_tables=["orders"]))
    assert "snapshot.mode = recovery" in " ".join(_card(first, inventory=_inventory()).texts)


def _probe_with(params):
    class _D:
        stack_status = "UPDATE_COMPLETE"
        is_stable = True

        def __init__(self, p):
            self.current_parameters = p

    class _Dep:
        def describe_stack_or_none(self, _n):
            return _D(params)

        def list_cdc_stacks(self):
            return []

    import dsql_migrator.core.cdc_deployer as dmod

    st = DataMigrationState()
    orig = dmod.build_cdc_stack_deployer
    dmod.build_cdc_stack_deployer = lambda *_a, **_k: _Dep()
    try:
        _status._probe_cdc_stack_phase(
            st, SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
        )
    finally:
        dmod.build_cdc_stack_deployer = orig
    return st


def test_the_probe_caches_the_live_snapshot_mode_from_the_same_describe():
    running = _probe_with({"MskBootstrapServers": "b-1:9098", "SnapshotMode": "schema_only"})
    assert running.cdc_deployed_snapshot_mode == "schema_only"
    # Stopped: the parameter is carried forward, but no connector runs with it.
    stopped = _probe_with({"MskBootstrapServers": "", "SnapshotMode": "recovery",
                           "DeploySink": "true"})
    assert stopped.cdc_deployed_snapshot_mode is None
    odd = _probe_with({"MskBootstrapServers": "b-1:9098", "SnapshotMode": "initial"})
    assert odd.cdc_deployed_snapshot_mode is None


def test_the_history_flag_and_live_mode_clear_with_the_stack():
    def _armed():
        st = DataMigrationState()
        st.note_cdc_history_missing(True)
        st.set_cdc_deployed_snapshot_mode("schema_only")
        return st

    gone = _armed()
    gone.set_cdc_stack_phase("absent")
    assert gone.cdc_history_missing is False and gone.cdc_deployed_snapshot_mode is None
    stopped = _armed()
    stopped.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    assert stopped.cdc_history_missing is True  # the same stack, still missing it
    adopted = _armed()
    assert adopted.adopt_cdc_stack("dsql-cdc-other")
    assert adopted.cdc_history_missing is False and adopted.cdc_deployed_snapshot_mode is None
    renamed = _armed()
    assert renamed.set_cdc_stack_name("dsql-cdc-renamed")
    assert renamed.cdc_history_missing is False and renamed.cdc_deployed_snapshot_mode is None
    same = _armed()
    assert same.set_cdc_stack_name(same.cdc_stack_name)
    assert same.cdc_history_missing is True


# ---------------------------------------------------------------------------
# G5: a Manual point records the captured tables' fingerprints at Fetch current position
# ---------------------------------------------------------------------------


class _FetchConn:
    def __init__(self, log, *, status_error=None, catalog_error=None):
        self.log, self.status_error, self.catalog_error = log, status_error, catalog_error


def _patch_fetch(monkeypatch, *, status_error=None, catalog_error=None, gtid_mode="ON"):
    import dsql_migrator.core.cdc_catalog_drift as drift
    import dsql_migrator.core.watermark as wm
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.core.cdc_catalog_drift import LiveColumn, LiveTable

    log: list = []

    class _Engine:
        def connect(self):
            conn = _FetchConn(log, status_error=status_error, catalog_error=catalog_error)
            log.append(("connect", id(conn)))

            class _Ctx:
                def __enter__(self):
                    return conn

                def __exit__(self, *_a):
                    return False

            return _Ctx()

        def dispose(self):
            log.append(("dispose",))

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine())
    )

    def _catalog(conn, names, source_type=None):
        log.append(("catalog", id(conn), tuple(names)))
        if conn.catalog_error is not None:
            raise conn.catalog_error
        return {
            n: LiveTable(columns=(LiveColumn("id", "int", False, False, 0),),
                         primary_key=frozenset({"id"}))
            for n in names
        }

    def _status_row(conn):
        log.append(("status", id(conn)))
        if conn.status_error is not None:
            raise conn.status_error
        return {"File": "mysql-bin.000200", "Position": 77, "Executed_Gtid_Set": ""}

    def _gtid_mode(conn):
        log.append(("gtid_mode", id(conn)))
        return gtid_mode

    monkeypatch.setattr(drift, "read_live_catalog", _catalog)
    monkeypatch.setattr(wm, "read_binlog_status_row", _status_row)
    monkeypatch.setattr(wm, "read_gtid_mode", _gtid_mode)
    return log


_SESSION = SimpleNamespace(source_config=SimpleNamespace(), source_password="pw")


def test_fetch_reads_the_catalog_before_the_position_on_one_connection(monkeypatch):
    log = _patch_fetch(monkeypatch, gtid_mode="ON")
    row, fps, mode = _cdc_ui._fetch_start_position(_SESSION, ["app.orders"])
    assert row["File"] == "mysql-bin.000200"
    assert set(fps) == {"app.orders"} and fps["app.orders"].startswith("2:1:")
    # G19: the source's gtid_mode is read on the same connection, after the position.
    assert mode == "ON"
    kinds = [e[0] for e in log]
    assert kinds == ["connect", "catalog", "status", "gtid_mode", "dispose"]
    assert log[1][1] == log[2][1] == log[3][1] == log[0][1]  # ONE connection


def test_fetch_disposes_the_engine_and_keeps_the_position_when_the_catalog_fails(monkeypatch):
    import pytest

    log = _patch_fetch(monkeypatch, catalog_error=RuntimeError("timeout"))
    row, fps, _mode = _cdc_ui._fetch_start_position(_SESSION, ["app.orders"])
    assert row["Position"] == 77 and fps is None
    log = _patch_fetch(monkeypatch, status_error=RuntimeError("denied"))
    with pytest.raises(RuntimeError):
        _cdc_ui._fetch_start_position(_SESSION, ["app.orders"])
    assert log[-1] == ("dispose",)


class _InputsUi(_RecordingUi):
    """Inputs with a settable value, and recorded notifications."""

    def __init__(self):
        super().__init__()
        self.inputs: dict = {}
        self.notified: list = []

    class _Input(_RecordingUi._El):
        def __init__(self, ui, value=""):
            super().__init__(ui)
            self.value = value

        def set_value(self, value):
            self.value = value

    def input(self, label="", *_a, value="", **_k):
        el = _InputsUi._Input(self, value)
        self.inputs[str(label)] = el
        return el

    def notify(self, message="", *_a, **_k):
        self.notified.append((str(message), _k.get("type")))


def _manual_inputs(monkeypatch, fetch_result, *, state=None):
    import asyncio

    import nicegui

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    calls: list = []

    def _fetch(session, names):
        calls.append(list(names))
        # (row, fingerprints, gtid_mode); a 2-tuple fixture fetched while gtid_mode was OFF.
        return tuple(fetch_result) if len(fetch_result) == 3 else (*fetch_result, "OFF")

    monkeypatch.setattr(_cdc_ui, "_fetch_start_position", _fetch)
    if state is None:
        state = DataMigrationState()
        state.set_cdc_start_mode("manual")
    ui = _InputsUi()
    _cdc_ui._render_cdc_manual_inputs(
        ui, state, lambda: None, session=_SESSION, captured_names=lambda: ["app.orders"],
    )
    by = {b.text: b for b in ui.buttons}
    asyncio.run(by["Fetch current position"].on_click())
    return ui, state, by, calls


def test_use_this_start_point_binds_the_fingerprints_only_to_the_fetched_position(monkeypatch):
    fps = {"app.orders": "1:1:00000000:0000000000000000"}
    ui, state, by, calls = _manual_inputs(
        monkeypatch,
        ({"File": "mysql-bin.000200", "Position": 77, "Executed_Gtid_Set": ""}, fps),
    )
    assert calls == [["app.orders"]]
    assert ui.inputs["Binlog file:position"].value == "mysql-bin.000200:77"
    by["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == fps
    seed, origin = _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)
    assert origin == "manual" and seed.table_fingerprints == fps
    # An edited position is not the one the definitions were read with.
    ui.inputs["Binlog file:position"].set_value("mysql-bin.000200:78")
    by["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == {}


def test_a_fetch_whose_definitions_could_not_be_read_warns(monkeypatch):
    ui, state, by, _calls = _manual_inputs(
        monkeypatch,
        ({"File": "mysql-bin.000200", "Position": 77, "Executed_Gtid_Set": ""}, None),
    )
    warnings = [m for m, t in ui.notified if t == "warning"]
    assert warnings and "could not be recorded" in warnings[0]
    by["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == {}
    assert state.cdc_start_override().binlog_position == 77


def test_the_manual_fingerprints_follow_the_coordinates_they_were_recorded_for():
    from dsql_migrator.ui.data_migration import reset_stale_manual_start_point

    state = DataMigrationState()
    fps = {"app.orders": "1:1:00000000:0000000000000000"}
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77, fingerprints=fps)
    assert state.cdc_start_fingerprints() == fps
    # Without a binlog position there is nothing to bind them to.
    state.set_cdc_start_position(gtid="u:1-5", fingerprints=fps)
    assert state.cdc_start_fingerprints() == {}
    # Any later set without them clears them.
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77, fingerprints=fps)
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77)
    assert state.cdc_start_fingerprints() == {}
    # The Full Load's reset to Automatic keeps the coordinates, and so the fingerprints.
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77, fingerprints=fps)
    state.set_cdc_start_mode("manual")
    assert reset_stale_manual_start_point(
        state, MigrationType.FULL_LOAD_AND_CDC, SourceType.MYSQL, log=lambda **_k: None
    )
    assert state.cdc_start_fingerprints() == fps


def test_a_fetch_whose_definitions_could_not_be_read_says_what_to_do_next(monkeypatch):
    # The point is not applied yet when the warning shows, so it must name the next step
    # (the success notice it replaces did: "click 'Use this start point' to confirm").
    ui, state, by, _calls = _manual_inputs(
        monkeypatch,
        ({"File": "mysql-bin.000200", "Position": 77, "Executed_Gtid_Set": ""}, None),
    )
    warnings = [m for m, t in ui.notified if t == "warning"]
    assert state.cdc_start_override() is None  # nothing applied yet
    assert "Fetch again" in warnings[0] and "Use this start point" in warnings[0]


def test_fetch_never_swallows_a_read_only_guard_violation(monkeypatch):
    import pytest

    from dsql_migrator.core.introspector import ReadOnlySourceError

    log = _patch_fetch(monkeypatch, catalog_error=ReadOnlySourceError("refused"))
    with pytest.raises(ReadOnlySourceError):
        _cdc_ui._fetch_start_position(_SESSION, ["app.orders"])
    assert log[-1] == ("dispose",)


# ---------------------------------------------------------------------------
# G19: Fetch current position records the GTID set with the position (gtid_mode=ON only)
# ---------------------------------------------------------------------------

_U = "1DFF02DB-0000-0000-0000-000000000001"
_V = "1DFF02DB-0000-0000-0000-000000000002"
# MySQL prints ",\n" between server UUIDs.
_ROW_GTID = {"File": "mysql-bin.000200", "Position": 77,
             "Executed_Gtid_Set": f"{_U}:1-812,\n{_V}:1-3"}
_GTID_NORM = f"{_U}:1-812,{_V}:1-3"


def test_fetch_on_a_gtid_on_source_fills_the_normalized_set_and_says_so(monkeypatch):
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    assert ui.inputs["GTID set"].value == _GTID_NORM
    assert ui.inputs["Binlog file:position"].value == "mysql-bin.000200:77"
    positive = [m for m, t in ui.notified if t == "positive"]
    assert positive and "gtid_mode=ON: CDC starts after this GTID set" in positive[0]
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid == _GTID_NORM
    assert state.cdc_start_fingerprints() == _FPS  # the fetched (position, set) pair
    seed, origin = _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)
    assert origin == "manual" and seed.gtid_executed == _GTID_NORM
    saved = [m for m, t in ui.notified if t == "positive"][-1]
    assert saved == "Start point saved (not streaming yet) — mysql-bin.000200:77 with its GTID set"


def test_fetch_on_a_source_that_is_not_gtid_on_leaves_the_set_empty(monkeypatch):
    # A set read while gtid_mode is not ON is never offered: if the mode is switched to ON
    # later, it could make MySQL skip the changes written without a GTID.
    ui, state, by, _calls = _manual_inputs(
        monkeypatch, (dict(_ROW_GTID), _FPS, "OFF_PERMISSIVE")
    )
    assert ui.inputs["GTID set"].value == ""
    positive = [m for m, t in ui.notified if t == "positive"]
    assert positive and "its gtid_mode is OFF_PERMISSIVE" in positive[0]
    assert "starts from this binlog file:position" in positive[0]
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid is None
    assert state.cdc_start_fingerprints() == _FPS
    # An unread mode keeps the old wording and fills nothing either.
    ui2, _state2, _by2, _c = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, None))
    assert ui2.inputs["GTID set"].value == ""
    assert [m for m, t in ui2.notified if t == "positive"][0].startswith(
        "Fetched current position from source"
    )


def test_editing_only_the_position_after_a_fetch_drops_the_fetched_set(monkeypatch):
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    ui.inputs["Binlog file:position"].set_value("mysql-bin.000200:99")
    by["Use this start point"].on_click()
    # The set was read at :77, not :99 -- storing it would skip or re-read changes.
    assert state._cdc_start_gtid is None
    assert (state._cdc_start_binlog_file, state._cdc_start_binlog_pos) == (
        "mysql-bin.000200", 99
    )
    assert state.cdc_start_fingerprints() == {}
    assert ui.inputs["GTID set"].value == ""
    warned = [m for m, t in ui.notified if t == "warning"]
    assert warned and warned[-1].startswith("Start point saved without a GTID set:")
    assert "read at mysql-bin.000200:77, not at mysql-bin.000200:99" in warned[-1]


def test_a_stored_set_beside_an_edited_position_is_dropped_after_a_re_render(monkeypatch):
    _ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    by["Use this start point"].on_click()
    # Re-confirming the stored (position, set) pair after a re-render keeps both.
    ui2, by2 = _rerender(state)
    assert ui2.inputs["GTID set"].value == _GTID_NORM
    by2["Use this start point"].on_click()
    assert state._cdc_start_gtid == _GTID_NORM and state.cdc_start_fingerprints() == _FPS
    # Editing only the position: the stored set was read at :77.
    ui3, by3 = _rerender(state)
    ui3.inputs["Binlog file:position"].set_value("mysql-bin.000201:4")
    by3["Use this start point"].on_click()
    assert state._cdc_start_gtid is None and state.cdc_start_fingerprints() == {}
    assert any(
        m.startswith("Start point saved without a GTID set:") for m, _t in ui3.notified
    )
    # A set the operator types with a position is theirs to assert: stored (normalized).
    ui4, by4 = _rerender(state)
    ui4.inputs["GTID set"].set_value(f" {_U}:1-900 ")
    by4["Use this start point"].on_click()
    assert state._cdc_start_gtid == f"{_U}:1-900"


def test_the_start_point_line_names_the_gtid_set_seeded_with_the_position():
    state = _state(committed=False)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(
        gtid=f"{_U}:1-812,\n {_V}:1-3", binlog_file="mysql-bin.000200", binlog_pos=77
    )
    assert state._cdc_start_gtid == _GTID_NORM  # stored normalized
    blob = _card_text(state)
    assert f"Start point set — binlog mysql-bin.000200:77 · GTID set {_GTID_NORM}" in blob
    # The connector-config preview names the set too, and when Debezium uses it.
    from dsql_migrator.core.models import TableSelection
    from tests.test_ui_data_migration import _inventory

    state.set_selection(TableSelection(selected_tables=["orders"]))
    preview = " ".join(_card(state, inventory=_inventory()).texts)
    assert f"gtid (start, used when gtid_mode=ON) = {_GTID_NORM}" in preview
    assert "binlog (start) = mysql-bin.000200:77" in preview
    without = _state(committed=False)
    without.set_cdc_start_mode("manual")
    without.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77)
    line = _card_text(without)
    assert "Start point set — binlog mysql-bin.000200:77" in line
    assert "· GTID set" not in line


_FPS = {"app.orders": "2:1:00000000:0000000000000000"}
_ROW = {"File": "mysql-bin.000200", "Position": 77, "Executed_Gtid_Set": ""}


def _rerender(state):
    ui = _InputsUi()
    _cdc_ui._render_cdc_manual_inputs(
        ui, state, lambda: None, session=_SESSION, captured_names=lambda: ["app.orders"],
    )
    return ui, {b.text: b for b in ui.buttons}


def test_reconfirming_the_stored_point_after_a_re_render_keeps_its_fingerprints(monkeypatch):
    # "Use this start point" refreshes the card, so the next click is on a NEW render whose
    # fetch cell is empty: re-confirming the same point must not wipe the definitions
    # recorded for it (the check would drop to a warning for an unchanged point).
    _ui, state, by, _calls = _manual_inputs(monkeypatch, (_ROW, _FPS))
    by["Use this start point"].on_click()
    ui2, by2 = _rerender(state)
    assert ui2.inputs["Binlog file:position"].value == "mysql-bin.000200:77"
    by2["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == _FPS
    assert _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)[0].table_fingerprints == _FPS
    # G19: editing only the GTID set drops them -- on a gtid_mode=ON source the set IS the
    # start point, so definitions recorded with another set do not describe it.
    ui2.inputs["GTID set"].set_value("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
    by2["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == {}
    assert state._cdc_start_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    # Another position never carries them.
    ui2.inputs["Binlog file:position"].set_value("mysql-bin.000200:78")
    by2["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == {}


def test_reconfirming_a_restored_point_keeps_its_fingerprints():
    # Session restore puts back the point WITH its fingerprints; clicking the prefilled
    # card must keep them.
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77, fingerprints=_FPS)
    _ui, by = _rerender(state)
    by["Use this start point"].on_click()
    seed, origin = _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)
    assert origin == "manual" and seed.table_fingerprints == _FPS


# ---------------------------------------------------------------------------
# G5: the Manual route the refusals and the replay diagnoses name is on the card
# ---------------------------------------------------------------------------


class _CardInputsUi(_InputsUi, _CardUi):
    """The start-point card with its radio, inputs and notifications."""


def _src_session():
    from dsql_migrator.core.models import SourceConnectionConfig

    return SimpleNamespace(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password="pw", target_config=None, aws_profile=None,
    )


def _live_card(monkeypatch, state, *, migration_type, job=None):
    """Render the real source-config card with a source session; Fetch is stubbed and
    records the table names it was asked to fingerprint."""
    import nicegui

    from tests.test_ui_data_migration import _g5_inventory

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    calls: list = []

    def _fetch(_session, names):
        calls.append(list(names))
        return dict(_ROW), dict(_FPS), "OFF"

    monkeypatch.setattr(_cdc_ui, "_fetch_start_position", _fetch)
    ui = _CardInputsUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(job or _job(None)), lambda: None, inventory=_g5_inventory(),
        migration_type=migration_type, session=_src_session(),
    )
    return ui, {b.text: b for b in ui.buttons}, calls


def _selected(state):
    from dsql_migrator.core.models import TableSelection

    state.set_selection(TableSelection(selected_tables=["app.orders"]))
    return state


_MANUAL_DISCLOSURE = "Manual — start from a later binlog position instead"


def test_a_stopped_cdc_only_stack_names_new_topics_for_a_later_start_point(monkeypatch):
    # CDC only, streamed then stopped (e.g. after a replay crash): a plain Start resumes with
    # schema_only. DELIBERATE G25 UPDATE (A2): a start point past where this stack streamed
    # needs new CDC topics -- the sink would apply its unapplied tail over the rows corrected
    # since, while the source skipped the changes before the point -- so the diagnosis names
    # Delete CDC infrastructure first and the Manual alternative on the NEW stack, the resume
    # card offers no Manual control, and a Manual point set anyway is refused with the route.
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log
    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER

    d1 = _diagnose_connector_log("src", "internal schema size 4, but UPDATE size 3")
    assert d1.index("Delete CDC infrastructure and deploy it again") < d1.index(
        "On the new stack you can instead choose Manual, Fetch current position and Use this "
        "start point"
    )
    state = _selected(_state())  # committed offset; CDC only: no watermark was seeded
    state.migration_type = MigrationType.CDC_ONLY
    ui, by, _calls = _live_card(monkeypatch, state, migration_type=MigrationType.CDC_ONLY)
    blob = " ".join(ui.texts)
    assert "Resuming from the last streamed position" in blob
    assert _MANUAL_DISCLOSURE not in blob
    assert "Manual — enter a binlog file:position" not in blob  # no radio on a resume
    assert "Fetch current position" not in by and "Use this start point" not in by
    # A Manual point set before (a restored session): refused, with the CDC only route.
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77)
    seed, origin = _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)
    assert origin == "manual" and _cdc_ui.cdc_start_resumes(state, seed, SourceType.MYSQL) is False
    after = " ".join(
        _live_card(monkeypatch, state, migration_type=MigrationType.CDC_ONLY)[0].texts
    )
    assert "Resuming from the last streamed position" not in after
    assert NEWER_POINT_HEADER in after and "Start point set" not in after
    assert "then Start CDC on the new stack" in after
    # Automatic (no Full Load watermark here) would continue the stream: said.
    assert "set the CDC start point back to Automatic" in after


def test_a_history_missing_resume_says_it_rebuilds_and_names_new_topics(monkeypatch):
    # The flagged next Start runs recovery after the start-point check; the card must not
    # claim "nothing re-applied, no choice". DELIBERATE G25 UPDATE: if that check names a
    # changed table, a start point after the change needs new CDC topics (A2), so the card
    # names that route and the topics' condition, and offers no Manual control.
    from dsql_migrator.core.cdc import mysql_snapshot_mode, start_point_check_due

    state = _selected(_state(deployed=("mysql-bin-changelog.000007", "108063011")))
    state.note_cdc_history_missing(True)
    seed, _origin = _cdc_ui._cdc_start_seed(state, _WM, SourceType.MYSQL)
    facts = _cdc_ui.mysql_start_facts(state, seed)
    assert facts.resumes and mysql_snapshot_mode(True, facts) == "recovery"
    assert start_point_check_due("recovery", facts)
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        ui, by, _calls = _live_card(monkeypatch, state, migration_type=mtype, job=_job())
        blob = " ".join(ui.texts)
        assert "Resuming from the last streamed position" in blob
        assert "(snapshot.mode=recovery)" in blob and "nothing re-applied" not in blob
        assert (
            "a start point after the change needs new CDC topics: Delete CDC infrastructure "
            "and deploy it again"
        ) in blob
        assert "gapless only while the topics still retain them" in blob
        assert "choose Manual below" not in blob
        assert _MANUAL_DISCLOSURE not in blob and "Fetch current position" not in by
    # A plain resume keeps its copy. (G25 round 6: it states the CDC topics' condition
    # instead of "nothing re-applied" -- the sink re-applies from its last commit, and what
    # it had not applied at the Stop comes only from the topics.)
    state.note_cdc_history_missing(False)
    ui, by, _calls = _live_card(
        monkeypatch, state, migration_type=MigrationType.FULL_LOAD_AND_CDC, job=_job()
    )
    blob = " ".join(ui.texts)
    assert "gapless only while the topics still retain them" in blob
    assert "nothing re-applied" not in blob and _MANUAL_DISCLOSURE not in blob


def test_a_postgres_resume_offers_no_binlog_route():
    ui = _CardUi()
    _cdc_ui._render_cdc_start_point_card(
        ui, _state(), lambda: None, wm_resume=None, wm_usable=False, effective_resume=None,
        mode="auto", session=None, source_type=SourceType.POSTGRES, resumes_from_offset=True,
    )
    assert _MANUAL_DISCLOSURE not in " ".join(ui.texts)


def test_the_regular_manual_card_records_the_captured_tables_at_fetch(monkeypatch):
    # The card (not only the inputs helper) must hand Fetch the captured table names:
    # without them the fetch records no fingerprint, no warning shows, and the first
    # Start of a CDC-only migration silently drops to an unchecked start.
    import asyncio

    state = _selected(_state(committed=False))
    state.set_cdc_start_mode("manual")
    ui, by, calls = _live_card(monkeypatch, state, migration_type=MigrationType.CDC_ONLY)
    assert _MANUAL_DISCLOSURE not in " ".join(ui.texts)  # the radio offers Manual here
    asyncio.run(by["Fetch current position"].on_click())
    assert calls == [["app.orders"]]
    by["Use this start point"].on_click()
    assert state.cdc_start_fingerprints() == _FPS


def test_after_a_reload_reset_choosing_manual_again_brings_the_fetched_point_back():
    # E1/D1's Manual route: Fetch + Use, then Drop & reload -- which resets the start point
    # to Automatic. The copy says to choose Manual again; the point and its fingerprints
    # are kept, so that start is checked against the fetched point, not the watermark.
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_RESETS_MANUAL
    from dsql_migrator.ui.data_migration import reset_stale_manual_start_point

    assert "back to Automatic" in RELOAD_RESETS_MANUAL
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=77, fingerprints=_FPS)
    assert reset_stale_manual_start_point(
        state, MigrationType.FULL_LOAD_AND_CDC, SourceType.MYSQL, log=lambda **_k: None
    )
    assert _cdc_ui._cdc_start_seed(state, _WM, SourceType.MYSQL)[1] == "full_load"
    state.set_cdc_start_mode("manual")
    seed, origin = _cdc_ui._cdc_start_seed(state, _WM, SourceType.MYSQL)
    assert origin == "manual" and seed.table_fingerprints == _FPS


# ---------------------------------------------------------------------------
# G19 review: a fetched GTID set is never stored without the position it was read at
# ---------------------------------------------------------------------------


def _manual_seed(state):
    return _cdc_ui._cdc_start_seed(state, None, SourceType.MYSQL)[0]


def test_a_fetched_set_saved_without_its_position_is_not_kept(monkeypatch):
    # Fetch (set read at :77), clear the position, Use: the set was read at :77, so it is
    # not saved alone -- a position typed on the next render would silently carry it.
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    ui.inputs["Binlog file:position"].set_value("")
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid is None
    assert ui.inputs["GTID set"].value == ""
    warned = [m for m, t in ui.notified if t == "warning"]
    assert warned[-1] == (
        "The GTID set shown was read at mysql-bin.000200:77, so it is not saved without "
        "that position. Enter a binlog file:position (on a source with gtid_mode=ON, with "
        "the GTID set executed at it), or Fetch current position again."
    )
    # The next render: an earlier position typed is saved WITHOUT a set (Start CDC then
    # asks for the set on a gtid_mode=ON source instead of skipping :5 to :77).
    ui2, by2 = _rerender(state)
    assert ui2.inputs["GTID set"].value == ""
    ui2.inputs["Binlog file:position"].set_value("mysql-bin.000200:5")
    by2["Use this start point"].on_click()
    seed = _manual_seed(state)
    assert (seed.binlog_position, seed.gtid_executed) == (5, None)
    assert not any("with its GTID set" in m for m, _t in ui2.notified)


def test_a_stored_pair_whose_position_is_cleared_drops_its_set(monkeypatch):
    _ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid == _GTID_NORM
    ui2, by2 = _rerender(state)
    ui2.inputs["Binlog file:position"].set_value("")
    by2["Use this start point"].on_click()
    assert state._cdc_start_gtid is None
    assert any("read at mysql-bin.000200:77, so it is not saved" in m
               for m, _t in ui2.notified)
    ui3, by3 = _rerender(state)
    ui3.inputs["Binlog file:position"].set_value("mysql-bin.000200:5")
    by3["Use this start point"].on_click()
    assert _manual_seed(state).gtid_executed is None


def test_a_mistyped_position_after_a_fetch_does_not_keep_the_set(monkeypatch):
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    ui.inputs["Binlog file:position"].set_value("mysql-bin.000200:5x")
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid is None
    negative = [m for m, t in ui.notified if t == "negative"]
    assert negative[-1] == (
        "Binlog must be 'file:position' (e.g. mysql-bin.000123:45678). The GTID set shown "
        "was cleared: it was read at mysql-bin.000200:77 and is kept only with that "
        "position."
    )
    ui2, by2 = _rerender(state)
    ui2.inputs["Binlog file:position"].set_value("mysql-bin.000200:5")
    by2["Use this start point"].on_click()
    assert _manual_seed(state).gtid_executed is None


def test_a_set_the_operator_typed_alone_is_kept_with_a_later_position(monkeypatch):
    # Theirs to assert (Start CDC checks it on a gtid_mode=ON source): not a fetched set.
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW), _FPS, "OFF"))
    ui.inputs["Binlog file:position"].set_value("")
    ui.inputs["GTID set"].set_value(f"{_U}:1-50")
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid == f"{_U}:1-50"
    ui2, by2 = _rerender(state)
    ui2.inputs["Binlog file:position"].set_value("mysql-bin.000200:5")
    by2["Use this start point"].on_click()
    assert _manual_seed(state).gtid_executed == f"{_U}:1-50"


def test_editing_only_the_set_right_after_a_fetch_drops_the_fetched_fingerprints(monkeypatch):
    # Same render as the Fetch: its cell still holds (:77, set, fingerprints). On a
    # gtid_mode=ON source the set IS the start point, so definitions read with another set
    # do not describe it -- the start-point check then warns instead of passing.
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW_GTID), _FPS, "ON"))
    ui.inputs["GTID set"].set_value(f" {_U}:1-700,\n{_V}:1-3 ")
    by["Use this start point"].on_click()
    assert state._cdc_start_gtid == f"{_U}:1-700,{_V}:1-3"  # typed set stored, normalized
    assert (state._cdc_start_binlog_file, state._cdc_start_binlog_pos) == (
        "mysql-bin.000200", 77
    )
    assert state.cdc_start_fingerprints() == {}
    assert not _manual_seed(state).table_fingerprints


def test_a_malformed_set_is_saved_with_a_warning_that_start_refuses_it(monkeypatch):
    ui, state, by, _calls = _manual_inputs(monkeypatch, (dict(_ROW), _FPS, "OFF"))
    ui.inputs["GTID set"].set_value(f"'{_U}:1-5'")
    by["Use this start point"].on_click()
    warned = [m for m, t in ui.notified if t == "warning"]
    assert warned[-1] == (
        "Start point saved, but its GTID set is not a valid GTID set (<server "
        "UUID>:<interval>, comma-separated), so Start CDC refuses it — correct it, or "
        "clear it if the source's gtid_mode is not ON."
    )


def test_an_attached_stack_offered_no_gtid_set_is_refused_by_the_start_dialog_check(
    monkeypatch,
):
    # The dialog's own chain on a reset session: Attach -> the phase probe caches the
    # stack's Watermark* -> _mysql_start_context (no start point: a resume) -> the GTID
    # check. The stack's last Start offered a binlog position and NO GTID set, so on a
    # gtid_mode=ON source it is refused although the session holds no start point.
    import dsql_migrator.core.cdc_deployer as dmod
    from dsql_migrator.ui.cdc_gtid_gate import GtidSourceState

    params = {"MskBootstrapServers": "", "DeploySink": "true", "SnapshotMode": "recovery",
              "WatermarkBinlogFile": "mysql-bin-changelog.000010", "WatermarkBinlogPos": "4",
              "WatermarkGtids": ""}

    class _D:
        stack_status = "UPDATE_COMPLETE"
        is_stable = True
        current_parameters = params

    class _Dep:
        def describe_stack_or_none(self, _n):
            return _D()

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(dmod, "build_cdc_stack_deployer", lambda *_a, **_k: _Dep())
    reads: list = []
    monkeypatch.setattr(
        _status, "read_source_gtid_state_once",
        lambda _s, gtid_set: reads.append(gtid_set) or GtidSourceState(mode="ON"),
    )
    session = SimpleNamespace(
        target_config=SimpleNamespace(region="us-east-1"), aws_profile=None,
        source_config=SimpleNamespace(source_type=SourceType.MYSQL), source_password="pw",
    )
    state = DataMigrationState()
    assert state.adopt_cdc_stack("dsql-cdc-stack")
    _status._probe_cdc_stack_phase(state, session)
    seed, origin, facts = _cdc_ui._mysql_start_context(state, _JM(None), None, session)[:3]
    assert seed is None and facts.resumes is True
    verdict = _cdc_ui._probe_cdc_gtid_start(state, session, seed=seed, facts=facts,
                                            origin=origin)
    assert verdict.kind == "no-gtid" and verdict.blocks
    assert verdict.stack_point == "binlog mysql-bin-changelog.000010:4"
    assert reads == [None]
    # The same stack offered a GTID set: continues after the set Debezium committed.
    params["WatermarkGtids"] = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    _status._probe_cdc_stack_phase(state, session)
    verdict = _cdc_ui._probe_cdc_gtid_start(state, session, seed=seed, facts=facts,
                                            origin=origin)
    assert verdict.kind == "proceed" and verdict.positioning == "gtid-resume"


def test_g20_cdc_deployed_gtid_mode_parses_the_stack_record():
    f = _status.cdc_deployed_gtid_mode
    assert f(None) is None and f({}) is None
    assert f({"StartGtidMode": ""}) is None and f({"StartGtidMode": "   "}) is None
    assert f({"StartGtidMode": " on "}) == "ON"
    assert f({"StartGtidMode": "off_permissive"}) == "OFF_PERMISSIVE"
    assert f({"StartGtidMode": "x" * 100}) == "X" * 32
    # Independent of Watermark*: a seedless Start blanks those, never this.
    assert f({"WatermarkBinlogFile": "", "StartGtidMode": "ON"}) == "ON"


def test_g20_the_phase_probe_caches_the_gtid_mode_record_and_adopt_clears_it():
    import dsql_migrator.core.cdc_deployer as dmod

    class _D:
        stack_status = "UPDATE_COMPLETE"
        is_stable = True

        def __init__(self, p):
            self.current_parameters = p

    class _Dep:
        def __init__(self, p):
            self._p = p

        def describe_stack_or_none(self, _n):
            return _D(self._p)

        def list_cdc_stacks(self):
            return []

    def probe(p, st=None):
        st = st or DataMigrationState()
        orig = dmod.build_cdc_stack_deployer
        dmod.build_cdc_stack_deployer = lambda *_a, **_k: _Dep(p)
        try:
            _status._probe_cdc_stack_phase(
                st, SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"),
                                    aws_profile=None)
            )
        finally:
            dmod.build_cdc_stack_deployer = orig
        return st

    seedless = probe({"MskBootstrapServers": "", "DeploySink": "true",
                      "WatermarkBinlogFile": "", "WatermarkBinlogPos": "",
                      "StartGtidMode": "on"})
    assert seedless.cdc_deployed_watermark is None  # Watermark* blank...
    assert seedless.cdc_deployed_gtid_mode == "ON"  # ...the record survives
    assert probe({"MskBootstrapServers": "", "DeploySink": "true",
                  "StartGtidMode": ""}).cdc_deployed_gtid_mode is None
    # An older stack (no such parameter) re-probed over a cached record: cleared.
    assert probe({"MskBootstrapServers": "", "DeploySink": "true"},
                 st=seedless).cdc_deployed_gtid_mode is None
    seedless.set_cdc_deployed_gtid_mode("OFF")
    assert seedless.adopt_cdc_stack("dsql-cdc-other")
    assert seedless.cdc_deployed_gtid_mode is None
    # A different stack name, and a stack found absent, drop it too.
    other = DataMigrationState()
    other.set_cdc_deployed_gtid_mode("OFF")
    assert other.set_cdc_stack_name("dsql-cdc-renamed")
    assert other.cdc_deployed_gtid_mode is None
    gone = DataMigrationState()
    gone.set_cdc_deployed_gtid_mode("OFF")
    gone.set_cdc_stack_phase("absent")
    assert gone.cdc_deployed_gtid_mode is None
