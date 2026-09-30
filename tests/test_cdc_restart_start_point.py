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
_WM = Watermark(
    snapshot_timestamp=_NOW,
    binlog_file="mysql-bin-changelog.000007",
    binlog_position=108063011,
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
    cdc_only = probe({"MskBootstrapServers": "", "DeploySink": "true",
                      "WatermarkBinlogFile": "", "WatermarkBinlogPos": ""})
    assert cdc_only.cdc_deployed_watermark is None
    # Adopting another stack drops it (it belongs to the previous stack).
    seeded.adopt_cdc_stack("dsql-cdc-other")
    assert seeded.cdc_deployed_watermark is None


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


def test_start_button_after_a_new_full_load_is_a_watermark_start_not_a_resume():
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui, _state(), _JM(_job()), lambda: None,
                                     inventory=None, session=None)
    blob = " ".join(ui.texts)
    assert "Ready to resume CDC" not in blob
    assert "continues from exactly where streaming stopped" not in blob
    assert "Ready to start CDC" in blob
    assert "Set the CDC start point above first." not in blob


def test_start_point_card_after_a_new_full_load_shows_the_watermark_start():
    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, _state(), _JM(_job()), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    blob = " ".join(ui.texts)
    assert "Resuming from the last streamed position" not in blob
    assert "Automatic — gapless from Full Load (recommended)" in blob


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
