# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 7, package CDC: the Start-side backstop of the new-start-point gate (decision A2),
and the CDC copy that named a route the gate refuses or claimed gaplessness unconditionally.

A2: the Full Load step refuses a Full Load that takes a new start point on a CDC stack that has
streamed, but a detour gets around it (the migration type switched to Full load only for a
Re-run, then back -- or on to CDC only). On MySQL the next Start CDC would then SEED the newer
watermark: the seeder moves the source forward while the sink group resumes from its last
commit on the same topics and applies its unapplied tail over the reload -- silently. So Start
CDC itself refuses a start point newer than the one a streamed stack last started from, in the
card, the dialog, the click and the worker (against a fresh describe), naming the route: Delete
CDC infrastructure and deploy it again, then (Full load + CDC) Re-run Full Load with Drop &
reload / (CDC only) Start CDC on the new stack. A resume (a seed at or before that point) and a
never-started stack are not refused. PostgreSQL is not refused here: its Start continues from
the stack's stored offset and slot whatever the job watermark says, so a Full load only detour
-- which leaves the slot alone -- replays the stream completely and converges.

Each test FAILS on the code before this round. Fakes only: no AWS.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import Watermark
from dsql_migrator.core.models import MigrationMode, SourceType, TableSelection
from dsql_migrator.ui.cdc_start_point_gate import (
    NEWER_POINT_HEADER,
    newer_point_line,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType, _cdc_ui
from dsql_migrator.ui.data_migration import _cdc_status as st

_NOW = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)
# The Full Load the stack was started from (W1), and the detour's newer one (W2).
_W1 = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000090", binlog_position=4)
_W2 = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000100", binlog_position=4)
_W0 = Watermark(snapshot_timestamp=_NOW, binlog_file="mysql-bin.000080", binlog_position=4)
_DEPLOYED = ("mysql-bin.000090", "4")
_ROUTE_FL = (
    "To start from a new start point, use Delete CDC infrastructure and deploy it again (the "
    "new stack starts with empty topics), then Re-run Full Load on the Full Load step choosing "
    "'Drop & reload', then Start CDC."
)
_ROUTE_CDC_ONLY = (
    "To start from a new start point, use Delete CDC infrastructure and deploy it again (the "
    "new stack starts with empty topics), then Start CDC on the new stack."
)


class _JM:
    def __init__(self, wm):
        self._job = SimpleNamespace(watermark=wm, status="DONE") if wm is not None else None

    def get_status(self, _job_id):
        if self._job is None:
            from dsql_migrator.core.job_manager import JobNotFoundError

            raise JobNotFoundError(_job_id)
        return self._job


def _stopped(*, deployed=_DEPLOYED, migration_type=MigrationType.FULL_LOAD_AND_CDC):
    """A MySQL stack that streamed and was stopped: committed offset, last started from W1."""
    state = DataMigrationState()
    state.migration_type = migration_type
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_cdc_has_committed_offset(True)
    if deployed is not None:
        state.set_cdc_deployed_watermark(deployed)
    state.job_id = "job-1"
    return state


def _never_started(**kw):
    state = _stopped(deployed=None, **kw)
    state.set_cdc_has_committed_offset(False)
    return state


def _rolled_back(offered=_W1):
    """A first Start CloudFormation rolled back: parameters restored (no committed offset)."""
    state = _never_started()
    state.set_cdc_stack_phase("infra", status="UPDATE_ROLLBACK_COMPLETE")
    if offered is not None:
        state.note_cdc_offered_seed(offered)
    return state


def _truth(seed, **kw):
    kw.setdefault("has_committed_offset", False)
    kw.setdefault("connectors_exist", False)
    kw.setdefault("rolled_back", False)
    kw.setdefault("deployed_watermark", None)
    kw.setdefault("source_type", SourceType.MYSQL)
    return st.newer_start_point(seed, **kw)


# --- the truth table ---------------------------------------------------------------------------


def test_a_newer_seed_on_a_stopped_stack_is_refused_and_a_resume_is_not() -> None:
    # Stopped (committed offset), last started from W1: W2 is newer -> refused.
    refused = _truth(_W2, has_committed_offset=True, deployed_watermark=_DEPLOYED)
    assert refused == st.NewerStartPoint(_DEPLOYED)
    # The same or an earlier seed: the seeder keeps the committed offset -- a resume.
    for seed in (_W1, _W0):
        assert _truth(seed, has_committed_offset=True, deployed_watermark=_DEPLOYED) is None
    # No seed (a resume of whatever is committed).
    assert _truth(None, has_committed_offset=True, deployed_watermark=_DEPLOYED) is None
    # Judged against no recorded start offset (a Start with no start point): not provably a
    # resume, so refused (last unknown).
    assert _truth(_W2, has_committed_offset=True) == st.NewerStartPoint(None)
    # Never started: a first Start, never refused here.
    assert _truth(_W2) is None
    # PostgreSQL: never refused (its Start continues from the stack's own offset and slot).
    assert _truth(
        _W2, has_committed_offset=True, deployed_watermark=_DEPLOYED,
        source_type=SourceType.POSTGRES,
    ) is None


def test_connectors_in_place_and_a_rolled_back_start_are_judged_against_their_record() -> None:
    # Retry CDC on an incomplete pipeline: refused only when provably past the deployed point.
    assert _truth(_W2, connectors_exist=True, deployed_watermark=_DEPLOYED) == (
        st.NewerStartPoint(_DEPLOYED)
    )
    assert _truth(_W1, connectors_exist=True, deployed_watermark=_DEPLOYED) is None
    assert _truth(_W2, connectors_exist=True) is None
    # A first Start CloudFormation rolled back (its parameters restored): judged against the
    # seed it offered, as this process recorded it; the same seed -- the retry -- is allowed.
    offered = ("mysql-bin.000090", 4)
    assert _truth(_W2, rolled_back=True, offered_seed=offered) == st.NewerStartPoint(
        offered, rolled_back=True
    )
    assert _truth(_W1, rolled_back=True, offered_seed=offered) is None
    # An unknown offer (an app restart): the stack records only the rollback, so no seed can
    # be shown to be at or before it -- every seed is refused (G25 round 8).
    for seed in (_W0, _W1, _W2):
        assert _truth(seed, rolled_back=True) == st.NewerStartPoint(None, rolled_back=True)


# --- the detour: Full load only, then back -- refused everywhere, with the route ----------------


class _ButtonUi:
    """Records text, and the Start CDC button's disabled state and tooltip."""

    def __init__(self):
        from tests.test_ui_data_migration import _StartButtonUi

        self._inner = _StartButtonUi()
        self.start_tooltip = ""
        outer = self

        class _El(_StartButtonUi._El):
            def tooltip(self, text="", *_a, **_k):
                if self._label == "Start CDC":
                    outer.start_tooltip = str(text)
                return self

        self._El = _El

    @property
    def texts(self):
        return self._inner.texts

    @property
    def start_disabled(self):
        return self._inner.start_disabled

    def button(self, text="", *_a, **_k):
        self._inner.texts.append(str(text))
        return self._El(self._inner, str(text))

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _start_card(state, wm):
    ui = _ButtonUi()
    _cdc_ui._render_cdc_start_button(ui, state, _JM(wm), lambda: None, inventory=None,
                                     session=None)
    return ui


def _point_card(state, wm):
    from tests.test_cdc_restart_start_point import _CardUi

    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(wm), lambda: None, inventory=None,
        migration_type=getattr(state, "migration_type", None), session=None,
    )
    return ui


@pytest.mark.parametrize("detour", [MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY])
def test_a_newer_watermark_after_a_detour_is_refused_on_the_cards(detour) -> None:
    # FAILS WITHOUT THE FIX: "Ready to start CDC ... begins streaming", "Automatic — gapless
    # from Full Load (recommended)" and an enabled Start -- the seeded start that moves a
    # streamed stack forward under its sink's unapplied tail.
    state = _stopped()
    # The detour: switched to Full load only (re-run -> W2), then back -- or on to CDC only.
    state.migration_type = detour
    state.migration_type = (
        MigrationType.FULL_LOAD_AND_CDC if detour is MigrationType.FULL_LOAD_ONLY else detour
    )
    start = _start_card(state, _W2)
    blob = " ".join(start.texts)
    assert NEWER_POINT_HEADER in blob
    assert "Ready to start CDC" not in blob and "begins streaming" not in blob
    assert start.start_disabled is True
    assert start.start_tooltip == newer_point_line(state.migration_type)
    route = _ROUTE_CDC_ONLY if detour is MigrationType.CDC_ONLY else _ROUTE_FL
    assert route in blob
    assert (
        "the position it committed is at or past binlog mysql-bin.000090:4, the start offset "
        "its last Start offered, and the Full Load watermark (binlog mysql-bin.000100:4"
    ) in blob
    card = " ".join(_point_card(state, _W2).texts)
    assert NEWER_POINT_HEADER in card and "Action needed" in card
    assert "gapless" not in card.split(NEWER_POINT_HEADER)[0]
    assert "Automatic — from the Full Load watermark" in card
    assert "Start point set" not in card


def test_the_one_line_refusal_fits_a_job_error() -> None:
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        line = f"Start CDC blocked: {newer_point_line(mtype)}"
        assert len(line) <= 300, len(line)
        assert "Delete CDC infrastructure and deploy it again" in line
    assert "Re-run Full Load choosing 'Drop & reload'" in newer_point_line(
        MigrationType.FULL_LOAD_AND_CDC
    )
    assert "Start CDC on the new stack" in newer_point_line(MigrationType.CDC_ONLY)


def _capture(monkeypatch, state, wm, **kw):
    from tests.test_ui_data_migration import _run_start_cdc_capture

    from dsql_migrator.ui.cdc_start_point_gate import StartPointVerdict

    state.set_selection(TableSelection(selected_tables=["orders"]))
    return _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, job_watermark=wm, state=state,
        catch=True, start_point_read=lambda *_a, **_k: StartPointVerdict(kind="ok"), **kw,
    )


def test_the_click_refuses_a_newer_watermark_before_any_job(monkeypatch) -> None:
    run = _capture(monkeypatch, _stopped(), _W2)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == [NEWER_POINT_HEADER]
    assert _ROUTE_FL in run["notices"][0]["body"]


def test_the_worker_refuses_on_a_fresh_describe_with_a_300_char_first_line(monkeypatch) -> None:
    # The page read the stack before it streamed (never started); another session started and
    # stopped it since. Retry CDC / a stale dialog reach the worker, which judges the FRESH
    # describe: stopped, last started from W1 -- so W2 is refused before anything reads.
    state = _never_started()
    run = _capture(
        monkeypatch, state, _W2,
        describe_params={
            "DeploySink": "true", "MskBootstrapServers": "",
            "WatermarkBinlogFile": "mysql-bin.000090", "WatermarkBinlogPos": "4",
        },
    )
    assert run["submitted"] and run["kwargs"] == {}  # run_cdc_start never called
    assert run["gtid_reads"] == []
    first = str(run["error"]).splitlines()[0]
    assert first == f"Start CDC blocked: {newer_point_line(MigrationType.FULL_LOAD_AND_CDC)}"
    assert len(first) <= 300
    recorded = getattr(state, "cdc_start_refusal_notice")
    assert recorded[3] == NEWER_POINT_HEADER and _ROUTE_FL in recorded[4]


def test_a_resume_and_a_first_start_are_not_refused(monkeypatch) -> None:
    # The never-started cards keep the gapless Automatic handoff (rendered first: the capture
    # below records notices instead of rendering them).
    assert "Ready to start CDC" in " ".join(_start_card(_never_started(), _W2).texts)
    assert "Automatic — gapless from Full Load (recommended)" in " ".join(
        _point_card(_never_started(), _W2).texts
    )
    resume = _capture(monkeypatch, _stopped(), _W1)
    assert resume["submitted"] and resume["error"] is None
    assert dict(resume["kwargs"]["params"].filled)["SnapshotMode"] == "schema_only"
    older = _capture(monkeypatch, _stopped(), _W0)
    assert older["submitted"] and older["error"] is None
    first = _capture(monkeypatch, _never_started(), _W2)
    assert first["submitted"] and first["error"] is None
    assert dict(first["kwargs"]["params"].filled)["SnapshotMode"] == "recovery"


def test_a_newer_manual_point_is_refused_and_automatic_named_as_the_way_to_continue() -> None:
    state = _stopped()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=4)
    card = " ".join(_point_card(state, _W1).texts)
    assert NEWER_POINT_HEADER in card
    assert "your Manual start point (binlog mysql-bin.000200:4)" in card
    # Automatic seeds W1 -- at the stack's start offset, a resume -- so it is the way on.
    assert "To continue this stream instead, set the CDC start point back to Automatic." in card
    # FAILS WITHOUT THE FIX: the card said the point "is applied only if it is past".
    assert "is applied only if it is past" not in card
    # With the detour's W2 as Automatic's seed, Automatic would be refused too: not offered.
    card2 = " ".join(_point_card(state, _W2).texts)
    assert "set the CDC start point back to Automatic" not in card2


def test_the_dialog_refuses_and_skips_the_start_position_reads(monkeypatch) -> None:
    import asyncio

    import nicegui

    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from tests.test_ui_data_migration import _DialogUi

    from dsql_migrator.ui.cdc_gtid_gate import G1_HEADER, G2_HEADER
    from dsql_migrator.ui.cdc_start_point_gate import E1_HEADER, W1_HEADER

    # The stand-ins RECORD each read and return None. They used to raise, and the dialog wraps
    # every probe in ``except Exception`` (advisory / "the job re-checks"), which swallowed the
    # AssertionError -- so a dropped ``newer is None`` guard still passed (G25 round 8).
    reads: list[str] = []

    def _recorder(name):
        def _read(*_a, **_k):
            reads.append(name)
            return None

        return _read

    probes = ("_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap")
    for name in probes:
        monkeypatch.setattr(cdc_ui, name, _recorder(name))
    monkeypatch.setattr(cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_pg_objects_gate_block", lambda *_a, **_k: None)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)

    def _open(state):
        ui = _DialogUi()
        ui.bodies = {}

        def _notice(_ui, *, tone="info", icon=None, header="", body=""):
            ui.notices.append((tone, header))
            ui.bodies[header] = body

        monkeypatch.setattr(cdc_ui, "_render_notice", _notice)
        asyncio.run(
            cdc_ui._open_cdc_start_dialog(
                ui, state, lambda: None, session=None, job_manager=_JM(_W2), inventory=None,
            )
        )
        return ui

    # The control: a Start that is not refused (a stack never started) runs every read, so the
    # stand-ins are wired -- the refused run below would see a read that ran.
    _open(_never_started())
    assert sorted(set(reads)) == sorted(probes)
    reads.clear()
    ui = _open(_stopped())
    assert reads == []
    headers = [header for _tone, header in ui.notices]
    for header in (
        G1_HEADER, G2_HEADER, E1_HEADER, W1_HEADER, "The start point's binary log has been purged",
    ):
        assert header not in headers, header
    assert ("error", NEWER_POINT_HEADER) in ui.notices
    assert _ROUTE_FL in ui.bodies[NEWER_POINT_HEADER]
    assert ui.buttons["Start CDC"].enabled is False


def test_a_rolled_back_first_start_refuses_a_newer_seed_and_words_its_retry() -> None:
    # Newer than the seed the rolled-back Start offered (this process recorded it): refused.
    refused = " ".join(_start_card(_rolled_back(), _W2).texts)
    assert NEWER_POINT_HEADER in refused
    assert "CloudFormation rolled back this stack's last Start, which offered binlog " \
           "mysql-bin.000090:4" in refused
    # The retry from the same watermark: allowed, and gapless only while the topics retain what
    # that Start streamed (FAILS WITHOUT THE FIX: "Ready to start CDC", "gapless from Full Load").
    retry = " ".join(_start_card(_rolled_back(), _W1).texts)
    assert "Ready to start CDC again" in retry and NEWER_POINT_HEADER not in retry
    condition = (
        "continues from the position the rolled-back Start recorded, if it recorded one, else "
        "from the start point above; any changes that Start streamed are applied from the CDC "
        "topics, so this is gapless only while the topics still retain them"
    )
    assert condition in retry
    card = " ".join(_point_card(_rolled_back(), _W1).texts)
    assert "Automatic — from the Full Load watermark (recommended)" in card
    assert "gapless from Full Load" not in card
    assert "Starting again after a rolled-back Start" in card
    line = _cdc_ui._cdc_start_detail(
        "dsql-cdc-x", _W1, mode="auto", source_type=SourceType.MYSQL, rolled_back=True
    )
    assert "gapless from the Full Load watermark" not in line
    assert "this stack's last Start was rolled back by CloudFormation" in line
    assert "gapless only while the topics still retain them" in line


def test_postgres_is_never_refused_by_the_backstop() -> None:
    state = _stopped()
    assert _cdc_ui.newer_start_point_refusal(state, _W2, SourceType.POSTGRES) is None


# --- the copy: a stack that has streamed gets new CDC topics first (rules A/B), no unconditional
# "gapless" (rule D) -------------------------------------------------------------------------


def test_every_replay_diagnosis_names_new_topics_before_any_new_start_point() -> None:
    from dsql_migrator.core import cdc_deployer as dep
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_RESETS_MANUAL

    needles = (
        "isn't known to this connector", dep._UNPARSEABLE_DDL_NEEDLE,
        dep._ANONYMOUS_GTID_NEEDLE, dep._PURGED_GTIDS_NEEDLE, "internal schema size",
        "trying to change column",
    )
    for needle in needles:
        (guidance,) = [g for n, g in dep._CONNECTOR_LOG_DIAGNOSES if n == needle]
        assert dep._NEW_START_POINT_ROUTE in guidance, needle
        delete = guidance.index("Delete CDC infrastructure and deploy it again")
        assert delete < guidance.index("Re-run Full Load for all tables"), needle
        assert delete < guidance.index("On the new stack you can instead choose Manual"), needle
        # FAILS WITHOUT THE FIX: "Stop CDC, then give CDC a start point ...: Re-run Full Load".
        assert "start point after the statement: Re-run" not in guidance, needle
        assert RELOAD_RESETS_MANUAL in guidance, needle
    # The resume remedies are kept: ON_PERMISSIVE, and the history rebuild.
    assert "ON_PERMISSIVE until cut over" in dep._ANONYMOUS_GTID_GUIDANCE
    assert "it rebuilds the history from the source tables" in dep._UNKNOWN_TABLE_GUIDANCE
    # The purged-GTIDs resume states the topics' condition (rule D).
    assert (
        "with no gap while the CDC topics still retain the changes the sink had not applied "
        "when CDC stopped"
    ) in dep._PURGED_GTIDS_GUIDANCE


def test_the_partition_quota_redeploy_names_what_it_costs_per_engine() -> None:
    from dsql_migrator.core import cdc_deployer as dep
    from dsql_migrator.core import cdc_pg_slot as pg

    (log_text,) = [g for n, g in dep._CONNECTOR_LOG_DIAGNOSES
                   if n == "quota exceeded for maximum number of partitions"]
    for text in (dep._PARTITION_QUOTA_GUIDANCE.format(status="UPDATE_ROLLBACK_COMPLETE"),
                 log_text.format(connector="c", statement="")):
        # FAILS WITHOUT THE FIX: "(CDC resumes from the source position you set)".
        assert "CDC resumes from the source position you set" not in text
        assert "with CDC only and neither, it begins at the source's current position" in text
        assert (
            "On a PostgreSQL source, deleting drops the replication slot: "
            + pg.pg_cdc_route(None, started=True, cdc_only=None)
        ) in text


def test_the_start_point_check_names_new_topics_on_a_stack_whose_connectors_started() -> None:
    from dsql_migrator.ui.cdc_start_point_gate import StartPointVerdict, start_point_notice

    changed = StartPointVerdict(kind="changed", changed=(("app.t", "columns added"),))
    unverified = StartPointVerdict(kind="unverified", unverified=("app.t",))
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        for verdict in (changed, unverified):
            body = start_point_notice(
                verdict, point_label="the Full Load watermark (binlog f:1)", coord="binlog f:1",
                migration_type=mtype, streamed=True,
            )[3]
            delete = body.index("Delete CDC infrastructure and deploy it again")
            if mtype is MigrationType.CDC_ONLY:
                assert delete < body.index("choose Manual on the new stack"), body
            else:
                assert delete < body.index("Re-run Full Load"), body
            # A resume is a stack that streamed, whatever ``streamed`` says.
            assert start_point_notice(
                verdict, point_label="p", coord="binlog f:1", migration_type=mtype,
                resumes=True,
            )[3].count("Delete CDC infrastructure and deploy it again") == 1
        # A stack never started keeps today's copy (no Delete step).
        plain = start_point_notice(
            changed, point_label="p", coord="binlog f:1", migration_type=mtype
        )[3]
        assert "Delete CDC infrastructure" not in plain


def test_the_gtid_check_names_the_redeploy_on_a_resume_and_after_a_rolled_back_start() -> None:
    from tests.test_cdc_gtid_start import _all_refusals, _g20_refusals

    from dsql_migrator.ui.cdc_gtid_gate import ROLLED_BACK_NEW_POINT, gtid_start_notice

    refusals = {**_all_refusals(), **_g20_refusals()}

    def body(name, **kw):
        verdict, origin = refusals[name]
        return gtid_start_notice(
            verdict, point_label="the Full Load watermark (binlog f:1)", coord="binlog f:1",
            origin=origin, migration_type=kw.pop("mtype", MigrationType.FULL_LOAD_AND_CDC),
            **kw,
        )[3]

    redeploy = "Or Delete CDC infrastructure on the CDC step, deploy it again"
    for name in ("G1-resume", "G2-resume"):  # FAILS WITHOUT THE FIX (default lead)
        assert redeploy in body(name), name
    for name in ("G1-FL", "G2-seeded"):
        assert redeploy not in body(name), name  # a stack never started: no Delete step
        assert redeploy in body(name, rolled_back=True), name
    for name, mtype in (("G4-purged-FL", MigrationType.FULL_LOAD_AND_CDC),
                        ("G4-purged-FL", MigrationType.CDC_ONLY),
                        ("G4-purged-manual", MigrationType.FULL_LOAD_AND_CDC),
                        ("G4-unexecuted-manual", MigrationType.CDC_ONLY),
                        ("G1-manual", MigrationType.FULL_LOAD_AND_CDC)):
        text = body(name, mtype=mtype, rolled_back=True)
        assert ROLLED_BACK_NEW_POINT in text, (name, mtype)
        assert ROLLED_BACK_NEW_POINT not in body(name, mtype=mtype), (name, mtype)
    # G2S seeded: no way to keep the point on this stack, only the redeploy -- named as the
    # way, not as an alternative ("Or ...") to the dropped one.
    seeded = body("G2S-seeded")
    assert "ON_PERMISSIVE until cut over" not in seeded
    assert "later than it. Delete CDC infrastructure on the CDC step" in seeded
    assert " Or Delete CDC infrastructure" not in seeded


def test_the_cards_state_the_topics_condition_and_no_same_stack_manual_route() -> None:
    from tests.test_cdc_restart_start_point import _CardUi

    # The last-causes "Ready to resume CDC" states the topics' condition (FAILS WITHOUT THE FIX).
    state = _stopped()
    state._cdc_connector_diagnoses = None
    from dsql_migrator.core.cdc_deployer import ConnectorLogDiagnosis

    diagnoses = {"dsql-cdc-stack-debezium-source": ConnectorLogDiagnosis(
        headline="Debezium could not parse a DDL statement in the binary log", guidance="g.")}
    state.cdc_connector_diagnoses = diagnoses
    blob = " ".join(_start_card(state, _W1).texts)
    assert "Ready to resume CDC" in blob
    assert "gapless only while the topics still retain them" in blob
    # The tooltip of the card says the stack continues from its recorded position.
    ui = _CardUi()
    _cdc_ui._render_cdc_start_point_card(
        ui, _stopped(), lambda: None, wm_resume=None, wm_usable=False, effective_resume=None,
        mode="auto", session=None, source_type=SourceType.MYSQL, resumes_from_offset=True,
    )
    tips = " ".join(ui.tooltips)
    assert "no gap, no overlap" not in tips
    assert "This stack has streamed, so Start CDC continues from its recorded stream " \
           "position" in tips


def test_retry_cdc_and_the_danger_zone_state_their_conditions() -> None:
    import inspect

    src = inspect.getsource(_cdc_ui._render_cdc_partial_actions)
    assert "Buffered changes let the new sink catch up — no gap." not in src
    assert "gapless only while the topics still retain them (their retention" in src
    start = inspect.getsource(_cdc_ui._render_cdc_start_action)
    assert "or when the steps above call for a " in start and "new start point" in start


def test_a_rolled_back_binlog_purge_and_gtid_only_card_name_new_topics(monkeypatch) -> None:
    import dsql_migrator.core.watermark as wm_mod
    import dsql_migrator.ui.connect as connect
    from tests.test_cdc_restart_start_point import _CardUi

    from dsql_migrator.ui.cdc_gtid_gate import ROLLED_BACK_NEW_POINT

    class _Engine:
        def connect(self):
            class _C:
                def __enter__(self):
                    return object()

                def __exit__(self, *_a):
                    return False

            return _C()

        def dispose(self):
            return None

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine())
    )
    monkeypatch.setattr(wm_mod, "list_binary_logs", lambda _c: ["mysql-bin.000095"])
    session = SimpleNamespace(source_config=SimpleNamespace(), source_password="pw")
    reason = _cdc_ui._probe_binlog_resume_gap(_rolled_back(), _JM(_W1), session)
    assert reason and reason.endswith(ROLLED_BACK_NEW_POINT)
    never = _cdc_ui._probe_binlog_resume_gap(_never_started(), _JM(_W1), session)
    assert never and ROLLED_BACK_NEW_POINT not in never
    # A GTID-only watermark on a stack whose last Start rolled back.
    gtid_only = Watermark(snapshot_timestamp=_NOW, gtid_executed="u:1-5")
    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, _rolled_back(offered=None), _JM(gtid_only), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    assert "This stack's last Start rolled back, so either way needs new CDC topics first" in (
        " ".join(ui.texts)
    )


def test_start_over_and_the_exclusion_runbooks_name_the_new_topics_order() -> None:
    from tests.test_workflow import _render_start_over_dialog

    from dsql_migrator.ui.start_over import _cdc_teardown_banner_copy

    ui = _render_start_over_dialog(
        cdc_deployed=True, on_reset_cdc=lambda *_a: None, cdc_stack_name="dsql-cdc-x",
    )
    tiles = " ".join(ui.texts)
    assert "Recommended." not in tiles  # FAILS WITHOUT THE FIX
    assert "A fresh Full load + CDC run needs new CDC topics: choose Delete instead." in tiles
    done = _cdc_teardown_banner_copy({"state": "done", "kind": "stop", "stack": "dsql-cdc-x"})
    assert "it resumes from where streaming left off" not in done[2]
    assert "a new Full Load start point on it needs Delete CDC infrastructure and deploy it " \
           "again first" in done[2]
    from dsql_migrator.core.assessment_strategist import _cdc_recovery_context

    text = " ".join(_cdc_recovery_context("MySQL").split())
    assert "before the new stack's first Start CDC: once that stack has streamed, a new Full " \
           "Load start point needs another Delete and redeploy" in text


def test_the_ai_facts_carry_whether_the_stack_has_streamed() -> None:
    import inspect

    from dsql_migrator.core.cdc_assist import CdcReadinessSignals, build_cdc_readiness_prompt
    from dsql_migrator.ui import ai_tools

    src = inspect.getsource(ai_tools)
    assert '"stack_has_streamed": bool(cdc_stack_has_streamed(_dm))' in src
    assert "watermark_handoff is where CDC resumes from after Full Load." not in src
    prompt = build_cdc_readiness_prompt(CdcReadinessSignals(gtid_enabled=False), [])
    assert "gapless resume" not in prompt
    assert "GTID enabled (Debezium positions by GTID set): no" in prompt
