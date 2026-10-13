# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 8, package CDC: the Start-side backstop (decision A2) after an app restart, and the
earlier-offer refusal's way on.

* A first MySQL Start that CloudFormation rolled back restores the stack's parameters, so the
  stack records only THAT it rolled back a Start (its ``UPDATE_ROLLBACK_*`` status, which the
  stack-phase probe reads again after an app restart) -- not the start offset that Start
  offered, which the app kept in memory. Round 7 judged a newer seed against that memory and
  let every seed through once it was gone: after a Full load only detour (Re-run -> W2) and a
  restart, Start CDC seeded W2 and the sink applied what the rolled-back Start had streamed over
  the reload, while the cards said "Ready to start CDC again ... continues from the position the
  rolled-back Start recorded". Now no seed can be shown to be at or before an offer the app no
  longer knows, so every seed is refused there, on the cards, the click, the dialog and the
  worker, with the route (new CDC topics first).
  DELIBERATE G26 UPDATE: since G26 the stack-phase probe reads that offer back from the stack's
  events (the offset seeder's resource properties), so after a restart a retry from it is taken
  again and a later or earlier point refused, as before the restart (tests/test_g26_cdc.py).
  The refusals pinned here hold while the offer stays unknown: ``_restarted``'s deployer reads
  no events unless given ``offer``, as when they cannot be read. Their header no longer calls
  the refused point newer: it may be that same one.
* The earlier-offer refusal ("Start from that offset or later") named a later point that A2
  refuses on a stack that has streamed (a rolled-back Start, or its connectors in place).

Each test FAILS on the code before this round, except the one pinning what stays
(``test_a_stack_that_never_streamed_keeps_or_later``). Fakes only: no AWS.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import MigrationMode, SourceType
from dsql_migrator.ui.cdc_start_point_gate import (
    NEWER_POINT_HEADER,
    OFFER_UNKNOWN_HEADER,
    new_topics_route,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType, _cdc_ui
from dsql_migrator.ui.data_migration import _cdc_status as st
from tests.test_g25_cdc_r7 import (
    _DEPLOYED,
    _JM,
    _W0,
    _W1,
    _W2,
    _capture,
    _never_started,
    _point_card,
    _rolled_back,
    _start_card,
)

MYSQL = SourceType.MYSQL
# The rolled-back Start's parameters, restored: the infra-only stack (no committed offset, no
# connectors, no recorded start offset).
_RESTORED = {
    "DeploySink": "false",
    "MskBootstrapServers": "",
    "WatermarkBinlogFile": "",
    "WatermarkBinlogPos": "",
    "TableIncludeList": "app.orders",
}
# The start offset the rolled-back Start offered (W1), as read from the stack's events (G26).
_OFFER = ("mysql-bin.000090", 4)
# DELIBERATE G26 UPDATE: "the app kept that offset only in memory" is no longer true -- the
# stack-phase probe also reads it from the stack's events. DELIBERATE G26 REPAIR UPDATE: the
# lead names an app restart and a Start made outside this session only as possible reasons
# (the worker reads the events too: tests/test_g26_repair_cdc.py).
_UNKNOWN_LEAD = (
    "CloudFormation rolled back this stack's last Start, and the start offset that Start "
    "offered is no longer known: the rollback restored the stack's parameters, this session "
    "holds no record of that offset (the app may have restarted since that Start, or that "
    "Start may have been made outside this session), and it could not be read from the "
    "stack's events."
)
_LINE = {
    MigrationType.FULL_LOAD_AND_CDC: (
        "This CDC stack's last Start rolled back and the start offset it offered is no longer "
        "known, so no start point can be checked against it: Delete CDC infrastructure and "
        "deploy it again, then Re-run Full Load choosing 'Drop & reload', then Start CDC. "
        "Nothing was changed."
    ),
    # DELIBERATE G26 UPDATE: the new stack starts from the Full Load's watermark, so the CDC
    # only route carries the Drop & reload condition (tests/test_g26_cdc.py).
    MigrationType.CDC_ONLY: (
        "This CDC stack's last Start rolled back and the start offset it offered is no longer "
        "known: Delete CDC infrastructure and deploy it again, then Start CDC (if the Full Load "
        "appended to rows already on the target, re-run it choosing 'Drop & reload' first). "
        "Nothing was changed."
    ),
}
_NEW_WAY = (
    "Start from that offset, or delete the CDC infrastructure and deploy it again to start "
    "from another point."
)
_OLD_WAY = (
    "Start from that offset or later, or delete the CDC infrastructure and deploy again to "
    "start earlier."
)


def _restarted(monkeypatch, migration_type=MigrationType.FULL_LOAD_AND_CDC, *,
               status="UPDATE_ROLLBACK_COMPLETE", offer=None):
    """The session after an app restart: a new state (no offered seed in memory), then the
    stack-phase probe reads the rolled-back stack again. ``offer`` (G26): the ``(file, pos)``
    its deployer reads from the stack's events; ``None`` -- it reads none, as when they cannot
    be read (the real read: tests/test_g26_cdc.py)."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.core.cdc_stack_deployer import CdcStackDiscovery

    class _Deployer:
        def describe_stack_or_none(self, _name):
            return CdcStackDiscovery(status, dict(_RESTORED), True)

        def list_cdc_stacks(self):
            return []

    if offer is not None:
        _Deployer.rolled_back_start_offer = lambda self, _name: offer
    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _Deployer())
    state = DataMigrationState()
    state.migration_type = migration_type
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.job_id = "job-1"
    st._probe_cdc_stack_phase(
        state, SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
    )
    return state


# --- A2 after an app restart: judged from what the stack records ------------------------------


def test_the_rolled_back_start_is_judged_from_the_stack_after_an_app_restart(monkeypatch) -> None:
    # Before the restart this process holds the seed the rolled-back Start offered (W1): the
    # retry from it is allowed, a newer one refused.
    before = _rolled_back()
    assert _cdc_ui.newer_start_point_refusal(before, _W1, MYSQL) is None
    assert _cdc_ui.newer_start_point_refusal(before, _W2, MYSQL) is not None
    # After it the probe reads the rollback again, and the offer is gone with the memory (and,
    # here, the stack's events do not give it either -- G26).
    after = _restarted(monkeypatch)
    assert after.cdc_stack_phase == "infra" and st.cdc_start_rolled_back(after)
    assert after.cdc_offered_seed is None and after.cdc_deployed_watermark is None
    assert st.cdc_stack_has_streamed(after)
    # FAILS WITHOUT THE FIX: no seed was refused (round 7: "unknown is not refused"), so the
    # detour's W2 was seeded under the rolled-back Start's streamed changes.
    for seed in (_W0, _W1, _W2):
        refusal = _cdc_ui.newer_start_point_refusal(after, seed, MYSQL)
        assert refusal is not None, seed
        (tone, _icon, header, body), line = refusal
        # DELIBERATE G26 UPDATE: the header no longer calls W1 -- that same point -- newer.
        assert (tone, header) == ("error", OFFER_UNKNOWN_HEADER)
        assert body.startswith(_UNKNOWN_LEAD)
        assert "None" not in body and "is later than" not in body
        assert body.endswith(
            f"To start CDC, use {new_topics_route(MigrationType.FULL_LOAD_AND_CDC)}. No "
            "connectors were changed."
        )
        assert line == _LINE[MigrationType.FULL_LOAD_AND_CDC]
    # A Start that seeds nothing is not judged here, and PostgreSQL is never refused.
    assert _cdc_ui.newer_start_point_refusal(after, None, MYSQL) is None
    assert _cdc_ui.newer_start_point_refusal(after, _W2, SourceType.POSTGRES) is None
    # Any rollback status counts, as the stack-phase probe reads it.
    for status in ("UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"):
        assert _cdc_ui.newer_start_point_refusal(
            _restarted(monkeypatch, status=status), _W1, MYSQL
        ) is not None, status
    # A stack whose last update did not roll back is a first Start: not refused.
    assert _cdc_ui.newer_start_point_refusal(
        _restarted(monkeypatch, status="UPDATE_COMPLETE"), _W2, MYSQL
    ) is None


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_after_a_restart_the_cards_refuse_and_claim_no_continuity(monkeypatch, mtype) -> None:
    # The detour (Full load only Re-run -> W2), a restart, then back to Full load + CDC -- or on
    # to CDC only.
    state = _restarted(monkeypatch, mtype)
    start = _start_card(state, _W2)
    blob = " ".join(start.texts)
    assert OFFER_UNKNOWN_HEADER in blob and _UNKNOWN_LEAD in blob
    assert f"To start CDC, use {new_topics_route(mtype)}." in blob
    # FAILS WITHOUT THE FIX: "Ready to start CDC again ... continues from the position the
    # rolled-back Start recorded, if it recorded one" over an enabled Start.
    assert "Ready to start CDC" not in blob
    assert "continues from the position the rolled-back Start recorded" not in blob
    assert start.start_disabled is True
    assert start.start_tooltip == _LINE[mtype]
    card = " ".join(_point_card(state, _W2).texts)
    assert OFFER_UNKNOWN_HEADER in card and _UNKNOWN_LEAD in card and "Action needed" in card
    assert "Automatic — from the Full Load watermark" in card
    assert "Automatic — from the Full Load watermark (recommended)" not in card
    assert "Starting again after a rolled-back Start" not in card
    assert "continues from the position the rolled-back Start recorded" not in card


@pytest.mark.parametrize(
    "mtype,seed,offer",
    [
        # DELIBERATE G26 UPDATE: the retry from the start point the rolled-back Start offered
        # (W1) is refused only while that offer stays unknown -- here the stack's events give
        # none. Read from them, the same retry is taken (tests/test_g26_cdc.py).
        (MigrationType.FULL_LOAD_AND_CDC, _W1, None),
        (MigrationType.CDC_ONLY, _W2, None),  # the detour's watermark, on to CDC only
        # G26: with the offer read from the stack's events (W1), the detour's later W2 is
        # refused against it (A2), no longer as unknown.
        (MigrationType.CDC_ONLY, _W2, _OFFER),
    ],
    ids=["retry, offer unknown", "detour, offer unknown", "detour, offer read"],
)
def test_after_a_restart_the_click_refuses_before_any_job(
    monkeypatch, mtype, seed, offer
) -> None:
    state = _restarted(monkeypatch, mtype, offer=offer)
    run = _capture(monkeypatch, state, seed)
    assert not run["submitted"] and run["kwargs"] == {}
    (notice,) = run["notices"]
    if offer is None:
        assert notice["header"] == OFFER_UNKNOWN_HEADER
        assert notice["body"].startswith(_UNKNOWN_LEAD)
    else:
        assert notice["header"] == NEWER_POINT_HEADER
        assert notice["body"].startswith(
            "CloudFormation rolled back this stack's last Start, which offered binlog "
            "mysql-bin.000090:4; "
        )
    assert new_topics_route(mtype) in notice["body"]


def test_the_worker_refuses_on_a_fresh_describe_that_shows_the_rollback() -> None:
    # The page read the stack before (never started); another session's Start rolled back
    # since. The worker judges ITS describe: the rollback, and an offer it never held.
    state = _never_started()
    refusal = _cdc_ui.newer_start_point_refusal(
        state, _W2, MYSQL, stack_params=dict(_RESTORED), stack_status="UPDATE_ROLLBACK_COMPLETE",
    )
    assert refusal is not None and refusal[0][3].startswith(_UNKNOWN_LEAD)
    first = str(_cdc_ui._start_refusal(state, refusal[0], "job-9", first_line=refusal[1]))
    first = first.splitlines()[0]
    assert first == f"Start CDC blocked: {_LINE[MigrationType.FULL_LOAD_AND_CDC]}"
    assert len(first) <= 300
    # The same describe without the rollback: a first Start, not refused.
    assert _cdc_ui.newer_start_point_refusal(
        state, _W2, MYSQL, stack_params=dict(_RESTORED), stack_status="UPDATE_COMPLETE",
    ) is None


def test_the_one_line_form_fits_a_job_error_for_either_type() -> None:
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        _notice, line = _cdc_ui._offer_unknown_refusal("the Full Load watermark", mtype)
        assert line == _LINE[mtype]
        assert len(f"Start CDC blocked: {line}") <= 300, mtype


def test_after_a_restart_the_dialog_refuses_and_runs_no_start_position_read(monkeypatch) -> None:
    import asyncio

    import nicegui

    from tests.test_ui_data_migration import _DialogUi

    state = _restarted(monkeypatch)
    reads: list[str] = []

    def _recorder(name):
        def _read(*_a, **_k):
            reads.append(name)
            return None

        return _read

    for name in ("_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap"):
        monkeypatch.setattr(_cdc_ui, name, _recorder(name))
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_pg_objects_gate_block", lambda *_a, **_k: None)
    ui = _DialogUi()
    bodies: dict = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        bodies[header] = body

    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(
            ui, state, lambda: None, session=None, job_manager=_JM(_W1), inventory=None,
        )
    )
    assert ("error", OFFER_UNKNOWN_HEADER) in ui.notices
    assert bodies[OFFER_UNKNOWN_HEADER].startswith(_UNKNOWN_LEAD)
    assert reads == []
    assert ui.buttons["Start CDC"].enabled is False


# --- the earlier-offer refusal names only a way on that Start CDC takes ------------------------


def _offered(phase: str, status: str, *, deployed=None) -> DataMigrationState:
    """A MySQL stack a Start offered W1 to (this process recorded it), with no committed
    offset."""
    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    state.set_cdc_stack_phase(phase, status=status)
    if deployed is not None:
        state.set_cdc_deployed_watermark(deployed)
    state.note_cdc_offered_seed(_W1)
    return state


@pytest.mark.parametrize(
    "phase,status,deployed",
    [
        ("infra", "UPDATE_ROLLBACK_COMPLETE", None),  # a first Start CloudFormation rolled back
        ("running", "UPDATE_COMPLETE", _DEPLOYED),  # that Start's connectors in place
    ],
)
def test_the_earlier_offer_refusal_drops_or_later_on_a_stack_that_has_streamed(
    phase, status, deployed
) -> None:
    state = _offered(phase, status, deployed=deployed)
    reason = _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, MYSQL)
    # FAILS WITHOUT THE FIX: "Start from that offset or later" -- a later point A2 refuses.
    assert reason is not None and _NEW_WAY in reason
    assert "or later" not in reason
    assert reason.endswith(" No connectors were changed.")
    # Both ways it names are ones Start CDC takes: that offset is not refused, a later one is.
    assert _cdc_ui.newer_start_point_refusal(state, _W1, MYSQL) is None
    assert _cdc_ui.newer_start_point_refusal(state, _W2, MYSQL) is not None


def test_a_stack_that_never_streamed_keeps_or_later() -> None:
    # A Start that failed after seeding with no rollback and no connectors (say, its update was
    # never applied): a later point IS taken there, so "or later" stays true.
    state = _offered("infra", "UPDATE_COMPLETE")
    assert not st.cdc_stack_has_streamed(state)
    reason = _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, MYSQL)
    assert reason is not None and _OLD_WAY in reason
    assert _cdc_ui.newer_start_point_refusal(state, _W2, MYSQL) is None


def test_the_click_refuses_an_earlier_manual_point_with_the_way_on(monkeypatch) -> None:
    state = _rolled_back()  # the rolled-back Start offered W1, this process recorded it
    run = _capture(
        monkeypatch, state, _W1, start_mode="manual", manual_binlog=("mysql-bin.000080", 4),
    )
    assert not run["submitted"]
    assert [n["header"] for n in run["notices"]] == ["This start point cannot be applied"]
    assert _NEW_WAY in run["notices"][0]["body"]
    assert "or later" not in run["notices"][0]["body"]
