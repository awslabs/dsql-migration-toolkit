# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A runtime REPLICA IDENTITY read that crosses Start over lands nowhere (review A6d).

The source-schema re-check (G4) stamps each read with the migration it was claimed for
(``_cdc_catalog_drift_epoch``): Start over re-runs ``__init__`` on the SAME state object
(``reset_in_place``), so a read still in flight then is dropped when it lands and does not
release the new migration's claim. The older REPLICA IDENTITY re-check (G2) had neither: an
in-flight read folded its weak findings -- persisted, cleared only by another Start over, and
holding Cut over -- into the new migration, and released whatever claim the new migration had
taken. It also kept the claim when spawning the background read failed (the 2x-TTL watchdog
was the only release). Both are ported from the drift re-check, nothing else. Every claim
site is driven: the CDC poll, Check again, and the Validation / Cut over scheduler. Offline:
the source read is a double.
"""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timezone

import pytest

from tests.test_cdc_replica_identity_gate import (  # noqa: F401 - logged is a fixture
    KEY,
    T0,
    _probe,
    _read,
    _session,
    logged,
)


def _store_state():
    from dsql_migrator.ui.data_migration import DataMigrationStore

    store = DataMigrationStore()
    state = store.get_or_create("s1")
    state.cdc_deployed_message_key_columns = {"app.t": list(KEY)}
    state.set_cdc_ops_window_start(T0)
    return store, state


def _weak_read(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_status as status

    monkeypatch.setattr(
        status,
        "read_cdc_replica_identity_once",
        lambda _session, _scope: _probe(
            {"app.t": _read("d")}, started=datetime.now(timezone.utc)
        ),
    )


@pytest.fixture
def gated_run(monkeypatch):
    """``nicegui.run.io_bound`` held until the test releases it (the read is "in flight");
    it sets ``entered`` once the read is waiting there."""
    gate: dict = {}

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            gate["entered"].set()
            await gate["release"].wait()
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    return gate


@pytest.mark.parametrize("route", ["poll", "check-again", "scheduler"])
@pytest.mark.parametrize("start_over", [True, False])
def test_a_read_in_flight_during_start_over_lands_nowhere(
    monkeypatch, gated_run, logged, route, start_over
) -> None:
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        probe_replica_identity_now,
        schedule_replica_identity_probe,
        start_replica_identity_probe,
    )

    _weak_read(monkeypatch)
    store, state = _store_state()

    async def _scenario():
        gated_run["entered"] = asyncio.Event()
        gated_run["release"] = asyncio.Event()
        loop = asyncio.get_running_loop()
        if route == "poll":
            spawned: list = []
            monkeypatch.setattr(status, "spawn_detached", lambda work: spawned.append(work))
            assert start_replica_identity_probe(_session(), state) is True
            (work,) = spawned
            task = loop.create_task(work())
        elif route == "check-again":
            task = loop.create_task(probe_replica_identity_now(_session(), state))
        else:
            # Validation / Cut over: the render arms a one-shot timer; the claim is taken
            # inside its callback, which the recording ui.timer double hands to the test.
            timers: list = []
            ui = types.SimpleNamespace(
                timer=lambda interval, cb, once=False: timers.append((interval, cb, once))
            )
            schedule_replica_identity_probe(ui, _session(), state, lambda: None, evidence_at=None)
            ((interval, check, once),) = timers
            assert (interval, once) == (0.05, True)
            assert not state.cdc_replica_identity_probe_running  # nothing claimed on render
            task = loop.create_task(check())
        await asyncio.wait_for(gated_run["entered"].wait(), timeout=3)  # the read is in flight
        assert state.cdc_replica_identity_probe_running
        if start_over:
            store.reset_in_place("s1")
            assert state.cdc_replica_identity_probe_running is False
            assert state.begin_cdc_replica_identity_probe()  # the new migration's own read
        gated_run["release"].set()
        await task

    asyncio.run(_scenario())
    if start_over:
        # FAILED WITHOUT THE FIX: the old read opened app.t's finding in the new migration
        # (and logged it), stored itself as its last probe, and released the new claim.
        assert state.cdc_replica_identity_findings == {}
        assert state.cdc_replica_identity_last_probe is None and logged == []
        assert state.cdc_replica_identity_probe_running  # the new claim was not released
    else:
        assert "app.t" in state.cdc_replica_identity_findings
        assert len(logged) == 1 and not state.cdc_replica_identity_probe_running


def test_the_claim_is_released_when_spawning_the_read_fails(monkeypatch) -> None:
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import start_replica_identity_probe

    _weak_read(monkeypatch)
    _store, state = _store_state()

    def _spawn(_work):
        raise RuntimeError("spawn")

    monkeypatch.setattr(status, "spawn_detached", _spawn)

    async def _scenario():
        with pytest.raises(RuntimeError):
            start_replica_identity_probe(_session(), state)

    asyncio.run(_scenario())
    # FAILED WITHOUT THE FIX: the claim stayed taken until the 2x-TTL watchdog.
    assert state.cdc_replica_identity_probe_running is False


def test_a_stale_epoch_neither_lands_nor_releases() -> None:
    _store, state = _store_state()
    old = state._cdc_replica_identity_epoch
    _store.reset_in_place("s1")
    assert state._cdc_replica_identity_epoch is not old  # Start over gives a new one
    assert state.begin_cdc_replica_identity_probe()
    assert state.apply_cdc_replica_identity_probe(_probe({"app.t": _read("d")}), old) == []
    assert state.cdc_replica_identity_findings == {}
    state.finish_cdc_replica_identity_probe(old)
    assert state.cdc_replica_identity_probe_running
    # The current epoch (or none: the callers that predate it) still lands and releases.
    current = state._cdc_replica_identity_epoch
    assert state.apply_cdc_replica_identity_probe(_probe({"app.t": _read("d")}), current)
    state.finish_cdc_replica_identity_probe(current)
    assert state.cdc_replica_identity_probe_running is False
