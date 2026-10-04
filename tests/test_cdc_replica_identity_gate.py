# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G2: the runtime REPLICA IDENTITY re-check for re-keyed PostgreSQL tables.

A re-keyed table's DELETE carries the key's added column only under REPLICA IDENTITY FULL;
below it the delete can be silently lost (on infrastructure deployed by an earlier version, or
before the source-key unique index is valid -- G15). These tests pin the fold (finding
transitions), the scope and throttle of the background read, the cut-over gate's hold and
release rules, and the copy. Offline: every source read is a double.
"""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead
from dsql_migrator.core.models import (
    CdcReplicaIdentityFinding,
    CdcReplicaIdentityWaiver,
    SourceType,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)

T0 = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
T_WEAK = datetime(2026, 9, 1, 11, 0, tzinfo=timezone.utc)
T_FIX = datetime(2026, 9, 1, 11, 30, tzinfo=timezone.utc)
KEY = ("tenant_id", "id")


def _read(identity="d", pk=("id",), weak=0, names=()):
    return RekeyedIdentityRead(identity, tuple(pk), weak, tuple(names))


def _probe(reads, *, scope=None, ok=True, started=T_WEAK, problem=None, error_class=None):
    from dsql_migrator.ui.cdc_replica_identity_gate import CdcReplicaIdentityProbe

    return CdcReplicaIdentityProbe(
        started_at=started,
        finished_at=started + timedelta(seconds=1),
        ok=ok,
        problem=problem,
        error_class=error_class,
        scope=scope if scope is not None else {t: KEY for t in reads},
        reads=dict(reads),
    )


def _finding(*, first=T_WEAK, restored=None, by=None, identity="d", key=KEY, pk=("id",),
             weak=0, names=()):
    return CdcReplicaIdentityFinding(
        key_columns=list(key), primary_key=list(pk), first_weak_at=first,
        restored_at=restored, restored_by=by if restored is not None else None,
        identity=identity, weak_partitions=weak, weak_partition_names=list(names),
    )


def _session(*, pg=True, password=True, host="db.example.internal"):
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import SourceConnectionConfig

    config = SourceConnectionConfig(
        source_type=SourceType.POSTGRES if pg else SourceType.MYSQL,
        host=host, port=5432 if pg else 3306, database="app", username="u",
    )
    return SimpleNamespace(
        source_config=config,
        source_password=SecretValue("pw-secret-123") if password else None,
        restored_source_type=None,
    )


def _state(*, deployed=None, session_keys=None, findings=None, evidence=True):
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    if session_keys is not None:
        state.set_cdc_message_key_columns(session_keys)
    if deployed is not None:
        state.cdc_deployed_message_key_columns = dict(deployed)
    if findings:
        state.cdc_replica_identity_findings = dict(findings)
    if evidence:
        state.set_cdc_ops_window_start(T0)
    return state


@pytest.fixture
def logged(monkeypatch):
    import dsql_migrator.core.activity_log as act

    lines: list[dict] = []
    monkeypatch.setattr(
        act, "log_activity",
        lambda category, action, **kw: lines.append({"category": category, "action": action, **kw}),
    )
    return lines


# ---------------------------------------------------------------------------
# The fold
# ---------------------------------------------------------------------------


def test_a_full_read_creates_nothing():
    from dsql_migrator.ui.cdc_replica_identity_gate import fold_replica_identity_probe

    findings, transitions = fold_replica_identity_probe({}, _probe({"app.t": _read("f")}))
    assert findings == {} and transitions == []


def test_a_weak_read_opens_one_finding_and_a_repeat_is_silent(logged):
    from dsql_migrator.core.activity_log import ActivityStatus
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        log_replica_identity_transitions,
    )

    findings, transitions = fold_replica_identity_probe({}, _probe({"app.t": _read("d")}))
    (finding,) = findings.values()
    assert finding.first_weak_at == T_WEAK and finding.restored_at is None
    assert finding.key_columns == list(KEY) and finding.primary_key == ["id"]
    assert finding.identity == "d" and finding.missing == ["tenant_id"]
    assert [t.kind for t in transitions] == ["opened"]
    log_replica_identity_transitions(transitions)
    (line,) = logged
    assert line["status"] is ActivityStatus.FAILURE and line["target"] == "app.t"
    assert "ALTER TABLE app.t REPLICA IDENTITY FULL" in line["detail"]
    assert "REPLICA IDENTITY DEFAULT" in line["detail"]
    # The same weak state read again: nothing changes, nothing is logged.
    again, quiet = fold_replica_identity_probe(
        findings, _probe({"app.t": _read("d")}, started=T_WEAK + timedelta(minutes=2))
    )
    assert quiet == [] and again["app.t"] is finding


def test_full_restores_and_a_new_drop_reopens(logged):
    from dsql_migrator.core.activity_log import ActivityStatus
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        log_replica_identity_transitions,
    )

    opened, _ = fold_replica_identity_probe({}, _probe({"app.t": _read("d")}))
    restored, transitions = fold_replica_identity_probe(
        opened, _probe({"app.t": _read("f")}, started=T_FIX)
    )
    assert restored["app.t"].restored_at == T_FIX + timedelta(seconds=1)  # the read's finish
    assert restored["app.t"].restored_by == "full"
    assert restored["app.t"].first_weak_at == T_WEAK
    assert [t.kind for t in transitions] == ["restored"]
    log_replica_identity_transitions(transitions)
    assert logged[-1]["status"] is ActivityStatus.WARNING
    assert logged[-1]["action"] == "re-keyed table's REPLICA IDENTITY is FULL again"
    # Restored stays restored on a further FULL read.
    still, none = fold_replica_identity_probe(restored, _probe({"app.t": _read("f")}))
    assert none == [] and still["app.t"].restored_at == restored["app.t"].restored_at
    later = T_FIX + timedelta(hours=1)
    reopened, transitions = fold_replica_identity_probe(
        restored, _probe({"app.t": _read("n")}, started=later)
    )
    assert [t.kind for t in transitions] == ["reopened"]
    assert reopened["app.t"].first_weak_at == later and reopened["app.t"].restored_at is None
    assert reopened["app.t"].identity == "n"


def test_a_live_primary_key_that_covers_the_key_closes_as_not_rekeyed():
    from dsql_migrator.ui.cdc_replica_identity_gate import fold_replica_identity_probe

    opened, _ = fold_replica_identity_probe({}, _probe({"app.t": _read("d")}))
    closed, transitions = fold_replica_identity_probe(
        opened, _probe({"app.t": _read("d", pk=("tenant_id", "id"))}, started=T_FIX)
    )
    assert [t.kind for t in transitions] == ["not-rekeyed"]
    assert closed["app.t"].restored_by == "not-rekeyed"


def test_a_failed_read_or_a_missing_row_changes_nothing():
    from dsql_migrator.ui.cdc_replica_identity_gate import fold_replica_identity_probe

    opened, _ = fold_replica_identity_probe({}, _probe({"app.t": _read("d")}))
    failed, t1 = fold_replica_identity_probe(
        opened, _probe({}, scope={"app.t": KEY}, ok=False, problem="read-failed")
    )
    assert t1 == [] and failed == opened
    missing, t2 = fold_replica_identity_probe(opened, _probe({}, scope={"app.t": KEY}))
    assert t2 == [] and missing["app.t"].restored_at is None
    # ...and a missing row never OPENS one either.
    none, t3 = fold_replica_identity_probe({}, _probe({}, scope={"app.t": KEY}))
    assert none == {} and t3 == []


def test_partitions_are_stored_bounded():
    from dsql_migrator.ui.cdc_replica_identity_gate import fold_replica_identity_probe

    names = [f"app.t_p{i}" for i in range(5)]
    findings, _ = fold_replica_identity_probe(
        {}, _probe({"app.t": _read("f", weak=37, names=names)})
    )
    assert findings["app.t"].weak_partitions == 37
    assert findings["app.t"].weak_partition_names == names
    with pytest.raises(Exception):
        CdcReplicaIdentityFinding(
            key_columns=["a"], first_weak_at=T0, weak_partition_names=[str(i) for i in range(6)]
        )


def test_no_activity_line_carries_a_secret_host_or_value(logged):
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        log_replica_identity_transitions,
    )

    opened, t1 = fold_replica_identity_probe({}, _probe({"app.t": _read("d", weak=2, names=["app.t_p1", "app.t_p2"])}))
    restored, t2 = fold_replica_identity_probe(opened, _probe({"app.t": _read("f")}, started=T_FIX))
    log_replica_identity_transitions(t1 + t2)
    assert len(logged) == 2
    for line in logged:
        text = " ".join(str(v) for v in line.values())
        assert "pw-secret-123" not in text and "db.example.internal" not in text
        assert len(line["detail"]) <= 600


# ---------------------------------------------------------------------------
# Scope, possibility, throttle
# ---------------------------------------------------------------------------


def test_a_mysql_session_has_no_scope_and_is_never_read():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        replica_identity_probe_possible,
        replica_identity_scope,
    )

    state = _state(deployed={"app.t": list(KEY)}, session_keys={"app.t": list(KEY)})
    assert replica_identity_scope(state, _session(pg=False)) == {}
    assert replica_identity_probe_possible(state, _session(pg=False)) is False
    assert replica_identity_scope(state, _session()) == {"app.t": list(KEY)}


def test_the_deployed_map_wins_and_the_session_map_is_limited_to_the_selection():
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_scope

    s = _session()
    state = _state(session_keys={"app.a": ["x", "id"], "app.b": ["y", "id"]})
    assert replica_identity_scope(state, s) == {"app.a": ["x", "id"], "app.b": ["y", "id"]}
    state.selection = TableSelection(selected_tables=["app.a"])
    assert replica_identity_scope(state, s) == {"app.a": ["x", "id"]}
    state.cdc_deployed_message_key_columns = {"app.b": ["y", "id"]}
    assert replica_identity_scope(state, s) == {"app.b": ["y", "id"]}
    state.cdc_deployed_message_key_columns = {}  # deployed with no re-key at all
    assert replica_identity_scope(state, s) == {}


def test_a_sink_keyed_table_stays_in_the_replica_identity_scope(logged):
    """G15: a stack that keys a re-keyed table on its SOURCE primary key lists it in
    SinkKeyColumns, not MessageKeyColumns. It is still re-keyed -- its deletes can still miss
    while the source-key unique index builds -- so the re-check keeps it in scope and an open
    finding is not closed as "not-rekeyed"."""
    from dsql_migrator.core.cdc import deployed_rekey_map
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_scope

    s = _session()
    state = _state(session_keys={}, findings={"app.t": _finding()})
    deployed = deployed_rekey_map(
        {"MessageKeyColumns": "", "SinkKeyColumns": r"app\.t:tenant_id,id"}
    )
    state.set_cdc_deployed_message_key_columns(deployed, now=T_FIX)
    assert state.cdc_replica_identity_findings["app.t"].restored_at is None
    assert replica_identity_scope(state, s) == {"app.t": list(KEY)}
    assert logged == []  # nothing closed, nothing logged
    # Restored FULL later, the routed table stays in scope (it is not a "not re-keyed" table).
    state.cdc_replica_identity_findings = {}
    assert replica_identity_scope(state, s) == {"app.t": list(KEY)}


def test_an_open_finding_stays_in_scope_and_a_restored_one_does_not():
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_scope

    s = _session()
    state = _state(session_keys={}, findings={"app.t": _finding()})
    assert replica_identity_scope(state, s) == {"app.t": list(KEY)}
    state.cdc_replica_identity_findings = {"app.t": _finding(restored=T_FIX, by="full")}
    assert replica_identity_scope(state, s) == {}


def test_a_table_the_last_read_graded_not_rekeyed_is_dropped():
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_scope

    s = _session()
    state = _state(session_keys={"app.t": ["b", "a"], "app.u": list(KEY)})
    state.cdc_replica_identity_last_probe = _probe(
        {"app.t": _read("d", pk=("a", "b")), "app.u": _read("f")},
        scope={"app.t": ("b", "a"), "app.u": KEY},
    )
    assert replica_identity_scope(state, s) == {"app.u": list(KEY)}
    # ...but every read still asks about it (same statement), so the covered scope never
    # shrinks and re-grows between reads -- no second read per TTL.
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        replica_identity_probe_due,
        replica_identity_read_scope,
    )

    read_scope = replica_identity_read_scope(state, s)
    assert read_scope == {"app.t": ["b", "a"], "app.u": list(KEY)}
    assert not replica_identity_probe_due(
        state.cdc_replica_identity_last_probe, read_scope, None, T_WEAK + timedelta(seconds=30)
    )
    # A different key since that read: graded again.
    state.set_cdc_message_key_columns({"app.t": ["c", "a"], "app.u": list(KEY)})
    assert "app.t" in replica_identity_scope(state, s)


def test_when_a_read_is_due():
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_probe_due as due

    scope = {"app.t": KEY}
    last = _probe({"app.t": _read("f")}, started=T0)
    assert due(None, scope, None, T0)
    assert due(last, {"app.t": KEY, "app.u": KEY}, None, T0 + timedelta(seconds=5))
    assert due(last, scope, T0 + timedelta(seconds=1), T0 + timedelta(seconds=5))
    assert due(last, scope, None, T0 + timedelta(seconds=121))
    assert not due(last, scope, None, T0 + timedelta(seconds=119))
    assert not due(last, scope, T0 - timedelta(seconds=1), T0 + timedelta(seconds=5))
    # A failed read is not retried before the TTL either.
    failed = _probe({}, scope=scope, ok=False, problem="read-failed", started=T0)
    assert not due(failed, scope, None, T0 + timedelta(seconds=60))


def test_the_claim_is_single_flight_with_a_watchdog():
    from dsql_migrator.ui.cdc_replica_identity_gate import CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS

    state = _state()
    assert state.begin_cdc_replica_identity_probe(now_monotonic=100.0) is True
    assert state.begin_cdc_replica_identity_probe(now_monotonic=101.0) is False
    assert state.begin_cdc_replica_identity_probe(
        now_monotonic=100.0 + 2 * CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS + 1
    ) is True
    state.finish_cdc_replica_identity_probe()
    assert state.cdc_replica_identity_probe_running is False


class _FakeRun:
    calls = 0

    @classmethod
    async def io_bound(cls, fn, *a, **k):
        cls.calls += 1
        return fn(*a, **k)


@pytest.fixture
def fake_run(monkeypatch):
    _FakeRun.calls = 0
    fake = types.ModuleType("nicegui")
    fake.run = _FakeRun  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    return _FakeRun


def test_the_poll_starts_one_read_per_ttl_and_force_bypasses_it(monkeypatch, fake_run, logged):
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import start_replica_identity_probe

    reads: list[dict] = []

    def _once(session, scope):
        reads.append(dict(scope))
        return _probe({"app.t": _read("d")}, started=datetime.now(timezone.utc))

    monkeypatch.setattr(status, "read_cdc_replica_identity_once", _once)
    s = _session()
    state = _state(deployed={"app.t": list(KEY)})

    async def scenario():
        started = [start_replica_identity_probe(s, state) for _ in range(5)]
        for _ in range(20):
            await asyncio.sleep(0)
        later = [start_replica_identity_probe(s, state) for _ in range(5)]
        for _ in range(20):
            await asyncio.sleep(0)
        forced = start_replica_identity_probe(s, state, force=True)
        for _ in range(20):
            await asyncio.sleep(0)
        return started, later, forced

    started, later, forced = asyncio.run(scenario())
    assert started == [True, False, False, False, False]  # single flight
    assert later == [False] * 5  # within the TTL
    assert forced is True
    assert len(reads) == 2 and fake_run.calls == 2
    assert "app.t" in state.cdc_replica_identity_findings
    assert [line["action"] for line in logged].count(
        "re-keyed table's REPLICA IDENTITY is below FULL: its deletes can be lost"
    ) == 1  # one line per transition, not per read
    assert state.cdc_replica_identity_probe_running is False


def test_without_a_password_nothing_is_read(monkeypatch, fake_run):
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import start_replica_identity_probe

    monkeypatch.setattr(status, "read_cdc_replica_identity_once", lambda *a: pytest.fail("read"))
    state = _state(deployed={"app.t": list(KEY)})

    async def scenario():
        return start_replica_identity_probe(_session(password=False), state)

    assert asyncio.run(scenario()) is False and fake_run.calls == 0


def test_the_blocking_read_needs_a_password_and_keeps_only_the_error_class(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_status as status

    probe = status.read_cdc_replica_identity_once(_session(password=False), {"app.t": KEY})
    assert probe.ok is False and probe.problem == "no-source-connection"

    disposed: list[int] = []

    class _Engine:
        def connect(self):
            raise ConnectionError("could not connect to db.example.internal with pw-secret-123")

        def dispose(self):
            disposed.append(1)

    seen: dict = {}

    def _factory(password, *, read_timeout_seconds=None):
        seen["timeout"] = read_timeout_seconds
        return lambda _cfg: _Engine()

    monkeypatch.setattr(status, "make_source_engine_factory", _factory)
    failed = status.read_cdc_replica_identity_once(_session(), {"app.t": KEY})
    assert failed.ok is False and failed.problem == "read-failed"
    assert failed.error_class == "ConnectionError"
    assert "db.example" not in repr(failed) and "pw-secret" not in repr(failed)
    assert disposed == [1] and seen["timeout"] == 10


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


def _item(table="app.t", *, checksum=True, matched=True):
    return TableValidationResult(
        table=table, source_row_count=1, target_row_count=1 if matched else 0,
        row_count_match=matched, checksum_match=checksum, matched=matched,
    )


def _report(*items, mode=ValidationMode.CHECKSUM):
    return ValidationReport.build(mode=mode, items=list(items))


_D = object()


def _gate(*, findings=_D, scope=_D, report=_D, run_start=T_FIX + timedelta(minutes=5),
          release="clean", explained=(), last_probe=_D, has_source=True, waiver=None,
          cdc_in_use=True, evidence_at=None, check_probe=True, is_pg=True,
          migration_type=None, overrides=None, cdc_evidence=True):
    from dsql_migrator.ui.cdc_replica_identity_gate import cdc_replica_identity_cutover_gate

    return cdc_replica_identity_cutover_gate(
        findings={"app.t": _finding(restored=T_FIX, by="full")} if findings is _D else findings,
        scope={"app.u": KEY} if scope is _D else scope,
        cdc_in_use=cdc_in_use, cdc_evidence=cdc_evidence, is_pg=is_pg, check_probe=check_probe,
        report=_report(_item("app.t"), _item("app.u")) if report is _D else report,
        release=release, explained_tables=explained, compared_run_start=run_start,
        compared_overrides=overrides, evidence_at=evidence_at,
        last_probe=(
            _probe({"app.u": _read("f")}, started=T_FIX + timedelta(minutes=10))
            if last_probe is _D else last_probe
        ),
        has_source_connection=has_source, waiver=waiver, migration_type=migration_type,
    )


def test_weak_finding_holds_even_with_a_value_verified_checksum():
    """The gap itself: deletes can keep being lost, so no CHECKSUM can release it."""
    gate = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY},
                 last_probe=_probe({"app.t": _read("d")}, started=T_FIX + timedelta(minutes=10)),
                 run_start=T_WEAK + timedelta(hours=2))
    assert [i.table for i in gate.weak] == ["app.t"]
    assert gate.blocks and gate.held and not gate.can_waive
    assert gate.recheckable == ()  # never offered for a re-check


def test_a_restored_table_needs_a_checksum_started_after_the_fix_plus_a_minute():
    early = _gate(run_start=T_FIX + timedelta(seconds=61))  # restored_at = T_FIX
    assert early.issues == () and early.resolved == ("app.t",) and not early.held
    margin = _gate(run_start=T_FIX + timedelta(seconds=60))
    assert [i.reason for i in margin.issues] == ["compared-before"] and margin.held
    assert margin.recheckable == ("app.t",) and not margin.can_waive
    counted = _gate(report=_report(_item("app.t", checksum=None), _item("app.u"),
                                   mode=ValidationMode.ROW_COUNT))
    assert [i.reason for i in counted.issues] == ["row-count-only"] and counted.held


def test_tables_validation_cannot_prove_are_waivable_and_a_waiver_is_scoped():
    left_out = _gate(report=_report(_item("app.u")))
    assert [i.reason for i in left_out.issues] == ["not-validated"]
    assert left_out.blocks and left_out.can_waive
    gap = _gate(report=_report(_item("app.t", matched=False, checksum=False), _item("app.u")),
                release="accepted", explained=("app.t",))
    assert [i.reason for i in gap.issues] == ["accepted-gap"] and gap.can_waive
    waiver = CdcReplicaIdentityWaiver(tables={"app.t": T_FIX}, waived_at=T_FIX + timedelta(hours=1))
    waived = _gate(report=_report(_item("app.u")), waiver=waiver)
    assert waived.waived and not waived.blocks and not waived.held and not waived.can_waive
    # A later drop and a new restore: the old waiver no longer covers it.
    renewed = {"app.t": _finding(first=T_FIX + timedelta(hours=2),
                                 restored=T_FIX + timedelta(hours=3), by="full")}
    stale = _gate(findings=renewed, report=_report(_item("app.u")), waiver=waiver)
    assert not stale.waived and stale.blocks


def test_pending_holds_only_for_a_read_cut_over_waits_on():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        PROBE_STATE_FAILED,
        PROBE_STATE_OK,
        PROBE_STATE_PENDING,
        PROBE_STATE_UNAVAILABLE,
    )

    clean = {"findings": {}, "run_start": T_FIX}
    none_yet = _gate(last_probe=None, **clean)
    assert none_yet.probe_state == PROBE_STATE_PENDING and none_yet.held
    stale = _gate(evidence_at=T_FIX + timedelta(minutes=20), **clean)  # read before evidence
    assert stale.probe_state == PROBE_STATE_PENDING and stale.held
    uncovered = _gate(scope={"app.u": KEY, "app.v": KEY}, **clean)
    assert uncovered.probe_state == PROBE_STATE_PENDING
    fresh = _gate(**clean)
    assert fresh.probe_state == PROBE_STATE_OK and not fresh.held
    # FAIL-OPEN by design: no password, a failed read, a table not found -- never held.
    unavailable = _gate(has_source=False, last_probe=None, **clean)
    assert unavailable.probe_state == PROBE_STATE_UNAVAILABLE and not unavailable.held
    assert unavailable.unchecked
    failed = _gate(last_probe=_probe({}, scope={"app.u": KEY}, ok=False, problem="read-failed",
                                     error_class="OperationalError",
                                     started=T_FIX + timedelta(minutes=10)), **clean)
    assert failed.probe_state == PROBE_STATE_FAILED and not failed.held and failed.unchecked
    unread = _gate(last_probe=_probe({}, scope={"app.u": KEY}, started=T_FIX + timedelta(minutes=10)),
                   **clean)
    assert unread.unread == ("app.u",) and not unread.held and unread.unchecked
    # Validation's view has no probe state at all.
    assert _gate(check_probe=False, last_probe=None, **clean).probe_state is None


def test_the_gate_is_inert_without_findings_or_scope():
    assert not _gate(findings={}, scope={}).applies
    assert not _gate(findings={}, cdc_in_use=False).applies
    assert not _gate(findings={}, is_pg=False).applies
    # A finding left from earlier CDC still holds in Full load only (like G1's ledger).
    leftover = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY}, cdc_in_use=False)
    assert leftover.applies and leftover.held


def test_the_forced_checksum_set_and_the_proven_set():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cdc_checksum_forced_tables,
        replica_identity_proven_tables,
    )
    from dsql_migrator.ui.validation import ValidationState

    state = _state(findings={"app.t": _finding(restored=T_FIX, by="full"), "app.w": _finding()})
    state.fold_cdc_dlq_ledger([])
    assert cdc_checksum_forced_tables(state) == {"app.t", "app.w"}
    vs = ValidationState()
    assert replica_identity_proven_tables(state.cdc_replica_identity_findings, vs) == set()
    vs.mark_run_started()
    vs._run_started_wall = T_FIX + timedelta(minutes=2)
    vs.set_result(_report(_item("app.t"), _item("app.w")))
    assert replica_identity_proven_tables(state.cdc_replica_identity_findings, vs) == {"app.t"}


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------


def test_the_weak_notice_names_the_table_the_fix_and_each_partition():
    from dsql_migrator.ui.cdc_replica_identity_gate import monitor_weak_notice

    header, body = monitor_weak_notice({"app.t": _finding()}, has_password=True)
    assert header == (
        "Deletes on re-keyed tables can be lost — REPLICA IDENTITY is not FULL on the source"
    )
    # G15: true on new and older CDC infrastructure alike -- a delete CAN miss (the sink
    # deletes by the source key once its unique index is valid), never "are being lost".
    assert (
        "These tables have the composite primary key chosen in Schema Conversion, and a "
        "DELETE carries that key's added column only under REPLICA IDENTITY FULL. While it is "
        "below FULL, a DELETE can leave its row on Aurora DSQL with no error and no dead "
        "letter, and an UPDATE that changes a key column can be lost too."
    ) in body
    assert "CDC keys these tables" not in body and "are being lost" not in body
    assert "app.t (REPLICA IDENTITY DEFAULT)" in body
    assert "ALTER TABLE <table> REPLICA IDENTITY FULL" in body and "each partition" in body
    assert "found at 2026-09-01 11:00 UTC" in body and "Check again" in body
    assert "reconnect the source on the Connect step" not in body
    _h, nopw = monitor_weak_notice({"app.t": _finding()}, has_password=False)
    assert "reconnect the source on the Connect step" in nopw
    assert monitor_weak_notice({"app.t": _finding(restored=T_FIX, by="full")},
                               has_password=True) is None


def test_g15_copy_says_a_change_can_be_lost_on_any_infrastructure():
    """G15: on CDC infrastructure with sink plugin v51+ a re-keyed table is keyed on its source
    primary key and its deletes land once the unique index on it is valid; on older
    infrastructure (and before that) they still miss. Every G2 string must be true on both --
    "can be lost", never "are being lost" -- and keep the fix and the Check again action."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        ReplicaIdentityTransition,
        _transition_line,
        cutover_unchecked_notice,
        cutover_weak_notice,
        monitor_restored_notice,
        monitor_unchecked_notice,
        replica_identity_verdict_hold,
    )

    header, body = cutover_weak_notice(_gate(findings={"app.t": _finding()}, scope={"app.t": KEY}))
    assert header == "Cut over is held: deletes on 1 re-keyed table(s) can be lost"
    assert body.startswith(
        "REPLICA IDENTITY on app.t (REPLICA IDENTITY DEFAULT) is below FULL on the source, so "
        "their DELETEs (and UPDATEs that change the source primary key) can miss their rows on "
        "Aurora DSQL with no error and no dead letter, and an UPDATE of the added key column "
        "alone is dead-lettered and leaves the old row."
    ), body
    # An UPDATE of the added key column alone IS dead-lettered (23505 on the unique index over
    # the source key): it must not be lumped in with the silent "no dead letter" case.
    assert "(and UPDATEs that change a key column) can miss" not in body
    assert "REPLICA IDENTITY FULL" in body and "Check again" in body
    status, action, detail = _transition_line(
        ReplicaIdentityTransition(kind="opened", table="app.t", finding=_finding())
    )
    assert status == "FAILURE"
    assert action == "re-keyed table's REPLICA IDENTITY is below FULL: its deletes can be lost"
    assert detail == (
        "REPLICA IDENTITY DEFAULT; the composite key (tenant_id, id) adds tenant_id, which a "
        "DELETE carries only under REPLICA IDENTITY FULL, so deletes on app.t can miss Aurora "
        "DSQL with no error and no dead letter. Fix: ALTER TABLE app.t REPLICA IDENTITY FULL "
        "on the source (each partition too). Cut over is held."
    )
    _tone, _h, info = monitor_unchecked_notice({"app.t": KEY}, None, has_password=False)
    assert "because a DELETE on them can be lost silently if it drops below FULL" in info
    unchecked = cutover_unchecked_notice(
        _gate(findings={}, scope={"app.t": KEY}, has_source=False)
    )
    assert unchecked is not None
    assert "because a DELETE on them can be lost silently while it is below FULL" in unchecked[1]
    # The CDC monitor's restored notice and Validation's verdict while still below FULL: a
    # delete made then may or may not have landed (G15), so neither says it certainly missed.
    restored_h, restored_b = monitor_restored_notice(
        {"app.t": _finding(restored=T_FIX, by="full")}, set(), None
    )
    assert restored_h == (
        "REPLICA IDENTITY is FULL again — deletes made before the fix may be missing"
    )
    assert (
        "Deletes made in that time may not have removed their rows on Aurora DSQL, and nothing "
        "replays them."
    ) in restored_b, restored_b
    verdict = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY},
                    last_probe=_probe({"app.t": _read("d")}, started=T_FIX + timedelta(minutes=10)),
                    check_probe=False)
    tone, verdict_h, verdict_b = replica_identity_verdict_hold(verdict, total=2, as_of="now")
    assert tone == "error"  # the hold and its severity are unchanged
    assert "so their DELETEs can miss Aurora DSQL and this match may not last" in verdict_b
    for text in (header, body, action, detail, info, unchecked[1], restored_h, restored_b,
                 verdict_h, verdict_b):
        assert "are being lost" not in text and "do not reach Aurora DSQL" not in text
        assert "is lost silently" not in text
        assert "did not remove" not in text and "keep missing" not in text
        assert "will not last" not in text


def test_the_repair_line_follows_the_migration_type():
    from dsql_migrator.ui.cdc_replica_identity_gate import repair_line
    from dsql_migrator.ui.data_migration import MigrationType

    both = repair_line(MigrationType.FULL_LOAD_AND_CDC)
    assert "Drop & reload" in both and "let CDC catch up" in both and "Stop CDC" in both
    only = repair_line(MigrationType.CDC_ONLY)
    assert "by hand" in only and "Drop & reload" not in only


def test_the_wrong_row_sentence_only_when_the_key_omits_a_source_pk_column():
    from dsql_migrator.ui.cdc_replica_identity_gate import cutover_weak_notice

    covered = cutover_weak_notice(_gate(findings={"app.t": _finding()}, scope={"app.t": KEY}))
    assert "different row" not in covered[1]
    omits = _finding(key=("tenant_id", "a"), pk=("a", "b"))
    assert omits.key_omits_source_pk
    notice = cutover_weak_notice(_gate(findings={"app.t": omits}, scope={"app.t": ("tenant_id", "a")}))
    assert "The key of app.t does not contain the whole source primary key" in notice[1]
    assert "can also remove a different row" in notice[1]


def test_lists_are_capped():
    from dsql_migrator.ui.cdc_replica_identity_gate import identity_label, monitor_weak_notice

    findings = {f"app.t{i}": _finding() for i in range(8)}
    _h, body = monitor_weak_notice(findings, has_password=True)
    assert "(+2 more)" in body and "app.t7" not in body
    label = identity_label(_finding(identity="f", weak=37, names=[f"p{i}" for i in range(5)]))
    assert label == "FULL on the table, but 37 partition(s) are not: p0, p1, p2, p3, p4 (+32 more)"
    label = identity_label(_finding(identity="i", weak=2, names=["p0", "p1"]))
    assert label == "REPLICA IDENTITY USING INDEX, and 2 partition(s) are not FULL: p0, p1"


def test_the_log_clauses_for_each_state():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        ReplicaIdentityGate,
        replica_identity_ack_log_clause as ack,
        replica_identity_held_log_clause as held,
    )

    assert ack(ReplicaIdentityGate()) == "" and held(ReplicaIdentityGate()) is None
    weak = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY})
    assert held(weak) == "; cut over held: REPLICA IDENTITY below FULL on app.t"
    must = _gate(run_start=T_FIX)
    assert held(must).startswith("; cut over held: 1 re-keyed table(s) lack a CHECKSUM match")
    resolved = _gate()
    assert held(resolved) is None
    assert ack(resolved) == (
        "; re-keyed tables' REPLICA IDENTITY: below FULL earlier on app.t, each cleared by a "
        "CHECKSUM match after the fix"
    )
    ok = _gate(findings={}, run_start=T_FIX)
    assert ack(ok) == "; re-keyed tables' REPLICA IDENTITY: FULL at 2026-09-01 11:40:00 UTC"
    assert ack(_gate(findings={}, has_source=False, last_probe=None)) == (
        "; re-keyed tables' REPLICA IDENTITY: not re-checked (source password not in memory)"
    )
    failed = _gate(findings={}, last_probe=_probe({}, scope={"app.u": KEY}, ok=False,
                                                    problem="read-failed",
                                                    error_class="OperationalError",
                                                    started=T_FIX + timedelta(minutes=10)))
    assert ack(failed).endswith("not re-checked (read failed: OperationalError)")
    unread = _gate(findings={}, last_probe=_probe({}, scope={"app.u": KEY},
                                                    started=T_FIX + timedelta(minutes=10)))
    assert ack(unread).endswith("not re-checked (not found: app.u)")
    waiver = CdcReplicaIdentityWaiver(tables={"app.t": T_FIX}, waived_at=T_FIX)
    waived = _gate(report=_report(_item("app.u")), waiver=waiver)
    assert ack(waived) == "; REPLICA IDENTITY check WAIVED (CHECKSUM proof for app.t)"


def test_a_pure_primary_key_reorder_is_never_read_or_named():
    """Pure reorders must be unchanged: with the inventory's primary key known, the table is
    out of scope before any read (the Start prerequisite's own "nothing to grade")."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cdc_replica_identity_cutover_gate,
        monitor_unchecked_notice,
        replica_identity_probe_possible,
        replica_identity_read_scope,
        replica_identity_scope,
    )

    state = _state(deployed={"app.t": ["b", "a"], "app.u": list(KEY)})
    state.source_primary_key_for = {"app.t": ["a", "b"], "app.u": ["id"]}.get
    s = _session(password=False)
    assert replica_identity_scope(state, s) == {"app.u": list(KEY)}
    assert replica_identity_read_scope(state, s) == {"app.u": list(KEY)}
    _t, _h, body = monitor_unchecked_notice(
        replica_identity_scope(state, s), None, has_password=False
    )
    assert "app.t" not in body
    state.cdc_deployed_message_key_columns = {"app.t": ["b", "a"]}
    assert replica_identity_scope(state, s) == {}
    assert not replica_identity_probe_possible(state, _session())
    inert = cdc_replica_identity_cutover_gate(
        findings={}, scope=replica_identity_scope(state, s), cdc_in_use=True, cdc_evidence=True,
        is_pg=True, check_probe=True, report=None, release="clean",
    )
    assert not inert.applies


def test_a_page_waits_for_a_read_another_tab_holds_and_then_refreshes(monkeypatch, fake_run):
    import dsql_migrator.ui.cdc_replica_identity_gate as gate_mod
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import schedule_replica_identity_probe

    monkeypatch.setattr(status, "read_cdc_replica_identity_once", lambda *a: pytest.fail("read"))
    monkeypatch.setattr(gate_mod, "_WAIT_FOR_OTHER_READ_SECONDS", 5.0)
    state = _state(deployed={"app.t": list(KEY)})
    timers: list = []
    ui = SimpleNamespace(timer=lambda _i, cb, once=False: timers.append(cb))
    refreshes: list = []
    assert state.begin_cdc_replica_identity_probe() is True  # another tab's read runs
    schedule_replica_identity_probe(ui, _session(), state, lambda: refreshes.append(1),
                                    evidence_at=None)
    (check,) = timers

    async def scenario():
        task = asyncio.get_running_loop().create_task(check())
        await asyncio.sleep(0.1)
        # The other read lands: a new finding, and the claim is released.
        state.apply_cdc_replica_identity_probe(_probe({"app.t": _read("d")},
                                                      started=datetime.now(timezone.utc)))
        state.finish_cdc_replica_identity_probe()
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(scenario())
    assert refreshes == [1]


def test_a_read_that_landed_before_the_timer_fired_still_refreshes_the_page(monkeypatch, fake_run):
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import schedule_replica_identity_probe

    monkeypatch.setattr(status, "read_cdc_replica_identity_once", lambda *a: pytest.fail("read"))
    state = _state(deployed={"app.t": list(KEY)})
    timers: list = []
    ui = SimpleNamespace(timer=lambda _i, cb, once=False: timers.append(cb))
    refreshes: list = []
    schedule_replica_identity_probe(ui, _session(), state, lambda: refreshes.append(1),
                                    evidence_at=None)
    (check,) = timers
    # The CDC poll's read lands first (a fresh weak read).
    state.apply_cdc_replica_identity_probe(
        _probe({"app.t": _read("d")}, started=datetime.now(timezone.utc))
    )
    asyncio.run(check())
    assert refreshes == [1]


# ---------------------------------------------------------------------------
# Review fixes: copy that must match what the screens actually allow
# ---------------------------------------------------------------------------


def _failed(started=T_FIX, error_class="OperationalError"):
    return _probe({}, scope={"app.t": KEY}, ok=False, problem="read-failed",
                  error_class=error_class, started=started)


def test_an_open_finding_names_a_failed_re_read_so_check_again_is_not_a_dead_end():
    """With a finding open, a failed re-read (a rotated password, a changed security group)
    showed nowhere: Check again redrew the same red notice. Now every weak notice says so."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        PROBE_STATE_FAILED,
        cutover_weak_notice,
        monitor_weak_notice,
        replica_identity_verdict_hold,
    )

    _h, body = monitor_weak_notice({"app.t": _finding()}, has_password=True, last_probe=_failed())
    assert "The last re-read at 2026-09-01 11:30:00 UTC failed (OperationalError)" in body
    assert "reconnect the source on the Connect step, then choose Check again" in body
    for quiet_probe in (None, _probe({"app.t": _read("d")})):
        _h, quiet = monitor_weak_notice({"app.t": _finding()}, has_password=True,
                                        last_probe=quiet_probe)
        assert "failed (" not in quiet and "credentials" not in quiet
    # Without the password the read never ran: its own sentence, never the failed one.
    _h, nopw = monitor_weak_notice({"app.t": _finding()}, has_password=False, last_probe=_failed())
    assert "kept in memory only" in nopw and "failed (" not in nopw
    gate = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY}, last_probe=_failed())
    assert gate.probe_state == PROBE_STATE_FAILED and gate.held  # still held: fail-closed
    _h, cut = cutover_weak_notice(gate)
    assert "failed (OperationalError)" in cut and "then choose Check again" in cut
    ok = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY},
               last_probe=_probe({"app.t": _read("d")}, started=T_FIX + timedelta(minutes=10)))
    assert "failed (" not in cutover_weak_notice(ok)[1]
    # Validation's verdict has no Check again of its own.
    verdict = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY},
                    last_probe=_failed(), check_probe=False)
    _t, _h, vbody = replica_identity_verdict_hold(verdict, total=2, as_of="now")
    assert "failed (OperationalError)" in vbody and "Check again" not in vbody
    no_source = _gate(findings={"app.t": _finding()}, scope={"app.t": KEY}, has_source=False,
                      last_probe=None, check_probe=False)
    assert "kept in memory only" in replica_identity_verdict_hold(no_source, total=2, as_of="now")[2]


def test_the_waivable_notice_offers_the_waiver_only_when_its_button_is_drawn():
    from dsql_migrator.ui.cdc_replica_identity_gate import cutover_waivable_notice

    restored = _finding(restored=T_FIX, by="full")
    left_out = _gate(report=_report(_item("app.u")))  # app.t not in the report
    assert left_out.can_waive
    body = cutover_waivable_notice(left_out)[1]
    assert "Or continue without this check" in body and "re-run Validation" in body
    assert "Drop & reload" not in body
    # Another restored table still needs a re-check: no button, so no outright offer.
    mixed = _gate(findings={"app.t": restored, "app.v": restored},
                  report=_report(_item("app.v", checksum=None), _item("app.u"),
                                 mode=ValidationMode.ROW_COUNT))
    assert mixed.must_recheck and mixed.waivable and not mixed.can_waive
    body = cutover_waivable_notice(mixed)[1]
    assert "Once the tables above are re-checked, you can also continue without this check" in body
    assert "Or continue without this check" not in body
    weak = _gate(findings={"app.t": restored, "app.w": _finding()},
                 report=_report(_item("app.u")))
    assert weak.weak and weak.waivable and not weak.can_waive
    body = cutover_waivable_notice(weak)[1]
    assert "back at REPLICA IDENTITY FULL and re-checked" in body
    assert "Or continue without this check" not in body
    pending = _gate(last_probe=None, report=_report(_item("app.u")))
    assert pending.pending and pending.waivable and not pending.can_waive
    assert "Once the re-read above finishes" in cutover_waivable_notice(pending)[1]
    # An accepted-gap table: re-running cannot clear it, so the copy never says to.
    gap = _gate(report=_report(_item("app.t", matched=False, checksum=False), _item("app.u")),
                release="accepted", explained=("app.t",))
    assert gap.can_waive
    body = cutover_waivable_notice(gap)[1]
    assert "'Drop & reload'" in body and "re-run Validation" not in body
    assert "Or continue without this check" in body


def test_times_carry_the_date_and_the_checksum_threshold_is_never_shown_early():
    from dsql_migrator.ui.cdc_dlq_gate import table_proof_reason
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cutover_must_recheck_notice,
        monitor_restored_notice,
    )

    restored_at = datetime(2026, 9, 1, 10, 5, 45, tzinfo=timezone.utc)
    _h, body = monitor_restored_notice(
        {"app.t": _finding(first=T0, restored=restored_at, by="full")}, set(), None
    )
    assert "started after 2026-09-01 10:06:45 UTC" in body and "after 10:06 UTC" not in body
    # The printed threshold is exactly the gate's: a start AT it is still too early.
    at = restored_at + timedelta(seconds=60)
    assert table_proof_reason(_item(), release="clean", explained=False, started=at,
                              last_at=restored_at) == "compared-before"
    assert table_proof_reason(_item(), release="clean", explained=False,
                              started=at + timedelta(seconds=1), last_at=restored_at) is None
    # A window across midnight reads forwards.
    first = datetime(2026, 10, 1, 10, 0, 45, tzinfo=timezone.utc)
    fixed = datetime(2026, 10, 2, 9, 30, 45, tzinfo=timezone.utc)
    gate = _gate(findings={"app.t": _finding(first=first, restored=fixed, by="full")},
                 run_start=fixed)
    _h, must = cutover_must_recheck_notice(gate)
    assert "from at least 2026-10-01 10:00 UTC until 2026-10-02 09:30 UTC" in must


def test_a_finding_closed_because_cdc_stopped_re_keying_is_not_called_full_again():
    """Closed as not-rekeyed (redeployed with Keep source PK), the identity can still be
    DEFAULT: no notice may say it is FULL again or name a FULL read that never happened."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cutover_must_recheck_notice,
        monitor_restored_notice,
        replica_identity_verdict_hold,
    )

    closed = _finding(first=T_WEAK, restored=T_FIX, by="not-rekeyed")
    header, body = monitor_restored_notice({"app.t": closed}, set(), None)
    assert "FULL again" not in header
    assert ("app.t (below FULL from at least 2026-09-01 11:00 UTC; CDC stopped re-keying it at "
            "2026-09-01 11:30 UTC)") in body
    mixed, _b = monitor_restored_notice(
        {"app.t": closed, "app.u": _finding(restored=T_FIX, by="full")}, set(), None
    )
    assert "FULL again" not in mixed
    full_header, full_body = monitor_restored_notice(
        {"app.u": _finding(restored=T_FIX, by="full")}, set(), None
    )
    assert full_header == "REPLICA IDENTITY is FULL again — deletes made before the fix may be missing"
    assert "until 2026-09-01 11:30 UTC" in full_body
    gate = _gate(findings={"app.t": closed}, run_start=T_FIX)
    _h, must = cutover_must_recheck_notice(gate)
    assert "stopped re-keying it at 2026-09-01 11:30 UTC" in must
    assert "until 2026-09-01 11:30" not in must
    verdict = _gate(findings={"app.t": closed}, run_start=T_FIX, check_probe=False)
    _t, _h, vbody = replica_identity_verdict_hold(verdict, total=2, as_of="now")
    assert "was below FULL while CDC re-keyed them, until 2026-09-01 11:30 UTC" in vbody
    restored_gate = _gate(run_start=T_FIX, check_probe=False)  # a "full" restore
    assert "was below FULL until 2026-09-01 11:30 UTC" in replica_identity_verdict_hold(
        restored_gate, total=2, as_of="now")[2]


def test_the_monitor_does_not_say_cut_over_waits_for_a_waived_table():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        monitor_restored_notice,
        replica_identity_waived_tables,
    )

    findings = {"app.t": _finding(restored=T_FIX, by="full")}
    waiver = CdcReplicaIdentityWaiver(tables={"app.t": T_FIX}, waived_at=T_FIX + timedelta(hours=1))
    waived = replica_identity_waived_tables(findings, waiver)
    assert waived == {"app.t"}
    _h, body = monitor_restored_notice(findings, set(), None, waived=waived)
    assert "Cut over waits" not in body
    assert "You chose on the Cut over step to continue without this proof for app.t" in body
    # Restored again after the waiver: it waits again.
    later = {"app.t": _finding(first=T_FIX + timedelta(hours=2),
                               restored=T_FIX + timedelta(hours=3), by="full")}
    assert replica_identity_waived_tables(later, waiver) == set()
    _h, body = monitor_restored_notice(later, set(), None,
                                       waived=replica_identity_waived_tables(later, waiver))
    assert "Cut over waits until each matches" in body and "You chose" not in body
    # A waived table next to one that still waits: only the waiting one is said to hold.
    both = dict(findings, **{"app.u": _finding(restored=T_FIX + timedelta(hours=1), by="full")})
    _h, body = monitor_restored_notice(both, set(), None,
                                       waived=replica_identity_waived_tables(both, waiver))
    assert "Cut over waits until each of app.u matches" in body
    assert "continue without this proof for app.t" in body
    # No waiver: unchanged.
    assert replica_identity_waived_tables(findings, None) == set()
    _h, plain = monitor_restored_notice(findings, set(), None)
    assert "Cut over waits until each matches" in plain and "You chose" not in plain


def test_a_failed_read_is_a_warning_and_a_missing_password_is_info():
    from dsql_migrator.ui.cdc_replica_identity_gate import monitor_unchecked_notice

    scope = {"app.t": list(KEY)}
    tone, header, body = monitor_unchecked_notice(scope, _failed(), has_password=True)
    assert tone == "warning" and header == "Can't re-check REPLICA IDENTITY on the source"
    assert "failed (OperationalError)" in body
    tone, _h, body = monitor_unchecked_notice(scope, None, has_password=False)
    assert tone == "info" and "reconnect the source on the Connect step" in body
    assert monitor_unchecked_notice(scope, _probe({"app.t": _read("f")}), has_password=True) is None


# ---------------------------------------------------------------------------
# Review fixes: rules the suite did not pin
# ---------------------------------------------------------------------------


def test_a_per_table_checksum_recheck_after_the_fix_releases_it():
    """The documented recovery: the run started before the fix, then "Re-check with CHECKSUM"
    records a per-table start after it. Only the override can release the hold."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        replica_identity_gate_for,
        replica_identity_proven_tables,
    )
    from dsql_migrator.ui.validation import ValidationState

    rechecked = _gate(run_start=T_FIX - timedelta(minutes=30),
                      overrides={"app.t": T_FIX + timedelta(minutes=2)})
    assert rechecked.issues == () and rechecked.resolved == ("app.t",) and not rechecked.held
    margin = _gate(run_start=T_FIX - timedelta(minutes=30),
                   overrides={"app.t": T_FIX + timedelta(seconds=60)})
    assert [i.reason for i in margin.issues] == ["compared-before"]
    # Through the real ValidationState re-check flow.
    vs = ValidationState()
    vs.mark_run_started()
    vs._run_started_wall = T_FIX - timedelta(minutes=30)
    vs.set_result(_report(_item("app.t"), _item("app.u")))
    ms = _state(findings={"app.t": _finding(restored=T_FIX, by="full")},
                deployed={"app.t": list(KEY)})

    def gate():
        return replica_identity_gate_for(ms, vs, _session(), cdc_in_use=True, release="clean",
                                         summary=None, check_probe=False)

    assert [i.reason for i in gate().issues] == ["compared-before"]
    assert gate().recheckable == ("app.t",)
    assert replica_identity_proven_tables(ms.cdc_replica_identity_findings, vs) == set()
    vs.start_recheck(["app.t"])
    vs._recheck_started_wall = T_FIX + timedelta(minutes=2)
    assert vs.merge_recheck_result([_item("app.t")]) is True
    released = gate()
    assert released.issues == () and released.resolved == ("app.t",) and not released.held
    assert replica_identity_proven_tables(ms.cdc_replica_identity_findings, vs) == {"app.t"}


def test_cdc_evidence_gates_the_read_unless_a_finding_is_open():
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        PROBE_STATE_PENDING,
        cdc_evidence,
        cdc_replica_identity_cutover_gate,
        replica_identity_probe_possible,
        replica_identity_scope,
    )

    assert cdc_evidence(_state()) is True
    quiet = _state(evidence=False)
    assert cdc_evidence(quiet) is False
    quiet.set_cdc_connector_names(["dsql-cdc-x-source"])
    assert cdc_evidence(quiet) is True
    # CDC never ran: nothing is read and the gate is inert.
    never = _state(deployed={"app.t": list(KEY)}, evidence=False)
    assert replica_identity_probe_possible(never, _session()) is False
    inert = cdc_replica_identity_cutover_gate(
        findings={}, scope=replica_identity_scope(never, _session()), cdc_in_use=True,
        cdc_evidence=False, is_pg=True, check_probe=True, report=None, release="clean",
        has_source_connection=True,
    )
    assert not inert.applies and not inert.held
    # A finding left open (e.g. after attaching another stack) can still be re-read and so
    # cleared: never a permanent hold.
    leftover = _state(findings={"app.t": _finding()}, evidence=False)
    assert replica_identity_probe_possible(leftover, _session()) is True
    held = cdc_replica_identity_cutover_gate(
        findings=leftover.cdc_replica_identity_findings,
        scope=replica_identity_scope(leftover, _session()), cdc_in_use=False,
        cdc_evidence=False, is_pg=True, check_probe=True, report=None, release="clean",
        has_source_connection=True,
    )
    assert held.held and held.probe_state == PROBE_STATE_PENDING


def test_can_waive_needs_nothing_else_open():
    restored = _finding(restored=T_FIX, by="full")
    waivable = {"report": _report(_item("app.u"))}  # app.t is not in the report
    assert _gate(**waivable).can_waive
    weak = _gate(findings={"app.t": restored, "app.w": _finding()}, **waivable)
    assert weak.waivable and weak.weak and not weak.can_waive
    must = _gate(findings={"app.t": restored, "app.v": restored},
                 report=_report(_item("app.v"), _item("app.u")),
                 run_start=T_FIX + timedelta(seconds=60))
    assert must.waivable and must.must_recheck and not must.can_waive
    pending = _gate(last_probe=None, **waivable)
    assert pending.waivable and pending.pending and not pending.can_waive


def test_a_new_verdict_drops_the_waiver_and_a_recheck_keeps_it():
    from dsql_migrator.ui.validation import ValidationState

    waiver = CdcReplicaIdentityWaiver(tables={"app.t": T_FIX}, waived_at=T_FIX)
    state = ValidationState()
    state.set_result(_report(_item()))
    state.cutover_replica_identity_waiver = waiver
    state.start_recheck(["app.t"])
    assert state.merge_recheck_result([_item()]) is True
    assert state.cutover_replica_identity_waiver is waiver
    state.set_result(_report(_item()))
    assert state.cutover_replica_identity_waiver is None
    state.cutover_replica_identity_waiver = waiver
    state.clear_outputs()
    assert state.cutover_replica_identity_waiver is None


def test_the_claim_watchdog_is_twice_the_ttl():
    from dsql_migrator.ui.cdc_replica_identity_gate import CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS

    state = _state()
    assert state.begin_cdc_replica_identity_probe(now_monotonic=100.0) is True
    assert state.begin_cdc_replica_identity_probe(
        now_monotonic=100.0 + CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS + 1
    ) is False


def test_an_open_finding_refreshes_its_facts_silently():
    """DEFAULT -> NOTHING, or more partitions below FULL, while it stays open: the notices
    must name the CURRENT state (no transition, no log line, the start time kept)."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        identity_label,
    )

    opened, _ = fold_replica_identity_probe({}, _probe({"app.t": _read("d")}))
    changed, quiet = fold_replica_identity_probe(
        opened, _probe({"app.t": _read("n")}, started=T_WEAK + timedelta(minutes=2))
    )
    assert quiet == [] and changed["app.t"].identity == "n"
    assert changed["app.t"].first_weak_at == T_WEAK
    names = ["app.t_p1", "app.t_p2", "app.t_p3"]
    parted, _ = fold_replica_identity_probe({}, _probe({"app.t": _read("f", weak=1, names=names[:1])}))
    grown, quiet = fold_replica_identity_probe(
        parted, _probe({"app.t": _read("f", weak=3, names=names)}, started=T_WEAK + timedelta(minutes=2))
    )
    assert quiet == [] and grown["app.t"].weak_partitions == 3
    assert identity_label(grown["app.t"]).endswith("app.t_p1, app.t_p2, app.t_p3")
    # Landed through the state: the refreshed finding is stored though nothing transitioned.
    state = _state()
    assert [t.kind for t in state.apply_cdc_replica_identity_probe(_probe({"app.t": _read("d")}))] == ["opened"]
    assert state.apply_cdc_replica_identity_probe(
        _probe({"app.t": _read("i")}, started=T_WEAK + timedelta(minutes=2))
    ) == []
    assert state.cdc_replica_identity_findings["app.t"].identity == "i"


def test_a_ttl_re_read_that_opens_a_finding_refreshes_the_page(monkeypatch, fake_run):
    """Same scope, same tables read: only the findings differ, and the page must refresh."""
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import schedule_replica_identity_probe

    monkeypatch.setattr(status, "read_cdc_replica_identity_once",
                        lambda _s, _scope: _probe({"app.t": _read("d")},
                                                  started=datetime.now(timezone.utc)))
    state = _state(deployed={"app.t": list(KEY)})
    state.cdc_replica_identity_last_probe = _probe(
        {"app.t": _read("f")}, started=datetime.now(timezone.utc) - timedelta(minutes=3)
    )  # past the TTL
    timers: list = []
    ui = SimpleNamespace(timer=lambda _i, cb, once=False: timers.append(cb))
    refreshes: list = []
    schedule_replica_identity_probe(ui, _session(), state, lambda: refreshes.append(1),
                                    evidence_at=None)
    (check,) = timers
    asyncio.run(check())
    assert "app.t" in state.cdc_replica_identity_findings
    assert refreshes == [1]


def test_the_log_clauses_name_a_table_missing_from_the_report():
    from dsql_migrator.ui.cdc_replica_identity_gate import replica_identity_held_log_clause as held

    clause = held(_gate(report=_report(_item("app.u"))))  # app.t not-validated
    assert clause is not None and "app.t" in clause
    waiver = CdcReplicaIdentityWaiver(tables={"app.t": T_FIX}, waived_at=T_FIX)
    assert held(_gate(report=_report(_item("app.u")), waiver=waiver)) is None
