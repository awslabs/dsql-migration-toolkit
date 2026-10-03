# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G4: the runtime source-schema re-check -- probe build, fold, scheduling, gate and copy."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc_catalog_drift import (
    KIND_ADD_COLUMN,
    KIND_DEFINITION_CHANGE,
    KIND_DROP_COLUMN,
    KIND_RENAME_COLUMN,
    KIND_SOURCE_TABLE_MISSING,
    KIND_TYPE_CHANGE,
    CatalogDiff,
    LiveColumn,
    LiveTable,
)
from dsql_migrator.core.models import (
    CdcCatalogDriftChange,
    CdcCatalogDriftFinding,
    CdcCatalogDriftWaiver,
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.ui import cdc_catalog_drift_gate as g4
from dsql_migrator.ui.cdc_catalog_drift_gate import (
    CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS,
    CatalogBaseline,
    CatalogDriftRequest,
    CdcCatalogDriftProbe,
    build_catalog_drift_probe,
    catalog_drift_probe_due,
    catalog_drift_probe_possible,
    catalog_drift_scope,
    cdc_catalog_drift_cutover_gate,
    fold_catalog_drift_probe,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def _table(name="app.orders", cols=(("id", "int", False), ("notes", "text", True))):
    return TableDef(
        name=name,
        columns=[ColumnDef(name=n, mysql_type=t, nullable=nl) for n, t, nl in cols],
        primary_key=["id"],
    )


def _live(cols=(("id", "int", False), ("notes", "text", True))):
    return LiveTable(
        columns=tuple(LiveColumn(n, t, nl, False, i) for i, (n, t, nl) in enumerate(cols)),
        primary_key=frozenset({"id"}),
    )


def _request(*tables, not_in=(), token=("inv", ())):
    return CatalogDriftRequest(
        tables=tuple(tables), source_type=SourceType.MYSQL, token=token,
        not_in_baseline=tuple(not_in),
    )


def _diff(kind=KIND_DROP_COLUMN, column="notes"):
    return CatalogDiff(
        kinds=(kind,), changes=(CdcCatalogDriftChange(kind=kind, column=column),)
    )


def _probe(scope, *, drift=None, digests=None, ok=True, problem=None, not_in=(),
           started=T0, token=()):
    return CdcCatalogDriftProbe(
        started_at=started, finished_at=started + timedelta(seconds=1), ok=ok,
        problem=problem, scope=tuple(sorted(scope)), not_in_baseline=tuple(not_in),
        drift=drift or {}, digests={t: "d0" for t in scope} if digests is None else digests,
        token=token,
    )


def _finding(*, first=T0, resolved=None, by="matched", kinds=(KIND_DROP_COLUMN,), fixable=False,
             changes=None):
    return CdcCatalogDriftFinding(
        kinds=list(kinds),
        changes=list(changes if changes is not None else
                     [CdcCatalogDriftChange(kind=kinds[0], column="notes")]),
        fixable=fixable, first_seen_at=first, resolved_at=resolved,
        resolved_by=by if resolved is not None else None,
    )


# --- build_catalog_drift_probe -------------------------------------------------------------


def test_two_tables_and_none_found_is_one_read_problem_never_two_findings() -> None:
    req = _request(_table("app.a"), _table("app.b"))
    probe = build_catalog_drift_probe(req, {}, started=T0, finished=T0)
    assert probe.ok is False and probe.problem == "none-visible" and probe.drift == {}
    findings, digests, transitions = fold_catalog_drift_probe({}, {}, probe)
    assert findings == {} and digests == {} and transitions == []


def test_one_missing_table_of_several_opens_a_finding_and_every_table_is_digested() -> None:
    req = _request(_table("app.a"), _table("app.b"), _table("app.c"))
    probe = build_catalog_drift_probe(
        req, {"app.a": _live(), "app.c": _live(cols=(("id", "int", False),))},
        started=T0, finished=T0,
    )
    assert probe.ok and probe.found == 2
    assert set(probe.digests) == {"app.a", "app.b", "app.c"}
    assert probe.drift["app.b"].kinds == (KIND_SOURCE_TABLE_MISSING,)
    assert probe.drift["app.c"].kinds == (KIND_DROP_COLUMN,)
    assert "app.a" not in probe.drift


def test_a_single_table_not_found_is_a_missing_table() -> None:
    probe = build_catalog_drift_probe(_request(_table()), {}, started=T0, finished=T0)
    assert probe.ok and probe.drift["app.orders"].kinds == (KIND_SOURCE_TABLE_MISSING,)


# --- the fold ------------------------------------------------------------------------------


def test_the_fold_opens_stays_silent_refreshes_resolves_and_reopens() -> None:
    first, digests, tr = fold_catalog_drift_probe({}, {}, _probe(["app.t"]))
    assert first == {} and digests == {"app.t": "d0"} and tr == []  # the first read seeds
    opened, digests, tr = fold_catalog_drift_probe(
        first, digests, _probe(["app.t"], drift={"app.t": _diff()})
    )
    finding = opened["app.t"]
    assert finding.resolved_at is None and finding.first_seen_at == T0
    assert [t.kind for t in tr] == ["opened"]
    again, _d, tr = fold_catalog_drift_probe(
        opened, digests, _probe(["app.t"], drift={"app.t": _diff()}, started=T0 + timedelta(minutes=5))
    )
    assert tr == [] and again["app.t"] == finding  # a repeat is silent
    refreshed, _d, tr = fold_catalog_drift_probe(
        opened, digests, _probe(["app.t"], drift={"app.t": _diff(KIND_ADD_COLUMN, "x")})
    )
    assert tr == [] and refreshed["app.t"].kinds == [KIND_ADD_COLUMN]
    assert refreshed["app.t"].first_seen_at == T0
    later = T0 + timedelta(hours=1)
    resolved, digests, tr = fold_catalog_drift_probe(opened, digests, _probe(["app.t"], started=later))
    assert [t.kind for t in tr] == ["resolved"]
    assert resolved["app.t"].resolved_by == "matched"
    assert resolved["app.t"].resolved_at == later + timedelta(seconds=1)
    reopened, _d, tr = fold_catalog_drift_probe(
        resolved, digests,
        _probe(["app.t"], drift={"app.t": _diff()}, started=later + timedelta(hours=1)),
    )
    assert [t.kind for t in tr] == ["reopened"]
    assert reopened["app.t"].resolved_at is None
    assert reopened["app.t"].first_seen_at == later + timedelta(hours=1)


def test_a_failed_read_changes_neither_findings_nor_digests() -> None:
    findings = {"app.t": _finding()}
    out, digests, tr = fold_catalog_drift_probe(
        findings, {"app.t": "d0"}, _probe(["app.t"], ok=False, problem="read-failed")
    )
    assert out == findings and digests == {"app.t": "d0"} and tr == []
    assert fold_catalog_drift_probe(findings, {}, None)[0] == findings


def test_a_step_1_redefinition_is_a_resolved_definition_change() -> None:
    out, digests, tr = fold_catalog_drift_probe({}, {"app.t": "old"}, _probe(["app.t"]))
    finding = out["app.t"]
    assert finding.kinds == [KIND_DEFINITION_CHANGE]
    assert finding.resolved_by == "rebaselined" and finding.resolved_at == finding.first_seen_at
    assert [t.kind for t in tr] == ["rebaselined"] and digests == {"app.t": "d0"}
    earlier = _finding(resolved=T0 - timedelta(hours=2))
    out, _d, tr = fold_catalog_drift_probe({"app.t": earlier}, {"app.t": "old"}, _probe(["app.t"]))
    assert out["app.t"].resolved_by == "rebaselined"
    assert out["app.t"].resolved_at == T0 + timedelta(seconds=1)  # moved forward
    assert out["app.t"].first_seen_at == earlier.first_seen_at


def test_a_table_that_left_the_inventory_is_closed_or_recorded_and_its_digest_pruned() -> None:
    out, digests, tr = fold_catalog_drift_probe(
        {"app.gone": _finding()}, {"app.gone": "d", "app.t": "d0"},
        _probe(["app.t"], not_in=["app.gone"]),
    )
    assert out["app.gone"].resolved_by == "not-in-inventory"
    assert [t.kind for t in tr] == ["not-in-inventory"] and digests == {"app.t": "d0"}
    out, digests, tr = fold_catalog_drift_probe(
        {}, {"app.gone": "d"}, _probe([], not_in=["app.gone"])
    )
    assert out["app.gone"].kinds == [KIND_SOURCE_TABLE_MISSING]
    assert out["app.gone"].resolved_by == "not-in-inventory" and digests == {}
    # Never compared before (no digest), no finding: nothing to record.
    assert fold_catalog_drift_probe({}, {}, _probe([], not_in=["app.x"]))[0] == {}


# --- scope, possibility, due-ness, single flight -------------------------------------------


def _session(password=True):
    return SimpleNamespace(source_config=object(), source_password=object() if password else None)


def _state(baseline="default", *, evidence=True):
    ms = DataMigrationState()
    if evidence:
        ms.set_cdc_ops_window_start(T0)
    if baseline == "default":
        table = _table()
        inv = SourceInventory(tables=[table])
        baseline = CatalogBaseline(inventory=inv, captured=(table,),
                                   capture_names=frozenset({table.name}),
                                   source_type=SourceType.MYSQL)
    if baseline is not None:
        ms.cdc_catalog_baseline = (baseline if callable(baseline) else (lambda b=baseline: b))
    return ms


def test_inert_without_the_handle_or_with_no_baseline_and_an_open_finding_stays_open() -> None:
    assert not catalog_drift_probe_possible(_state(None), _session())
    none = _state(lambda: None)
    none.cdc_catalog_drift_findings = {"app.orders": _finding()}
    assert not catalog_drift_probe_possible(none, _session())
    assert g4.start_catalog_drift_probe(_session(), none) is False
    assert none.cdc_catalog_drift_findings["app.orders"].resolved_at is None
    empty = CatalogBaseline(inventory=SourceInventory(tables=[_table()]), captured=())
    assert not catalog_drift_probe_possible(_state(empty), _session())
    gate = g4.catalog_drift_gate_for(_state(empty), None, _session(), cdc_in_use=True,
                                     release="clean", summary=None)
    assert gate.probe_state == "no-baseline" and gate.unchecked  # nothing known to check
    assert catalog_drift_probe_possible(_state(), _session())
    assert not catalog_drift_probe_possible(_state(), _session(password=False))
    assert not catalog_drift_probe_possible(_state(evidence=False), _session())


def test_open_finding_tables_stay_in_scope() -> None:
    ms = _state()
    ms.cdc_catalog_drift_findings = {"app.other": _finding()}
    # Not in the inventory: it is read as "no longer in the inventory", still in scope.
    assert catalog_drift_scope(ms) == ("app.orders", "app.other")
    assert g4._scope_parts(ms, g4.catalog_drift_baseline(ms))[1] == ("app.other",)


def test_due_rules_and_the_ttl() -> None:
    assert CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS == 300
    scope, token = ("app.t",), ("inv", ())
    last = _probe(["app.t"], token=token)
    now = T0 + timedelta(seconds=10)
    assert catalog_drift_probe_due(None, scope, token, None, now)
    assert not catalog_drift_probe_due(last, scope, token, None, now)
    assert catalog_drift_probe_due(last, ("app.t", "app.u"), token, None, now)
    assert catalog_drift_probe_due(last, scope, ("other", ()), None, now)
    assert catalog_drift_probe_due(last, scope, token, T0 + timedelta(seconds=5), now)
    assert catalog_drift_probe_due(last, scope, token, None, T0 + timedelta(seconds=302))
    failed = _probe(["app.t"], ok=False, problem="read-failed", token=token)
    assert not catalog_drift_probe_due(failed, scope, token, None, now)  # no tight loop
    assert catalog_drift_probe_due(failed, scope, ("new-inventory", ()), None, now)
    gone = _probe([], not_in=["app.t"], token=token)
    assert not catalog_drift_probe_due(gone, scope, token, None, now)  # covered


def test_single_flight_with_a_600_second_watchdog() -> None:
    ms = DataMigrationState()
    assert ms.begin_cdc_catalog_drift_probe(now_monotonic=1000.0)
    assert not ms.begin_cdc_catalog_drift_probe(now_monotonic=1599.0)
    assert ms.begin_cdc_catalog_drift_probe(now_monotonic=1601.0)
    ms.finish_cdc_catalog_drift_probe()
    assert not ms.cdc_catalog_drift_probe_running


def test_the_poll_start_is_throttled_and_force_bypasses_the_ttl(monkeypatch) -> None:
    import asyncio

    import dsql_migrator.ui.data_migration._cdc_status as status

    spawned: list = []
    monkeypatch.setattr(status, "spawn_detached", lambda work: spawned.append(work))
    ms = _state()
    session = _session()

    async def _scenario():
        assert g4.start_catalog_drift_probe(session, ms) is True
        ms.finish_cdc_catalog_drift_probe()
        ms.cdc_catalog_drift_last_probe = _probe(
            ["app.orders"], started=datetime.now(timezone.utc),
            token=g4.catalog_drift_token(ms, g4.catalog_drift_baseline(ms)),
        )
        assert g4.start_catalog_drift_probe(session, ms) is False  # within the TTL
        assert g4.start_catalog_drift_probe(session, ms, force=True) is True
        ms.finish_cdc_catalog_drift_probe()
        # A Fix-added column changes the token: due at once.
        ms.record_cdc_target_added_columns("app.orders", {"x": "int"})
        assert g4.start_catalog_drift_probe(session, ms) is True

    asyncio.run(_scenario())
    assert len(spawned) == 3


# --- the gate ------------------------------------------------------------------------------


def _item(table="app.t", *, checksum=True, error=None, matched=True):
    return TableValidationResult(
        table=table, source_row_count=1, target_row_count=1, row_count_match=matched,
        checksum_match=checksum, matched=matched, error=error,
    )


def _gate(findings, *, items=(), run_start=None, scope=("app.t",), check_probe=False,
          last_probe=None, has_password=True, baseline_known=True, token=None, waiver=None,
          cdc_in_use=True, evidence=True, inventory_names=frozenset({"app.t"}), evidence_at=None,
          overrides=None, explained=(), release="clean"):
    report = ValidationReport.build(mode=ValidationMode.CHECKSUM, items=list(items))
    return cdc_catalog_drift_cutover_gate(
        findings=findings, scope=scope, cdc_in_use=cdc_in_use, cdc_evidence=evidence,
        registered=True, baseline_known=baseline_known, check_probe=check_probe,
        report=report, release=release, inventory_names=set(inventory_names),
        explained_tables=explained, compared_run_start=run_start,
        compared_overrides=overrides, last_probe=last_probe, token=token,
        has_source_connection=has_password, waiver=waiver, evidence_at=evidence_at,
    )


def test_an_open_finding_holds_even_after_a_later_value_verified_checksum_4c() -> None:
    gate = _gate({"app.t": _finding()}, items=[_item()], run_start=T0 + timedelta(hours=1))
    assert gate.held and gate.blocks and [i.reason for i in gate.open] == ["drifted"]
    assert gate.recheckable == () and not gate.can_waive


def test_a_resolved_finding_needs_a_checksum_started_more_than_a_minute_after() -> None:
    resolved = {"app.t": _finding(resolved=T0)}
    early = _gate(resolved, items=[_item()], run_start=T0 + timedelta(seconds=59))
    assert [i.reason for i in early.issues] == ["compared-before"] and early.held
    assert early.recheckable == ("app.t",)
    late = _gate(resolved, items=[_item()], run_start=T0 + timedelta(seconds=61))
    assert late.resolved == ("app.t",) and not late.held
    counted = _gate(resolved, items=[_item(checksum=None)], run_start=T0 + timedelta(hours=1))
    assert [i.reason for i in counted.issues] == ["row-count-only"]
    unverified = _gate(resolved, items=[_item(error="UndefinedColumn", matched=False)],
                       run_start=T0 + timedelta(hours=1))
    assert [i.reason for i in unverified.issues] == ["not-matched"]


def test_a_table_gone_from_the_inventory_is_not_validated_and_waivable() -> None:
    gone = {"app.t": _finding(resolved=T0, by="not-in-inventory",
                              kinds=(KIND_SOURCE_TABLE_MISSING,), changes=[])}
    gate = _gate(gone, items=[_item()], run_start=T0 + timedelta(hours=1),
                 inventory_names=frozenset())
    assert [i.reason for i in gate.issues] == ["not-validated"] and gate.can_waive
    # Back in the inventory (re-created): graded on its report item again.
    back = _gate(gone, items=[_item()], run_start=T0 + timedelta(hours=1))
    assert back.resolved == ("app.t",)


def test_the_waiver_is_all_or_nothing_and_scoped_to_resolved_at() -> None:
    findings = {
        "app.a": _finding(resolved=T0),
        "app.b": _finding(resolved=T0 + timedelta(minutes=1)),
    }
    names = frozenset({"app.t"})
    partial = CdcCatalogDriftWaiver(tables={"app.a": T0}, waived_at=T0)
    assert not _gate(findings, inventory_names=names, waiver=partial).waived
    full = CdcCatalogDriftWaiver(
        tables={"app.a": T0, "app.b": T0 + timedelta(minutes=1)}, waived_at=T0
    )
    gate = _gate(findings, inventory_names=names, waiver=full)
    assert gate.waived and not gate.held
    moved = dict(findings, **{"app.a": _finding(resolved=T0 + timedelta(hours=2))})
    assert not _gate(moved, inventory_names=names, waiver=full).waived


def test_probe_states_and_fail_open() -> None:
    token = ("inv", ())
    base = dict(check_probe=True, token=token, evidence_at=T0)
    assert _gate({}, has_password=False, **base).probe_state == "unavailable"
    assert _gate({}, baseline_known=False, **base).probe_state == "no-baseline"
    assert _gate({}, **base).probe_state == "pending"  # no read yet
    fresh = _probe(["app.t"], started=T0 + timedelta(seconds=5), token=token)
    ok = _gate({}, last_probe=fresh, **base)
    assert ok.probe_state == "ok" and not ok.unchecked and not ok.held
    failed = _probe(["app.t"], ok=False, problem="read-failed",
                    started=T0 + timedelta(seconds=5), token=token)
    gate = _gate({}, last_probe=failed, **base)
    assert gate.probe_state == "failed" and gate.unchecked and not gate.held  # fail-open
    blind = _probe(["app.t"], ok=False, problem="none-visible",
                   started=T0 + timedelta(seconds=5), token=token)
    assert _gate({}, last_probe=blind, **base).problem == "none-visible"
    for state in ("unavailable", "no-baseline"):
        kw = {"has_password": False} if state == "unavailable" else {"baseline_known": False}
        g = _gate({}, **base, **kw)
        assert g.unchecked and not g.held
    # A read before the latest validation, or with another baseline: pending again.
    assert _gate({}, last_probe=fresh, **dict(base, evidence_at=T0 + timedelta(minutes=1))).pending
    assert _gate({}, last_probe=fresh, **dict(base, token=("other", ()))).pending
    # ...but an open finding still holds without a password.
    held = _gate({"app.t": _finding()}, has_password=False, **base)
    assert held.held and held.probe_state == "unavailable"


def test_full_load_only_is_inert_but_persisted_findings_still_apply() -> None:
    assert not _gate({}, cdc_in_use=False).applies
    assert not _gate({}, evidence=False).applies
    persisted = _gate({"app.t": _finding()}, cdc_in_use=False)
    assert persisted.applies and persisted.held


def test_checksum_forced_tables_union_every_gate() -> None:
    from dsql_migrator.ui.cdc_replica_identity_gate import cdc_checksum_forced_tables

    ms = SimpleNamespace(
        cdc_dlq_ledger={"app.a": object()},
        cdc_replica_identity_findings={"app.b": object()},
        cdc_catalog_drift_findings={"app.c": _finding()},
    )
    assert cdc_checksum_forced_tables(ms) == {"app.a", "app.b", "app.c"}


# --- copy ----------------------------------------------------------------------------------


def _open_notice(findings, *, pg=False, mtype=MigrationType.FULL_LOAD_AND_CDC, fix=(),
                 has_password=True, last=None, baseline_known=True):
    return g4.monitor_open_notice(
        findings, is_pg=pg, migration_type=mtype, has_password=has_password,
        last_probe=last, baseline_known=baseline_known, fix_tables=set(fix),
    )


def test_every_label_and_why_and_the_engine_specific_ones() -> None:
    kinds = [
        ("source-table-missing", "table not found on the source"),
        ("key-change", "primary key changed at the source"),
        ("type-change", "column type changed at the source"),
        ("drop-column", "column dropped at the source"),
        ("rename-column", "column renamed at the source"),
        ("nullability-change", "NULL rule changed at the source"),
        ("add-column", "column added at the source"),
        ("definition-change", "definition changed in the Step 1 inventory"),
    ]
    for kind, label in kinds:
        f = _finding(kinds=(kind,), changes=[])
        assert g4.kind_labels(f) == [label]
        assert g4.kind_whys(f, is_pg=False)
    missing = _finding(kinds=("source-table-missing",), changes=[])
    assert "can no longer see it" in g4.kind_whys(missing, is_pg=False)[0]
    assert "until the connector restarts" in g4.kind_whys(missing, is_pg=True)[0]
    add = _finding(kinds=("add-column",),
                   changes=[CdcCatalogDriftChange(kind="add-column", column="x", after="int")])
    assert "interval[]" not in g4.kind_whys(add, is_pg=False)[0]
    assert "interval[] or money[]" in g4.kind_whys(add, is_pg=True)[0]
    gen = _finding(kinds=("add-column",),
                   changes=[CdcCatalogDriftChange(kind="add-column", column="g", generated=True)])
    assert g4.kind_whys(gen, is_pg=True) == [g4._WHY_ADD_GENERATED]
    assert "Fix target schema… does not add it" in g4._WHY_ADD_GENERATED
    row = g4.finding_row("app.orders", _finding(), is_pg=False)
    assert row.startswith("app.orders: column dropped at the source (dropped notes) — CDC no ")


def test_the_runbook_and_kind_sentences_follow_the_type_and_the_kinds() -> None:
    flc = _open_notice({"app.t": _finding()})
    assert "'Drop & reload'" in flc.runbook and "Run no other DDL" in flc.runbook
    # A renamed column is renamed (Fix does it), never dropped; a key column is re-applied
    # with Replace -- Aurora DSQL cannot drop it.
    assert "Fix target schema… does it" in flc.runbook and "Replace" in flc.runbook
    assert "Drop a removed or renamed column" not in flc.runbook
    cdc_only = _open_notice({"app.t": _finding()}, mtype=MigrationType.CDC_ONLY)
    assert "by hand" in cdc_only.runbook and "Drop & reload" not in cdc_only.runbook
    assert "renamed column" not in flc.tail and "primary key" not in flc.tail
    assert "Fix target schema… adds it" not in flc.tail
    mixed = _open_notice({
        "app.a": _finding(kinds=("source-table-missing",), changes=[]),
        "app.b": _finding(kinds=("rename-column",), changes=[]),
        "app.c": _finding(kinds=("key-change",), changes=[]),
        "app.d": _finding(kinds=("add-column",), fixable=True,
                          changes=[CdcCatalogDriftChange(kind="add-column", column="x")]),
    }, fix={"app.d"})
    assert "A table not found on the source cannot be realigned here" in mixed.tail
    assert "Do not use Fix" not in mixed.tail
    assert ("For a renamed column, Fix target schema… renames it on Aurora DSQL so it keeps "
            "its values (not a primary-key or CDC key column, nor an excluded one)") in mixed.tail
    assert "re-run Step 1 and reload the table with 'Drop & reload'" in mixed.tail
    assert "re-apply the table in Schema Conversion with Replace" in mixed.tail
    assert ("For an added column, Fix target schema… adds it to Aurora DSQL now, except a "
            "generated column or one your exclusions keep out (it says why and what to do "
            "instead); then re-run Step 1 so Validation can compare it.") in mixed.tail
    key_cdc_only = _open_notice({"app.c": _finding(kinds=("key-change",), changes=[])},
                                mtype=MigrationType.CDC_ONLY)
    assert "use Start over to migrate the table with its new key" in key_cdc_only.tail
    assert "Replace" not in key_cdc_only.tail
    assert mixed.tail.index(g4.MONITOR_HOLD_SENTENCE) > mixed.tail.index("Fix target schema… adds")
    # The plain-added sentence needs a fix table whose finding HAS an added column: a rename
    # finding's Fix renames, so the sentence would describe a button that is not there.
    rename_only = _open_notice(
        {"app.b": _finding(kinds=("rename-column",), fixable=True, changes=[])}, fix={"app.b"}
    )
    assert "Fix target schema… renames it" in rename_only.tail
    assert "For an added column" not in rename_only.tail
    # A fixable rename is no re-key refusal.
    assert "Composite key" not in rename_only.tail


def test_the_probe_passes_the_effective_exclusions_apart_from_skip() -> None:
    # A PostgreSQL STORED generated `g` (skipped, never excluded) renamed to a Fix-added `x`:
    # the dialog would show a name-taken hold, so the row is fixable. Were the probe to pass
    # ``skip`` as the exclusions, g would read as excluded and the hold (and Fix) vanish.
    name = "app.orders"
    base = TableDef(name=name, columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False),
        ColumnDef(name="g", mysql_type="integer", nullable=True, generated=True),
        ColumnDef(name="n", mysql_type="text", nullable=True),
    ], primary_key=["id"])
    live = LiveTable(columns=(LiveColumn("id", "integer", False, False, 0),
                              LiveColumn("x", "integer", True, True, 1),
                              LiveColumn("n", "text", True, False, 2)),
                     primary_key=frozenset({"id"}))
    request = CatalogDriftRequest(tables=(base,), skip={name: frozenset({"g"})},
                                  target_added={name: {"x": "integer"}},
                                  source_type=SourceType.POSTGRES)
    probe = build_catalog_drift_probe(request, {name: live}, started=T0, finished=T0)
    assert probe.drift[name].kinds == (KIND_ADD_COLUMN,) and probe.drift[name].fixable is True


def test_the_open_notice_gives_the_recreate_path_for_a_renamed_rekey_column() -> None:
    """A rename finding the shared plan refuses with the source key unchanged renamed a CDC
    key column (Composite key): there is no Fix button, and the runbook's "Fix target
    schema… does it" would mislead without the recreate path the dialog's refusal gives."""
    from dsql_migrator.core.cdc_catalog_drift import diff_table

    base = _table(cols=(("id", "int", False), ("tenant_id", "int", True)))
    live = _live(cols=(("id", "int", False), ("org_id", "int", True)))
    diff = diff_table(base, live, rekey_columns={"tenant_id", "id"}, source_type=SourceType.MYSQL)
    assert diff.kinds == ("rename-column",) and not diff.fixable
    finding = _finding(kinds=diff.kinds, changes=list(diff.changes), fixable=diff.fixable)
    flc = _open_notice({"app.t": finding})
    assert ("In app.t, a renamed column is part of the key CDC writes and deletes by (Schema "
            "Conversion's Composite key), so neither a rename nor Fix target schema… repairs "
            "it") in flc.tail
    assert "re-apply the table in Schema Conversion with Replace and choose the Composite key " \
        "again" in flc.tail
    assert "Delete CDC infrastructure and deploy it again before Start CDC" in flc.tail
    cdc_only = _open_notice({"app.t": finding}, mtype=MigrationType.CDC_ONLY)
    assert "rename it back on the source, or use Start over" in cdc_only.tail
    assert "Replace" not in cdc_only.tail
    # A key change already carries its own sentence.
    keyed = _finding(kinds=("key-change", "rename-column"), changes=[])
    assert "Composite key" not in _open_notice({"app.t": keyed}).tail


def test_the_added_column_sentence_names_the_columns_the_dialog_does_not_add() -> None:
    """A renamed excluded column reads as an added one to the re-check (it skips excluded
    columns) and stays fixable so the dialog can explain -- but that dialog adds nothing, so
    the sentence must not promise a plain add."""
    from dsql_migrator.core.cdc_catalog_drift import diff_table
    from dsql_migrator.core.cdc_schema_evolve import plan_target_fix

    base = _table(cols=(("id", "int", False), ("ssn", "text", True)))
    live = _live(cols=(("id", "int", False), ("ssn2", "text", True)))
    diff = diff_table(base, live, skip={"ssn"}, source_type=SourceType.MYSQL)
    assert diff.kinds == ("add-column",) and diff.fixable
    plan = plan_target_fix("app.t", live, ["id", "ssn"], baseline=base, excluded={"ssn"},
                           rule_matches={})
    assert plan.steps == () and plan.held
    finding = _finding(kinds=diff.kinds, changes=list(diff.changes), fixable=True)
    tail = _open_notice({"app.t": finding}, fix={"app.t"}).tail
    assert "except a generated column or one your exclusions keep out" in tail
    assert "For a plain added column" not in tail


def test_the_open_notice_says_why_a_read_cannot_clear_it() -> None:
    f = {"app.t": _finding()}
    assert "reconnect the source on the Connect step" in _open_notice(f, has_password=False).tail
    assert "there is none right now" in _open_notice(f, baseline_known=False).tail
    failed = _probe(["app.t"], ok=False, problem="read-failed")
    failed = CdcCatalogDriftProbe(**{**failed.__dict__, "error_class": "OperationalError"})
    assert "failed (OperationalError)" in _open_notice(f, last=failed).tail
    blind = _probe(["app.t", "app.u"], ok=False, problem="none-visible")
    assert "found none of the captured tables" in _open_notice(f, last=blind).tail


def test_rows_and_names_are_bounded() -> None:
    many = {f"app.t{i:02d}": _finding() for i in range(9)}
    notice = _open_notice(many)
    assert len(notice.rows) == 6 and notice.more_tables == "+3 more tables"
    facts = _finding(changes=[CdcCatalogDriftChange(kind="drop-column", column=f"c{i}")
                              for i in range(5)])
    facts = facts.model_copy(update={"more_changes": 2})
    assert "+4 more changes" in g4.finding_row("app.t", facts, is_pg=False)


def test_the_resolved_notice_names_each_window_and_the_exact_threshold() -> None:
    findings = {
        "app.a": _finding(first=T0 - timedelta(hours=1), resolved=T0),
        "app.b": _finding(resolved=T0 + timedelta(minutes=2), by="rebaselined"),
        "app.c": _finding(resolved=T0, by="not-in-inventory",
                          kinds=("source-table-missing",), changes=[]),
    }
    header, body = g4.monitor_resolved_notice(
        findings, set(), MigrationType.FULL_LOAD_AND_CDC,
        inventory_names={"app.a", "app.b"},
    )
    assert header == ("The source schema changed while CDC ran — values written meanwhile "
                      "may differ on Aurora DSQL")
    assert "app.a (changed from at least 2026-09-01 11:00 UTC until 2026-09-01 12:00 UTC)" in body
    assert "app.b (changed before 2026-09-01 12:02 UTC, found when Step 1 was re-run)" in body
    assert "app.c (no longer in the Step 1 inventory since 2026-09-01 12:00 UTC)" in body
    assert ("Cut over waits until each of app.a, app.b matches in a comparison started after "
            "2026-09-01 12:03:00 UTC.") in body
    assert ("app.c was dropped or renamed at the source, so Validation cannot compare it: if "
            "that was intended, acknowledge it on the Cut over step.") in body
    assert body.count("no longer in the Step 1 inventory") == 1
    assert "'Drop & reload'" in body  # app.a and app.b can still be compared and repaired
    assert g4.monitor_resolved_notice(findings, {"app.a", "app.b", "app.c"}, None) is None
    cdc_only = g4.monitor_resolved_notice({"app.a": findings["app.a"]}, set(),
                                          MigrationType.CDC_ONLY)[1]
    assert "Cut over waits until each matches" in cdc_only and "by hand" in cdc_only


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_a_table_only_gone_from_the_inventory_gets_no_checksum_or_repair_promise(mtype) -> None:
    """8a on purpose: the only resolved table left the Step 1 inventory. The notice said
    Validation compares it, then that it cannot, named the inventory twice and ended with a
    Drop & reload repair for a table that no longer exists."""
    gone = _finding(resolved=T0, by="not-in-inventory", kinds=("source-table-missing",),
                    changes=[])
    _header, body = g4.monitor_resolved_notice({"app.orders": gone}, set(), mtype,
                                               inventory_names={"app.other"})
    assert body == (
        "app.orders (no longer in the Step 1 inventory since 2026-09-01 12:00 UTC). app.orders "
        "was dropped or renamed at the source, so Validation cannot compare it: if that was "
        "intended, acknowledge it on the Cut over step."
    )
    two = {"app.a": gone, "app.b": gone}
    plural = g4.monitor_resolved_notice(two, set(), mtype, inventory_names={"app.other"})[1]
    assert "app.a, app.b were dropped or renamed at the source, so Validation cannot compare them" in plural
    # Once waived on the Cut over step: the recorded decision, nothing to compare or repair.
    _header, waived = g4.monitor_resolved_notice({"app.orders": gone}, set(), mtype,
                                                 waived={"app.orders"},
                                                 inventory_names={"app.other"})
    assert "that decision is in the activity log" in waived
    for absent in ("compares these tables by CHECKSUM", "Drop & reload", "by hand",
                   "Cut over waits until"):
        assert absent not in waived and absent not in body


def test_the_waivable_notice_agrees_with_the_number_of_tables() -> None:
    gone = _finding(resolved=T0, by="not-in-inventory", kinds=(KIND_SOURCE_TABLE_MISSING,),
                    changes=[])
    _h, one = g4.cutover_waivable_notice(_gate({"app.u": gone}, inventory_names=frozenset()))
    assert one.startswith("app.u is not in the validation report (left out on the Validation "
                          "step, or no longer in the Step 1 inventory because it was dropped")
    assert "or is part of the gap you accepted, so Validation cannot show that its values" in one
    _h, two = g4.cutover_waivable_notice(
        _gate({"app.u": gone, "app.v": gone}, inventory_names=frozenset()))
    assert two.startswith("app.u, app.v are not in the validation report")
    assert "because they were dropped" in two and "that their values are right" in two


def test_no_header_says_drift() -> None:
    gate = _gate({"app.t": _finding(), "app.u": _finding(resolved=T0)},
                 items=[], scope=("app.t", "app.u"), inventory_names=frozenset({"app.t"}))
    headers = [
        _open_notice({"app.t": _finding()}).header,
        g4.monitor_resolved_notice({"app.t": _finding(resolved=T0)}, set(), None)[0],
        g4.monitor_unchecked_notice(("app.t",), None, has_password=False)[1],
        g4.cutover_open_notice(gate)[0],
        g4.cutover_waivable_notice(
            _gate({"app.u": _finding(resolved=T0)}, inventory_names=frozenset()))[0],
        g4.catalog_drift_verdict_hold(gate, total=1, as_of="x")[1],
    ]
    assert all("drift" not in h.lower() for h in headers)


# --- logging and secrets -------------------------------------------------------------------


def _logged(monkeypatch):
    import dsql_migrator.core.activity_log as act

    lines: list = []
    monkeypatch.setattr(act, "log_activity", lambda _c, action, **kw: lines.append((action, kw)))
    return lines


def test_a_thousand_tables_opening_at_once_write_one_line(monkeypatch) -> None:
    from dsql_migrator.core.activity_log import ActivityStatus

    lines = _logged(monkeypatch)
    names = [f"app.t{i:04d}" for i in range(1000)]
    _f, _d, tr = fold_catalog_drift_probe(
        {}, {}, _probe(names, drift={n: _diff() for n in names})
    )
    g4.log_catalog_drift_transitions(tr)
    ((action, kw),) = lines
    assert action == "source schema changed on 1000 captured tables: CDC does not carry it"
    assert kw["status"] is ActivityStatus.FAILURE and kw["target"] == "1000 tables"
    assert len(kw["detail"]) <= 600 and "+994 more tables" in kw["detail"]
    lines.clear()
    _f, _d, tr = fold_catalog_drift_probe({}, {}, _probe(["app.t"], drift={"app.t": _diff()}))
    g4.log_catalog_drift_transitions(tr)
    ((action, kw),) = lines
    assert action == "source schema changed on a captured table: CDC does not carry it"
    assert kw["target"] == "app.t" and kw["detail"].startswith("app.t: column dropped")


def test_resolved_rebaselined_and_left_lines_are_warnings(monkeypatch) -> None:
    from dsql_migrator.core.activity_log import ActivityStatus

    lines = _logged(monkeypatch)
    _f, _d, tr = fold_catalog_drift_probe(
        {"app.a": _finding(), "app.g": _finding()}, {"app.b": "old"},
        _probe(["app.a", "app.b"], not_in=["app.g"]),
    )
    g4.log_catalog_drift_transitions(tr)
    assert [a for a, _kw in lines] == [
        "source schema matches the Step 1 inventory again",
        "Step 1 reads a changed definition for a captured table",
        "captured table left the Step 1 inventory: source schema finding closed",
    ]
    assert all(kw["status"] is ActivityStatus.WARNING for _a, kw in lines)
    assert "Cut over waits for a CHECKSUM match started after 2026-09-01 12:01:01 UTC" in (
        lines[0][1]["detail"]
    )


def _patched_factory(monkeypatch, rows=(), fail=None):
    import dsql_migrator.ui.data_migration._cdc_status as status

    calls, built = [], []

    class _Conn:
        def execute(self, statement, params=None):
            calls.append((str(statement), dict(params or {})))
            return SimpleNamespace(fetchall=lambda: list(rows))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Engine:
        def connect(self):
            if fail is not None:
                raise fail
            return _Conn()

        def dispose(self):
            built.append("disposed")

    def _factory(password, *, read_timeout_seconds=None):
        built.append(("factory", read_timeout_seconds))
        return lambda _config: _Engine()

    monkeypatch.setattr(status, "make_source_engine_factory", _factory)
    return calls, built


def test_the_blocking_read_never_connects_without_a_password(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_catalog_drift_once

    calls, built = _patched_factory(monkeypatch)
    probe = read_cdc_catalog_drift_once(_session(password=False), _request(_table()))
    assert probe.ok is False and probe.problem == "no-source-connection"
    assert calls == [] and built == []
    # No table to read: no connection either, but a gone table is still folded.
    probe = read_cdc_catalog_drift_once(_session(), _request(not_in=["app.gone"]))
    assert probe.ok and probe.not_in_baseline == ("app.gone",) and built == []


def test_the_blocking_read_is_bounded_disposed_and_diffs_on_the_worker(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_catalog_drift_once

    rows = [("c", "app", "orders", "id", 1, "int", "NO", ""), ("k", "app", "orders", "id", 1,
                                                                 None, None, None)]
    calls, built = _patched_factory(monkeypatch, rows)
    probe = read_cdc_catalog_drift_once(_session(), _request(_table()))
    assert ("factory", 10) in built and "disposed" in built and len(calls) == 1
    assert probe.ok and probe.drift["app.orders"].kinds == (KIND_DROP_COLUMN,)


def test_a_failed_read_keeps_only_the_error_class(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_catalog_drift_once

    lines = _logged(monkeypatch)
    _patched_factory(monkeypatch, fail=ConnectionError("db.example.com refused s3cr3t-pw"))
    ms = _state()
    ms.cdc_catalog_drift_findings = {"app.orders": _finding()}
    probe = read_cdc_catalog_drift_once(_session(), _request(_table()))
    assert probe.error_class == "ConnectionError" and probe.problem == "read-failed"
    g4.log_catalog_drift_transitions(ms.apply_cdc_catalog_drift_probe(probe))
    text = repr(probe) + repr(ms.cdc_catalog_drift_findings) + repr(lines)
    gate = g4.catalog_drift_gate_for(ms, None, _session(), cdc_in_use=True, release="clean",
                                     summary=None)
    copy = g4.cutover_open_notice(gate)[1] + _open_notice(
        ms.cdc_catalog_drift_findings, last=probe).tail
    assert "failed (ConnectionError)" in copy
    for secret in ("db.example.com", "s3cr3t-pw"):
        assert secret not in text and secret not in copy


def test_the_request_skips_exclusions_and_stored_generated_columns(monkeypatch) -> None:
    table = TableDef(
        name="s.t",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="g", mysql_type="integer", generated=True, generated_kind="STORED",
                      generated_expression="id * 2"),
            ColumnDef(name="doc", mysql_type="text"),
        ],
        primary_key=["id"],
    )
    inv = SourceInventory(tables=[table])
    ms = _state(CatalogBaseline(inventory=inv, captured=(table,), capture_names=frozenset({"s.t"}),
                                source_type=SourceType.POSTGRES))
    import dsql_migrator.ui.data_migration._models as models

    monkeypatch.setattr(models, "effective_lob_exclusions",
                        lambda _ms, _inv, **_k: {"s.t": frozenset({"doc"})})
    ms.record_cdc_target_added_columns("s.t", {"x": "integer"})
    req = g4.catalog_drift_request(ms, g4.catalog_drift_baseline(ms))
    assert req.skip == {"s.t": frozenset({"doc", "g"})}
    assert req.target_added == {"s.t": {"x": "integer"}}
    assert [t.name for t in req.tables] == ["s.t"] and req.token[1] == (("s.t", (("x", "integer"),)),)
    probe = build_catalog_drift_probe(
        req,
        {"s.t": LiveTable(columns=(LiveColumn("id", "integer", False, False, 0),
                                   LiveColumn("x", "integer", True, False, 1)),
                          primary_key=frozenset({"id"}))},
        started=T0, finished=T0,
    )
    assert probe.drift == {}  # doc / g dropped: not captured; x: added through Fix


def test_the_request_carries_effective_exclusions_and_both_message_key_maps(monkeypatch) -> None:
    def _t(name, cols, generated=()):
        return TableDef(
            name=name,
            columns=[
                ColumnDef(name=c, mysql_type="integer", nullable=c != "id",
                          generated=c in generated,
                          generated_kind="STORED" if c in generated else None,
                          generated_expression="id * 2" if c in generated else None)
                for c in cols
            ],
            primary_key=["id"],
        )

    t = _t("app.t", ("id", "tenant_id", "doc", "g"), generated=("g",))
    u = _t("app.u", ("id", "org"))
    inv = SourceInventory(tables=[t, u])
    ms = _state(CatalogBaseline(inventory=inv, captured=(t, u),
                                capture_names=frozenset({"app.t", "app.u"}),
                                source_type=SourceType.POSTGRES))
    import dsql_migrator.ui.data_migration._models as models

    monkeypatch.setattr(models, "effective_lob_exclusions",
                        lambda _ms, _inv, **_k: {"app.t": frozenset({"doc"})})
    ms.set_cdc_message_key_columns({"app.t": ["tenant_id", "id"]})
    ms.cdc_deployed_message_key_columns = {"app.u": ["org", "id"]}
    assert g4.cdc_rekey_columns(ms) == {
        "app.t": frozenset({"tenant_id", "id"}), "app.u": frozenset({"org", "id"}),
    }
    req = g4.catalog_drift_request(ms, g4.catalog_drift_baseline(ms))
    assert req.rekey == {"app.t": frozenset({"tenant_id", "id"}),
                         "app.u": frozenset({"org", "id"})}
    # The effective exclusions only: a generated column is skipped, never "excluded".
    assert req.excluded == {"app.t": frozenset({"doc"})}
    assert req.skip == {"app.t": frozenset({"doc", "g"})}

    def _live_t(second):
        return LiveTable(
            columns=(LiveColumn("id", "integer", False, False, 0),
                     LiveColumn(second, "integer", True, False, 1),
                     LiveColumn("doc", "integer", True, False, 2),
                     LiveColumn("g", "integer", True, True, 3)),
            primary_key=frozenset({"id"}),
        )

    live = {"app.t": _live_t("org_id"),
            "app.u": LiveTable(columns=(LiveColumn("id", "integer", False, False, 0),
                                        LiveColumn("org", "integer", True, False, 1)),
                               primary_key=frozenset({"id"}))}
    probe = build_catalog_drift_probe(req, live, started=T0, finished=T0)
    # tenant_id (a re-key column) renamed to org_id: reported, and Fix is not offered.
    renamed = probe.drift["app.t"]
    assert renamed.kinds == (KIND_RENAME_COLUMN,) and renamed.fixable is False
    # Without the re-key map the same rename is fixable (a plain column).
    plain = build_catalog_drift_probe(
        g4.CatalogDriftRequest(**{**req.__dict__, "rekey": {}}), live, started=T0, finished=T0
    )
    assert plain.drift["app.t"].fixable is True
    # Unreadable maps are "no re-key known", never a crash.
    broken = SimpleNamespace(cdc_message_key_columns=lambda: 1 / 0)
    assert g4.cdc_rekey_columns(broken) == {}


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_cut_over_copy_per_migration_type(mtype) -> None:
    gate = g4.CatalogDriftGate(
        applies=True,
        issues=(g4.CatalogDriftIssue("app.t", _finding(resolved=T0), "compared-before"),),
        migration_type=mtype, has_source_connection=True,
    )
    header, body = g4.cutover_must_recheck_notice(gate)
    assert header == "Re-check 1 schema-changed table(s) with CHECKSUM before cutting over"
    assert "app.t: changed from at least" in body and "its last comparison started before that" in body
    assert ("'Drop & reload'" in body) is (mtype is MigrationType.FULL_LOAD_AND_CDC)


def test_the_only_captured_table_dropped_on_purpose_can_still_be_closed(monkeypatch) -> None:
    """8a on purpose: the one captured table is dropped and Step 1 re-run. The captured set
    is now empty, yet the open finding must close as "not-in-inventory" (then waivable),
    never stay open forever."""
    import asyncio

    import dsql_migrator.ui.data_migration._cdc_status as status

    monkeypatch.setattr(status, "spawn_detached",
                        lambda work: asyncio.get_running_loop().create_task(work()))
    other = _table("app.other")
    ms = _state(CatalogBaseline(inventory=SourceInventory(tables=[other]), captured=(),
                                capture_names=frozenset({"app.orders"})))
    ms.cdc_catalog_drift_findings = {"app.orders": _finding(kinds=(KIND_SOURCE_TABLE_MISSING,),
                                                            changes=[])}
    session = _session()
    reads: list = []

    def _once(_session, request):
        reads.append(request)
        now = datetime.now(timezone.utc)
        return build_catalog_drift_probe(request, {}, started=now, finished=now)

    monkeypatch.setattr(status, "read_cdc_catalog_drift_once", _once)
    import sys
    import types

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(g4, "log_catalog_drift_transitions", lambda _t: None)

    async def _scenario():
        assert g4.start_catalog_drift_probe(session, ms) is True
        for _ in range(10):
            await asyncio.sleep(0)

    asyncio.run(_scenario())
    (request,) = reads
    assert request.tables == () and request.not_in_baseline == ("app.orders",)
    finding = ms.cdc_catalog_drift_findings["app.orders"]
    assert finding.resolved_by == "not-in-inventory"
    gate = g4.catalog_drift_gate_for(ms, None, session, cdc_in_use=True, release="clean",
                                     summary=None, check_probe=False)
    assert [i.reason for i in gate.issues] == ["not-validated"] and gate.can_waive


def test_name_lists_are_bounded_at_six() -> None:
    names = [f"app.t{i}" for i in range(8)]
    hint = g4.catalog_drift_options_hint(names)
    assert "app.t5 (+2 more)" in hint and "app.t6" not in hint
    assert g4.catalog_drift_options_hint([]) == ""
    clause = g4.catalog_drift_start_log_clause(names)
    assert clause.endswith("(+2 more)")


# --- review follow-ups: scope branches -----------------------------------------------------


def test_an_open_finding_on_an_uncaptured_inventory_table_is_still_read() -> None:
    """A changed selection or watermark can drop a table with an open finding from the
    captured set while it stays in the inventory: it must still be read, or the finding can
    never resolve and Cut over is held for good."""
    orders, other = _table(), _table("app.other")
    ms = _state(CatalogBaseline(inventory=SourceInventory(tables=[orders, other]),
                                captured=(orders,), capture_names=frozenset({orders.name}),
                                source_type=SourceType.MYSQL))
    ms.cdc_catalog_drift_findings = {"app.other": _finding()}
    request = g4.catalog_drift_request(ms, g4.catalog_drift_baseline(ms))
    assert [t.name for t in request.tables] == ["app.orders", "app.other"]
    assert request.not_in_baseline == ()
    probe = build_catalog_drift_probe(request, {"app.orders": _live(), "app.other": _live()},
                                      started=T0, finished=T0)
    out, _d, _t = fold_catalog_drift_probe(ms.cdc_catalog_drift_findings, {}, probe)
    assert out["app.other"].resolved_by == "matched"


def test_a_compared_table_gone_after_a_step_1_rerun_is_recorded_not_in_inventory() -> None:
    """8a: a captured, already compared table is dropped and Step 1 re-run before the next
    read. It must not silently leave the check: it is recorded, and Cut over asks for an
    acknowledgement."""
    other = _table("app.other")
    ms = _state(CatalogBaseline(inventory=SourceInventory(tables=[other]), captured=(other,),
                                capture_names=frozenset({"app.orders", "app.other"}),
                                source_type=SourceType.MYSQL))
    # Never compared (no digest): nothing to record.
    assert g4.catalog_drift_request(ms, g4.catalog_drift_baseline(ms)).not_in_baseline == ()
    ms.cdc_catalog_drift_digests = {"app.orders": "d", "app.other": "d"}
    request = g4.catalog_drift_request(ms, g4.catalog_drift_baseline(ms))
    assert request.not_in_baseline == ("app.orders",)
    probe = build_catalog_drift_probe(request, {"app.other": _live()}, started=T0, finished=T0)
    out, digests, _t = fold_catalog_drift_probe({}, ms.cdc_catalog_drift_digests, probe)
    assert out["app.orders"].kinds == [KIND_SOURCE_TABLE_MISSING]
    assert out["app.orders"].resolved_by == "not-in-inventory" and "app.orders" not in digests


def test_an_open_finding_keeps_the_read_possible_without_cdc_evidence() -> None:
    """A stack switch clears the CDC evidence but keeps the findings: an open one must still
    be re-read, or it could never clear."""
    ms = _state(evidence=False)
    assert not catalog_drift_probe_possible(ms, _session())
    ms.cdc_catalog_drift_findings = {"app.orders": _finding()}
    assert catalog_drift_probe_possible(ms, _session())


# --- review follow-ups: the gate -----------------------------------------------------------


def test_continue_without_this_check_is_withheld_while_anything_else_holds() -> None:
    """A waivable table (gone from the inventory) next to an open finding, a due re-check
    or a pending read: the waiver is offered only once nothing else holds."""
    gone = _finding(resolved=T0, by="not-in-inventory", kinds=(KIND_SOURCE_TABLE_MISSING,),
                    changes=[])
    opened = _gate({"app.a": _finding(), "app.gone": gone}, items=[_item("app.a")],
                   run_start=T0 + timedelta(hours=1), scope=("app.a",),
                   inventory_names={"app.a"})
    assert opened.open and opened.waivable and not opened.can_waive
    must = _gate({"app.a": _finding(resolved=T0), "app.gone": gone}, items=[_item("app.a")],
                 run_start=T0 + timedelta(seconds=30), scope=("app.a",),
                 inventory_names={"app.a"})
    assert must.must_recheck and must.waivable and not must.open and not must.can_waive
    pending = _gate({"app.gone": gone}, items=[_item()], run_start=T0 + timedelta(hours=1),
                    check_probe=True, token=("inv", ()), evidence_at=T0)
    assert pending.pending and pending.waivable and not pending.can_waive
    alone = _gate({"app.gone": gone}, items=[_item()], run_start=T0 + timedelta(hours=1),
                  inventory_names=frozenset())
    assert alone.can_waive  # nothing else holds: offered


def test_a_per_table_recheck_clears_a_resolved_finding() -> None:
    """Validation's "Re-check them below" records a per-table start time: it is what proves
    the table, even though the full run started before the resolve."""
    resolved = {"app.t": _finding(resolved=T0)}
    early = _gate(resolved, items=[_item()], run_start=T0 - timedelta(hours=2))
    assert [i.reason for i in early.issues] == ["compared-before"] and early.blocks
    rechecked = _gate(resolved, items=[_item()], run_start=T0 - timedelta(hours=2),
                      overrides={"app.t": T0 + timedelta(minutes=5)})
    assert rechecked.resolved == ("app.t",) and not rechecked.blocks and not rechecked.held
    vs = SimpleNamespace(
        result=ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[_item()]),
        compared_run_start=T0 - timedelta(hours=2),
        compared_overrides={"app.t": T0 + timedelta(minutes=5)},
    )
    assert g4.catalog_drift_proven_tables(resolved, vs, {"app.t"}) == {"app.t"}


def test_an_accepted_gap_table_is_waivable_with_its_own_remedy() -> None:
    resolved = {"app.t": _finding(resolved=T0)}
    gate = _gate(resolved, items=[_item()], run_start=T0 + timedelta(hours=1),
                 explained=("app.t",), release="accepted")
    assert [i.reason for i in gate.issues] == ["accepted-gap"] and gate.can_waive
    header, body = g4.cutover_waivable_notice(gate)
    assert header == "1 table(s) whose source schema changed have no CHECKSUM proof"
    assert "cannot be cleared by a CHECKSUM unless its source rows are corrected" in body


def test_cut_over_waits_for_a_read_that_started_after_the_latest_validation() -> None:
    """A read that started before the latest validation cannot show the source the
    validation compared: the gate built from the real states is pending (and holds)."""
    ms = _state()
    baseline = g4.catalog_drift_baseline(ms)
    request = g4.catalog_drift_request(ms, baseline)
    now = datetime.now(timezone.utc)
    stale = now - timedelta(minutes=2)
    ms.apply_cdc_catalog_drift_probe(
        build_catalog_drift_probe(request, {"app.orders": _live()}, started=stale,
                                  finished=stale)
    )
    vs = SimpleNamespace(result=None, completed_at=now - timedelta(minutes=1),
                         rechecked_at=None, compared_run_start=None, compared_overrides={},
                         cutover_catalog_drift_waiver=None)
    gate = g4.catalog_drift_gate_for(ms, vs, _session(), cdc_in_use=True, release="clean",
                                     summary=None)
    assert gate.probe_state == "pending" and gate.held
    fresh = now + timedelta(seconds=1)
    ms.apply_cdc_catalog_drift_probe(
        build_catalog_drift_probe(request, {"app.orders": _live()}, started=fresh,
                                  finished=fresh)
    )
    gate = g4.catalog_drift_gate_for(ms, vs, _session(), cdc_in_use=True, release="clean",
                                     summary=None)
    assert gate.probe_state == "ok" and not gate.held


# --- review follow-ups: copy ---------------------------------------------------------------


def test_a_dropped_or_added_primary_key_reads_none_never_empty_parentheses() -> None:
    from dsql_migrator.core.cdc_catalog_drift import KIND_KEY_CHANGE, diff_table

    keyless = LiveTable(columns=_live().columns, primary_key=frozenset())
    dropped = diff_table(_table(), keyless, source_type=SourceType.MYSQL)
    assert dropped.kinds == (KIND_KEY_CHANGE,)
    assert g4.change_fact(dropped.changes[0]) == "primary key was (id), now none"
    finding = CdcCatalogDriftFinding(kinds=list(dropped.kinds), changes=list(dropped.changes),
                                     first_seen_at=T0)
    assert "()" not in g4.finding_row("app.orders", finding, is_pg=False)
    no_key = TableDef(name="app.orders", columns=_table().columns, primary_key=[])
    added = diff_table(no_key, _live(), source_type=SourceType.MYSQL)
    assert g4.change_fact(added.changes[0]) == "primary key was none, now (id)"


# --- review follow-ups: logging, state, the claim ------------------------------------------


def test_a_reopened_finding_is_logged_as_a_failure(monkeypatch) -> None:
    from dsql_migrator.core.activity_log import ActivityStatus

    lines = _logged(monkeypatch)
    resolved = {"app.t": _finding(resolved=T0)}
    _f, _d, tr = fold_catalog_drift_probe(
        resolved, {"app.t": "d0"},
        _probe(["app.t"], drift={"app.t": _diff()}, started=T0 + timedelta(hours=1)),
    )
    assert [t.kind for t in tr] == ["reopened"]
    g4.log_catalog_drift_transitions(tr)
    ((action, kw),) = lines
    assert action == "source schema changed on a captured table: CDC does not carry it"
    assert kw["status"] is ActivityStatus.FAILURE and kw["target"] == "app.t"


def test_a_successful_read_draws_no_cant_recheck_notice() -> None:
    ok = _probe(["app.t"])
    assert g4.monitor_unchecked_notice(("app.t",), ok, has_password=True) is None
    assert g4.monitor_unchecked_notice(("app.t",), None, has_password=True) is None


def test_a_second_fix_on_the_same_table_keeps_the_first_fixs_columns() -> None:
    ms = DataMigrationState()
    ms.record_cdc_target_added_columns("app.t", {"a": "int"})
    first = ms.cdc_target_added_columns
    ms.record_cdc_target_added_columns("app.t", {"b": "text"})
    assert ms.cdc_target_added_columns == {"app.t": {"a": "int", "b": "text"}}
    assert first == {"app.t": {"a": "int"}}  # copy-on-write


def test_a_silent_fact_refresh_is_stored_on_the_state() -> None:
    """An open finding whose facts change (here it stops being fixable) refreshes with no
    transition -- the state must still store it, or Fix stays offered for a stale plan."""
    ms = DataMigrationState()
    ms.cdc_catalog_drift_findings = {"app.t": _finding(
        kinds=(KIND_ADD_COLUMN,), fixable=True,
        changes=[CdcCatalogDriftChange(kind=KIND_ADD_COLUMN, column="x")],
    )}
    ms.cdc_catalog_drift_digests = {"app.t": "d0"}
    refreshed = CatalogDiff(
        kinds=(KIND_ADD_COLUMN,),
        changes=(CdcCatalogDriftChange(kind=KIND_ADD_COLUMN, column="x"),
                 CdcCatalogDriftChange(kind=KIND_ADD_COLUMN, column="g", generated=True)),
        fixable=False,
    )
    assert ms.apply_cdc_catalog_drift_probe(_probe(["app.t"], drift={"app.t": refreshed})) == []
    finding = ms.cdc_catalog_drift_findings["app.t"]
    assert finding.fixable is False and len(finding.changes) == 2
    assert finding.first_seen_at == T0


def test_the_claim_is_released_when_building_the_request_or_spawning_fails(monkeypatch) -> None:
    import asyncio

    import dsql_migrator.ui.data_migration._cdc_status as status

    ms = _state()
    real = g4.catalog_drift_request

    def _broken(*_a, **_k):
        raise RuntimeError("build")

    monkeypatch.setattr(g4, "catalog_drift_request", _broken)
    with pytest.raises(RuntimeError):
        g4._claim_and_build(ms, g4.catalog_drift_baseline(ms))
    assert ms.cdc_catalog_drift_probe_running is False
    monkeypatch.setattr(g4, "catalog_drift_request", real)

    def _spawn(_work):
        raise RuntimeError("spawn")

    monkeypatch.setattr(status, "spawn_detached", _spawn)

    async def _scenario():
        with pytest.raises(RuntimeError):
            g4.start_catalog_drift_probe(_session(), ms)

    asyncio.run(_scenario())
    assert ms.cdc_catalog_drift_probe_running is False


# --- review follow-ups: Start over during a read -------------------------------------------


@pytest.mark.parametrize("start_over", [True, False])
def test_a_read_in_flight_during_start_over_lands_nowhere(monkeypatch, start_over) -> None:
    """A read started before Start over must not fold into the new migration: it would open
    a finding (holding Cut over, even for Full load only) about the previous one, or seed
    digests that turn its first read into a "definition changed" record."""
    import asyncio
    import sys
    import types

    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.data_migration import DataMigrationStore

    lines = _logged(monkeypatch)
    store = DataMigrationStore()
    ms = store.get_or_create("s1")
    ms.set_cdc_ops_window_start(T0)
    table = _table()
    baseline = CatalogBaseline(inventory=SourceInventory(tables=[table]), captured=(table,),
                               capture_names=frozenset({table.name}),
                               source_type=SourceType.MYSQL)
    ms.cdc_catalog_baseline = lambda: baseline
    dropped = {"app.orders": _live(cols=(("id", "int", False),))}
    monkeypatch.setattr(
        status, "read_cdc_catalog_drift_once",
        lambda _s, request: build_catalog_drift_probe(request, dropped, started=T0,
                                                      finished=T0),
    )
    gate: dict = {}

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            await gate["release"].wait()
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    async def _scenario():
        gate["release"] = asyncio.Event()
        request = g4._claim_and_build(ms, g4.catalog_drift_baseline(ms))
        assert request.epoch is ms._cdc_catalog_drift_epoch
        task = asyncio.get_running_loop().create_task(g4._probe_and_land(_session(), ms, request))
        await asyncio.sleep(0)
        if start_over:
            before = ms._cdc_catalog_drift_epoch
            store.reset_in_place("s1")
            assert ms._cdc_catalog_drift_epoch is not before
            assert ms.cdc_catalog_baseline is not None  # the builder handle survives
            assert ms.begin_cdc_catalog_drift_probe()  # the new migration's own read
        gate["release"].set()
        await task

    asyncio.run(_scenario())
    if start_over:
        assert ms.cdc_catalog_drift_findings == {} and ms.cdc_catalog_drift_digests == {}
        assert ms.cdc_catalog_drift_last_probe is None and lines == []
        assert ms.cdc_catalog_drift_probe_running  # the new claim was not released
    else:
        assert ms.cdc_catalog_drift_findings["app.orders"].kinds == [KIND_DROP_COLUMN]
        assert len(lines) == 1 and not ms.cdc_catalog_drift_probe_running


# --- review follow-ups: the Validation / Cut over scheduler --------------------------------


@pytest.fixture
def fake_run(monkeypatch):
    import sys
    import types

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)


def _timers():
    timers: list = []
    return timers, SimpleNamespace(timer=lambda _i, cb, once=False: timers.append(cb))


def _fresh_ok_probe(ms, *, digests=None):
    baseline = g4.catalog_drift_baseline(ms)
    now = datetime.now(timezone.utc)
    return CdcCatalogDriftProbe(
        started_at=now, finished_at=now, ok=True, scope=("app.orders",),
        token=g4.catalog_drift_token(ms, baseline), inventory=baseline.inventory,
        digests=digests if digests is not None else {"app.orders": "d"},
    )


def test_check_again_forces_a_read_even_when_the_last_one_is_fresh(fake_run, monkeypatch) -> None:
    import asyncio

    import dsql_migrator.ui.data_migration._cdc_status as status

    ms = _state()
    ms.apply_cdc_catalog_drift_probe(_fresh_ok_probe(ms))
    reads: list = []

    def _read(_session, request):
        reads.append(1)
        return _fresh_ok_probe(ms)

    monkeypatch.setattr(status, "read_cdc_catalog_drift_once", _read)
    timers, ui = _timers()
    refreshes: list = []
    g4.schedule_catalog_drift_probe(ui, _session(), ms, lambda: refreshes.append(1),
                                    evidence_at=None)
    assert timers == []  # fresh: nothing due
    g4.schedule_catalog_drift_probe(ui, _session(), ms, lambda: refreshes.append(1),
                                    evidence_at=None, force=True)
    (check,) = timers
    asyncio.run(check())
    assert reads == [1] and refreshes == [1]


def test_a_page_waits_for_a_read_another_tab_holds_and_then_refreshes(fake_run,
                                                                       monkeypatch) -> None:
    import asyncio

    import dsql_migrator.ui.data_migration._cdc_status as status

    monkeypatch.setattr(status, "read_cdc_catalog_drift_once",
                        lambda *_a: pytest.fail("the other tab's read is not repeated"))
    monkeypatch.setattr(g4, "_WAIT_FOR_OTHER_READ_SECONDS", 5.0)
    ms = _state()
    timers, ui = _timers()
    refreshes: list = []
    assert ms.begin_cdc_catalog_drift_probe() is True  # another tab (or the poll) reads
    g4.schedule_catalog_drift_probe(ui, _session(), ms, lambda: refreshes.append(1),
                                    evidence_at=None)
    (check,) = timers

    async def _scenario():
        task = asyncio.get_running_loop().create_task(check())
        await asyncio.sleep(0.1)
        assert refreshes == []  # still waiting
        ms.apply_cdc_catalog_drift_probe(_fresh_ok_probe(ms))
        ms.finish_cdc_catalog_drift_probe()
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(_scenario())
    assert refreshes == [1]


def test_the_type_change_why_and_runbook_reflect_the_value_fit_guard():
    """A widened type no longer lands rounded or as true on a current sink -- it is
    dead-lettered -- but the UI cannot tell which sink a deployed stack runs, so the copy must
    hedge for CDC infrastructure deployed by an earlier version, and for the one case the guard
    does not check (a FLOAT/DOUBLE value for a numeric column), for which this row is the only
    signal. And the runbook must not claim Drop & reload keeps a retyped column's old type: it
    recreates the table from a conversion of the re-run Step 1, except where the table's DDL
    was edited in Schema Conversion (the edit wins), so THAT is where the type must change."""
    from dsql_migrator.core.converter import parse_target_value_specs
    from dsql_migrator.core.exporter import value_fit_rule

    why = g4._WHYS[KIND_TYPE_CHANGE]
    assert "dead-lettered" in why
    assert "converted silently" not in why
    assert "Delete CDC infrastructure" in why
    # The residual the copy names is real on both write paths (the sink's ValueFitTest pins
    # the same verdict): a float is not checked against a numeric column's scale.
    assert "FLOAT or DOUBLE value for a numeric column" in why
    assert "still rounded" in why
    spec = parse_target_value_specs("CREATE TABLE t (c numeric(10,2))")["c"]
    assert value_fit_rule(1.2345, spec) is None
    full = g4.monitor_runbook(MigrationType.FULL_LOAD_AND_CDC)
    assert "keeps its old type" not in full
    assert "with its type from the re-run Step 1" in full
    assert "edited that table's DDL in Schema Conversion" in full
    assert "the edit wins" in full
    assert "edited" not in g4.monitor_runbook(MigrationType.CDC_ONLY)


def test_drop_and_reload_recreates_a_retyped_column_with_the_re_run_step1_type(monkeypatch):
    """Grounds the runbook sentence above: the recreate DDL comes from the CURRENT Step 1
    inventory plus the Schema Conversion edits, so an unedited table gets the new type and an
    edited one keeps its edit."""
    from unittest import mock

    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.data_migration import _full_load_engine as fle
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    def inventory(price: str, flag: str) -> SourceInventory:
        return SourceInventory(
            tables=[_table("orders", (("id", "int", False), ("price", price, True),
                                      ("flag", flag, True)))]
        )

    seen: list[str] = []
    monkeypatch.setattr(fle, "_target_columns_the_conversion_would_drop", lambda *a, **k: [])
    monkeypatch.setattr(fle, "DsqlConnector", mock.MagicMock())
    monkeypatch.setattr(
        fle, "recreate_table", lambda _schema_ddls, ddl, **_k: seen.append(ddl)
    )

    def recreated(inv: SourceInventory, edits: dict) -> str:
        inputs = mock.MagicMock()
        inputs.table_conversions = applied_table_conversions(
            SchemaConverter().convert(inv), edits
        )
        fle._default_table_recreator(inputs)(inv.tables[0])
        return seen[-1].upper().replace(" ", "")

    old = inventory("decimal(10,2)", "tinyint(1)")
    assert "DECIMAL(10,2)" in recreated(old, {}) and "BOOLEAN" in recreated(old, {})
    # Unedited, after the source widened and Step 1 was re-run: the new types.
    new = inventory("decimal(12,4)", "int")
    assert "DECIMAL(12,4)" in recreated(new, {})
    assert "BOOLEAN" not in recreated(new, {})
    # Edited in Step 2: the edit survives the Step 1 re-run, old type and all.
    edited = SchemaConverter().convert(old).tables[0].target_ddl
    assert "DECIMAL(10,2)" in recreated(new, {"orders": edited})
