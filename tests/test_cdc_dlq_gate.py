# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G1: cut over is held while a table CDC dead-lettered a change for lacks a CHECKSUM match
after its last dead letter, and until a fresh, complete read of the dead-letter queue covers
CDC's whole history -- fail closed, with a recorded way forward. Offline: every AWS read is a
double, and the screen renders into a recorder."""

from __future__ import annotations

import asyncio
import dataclasses
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import CdcConnectorError
from dsql_migrator.core.models import (
    ReconcileResult,
    TableValidationResult,
    TargetConnectionConfig,
    ValidationMode,
    ValidationReport,
)

GROUP = "/msk-connect/dsql-cdc-stack-cdc"  # the default stack's dead-letter log group


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _item(
    table: str = "app.t",
    *,
    checksum: bool | None = True,
    matched: bool = True,
    skipped: bool = False,
    error: str | None = None,
    excluded: tuple[str, ...] = (),
) -> TableValidationResult:
    return TableValidationResult(
        table=table,
        source_row_count=1,
        target_row_count=1 if matched else 0,
        row_count_match=matched,
        checksum_match=checksum,
        matched=matched,
        deep_checks_skipped=skipped,
        error=error,
        checksum_excluded_columns=list(excluded),
    )


def _report(*items, mode: ValidationMode = ValidationMode.CHECKSUM, **kw) -> ValidationReport:
    return ValidationReport.build(mode=mode, items=list(items), **kw)


# ---------------------------------------------------------------------------
# The pure gate
# ---------------------------------------------------------------------------

T_OPS = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)  # the CDC start (watermark)
T_DL = datetime(2026, 9, 1, 11, 0, tzinfo=timezone.utc)  # the latest dead letter
T_RUN = datetime(2026, 9, 1, 11, 30, tzinfo=timezone.utc)  # the validation run started
T_EVID = datetime(2026, 9, 1, 11, 40, tzinfo=timezone.utc)  # ...and finished
T_READ = datetime(2026, 9, 1, 11, 45, tzinfo=timezone.utc)  # the dead-letter read started


def _entry(count: int = 1, last_at: datetime = T_DL, kinds=()):
    from dsql_migrator.core.models import CdcDlqLedgerEntry

    return CdcDlqLedgerEntry(count=count, last_at=last_at, kinds=list(kinds))


def _read(*, ok=True, complete=True, code=None, group=GROUP, started=T_READ, history=None):
    from dsql_migrator.core.cdc_dlq import CdcDlqReadRecord

    return CdcDlqReadRecord(
        log_group=group, started_at=started, ok=ok, complete=complete, error_code=code,
        history_start_ms=history,
    )


_DEFAULT = object()


def _gate(
    *,
    ledger=_DEFAULT,
    report=_DEFAULT,
    release="clean",
    explained=(),
    run_start=T_RUN,
    overrides=None,
    evidence_at=T_EVID,
    last_read=_DEFAULT,
    history_from=_DEFAULT,
    cdc_start=T_OPS,
    waiver=None,
    cdc_in_use=True,
    cdc_evidence=True,
    check_reads=True,
):
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_cutover_gate

    return cdc_dlq_cutover_gate(
        ledger={"app.t": _entry()} if ledger is _DEFAULT else ledger,
        cdc_in_use=cdc_in_use,
        cdc_evidence=cdc_evidence,
        check_reads=check_reads,
        report=_report(_item()) if report is _DEFAULT else report,
        release=release,
        explained_tables=explained,
        compared_run_start=run_start,
        compared_overrides=overrides,
        evidence_at=evidence_at,
        log_group=GROUP,
        last_read=_read() if last_read is _DEFAULT else last_read,
        history_from_ms=_ms(T_OPS - timedelta(minutes=5)) if history_from is _DEFAULT else history_from,
        cdc_start=cdc_start,
        waiver=waiver,
    )


@pytest.mark.parametrize("cdc_in_use", [True, False])
def test_no_ledger_and_no_cdc_evidence_is_inert(cdc_in_use):
    gate = _gate(ledger={}, cdc_evidence=False, cdc_in_use=cdc_in_use)
    assert not gate.applies and not gate.held and not gate.blocks and not gate.read_required


def test_no_migration_state_is_inert():
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_gate_for

    gate = cdc_dlq_gate_for(None, None, cdc_in_use=True, release="clean", summary=None)
    assert not gate.applies and not gate.held


def test_full_load_only_without_a_ledger_is_inert_even_with_old_cdc_evidence():
    gate = _gate(ledger={}, cdc_in_use=False, cdc_evidence=True)
    assert not gate.applies


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"report": _report(_item("app.other"))}, "not-validated"),
        (
            {"report": _report(_item(matched=False, checksum=False)), "release": "accepted",
             "explained": ("app.t",)},
            "accepted-gap",
        ),
        ({"report": _report(_item(matched=False, checksum=False))}, "not-matched"),
        ({"report": _report(_item(error="boom", matched=False, checksum=None))}, "not-matched"),
        ({"report": _report(_item(checksum=None), mode=ValidationMode.ROW_COUNT)}, "row-count-only"),
        ({"report": _report(_item(skipped=True))}, "row-count-only"),
        ({"run_start": None}, "compare-time-unknown"),
        ({"run_start": T_DL + timedelta(seconds=59)}, "compared-before"),
        ({"run_start": T_DL - timedelta(minutes=5)}, "compared-before"),
    ],
)
def test_each_reason_in_order(kwargs, reason):
    gate = _gate(**kwargs)
    (issue,) = gate.issues
    assert issue.reason == reason and issue.table == "app.t"
    assert gate.blocks and gate.held


def test_a_checksum_started_more_than_a_minute_after_the_last_dead_letter_resolves():
    gate = _gate(run_start=T_DL + timedelta(seconds=61))
    assert gate.issues == () and gate.resolved == ("app.t",)
    assert not gate.blocks and not gate.held and gate.read_state == "ok"


def test_a_re_check_start_beats_the_run_start():
    late_dl = T_RUN + timedelta(minutes=5)  # landed during/after the run
    stale = _gate(ledger={"app.t": _entry(last_at=late_dl)})
    assert [i.reason for i in stale.issues] == ["compared-before"]
    fresh = _gate(
        ledger={"app.t": _entry(last_at=late_dl)},
        overrides={"app.t": late_dl + timedelta(minutes=2)},
    )
    assert fresh.issues == () and fresh.resolved == ("app.t",)


def test_columns_checksum_cannot_compare_are_disclosed_not_blocking():
    gate = _gate(report=_report(_item(excluded=("doc", "score"))))
    assert gate.issues == () and dict(gate.unverifiable) == {"app.t": ("doc", "score")}
    assert not gate.held


@pytest.mark.parametrize(
    "last_read,evidence_at",
    [
        (None, T_EVID),
        (_read(group="/msk-connect/other-cdc"), T_EVID),
        (_read(started=T_EVID), T_EVID),
        (_read(started=T_EVID - timedelta(seconds=1)), T_EVID),
    ],
)
def test_the_read_is_pending_until_one_starts_after_the_latest_validation(last_read, evidence_at):
    gate = _gate(last_read=last_read, evidence_at=evidence_at)
    assert gate.read_state == "pending" and gate.pending and gate.held
    assert not gate.blocks and not gate.can_waive  # pending is never waivable


def test_a_failed_read_blocks_and_carries_its_code():
    gate = _gate(last_read=_read(ok=False, complete=False, code="AccessDeniedException"))
    assert gate.read_state == "failed" and gate.read_problem == "failed"
    assert gate.blocks and gate.can_waive
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    tone, header, body = read_state_notice(gate)
    assert tone == "error" and header == "Can't read the CDC dead-letter queue"
    assert "AccessDeniedException" in body and "logs:FilterLogEvents" in body


def test_a_page_capped_read_is_incomplete():
    gate = _gate(last_read=_read(complete=False))
    assert gate.read_state == "incomplete" and gate.blocks


def test_a_history_that_starts_after_the_cdc_start_is_partial():
    # ...and after the last comparison, so no CHECKSUM run has cleared it yet.
    gate = _gate(history_from=_ms(T_RUN + timedelta(minutes=1)))
    assert gate.read_state == "partial-history" and gate.blocks and gate.can_waive


def test_an_unknown_cdc_start_is_partial():
    gate = _gate(cdc_start=None, history_from=_ms(T_RUN + timedelta(minutes=1)))
    assert gate.read_state == "partial-history"
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    assert "the pipeline was attached while already running" in read_state_notice(gate)[2]


def test_one_full_checksum_run_after_the_history_start_clears_a_partial_history():
    history = T_DL + timedelta(minutes=10)
    cleared = _gate(
        history_from=_ms(history),
        report=_report(_item(), _item("app.u")),
        run_start=history + timedelta(seconds=61),
    )
    assert cleared.read_state == "ok" and not cleared.held
    one_count_only = _gate(
        history_from=_ms(history),
        report=_report(_item(), _item("app.u", checksum=None)),
        run_start=history + timedelta(seconds=61),
    )
    assert one_count_only.read_state == "partial-history"


def test_full_load_only_with_a_ledger_still_holds_the_tables_but_reads_nothing():
    gate = _gate(cdc_in_use=False, report=_report(_item(checksum=None), mode=ValidationMode.ROW_COUNT))
    assert gate.applies and not gate.read_required and gate.read_state is None
    assert [i.reason for i in gate.issues] == ["row-count-only"] and gate.blocks


def test_held_blocks_and_can_waive_truth_table():
    must = _gate(report=_report(_item(checksum=None)), last_read=_read(ok=False, complete=False))
    assert must.blocks and not must.can_waive  # a re-checkable table is never waivable
    only_read = _gate(last_read=_read(ok=False, complete=False))
    assert only_read.blocks and only_read.can_waive
    only_table = _gate(report=_report(_item("app.other")))
    assert only_table.blocks and only_table.can_waive
    clean = _gate()
    assert not clean.blocks and not clean.held and not clean.can_waive
    assert clean.recheckable == ()
    assert must.recheckable == ("app.t",)


# ---------------------------------------------------------------------------
# The waiver
# ---------------------------------------------------------------------------


def _waiver(tables=None, read_problem=None):
    from dsql_migrator.core.models import CdcDlqWaiver

    return CdcDlqWaiver(tables=tables or {}, read_problem=read_problem, waived_at=T_READ)


def test_a_waiver_covers_an_unprovable_table_at_its_count_and_reopens_on_a_new_one():
    report = _report(_item("app.other"))
    assert _gate(report=report, waiver=_waiver({"app.t": 1})).waived
    grown = _gate(report=report, ledger={"app.t": _entry(count=2)}, waiver=_waiver({"app.t": 1}))
    assert not grown.waived and grown.blocks
    another = _gate(
        report=report,
        ledger={"app.t": _entry(), "app.u": _entry()},
        waiver=_waiver({"app.t": 1}),
    )
    assert not another.waived and another.blocks


def test_a_waiver_covers_the_same_read_problem_only():
    failed = _read(ok=False, complete=False, code="AccessDeniedException")
    assert _gate(last_read=failed, waiver=_waiver(read_problem="failed")).waived
    partial = _gate(last_read=_read(complete=False), waiver=_waiver(read_problem="failed"))
    assert not partial.waived and partial.blocks


def test_a_waiver_never_clears_a_table_that_can_be_rechecked():
    gate = _gate(report=_report(_item(checksum=None)), waiver=_waiver({"app.t": 5}))
    assert gate.blocks and not gate.waived


def test_a_new_verdict_drops_the_waiver_and_a_recheck_keeps_it():
    from dsql_migrator.ui.validation import ValidationState

    state = ValidationState()
    state.set_result(_report(_item()))
    state.cutover_dlq_waiver = _waiver({"app.t": 1})
    state.start_recheck(["app.t"])
    assert state.merge_recheck_result([_item()]) is True
    assert state.cutover_dlq_waiver is not None
    state.set_result(_report(_item()))
    assert state.cutover_dlq_waiver is None
    state.cutover_dlq_waiver = _waiver({"app.t": 1})
    state.clear_outputs()
    assert state.cutover_dlq_waiver is None


def test_the_compare_start_stamps():
    from dsql_migrator.ui.validation import ValidationState

    state = ValidationState()
    state.set_result(_report(_item()))
    assert state.compared_from("app.t") is None  # no start recorded -> unknown
    before = _now()
    state.mark_run_started()
    state.mark_run_finished()  # runs before set_result in the worker's finally
    state.set_result(_report(_item()))
    start = state.compared_run_start
    assert start is not None and start >= before and state.compared_from("app.t") == start
    state.start_recheck(["app.t"])
    recheck_start = _now()
    state.merge_recheck_result([_item()])
    assert state.compared_from("app.t") >= start
    assert state.compared_from("app.t") <= recheck_start
    assert state.compared_overrides.keys() == {"app.t"}


# ---------------------------------------------------------------------------
# The cut-over screen
# ---------------------------------------------------------------------------


class _Cutover:
    """The REAL cut-over screen rendered into a recorder, with the background read doubled."""

    def __init__(self, monkeypatch, *, cdc=True, mtype="FULL_LOAD_AND_CDC", with_store=True,
                 ops_start=True):
        import dsql_migrator.core.activity_log as act
        import dsql_migrator.ui.validation as uiv
        from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
        from dsql_migrator.ui.session import SessionStore
        from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen

        from tests.test_ui_validation import _ScreenUi

        class _Btn(_ScreenUi._El):
            def __init__(self, ui, text):
                super().__init__(ui)
                self.text, self.disabled = text, False

            def props(self, value="", remove=None, **_k):
                if remove and "disable" in str(remove):
                    self.disabled = False
                elif "disable" in str(value).split():
                    self.disabled = True
                return self

        class _Ui(_ScreenUi):
            def __init__(self):
                super().__init__()
                self.timers, self.notes, self.buttons = [], [], []

            def timer(self, _interval, callback, once=False):
                self.timers.append(callback)
                return self._El(self)

            def notify(self, message, **k):
                self.notes.append((message, k.get("type")))

            def button(self, text="", *_a, on_click=None, **_k):
                if on_click is not None:
                    self.clicks.append((str(text), on_click))
                if text:
                    self.texts.append(str(text))
                btn = _Btn(self, str(text))
                self.buttons.append(btn)
                return btn

        class _Run:
            @staticmethod
            async def io_bound(fn, *a, **k):
                return fn(*a, **k)

        self.ui = _Ui()
        fake = types.ModuleType("nicegui")
        fake.ui, fake.run = self.ui, _Run
        monkeypatch.setitem(sys.modules, "nicegui", fake)
        monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: cdc)
        monkeypatch.setattr(uiv, "_cutover_pending_foreign_keys", lambda *a, **k: 0)
        monkeypatch.setattr(uiv, "_cutover_gate_inputs_missing", lambda *a, **k: False)
        self.notices: list[dict] = []

        def _notice(_ui, *, tone, header, body="", **_k):
            self.notices.append({"tone": tone, "header": header, "body": body})
            self.ui.texts.extend([header, body])

        monkeypatch.setattr(uiv, "render_notice", _notice)
        self.logged: list[tuple] = []
        _log = lambda _cat, action, **kw: self.logged.append((action, kw))  # noqa: E731
        monkeypatch.setattr(uiv, "log_activity", _log)
        monkeypatch.setattr(act, "log_activity", _log)
        self.reads: list[int] = []
        self.next_read = None
        import dsql_migrator.ui.data_migration._cdc_status as status

        def _read_once(_ms, _session):
            self.reads.append(1)
            return [], self.next_read, None

        monkeypatch.setattr(status, "read_cdc_dlq_once", _read_once)
        sessions = SessionStore()
        self.session = sessions.get_or_create("s1")
        self.session.set_target(
            TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")
        )
        self.vstore, self.mstore = ValidationStore(), DataMigrationStore()
        self.content, self.runner = build_cutover_screen(
            sessions, "s1", validation_store=self.vstore,
            migration_store=self.mstore if with_store else None,
        )
        self.vs = self.vstore.get_or_create("s1")
        self.vs.proceed_without_foreign_keys = True  # the FK finish gate is not under test
        self.ms = self.mstore.get_or_create("s1")
        self.ms.set_migration_type(getattr(MigrationType, mtype))
        self.t0 = _now()
        if ops_start:
            self.ms.set_cdc_ops_window_start(self.t0 - timedelta(hours=3))
        self.refreshes: list[int] = []

    def dead_letter(self, table="app.t", *, ago=timedelta(hours=2), code="42804", offset=1):
        from dsql_migrator.ui.data_migration import _apply_cdc_status

        err = CdcConnectorError(
            table=table, message=f"DLQ offset={offset}: boom", error_code=code,
            occurred_at=_now() - ago,
        )
        _apply_cdc_status(self.ms, ([], {}, [err], {}, {}, []))

    def validate(self, *items, mode=ValidationMode.ROW_COUNT, started_ago=timedelta(minutes=90),
                 finished_ago=timedelta(minutes=80), **kw):
        self.vs.mark_run_started()
        self.vs._run_started_wall = _now() - started_ago
        self.vs.mark_run_finished()
        self.vs._completed_at = _now() - finished_ago
        self.vs.set_result(ValidationReport.build(mode=mode, items=list(items), **kw))

    def record(self, *, ok=True, complete=True, code=None, ago=timedelta(seconds=1)):
        from dsql_migrator.core.cdc_dlq import CdcDlqReadRecord

        return CdcDlqReadRecord(
            log_group=GROUP, started_at=_now() - ago, ok=ok, complete=complete,
            error_code=code, history_start_ms=_ms(self.t0 - timedelta(hours=4)),
        )

    def landed(self, **kw):
        """A dead-letter read that already landed (promoted after its fold)."""
        self.ms.promote_cdc_dlq_read(self.record(**kw))

    def render(self):
        self.ui.texts.clear()
        self.ui.clicks.clear()
        self.ui.timers.clear()
        self.ui.buttons.clear()
        self.notices.clear()
        self.content(lambda: self.refreshes.append(1))

    def run(self, callback):
        asyncio.run(callback())

    def click(self, text):
        (handler,) = [cb for label, cb in self.ui.clicks if label == text]
        handler()

    def button(self, text):
        (btn,) = [b for b in self.ui.buttons if b.text == text]
        return btn

    def body(self):
        return self.ui.body()

    def acknowledged(self):
        return [kw for action, kw in self.logged if action == "cut over acknowledged"]


def test_a_row_count_match_does_not_release_cut_over_while_a_table_has_a_dead_letter(monkeypatch):
    """The gap itself: a CDC migration's clean ROW_COUNT report released the runbook."""
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(checksum=None))
    s.render()
    assert "I've cut over to DSQL" not in s.ui.texts
    assert "Validation passed — you're ready to switch your app to DSQL" not in s.body()
    assert "Re-check 1 table(s) with CHECKSUM before cutting over" in s.body()


def test_the_render_never_reads_and_one_background_read_releases_the_runbook(monkeypatch):
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(), mode=ValidationMode.CHECKSUM)  # compared after the dead letter
    s.render()
    assert "Checking the CDC dead-letter queue" in s.body()
    assert "I've cut over to DSQL" not in s.ui.texts
    assert len(s.ui.timers) == 1 and s.reads == []  # armed, never read on the render
    s.next_read = s.record()
    s.run(s.ui.timers[0])
    assert s.reads == [1] and s.refreshes == [1]
    assert s.ms.cdc_dlq_last_read is s.next_read
    s.render()
    assert "I've cut over to DSQL" in s.ui.texts
    assert "Validation passed — you're ready to switch your app to DSQL" in s.body()
    assert s.ui.timers == []  # fresh: nothing to read again yet


def test_a_failed_read_blocks_is_not_retried_before_the_ttl_and_check_again_forces_one(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed(ok=False, complete=False, code="AccessDeniedException")
    s.render()
    assert "Can't read the CDC dead-letter queue" in s.body()
    assert [n["tone"] for n in s.notices if n["header"] == "Can't read the CDC dead-letter queue"] == ["error"]
    assert "I've cut over to DSQL" not in s.ui.texts
    assert "Continue without this check" in s.ui.texts and "Check again" in s.ui.texts
    assert s.ui.timers == []  # no tight retry loop on a failed read
    s.click("Check again")
    assert len(s.ui.timers) == 1 and s.button("Check again").disabled
    s.next_read = s.record()
    s.run(s.ui.timers[0])
    assert s.reads == [1]
    # The TTL: an old read is re-read once on the next render.
    s.landed(ok=False, complete=False, code="AccessDeniedException", ago=timedelta(seconds=61))
    s.render()
    assert len(s.ui.timers) == 1


def test_continuing_without_the_check_is_logged_scoped_and_releases(monkeypatch):
    from dsql_migrator.core.activity_log import ActivityStatus

    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed(ok=False, complete=False, code="AccessDeniedException")
    s.render()
    s.click("Continue without this check")
    (waived,) = [kw for action, kw in s.logged if action == "CDC dead-letter check waived at cut over"]
    assert waived["status"] is ActivityStatus.WARNING
    assert "a readable dead-letter queue" in waived["detail"] and len(waived["detail"]) < 500
    assert s.vs.cutover_dlq_waiver.read_problem == "failed" and s.refreshes == [1]
    s.render()
    assert "I've cut over to DSQL" in s.ui.texts
    assert "Cutting over without the full CDC dead-letter check" in s.body()
    s.click("I've cut over to DSQL")
    (ack,) = s.acknowledged()
    assert "; CDC dead-letter check WAIVED (a readable dead-letter queue)" in ack["detail"]


@pytest.mark.parametrize("kind", ["blocked", "unverified", "acceptable"])
def test_the_release_gate_still_decides_first(monkeypatch, kind):
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    if kind == "blocked":
        s.validate(_item(matched=False, checksum=False), mode=ValidationMode.CHECKSUM)
        expect = "Get a clean validation before you cut over"
    elif kind == "unverified":
        s.validate(_item(checksum=None), reconcile_requested=True)
        expect = "Row counts match, but records were never reconciled"
    else:
        quarantined = TableValidationResult(
            table="app.t", source_row_count=15, target_row_count=12, row_count_match=False,
            checksum_match=False, matched=False, rows_quarantined=3,
            reconcile=ReconcileResult(pk_column="id", source_count=15, target_count=12,
                                      missing_on_target=3, consistent=False),
        )
        s.validate(quarantined, mode=ValidationMode.CHECKSUM)
        expect = "Every difference is explained — cut-over needs your sign-off"
    s.vs.cutover_dlq_waiver = _waiver({"app.t": 99}, read_problem="failed")
    s.render()
    assert expect in s.body()
    assert "CDC dead-letter check" not in s.ui.texts and s.ui.timers == []


def test_columns_checksum_cannot_compare_are_disclosed_on_a_released_gate(monkeypatch):
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(excluded=("doc",)), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    assert "CHECKSUM could not compare every column of the dead-lettered tables" in s.body()
    assert "app.t (doc)" in s.body() and "I've cut over to DSQL" in s.ui.texts


def test_the_acknowledgement_states_what_the_gate_rested_on(monkeypatch):
    s = _Cutover(monkeypatch)  # CDC evidence, no dead letter at all
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    s.click("I've cut over to DSQL")
    assert "; CDC dead letters: none recorded" in s.acknowledged()[-1]["detail"]

    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    s.click("I've cut over to DSQL")
    assert (
        "; CDC dead letters: 1 record(s) on 1 table(s), each cleared by a CHECKSUM match "
        "after its last one" in s.acknowledged()[-1]["detail"]
    )

    s = _Cutover(monkeypatch, cdc=False, mtype="FULL_LOAD_ONLY", ops_start=False)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.render()
    s.click("I've cut over to DSQL")
    assert "CDC dead" not in s.acknowledged()[-1]["detail"]


def test_a_dead_letter_folded_after_the_render_stops_the_acknowledgement(monkeypatch):
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    assert "I've cut over to DSQL" in s.ui.texts
    s.dead_letter("app.t", ago=timedelta(seconds=0), offset=2)  # after the comparison
    s.click("I've cut over to DSQL")
    assert s.acknowledged() == []
    assert s.ui.notes and s.ui.notes[-1][1] == "warning" and s.refreshes == [1]


def test_full_load_only_renders_exactly_as_before(monkeypatch):
    def _render(with_store):
        s = _Cutover(monkeypatch, cdc=False, mtype="FULL_LOAD_ONLY", with_store=with_store,
                     ops_start=False)
        s.validate(_item(), mode=ValidationMode.CHECKSUM)
        s.render()
        return s

    with_store, without = _render(True), _render(False)
    assert with_store.ui.texts == without.ui.texts
    assert [t for t, _c in with_store.ui.clicks] == [t for t, _c in without.ui.clicks]
    assert with_store.ui.timers == [] and without.ui.timers == []


def test_full_load_only_with_a_leftover_ledger_still_holds_the_tables(monkeypatch):
    s = _Cutover(monkeypatch, cdc=False, mtype="FULL_LOAD_ONLY")
    s.dead_letter("app.t")
    s.validate(_item(checksum=None))
    s.render()
    assert "Re-check 1 table(s) with CHECKSUM before cutting over" in s.body()
    assert "Reload these tables on the Full Load step with 'Drop & reload'." in s.body()
    assert s.ui.timers == []  # no dead-letter read without CDC


def test_cdc_only_has_its_own_way_forward_and_names_oversized_values(monkeypatch):
    s = _Cutover(monkeypatch, mtype="CDC_ONLY")
    s.dead_letter("app.t", code="OVERSIZED_VALUE")
    s.validate(_item(checksum=None))
    s.render()
    assert "CDC only has no Full Load step to reload from" in s.body()
    assert "a value over Aurora DSQL's per-value limit" in s.body()
    assert "exclude that column (Exclude tables & columns on the Data Migration step)" in s.body()


# ---------------------------------------------------------------------------
# A read cancelled by a refresh still lands (the timer is not the read's owner)
# ---------------------------------------------------------------------------


def _real_timer_ui():
    """A ui double whose ``timer()`` drives NiceGUI's REAL base Timer (``_run_once`` ->
    ``_invoke_callback``); ``clear()`` is exactly what a refresh does to the timers it drew
    (``Timer._handle_delete`` -> ``cancel(with_current_invocation=True)``)."""
    from nicegui.timer import Timer as _BaseTimer

    class _Timer(_BaseTimer):
        def _skip_registration(self) -> bool:  # driven by hand, no app running
            return True

        def _should_stop(self) -> bool:
            return self._is_canceled

    class _Ui:
        def __init__(self):
            self.live: list = []
            self.tasks: list = []
            self.armed = 0

        def timer(self, interval, callback, once=False):
            timer = _Timer(interval, callback, once=once)
            self.armed += 1
            self.live.append(timer)
            self.tasks.append(asyncio.get_running_loop().create_task(timer._run_once()))
            return timer

        def clear(self):
            for timer in self.live:
                timer.cancel(with_current_invocation=True)
            self.live = []

    return _Ui()


class _SlowLogs:
    def __init__(self, events, delay):
        self.events, self.delay, self.calls = list(events), delay, 0

    def filter_log_events(self, **kw):
        import time as _time

        self.calls += 1
        _time.sleep(self.delay)
        return {"events": [e for e in self.events if e["timestamp"] >= kw["startTime"]]}


def test_a_refresh_during_the_background_read_cannot_drop_its_dead_letters(monkeypatch):
    """The read consumed its records from the SHARED controller (cursor + seen ids); a
    refresh cancelled the timer and run.io_bound returned None, so they never reached the
    ledger -- and no later read could see them again: the gate failed open."""
    import time as _time

    from nicegui import run as real_run

    import dsql_migrator.core.activity_log as act
    from dsql_migrator.core.msk_connect_controller import MskConnectController
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_gate_for, schedule_cdc_dlq_check
    from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType

    monkeypatch.setattr(act, "log_activity", lambda *a, **k: None)
    fake = types.ModuleType("nicegui")
    fake.run = real_run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    ms = DataMigrationStore().get_or_create("s1")
    ms.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    ms.set_cdc_ops_window_start(_now() - timedelta(hours=2))
    logs = _SlowLogs(
        [{"eventId": "e1", "timestamp": int(_time.time() * 1000) - 120_000,
          "message": "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, "
                     "offset=7): apply failed (sqlstate=42804)"}],
        delay=0.3,
    )
    ctrl = MskConnectController("us-east-1")
    ctrl._client = lambda _svc: logs  # type: ignore[assignment]
    ms.set_cdc_controller(ctrl, ("us-east-1", None))
    session = SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
    ui = _real_timer_ui()

    def render():
        schedule_cdc_dlq_check(ui, session, ms, refresh, evidence_at=None)

    def refresh():
        ui.clear()
        render()

    async def scenario():
        render()  # arms the one-shot read
        await asyncio.sleep(0.15)  # fired at 0.05 s: the read is on the worker now
        assert ms.cdc_dlq_check_running
        refresh()  # e.g. the 0.5 s re-check poll or the DEFAULT fill lands first
        for _ in range(60):
            await asyncio.sleep(0.05)
            if ms.cdc_dlq_last_read is not None and not ms.cdc_dlq_check_running:
                break
        await asyncio.gather(*ui.tasks, return_exceptions=True)

    asyncio.run(scenario())
    assert "shop.orders" in ms.cdc_dlq_ledger  # landed despite the cancelled timer
    assert ms.cdc_dlq_last_read is not None and ms.cdc_dlq_last_read.ok
    assert logs.calls == 1  # single flight: the re-render armed no second read
    vs = SimpleNamespace(
        result=ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[
            TableValidationResult(table="shop.orders", source_row_count=1, target_row_count=1,
                                  row_count_match=True, matched=True)]),
        compared_run_start=_now() - timedelta(minutes=10), compared_overrides={},
        completed_at=_now() - timedelta(minutes=5), rechecked_at=None,
        cutover_dlq_waiver=None,
    )
    gate = cdc_dlq_gate_for(ms, vs, cdc_in_use=True, release="clean", summary=None)
    assert gate.held and [i.reason for i in gate.issues] == ["row-count-only"]
    # ...and the landed read counts toward the TTL: a render now arms nothing.
    armed = ui.armed

    async def again():
        render()

    asyncio.run(again())
    assert ui.armed == armed


# ---------------------------------------------------------------------------
# Copy says what the gate will actually do
# ---------------------------------------------------------------------------


def test_a_failed_read_waiver_does_not_promise_to_relock(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed(ok=False, complete=False, code="AccessDeniedException")
    s.render()
    s.click("Continue without this check")
    (detail,) = [kw["detail"] for a, kw in s.logged if a == "CDC dead-letter check waived at cut over"]
    assert "re-opens if another arrives" not in detail
    assert "dead letters that arrive while it stays unreadable will not re-open the check" in detail
    s.render()
    (body,) = [n["body"] for n in s.notices
               if n["header"] == "Cutting over without the full CDC dead-letter check"]
    assert "can't see a change CDC sets aside" in body and "locks again" not in body
    assert "for these tables" not in body


def test_a_waiver_the_tool_can_still_see_past_keeps_its_relock_promise():
    from dsql_migrator.ui.cdc_dlq_gate import waived_notice, waiver_log_detail

    incomplete = _gate(last_read=_read(complete=False), waiver=_waiver(read_problem="incomplete"))
    tables = _gate(report=_report(_item("app.other")), waiver=_waiver({"app.t": 1}))
    for gate in (incomplete, tables):
        assert gate.waived
        _header, body = waived_notice(gate)
        assert "this step locks again if a later read finds another change" in body
        assert "for these tables" not in body
    assert waiver_log_detail(tables).endswith("the check re-opens if another arrives")


def test_a_failed_read_waiver_never_relocks_while_reads_keep_failing():
    """Documents the limit the copy now states: no read, no newer dead letter to see."""
    failed = _read(ok=False, complete=False, code="AccessDeniedException")
    gate = _gate(ledger={}, last_read=failed, waiver=_waiver(read_problem="failed"))
    assert gate.waived and not gate.held


@pytest.mark.parametrize(
    "reasons_kwargs,expected,unexpected",
    [
        ({"run_start": T_DL - timedelta(minutes=10)},
         "last CHECKSUM comparison started before (or within a minute of)",
         "row-count match cannot reveal"),
        ({"run_start": None}, "was not kept across the restore", "row-count match cannot reveal"),
        ({"report": _report(_item(checksum=None), mode=ValidationMode.ROW_COUNT)},
         "a row-count match cannot reveal a missed UPDATE", "started before"),
    ],
)
def test_the_validation_hold_names_the_real_cause(reasons_kwargs, expected, unexpected):
    from dsql_migrator.ui.cdc_dlq_gate import verdict_hold

    header, body = verdict_hold(_gate(**reasons_kwargs), total=1, as_of="now")
    assert header == "Tables match — cut over waits for a CHECKSUM re-check of 1 table(s)"
    assert expected in body and unexpected not in body


@pytest.mark.parametrize(
    "read",
    [
        _read(ok=False, complete=False, code="ThrottlingException"),
        _read(complete=False),
    ],
)
def test_a_read_problem_offers_the_waiver_only_after_the_recheck(read):
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    must = _gate(report=_report(_item(checksum=None)), last_read=read)
    assert not must.can_waive
    body = read_state_notice(must)[2]
    assert "the tables below are re-checked" in body
    assert "If you have confirmed the stream another way, you can continue" not in body
    assert "you can continue without this check; that" not in body
    free = _gate(last_read=read)
    assert free.can_waive and "the tables below are re-checked" not in read_state_notice(free)[2]


def test_a_partial_history_offers_the_waiver_only_after_the_recheck():
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    late = _ms(T_RUN + timedelta(minutes=1))
    must = _gate(history_from=late, report=_report(_item(checksum=None)))
    assert must.read_state == "partial-history" and not must.can_waive
    body = read_state_notice(must)[2]
    assert "Or, once the tables below are re-checked, continue without this check" in body
    assert "Or continue without this check" not in body
    assert "Or continue without this check" in read_state_notice(_gate(history_from=late))[2]


def test_the_pending_notice_promises_the_runbook_only_with_nothing_else_open():
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    with_tables = _gate(report=_report(_item(checksum=None)), last_read=None)
    assert "the runbook appears when it finishes" not in read_state_notice(with_tables)[2]
    assert "this check updates when it finishes" in read_state_notice(with_tables)[2]
    nothing = _gate(last_read=None)
    assert "the runbook appears when it finishes" in read_state_notice(nothing)[2]


def test_a_partial_history_is_a_warning_and_a_broken_read_stays_an_error():
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    late = _ms(T_RUN + timedelta(minutes=1))
    assert read_state_notice(_gate(history_from=late))[0] == "warning"
    assert read_state_notice(_gate(cdc_start=None, history_from=late))[0] == "warning"
    assert read_state_notice(_gate(last_read=_read(complete=False)))[0] == "error"
    assert read_state_notice(_gate(last_read=_read(ok=False, complete=False)))[0] == "error"


def test_the_incomplete_notice_says_each_read_continues():
    from dsql_migrator.ui.cdc_dlq_gate import read_state_notice

    body = read_state_notice(_gate(last_read=_read(complete=False)))[2]
    assert "each read continues where the last one stopped" in body


def test_a_table_outside_the_report_is_cleared_by_a_run_not_a_recheck():
    from dsql_migrator.ui.cdc_dlq_gate import issue_notice, way_forward_lines

    gate = _gate(report=_report(_item("app.other")))
    assert [i.reason for i in gate.issues] == ["not-validated"]
    body = issue_notice(gate)[2]
    assert "re-run Validation" in body and "re-check them" not in body
    steps = way_forward_lines(gate)
    assert not any(step.startswith("Re-check these tables") for step in steps)
    assert any("re-run Validation" in step for step in steps)


def test_an_accepted_gap_table_is_not_promised_a_clearing_recheck():
    from dsql_migrator.ui.cdc_dlq_gate import issue_notice, way_forward_lines

    gate = _gate(
        report=_report(_item(matched=False, checksum=False)), release="accepted",
        explained=("app.t",),
    )
    assert [i.reason for i in gate.issues] == ["accepted-gap"]
    assert "unless its source rows are corrected" in issue_notice(gate)[2]
    last = way_forward_lines(gate)[-1]
    assert "clears by itself once each matches" not in last
    assert "cannot match until its source rows are corrected" in last


def test_a_table_to_recheck_keeps_the_recheck_step():
    from dsql_migrator.ui.cdc_dlq_gate import way_forward_lines

    steps = way_forward_lines(_gate(report=_report(_item(checksum=None))))
    assert steps[-1].startswith("Re-check these tables on the Validation step")


def test_the_repair_order_matches_the_fix_dialog():
    """Evaluation, then Stop CDC: the Fix-target-schema dialog's "in this order" runbook."""
    from dsql_migrator.ui.cdc_dlq_gate import way_forward_lines
    from dsql_migrator.ui.data_migration import MigrationType

    gate = dataclasses.replace(
        _gate(report=_report(_item(checksum=None))),
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    (line,) = [step for step in way_forward_lines(gate) if "Drop & reload" in step]
    assert (
        line.index("Evaluation") < line.index("Stop CDC")
        < line.index("Drop & reload") < line.index("Start CDC")
    )


def test_the_runbook_says_cut_over_rereads_and_when(monkeypatch):
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    assert "Cut over re-reads the CDC dead-letter queue before this runbook opens" in s.body()
    assert "at most once a minute" in s.body()
    assert "this step re-reads" not in s.body()


def test_issue_lines_are_capped_at_twenty_with_a_count_tail():
    from dsql_migrator.ui.cdc_dlq_gate import issue_lines

    names = [f"app.t{i:02d}" for i in range(25)]
    gate = _gate(
        ledger={n: _entry() for n in names},
        report=_report(*(_item(n, checksum=None) for n in names), mode=ValidationMode.ROW_COUNT),
    )
    lines = issue_lines(gate)
    assert len(gate.issues) == 25 and len(lines) == 21
    assert lines[-1] == "and 5 more table(s)"


# ---------------------------------------------------------------------------
# Validation's state-only view of an unconfirmed history
# ---------------------------------------------------------------------------


def _validation_gate(**kw):
    return _gate(check_reads=False, last_read=None, **kw)


def test_an_attached_pipeline_holds_validation_until_one_full_checksum_run():
    from dsql_migrator.ui.cdc_dlq_gate import held_log_clause, history_options_hint, verdict_hold

    rows = _report(_item(checksum=None), mode=ValidationMode.ROW_COUNT)
    gate = _validation_gate(ledger={}, report=rows, cdc_start=None, history_from=None)
    assert gate.history_unconfirmed and not gate.blocks  # Cut over decides with its own read
    header, body = verdict_hold(gate, total=1, as_of="now")
    assert header == "Tables match — cut over waits for one CHECKSUM run of every table"
    assert "attached while already running" in body and "Fast sweep off" in body
    assert "the CDC dead-letter history can't be confirmed" in held_log_clause(gate)
    assert "Cut over also needs one CHECKSUM run of every table" in history_options_hint(gate)
    # One full CHECKSUM run started after the history began clears it (once a read
    # has said where that history begins).
    history = T_DL
    cleared = _validation_gate(
        ledger={}, report=_report(_item()), cdc_start=None, history_from=_ms(history),
        run_start=history + timedelta(seconds=61),
    )
    assert not cleared.history_unconfirmed
    assert verdict_hold(cleared, total=1, as_of="now") is None
    assert held_log_clause(cleared) is None and history_options_hint(cleared) == ""


def test_a_count_that_starts_after_the_cdc_start_holds_validation_too():
    from dsql_migrator.ui.cdc_dlq_gate import verdict_hold

    gate = _validation_gate(
        ledger={}, report=_report(_item(checksum=None), mode=ValidationMode.ROW_COUNT),
        history_from=_ms(T_OPS + timedelta(hours=1)),
    )
    assert gate.history_unconfirmed
    assert "starts at 2026-09-01 11:00 UTC" in verdict_hold(gate, total=1, as_of="now")[1]


@pytest.mark.parametrize(
    "kw",
    [
        {"history_from": _ms(T_OPS - timedelta(minutes=5))},  # reaches the CDC start
        {"history_from": None},  # known start, no read yet: that read proves it
        {"cdc_in_use": False},  # Full Load only
    ],
)
def test_a_confirmable_history_leaves_validation_green(kw):
    from dsql_migrator.ui.cdc_dlq_gate import held_log_clause, verdict_hold

    gate = _validation_gate(
        ledger={}, report=_report(_item(checksum=None), mode=ValidationMode.ROW_COUNT), **kw
    )
    assert not gate.history_unconfirmed
    assert verdict_hold(gate, total=1, as_of="now") is None and held_log_clause(gate) is None


# ---------------------------------------------------------------------------
# The scheduler's guards
# ---------------------------------------------------------------------------


def test_a_fresh_read_that_started_before_the_latest_validation_is_read_again(monkeypatch):
    s = _Cutover(monkeypatch)
    s.landed(ago=timedelta(seconds=10))  # well inside the TTL ...
    s.validate(_item(), mode=ValidationMode.CHECKSUM, started_ago=timedelta(seconds=8),
               finished_ago=timedelta(seconds=5))  # ... but before the latest validation
    s.render()
    assert "Checking the CDC dead-letter queue" in s.body()
    assert len(s.ui.timers) == 1
    s.next_read = s.record()
    s.run(s.ui.timers[0])
    assert s.reads == [1]
    s.render()
    assert "I've cut over to DSQL" in s.ui.texts


def _two_armed(s):
    s.render()
    first = list(s.ui.timers)
    s.render()  # nothing landed yet: this render arms its own timer
    second = list(s.ui.timers)
    assert len(first) == 1 and len(second) == 1
    return first[0], second[0]


def test_two_timers_fired_back_to_back_read_once(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    a, b = _two_armed(s)
    s.next_read = s.record()
    s.run(a)
    s.run(b)
    assert s.reads == [1]


def test_two_overlapping_timers_read_once(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    a, b = _two_armed(s)
    s.next_read = s.record()

    class _YieldingRun:
        @staticmethod
        async def io_bound(fn, *args, **kw):
            await asyncio.sleep(0)  # a real worker-thread read yields the loop
            return fn(*args, **kw)

    monkeypatch.setattr(sys.modules["nicegui"], "run", _YieldingRun)

    async def _both():
        await asyncio.gather(a(), b())

    asyncio.run(_both())
    assert s.reads == [1]


def test_a_render_while_a_read_is_in_flight_arms_no_timer(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    assert s.ms.begin_cdc_dlq_check()
    s.render()
    assert "Checking the CDC dead-letter queue" in s.body()
    assert s.ui.timers == []


def test_a_pending_gate_is_never_waivable_even_with_an_unprovable_table(monkeypatch):
    gate = _gate(report=_report(_item("app.other")), last_read=None)
    assert gate.pending and gate.waivable_issues and not gate.can_waive
    s = _Cutover(monkeypatch)
    s.dead_letter("app.t")
    s.validate(_item("app.other"), mode=ValidationMode.CHECKSUM)
    s.render()
    assert "Checking the CDC dead-letter queue" in s.body()
    assert "Continue without this check" not in s.ui.texts


# ---------------------------------------------------------------------------
# Click-time guards
# ---------------------------------------------------------------------------


def test_a_waive_click_after_the_state_moved_records_nothing(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed(ok=False, complete=False, code="AccessDeniedException")
    s.render()
    assert "Continue without this check" in s.ui.texts
    s.dead_letter("app.t", ago=timedelta(seconds=0), offset=2)  # now a table to re-check
    s.click("Continue without this check")
    assert s.vs.cutover_dlq_waiver is None
    assert not [a for a, _kw in s.logged if a == "CDC dead-letter check waived at cut over"]
    assert s.refreshes == [1]


def test_acknowledging_while_a_check_runs_waits_for_it(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    s.render()
    assert "I've cut over to DSQL" in s.ui.texts
    assert s.ms.begin_cdc_dlq_check() is True
    s.click("I've cut over to DSQL")
    assert s.acknowledged() == []
    assert s.ui.notes and s.ui.notes[-1][1] == "info"


def test_check_again_is_disabled_while_a_check_runs(monkeypatch):
    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed(ok=False, complete=False, code="AccessDeniedException")
    assert s.ms.begin_cdc_dlq_check() is True
    s.render()
    assert s.button("Check again").disabled
