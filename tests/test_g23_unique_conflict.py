"""G23: a 23505 names both causes; a CHECKSUM-verified table reads as verified on the CDC step.

The CDC drift banner showed the red "Stop CDC and reload these tables" notice for a table
Validation had matched by CHECKSUM in a comparison started after its last dead letter (after a
reload, or a later full-row change of each row) -- the exact condition the cut-over gate's
dead-letter check uses to stop holding it. The banner now reuses that rule
(``cdc_dlq_proven_tables``) and shows such a table's target-side groups in one "Verified by
CHECKSUM" notice, keeping the error for every table not yet resolved (G25: the notice speaks
for the dead-letter check only, and names an open source-schema / REPLICA IDENTITY finding that
still holds a table at Cut over).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import CdcDlqLedgerEntry, SchemaDriftSummary

T0 = datetime(2026, 10, 9, 1, 0, tzinfo=timezone.utc)


def _item(table, *, matched=True, checksum=True, skipped=False, error=None):
    return SimpleNamespace(
        table=table, matched=matched, checksum_match=checksum,
        deep_checks_skipped=skipped, error=error,
    )


def _validation(items, *, run_start=None, overrides=None):
    return SimpleNamespace(
        result=SimpleNamespace(items=list(items)),
        compared_run_start=run_start,
        compared_overrides=dict(overrides or {}),
        cutover_dlq_waiver=None,
        completed_at=None,
        rechecked_at=None,
    )


def _state(ledger, validation=None, *, provider=None):
    return SimpleNamespace(
        cdc_dlq_ledger=ledger,
        validation_state_provider=provider or (lambda: validation),
        migration_type=None,
        lob_exclusions=lambda: {},
    )


_LAST = T0
_CASES = {
    # name: (item kwargs, run_start, overrides, expected proven)
    "matched after the margin": ({}, T0 + timedelta(minutes=5), None, True),
    "inside the margin": ({}, T0 + timedelta(seconds=30), None, False),
    "row-count only": ({"checksum": None}, T0 + timedelta(minutes=5), None, False),
    "deep checks skipped": ({"skipped": True}, T0 + timedelta(minutes=5), None, False),
    "compare time unknown": ({}, None, None, False),
    "not matched": ({"matched": False}, T0 + timedelta(minutes=5), None, False),
    "override wins over the run start": (
        {}, T0 - timedelta(hours=1), {"app.customers": T0 + timedelta(minutes=5)}, True,
    ),
    "override before the last dead letter": (
        {}, T0 + timedelta(hours=1), {"app.customers": T0 - timedelta(minutes=5)}, False,
    ),
}


@pytest.mark.parametrize("name", sorted(_CASES))
def test_the_proven_tables_are_exactly_the_cut_over_gates_resolved_tables(name) -> None:
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_cutover_gate, cdc_dlq_proven_tables

    kwargs, run_start, overrides, expected = _CASES[name]
    ledger = {"app.customers": CdcDlqLedgerEntry(count=3, last_at=_LAST, kinds=["unique-conflict"])}
    validation = _validation(
        [_item("app.customers", **kwargs)], run_start=run_start, overrides=overrides
    )
    proven = cdc_dlq_proven_tables(_state(ledger), validation)
    gate = cdc_dlq_cutover_gate(
        ledger=ledger, cdc_in_use=True, cdc_evidence=True, check_reads=False,
        report=validation.result, release="released",
        compared_run_start=run_start, compared_overrides=overrides,
    )
    assert ("app.customers" in proven) is expected, name
    assert set(proven) == set(gate.resolved), name
    if expected:
        assert proven["app.customers"] == (overrides or {}).get("app.customers", run_start)


def test_the_proven_tables_need_a_validation_state_and_reopen_on_a_newer_dead_letter() -> None:
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_proven_tables

    ledger = {"app.customers": CdcDlqLedgerEntry(count=1, last_at=_LAST)}
    assert cdc_dlq_proven_tables(_state(ledger), None) == {}
    assert cdc_dlq_proven_tables(None, _validation([])) == {}
    validation = _validation([_item("app.customers")], run_start=T0 + timedelta(minutes=5))
    assert set(cdc_dlq_proven_tables(_state(ledger), validation)) == {"app.customers"}
    newer = {"app.customers": CdcDlqLedgerEntry(count=2, last_at=T0 + timedelta(minutes=10))}
    assert cdc_dlq_proven_tables(_state(newer), validation) == {}


# --- The banner ----------------------------------------------------------------------------------


class _El:
    def classes(self, *a, **k):
        return self

    def props(self, *a, **k):
        return self

    def tooltip(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Ui:
    """Records every label text, and each notice box's border class (its tone)."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.classes_seen: list[str] = []

    def _el(self):
        ui = self

        class _Tracked(_El):
            def classes(self, *a, **k):
                ui.classes_seen.extend(str(x) for x in a)
                return self

        return _Tracked()

    def column(self, *a, **k):
        return self._el()

    def row(self, *a, **k):
        return self._el()

    def label(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def icon(self, *a, **k):
        return self._el()

    def button(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def space(self, *a, **k):
        return self._el()

    def markdown(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def html(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()


def _render(groups, migration_state):
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    ui = _Ui()
    view = SimpleNamespace(schema_drift=[SchemaDriftSummary(**g) for g in groups])
    cm._render_cdc_schema_drift_banner(ui, view, migration_state=migration_state)
    return " ".join(ui.texts), ui


_MATCHED = T0 + timedelta(minutes=5)


def _ledger(*tables, last_at=_LAST):
    return {t: CdcDlqLedgerEntry(count=1, last_at=last_at, kinds=["unique-conflict"]) for t in tables}


def test_a_reloaded_and_verified_table_reads_as_resolved_not_as_an_error() -> None:
    from dsql_migrator.ui.design import NOTICE_STYLE

    state = _state(
        _ledger("app.customers"),
        _validation([_item("app.customers")], run_start=_MATCHED),
    )
    blob, ui = _render(
        [{"table": "app.customers", "kind": "unique-conflict", "count": 3}], state
    )
    assert "Verified by CHECKSUM" in blob
    assert (
        "app.customers matched by CHECKSUM in a Validation comparison started "
        "2026-10-09 01:05 UTC, after its last dead-lettered record" in blob
    )
    assert "Cut over's dead-letter check no longer holds these tables" in blob
    assert "Stop CDC and reload these tables" not in blob
    assert "Rows may be missing from the target" not in blob
    assert NOTICE_STYLE["success"][1] in " ".join(ui.classes_seen)
    assert NOTICE_STYLE["error"][1] not in " ".join(ui.classes_seen)


def test_without_the_fix_state_the_error_stays() -> None:
    # No validation (or a comparison that does not prove it): today's error, unchanged.
    for validation in (None, _validation([_item("app.customers")], run_start=None)):
        state = _state(_ledger("app.customers"), validation)
        blob, _ = _render(
            [{"table": "app.customers", "kind": "unique-conflict", "count": 3}], state
        )
        assert "Stop CDC and reload these tables" in blob
        assert "Verified by CHECKSUM" not in blob


def test_mixed_the_error_names_only_the_open_table() -> None:
    state = _state(
        _ledger("app.customers", "app.orders"),
        _validation(
            [_item("app.customers"), _item("app.orders", checksum=None)], run_start=_MATCHED
        ),
    )
    blob, _ = _render(
        [
            {"table": "app.customers", "kind": "unique-conflict", "count": 3},
            {"table": "app.orders", "kind": "unique-conflict", "count": 1},
        ],
        state,
    )
    assert "Stop CDC and reload these tables" in blob
    assert "so app.orders will not catch up on its own" in blob
    assert "app.customers will not catch up" not in blob
    assert "app.customers: a unique constraint rejected the row" not in blob
    assert "app.orders: a unique constraint rejected the row" in blob
    assert "Verified by CHECKSUM" in blob and "app.customers matched by CHECKSUM" in blob


def test_a_newer_dead_letter_brings_the_error_back() -> None:
    validation = _validation([_item("app.customers")], run_start=_MATCHED)
    state = _state(_ledger("app.customers", last_at=_MATCHED + timedelta(minutes=1)), validation)
    blob, _ = _render([{"table": "app.customers", "kind": "unique-conflict", "count": 4}], state)
    assert "Stop CDC and reload these tables" in blob
    assert "Verified by CHECKSUM" not in blob


def test_a_provider_that_raises_leaves_the_banner_unchanged() -> None:
    def broken():
        raise RuntimeError("no validation state")

    state = _state(_ledger("app.customers"), provider=broken)
    blob, _ = _render([{"table": "app.customers", "kind": "unique-conflict", "count": 3}], state)
    assert "Stop CDC and reload these tables" in blob
    assert "Verified by CHECKSUM" not in blob


def test_a_source_side_group_of_a_resolved_table_stays_open() -> None:
    # Only the target-side (reload) groups resolve: a column the target lacks still needs DDL.
    state = _state(
        _ledger("db.customers"),
        _validation([_item("db.customers")], run_start=_MATCHED),
    )
    blob, _ = _render(
        [
            {"table": "db.customers", "kind": "unique-conflict", "count": 3},
            {"table": "db.customers", "kind": "add-column", "count": 1},
        ],
        state,
    )
    assert "Source schema change detected" in blob
    assert "Stop CDC and reload these tables" not in blob
    assert "Verified by CHECKSUM" in blob


# --- The 23505 copy (Part C) -------------------------------------------------------------------


def test_the_unique_conflict_copy_names_both_causes_and_a_remedy() -> None:
    from dsql_migrator.ui.data_migration._cdc_monitoring import _DRIFT_LABELS

    label, why = _DRIFT_LABELS["unique-conflict"]
    assert label == "a unique constraint rejected the row"
    for phrase in (
        "UPDATE of the primary key",
        "a unique value passed from one row to another",
        "up to 5 minutes",
        "Delete and Deploy CDC infrastructure",
        "Composite key",
        "REPLICA IDENTITY below FULL",
        "reload the table as below and validate it",
    ):
        assert phrase in why, phrase
    assert "{" not in why  # fixed text, no value


def test_the_drift_fold_names_the_unique_index_a_23505_reports(monkeypatch) -> None:
    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.ui.data_migration import _cdc_status

    reason = (
        "sqlstate=23505 duplicate key value violates unique constraint \"uk_e2e567_email\" "
        "[constraint: uk_e2e567_email] | pk: id=100376 | op: c/r | sql: INSERT INTO x"
    )
    records = [
        CdcConnectorError(
            table="migration_schema.e2e567_customers", message=reason, error_code="23505"
        ),
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda state, key: records)
    (group,) = _cdc_status.cdc_schema_drift_summary(SimpleNamespace(), "k")
    assert group.kind == "unique-conflict"
    assert group.columns == ["uk_e2e567_email"]
    blob, _ = _render(
        [{"table": group.table, "kind": group.kind, "count": 1, "columns": group.columns}],
        _state({}),
    )
    assert (
        "migration_schema.e2e567_customers: a unique constraint rejected the row "
        "(uk_e2e567_email) — " in blob
    )
