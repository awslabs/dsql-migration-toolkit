"""G25 round 4 (CDC UI): a CHECKSUM-proven table an open finding holds is verified for its
dead letters.

f[5]: a table whose dead letters a CHECKSUM proved repaired (a comparison started after its last
dead letter matched it) was shown as "Rows may be missing from the target" with "Stop CDC and
reload these tables ... will not catch up on its own" whenever an unrelated REPLICA IDENTITY or
source schema finding was open for it -- prescribing a Drop & reload the CHECKSUM shows is not
needed, and that fixes neither finding. The AI DBA's facts and get_cdc_status left the table out
of the verified tables the same way. Now the dead-letter repair is proven whatever else holds
the table: its target-side groups move into the success "Verified by CHECKSUM" notice, which
gains one sentence naming the open finding that still holds Cut over; the reload notice stays
for the tables whose dead letters are not proven.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from dsql_migrator.core.models import CdcDlqLedgerEntry
from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_cdc_dlq import _render_drift
from tests.test_g23_unique_conflict import _item, _ledger, _state, _validation
from tests.test_g25_cdc_ui import (
    _MATCHED,
    _OPEN_CATALOG,
    _OPEN_RI,
    T0,
    _ask,
    _drift,
    _executor,
)

_ORDERS_23505 = {"table": "app.orders", "kind": "unique-conflict", "count": 2}
_PROVEN_CLAUSE = (
    "app.orders matched by CHECKSUM in a Validation comparison started 2026-10-09 01:05 UTC, "
    "after its last dead-lettered record"
)
_STILL_HELD = {
    "identity": "Cut over still holds app.orders for its open REPLICA IDENTITY finding (see "
    "that notice above).",
    "catalog": "Cut over still holds app.orders for its open source schema finding (see that "
    "notice above).",
}
# What only an UNPROVEN target-side table may be shown with.
_RELOAD = ("Rows may be missing from the target", "Stop CDC and reload these tables",
           "Stop CDC and repair these tables", "will not catch up on its own")


def _held_state(*, identity=None, catalog=None, ledger=None, items=None):
    state = _state(
        ledger or _ledger("app.orders"),
        _validation(items or [_item("app.orders")], run_start=_MATCHED),
    )
    state.cdc_replica_identity_findings = dict(identity or {})
    state.cdc_catalog_drift_findings = dict(catalog or {})
    return state


def _notices(monkeypatch):
    """Record every render_notice call of the banner as (tone, header, body), and still draw
    it (so the rendered text is checked too)."""
    seen: list = []
    real = cm.render_notice

    def _record(ui, *, tone, header, body="", **kw):
        seen.append((tone, header, body))
        return real(ui, tone=tone, header=header, body=body, **kw)

    monkeypatch.setattr(cm, "render_notice", _record)
    return seen


@pytest.mark.parametrize("which", ["identity", "catalog"])
@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_a_proven_table_an_open_finding_holds_is_verified_for_its_dead_letters(
    monkeypatch, which, mtype
) -> None:
    findings = {"identity": {"app.orders": _OPEN_RI}, "catalog": {"app.orders": _OPEN_CATALOG}}
    seen = _notices(monkeypatch)
    blob, _ = _render_drift(
        [_ORDERS_23505], mtype, migration_state=_held_state(**{which: findings[which]})
    )
    # One success notice, for its dead letters, naming the check that still holds Cut over.
    assert [(tone, header) for tone, header, _body in seen] == [
        ("success", "Verified by CHECKSUM")
    ]
    body = seen[0][2]
    assert body.startswith(_PROVEN_CLAUSE + ". Cut over's dead-letter check no longer holds")
    assert body.endswith(" " + _STILL_HELD[which])
    assert body.count("Cut over still holds") == 1
    # Never the reload the CHECKSUM shows is not needed.
    for absent in _RELOAD:
        assert absent not in blob, (which, mtype, absent)
    assert _STILL_HELD[which] in blob


def test_a_proven_table_with_no_open_finding_names_no_other_check(monkeypatch) -> None:
    closed = _OPEN_RI.model_copy(
        update={"restored_at": T0 + timedelta(minutes=1), "restored_by": "full"}
    )
    seen = _notices(monkeypatch)
    for state in (_held_state(), _held_state(identity={"app.orders": closed})):
        seen.clear()
        _render_drift([_ORDERS_23505], migration_state=state)
        ((tone, header, body),) = seen
        assert (tone, header) == ("success", "Verified by CHECKSUM")
        assert "Cut over still holds" not in body
        assert body.endswith("a new dead letter reopens this.")


def test_both_open_findings_are_named_in_one_sentence(monkeypatch) -> None:
    seen = _notices(monkeypatch)
    _render_drift(
        [_ORDERS_23505],
        migration_state=_held_state(
            identity={"app.orders": _OPEN_RI}, catalog={"app.orders": _OPEN_CATALOG}
        ),
    )
    ((_tone, _header, body),) = seen
    assert body.endswith(
        " Cut over still holds app.orders for its open REPLICA IDENTITY and source schema "
        "findings (see those notices above)."
    )


def test_the_reload_notice_names_only_the_table_whose_dead_letters_are_not_proven(
    monkeypatch,
) -> None:
    # app.orders: proven, held by an open REPLICA IDENTITY finding. app.lines: its last dead
    # letter came after the comparison started, so its rows may still be missing.
    ledger = {
        **_ledger("app.orders"),
        "app.lines": CdcDlqLedgerEntry(
            count=1, last_at=_MATCHED + timedelta(minutes=1), kinds=["unique-conflict"]
        ),
    }
    state = _held_state(
        identity={"app.orders": _OPEN_RI}, ledger=ledger,
        items=[_item("app.orders"), _item("app.lines")],
    )
    seen = _notices(monkeypatch)
    blob, _ = _render_drift(
        [_ORDERS_23505, {"table": "app.lines", "kind": "unique-conflict", "count": 1}],
        MigrationType.FULL_LOAD_AND_CDC, migration_state=state,
    )
    assert "Rows may be missing from the target" in blob
    stop = {header: body for _tone, header, body in seen}
    assert "so app.lines will not catch up on its own" in stop["Stop CDC and reload these tables"]
    assert "app.orders" not in stop["Stop CDC and reload these tables"]
    assert "app.lines: a unique constraint rejected the row" in blob
    assert "app.orders: a unique constraint rejected the row" not in blob
    assert stop["Verified by CHECKSUM"].startswith(_PROVEN_CLAUSE)
    assert stop["Verified by CHECKSUM"].endswith(" " + _STILL_HELD["identity"])
    assert [tone for tone, _h, _b in seen] == ["error", "success"]


def test_the_banners_ai_dba_facts_tell_the_same_story() -> None:
    # A proven 23505 table held by an open REPLICA IDENTITY finding, beside an open TRUNCATE
    # group of another table (so the banner, and its Ask AI DBA, are drawn).
    ledger = {
        **_ledger("app.orders"),
        "app.events": CdcDlqLedgerEntry(
            count=1, last_at=_MATCHED + timedelta(minutes=20), kinds=["source-truncate"]
        ),
    }
    state = _held_state(
        identity={"app.orders": _OPEN_RI}, ledger=ledger,
        items=[_item("app.orders"), _item("app.events", matched=False)],
    )
    blob, facts, seed, _tips = _ask(
        [_ORDERS_23505, {"table": "app.events", "kind": "source-truncate", "count": 1}],
        migration_type=MigrationType.FULL_LOAD_AND_CDC, migration_state=state,
    )
    assert "Verified by CHECKSUM" in blob and "Rows may be missing from the target" not in blob
    assert seed == cm._CDC_TRUNCATE_SEED
    assert "- app.orders: unique-conflict" not in facts
    assert (
        cm._VERIFIED_FACTS_LEAD + _PROVEN_CLAUSE + ". Cut over still holds app.orders for its "
        "open REPLICA IDENTITY finding."
    ) in facts


def test_the_dlq_cards_ai_facts_name_a_held_proven_table_as_verified(monkeypatch) -> None:
    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.ui.data_migration import _cdc_status

    records = [CdcConnectorError(table="app.orders", message="sqlstate=23505 dup",
                                 error_code="23505")]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda *_a: records)
    state = _held_state(catalog={"app.orders": _OPEN_CATALOG})
    state.cdc_activity = None
    facts = cm._cdc_dlq_facts(state, "k")
    assert (
        cm._VERIFIED_FACTS_LEAD + _PROVEN_CLAUSE + ". Cut over still holds app.orders for its "
        "open source schema finding."
    ) in facts


def test_get_cdc_status_lists_a_held_proven_table_with_what_still_holds_it(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_status
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    state.cdc_dlq_ledger = {
        "app.customers": CdcDlqLedgerEntry(count=3, last_at=T0, kinds=["unique-conflict"]),
        "app.held": CdcDlqLedgerEntry(count=1, last_at=T0, kinds=["unique-conflict"]),
    }
    state.cdc_replica_identity_findings = {"app.held": _OPEN_RI}
    state.cdc_catalog_drift_findings = {"app.held": _OPEN_CATALOG}
    monkeypatch.setattr(
        _cdc_status, "cdc_schema_drift_summary",
        lambda *_a: _drift(
            {"table": "app.customers", "kind": "unique-conflict", "count": 3},
            {"table": "app.held", "kind": "unique-conflict", "count": 1},
        ),
    )
    validation = _validation([_item("app.customers"), _item("app.held")], run_start=_MATCHED)
    payload = json.loads(_executor(state, validation)("get_cdc_status", {}))
    assert payload["resolved_tables"] == [
        {"table": "app.customers", "checksum_compared_from": "2026-10-09T01:05:00+00:00"},
        {"table": "app.held", "checksum_compared_from": "2026-10-09T01:05:00+00:00",
         "still_held_by": ["REPLICA IDENTITY", "source schema"]},
    ]
    assert payload["resolved_tables_total"] == 2
    assert (
        "one with still_held_by is still held at Cut over by its open REPLICA IDENTITY or "
        "source schema finding, which keeps its own fix" in payload["note"]
    )


def test_the_open_finding_reader_is_pure_and_best_effort() -> None:
    from types import SimpleNamespace

    from dsql_migrator.ui.cdc_dlq_gate import (
        cdc_dlq_open_finding_holds,
        cdc_dlq_still_held_sentence,
    )

    closed = _OPEN_CATALOG.model_copy(
        update={"resolved_at": T0 + timedelta(minutes=1), "resolved_by": "matched"}
    )
    state = SimpleNamespace(
        cdc_replica_identity_findings={"app.a": _OPEN_RI, "app.other": _OPEN_RI},
        cdc_catalog_drift_findings={"app.a": _OPEN_CATALOG, "app.b": closed},
    )
    # Only the tables asked about, only OPEN findings, checks in a fixed order.
    assert cdc_dlq_open_finding_holds(state, ["app.a", "app.b", "app.c"]) == {
        "app.a": ("REPLICA IDENTITY", "source schema"),
    }
    assert cdc_dlq_open_finding_holds(state, []) == {}
    assert cdc_dlq_open_finding_holds(None, ["app.a"]) == {}

    class _Broken:
        @property
        def cdc_replica_identity_findings(self):
            raise RuntimeError("unreadable")

    assert cdc_dlq_open_finding_holds(_Broken(), ["app.a"]) == {}
    assert cdc_dlq_still_held_sentence({}) == ""
    # Bounded for the AI facts, with no pointer at a notice it cannot see.
    many = {f"app.t{i}": ("REPLICA IDENTITY",) for i in range(8)}
    text = cdc_dlq_still_held_sentence(many, limit=6, pointer=False)
    assert text.startswith("Cut over still holds app.t0 for its open REPLICA IDENTITY finding, ")
    assert text.endswith(
        "finding, app.t4 for its open REPLICA IDENTITY finding and app.t5 for its open "
        "REPLICA IDENTITY finding (+2 more)."
    )
    assert "app.t6" not in text and "notice" not in text
