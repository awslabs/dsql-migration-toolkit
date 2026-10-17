"""G91: the review of G84 -- a programmatic ask bound to its own scope, and the remedy of a
view the conversion could not convert. (Its punt-detector tests went with the detector in
G116: tool_chat's reply comes through an answer tool, test_g116_ai_protocol.)"""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor


def test_a_retest_that_finishes_after_another_scope_opened_is_not_asked_there() -> None:
    """The Query Converter's re-test was asked in the 'What's next?' thread with its
    streamer: the send handle was never bound to its scope."""
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    panel = build_ai_panel(ui, state=state)
    tune_asked: list = []
    briefing_asked: list = []
    send = panel.open_scope(
        scope_id="query-converter:tune", title="AI DBA",
        streamer=_make_streamer("REWRITE", record=tune_asked), seed_question="Rewrite it",
    )
    _pump(ui)
    panel.open_scope(
        scope_id="readiness", title="AI DBA",
        streamer=_make_streamer("BRIEFING", record=briefing_asked), seed_question="What next?",
    )
    _pump(ui)
    send("RETEST: the rewrite measured 12 DPU vs 40 baseline")
    _pump(ui)
    texts = [m["text"] for m in state.ai_conversation.messages if m.get("role") == "user"]
    assert not any("RETEST" in t for t in texts)
    assert len(tune_asked) == 1 and len(briefing_asked) == 1
    assert notices and "no longer open" in notices[-1]


def test_a_briefing_queued_in_one_tab_survives_another_tabs_scope() -> None:
    """tab2 opening an object chat moved the session-wide scope, and tab1's queued
    'What's next?' was dropped when its reply ended."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui1, ui2 = _Ui(), _Ui()
    tab1, tab2 = build_ai_panel(ui1, state=state), build_ai_panel(ui2, state=state)
    gate = threading.Event()

    def slow(messages, on_delta):  # noqa: ANN001
        gate.wait(5)
        on_delta("A1")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="A1")

    tab1.open_scope(scope_id="eval:orders", title="orders", streamer=slow, seed_question="Q1")
    briefing: list = []
    send = tab1.open_scope(
        scope_id="readiness", title="AI DBA",
        streamer=_make_streamer("BRIEFING", record=briefing), seed_question=None,
    )
    send("what should I do next?")
    tab2.open_scope(scope_id="eval:items", title="items", streamer=_make_streamer("B"),
                    seed_question="QB")
    _pump(ui2)
    gate.set()
    _pump(ui1, max_iters=400)
    _pump(ui1, max_iters=400)
    assert briefing and briefing[-1][-1]["text"] == "what should I do next?"


def _not_converted_note(inventory) -> str:  # noqa: ANN001
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None),
                        SchemaConversionState())
    out = json.loads(execute("get_converted_ddl", {"object_name": inventory.views[0].name}))
    assert out["edited"] is False
    return out["note"]


def test_a_not_converted_views_note_carries_its_own_remedy() -> None:
    """Every not-converted view was told 're-created by hand on Aurora DSQL' -- wrong for a
    view whose name collides (a hand-made one collides too) and for a materialized view."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef

    long = "order_line_archive_" + "x" * 50
    table = TableDef(name=f"shop.{long}_table",
                     columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                     primary_key=["id"])
    collision = SourceInventory(
        tables=[table],
        views=[ViewDef(name=f"shop.{long}_view", definition=f"SELECT id FROM shop.{long}_table")],
    )
    note = _not_converted_note(collision)
    assert "Shorten the names on the source" in note and "by hand" not in note

    matview = SourceInventory(
        tables=[], views=[ViewDef(name="shop.daily_totals", unsupported_kind="materialized view")]
    )
    note = _not_converted_note(matview)
    assert "materialized view" in note and "application-refreshed table" in note

    missing = SourceInventory(tables=[], views=[ViewDef(name="shop.v_orders", definition="")])
    assert "reimplement the view manually" in _not_converted_note(missing)
