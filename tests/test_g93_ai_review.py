"""G93: the review of G91 -- a send bound to its scope INSTANCE; a chat per Query Converter
query; a not-converted view's note with every reason. (Its punt-detector and nudge tests went
with the detector in G116: tool_chat's reply comes through an answer tool,
test_g116_ai_protocol.)"""

from __future__ import annotations

import json
from types import SimpleNamespace

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor


def _panel():
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    general: list = []
    panel = build_ai_panel(
        ui, state=state,
        general_streamer_factory=lambda: _make_streamer("GENERAL", record=general),
    )
    return state, ui, panel, notices, general


def test_a_send_asks_only_while_its_scope_instance_is_open() -> None:
    """Bound to a scope id, a re-test still landed in the SAME id reopened in a new window,
    in the new journey after Start over, and in the general chat after AI was re-homed."""
    state, ui, panel, notices, general = _panel()
    asked: list = []
    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui)
    panel.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"),
                     seed_question="What next?")
    _pump(ui)
    reopened: list = []
    panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                     streamer=_make_streamer("A2", record=reopened), seed_question="Tune Q1")
    _pump(ui)
    send("RETEST of the first window's rewrite")
    _pump(ui)
    assert len(reopened) == 1 and notices  # refused, and the operator is told

    # Start over: a send of the old journey never asks in the new one.
    notices.clear()
    send2 = panel.open_scope(scope_id="query-converter:tune:q2", title="AI DBA",
                             streamer=_make_streamer("B"), seed_question=None)
    state.ai_conversation.reset()
    panel.reset()
    send2("RETEST after Start over")
    assert notices and not state.ai_conversation.messages

    # Re-homed to general (AI off and on, panel reopened): the tune send is refused there.
    notices.clear()
    send3 = panel.open_scope(scope_id="query-converter:tune:q3", title="AI DBA",
                             streamer=_make_streamer("C"), seed_question=None)
    state.ai_assist.enabled = False
    send3("anything")  # inert while AI is off: the composer's streamer is dropped
    state.ai_assist.enabled = True
    panel.set_visible(True)
    send3("RETEST after AI came back")
    _pump(ui)
    assert not any("RETEST" in str(m) for call in general for m in call)
    assert notices


def test_each_query_opens_its_own_chat() -> None:
    from dsql_migrator.core.models import Classification
    from dsql_migrator.core.query_converter import QueryConversionResult, StatementKind
    from dsql_migrator.ui.query_playground import _query_scope_key

    def result(sql: str) -> QueryConversionResult:
        return QueryConversionResult(original_sql=sql, converted_sql=sql,
                                     classification=Classification.AUTO,
                                     statement_kind=StatementKind.SELECT)

    assert _query_scope_key(result("SELECT 1")) != _query_scope_key(result("SELECT 2"))
    assert _query_scope_key(result("SELECT 1")) == _query_scope_key(result("SELECT 1"))


def test_a_not_converted_views_note_names_every_reason() -> None:
    """A materialized view whose name also collides was told only to shorten the names."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    long = "daily_totals_" + "x" * 55
    inventory = SourceInventory(
        tables=[TableDef(name=f"shop.{long}_a", columns=[ColumnDef(name="id", mysql_type="int",
                                                                      nullable=False)],
                         primary_key=["id"])],
        views=[ViewDef(name=f"shop.{long}_b", unsupported_kind="materialized view")],
    )
    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None), SchemaConversionState())
    note = json.loads(execute("get_converted_ddl", {"object_name": f"shop.{long}_b"}))["note"]
    assert "materialized view" in note and "Shorten the names on the source" in note
