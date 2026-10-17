"""G99: the review of G93 -- a not-converted view's note names only what blocks it, a chat
instance is this tab's, and programmatic asks queue first in, first out. (Its punt-detector
and nudge tests went with the detector in G116: tool_chat's reply comes through an answer
tool, test_g116_ai_protocol.)"""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor


def test_a_colliding_views_note_names_only_what_blocks_it() -> None:
    """Its body's 'rewrite it in the target DDL' followed 'an edit is not applied'."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    long = "order_line_archive_" + "x" * 50
    inventory = SourceInventory(
        tables=[TableDef(name=f"shop.{long}_table", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                         primary_key=["id"])],
        views=[__import__("dsql_migrator.core.models", fromlist=["ViewDef"]).ViewDef(
            name=f"shop.{long}_view",
            definition=f"select substring_index(`t`.`id`,',',1) AS `x` from `shop`.`{long}_table` `t`",
        )],
    )
    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None), SchemaConversionState())
    note = json.loads(execute("get_converted_ddl", {"object_name": f"shop.{long}_view"}))["note"]
    assert "Shorten the names on the source" in note and "SUBSTRING_INDEX" not in note


def test_another_tab_moving_the_session_scope_keeps_this_tabs_chat_open() -> None:
    """Tab A re-focused its own still-open tune chat after tab B opened another one, got a
    new instance, and its running re-test was refused."""
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui1, ui2 = _Ui(), _Ui()
    tab_a, tab_b = build_ai_panel(ui1, state=state), build_ai_panel(ui2, state=state)
    asked: list = []
    send = tab_a.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui1)
    tab_b.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"), seed_question="Next?")
    _pump(ui2)
    tab_a.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                     streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui1)
    assert send.is_open()
    send("RETEST")
    _pump(ui1)
    assert any(m.get("text") == "RETEST" for call in asked for m in call)


def test_an_earlier_chats_send_says_it_is_closed() -> None:
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A"), seed_question=None)
    panel.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"), seed_question=None)
    assert not send.is_open()


def test_programmatic_asks_queue_first_in_first_out() -> None:
    """Two asks while a reply streamed shared one slot: the first was overwritten."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()
    seen: list = []

    def streamer(messages, on_delta):  # noqa: ANN001
        seen.append(messages[-1]["text"])
        if len(seen) == 1:
            gate.wait(5)
        on_delta("A")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown=f"A{len(seen)}")

    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA", streamer=streamer,
                            seed_question="Tune Q1")
    send("RETEST 1")
    send("RETEST 2")
    gate.set()
    for _ in range(4):
        _pump(ui, max_iters=400)
    assert seen == ["Tune Q1", "RETEST 1", "RETEST 2"]


def test_a_queued_ask_dropped_by_another_chat_is_announced() -> None:
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()

    def slow(messages, on_delta):  # noqa: ANN001
        gate.wait(5)
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="A")

    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA", streamer=slow,
                            seed_question="Tune Q1")
    send("RETEST")
    panel.open_scope(scope_id="query-converter:review:q1", title="AI DBA", streamer=_make_streamer("R"),
                     seed_question=None)
    gate.set()
    _pump(ui, max_iters=400)
    assert notices and "no longer open" in notices[-1]
