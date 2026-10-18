"""G121: the final regression round on G112-G117 (AI). An answer cut at the output limit before
any of it arrived is asked for once more, shorter, and otherwise the turn says the reply was too
long (not 'could not be parsed'); an answer held back beside look-ups is the reply when the
rounds after it fail. A reply cut inside its last code fence offers no footer, so 'Use as target
DDL' and 'Test rewrite on target' never take the block before it. And the code says where the
no-punt guarantee rests on the endpoint honouring tool_choice."""

from __future__ import annotations

import pytest

from tests.test_g104_ai_review import _ClientError
from tests.test_g116_ai_protocol import _REAL, _Recorder, _answer, _chat, _look

# What the buffered API returns for an answer cut at the output limit: the call, no markdown.
_CUT = _answer(None, stop="max_tokens")


def _beside(markdown: str, *, look_id: str = "t1", answer_id: str = "a1") -> dict:
    """A turn that calls a look-up and the answer together."""
    return {"stop_reason": "tool_use", "content": [
        {"type": "tool_use", "id": look_id, "name": "list_cdc_dlq_samples", "input": {}},
        {"type": "tool_use", "id": answer_id, "name": "answer", "input": {"markdown": markdown}}]}


def _text(text: str, *, stop: str = "end_turn") -> dict:
    return {"stop_reason": stop, "content": [{"type": "text", "text": text}]}


def test_an_answer_cut_before_any_of_it_arrived_is_asked_for_again_shorter() -> None:
    """The answer's Markdown is one tool parameter, and the API hands one back only once it is
    complete: a reply longer than the output limit came back as an answer call with no
    markdown, and the turn ended 'could not be parsed ... Re-run', which a re-run hit again."""
    short = "The first 40 of 400 routines: ... Ask for the next part to see the rest."
    client = _Recorder([
        _answer(None, stop="max_tokens", call_id="a1"), _answer(short, call_id="a2"),
    ])
    outcome, shown, ran = _chat(client)
    assert outcome.available and outcome.markdown == short and shown == [short]
    assert len(client.sent) == 2 and ran == []
    # The cut call stays in the transcript, answered as the API requires: with what to do.
    *_, cut_turn, told = client.sent[1]["messages"]
    assert cut_turn["role"] == "assistant" and cut_turn["content"][0]["id"] == "a1"
    assert told["role"] == "user" and [b["tool_use_id"] for b in told["content"]] == ["a1"]
    assert "cut at the output limit" in told["content"][0]["content"]
    assert "12,000 characters" in told["content"][0]["content"]


def test_a_reply_too_long_to_write_ends_the_turn_saying_so() -> None:
    """Asked once only -- a cut round spends the whole output budget: cut again, or cut with no
    round left to ask in, the turn says why there is no reply."""
    for script, max_rounds, calls in (
        ([_CUT], 6, 2),  # cut, asked again, cut again
        ([_CUT], 1, 1),  # no round left to ask in
        ([_look(), _CUT], 2, 2),  # cut on the last round, after a look-up
        ([_CUT, _look("t2"), _CUT], 6, 3),  # asked once per turn
    ):
        client = _Recorder(script)
        outcome, shown, _ran = _chat(client, max_rounds=max_rounds)
        assert not outcome.available and outcome.reason == "INVALID_OUTPUT" and shown == []
        assert "longer than the assistant can write in one turn" in outcome.detail
        assert len(client.sent) == calls


@pytest.mark.parametrize(
    "after",
    [
        _ClientError("ThrottlingException"),
        _ClientError("ExpiredTokenException"),
        {"stop_reason": "end_turn", "content": []},
        _CUT,
        _look("t2"),  # an endpoint that ignores tool_choice looks things up to the end
    ],
    ids=["throttled", "denied", "empty", "cut", "out of rounds"],
)
def test_an_answer_held_back_beside_a_look_up_is_the_reply_when_the_rounds_after_it_fail(
    after,
) -> None:
    """An answer called beside a look-up waits for its results; when the round that reads them
    failed, the turn ended 'AI reply unavailable' and the answer already received was lost. It
    is the reply, saying it was written before them."""
    outcome, shown, ran = _chat(_Recorder([_beside(_REAL), after]), max_rounds=3)
    assert outcome.available and outcome.markdown.startswith(_REAL + "\n\n")
    assert "Written before the results of the look-ups it asked for" in outcome.markdown
    assert shown == [outcome.markdown] and ran
    # The latest one held is the reply.
    client = _Recorder([
        _beside("A first draft."), _beside("A second draft.", look_id="t2", answer_id="a2"),
        _ClientError("ThrottlingException"),
    ])
    assert _chat(client)[0].markdown.startswith("A second draft.\n\n")


def test_a_reply_cut_inside_its_last_fence_offers_no_footer(monkeypatch) -> None:
    """Cut inside its last sql block, a reply still offered 'Use as target DDL', which took the
    complete block BEFORE it -- the current DDL, shown first -- and 'Test rewrite on target'
    tested the 'before' query as the rewrite."""
    from dsql_migrator.core.models import SourceInventory
    from dsql_migrator.ui.query_playground import extract_sql_from_reply
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply
    from tests.test_g105_schema_review import (
        _conversion_chat,
        _customers,
        _orders,
        _orders_script,
        _sql,
    )

    def past_the_cap(markdown: str) -> str:
        """tool_chat's reply for an answer longer than the panel shows, cut at its cap."""
        reply = _chat(_Recorder([_answer(markdown + "\n  -- " + "x" * 13000 + "\n")]))[0].markdown
        assert "cut at its length limit" in reply
        return reply

    _ui, _conv_state, open_chat = _conversion_chat(
        monkeypatch, SourceInventory(tables=[_customers(), _orders()])
    )
    _result, table, script = _orders_script()
    chat = open_chat("shop.orders", script)
    fixed = table.target_ddl.replace('"status" TEXT', '"status" VARCHAR(6)')
    shown_first = "Your current target DDL:\n" + _sql(table.target_ddl + ";") + "\nThe fix:\n"
    cut = past_the_cap(shown_first + "```sql\n" + fixed.split('"status"')[0])
    assert chat["footer_visible"](cut) is False
    adoption = adopt_ddl_from_reply(cut, "shop.orders", script)
    assert adoption.script is None and "cut inside its last code block" in adoption.notice
    # Whole, the same reply adopts its last block: the fix.
    whole = shown_first + _sql(fixed + ";")
    assert chat["footer_visible"](whole) is True
    assert "VARCHAR(6)" in (adopt_ddl_from_reply(whole, "shop.orders", script).script or "")

    before = "Before:\n" + _sql("SELECT * FROM app.orders;") + "\nRewrite:\n"
    cut = past_the_cap(before + "```sql\nSELECT o.id FROM app.orders o")
    assert extract_sql_from_reply(cut) is None
    # The plain chat's reply cut at the output limit ends the same way, with no note.
    assert extract_sql_from_reply(before + "```sql\nSELECT o.id FROM") is None
    assert extract_sql_from_reply(before + _sql("SELECT o.id FROM app.orders o;")) == (
        "SELECT o.id FROM app.orders o"
    )
    from dsql_migrator.ui.ai_chat_drawer import ends_in_open_fence

    assert ends_in_open_fence("ok\n```sql\nSELECT unfinished")
    assert ends_in_open_fence(_sql("SELECT 1;") + "\n1. Then:\n   ```sql\n   SELECT 2")
    assert not ends_in_open_fence(_sql("SELECT 1;") + "\nThat is all.")
    assert not ends_in_open_fence(_sql("SELECT 1;") + "\nRun it as is: no ``` around it.")
    assert not ends_in_open_fence("no code at all") and not ends_in_open_fence("")


def test_the_no_punt_guarantee_is_stated_where_it_holds() -> None:
    """'A turn cannot end with an announcement' holds only where the endpoint honours
    tool_choice, and the answer's own Markdown is not read: a model or proxy that ignores
    tool_choice still ends a turn in text, which is shown as it stands (cut, it says so), and
    an announcement written into the answer tool is shown too. The code claimed more."""
    import inspect

    from dsql_migrator.core import assessment_strategist as module

    comments = " ".join(inspect.getsource(module).replace("#", " ").split())
    assert "a turn cannot end with an announcement" not in comments
    assert "a turn cannot END IN TEXT, and only where the endpoint honours tool_choice" in comments
    assert "an announcement written INTO the answer tool is shown too" in comments
    doc = " ".join((module.AssessmentStrategist.tool_chat.__doc__ or "").split())
    assert 'never only "let me check ..."' not in doc
    assert "where the endpoint honours ``tool_choice`` (Bedrock)" in doc
    assert "is the reply as it stands, whatever it says" in doc
    # The plain-text fallback is that: shown as it stands, and when cut it says so.
    punt = "Let me check the DLQ samples."
    assert _chat(_Recorder([_text(punt)]))[1] == [punt]
    cut = _chat(_Recorder([_text("app.orders is missing 3 rows. Reload it", stop="max_tokens")]))
    assert cut[0].markdown.startswith("app.orders is missing 3 rows. Reload it\n\n")
    assert "cut at its length limit" in cut[0].markdown
