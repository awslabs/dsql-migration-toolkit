# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The AI DBA's Amazon Bedrock chats, grounded on the deterministic migration facts.

:class:`AssessmentStrategist` answers the AI DBA panel's turns:
:meth:`AssessmentStrategist.stream_chat` streams a grounded reply, and
:meth:`AssessmentStrategist.tool_chat` lets the model look facts up through read-only
tools before it answers. The ``build_*_system`` functions write each scope's
grounding, most of them on top of the known Aurora DSQL constraints
(:data:`DSQL_CONSTRAINTS`; Requirement 11); they are
:mod:`dsql_migrator.core.ai_prompts`'s, re-exported here.

Design guarantees (consistent with the existing AI-assist path):

- **Augment, never replace** (Property 6 / Requirement 11.1, 11.2): the
  deterministic assessment is authoritative for the hard DSQL facts (FK/PK/
  trigger/type/etc.) and Property 8 holds regardless of AI. This layer only
  adds advice in the AI DBA's replies; AI never changes a deterministic
  classification or effort.
- **Read-only** (Property 1 / Requirement 11.11): inputs are already-extracted
  in-memory models; no source/target connection is opened here.
- **Untrusted output** (Requirement 11.8): the model output is read
  defensively -- an empty or malformed reply degrades to an unavailable outcome
  -- and reply and tool-result sizes are capped.
- **Graceful degradation** (Requirement 11.10): the chat methods never raise; on
  any Bedrock failure or an empty reply they return an unavailable
  :class:`ObjectGuidanceOutcome`, so the deterministic workflow goes on without
  the reply.
- **Credential confidentiality** (Property 7): the Bedrock client uses IAM auth
  via the shared session/profile; no credential value is read or logged, and
  failure messages are fixed and credential-free.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

from dsql_migrator.core.ai_assistant import (
    _ANTHROPIC_VERSION,
    AiAssistUnavailableError,
    _classify_bedrock_error,
    build_bedrock_runtime_client,
)
from dsql_migrator.core.models import (
    AiAssistConfig,
    AssessmentItem,
    SourceType,
)


def source_engine_word(source_type: Optional["SourceType"]) -> str:
    """Map a :class:`SourceType` to the engine word used to ground the AI DBA.

    The TARGET is always Aurora DSQL (PostgreSQL-compatible); this names the
    SOURCE dialect so every prompt reasons about the right one. Defaults to
    ``"MySQL"`` (the original engine) for a missing/unknown type.
    """
    return "PostgreSQL" if source_type is SourceType.POSTGRES else "MySQL"

# Output-token budgets. These are the `max_tokens` CAP per model turn, not a
# target -- the _RESPONSE_STYLE directive keeps answers concise, so a bigger cap
# only adds headroom, it does not bloat normal replies.
#
# Keyed by CALL SHAPE, not by screen/feature. The AI DBA is now ONE persistent
# app-wide panel, so every interactive turn shares a SINGLE chat budget
# (_CHAT_MAX_TOKENS) -- there is no per-screen limit to keep in sync. The only
# other budget is a genuinely different SHAPE of call: the bare permission probe
# (_ACCESS_CHECK_MAX_TOKENS in core/ai_assistant.py).
#
# IMPORTANT: the current models (Claude 5 family, e.g. global.anthropic.
# claude-sonnet-5) use EXTENDED THINKING, and thinking tokens count against
# `max_tokens`. A single reasoning-heavy turn can spend a few thousand tokens on
# thinking BEFORE emitting any answer/tool call; if the cap is too small the turn
# stops (stop_reason="max_tokens") with only a thinking block -- no text and no
# completable tool_use -- which the chat then had to treat as an empty/unparseable
# reply. So the generation cap must comfortably fit THINKING + the answer.
# (Verified: a tuning turn burned an entire 1536-token cap on thinking alone and
# produced no text.) Keep it generous.

# EVERY interactive AI DBA turn -- plain grounded chat (stream_chat) and
# tool-calling chat (tool_chat), which every scope's chat goes through (an
# object's guidance, Query Converter tuning, the CDC error chat, ...). Single
# source of truth for the persistent panel: one place to tune the chat budget.
# With extended thinking DISABLED (see _THINKING_OFF) the whole budget is
# available for the answer.
_CHAT_MAX_TOKENS = 8192

# The current models (Claude 5 family) do EXTENDED THINKING by default, and thinking
# tokens count against max_tokens. On a reasoning-heavy / multi-tool turn the model
# could spend the ENTIRE budget on thinking and stop (stop_reason="max_tokens") with
# only a thinking block -- no answer text -- which surfaced to the user as "AI reply
# unavailable / could not be parsed" (seen on Query Converter "Tune" and the header
# "What's next?" briefing). Raising the cap only reduced the frequency; it never
# eliminated the tail. Since these replies are GROUNDED (tool results + the system
# context carry the facts, not deep model reasoning) and _RESPONSE_STYLE keeps them
# concise, we turn thinking OFF for every AI-DBA generation: the full budget goes to
# the answer, so the empty-thinking failure cannot occur. (sonnet-5 rejects
# thinking.type.enabled/budget -- "disabled" is the supported off switch here.) tool_chat's
# forced tool_choice (see _ANSWER_TOOL) needs it off as well.
_THINKING_OFF = {"type": "disabled"}

# Upper bound on the running multi-turn chat transcript (characters) sent to the
# model per turn. Bounds token cost / context growth on a long conversation: the
# most recent turns within this budget are kept (oldest dropped first), so the
# chat stays responsive and affordable without ending the conversation.
_MAX_CHAT_TRANSCRIPT_CHARS = 24000

# Upper bound on the in-process tool-use agentic loop (model round-trips per user
# turn). Each round may call one or more read-only tools; the cap stops a runaway
# call-loop and bounds cost. A handful is plenty for "look up X, then answer".
_MAX_TOOL_ROUNDS = 6

# tool_chat's reply comes through a tool. A model sometimes ENDED its turn with only "Let me
# pull the assessment details..." and no tool call, and that preamble was the whole reply (the
# "What's next?" briefing). Telling such a punt from an answer that promises a later check by
# the reply's wording never settled: each round of tuning missed new punts and nudged complete
# answers (lost when the nudged round then failed). So every round asks for a tool call
# (tool_choice "any") -- a look-up, or this one, whose Markdown is the reply -- and the last
# round can only answer (tool_choice "tool").
#
# What that guarantees is that a turn cannot END IN TEXT, and only where the endpoint honours
# tool_choice: under "any" / "tool" the API prefills the assistant turn with a tool call, as
# Bedrock does for every model in SUPPORTED_BEDROCK_MODELS. A model or proxy that ignores it can
# still end a turn in text, which tool_chat shows as it stands -- an announcement included, as
# nothing reads a reply's wording. Nor is the answer's own Markdown read: an announcement
# written INTO the answer tool is shown too, with only the tool's description and
# _TOOL_CHAT_STYLE against it.
#
# Forced tool use needs thinking off (_THINKING_OFF: the API refuses tool_choice "any" / "tool"
# beside extended thinking). The models that refuse forced tool use (Claude Sonnet 5.5, Opus 5.5,
# Fable 5.1) refuse disabled thinking too, so a move to one of them changes both.
_ANSWER_TOOL = "answer"
_ANSWER_TOOL_SCHEMA: dict = {
    "name": _ANSWER_TOOL,
    "description": (
        "Deliver your reply to the operator: the complete answer, in Markdown. Call it once "
        "you have what you need -- after the look-ups the question needs. The operator sees "
        "nothing else you write, so never use it to say you will look something up: call "
        "that tool instead."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "markdown": {
                "type": "string",
                "description": "The complete reply to the operator, in Markdown.",
            }
        },
        "required": ["markdown"],
    },
}
# Appended after _RESPONSE_STYLE in tool_chat only (stream_chat has no tools).
_TOOL_CHAT_STYLE = (
    "\n- Call the look-up tools the question needs (several at once when they are "
    "independent), then deliver the reply with the answer tool: the operator sees nothing "
    "else. Never reply that you will look something up -- call the tool."
)
# A reply cut short -- at the model's output limit, or at the cap on what the panel shows --
# says so, rather than ending mid-sentence as if it were whole. A code fence it cut stays open:
# closed, the cut statement would be a complete block that "Use as target DDL" or "Test
# rewrite on target" takes (and open, neither takes the block before it: ends_in_open_fence).
_CUT_NOTE = "\n\n_(The reply was cut at its length limit: ask for one part at a time.)_"
# An answer called beside look-ups waits for their results (tool_chat), but it was received:
# when the turn would end without a reply after all, it is the reply, saying what it lacks.
_HELD_NOTE = (
    "\n\n_(Written before the results of the look-ups it asked for; the answer from those "
    "results did not come through, so ask again for it.)_"
)


def _answer_markdown(block: Mapping[str, Any]) -> str:
    """The Markdown an answer call carries; ``""`` for any other shape (untrusted). Pure."""
    given = block.get("input")
    markdown = given.get("markdown") if isinstance(given, Mapping) else None
    return markdown.strip() if isinstance(markdown, str) else ""


# A tool result handed back to the model: larger than a prompt field (a list of objects is
# what the tools exist for), and when cut it SAYS so -- cut silently mid-JSON, the model read
# a broken list as the whole answer.
_MAX_TOOL_OUTPUT_CHARS = 12000


def bounded_tool_output(text: str) -> str:
    """``text`` within :data:`_MAX_TOOL_OUTPUT_CHARS`, with a marker when it was cut. Pure."""
    if len(text) <= _MAX_TOOL_OUTPUT_CHARS:
        return text
    return (
        text[:_MAX_TOOL_OUTPUT_CHARS]
        + f"\n...[TRUNCATED: {len(text) - _MAX_TOOL_OUTPUT_CHARS:,} more characters were "
        "cut; this result is incomplete -- narrow the request (one classification, one "
        "object) and call the tool again]"
    )


# A chat reply is a free-form Markdown narrative; cap it generously so a verbose
# model reply cannot flood the panel (best practice: accept prose, not brittle
# JSON, and render it as Markdown).
_MAX_NARRATIVE_CHARS = 12000

# Shared response-style directive appended to EVERY chat system prompt in
# :meth:`AssessmentStrategist.stream_chat`, so the assistant keeps replies lean and
# scannable no matter which scope built the base prompt. It bounds the RECEIVED text
# (concise, high-signal, no filler/recap) and pushes VISUAL structure (short bullets,
# a small table for comparisons, bold for the verdict, fenced code for SQL) so an
# answer reads at a glance in the narrow panel instead of as a wall of prose. The SENT
# text is bounded separately (the transcript trim above + the caller's grounding).
_RESPONSE_STYLE = (
    "\n\nHow to answer (important):\n"
    "- GROUND every answer in the specific facts, values, names, counts, and errors "
    "given above — refer to the ACTUAL ones and tailor the advice to THIS situation. "
    "Do NOT give generic, textbook advice that ignores the provided context; if a fact "
    "you would need is missing, state the assumption you are making (or ask for it) "
    "rather than answering generically.\n"
    "- Be concise and high-signal: lead with the answer, include only what is useful "
    "for THIS decision, and skip preamble, restating the question, and long recaps.\n"
    "- Make it easy to scan: a few short bullets, a small Markdown table for a "
    "comparison, **bold** for the key term or verdict, and a fenced ```sql block for "
    "any SQL/DDL. Use only as much text as the question needs — a one- or two-line "
    "answer is good when that is enough."
)

# Every scope's system prompt -- the Aurora DSQL constraints, the query rubric, the
# recovery contexts and the build_*_system functions -- is core/ai_prompts.py (A11):
# pure text, while this module sends the chats. Re-exported -- every name, private ones
# too -- so this module's own calls and every `from
# dsql_migrator.core.assessment_strategist import ...` (the UI's, the tests') resolve
# unchanged.
from dsql_migrator.core.ai_prompts import (  # noqa: E402,F401
    DSQL_CONSTRAINTS,
    build_object_chat_system,
    build_general_chat_system,
    build_conversion_chat_system,
    build_reimplementation_chat_system,
    build_query_chat_system,
    DSQL_QUERY_EFFICIENCY_RUBRIC,
    build_query_optimize_system,
    build_validation_chat_system,
    build_full_load_error_chat_system,
    build_cdc_error_chat_system,
    build_connection_error_chat_system,
    _MAX_TEXT_CHARS,
    _cdc_capture_phrase,
    _validation_recovery_context,
    _FULL_LOAD_RECOVERY_CONTEXT,
    _cdc_recovery_context,
)


def _build_chat_body(
    system: str, messages: Sequence[Mapping[str, str]], max_tokens: int
) -> str:
    """Serialize an Anthropic-style multi-turn messages request body.

    ``messages`` is the running transcript as ``{"role", "text"}`` entries
    (alternating user/assistant, starting with the user's first question);
    ``system`` is the persistent grounding from :func:`build_object_chat_system`.
    """
    return json.dumps(
        {
            "anthropic_version": _ANTHROPIC_VERSION,
            "max_tokens": max_tokens,
            "thinking": _THINKING_OFF,
            "system": system,
            "messages": [
                {
                    "role": "assistant"
                    if str(entry.get("role")) == "assistant"
                    else "user",
                    "content": [{"type": "text", "text": str(entry.get("text", ""))}],
                }
                for entry in messages
            ],
        }
    )


def _trim_chat_messages(
    messages: Sequence[Mapping[str, str]], max_chars: int
) -> list[Mapping[str, str]]:
    """Keep the most recent turns whose text fits ``max_chars`` (oldest dropped).

    Bounds the transcript sent to the model on a long conversation. Always keeps
    at least the latest turn, and ensures the result still starts with a ``user``
    turn (Anthropic requires the first message to be from the user), dropping a
    leading ``assistant`` turn if trimming exposed one.
    """
    kept: list[Mapping[str, str]] = []
    total = 0
    for entry in reversed(list(messages)):
        total += len(str(entry.get("text", "")))
        kept.append(entry)
        if total >= max_chars:
            break
    kept.reverse()
    while len(kept) > 1 and str(kept[0].get("role")) == "assistant":
        kept.pop(0)
    return kept


def _iter_stream_text(response: Mapping[str, Any]) -> Iterator[str]:
    """Yield text deltas from a Bedrock ``invoke_model_with_response_stream`` body.

    Parses the Anthropic streaming event format defensively (untrusted output,
    Requirement 11.8): each event's ``chunk.bytes`` is a JSON object, and only
    ``content_block_delta`` events with a string ``delta.text`` contribute text.
    Anything malformed is skipped rather than raising, so a partial/odd stream
    degrades to whatever text was recovered.
    """
    stream = response.get("body") if isinstance(response, Mapping) else None
    if stream is None:
        return
    for event in stream:
        chunk = event.get("chunk") if isinstance(event, Mapping) else None
        if not isinstance(chunk, Mapping):
            continue
        raw = chunk.get("bytes")
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        if not isinstance(data, Mapping) or data.get("type") != "content_block_delta":
            continue
        delta = data.get("delta")
        if isinstance(delta, Mapping):
            text = delta.get("text")
            if isinstance(text, str) and text:
                yield text


class AssessmentStrategist:
    """Answers the AI DBA's chat turns through Amazon Bedrock (Req 11).

    Augments — never replaces — the deterministic results each chat is grounded
    on. The Bedrock client is injectable (tests pass a fake) and otherwise built
    lazily from the shared session honoring the optional global AWS profile, so
    constructing the strategist performs no network call (Property 7 /
    Requirement 9.5).
    """

    def __init__(
        self,
        config: AiAssistConfig,
        *,
        aws_profile: Optional[str] = None,
        client: Optional[Any] = None,
        source_engine: str = "MySQL",
    ) -> None:
        self._config = config
        self._aws_profile = aws_profile
        self._client = client
        # The migration's SOURCE engine word ("MySQL" / "PostgreSQL"), threaded into
        # every grounding prompt so the AI DBA reasons about the RIGHT source dialect
        # (the target is always Aurora DSQL / PostgreSQL). Defaults to "MySQL" — the
        # original engine — so old call sites stay correct.
        self._source_engine = source_engine or "MySQL"

    @property
    def source_engine(self) -> str:
        """The migration's SOURCE engine word grounding every prompt."""
        return self._source_engine

    @source_engine.setter
    def source_engine(self, value: str) -> None:
        # Injectable factories (e.g. the Evaluation screen's) build the strategist
        # before the session's source type is in scope; this lets the caller pin the
        # engine post-construction so the AI DBA is grounded on the right dialect.
        self._source_engine = value or "MySQL"

    def _get_client(self) -> Any:
        """Return the bedrock-runtime client, building it lazily if needed."""
        if self._client is None:
            self._client = build_bedrock_runtime_client(
                self._config, aws_profile=self._aws_profile
            )
        return self._client

    def stream_object_chat(
        self,
        item: AssessmentItem,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        execute: Optional[Callable[[str, Mapping[str, Any]], str]] = None,
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a multi-turn chat about ONE object.

        ``messages`` is the running transcript (``{"role", "text"}`` entries,
        ending with the latest user turn); the model is grounded by
        :func:`build_object_chat_system` so every reply stays specific to the
        object and consistent with the deterministic facts. Emits incremental
        text via ``on_delta`` (token-by-token reveal) and returns the assembled
        assistant reply as :class:`ObjectGuidanceOutcome` -- available with the
        full Markdown, or a fixed, credential-free unavailable message on any
        Bedrock failure / empty stream (Requirements 11.8, 11.10). Never raises.

        When ``tools`` + ``execute`` are supplied, the turn runs through
        :meth:`tool_chat` instead, so a chat that started on ONE object can still
        answer WIDER-migration questions (e.g. "list all unsupported objects") by
        looking up the real data on demand -- matching the general chat's reach.
        """
        if tools is not None and execute is not None:
            return self.tool_chat(
                build_object_chat_system(item, source_engine=self._source_engine),
                messages, on_delta, tools=tools, execute=execute,
            )
        return self.stream_chat(
            build_object_chat_system(item, source_engine=self._source_engine),
            messages, on_delta,
        )

    def stream_validation_chat(
        self,
        facts: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        scope: str = "table",
        table: Optional[str] = None,
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        execute: Optional[Callable[[str, Mapping[str, Any]], str]] = None,
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a chat about a validation mismatch.

        Grounded by :func:`build_validation_chat_system` on the deterministic,
        credential-free validation ``facts`` (counts, missing/extra summary,
        drift), so replies explain WHY the table/run diverged and HOW to fix it
        (re-run to backfill a standing gap, or quiesce/stop CDC for a cut-over
        check) consistent with this tool's Full Load + CDC model. Never raises.

        When ``tools`` + ``execute`` are supplied, the turn runs through
        :meth:`tool_chat`, so the mismatch chat can also look up the real
        converted DDL / target schema / row counts to root-cause the divergence.
        Those tools return RUN-WIDE data, so a ``scope="table"`` caller must also
        pass ``table`` -- that is what confines the answer to it (see
        :func:`build_validation_chat_system`).
        """
        system = build_validation_chat_system(
            facts, scope=scope, source_engine=self._source_engine, table=table
        )
        if tools is not None and execute is not None:
            return self.tool_chat(
                system, messages, on_delta, tools=tools, execute=execute
            )
        return self.stream_chat(system, messages, on_delta)

    def stream_full_load_error_chat(
        self,
        table_name: str,
        error_message: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        migration_context: str = "",
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        execute: Optional[Callable[[str, Mapping[str, Any]], str]] = None,
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a chat about a FAILED Full Load table.

        Grounded by :func:`build_full_load_error_chat_system` on the failed
        ``table_name`` + its captured ``error_message`` + the optional
        ``migration_context`` (this migration's situation: type, CDC, DROP+recreate,
        selection), so replies explain WHY the table failed and HOW to fix it
        (schema/DDL vs data vs transient, then per-table Reload / Retry) specific to
        THIS migration and consistent with this tool's Full Load model. Never raises.

        When ``tools`` + ``execute`` are supplied, the turn runs through
        :meth:`tool_chat`, so the diagnosis can look up the real converted DDL / target
        schema / source structure to root-cause a schema/DDL failure by name.
        """
        system = build_full_load_error_chat_system(
            table_name, error_message, migration_context=migration_context,
            source_engine=self._source_engine,
        )
        if tools is not None and execute is not None:
            return self.tool_chat(
                system, messages, on_delta, tools=tools, execute=execute
            )
        return self.stream_chat(system, messages, on_delta)

    def stream_cdc_chat(
        self,
        facts: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        scope: str = "dlq",
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        execute: Optional[Callable[[str, Mapping[str, Any]], str]] = None,
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a chat about a CDC DLQ / schema-drift problem.

        Grounded by :func:`build_cdc_error_chat_system` on the credential-free CDC
        ``facts`` (DLQ depth + poison tables + SQLSTATEs, detected drift, stall state),
        so replies explain WHY records dead-lettered / what drifted and HOW to fix it
        (Fix target schema / per-table Reload / Stop CDC), consistent with this tool's
        CDC model. When ``tools`` + ``execute`` are supplied the turn runs through
        :meth:`tool_chat` so it can look up the affected table's real DDL / schema.
        Never raises.
        """
        system = build_cdc_error_chat_system(
            facts, scope=scope, source_engine=self._source_engine
        )
        if tools is not None and execute is not None:
            return self.tool_chat(
                system, messages, on_delta, tools=tools, execute=execute
            )
        return self.stream_chat(system, messages, on_delta)

    def stream_chat(
        self,
        system: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a grounded multi-turn chat.

        Generic streaming chat shared by every AI chat drawer: ``system`` is the
        persistent grounding (built per screen), ``messages`` is the running
        transcript (``{"role", "text"}`` entries ending with the latest user
        turn, trimmed to a length budget here), and text is emitted incrementally
        via ``on_delta``. Returns the assembled reply as
        :class:`ObjectGuidanceOutcome` -- available with the full Markdown, or a
        fixed, credential-free unavailable message on any Bedrock failure / empty
        stream (Requirements 11.8, 11.10). Never raises.
        """
        body = _build_chat_body(
            # Append the shared lean/scannable response-style directive to whatever
            # grounding the caller built, so every scope's replies stay concise and
            # visually structured (bounds the received text; see _RESPONSE_STYLE).
            system + _RESPONSE_STYLE,
            _trim_chat_messages(messages, _MAX_CHAT_TRANSCRIPT_CHARS),
            _CHAT_MAX_TOKENS,
        )
        try:
            response = self._get_client().invoke_model_with_response_stream(
                modelId=self._config.model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
        except AiAssistUnavailableError as error:
            return ObjectGuidanceOutcome(
                available=False, reason=error.reason, detail=error.detail
            )
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            error = AiAssistUnavailableError(_classify_bedrock_error(exc))
            return ObjectGuidanceOutcome(
                available=False, reason=error.reason, detail=error.detail
            )

        parts: list[str] = []
        try:
            for text in _iter_stream_text(response):
                parts.append(text)
                on_delta(text)
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            error = AiAssistUnavailableError(_classify_bedrock_error(exc))
            return ObjectGuidanceOutcome(
                available=False, reason=error.reason, detail=error.detail
            )

        markdown = "".join(parts).strip()
        if not markdown:
            error = AiAssistUnavailableError("INVALID_OUTPUT")
            return ObjectGuidanceOutcome(
                available=False, reason=error.reason, detail=error.detail
            )
        return ObjectGuidanceOutcome(
            available=True,
            reason="OK",
            detail="",
            markdown=markdown[:_MAX_NARRATIVE_CHARS],
            model_id=self._config.model_id,
        )

    def tool_chat(
        self,
        system: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        tools: Sequence[Mapping[str, Any]],
        execute: Callable[[str, Mapping[str, Any]], str],
        max_rounds: int = _MAX_TOOL_ROUNDS,
    ) -> "ObjectGuidanceOutcome":
        """Answer one chat turn that MAY call in-process tools (function calling).

        Like :meth:`stream_chat`, but the model is given ``tools`` (Anthropic tool
        schemas) it can call to fetch DETERMINISTIC migration facts on demand -- e.g.
        the converted DDL of a table, the assessment of an object, the validation
        verdict. Each requested tool is run via ``execute(name, input) -> str`` (a
        read-only, credential-free callable the shell supplies over its own state) and
        the result fed back, in a bounded agentic loop. The reply comes through one more
        tool, the answer tool (:data:`_ANSWER_TOOL_SCHEMA`), and every round asks for a
        tool call, so where the endpoint honours ``tool_choice`` (Bedrock) a turn is a
        look-up or the answer, never text that only says "let me check ...". The answer's
        Markdown is delivered through ``on_delta`` and returned as
        :class:`ObjectGuidanceOutcome`; a turn of plain text (from a model or proxy that
        ignored ``tool_choice``) is the reply as it stands, whatever it says. An answer cut
        at the output limit before any of it arrived is asked for once more, shorter; an
        answer held back beside look-ups is the reply if the turn would end without one.

        Non-streaming per round (tool-use and token streaming do not compose cleanly),
        so the final answer arrives at once; ``on_delta`` still receives it so the
        panel renders it identically (Markdown tables / code included). Never raises:
        any Bedrock failure or empty output maps to an unavailable outcome. Tool
        results are length-capped and untrusted (Requirement 11.8).
        """
        convo: list[dict] = [
            {
                "role": "assistant" if str(m.get("role")) == "assistant" else "user",
                "content": [{"type": "text", "text": str(m.get("text", ""))}],
            }
            for m in _trim_chat_messages(messages, _MAX_CHAT_TRANSCRIPT_CHARS)
        ]
        rounds = max(1, max_rounds)
        held = ""  # the latest answer called beside look-ups, held back while they run
        asked_shorter = False

        def reply(final: str, *, cut: bool = False, note: str = "") -> "ObjectGuidanceOutcome":
            """``final`` as the turn's reply, within the panel's cap and saying when cut."""
            markdown = final[:_MAX_NARRATIVE_CHARS]
            if cut or len(final) > _MAX_NARRATIVE_CHARS:
                markdown += _CUT_NOTE
            markdown += note
            on_delta(markdown)
            return ObjectGuidanceOutcome(
                available=True,
                reason="OK",
                detail="",
                markdown=markdown,
                model_id=self._config.model_id,
            )

        def ended(failure: "ObjectGuidanceOutcome") -> "ObjectGuidanceOutcome":
            """The turn ends without an answer: ``failure`` -- unless one was held back beside
            look-ups. It was received, so it is the reply rather than lost."""
            return reply(held, note=_HELD_NOTE) if held else failure

        try:
            for round_number in range(rounds):
                last = round_number == rounds - 1
                body = json.dumps(
                    {
                        "anthropic_version": _ANTHROPIC_VERSION,
                        "max_tokens": _CHAT_MAX_TOKENS,
                        "thinking": _THINKING_OFF,
                        "system": system + _RESPONSE_STYLE + _TOOL_CHAT_STYLE,
                        "tools": [*tools, _ANSWER_TOOL_SCHEMA],
                        # A tool call every round -- a look-up or the answer -- and on the
                        # last one the answer.
                        "tool_choice": (
                            {"type": "tool", "name": _ANSWER_TOOL}
                            if last
                            else {"type": "any"}
                        ),
                        "messages": convo,
                    }
                )
                response = self._get_client().invoke_model(
                    modelId=self._config.model_id,
                    body=body,
                    contentType="application/json",
                    accept="application/json",
                )
                raw = response.get("body") if isinstance(response, Mapping) else None
                payload = raw.read() if hasattr(raw, "read") else raw
                data = json.loads(payload) if payload else {}
                content = data.get("content") if isinstance(data, Mapping) else None
                content = content if isinstance(content, list) else []
                stop = data.get("stop_reason") if isinstance(data, Mapping) else None
                tool_uses = [
                    b
                    for b in content
                    if isinstance(b, Mapping) and b.get("type") == "tool_use"
                ]
                answers = [b for b in tool_uses if b.get("name") == _ANSWER_TOOL]
                look_ups = [b for b in tool_uses if b.get("name") != _ANSWER_TOOL]
                if stop == "tool_use" and look_ups and not (answers and last):
                    # Record the assistant's tool-call turn verbatim, then answer each call.
                    # An answer called beside look-ups was written before their results came
                    # back, so it is not the reply: the model answers again from them -- but
                    # on the last round, with no round left to read them, the answer is. It
                    # is held, too: if the rounds after it fail, it is the reply (ended).
                    convo.append({"role": "assistant", "content": content})
                    results: list[dict] = []
                    for block in tool_uses:
                        name = str(block.get("name", ""))
                        if name == _ANSWER_TOOL:
                            out = (
                                "Not shown to the operator: it was written before the "
                                "results of the look-ups called with it. Read them, then "
                                "call answer with the complete reply."
                            )
                        else:
                            inp = block.get("input")
                            inp = dict(inp) if isinstance(inp, Mapping) else {}
                            try:
                                out = execute(name, inp)
                            except Exception:  # noqa: BLE001 - a bad tool never breaks chat
                                out = "{\"error\": \"tool failed\"}"
                        results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.get("id"),
                                "content": bounded_tool_output(str(out)),
                            }
                        )
                    if round_number == rounds - 2:
                        # The next round can only answer: say so, or the answer to a paged
                        # listing read to the end of the budget presents part of it as all.
                        results.append(
                            {
                                "type": "text",
                                "text": (
                                    "Tool budget: these are the last tool results you get "
                                    "in this reply. Answer now from the results you have, "
                                    "and say what was not fetched (e.g. the remaining pages "
                                    "of a list; the Evaluation export lists every object)."
                                ),
                            }
                        )
                    convo.append({"role": "user", "content": results})
                    held = "\n\n".join(md for md in map(_answer_markdown, answers) if md) or held
                    continue
                # The reply: the answer's Markdown -- or, from a turn that called no tool at
                # all, its text.
                if answers:
                    final = "\n\n".join(md for md in map(_answer_markdown, answers) if md)
                else:
                    final = "".join(
                        b.get("text", "")
                        for b in content
                        if isinstance(b, Mapping)
                        and b.get("type") == "text"
                        and isinstance(b.get("text"), str)
                    ).strip()
                if answers and not look_ups and not final and stop == "max_tokens":
                    # The answer's Markdown is one tool parameter, and the API hands a
                    # parameter back only once it is complete: cut at the output limit, none
                    # of it arrived. Ask for a shorter one while a round is left -- once, as a
                    # cut round spends the whole output budget -- else say why there is none.
                    if last or asked_shorter:
                        return ended(
                            ObjectGuidanceOutcome(
                                available=False,
                                reason="INVALID_OUTPUT",
                                detail=(
                                    "The reply was longer than the assistant can write in "
                                    "one turn. Ask for one part at a time."
                                ),
                            )
                        )
                    asked_shorter = True
                    convo.append({"role": "assistant", "content": content})
                    convo.append(
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.get("id"),
                                    "content": (
                                        "Not shown: the reply was cut at the output limit "
                                        "before any of it arrived. Call answer again with a "
                                        "shorter reply -- the operator sees at most "
                                        f"{_MAX_NARRATIVE_CHARS:,} characters: give the first "
                                        "part and say how to ask for the rest."
                                    ),
                                }
                                for block in answers
                            ],
                        }
                    )
                    continue
                if not final:
                    err = AiAssistUnavailableError("INVALID_OUTPUT")
                    return ended(
                        ObjectGuidanceOutcome(
                            available=False, reason=err.reason, detail=err.detail
                        )
                    )
                return reply(final, cut=stop == "max_tokens")
            return ended(
                ObjectGuidanceOutcome(
                    available=False,
                    reason="UNAVAILABLE",
                    detail=(
                        "The assistant kept looking things up without answering. "
                        "Try rephrasing the question."
                    ),
                )
            )
        except AiAssistUnavailableError as error:
            return ended(
                ObjectGuidanceOutcome(
                    available=False, reason=error.reason, detail=error.detail
                )
            )
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            error = AiAssistUnavailableError(_classify_bedrock_error(exc))
            return ended(
                ObjectGuidanceOutcome(
                    available=False, reason=error.reason, detail=error.detail
                )
            )


@dataclass(frozen=True)
class ObjectGuidanceOutcome:
    """The outcome of one AI DBA chat turn; it never breaks the workflow.

    Every chat method returns it, whatever the scope. When :attr:`available` is
    ``True`` the :attr:`markdown` carries the reply and :attr:`model_id` its
    provenance; otherwise :attr:`detail` is a fixed, credential-free message
    explaining why AI was unavailable. ``reason`` is ``"OK"`` exactly when
    available.
    """

    available: bool
    reason: Literal[
        "OK",
        "ACCESS_DENIED",
        "MODEL_AGREEMENT_REQUIRED",
        "THROTTLED",
        "NETWORK",
        "UNAVAILABLE",
        "INVALID_OUTPUT",
    ]
    detail: str
    markdown: str = ""
    model_id: str = ""


__all__ = [
    "DSQL_CONSTRAINTS",
    "DSQL_QUERY_EFFICIENCY_RUBRIC",
    "AssessmentStrategist",
    "ObjectGuidanceOutcome",
    "build_object_chat_system",
    "build_general_chat_system",
    "build_validation_chat_system",
    "build_conversion_chat_system",
    "build_query_chat_system",
    "build_query_optimize_system",
    "build_full_load_error_chat_system",
    "build_connection_error_chat_system",
    "source_engine_word",
]
