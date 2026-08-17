# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI-led migration assessment that augments the deterministic assessment.

The :class:`AssessmentStrategist` uses Amazon Bedrock to act as a "senior
migration analyst": grounded on the already-extracted source inventory, the
deterministic :class:`~dsql_migrator.core.models.AssessmentReport` (the factual
backbone), the target DSQL catalog, and the known Aurora DSQL constraints, it
produces an :class:`~dsql_migrator.core.models.AiAssessmentReport` with an
overall migration strategy, per-object expert insights, and additional advisory
findings the static rules did not catch (Requirement 11).

Design guarantees (consistent with the existing AI-assist path):

- **Augment, never replace** (Property 6 / Requirement 11.1, 11.2): the
  deterministic assessment is authoritative for the hard DSQL facts (FK/PK/
  trigger/type/etc.) and Property 8 holds regardless of AI. This layer only
  adds strategy, narrative, and advisory findings; AI never changes a
  deterministic classification or effort.
- **Read-only** (Property 1 / Requirement 11.11): inputs are already-extracted
  in-memory models; no source/target connection is opened here.
- **Untrusted output** (Requirement 11.8): the model output is parsed
  defensively. Object insights that do not map to a real assessed object are
  dropped, an unrecognized AI effort becomes ``None``, and sizes are capped.
- **Graceful degradation** (Requirement 11.10): :meth:`AssessmentStrategist.try_generate`
  never raises; on any Bedrock failure or unparseable output it returns an
  unavailable :class:`AiAssessmentOutcome` so the deterministic report still
  stands alone.
- **Credential confidentiality** (Property 7): the Bedrock client uses IAM auth
  via the shared session/profile; no credential value is read or logged, and
  failure messages are fixed and credential-free.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

from dsql_migrator.core.ai_assistant import (
    _ANTHROPIC_VERSION,
    AiAssistUnavailableError,
    _build_invoke_body,
    _classify_bedrock_error,
    _extract_suggestion_text,
    build_bedrock_runtime_client,
)
from dsql_migrator.core.models import (
    AiAssessmentFinding,
    AiAssessmentInsight,
    AiAssessmentReport,
    AiAssistConfig,
    AssessmentItem,
    AssessmentReport,
    EffortLevel,
    SourceInventory,
    TargetInventory,
)

# A larger output budget than the per-object suggestion path: the strategist
# returns a whole structured report (strategy + insights + findings).
_ASSESSMENT_MAX_TOKENS = 4096

# Output budget for the on-demand, single-object guidance shown in the
# Evaluation drawer: one focused remediation write-up (why it matters + how to
# fix it for DSQL + example), so a tighter cap than the whole-report path.
_OBJECT_GUIDANCE_MAX_TOKENS = 1536

# Upper bound on the running multi-turn chat transcript (characters) sent to the
# model per turn. Bounds token cost / context growth on a long conversation: the
# most recent turns within this budget are kept (oldest dropped first), so the
# chat stays responsive and affordable without ending the conversation.
_MAX_CHAT_TRANSCRIPT_CHARS = 24000

# Upper bound on the in-process tool-use agentic loop (model round-trips per user
# turn). Each round may call one or more read-only tools; the cap stops a runaway
# call-loop and bounds cost. A handful is plenty for "look up X, then answer".
_MAX_TOOL_ROUNDS = 6

# Defensive caps on untrusted output so a single response cannot flood the UI.
_MAX_INSIGHTS = 200
_MAX_FINDINGS = 100
_MAX_TEXT_CHARS = 4000
# The AI-led assessment is a free-form Markdown narrative; cap it generously so
# a verbose model reply cannot flood the page (best practice: accept prose, not
# brittle JSON, and render it as Markdown).
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

# Aurora DSQL constraints used to ground the assessment prompt. Mirrors the
# constraints the deterministic assessor/converter apply so the AI analysis
# stays consistent with the factual backbone.
DSQL_CONSTRAINTS = (
    "Aurora DSQL constraints: foreign keys are unsupported; every table requires "
    "a primary key; secondary indexes are created asynchronously "
    "(CREATE INDEX ASYNC); the 'C' collation is used; triggers and stored "
    "procedures are unsupported; AUTO_INCREMENT/monotonic keys cause hot "
    "partitions; MySQL native partitioning is not used (DSQL distributes data "
    "automatically); spatial/geometry types have no lossless mapping; there are "
    "transaction limits (a single DDL statement per transaction)."
)

# Mapping from a model-provided effort token to the EffortLevel enum. Anything
# not in this map (or missing) becomes None (advisory effort unknown).
_EFFORT_BY_NAME: dict[str, EffortLevel] = {level.value: level for level in EffortLevel}

_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


@dataclass(frozen=True)
class AiAssessmentOutcome:
    """An AI assessment result that never breaks the Evaluation workflow.

    When :attr:`available` is ``True`` the :attr:`report` carries the AI-led
    assessment; when ``False`` the deterministic report stands alone and
    :attr:`detail` is a fixed, credential-free message explaining why AI was
    unavailable. ``reason`` is ``"OK"`` exactly when :attr:`available` is True.
    """

    available: bool
    reason: Literal[
        "OK", "ACCESS_DENIED", "THROTTLED", "NETWORK", "UNAVAILABLE", "INVALID_OUTPUT"
    ]
    detail: str
    report: Optional[AiAssessmentReport] = None

    @classmethod
    def ok(cls, report: AiAssessmentReport) -> "AiAssessmentOutcome":
        """Build a successful outcome carrying the AI assessment ``report``."""
        return cls(available=True, reason="OK", detail="", report=report)

    @classmethod
    def unavailable(cls, error: AiAssistUnavailableError) -> "AiAssessmentOutcome":
        """Build a failed outcome from a typed unavailability error."""
        return cls(available=False, reason=error.reason, detail=error.detail)


def _summarize_assessment(report: AssessmentReport) -> str:
    """Render the deterministic assessment as compact grounding text."""
    lines = []
    for item in report.items:
        effort = item.effort.value if item.effort is not None else "NONE"
        risk = item.risk or "-"
        lines.append(
            f"- {item.object_name} | {item.classification.value} | effort={effort} "
            f"| rule={item.rule_id} | risk={risk}"
        )
    return "\n".join(lines) if lines else "(no objects)"


def _summarize_target(target: TargetInventory, conflicts: list[str]) -> str:
    """Render the target catalog and any conflicts as grounding text."""
    tables = sum(len(schema.tables) for schema in target.schemas)
    views = sum(len(schema.views) for schema in target.schemas)
    conflict_text = ", ".join(conflicts) if conflicts else "(none)"
    return (
        f"Target catalog: {len(target.schemas)} schemas, {tables} tables, "
        f"{views} views. Source objects already present on target: {conflict_text}."
    )


def build_assessment_prompt(
    inventory: SourceInventory,
    assessment: AssessmentReport,
    target: TargetInventory,
    conflicts: list[str],
) -> str:
    """Build the grounded prompt that asks the model for a structured report.

    The prompt grounds the model on the deterministic assessment (the factual
    backbone it must respect), the source inventory shape, the target catalog,
    and the DSQL constraints, and asks for a single JSON object so the output
    can be parsed defensively (Requirement 11.8).
    """
    return (
        "You are a senior AWS database migration analyst. Analyze a MySQL -> "
        "Amazon Aurora DSQL (PostgreSQL-compatible) migration and write a "
        "concise, well-structured assessment in GitHub-flavored Markdown that "
        "AUGMENTS a deterministic, rule-based assessment. The deterministic "
        "findings below are authoritative facts you MUST respect and never "
        "contradict; add expert strategy, prioritization, and any risks the "
        "rules may have missed.\n\n"
        f"Source inventory: {len(inventory.tables)} tables, "
        f"{len(inventory.views)} views, {len(inventory.triggers)} triggers, "
        f"{len(inventory.routines)} routines, {len(inventory.events)} events.\n\n"
        f"{_summarize_target(target, conflicts)}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}\n\n"
        "Deterministic assessment (authoritative facts):\n"
        f"{_summarize_assessment(assessment)}\n\n"
        "Structure the Markdown with these sections:\n"
        "- `## Migration strategy` -- the overall approach and a prioritized "
        "plan.\n"
        "- `## Prioritized recommendations` -- for each flagged object (use the "
        "EXACT object names from the deterministic assessment above), the "
        "concrete remediation and an effort of Simple / Medium / Significant.\n"
        "- `## Additional risks` -- risks or pitfalls the rules may have "
        "missed.\n\n"
        "Be concise and actionable. Only reference object names that appear in "
        "the deterministic assessment. Output the Markdown report only, with no "
        "preamble or sign-off."
    )


def build_object_guidance_prompt(item: AssessmentItem) -> str:
    """Build the prompt for on-demand, single-object remediation guidance.

    Grounds the model on ONE deterministic :class:`AssessmentItem` (its name,
    kind, classification, effort, rule, risk) and the DSQL constraints, and asks
    for a natural, conversational answer (as in a chat with a helpful engineer)
    rather than a fixed-section template. The deterministic classification/effort
    stay authoritative; this only explains and remediates in the model's own
    words.
    """
    effort = item.effort.value if item.effort is not None else "NONE"
    return (
        "You are a senior AWS database migration engineer chatting with a "
        "teammate who is migrating a MySQL database to Amazon Aurora DSQL "
        "(PostgreSQL-compatible). A deterministic, rule-based assessment flagged "
        "the object below. Answer naturally and conversationally, the way you "
        "would in a chat — explain it in your own words, in a friendly, helpful "
        "tone. Do NOT use a rigid template or fixed section headings (no "
        "\"Why this needs attention\" / \"How to fix it\" / \"Example\" "
        "boilerplate). Just talk it through: briefly say what the issue is and "
        "why it matters on DSQL, then how you'd handle it, weaving in a short "
        "code snippet only if it genuinely helps. The deterministic facts below "
        "are authoritative — build on them, never contradict them.\n\n"
        f"Object: {item.object_name}\n"
        f"Kind: {item.kind}\n"
        f"Classification: {item.classification.value}\n"
        f"Estimated manual effort: {effort}\n"
        f"Rule: {item.rule_id}\n"
        f"Risk (deterministic): {item.risk or '(none recorded)'}\n"
        f"Recommendation (deterministic): {item.recommendation or '(none)'}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}\n\n"
        "Keep it specific to this object and reasonably concise (a few short "
        "paragraphs). Light GitHub-flavored Markdown is fine (a bit of emphasis, "
        "an occasional fenced code block or short list) but keep it reading like "
        "a natural reply, not a form. Don't restate these instructions; just give "
        "the answer."
    )


def _coerce_text(value: Any) -> str:
    """Return a capped string for an untrusted model value (or empty)."""
    if not isinstance(value, str):
        return ""
    return value.strip()[:_MAX_TEXT_CHARS]


def build_object_chat_system(item: AssessmentItem) -> str:
    """Build the system grounding for a multi-turn chat about ONE object.

    Unlike :func:`build_object_guidance_prompt` (a single-shot user prompt), this
    is the persistent system context for a back-and-forth conversation: it pins
    the model to ONE deterministic :class:`AssessmentItem` and the DSQL
    constraints, sets a natural, conversational tone, and tells it the
    deterministic facts are authoritative. The turns themselves (the initial
    question and any follow-ups) are supplied as the chat ``messages``.
    """
    effort = item.effort.value if item.effort is not None else "NONE"
    return (
        "You are a senior AWS database migration engineer chatting with a "
        "teammate who is migrating a MySQL database to Amazon Aurora DSQL "
        "(PostgreSQL-compatible). Answer naturally and conversationally, the way "
        "you would in a chat — explain things in your own words, in a friendly, "
        "helpful tone, and answer follow-up questions in context. Do NOT use a "
        "rigid template or fixed section headings. Light GitHub-flavored Markdown "
        "is fine (a little emphasis, an occasional short list or fenced code "
        "block) but keep it reading like a natural reply, not a form. Be specific "
        "and reasonably concise.\n\n"
        "Your focus is migrating THIS object to Aurora DSQL, but you are also this "
        "migration's assistant. If the user asks about the WIDER migration -- other "
        "objects, the assessment as a whole, converted DDL, validation results, load "
        "status -- help them: USE ANY TOOLS you have to look up the real data and "
        "answer, rather than refusing just because it is not this object. Only decline "
        "questions that are genuinely off-topic for a MySQL -> Aurora DSQL migration "
        "(unrelated technologies, general chit-chat), politely, in one sentence.\n\n"
        "The conversation is about this single object, which a deterministic, "
        "rule-based assessment flagged. These facts are authoritative — build on "
        "them and never contradict them:\n"
        f"- Object: {item.object_name}\n"
        f"- Kind: {item.kind}\n"
        f"- Classification: {item.classification.value}\n"
        f"- Estimated manual effort: {effort}\n"
        f"- Rule: {item.rule_id}\n"
        f"- Risk: {item.risk or '(none recorded)'}\n"
        f"- Recommendation: {item.recommendation or '(none)'}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


def build_general_chat_system(
    *, current_step: str = "", migration_type: str = "", summary: str = ""
) -> str:
    """System grounding for the GENERAL (no specific object) assistant panel.

    Opened from the header, the persistent panel lets the user ask about the
    migration as a whole. This pins it to a MySQL -> Amazon Aurora DSQL migration
    assistant: it answers questions about THIS migration (schema conversion, data
    migration / CDC, validation, cut over, DSQL behavior and constraints) and the
    current progress -- grounded on the credential-free
    :class:`~dsql_migrator.core.models.MigrationContext` -- and DECLINES anything
    off-topic, using the SAME guardrail wording as the per-object chats. No
    credentials or row data ever enter the prompt (Property 7).
    """
    where = []
    if current_step:
        where.append(f"- Current step: {current_step}")
    if migration_type:
        where.append(f"- Migration type: {migration_type}")
    if summary:
        where.append(f"- Notes: {summary}")
    context_block = "\n".join(where) or "- (no step context captured yet)"
    return (
        "You are a senior AWS database migration engineer chatting with a teammate "
        "who is migrating a MySQL database to Amazon Aurora DSQL "
        "(PostgreSQL-compatible) using this tool. Answer naturally and "
        "conversationally; light GitHub-flavored Markdown is fine, but keep it "
        "reading like a natural reply, not a form. Be specific and reasonably "
        "concise.\n\n"
        "Stay strictly on the topic of THIS MySQL -> Aurora DSQL migration: schema "
        "conversion, data migration (Full Load / CDC), validation, cut over, and "
        "Aurora DSQL behavior/constraints, plus the current progress. If the user "
        "asks about anything off-topic — unrelated technologies, other systems, "
        "general chit-chat, or non-migration questions — politely decline in one "
        "sentence and steer back to the migration.\n\n"
        "Where the user is in the tool (deterministic, authoritative — never "
        "contradict it):\n"
        f"{context_block}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


def build_conversion_chat_system(
    object_name: str, source_ddl: str, deterministic_ddl: str
) -> str:
    """Build the system grounding for a chat about converting ONE object's DDL.

    The schema-conversion counterpart to :func:`build_object_chat_system`: it
    grounds the model on the object's source MySQL DDL and the tool's
    deterministic DSQL conversion, sets a natural, conversational tone, and keeps
    the chat scoped to converting THIS object for Aurora DSQL (declining
    off-topic questions). The turns (the initial question and follow-ups) are
    supplied as the chat ``messages``.
    """
    source = (source_ddl or "(unavailable)").strip()[:_MAX_TEXT_CHARS]
    target = (deterministic_ddl or "(none)").strip()[:_MAX_TEXT_CHARS]
    return (
        "You are a senior AWS database migration engineer chatting with a "
        "teammate who is converting a MySQL schema object to Amazon Aurora DSQL "
        "(PostgreSQL-compatible). Answer naturally and conversationally, the way "
        "you would in a chat — explain things in your own words, in a friendly, "
        "helpful tone, and answer follow-up questions in context. Do NOT use a "
        "rigid template or fixed section headings. When you propose DDL/SQL, put "
        "it in a fenced ```sql code block so it is easy to copy. Light "
        "GitHub-flavored Markdown is fine but keep it reading like a natural "
        "reply, not a form. Be specific and reasonably concise.\n\n"
        "Stay strictly on the topic of converting THIS object to Aurora DSQL and "
        "directly related concerns (its DSQL incompatibilities, the equivalent "
        "DDL, indexes, keys, types, and app-level follow-ups for this object). If "
        "the user asks about anything off-topic, politely decline in one sentence "
        "and steer back to converting this object.\n\n"
        f"Object: {object_name}\n\n"
        "Source DDL (MySQL):\n"
        f"```sql\n{source}\n```\n\n"
        "The tool's deterministic Aurora DSQL conversion (authoritative starting "
        "point — build on it, don't contradict it):\n"
        f"```sql\n{target}\n```\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


def build_query_chat_system(
    original_sql: str,
    converted_sql: str,
    *,
    target_error: Optional[str] = None,
) -> str:
    """Build the system grounding for a chat about converting/fixing ONE query.

    The Query Playground counterpart to :func:`build_conversion_chat_system`: it
    grounds the model on the user's original MySQL statement, the tool's
    deterministic Aurora DSQL conversion, and -- when the converted statement was
    tested and the target rejected it -- the exact Aurora DSQL error, so the model
    explains the conversion and fixes the real failure. Same natural,
    conversational tone, fenced ```sql for runnable SQL, and a scope guard that
    keeps the chat on THIS query.
    """
    source = (original_sql or "(unavailable)").strip()[:_MAX_TEXT_CHARS]
    target = (converted_sql or "(could not be converted)").strip()[:_MAX_TEXT_CHARS]
    error_block = ""
    if target_error:
        error_block = (
            "\n\nWhen the converted statement was tested on the target, Aurora DSQL "
            "rejected it with this error (fix the statement so it runs):\n"
            f"{target_error.strip()[:_MAX_TEXT_CHARS]}\n"
        )
    return (
        "You are a senior AWS database migration engineer chatting with a teammate "
        "who is converting a MySQL query to Amazon Aurora DSQL "
        "(PostgreSQL-compatible). Answer naturally and conversationally, the way "
        "you would in a chat — explain things in your own words, in a friendly, "
        "helpful tone, and answer follow-up questions in context. Do NOT use a "
        "rigid template or fixed section headings. When you propose SQL, put it in "
        "a fenced ```sql code block so it is easy to copy. Light GitHub-flavored "
        "Markdown is fine but keep it reading like a natural reply, not a form. Be "
        "specific and reasonably concise.\n\n"
        "Stay strictly on the topic of converting/running THIS query on Aurora DSQL "
        "and directly related concerns (its DSQL incompatibilities, the equivalent "
        "PostgreSQL SQL, functions, the FOR UPDATE / lock rules, and how to make it "
        "run). If the user asks about anything off-topic, politely decline in one "
        "sentence and steer back to this query.\n\n"
        "Original query (MySQL):\n"
        f"```sql\n{source}\n```\n\n"
        "The tool's deterministic Aurora DSQL conversion (starting point — build on "
        "it, don't contradict it unless it is what the target rejected):\n"
        f"```sql\n{target}\n```"
        f"{error_block}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


# Aurora DSQL query-efficiency guidance. DSQL is a DISTRIBUTED, PostgreSQL-
# compatible engine, so generic PostgreSQL tuning lore is frequently wrong here.
# This rubric grounds the "rewrite for efficiency" chat on how DSQL actually
# executes queries (AWS Database Blog "Improve query performance with EXPLAIN
# plans in Amazon Aurora DSQL") so the model reasons about DSQL, not vanilla PG.
DSQL_QUERY_EFFICIENCY_RUBRIC = (
    "How Aurora DSQL runs queries (this is DISTINCT from single-node PostgreSQL — "
    "do NOT apply generic PostgreSQL tuning lore):\n"
    "- There is NO heap: every table IS a B-tree organized by its primary key, so "
    "the PK is a fully covering index. A table with no usable index for the "
    "predicate is read with a 'Full Scan' (not a 'Seq Scan'). Range/equality "
    "filters on the primary key are physically sequential and inherently cheap.\n"
    "- Compute and storage are PHYSICALLY SEPARATED, so every row that crosses "
    "from storage to compute costs latency and DPU (Distributed Processing Unit — "
    "DSQL's real cost unit, shown as the 'Statement DPU Estimate' in EXPLAIN "
    "ANALYZE VERBOSE; ignore raw PostgreSQL cost= numbers as the goal). The single "
    "biggest lever is PUSHING FILTERS DOWN so fewer bytes move.\n"
    "- Three filter layers, best to worst: (1) Index Condition — equality/range on "
    "indexed key columns (put the most selective column leftmost in a composite "
    "index); (2) Storage Filter — add non-key filter columns to an index INCLUDE "
    "clause so storage filters before transfer; (3) Query Processor Filter — shows "
    "as a top-level 'Filter:' line, all unfiltered data already crossed the "
    "network (worst). Move predicates from layer 3 → 2 → 1.\n"
    "- Scan types, cheapest last: Full Scan (add a PK or an index on the selective "
    "column) → Index Scan with a 'Storage Lookup' node (an incomplete covering "
    "index — add the missing SELECT/WHERE columns to INCLUDE) → Index Only Scan "
    "(ideal).\n"
    "Efficient-query patterns that are specific to DSQL:\n"
    "- Replace SELECT * with an explicit column list (every projected column is "
    "fetched across the network).\n"
    "- A leading-wildcard LIKE ('%x%') cannot use an index condition; rewrite as a "
    "prefix match, or add a more selective indexed predicate.\n"
    "- In multi-table joins where a filter on one table logically applies to "
    "another through a business relationship the optimizer can't infer, add a "
    "REDUNDANT join predicate (one at a time) so it can use an index instead of a "
    "Full Scan.\n"
    "- For filter + ORDER BY + LIMIT that isn't fully index-covered, use CTE late "
    "materialization: narrow to the final rows using only indexed columns first, "
    "then join back to the base table for the remaining columns.\n"
    "- Prefer randomly-distributed keys (e.g. UUID) over monotonic ones "
    "(AUTO_INCREMENT, timestamps) which create hot partitions; secondary indexes "
    "are built with CREATE INDEX ASYNC.\n"
    "Do NOT suggest vanilla-PostgreSQL tactics that do not apply to DSQL: no "
    "VACUUM/ANALYZE-as-tuning (DSQL auto-analyzes), no REINDEX/CLUSTER, no "
    "fillfactor/HOT-update/bloat/autovacuum knobs, no planner GUCs, and do not "
    "frame success as lowering the PostgreSQL cost= number. Frame efficiency as "
    "fewer bytes crossing storage→compute (lower DPU)."
)


def build_query_optimize_system(
    original_sql: str,
    converted_sql: str,
    *,
    plan: Optional[str] = None,
    dpu_total: Optional[float] = None,
    analyzed: bool = False,
) -> str:
    """Build the system grounding for the "rewrite this query for efficiency" chat.

    A sibling of :func:`build_query_chat_system` for the Query Playground's second
    AI action. It grounds the model on the deterministic converted SQL, the same
    :data:`DSQL_CONSTRAINTS`, the Aurora-DSQL-specific efficiency rubric
    (:data:`DSQL_QUERY_EFFICIENCY_RUBRIC`, which BANS vanilla-PostgreSQL tuning
    lore), and -- when a "Test on target" probe captured them -- the REAL EXPLAIN
    plan text and the measured DPU total, so the advice is grounded in this query's
    actual DSQL plan rather than priors. Advisory only: the model proposes a
    rewrite for the operator to test, it is never auto-applied.
    """
    source = (original_sql or "(unavailable)").strip()[:_MAX_TEXT_CHARS]
    target = (converted_sql or "(could not be converted)").strip()[:_MAX_TEXT_CHARS]
    if plan:
        kind = "EXPLAIN ANALYZE (actually executed)" if analyzed else "EXPLAIN (planned, not executed)"
        dpu_line = (
            f"\nMeasured Aurora DSQL cost for the current query: {dpu_total:.5f} DPU total.\n"
            if dpu_total is not None
            else "\n"
        )
        plan_block = (
            f"\n\nThe current converted query's Aurora DSQL query plan "
            f"({kind}) — reason from THIS plan (scan types, Storage Lookup nodes, "
            f"top-level Filter: lines), not from priors:\n"
            f"```\n{plan.strip()[:_MAX_TEXT_CHARS]}\n```"
            f"{dpu_line}"
        )
    else:
        plan_block = (
            "\n\nNo query plan was captured yet. Suggest running \"Test on target\" "
            "with the EXPLAIN ANALYZE toggle on to get the real plan + DPU cost, and "
            "base concrete advice on that; do not invent plan details or a DPU number."
        )
    return (
        "You are a senior AWS database engineer helping a teammate make a converted "
        "query run efficiently on Amazon Aurora DSQL (a distributed, "
        "PostgreSQL-compatible database). Answer naturally and conversationally — "
        "explain in your own words, friendly and specific, not a rigid template. "
        "When you propose a rewritten query, put it in a fenced ```sql code block so "
        "it is easy to copy, and ALWAYS explain in detail WHAT you changed and WHY "
        "it is cheaper on DSQL (which scan type / filter layer it improves, and why "
        "fewer bytes cross from storage to compute / lower DPU). The rewrite MUST "
        "return the SAME results as the original — never change its semantics to "
        "make it faster; if a real speedup needs an index or schema change you "
        "cannot express in the query alone, say so explicitly.\n\n"
        "Stay strictly on making THIS query efficient on Aurora DSQL. If asked "
        "anything off-topic, decline in one sentence and steer back.\n\n"
        "Original query (MySQL):\n"
        f"```sql\n{source}\n```\n\n"
        "The tool's deterministic Aurora DSQL conversion (the query to optimize — "
        "keep its results identical):\n"
        f"```sql\n{target}\n```"
        f"{plan_block}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}\n\n"
        f"{DSQL_QUERY_EFFICIENCY_RUBRIC}"
    )


# How a migrated MySQL->DSQL pipeline can diverge, and what fixes it. Grounds the
# validation chat so the model reasons about THIS tool's Full Load + CDC model
# (not generic replication) and recommends the right recovery.
_VALIDATION_RECOVERY_CONTEXT = (
    "How this migration works (use it to reason about WHY a table diverges and "
    "WHAT fixes it):\n"
    "- Full Load is a one-shot bulk copy (the tool's own loader). CDC then streams "
    "ongoing changes (Debezium MySQL -> Kafka -> a custom DSQL sink) from a binlog "
    "watermark captured at the snapshot, for a gapless hand-off.\n"
    "- CDC only carries changes GOING FORWARD from the connector's offset. A row "
    "that was lost earlier (e.g. a past sink incident, or a Full Load<->CDC "
    "hand-off gap) is NOT re-delivered by CDC, so the difference does not shrink "
    "over time — it is a standing gap, not lag.\n"
    "- Equal-ish counts with the SAME max primary key on both sides, not shrinking "
    "over time, point to a standing gap (missing middle rows), not replication lag. "
    "Counts that ARE shrinking, or a target whose max PK trails the source, point "
    "to lag (CDC still catching up).\n"
    "- The idempotent loader makes a re-run safe: re-running Full Load (then CDC) "
    "backfills missing rows without creating duplicates (INSERT ... ON CONFLICT). "
    "This is the recommended fix for a standing gap.\n"
    "- For a DEFINITIVE zero-loss cut-over check, source writes should be quiesced "
    "(or CDC confirmed caught up) first, because a live stream makes the target a "
    "moving target. Stopping CDC is done from the previous (Data Migration) step.\n"
    "- A 'target has MORE rows than source' (extra rows) usually means a source "
    "delete CDC has not applied, or rows that predate the migration."
)


def build_validation_chat_system(facts: str, *, scope: str = "table") -> str:
    """Build the system grounding for a chat about a validation MISMATCH.

    ``facts`` is a pre-formatted, credential-free block the UI assembles from the
    deterministic validation result (row counts, count/checksum match, a
    missing/extra SUMMARY -- counts and PK ranges, never full row data -- and
    drift / CDC-active signals). ``scope`` is ``"table"`` for a single failing
    table or ``"run"`` for the whole report; it only tunes the framing sentence.
    The model is told these facts are authoritative and is pointed at this tool's
    Full Load + CDC recovery model, so its advice (re-run to backfill a standing
    gap, or quiesce/stop CDC before a cut-over check) matches what the tool can
    actually do. It must stay on the topic of explaining/fixing THIS mismatch.
    """
    subject = (
        "one table that did NOT match"
        if scope == "table"
        else "a validation run with mismatches"
    )
    return (
        "You are a senior AWS database migration engineer chatting with a teammate "
        "who just validated a MySQL -> Amazon Aurora DSQL migration and is looking "
        f"at {subject}. Answer naturally and conversationally — explain WHY it "
        "likely diverged and exactly HOW to fix it, in a friendly, helpful tone, "
        "answering follow-ups in context. Do NOT use a rigid template. Light "
        "GitHub-flavored Markdown is fine (a short list, a little emphasis, a fenced "
        "code block for any SQL/commands) but keep it reading like a natural reply. "
        "Be specific and concise, and give a concrete next action.\n\n"
        "Stay strictly on the topic of this validation mismatch and how to resolve "
        "it (root cause, whether it is lag vs a standing gap vs extra rows, and the "
        "recovery steps). If asked anything off-topic, politely decline in one "
        "sentence and steer back.\n\n"
        "These deterministic validation facts are authoritative — build on them and "
        "never contradict them:\n"
        f"{facts.strip()[:_MAX_TEXT_CHARS]}\n\n"
        f"{_VALIDATION_RECOVERY_CONTEXT}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


_FULL_LOAD_RECOVERY_CONTEXT = (
    "How this tool's Full Load works and how to recover a failed table:\n"
    "- Full Load is the tool's OWN Python bulk loader (not a Debezium snapshot): it "
    "streams source rows by primary-key keyset pagination and writes them to the "
    "target with idempotent, batched INSERT ... ON CONFLICT (<= 3000 rows/txn), "
    "each batch wrapped in optimistic-concurrency (SQLSTATE 40001) retry.\n"
    "- Because the load is idempotent and resumable, RE-RUNNING a failed table is "
    "safe: use the per-table 'Reload' button (re-runs Full Load for just that "
    "table) or 'Retry failed tables' -- no duplicates are created.\n"
    "- A table may also fail during the SCHEMA step (creating/replacing the target "
    "table's DDL) rather than the row copy. A 'DependentObjectsStillExist ... "
    "cannot drop table ... because ... view ... depends on it ... Use DROP ... "
    "CASCADE' error means the tool tried to drop/replace a target table that a "
    "VIEW (or other object) still depends on; DSQL blocks the drop. The fix is to "
    "drop the dependent object(s) first (or recreate them after), or re-run once "
    "the dependency order is resolved -- the tool applies one object per "
    "transaction, so a view that depends on several tables must be handled around "
    "those tables.\n"
    "- A transient error like 'InternalError_: server unavailable', a timeout, or a "
    "connection reset is usually not a data problem: just Reload that table (or "
    "Retry failed tables). If it persists, check target reachability / IAM token "
    "expiry and source read headroom.\n"
    "- 'quarantined row' entries are NOT failures: the table loaded and specific "
    "rows were skipped (e.g. a value over DSQL's ~1 MiB per-value limit).\n"
    "- Distinguish a SCHEMA/DDL cause (fix dependency or DDL, then reload) from a "
    "DATA cause (fix the source value, then reload) from a TRANSIENT cause (just "
    "reload). Point the user at the right one."
)


def build_full_load_error_chat_system(
    table_name: str, error_message: str, *, migration_context: str = ""
) -> str:
    """Build the system grounding for a chat about a FAILED Full Load table.

    ``table_name`` is the (schema-qualified) table that failed and
    ``error_message`` is the tool's captured, credential-free failure text (e.g. a
    ``DependentObjectsStillExist`` drop conflict, an ``InternalError_: server
    unavailable`` transient, or a per-row data error). ``migration_context`` is an
    optional pre-formatted, credential-free block describing the CURRENT migration
    situation (e.g. migration type Full-Load-only vs combined with CDC, whether
    this table was a DROP+recreate of an existing target, whether CDC is already
    streaming, how many tables were selected) so the reply is specific to THIS
    migration, not generic. The model is told these facts are authoritative and is
    pointed at this tool's Full Load recovery model (per-table Reload / Retry
    failed tables, schema-vs-data-vs-transient triage, DSQL constraints) so its
    advice matches what the tool can actually do. It must stay on the topic of
    explaining and fixing THIS table's failure.
    """
    facts = f"Failed table: {table_name}\nError message:\n{error_message.strip()}"
    if migration_context.strip():
        facts += f"\n\nCurrent migration context:\n{migration_context.strip()}"
    return (
        "You are a senior AWS database migration engineer chatting with a teammate "
        "who is running a MySQL -> Amazon Aurora DSQL migration and just had ONE "
        "table fail during Full Load. You understand this exact migration's "
        "situation from the context below — use it so your answer is specific to "
        "THIS migration, not generic advice. Answer naturally and conversationally "
        "— explain WHY this specific error likely happened and exactly HOW to fix "
        "it, in a friendly, helpful tone, answering follow-ups in context. Do NOT "
        "use a rigid template. Light GitHub-flavored Markdown is fine (a short "
        "list, a little emphasis, a fenced code block for any SQL/commands) but "
        "keep it reading like a natural reply. Be specific and concise, and give a "
        "concrete next action (which button to click, or what to change).\n\n"
        "Stay strictly on the topic of this table's Full Load failure and how to "
        "resolve it (root cause, whether it is a schema/DDL issue vs a data issue "
        "vs a transient error, and the recovery steps). If asked anything "
        "off-topic, politely decline in one sentence and steer back.\n\n"
        "These deterministic facts are authoritative — build on them and never "
        "contradict them:\n"
        f"{facts[:_MAX_TEXT_CHARS]}\n\n"
        f"{_FULL_LOAD_RECOVERY_CONTEXT}\n\n"
        f"Aurora DSQL constraints:\n{DSQL_CONSTRAINTS}"
    )


def build_connection_error_chat_system(
    *, side: str, coordinates: str, error_message: str
) -> str:
    """System grounding for a chat about a FAILED connection test (Connect screen).

    ``side`` is ``"source"`` (Amazon RDS/Aurora MySQL) or ``"target"`` (Amazon Aurora
    DSQL, IAM-token auth). ``coordinates`` is a pre-formatted, credential-free line of
    the NON-secret connection coordinates (host/port/db, or DSQL endpoint/region/db/
    role -- never a password or IAM token, Property 7); ``error_message`` is the tool's
    captured, credential-free failure detail. Steers the model to diagnose THIS SPECIFIC
    attempt from the entered values first (so it catches a typo/non-default value like a
    role ``admi`` -> ``admin``) and to answer briefly, rather than reciting a generic
    connection-troubleshooting checklist.
    """
    is_source = side == "source"
    engine = (
        "the SOURCE database (Amazon RDS/Aurora MySQL, username/password or Secrets "
        "Manager auth)"
        if is_source
        else "the TARGET database (Amazon Aurora DSQL, PostgreSQL-compatible, "
        "short-lived IAM-token auth -- there is NO password)"
    )
    # Reference the model CHECKS the entered values against -- deliberately NOT phrased
    # as a checklist of causes to recite (that produced generic, verbose answers).
    reference = (
        "Reference for spotting a bad value: a MySQL host is normally an RDS/Aurora "
        "endpoint like <name>.<hash>.<region>.rds.amazonaws.com and the port is 3306."
        if is_source
        else "Reference for spotting a bad value: Aurora DSQL uses IAM-token auth (no "
        "password); its default database is 'postgres' and its default role/username "
        "is 'admin'; the endpoint looks like <id>.dsql.<region>.on.aws and the region "
        "must match the cluster's; connecting requires dsql:DbConnect (or "
        "dsql:DbConnectAdmin) on the cluster."
    )
    facts = (
        f"Entered connection values:\n{coordinates}\n"
        f"Error message:\n{error_message.strip()}"
    )
    return (
        "You are a senior AWS database migration engineer helping a teammate whose "
        f"connection test to {engine} just FAILED on the Connect screen of a MySQL -> "
        "Amazon Aurora DSQL migration tool.\n\n"
        "Diagnose THIS SPECIFIC attempt, not connections in general. FIRST inspect the "
        "exact entered values and the error text below for a concrete mistake IN THEM "
        "— a misspelled or non-default value (a role/username, database, region, or "
        "endpoint that does not match the expected form), a wrong port, a region "
        "mismatch. If a value looks off, say so directly: quote the suspect value and "
        "the likely intended one (e.g. role 'admi' -> 'admin'). Only if nothing in the "
        "values looks wrong should you consider environmental causes (network / "
        "security-group reachability, IAM permissions, the cluster not existing).\n\n"
        "Be brief and specific: lead with the single MOST LIKELY cause and its exact "
        "fix (one short paragraph or a few bullets), then stop. Do NOT print a generic "
        "connection-troubleshooting checklist. A fenced code block for a corrected "
        "value/command is fine.\n\n"
        "Stay strictly on resolving THIS connection failure; if asked anything "
        "off-topic, decline in one sentence and steer back.\n\n"
        f"{reference}\n\n"
        "These deterministic facts are authoritative — build on them and never "
        "contradict them:\n"
        f"{facts[:_MAX_TEXT_CHARS]}"
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


def _coerce_effort(value: Any) -> Optional[EffortLevel]:
    """Map an untrusted effort token to an EffortLevel, or None."""
    if not isinstance(value, str):
        return None
    return _EFFORT_BY_NAME.get(value.strip().upper())


def parse_assessment_output(
    text: str, *, model_id: str, valid_object_names: set[str]
) -> AiAssessmentReport:
    """Parse untrusted model output into an :class:`AiAssessmentReport`.

    Tolerates code fences and extra prose; insights whose ``object_name`` is not
    a real assessed object are dropped (so AI cannot invent objects), efforts are
    validated against :class:`EffortLevel`, and list/text sizes are capped. A
    body that cannot yield a JSON object raises
    :class:`AiAssistUnavailableError` (``INVALID_OUTPUT``) so the caller degrades
    gracefully.
    """
    stripped = _JSON_FENCE.sub("", text.strip())
    try:
        data = json.loads(stripped)
    except (ValueError, json.JSONDecodeError):
        # Try to recover the first JSON object embedded in the text.
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise AiAssistUnavailableError("INVALID_OUTPUT") from None
        try:
            data = json.loads(stripped[start : end + 1])
        except (ValueError, json.JSONDecodeError):
            raise AiAssistUnavailableError("INVALID_OUTPUT") from None

    if not isinstance(data, dict):
        raise AiAssistUnavailableError("INVALID_OUTPUT")

    insights: list[AiAssessmentInsight] = []
    raw_insights = data.get("insights")
    if isinstance(raw_insights, list):
        for entry in raw_insights[:_MAX_INSIGHTS]:
            if not isinstance(entry, dict):
                continue
            name = _coerce_text(entry.get("object_name"))
            if not name or name not in valid_object_names:
                continue
            insights.append(
                AiAssessmentInsight(
                    object_name=name,
                    recommendation=_coerce_text(entry.get("recommendation")),
                    rationale=_coerce_text(entry.get("rationale")),
                    ai_effort=_coerce_effort(entry.get("effort")),
                )
            )

    findings: list[AiAssessmentFinding] = []
    raw_findings = data.get("additional_findings")
    if isinstance(raw_findings, list):
        for entry in raw_findings[:_MAX_FINDINGS]:
            if not isinstance(entry, dict):
                continue
            area = _coerce_text(entry.get("area"))
            if not area:
                continue
            findings.append(
                AiAssessmentFinding(
                    area=area,
                    risk=_coerce_text(entry.get("risk")),
                    recommendation=_coerce_text(entry.get("recommendation")),
                )
            )

    return AiAssessmentReport(
        strategy_summary=_coerce_text(data.get("strategy_summary")),
        insights=insights,
        additional_findings=findings,
        model_id=model_id,
    )


class AssessmentStrategist:
    """Generates an AI-led migration assessment from grounded inputs (Req 11).

    Augments — never replaces — the deterministic assessment. The Bedrock client
    is injectable (tests pass a fake) and otherwise built lazily from the shared
    session honoring the optional global AWS profile, so constructing the
    strategist performs no network call (Property 7 / Requirement 9.5).
    """

    def __init__(
        self,
        config: AiAssistConfig,
        *,
        aws_profile: Optional[str] = None,
        client: Optional[Any] = None,
    ) -> None:
        self._config = config
        self._aws_profile = aws_profile
        self._client = client

    def _get_client(self) -> Any:
        """Return the bedrock-runtime client, building it lazily if needed."""
        if self._client is None:
            self._client = build_bedrock_runtime_client(
                self._config, aws_profile=self._aws_profile
            )
        return self._client

    def generate(
        self,
        inventory: SourceInventory,
        assessment: AssessmentReport,
        target: TargetInventory,
        conflicts: list[str],
    ) -> AiAssessmentReport:
        """Produce the AI assessment, raising on Bedrock failure/invalid output.

        The prompt is grounded on the deterministic facts; the response is parsed
        defensively (Requirement 11.8). Any boto/Bedrock failure is mapped to a
        typed :class:`AiAssistUnavailableError` (credential-free), and empty or
        unparseable output becomes ``INVALID_OUTPUT``.
        """
        prompt = build_assessment_prompt(inventory, assessment, target, conflicts)
        try:
            response = self._get_client().invoke_model(
                modelId=self._config.model_id,
                body=_build_invoke_body(prompt, max_tokens=_ASSESSMENT_MAX_TOKENS),
                contentType="application/json",
                accept="application/json",
            )
        except AiAssistUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            raise AiAssistUnavailableError(_classify_bedrock_error(exc)) from None

        # Best practice: accept a free-form Markdown narrative rather than forcing
        # brittle JSON. Any non-empty reply is valid and is rendered as Markdown;
        # only an empty/unreadable body is treated as INVALID_OUTPUT. The narrative
        # is capped so a single response cannot flood the UI.
        text = _extract_suggestion_text(response).strip()
        if not text:
            raise AiAssistUnavailableError("INVALID_OUTPUT")
        return AiAssessmentReport(
            strategy_summary=text[:_MAX_NARRATIVE_CHARS],
            model_id=self._config.model_id,
        )

    def try_generate(
        self,
        inventory: SourceInventory,
        assessment: AssessmentReport,
        target: TargetInventory,
        conflicts: list[str],
    ) -> AiAssessmentOutcome:
        """Graceful-degradation wrapper around :meth:`generate` (Req 11.10).

        Never raises: on any Bedrock failure or unparseable output it returns an
        unavailable :class:`AiAssessmentOutcome`, so the deterministic assessment
        still stands alone.
        """
        try:
            report = self.generate(inventory, assessment, target, conflicts)
        except AiAssistUnavailableError as error:
            return AiAssessmentOutcome.unavailable(error)
        return AiAssessmentOutcome.ok(report)

    def generate_object_guidance(self, item: AssessmentItem) -> str:
        """Produce on-demand Markdown guidance for ONE assessed object.

        Grounds the prompt on the single deterministic :class:`AssessmentItem`
        and the DSQL constraints, calls bedrock-runtime ``InvokeModel`` via the
        injected/lazy client, and returns the model's Markdown narrative (capped).
        Any Bedrock failure is mapped to a typed, credential-free
        :class:`AiAssistUnavailableError`; empty output becomes
        ``INVALID_OUTPUT`` (Requirements 11.8, 11.10).
        """
        prompt = build_object_guidance_prompt(item)
        try:
            response = self._get_client().invoke_model(
                modelId=self._config.model_id,
                body=_build_invoke_body(
                    prompt, max_tokens=_OBJECT_GUIDANCE_MAX_TOKENS
                ),
                contentType="application/json",
                accept="application/json",
            )
        except AiAssistUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            raise AiAssistUnavailableError(_classify_bedrock_error(exc)) from None

        text = _extract_suggestion_text(response).strip()
        if not text:
            raise AiAssistUnavailableError("INVALID_OUTPUT")
        return text[:_MAX_NARRATIVE_CHARS]

    def try_generate_object_guidance(self, item: AssessmentItem) -> "ObjectGuidanceOutcome":
        """Graceful-degradation wrapper around :meth:`generate_object_guidance`.

        Never raises: on any Bedrock failure or unparseable output it returns an
        unavailable :class:`ObjectGuidanceOutcome` carrying a fixed,
        credential-free message, so the drawer can show the reason instead of an
        error (Requirement 11.10).
        """
        try:
            markdown = self.generate_object_guidance(item)
        except AiAssistUnavailableError as error:
            return ObjectGuidanceOutcome(
                available=False, reason=error.reason, detail=error.detail
            )
        return ObjectGuidanceOutcome(
            available=True,
            reason="OK",
            detail="",
            markdown=markdown,
            model_id=self._config.model_id,
        )

    def stream_object_guidance(
        self,
        item: AssessmentItem,
        on_delta: Callable[[str], None],
    ) -> "ObjectGuidanceOutcome":
        """Stream on-demand guidance for ONE object, emitting text via ``on_delta``.

        Calls bedrock-runtime ``InvokeModelWithResponseStream`` and invokes
        ``on_delta(text)`` for each incremental text chunk so the UI can render
        the answer as it is generated (a chat-like, token-by-token reveal). The
        returned :class:`ObjectGuidanceOutcome` carries the full assembled
        Markdown on success, or a fixed, credential-free unavailable message on
        any Bedrock failure / empty stream (Requirements 11.8, 11.10). Never
        raises -- the drawer must stay robust -- and ``on_delta`` is only ever
        called with already-received text, so a mid-stream failure still leaves
        the partial text visible while the outcome explains the interruption.
        """
        prompt = build_object_guidance_prompt(item)
        try:
            response = self._get_client().invoke_model_with_response_stream(
                modelId=self._config.model_id,
                body=_build_invoke_body(
                    prompt, max_tokens=_OBJECT_GUIDANCE_MAX_TOKENS
                ),
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
                build_object_chat_system(item), messages, on_delta,
                tools=tools, execute=execute,
            )
        return self.stream_chat(
            build_object_chat_system(item), messages, on_delta
        )

    def stream_validation_chat(
        self,
        facts: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        scope: str = "table",
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a chat about a validation mismatch.

        Grounded by :func:`build_validation_chat_system` on the deterministic,
        credential-free validation ``facts`` (counts, missing/extra summary,
        drift), so replies explain WHY the table/run diverged and HOW to fix it
        (re-run to backfill a standing gap, or quiesce/stop CDC for a cut-over
        check) consistent with this tool's Full Load + CDC model. Never raises.
        """
        return self.stream_chat(
            build_validation_chat_system(facts, scope=scope), messages, on_delta
        )

    def stream_full_load_error_chat(
        self,
        table_name: str,
        error_message: str,
        messages: Sequence[Mapping[str, str]],
        on_delta: Callable[[str], None],
        *,
        migration_context: str = "",
    ) -> "ObjectGuidanceOutcome":
        """Stream one assistant turn of a chat about a FAILED Full Load table.

        Grounded by :func:`build_full_load_error_chat_system` on the failed
        ``table_name`` + its captured ``error_message`` + the optional
        ``migration_context`` (this migration's situation: type, CDC, DROP+recreate,
        selection), so replies explain WHY the table failed and HOW to fix it
        (schema/DDL vs data vs transient, then per-table Reload / Retry) specific to
        THIS migration and consistent with this tool's Full Load model. Never raises.
        """
        return self.stream_chat(
            build_full_load_error_chat_system(
                table_name, error_message, migration_context=migration_context
            ),
            messages,
            on_delta,
        )

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
            _OBJECT_GUIDANCE_MAX_TOKENS,
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
        the result fed back, in a bounded agentic loop; the final assistant text is
        delivered through ``on_delta`` and returned as :class:`ObjectGuidanceOutcome`.

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
        try:
            for _ in range(max(1, max_rounds)):
                body = json.dumps(
                    {
                        "anthropic_version": _ANTHROPIC_VERSION,
                        "max_tokens": _OBJECT_GUIDANCE_MAX_TOKENS,
                        "system": system + _RESPONSE_STYLE,
                        "tools": list(tools),
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
                if stop == "tool_use" and tool_uses:
                    # Record the assistant's tool-call turn verbatim, then answer each.
                    convo.append({"role": "assistant", "content": content})
                    results: list[dict] = []
                    for block in tool_uses:
                        name = str(block.get("name", ""))
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
                                "content": str(out)[:_MAX_TEXT_CHARS],
                            }
                        )
                    convo.append({"role": "user", "content": results})
                    continue
                # Final answer (end_turn, or a turn with no tool call): join text blocks.
                final = "".join(
                    b.get("text", "")
                    for b in content
                    if isinstance(b, Mapping)
                    and b.get("type") == "text"
                    and isinstance(b.get("text"), str)
                ).strip()
                if not final:
                    err = AiAssistUnavailableError("INVALID_OUTPUT")
                    return ObjectGuidanceOutcome(
                        available=False, reason=err.reason, detail=err.detail
                    )
                on_delta(final)
                return ObjectGuidanceOutcome(
                    available=True,
                    reason="OK",
                    detail="",
                    markdown=final[:_MAX_NARRATIVE_CHARS],
                    model_id=self._config.model_id,
                )
            return ObjectGuidanceOutcome(
                available=False,
                reason="UNAVAILABLE",
                detail=(
                    "The assistant kept looking things up without answering. "
                    "Try rephrasing the question."
                ),
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


@dataclass(frozen=True)
class ObjectGuidanceOutcome:
    """On-demand single-object AI guidance that never breaks the workflow.

    When :attr:`available` is ``True`` the :attr:`markdown` carries the model's
    remediation write-up for one object and :attr:`model_id` its provenance;
    otherwise :attr:`detail` is a fixed, credential-free message explaining why
    AI was unavailable. ``reason`` is ``"OK"`` exactly when available.
    """

    available: bool
    reason: Literal[
        "OK", "ACCESS_DENIED", "THROTTLED", "NETWORK", "UNAVAILABLE", "INVALID_OUTPUT"
    ]
    detail: str
    markdown: str = ""
    model_id: str = ""


__all__ = [
    "DSQL_CONSTRAINTS",
    "DSQL_QUERY_EFFICIENCY_RUBRIC",
    "AiAssessmentOutcome",
    "AssessmentStrategist",
    "ObjectGuidanceOutcome",
    "build_assessment_prompt",
    "build_object_guidance_prompt",
    "build_object_chat_system",
    "build_general_chat_system",
    "build_validation_chat_system",
    "build_conversion_chat_system",
    "build_query_chat_system",
    "build_query_optimize_system",
    "build_full_load_error_chat_system",
    "build_connection_error_chat_system",
    "parse_assessment_output",
]
