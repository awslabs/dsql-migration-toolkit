# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""X1 and X1b: the never-wired AI suggest -> review -> apply pipeline and the never-called
AI report code stay deleted, and so does the cut-over chat's orphaned prompt.

X1: the pipeline (``core/ai_routing.py``, the ``AiConversionAssistant`` suggestion methods,
``CdcAssistant``, the ``AiConversionSuggestion`` model, the approve/reject review gate
in ``ui/ai_assist.py`` and ``build_ai_apply_objects``) never had a production caller:
``git log -S`` shows no call site was ever added for its entry points, and the one UI
that rendered its approve/reject buttons was removed as unreachable in v0.1.370. It was
deleted rather than parked.

X1b: the AI report code went the same way. ``AssessmentStrategist``'s one-shot report and
per-object guidance methods (``generate`` / ``try_generate``,
``generate_object_guidance`` / ``try_generate_object_guidance``,
``stream_object_guidance``) with their prompt builders and ``AiAssessmentOutcome``, the
``AiAssessmentReport`` / ``AiAssessmentInsight`` / ``AiAssessmentFinding`` models and the
HTML export's AI section that rendered them (``render_html_report(ai_report=...)``, which no
caller ever passed), ``_extract_suggestion_text``, and ``load_ai_assist_config`` (the
Connect screen seeds the AI settings through ``config.load_connect_defaults``): no call
site was ever added for any of them, or the last one went with X1.

The cut-over chat's prompt: ``build_cutover_chat_system`` lost its one caller,
``AssessmentStrategist.stream_cutover_chat``, when v0.1.479 removed the "Ask AI DBA about
cut over" chat (f5c490ee), and ``_cutover_recovery_context`` was used only by it. On the
Cut over step the AI DBA panel answers through its general chat
(``build_general_chat_system`` with ``tool_chat``), which stays.

These pins keep all three from coming back half-wired, and keep the live AI pieces that
shared their modules (the AI DBA's chats, the Verify AI access preflight, the SQL denylist
that guards "Use as target DDL", the Bedrock error classifier and the CDC dead-letter
summarizer the AI DBA's DLQ chat uses) where the live callers import them.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parent.parent / "src" / "dsql_migrator"

# module -> names deleted from it
_DELETED_NAMES: dict[str, tuple[str, ...]] = {
    "dsql_migrator.core.ai_assistant": (
        "AiSuggestionOutcome",
        "_build_schema_prompt",
        "_build_data_prompt",
        "_canonical_dsql_type",
        "_split_sql_and_rationale",
        "_SQL_FENCE_LANGS",
        "_FENCE_RE",
        # X1b: the report methods' reply parser and its cap, the default token budget
        # only the X1 suggestion call used, and the never-called env loader.
        "_extract_suggestion_text",
        "MAX_SUGGESTION_CHARS",
        "_MAX_TOKENS",
        "load_ai_assist_config",
        "_read",
        "ENV_BEDROCK_MODEL_ID",
        "ENV_BEDROCK_REGION",
    ),
    "dsql_migrator.core.cdc_assist": (
        "CdcAssistant",
        "CdcReadinessSignals",
        "build_cdc_readiness_prompt",
        "build_dlq_triage_prompt",
        "_CDC_MAX_TOKENS",
    ),
    "dsql_migrator.core.models": (
        "AiConversionSuggestion",
        # X1b
        "AiAssessmentReport",
        "AiAssessmentInsight",
        "AiAssessmentFinding",
    ),
    "dsql_migrator.core.converter": ("build_source_table_ddl",),
    "dsql_migrator.core.assessment_strategist": (
        # Its only caller was CdcAssistant._invoke (X1 follow-up).
        "parse_assessment_output",
        # X1b
        "AiAssessmentOutcome",
        "build_assessment_prompt",
        "build_object_guidance_prompt",
        "_summarize_assessment",
        "_summarize_target",
        "_ASSESSMENT_MAX_TOKENS",
        # The cut-over chat's system prompt, whose one caller (stream_cutover_chat) went in
        # f5c490ee (v0.1.479), and the recovery context only that prompt used.
        "build_cutover_chat_system",
        "_cutover_recovery_context",
    ),
    # X1b: the HTML export's AI section (its ai_report argument was never passed).
    "dsql_migrator.core.assessor": ("_render_ai_html_section",),
    # ...and the module the export now lives in (A8).
    "dsql_migrator.core.assessment_report": ("_render_ai_html_section",),
    "dsql_migrator.ui.ai_assist": (
        "AI_STATUS_PENDING_REVIEW",
        "AI_STATUS_EDITED",
        "AI_STATUS_APPROVED",
        "AI_STATUS_REJECTED",
        "AiConversionAssistant",
        "edit_suggestion",
        "approve_suggestion",
        "reject_suggestion",
        "is_approved",
        "approved_suggestions",
    ),
    "dsql_migrator.ui.schema_conversion_apply": (
        "build_ai_apply_objects",
        "_dedupe_apply_objects",
    ),
    "dsql_migrator.ui.schema_conversion": (
        "build_ai_apply_objects",
        "_dedupe_apply_objects",
    ),
}

# The suggestion surface removed from the live preflight class.
_DELETED_ASSISTANT_METHODS = (
    "suggest_schema_conversion",
    "suggest_data_transformation",
    "try_suggest_schema_conversion",
    "try_suggest_data_transformation",
    "_build_suggestion",
    "_invoke",
)

# X1b: the report surface removed from the AI DBA's strategist.
_DELETED_STRATEGIST_METHODS = (
    "generate",
    "try_generate",
    "generate_object_guidance",
    "try_generate_object_guidance",
    "stream_object_guidance",
)

# The AI DBA's chat methods, which share that class; the UI's chat handlers call them.
_LIVE_STRATEGIST_METHODS = (
    "stream_chat",
    "tool_chat",
    "stream_object_chat",
    "stream_validation_chat",
    "stream_full_load_error_chat",
    "stream_cdc_chat",
)

# Live names that shared these modules; production code in another module imports each
# from here. A name only its own module uses (ai_assistant's _build_invoke_body and
# _UNAVAILABLE_DETAILS) is not pinned: inlining or moving it breaks no importer in src.
_KEPT_NAMES: dict[str, tuple[str, ...]] = {
    "dsql_migrator.core.ai_assistant": (
        "AiConversionAssistant",
        "AiAssistUnavailableError",
        "validate_suggested_sql",
        "build_bedrock_runtime_client",
        "_ANTHROPIC_VERSION",
        "_classify_bedrock_error",
        "_ACCESS_CHECK_DETAILS",
        "_classify_access_check_error",
    ),
    "dsql_migrator.core.assessment_strategist": (
        "AssessmentStrategist",
        "ObjectGuidanceOutcome",
        "source_engine_word",
        "build_general_chat_system",
        "build_conversion_chat_system",
        "build_reimplementation_chat_system",
        "build_query_chat_system",
        "build_query_optimize_system",
        "build_connection_error_chat_system",
    ),
    "dsql_migrator.core.models": ("AiAssistConfig", "AiAccessCheckResult"),
    "dsql_migrator.core.assessor": ("render_html_report",),
    "dsql_migrator.core.cdc_assist": ("_summarize_errors",),
    "dsql_migrator.ui.ai_assist": (
        "BEDROCK_MODEL_FALLBACKS",
        "build_ai_assist_config",
        "ai_availability",
        "map_access_check_display",
        "run_verify_ai_access",
    ),
    # Defined here; the Schema Conversion screen (ui/schema_conversion.py) imports it.
    "dsql_migrator.ui.schema_conversion_apply": ("ai_candidate_object_names",),
}


def test_the_routing_module_is_gone() -> None:
    assert importlib.util.find_spec("dsql_migrator.core.ai_routing") is None
    assert not (_SRC / "core" / "ai_routing.py").exists()


@pytest.mark.parametrize("module_name", sorted(_DELETED_NAMES))
def test_the_deleted_names_are_gone_and_all_still_resolves(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for name in _DELETED_NAMES[module_name]:
        assert not hasattr(module, name), f"{module_name}.{name} came back"
    # No stale export: every __all__ entry still names something.
    for name in getattr(module, "__all__", ()):
        assert hasattr(module, name), f"{module_name}.__all__ lists missing {name}"


def test_the_preflight_class_has_no_suggestion_surface() -> None:
    from dsql_migrator.core.ai_assistant import AiConversionAssistant

    for name in _DELETED_ASSISTANT_METHODS:
        assert not hasattr(AiConversionAssistant, name), name
    assert callable(AiConversionAssistant.verify_access)


def test_the_strategist_has_no_report_surface_and_keeps_its_chats() -> None:
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist

    for name in _DELETED_STRATEGIST_METHODS:
        assert not hasattr(AssessmentStrategist, name), name
    for name in _LIVE_STRATEGIST_METHODS:
        assert callable(getattr(AssessmentStrategist, name, None)), name


def test_the_html_export_has_no_ai_section() -> None:
    from dsql_migrator.core.assessor import render_html_report
    from dsql_migrator.core.models import AssessmentReport

    assert "ai_report" not in inspect.signature(render_html_report).parameters
    markup = render_html_report(AssessmentReport.from_items([]))
    # Nor the styling of the section it never contained.
    for fragment in (".ai-report", ".ai-head", ".ai-badge", ".ai-body", ".advisory{", 'class="ai-'):
        assert fragment not in markup, fragment


@pytest.mark.parametrize("module_name", sorted(_KEPT_NAMES))
def test_the_live_ai_pieces_stay_where_their_callers_import_them(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for name in _KEPT_NAMES[module_name]:
        assert hasattr(module, name), f"{module_name}.{name} is live and must stay"


def test_no_src_module_defines_or_imports_a_deleted_name() -> None:
    deleted = {n for names in _DELETED_NAMES.values() for n in names}
    # _invoke and generate are generic names another class or function may legitimately
    # define (core/target_connection.py has a token-generating ``generate``).
    deleted |= set(_DELETED_ASSISTANT_METHODS) - {"_invoke"}
    deleted |= set(_DELETED_STRATEGIST_METHODS) - {"generate"}
    # A name deleted from one module but live in another stays allowed: the ui Protocol
    # AiConversionAssistant went, the core preflight class of that name is live; the AI
    # DBA's own _FENCE_RE (ui/ai_chat_drawer.py) is live, only ai_assistant's copy went;
    # config.py's _read (and many local _read helpers) are live, only ai_assistant's went.
    deleted -= {n for names in _KEPT_NAMES.values() for n in names}
    deleted.discard("_FENCE_RE")
    deleted.discard("_read")
    found: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        rel = path.relative_to(_SRC).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
        for node in ast.walk(tree):
            # An import binds its alias too: ``from m import x as <deleted>`` and
            # ``import m as <deleted>`` bring the name back as surely as importing it.
            if isinstance(node, ast.ImportFrom):
                if node.module == "dsql_migrator.core.ai_routing":
                    found.append(f"{rel}:{node.lineno} imports core.ai_routing")
                found.extend(
                    f"{rel}:{node.lineno} imports {_imported(alias)}"
                    for alias in node.names
                    if alias.name in deleted or alias.asname in deleted
                )
            elif isinstance(node, ast.Import):
                found.extend(
                    f"{rel}:{node.lineno} imports {_imported(alias)}"
                    for alias in node.names
                    if alias.name == "dsql_migrator.core.ai_routing"
                    or alias.asname in deleted
                )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if node.name in deleted:
                    found.append(f"{rel}:{node.lineno} defines {node.name}")
            elif isinstance(node, ast.Global):
                # ``global <deleted>`` lets a function bind the module's name.
                found.extend(
                    f"{rel}:{node.lineno} declares global {name}"
                    for name in node.names
                    if name in deleted
                )
        # A deleted module-level name (MAX_SUGGESTION_CHARS, ENV_BEDROCK_*, ...) bound
        # again in any module by any other statement: an assignment of any shape (an
        # unpacking ``_a, <deleted> = ...`` too), a for, with or := target, also inside a
        # module-level if or try. Function and class bodies are skipped: a local variable
        # or a class attribute of that name is not the module's (a def of it is caught
        # above, wherever it is).
        found.extend(
            f"{rel}:{node.lineno} assigns {node.id}"
            for node in _module_scope(tree)
            if isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Store)
            and node.id in deleted
        )
    assert found == []


def _imported(alias: ast.alias) -> str:
    return f"{alias.name} as {alias.asname}" if alias.asname else alias.name


def _module_scope(tree: ast.Module) -> Iterator[ast.AST]:
    """Every node that runs in the module's own scope: the module's statements and what
    they hold, but not the inside of a def, a class or a lambda (each its own scope)."""
    pending: list[ast.AST] = list(tree.body)
    while pending:
        node = pending.pop()
        yield node
        if not isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
        ):
            pending.extend(ast.iter_child_nodes(node))
