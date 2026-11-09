# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""X1: the never-wired AI suggest -> review -> apply pipeline stays deleted.

The pipeline (``core/ai_routing.py``, the ``AiConversionAssistant`` suggestion methods,
``CdcAssistant``, the ``AiConversionSuggestion`` model, the approve/reject review gate
in ``ui/ai_assist.py`` and ``build_ai_apply_objects``) never had a production caller:
``git log -S`` shows no call site was ever added for its entry points, and the one UI
that rendered its approve/reject buttons was removed as unreachable in v0.1.370. It was
deleted rather than parked. These pins keep it from coming back half-wired, and keep the
live AI pieces that shared its modules (the Verify AI access preflight, the SQL
denylist that guards "Use as target DDL", the Bedrock error classifier and the CDC
dead-letter summarizer the AI DBA's DLQ chat uses) where the live callers import them.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
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
    ),
    "dsql_migrator.core.cdc_assist": (
        "CdcAssistant",
        "CdcReadinessSignals",
        "build_cdc_readiness_prompt",
        "build_dlq_triage_prompt",
        "_CDC_MAX_TOKENS",
    ),
    "dsql_migrator.core.models": ("AiConversionSuggestion",),
    "dsql_migrator.core.converter": ("build_source_table_ddl",),
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

# Live names that shared these modules; their production callers import them from here.
_KEPT_NAMES: dict[str, tuple[str, ...]] = {
    "dsql_migrator.core.ai_assistant": (
        "AiConversionAssistant",
        "AiAssistUnavailableError",
        "validate_suggested_sql",
        "build_bedrock_runtime_client",
        "_build_invoke_body",
        "_classify_bedrock_error",
        "_extract_suggestion_text",
        "_UNAVAILABLE_DETAILS",
        "_ACCESS_CHECK_DETAILS",
        "_classify_access_check_error",
    ),
    "dsql_migrator.core.cdc_assist": ("_summarize_errors",),
    "dsql_migrator.ui.ai_assist": (
        "BEDROCK_MODEL_FALLBACKS",
        "build_ai_assist_config",
        "ai_availability",
        "map_access_check_display",
        "run_verify_ai_access",
    ),
    "dsql_migrator.ui.schema_conversion": ("ai_candidate_object_names",),
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


@pytest.mark.parametrize("module_name", sorted(_KEPT_NAMES))
def test_the_live_ai_pieces_stay_where_their_callers_import_them(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for name in _KEPT_NAMES[module_name]:
        assert hasattr(module, name), f"{module_name}.{name} is live and must stay"


def test_no_src_module_defines_or_imports_a_deleted_name() -> None:
    deleted = {n for names in _DELETED_NAMES.values() for n in names}
    # _invoke is a generic private name another class may legitimately define.
    deleted |= set(_DELETED_ASSISTANT_METHODS) - {"_invoke"}
    # A name deleted from one module but live in another stays allowed: the ui Protocol
    # AiConversionAssistant went, the core preflight class of that name is live; the AI
    # DBA's own _FENCE_RE (ui/ai_chat_drawer.py) is live, only ai_assistant's copy went.
    deleted -= {n for names in _KEPT_NAMES.values() for n in names}
    deleted.discard("_FENCE_RE")
    found: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        rel = path.relative_to(_SRC).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module == "dsql_migrator.core.ai_routing":
                    found.append(f"{rel}:{node.lineno} imports core.ai_routing")
                found.extend(
                    f"{rel}:{node.lineno} imports {alias.name}"
                    for alias in node.names
                    if alias.name in deleted
                )
            elif isinstance(node, ast.Import):
                found.extend(
                    f"{rel}:{node.lineno} imports {alias.name}"
                    for alias in node.names
                    if alias.name == "dsql_migrator.core.ai_routing"
                )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if node.name in deleted:
                    found.append(f"{rel}:{node.lineno} defines {node.name}")
    assert found == []
