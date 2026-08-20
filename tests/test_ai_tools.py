# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the extracted AI-DBA tools module (ui/ai_tools.py).

The tool schemas are a pure data list, and the executor is now a FACTORY
(``build_ai_tool_executor``) rather than a closure buried in ``build_page`` -- so
both are testable here without the whole app. These cover the schema contract and
the executor's store-driven "not_run / none / unknown" branches with tiny fakes
(no DB, no AWS, no NiceGUI).
"""
from __future__ import annotations

import json

from dsql_migrator.ui.ai_tools import (
    AI_TOOL_SCHEMAS,
    AI_TOOLS_SYSTEM_HINT,
    build_ai_tool_executor,
)


class _Obj:
    """Attribute bag."""

    def __init__(self, **kw) -> None:
        self.__dict__.update(kw)


class _Store:
    """Fake per-session store: get()/get_or_create() return a fixed object."""

    def __init__(self, obj=None, *, get_returns=None) -> None:  # noqa: ANN001
        self._obj = obj
        self._get = get_returns

    def get_or_create(self, _sid):  # noqa: ANN001
        return self._obj

    def get(self, _sid):  # noqa: ANN001
        return self._get


def _executor():
    """An executor wired to fakes whose result/ids are all empty (not_run/none)."""
    return build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False)),
        evaluation_store=_Store(get_returns=None),  # .get(sid) -> None => not_run
        schema_conversion_store=_Store(
            _Obj(generated_node_ids=[], apply_results=[])
        ),
        validation_store=_Store(_Obj(result=None)),
        data_migration_store=_Store(_Obj(job_id=None)),
        job_manager=_Obj(),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )


# ---------------------------------------------------------------------------
# Schema contract
# ---------------------------------------------------------------------------


def test_schemas_are_wellformed_and_names_unique() -> None:
    assert len(AI_TOOL_SCHEMAS) >= 15
    names = [t["name"] for t in AI_TOOL_SCHEMAS]
    assert len(names) == len(set(names)), "tool names must be unique"
    for t in AI_TOOL_SCHEMAS:
        assert t["name"] and isinstance(t["name"], str)
        assert t["description"] and isinstance(t["description"], str)
        assert t["input_schema"]["type"] == "object"
        assert isinstance(t["input_schema"].get("properties", {}), dict)


def test_system_hint_mentions_tools() -> None:
    assert "tools" in AI_TOOLS_SYSTEM_HINT.lower()


# ---------------------------------------------------------------------------
# Executor factory
# ---------------------------------------------------------------------------


def test_factory_returns_callable() -> None:
    assert callable(_executor())


def test_unknown_tool_returns_error_json_never_raises() -> None:
    out = json.loads(_executor()("no_such_tool", {}))
    assert out["status"] == "error" and "unknown tool" in out["message"]


def test_every_schema_tool_has_an_executor_branch() -> None:
    # A tool the model can be told about but the executor can't run would silently
    # fail; assert each schema name resolves to a non-"unknown tool" result.
    ex = _executor()
    for t in AI_TOOL_SCHEMAS:
        out = json.loads(ex(t["name"], {}))
        assert out.get("message", "") != f"unknown tool {t['name']}", t["name"]
        assert out.get("status") in {
            "ok", "none", "not_run", "not_found", "not_connected", "error"
        }


def test_not_run_and_none_branches() -> None:
    ex = _executor()
    assert json.loads(ex("list_converted_tables", {}))["status"] == "none"
    assert json.loads(ex("get_schema_apply_result", {}))["status"] == "not_run"
    assert json.loads(ex("list_objects_by_status", {}))["status"] == "not_run"
    assert json.loads(ex("get_validation_summary", {}))["status"] == "not_run"
    # A target read with no verified target degrades to not_connected (no AWS call).
    assert json.loads(ex("list_target_tables", {}))["status"] == "not_connected"
