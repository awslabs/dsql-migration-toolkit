# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/ai_prompts.py, the AI DBA's system prompts, imports on its own (A11).

It was split out of core/assessment_strategist.py, which imports it back to re-export
every name, private ones too -- so it must not import the strategist: that would be a
cycle, and importing the prompts before the strategist would fail on a partly initialised
module. It is also pure text: no Bedrock client, no boto3 -- the strategist sends the
chats. The import check runs in a fresh interpreter, because this test session has
already imported the strategist.
"""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import ai_prompts
from dsql_migrator.core import assessment_strategist as strategist

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_PATH = _SRC / "core" / "ai_prompts.py"
_TREE = ast.parse(_PATH.read_text(encoding="utf-8"))


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return names


_NAMES = _defined(_TREE)


def test_imports_without_loading_the_strategist() -> None:
    code = (
        "import sys, dsql_migrator.core.ai_prompts; "
        "print('dsql_migrator.core.assessment_strategist' in sys.modules, "
        "'dsql_migrator.core.ai_assistant' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False False", out.stdout


def test_it_imports_nothing_of_the_strategist_and_no_aws() -> None:
    for node in ast.walk(_TREE):
        if isinstance(node, ast.ImportFrom):
            assert node.module not in (
                "dsql_migrator.core.assessment_strategist",
                "dsql_migrator.core.ai_assistant",
            ), node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(
                    alias.name in ("assessment_strategist", "ai_assistant")
                    for alias in node.names
                )
            ), node.lineno
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] not in ("boto3", "botocore"), node.lineno


def test_the_strategist_re_exports_every_name() -> None:
    assert len(_NAMES) == 17
    for name in _NAMES:
        assert getattr(strategist, name) is getattr(ai_prompts, name), name


def test_the_call_protocol_stays_in_the_strategist() -> None:
    # What a call is sent WITH besides the grounding -- the style directive, the answer tool,
    # the budgets, the reply cap -- is the strategist's.
    for name in (
        "AssessmentStrategist",
        "ObjectGuidanceOutcome",
        "source_engine_word",
        "_RESPONSE_STYLE",
        "_TOOL_CHAT_STYLE",
        "_ANSWER_TOOL_SCHEMA",
        "_CHAT_MAX_TOKENS",
        "_MAX_NARRATIVE_CHARS",
    ):
        assert hasattr(strategist, name), name
        assert not hasattr(ai_prompts, name), name
