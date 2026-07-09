# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the shared AI chat drawer's pure helpers.

The drawer itself is NiceGUI-heavy, but its markdown segmentation (used to render
each code block as its own copy-enabled ui.code at the end of a streamed reply) is
pure and must stay screen-agnostic (no SQL knowledge).
"""
from __future__ import annotations

from dsql_migrator.ui.ai_chat_drawer import (
    markdown_has_code_block,
    split_markdown_segments,
)


def test_split_prose_only_is_single_text_segment() -> None:
    assert split_markdown_segments("just some prose") == [("text", "just some prose", "")]
    assert markdown_has_code_block("just some prose") is False


def test_split_prose_and_one_code_block_preserves_order() -> None:
    md = "Try this:\n```sql\nSELECT 1\n```\nDone."
    segs = split_markdown_segments(md)
    assert [k for k, _, _ in segs] == ["text", "code", "text"]
    assert segs[1][1].strip() == "SELECT 1"
    assert segs[1][2] == "sql"
    assert markdown_has_code_block(md) is True


def test_split_two_code_blocks_kept_in_order() -> None:
    md = "```sql\nSELECT a\n```\nand\n```sql\nSELECT b\n```"
    segs = split_markdown_segments(md)
    codes = [b.strip() for k, b, _ in segs if k == "code"]
    assert codes == ["SELECT a", "SELECT b"]


def test_split_fence_without_language_tag() -> None:
    segs = split_markdown_segments("```\nSELECT 1\n```")
    assert segs[0][0] == "code"
    assert segs[0][2] == ""  # no language


def test_split_uppercase_and_dialect_language_tags_captured() -> None:
    assert split_markdown_segments("```SQL\nSELECT 1\n```")[0][2] == "SQL"
    assert split_markdown_segments("```postgresql\nSELECT 1\n```")[0][2] == "postgresql"


def test_unterminated_trailing_fence_stays_text_no_data_loss() -> None:
    # A truncated reply whose final fence never closes must not be dropped.
    md = "ok\n```sql\nSELECT unfinished"
    assert markdown_has_code_block(md) is False
    assert split_markdown_segments(md) == [("text", md, "")]


def test_drawer_module_stays_sql_agnostic() -> None:
    # The shared drawer must not depend on the SQL-specific screen layer.
    import dsql_migrator.ui.ai_chat_drawer as drawer

    src = __import__("inspect").getsource(drawer)
    assert "query_playground" not in src
    assert "extract_sql_from_reply" not in src
