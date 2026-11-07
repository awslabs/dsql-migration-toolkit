# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""T1: the manual says what the CDC sink does with a value over Aurora DSQL's 1 MiB per-value
limit since plugin v56 -- it refuses only a bytea value before writing; a text or JSON value
goes to Aurora DSQL, which limits it by its size after compression (measured for text), and a
value Aurora DSQL refuses is dead-lettered as an oversized value, with the column named when it
can be determined. Chapter 6's row says text is limited like json / jsonb, bytea by its raw
size. In English, Korean and Japanese."""

from __future__ import annotations

import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"

# (chapter 4 §4.5, chapter 6's 1 MiB row): what each must say, and the old claims it must not.
_SAYS = {
    "en": (
        [
            "a `bytea` value by its raw size, a `text`,\n`json` or `jsonb` value by its size "
            "after Aurora DSQL's compression (for `text` this is\nmeasured)",
            "The sink refuses a `bytea` value over 1 MiB **before writing**",
            "A `text` or JSON value goes to Aurora DSQL, as in Full Load",
            "the row is dead-lettered as an oversized value, with the column named when it can "
            "be determined",
            "the sink refuses a `bytea` value before writing, and Aurora DSQL refuses a `text` "
            "or JSON value",
        ],
        [
            "`text` is limited the same way (measured: a 1.5 MiB JSON-shaped `text` value is "
            "stored, 1.05 MiB of random characters is not), and `bytea` by its raw size",
        ],
        ["The sink measures each value **before writing**", "The sink measures values **before**",
         "(a `TEXT`/`bytea` value)", "can't be stored, whatever the type"],
    ),
    "ko": (
        [
            "`bytea` 값은 원래 크기로, `text`·`json`·`jsonb`\n값은 Aurora DSQL이 압축한 뒤의 "
            "크기로 따지므로(`text`는 실측)",
            "싱크는 1 MiB를 넘는 `bytea` 값을 쓰기 **전에** 거부하고",
            "`text`나 JSON 값은 Full Load와 마찬가지로 Aurora DSQL로 보내며",
            "초대형 값으로 **DLQ**에 격리되고, 컬럼을 알아낼 수 있으면 그 컬럼 이름이 함께 "
            "표시됩니다",
            "`bytea` 값은 싱크가 쓰기 전에, `text`나 JSON 값은 Aurora DSQL이 거부합니다",
        ],
        ["`text`도 같은 방식으로 제한되고(실측:", "`bytea`는 원래 크기로 제한됩니다"],
        ["싱크가 쓰기 **전에** 각 값을 측정해", "싱크가 쓰기 **전에** 값을 측정하므로",
         "(`TEXT`/`bytea`)", "타입과 무관하게 저장할 수 없습니다"],
    ),
    "ja": (
        [
            "`bytea` 値は元のサイズで、`text`・`json`・`jsonb` 値は Aurora DSQL による圧縮後の"
            "サイズで判定されるため (`text` は実測)",
            "sink は 1 MiB を超える `bytea` 値を **書き込み前に** 拒否し",
            "`text` や JSON の値は Full Load と同じく Aurora DSQL に送られ",
            "特大サイズの値として **DLQ** に隔離され、列を特定できればその列名も示されます",
            "`bytea` 値は sink が書き込み前に、`text` や JSON の値は Aurora DSQL が拒否します",
        ],
        ["`text` も同じように制限され (実測:", "`bytea` は元のサイズで制限されます"],
        ["sink は **書き込み前に** 各値を測定し", "sink は書き込みの **前** に値を測定するため",
         "(`TEXT`/`bytea` 値)", "型に関係なく保存できません"],
    ),
}


@pytest.mark.parametrize("lang", sorted(_SAYS))
def test_the_manual_says_only_bytea_is_refused_before_writing(lang) -> None:
    chapter4 = (ROOT / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
    chapter6 = (ROOT / lang / "06-limitations.md").read_text(encoding="utf-8")
    in4, in6, gone = _SAYS[lang]
    for phrase in in4:
        assert phrase in chapter4, (lang, phrase)
    for phrase in in6:
        assert phrase in chapter6, (lang, phrase)
    for phrase in gone:
        assert phrase not in chapter4 and phrase not in chapter6, (lang, phrase)
    # The documented json / jsonb rule and its source stay in chapter 6's row.
    assert "supported-data-types.html" in chapter6
