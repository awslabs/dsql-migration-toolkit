# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The manual tells the Full Load the merged code runs, in every language (2026-10-29).

The performance branch changed what a user reads about a Full Load: *Reader shards per
large table* defaults to 0 = automatic and a MySQL table's ranges are one reader session's
(P1, P2); the batches in flight are capped per key range and per run (P3); a Drop & reload
creates the secondary indexes on the empty table before its rows (P4d), and the Data
Migration step shows their builds (P4b) or the indexes it never submitted (P4a); and the
live measurements replaced the old numbers in chapter 7.5 and the appendix, whose OCC
counts came from a harness that could not see the table workers (R0).

Fact pins, not sentence pins: the numbers are read from the code, the quoted UI strings are
checked against the app's own (English in every manual), and each stale phrase is one only
the old behaviour had (for ko and ja, the exact sentence the page carried). A ko or ja page
fails here until it mirrors the English one.
"""

from __future__ import annotations

import inspect

import pytest

from tests.test_g25_docs import LANGS, MANUAL, ROOT, _all_pages, _flat, _has, _page

_P03 = "03-full-load.md"
_P07 = "07-performance-and-tuning.md"
_P11 = "11-customer-faq.md"
_P12 = "12-performance-test-results.md"
_README = {"en": "README.md", "ko": "README.ko.md", "ja": "README.ja.md"}


def _row(text: str, key: str) -> str:
    """The one markdown table row whose first cell is ``key``."""
    (row,) = [line for line in text.splitlines() if line.startswith(f"| {key} |")]
    return row


def _section(lang: str, page: str, start: str, end: str) -> str:
    text = (MANUAL / lang / page).read_text(encoding="utf-8")
    return _flat(lang, text.split(start, 1)[1].split(end, 1)[0])


@pytest.mark.parametrize("lang", LANGS)
def test_the_reader_shards_default_and_rule_are_the_loaders(lang: str) -> None:
    # The code first: 0 = automatic by default, 0..32, one range per 2.5M rows from 5M.
    from dsql_migrator.config import AppConfig
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    field = AppConfig.model_fields["full_load_reader_shards"]
    bounds = {
        kind: getattr(meta, kind)
        for meta in field.metadata
        for kind in ("ge", "le")
        if getattr(meta, kind, None) is not None
    }
    assert field.default == 0 and bounds == {"ge": 0, "le": 32}
    assert engine._MAX_READER_RANGES == 32
    floor = f"{engine._AUTO_SPLIT_MIN_ROWS:,}"
    per_range = f"{engine._AUTO_ROWS_PER_RANGE:,}"
    assert (floor, per_range) == ("5,000,000", "2,500,000")

    key = "`DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS`"
    tuning = _row((MANUAL / lang / _P07).read_text(encoding="utf-8"), key)
    assert "`0`" in tuning and "≤ 32" in tuning and "≤ 8" not in tuning, lang
    readme = _row((ROOT / _README[lang]).read_text(encoding="utf-8"), key)
    assert "`0`" in readme and "32" in readme and "≤8" not in readme, lang
    for page in (_P03, _P07):
        text = _page(lang, page)
        assert floor in text and per_range in text, (lang, page)


@pytest.mark.parametrize("lang", LANGS)
def test_the_in_flight_caps_are_the_settings_notice(lang: str, monkeypatch) -> None:
    # The Settings > Full Load notice names the caps the loader applies (P3); chapter 7
    # quotes it, and the table rows give the run's cap.
    for seam in ("RANGE_IN_FLIGHT", "IN_FLIGHT_BUDGET"):
        monkeypatch.delenv(f"DSQL_MIGRATOR_FULL_LOAD_{seam}", raising=False)
    from dsql_migrator.ui.data_migration import _full_load_engine as engine
    from tests.test_ui_design import _FormUi, _render_tuning_tab

    ui = _FormUi()
    _render_tuning_tab("Full Load", ui)
    header = (
        f"Batches in flight are capped at {engine._range_in_flight()} per key range "
        f"(at least {engine._UNIT_IN_FLIGHT_FLOOR} per table or part) and "
        f"{engine._run_in_flight_budget()} per run"
    )
    assert header in " ".join(ui.texts)
    assert _has(lang, _page(lang, _P07), header), lang
    budget = str(engine._run_in_flight_budget())
    key = "`DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM`"
    assert budget in _row((MANUAL / lang / _P07).read_text(encoding="utf-8"), key), lang
    assert budget in _row((ROOT / _README[lang]).read_text(encoding="utf-8"), key), lang


# The per-unit rule in each language's words, its numbers the loader's (2026-10-31): at most
# R per key range, at least F per table or part. And the old cap of 4 per key range, in the
# words each page used for it.
def _in_flight_rule(lang: str) -> tuple:
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    r, f = engine._RANGE_IN_FLIGHT, engine._UNIT_IN_FLIGHT_FLOOR
    return {
        "en": (f"{r} per key range", f"at least {f} per table or part"),
        "ko": (f"키 범위당 {r}개", f"테이블 또는 부분당 최소 {f}개"),
        "ja": (f"キー範囲あたり {r}", f"テーブルまたは部分あたり最低 {f}"),
    }[lang]


_OLD_CAP = {"en": "at 4 per key range", "ko": "키 범위당 4개,", "ja": "キー範囲あたり 4、"}
_GUARDRAIL = {"en": "**Connection-quota guardrail.**", "ko": "**연결 쿼터 가드레일.**",
              "ja": "**接続クォータのガードレール。**"}


@pytest.mark.parametrize("lang", LANGS)
def test_the_in_flight_rule_is_the_loaders_wherever_it_is_stated(lang: str) -> None:
    per_range, floor = _in_flight_rule(lang)
    assert _in_flight_rule("en") == ("2 per key range", "at least 4 per table or part")
    key = "`DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM`"
    stated = {
        "7.1": _section(lang, _P07, "## 7.1", "### "),
        "7.2 row": _flat(lang, _row((MANUAL / lang / _P07).read_text(encoding="utf-8"), key)),
        "7.2 guardrail": _section(lang, _P07, _GUARDRAIL[lang], "### Validation"),
        _P11: _page(lang, _P11),
        "README row": _flat(lang, _row((ROOT / _README[lang]).read_text(encoding="utf-8"),
                                       key)),
    }
    for where, text in stated.items():
        assert _has(lang, text, per_range) and _has(lang, text, floor), (lang, where)
    # The 7.5 measurement that set the rule, cited where the manual gives in-flight numbers.
    measured = _section(lang, _P07, "## 7.5", "## 7.6")
    for fact in ("80M", "150,000", "1.4–1.6", "2.8", "`TotalDPU`"):
        assert _has(lang, measured, fact), (lang, fact)
    # The old cap is gone from every page of the manual and from the README -- in ko and ja
    # also in English, the words of the Settings notice they quote.
    readme = _flat(lang, (ROOT / _README[lang]).read_text(encoding="utf-8"))
    for old in sorted({_OLD_CAP[lang], _OLD_CAP["en"]}):
        stale = [name for name, text in {**_all_pages(lang), "README": readme}.items()
                 if _has(lang, text, old)]
        assert not stale, (lang, old, stale)


# The ten-minute bound on a Drop & reload's wait for the builds on its empty tables (P4d).
_INDEX_WAIT = {"en": "10 minutes", "ko": "10분", "ja": "10分"}


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_says_indexes_come_before_the_rows_and_quotes_the_step(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _full_load_engine as engine
    from dsql_migrator.ui.data_migration import _full_load_ui, _index_builds

    assert engine._INDEX_WAIT_BEFORE_LOAD_SECONDS == 600.0
    indexes = _section(lang, _P03, "## 3.4", "## 3.5")
    assert _has(lang, indexes, _INDEX_WAIT[lang]), lang
    # What the Data Migration step shows while it waits, builds or still owes the indexes:
    # each quote is the app's own text.
    shown = (
        (engine, "Waiting for Aurora DSQL to build the secondary indexes of "),
        (_index_builds, "Aurora DSQL is building secondary indexes: "),
        (_index_builds, "Secondary indexes built: all "),
        (_index_builds, "UNIQUE index build failed: a constraint is missing"),
        (_full_load_ui, "Secondary indexes not created yet ("),
    )
    for module, text in shown:
        assert text in inspect.getsource(module), text
        assert _has(lang, indexes, text.strip().rstrip("(").strip()), (lang, text)


@pytest.mark.parametrize("lang", LANGS)
def test_the_measurements_are_the_live_runs_of_this_version(lang: str) -> None:
    # Chapter 7.5 and the appendix give the conditions of the final runs, and no longer the
    # old ones (the 1 TB and 8 vCPU tables measured an earlier loader).
    for page in (_P07, _P12):
        text = _page(lang, page)
        assert "db.r8g.16xlarge" in text and _has(lang, text, "16 vCPU"), (lang, page)
    appendix = _page(lang, _P12)
    for stale in ("915.7M", "32,270"):
        assert stale not in appendix, (lang, stale)


# What the manual said before the performance branch: gone from every page. The first three
# hold in every language (the clamp, the old default, the connection product); the rest are
# each language's own sentence for indexes built after the load, a token minted per
# connection, "raise Batches per table" for a write-side wall, a split read "above 1", and a
# TOO_MANY_INDEXES failure "after Full Load has written every row".
_STALE_EVERYWHERE = (
    "32 ÷ `TABLE_PARALLELISM`",
    "`full_load_reader_shards=1`",
    "`table_parallelism × batch_parallelism`",
)
_STALE = {
    "en": ("**Indexes come last.**",
           "Loads the data first, then builds secondary indexes once",
           "or the index cannot be built after a Drop & reload",
           "Mints a fresh token for every new connection",
           "raise **`BATCH_PARALLELISM`** instead",
           "*Reader shards per large table* above 1",
           "fails **after** Full Load has written every row"),
    "ko": ("**인덱스는 마지막.**",
           "데이터를 먼저 적재한 뒤 안정된 데이터 위에서 보조 인덱스를 한 번만 빌드합니다",
           "Drop & reload 후 인덱스를 만들 수 없습니다",
           "새 연결마다 새 토큰을 발급해",
           "벽이 쓰기 측이면 대신 **`BATCH_PARALLELISM`**을 올리세요",
           "*Reader shards per large table*이 1보다",
           "Full Load로 모든 행을 쓴 **뒤에** 실패하므로"),
    "ja": ("**インデックスは最後です。**",
           "先にデータをロードし、安定したデータに対して二次インデックスを 1 回だけ構築します",
           "Drop & reload の後でインデックスを作成できません",
           "新しい接続ごとに新しいトークンを発行する",
           "壁が書き込み側なら代わりに **`BATCH_PARALLELISM`** を上げます",
           "*Reader shards per large table* が 1 より大き",
           "Full Load が全行を書き込んだ **後に** 失敗する"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_old_full_load_story_is_gone_from_every_page(lang: str) -> None:
    pages = _all_pages(lang)
    for phrase in (*_STALE_EVERYWHERE, *_STALE[lang]):
        stale = [name for name, text in pages.items() if _has(lang, text, phrase)]
        assert not stale, (lang, phrase, stale)
