"""G25 (docs): the manual, the sink README, the 0.1.567 changelog entry and the plugin history
say what the final G25 code does, in English, Korean and Japanese alike.

A max-effort review found user-facing copy that the code contradicts: a unique-index hold that
"shows as lag on that partition" (the tool has no per-partition lag, and a held change showed no
lag at all before G25), a 5-minute bound stated per partition (it is per held change), a skip
of a replaced change stated for every table (re-keyed tables never skip), "FULL always works"
for REPLICA IDENTITY (PostgreSQL 18 refuses it on a table with a generated column), a
"Reloaded and verified" header (nothing checks a reload), and 0.1.567 release notes that
overstate the GTID record (a legacy stack's first Start is judged the old way) and understate
the downgrade risk (a stack only deployed on 0.1.567 fails an older version's Start too).

The review of that fix then found two more: the deployment guides' teardown left out the plugin
bucket that no stack owns (and that G25's content-addressed keys make grow), and §3.1 placed the
publish_via_partition_root repair at the Full Load alone although Start CDC and the guard for a
table added after the Full Load write it too. A second review found that the manual restarted a
hold's 5 minutes after "the sink's" last transient failure, so that an outage "never" used them
up, although the restart is per sink task.

Round 3: the sink's redesigns of the skip of a replaced change (a skipped change "held after
all", then one "tried before each try" of a later hold) were reverted to G23's skip, so the
manual, the README and the v53 history described holds the plugin does not have and cited test
classes that do not exist. They now describe the skip that ships and its documented limit: a
change of another row that needs a value a skipped change would have freed waits for that
change's replacement, and is dead-lettered after its 5 minutes when the replacement is queued
behind it (or waits for it in turn). The same round's PostgreSQL and UI changes are described
where the manual covers them: an inheritance parent read FROM ONLY, the slot-position block's
way out by stack, a PostgreSQL Start after a stop mid-snapshot, a PostgreSQL 18 re-keyed
table's fix, and a refused Start recorded whole in the activity log.

Round 3, repair round 2: chapter 6 still said a Start CDC that snapshots "lets Debezium publish
the child tables" of an inheritance parent, which Start CDC (and, before it, the prerequisite
check) now refuses outright; the inheritance children's REPLICA IDENTITY is no longer graded at
all; the CDC monitor's PostgreSQL 18 fix depends on the migration type; a skip is not remembered,
so Connect's redelivery of its put runs the skipped change again; a held change's failed tries
back off and are logged; and the CDC-only dead-letter card withholds a key value that is not an
integer or a UUID. The code is read first wherever a check can, through its own constants.

Round 3, repair round 2 (continued, after the final PostgreSQL and UI repairs): chapter 4 said a
skipped change is "neither held nor dead-lettered", though a change its one-poll look-ahead
finds replaced is held until then, and the README said the same of every skipped change; §5.5's
CDC-only way forward still said the card "lists each record's primary key" with no word of the
key values it withholds; the re-keyed notice's PostgreSQL 18 fix is kept from the last
successful read, and after an app restart, until the source is read again, the notice names
FULL with that fix beside it; and the inheritance prerequisite row is not what refuses a
CDC-only Deploy (the Deploy dialog's own publication/slot gate does, as Start CDC does).

Round 4 (the closing round) changed what two of these said, so their pins moved to
test_g25_docs_r4.py: the slot-position check is stateless and judges only a stack's first Start
(no window is kept or saved for it), and the inheritance WARN is listed in every migration type,
Full load only included.

A line break is a space in en/ko and nothing in ja, so each check reads the text with the hard
wrap undone (and, in ja, with no whitespace at all).
"""

from __future__ import annotations

import json
import pathlib
import re

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
MANUAL = ROOT / "docs" / "manual"
DEPLOY = ROOT / "deploy"
LANGS = ("en", "ko", "ja")


def _flat(lang: str, text: str) -> str:
    text = re.sub(r"(?m)^[ \t]*>[ \t]?", "", text)  # a blockquote's continuation lines
    return re.sub(r"\s+", "", text) if lang == "ja" else re.sub(r"\s+", " ", text)


def _page(lang: str, page: str) -> str:
    return _flat(lang, (MANUAL / lang / page).read_text(encoding="utf-8"))


def _has(lang: str, text: str, phrase: str) -> bool:
    return _flat(lang, phrase).strip() in text


def _all_pages(lang: str) -> dict:
    return {
        md.name: _flat(lang, md.read_text(encoding="utf-8"))
        for md in sorted((MANUAL / lang).glob("*.md"))
    }


def _unique_row(lang: str) -> str:
    """Chapter 4's sink-table row for the unique-index hold (23505)."""
    text = (MANUAL / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
    (row,) = [
        line
        for line in text.splitlines()
        if line.startswith("| **") and "(`23505`)" in line.split("|")[1]
    ]
    return _flat(lang, row)


def _function(path: pathlib.Path, name: str) -> str:
    """The source of the function or method ``name``, nested functions included."""
    source = path.read_text(encoding="utf-8")
    head = re.search(rf"(?m)^([ \t]*)def {re.escape(name)}\(", source)
    indent, body = head.group(1), source[head.end() :]
    sibling = rf"{indent}(?:@|def |async def |class )" + (r"|\S" if indent else "")
    stop = re.search(rf"(?m)^(?:{sibling})", body)
    return body[: stop.start()] if stop else body


_SINK_TASK = ROOT / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
_SINK_TESTS = ROOT / "connectors/dsql-sink/src/test/java/dev/dsqlmigrator/connect"
_SINK_README = ROOT / "connectors" / "dsql-sink" / "README.md"
_CDC_UI = ROOT / "src/dsql_migrator/ui/data_migration/_cdc_ui.py"
_PAGE = {
    "04": "04-cdc-and-dsql-constraints.md",
    "06": "06-limitations.md",
    "08": "08-testing-and-verification.md",
    "13": "13-excluding-tables-and-columns.md",
}


def _v53_history() -> str:
    """The v53 entries of the plugin-version history in s3_provision.py, comment marks removed."""
    source = (ROOT / "src" / "dsql_migrator" / "core" / "s3_provision.py").read_text(
        encoding="utf-8"
    )
    start = source.index("# v53 (G23)")
    note = source[start : source.index("# v54:", start)]
    return " ".join(note.replace("#", " ").split())


_STALE_FULL = {"en": "`FULL` always works", "ko": "`FULL`은 항상 동작", "ja": "`FULL` は常に動作"}
_PG18_FULL = {
    "en": "except, on PostgreSQL 18, one with a generated column",
    "ko": "PostgreSQL 18에서 생성 컬럼이 있는 테이블은 예외",
    "ja": "PostgreSQL 18 で生成列を持つテーブルは例外",
}


@pytest.mark.parametrize("lang", LANGS)
def test_no_page_says_replica_identity_full_always_works(lang: str) -> None:
    # confirmed[21]: PostgreSQL 18 refuses UPDATE/DELETE on a published table whose replica
    # identity holds an unpublished generated column, and CDC's publication publishes none.
    pages = _all_pages(lang)
    stale = [name for name, text in pages.items() if _has(lang, text, _STALE_FULL[lang])]
    assert not stale, (lang, stale)
    for name in (
        "00-before-you-begin.md",
        "01-setup.md",
        "06-limitations.md",
        "11-customer-faq.md",
    ):
        assert _has(lang, pages[name], _PG18_FULL[lang]), (lang, name)


# Round 3, repair round 2: an inheritance child's REPLICA IDENTITY is no longer graded -- the
# tool's publication lists each table ONLY, and Start CDC refuses the snapshot that would let
# Debezium's own publication add the children -- so no page may still require one.
_STALE_CHILD_IDENTITY = {
    "en": ("child of a selected table", "and each one's inheritance children, keeps a usable",
           "each of its inheritance children must have a usable"),
    "ko": ("선택한 테이블의 상속(`INHERITS`) 자식 테이블", "그 상속 자식 테이블에 사용 가능한",
           "그 모든 상속 자식 테이블에는"),
    "ja": ("選択したテーブルの継承", "その継承の子テーブルに使用可能な",
           "そのすべての継承の子テーブルには"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_no_page_requires_a_replica_identity_of_an_inheritance_child(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot

    # The code first: the grading reads a table's own identity and its LEAF partitions only.
    assert "pg_inherits" not in cdc_pg_slot._REPLICA_IDENTITY_USABLE_SQL
    assert not hasattr(cdc_pg_slot, "is_child_tables_problem")
    pages = _all_pages(lang)
    stale = [
        (name, phrase)
        for name, text in pages.items()
        for phrase in _STALE_CHILD_IDENTITY[lang]
        if _has(lang, text, phrase)
    ]
    assert not stale, (lang, stale)


_ONLY = {
    "en": "(`FOR TABLE ONLY`: a table's inheritance children are not added with it",
    "ko": "publication(`FOR TABLE ONLY`: 테이블의 상속 자식 테이블은 함께 추가되지 않고",
    "ja": "publication(`FOR TABLE ONLY`: テーブルの継承の子テーブルは一緒に追加されず",
}
_SLOT_REFUSAL = {
    "en": "if a replication slot of the same name is in use, belongs to another database",
    "ko": "같은 이름의 복제 슬롯이 사용 중이거나, 다른 데이터베이스에 속하거나",
    "ja": "同じ名前のレプリケーションスロットが使用中、別のデータベースに属している",
}
_CHILD_ADD = {  # §13.7: the one-table ADD is ONLY, and so is what it grades
    "en": "the tool adds the table (not its inheritance children) to CDC's publication",
    "ko": "도구는 소스에서 그 테이블(상속 자식 테이블은 제외)을 CDC의 publication에 추가하고",
    "ja": "ツールはソース上でそのテーブル(継承の子テーブルは含めません)を CDC の publication に追加し",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_publication_is_only_the_migrated_tables_and_its_refusals_are_named(lang) -> None:
    # confirmed[20] + residuals[2]: FOR TABLE ONLY / ADD TABLE ONLY (a table's inheritance
    # children are never added with it), and the same-named slot of another pipeline refused
    # (never dropped).
    full_load = _page(lang, "03-full-load.md")
    assert _has(lang, full_load, _ONLY[lang]), lang
    assert _has(lang, full_load, _SLOT_REFUSAL[lang]), lang
    assert _has(lang, full_load, "`ALTER PUBLICATION`"), lang  # the failed root repair
    assert _has(lang, _page(lang, "13-excluding-tables-and-columns.md"), _CHILD_ADD[lang]), lang
    # The Start check on the slot's position quotes the UI header (English in every manual).
    cdc = _page(lang, "04-cdc-and-dsql-constraints.md")
    assert _has(lang, cdc, "CDC's replication slot is not at this Full Load's start point"), lang


_STALE_LAG = {"en": "lag on that partition", "ko": "그 파티션의 지연", "ja": "そのパーティションのラグ"}
_TABLE_LAG = {
    "en": "Stream lag on its table",
    "ko": "그 테이블의 Stream lag",
    "ja": "そのテーブルの Stream lag",
}
_HELD_LAG_BULLET = {
    "en": "counts as lag on its table while it waits",
    "ko": "기다리는 동안 그 테이블의 지연으로 집계됩니다",
    "ja": "待っている間そのテーブルの遅延として数えられます",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_held_change_shows_as_stream_lag_on_its_table_not_its_partition(lang: str) -> None:
    # confirmed[1], [5]: there is no per-partition lag; since G25 the held change itself counts
    # as its table's ReplicationLagMs.
    pages = _all_pages(lang)
    stale = [name for name, text in pages.items() if _has(lang, text, _STALE_LAG[lang])]
    assert not stale, (lang, stale)
    for name in ("04-cdc-and-dsql-constraints.md", "07-performance-and-tuning.md"):
        assert _has(lang, pages[name], _TABLE_LAG[lang]), (lang, name)
    assert _has(lang, pages["04-cdc-and-dsql-constraints.md"], _HELD_LAG_BULLET[lang]), lang


_PER_CHANGE = {
    "en": ("The 5 minutes apply to each held change", "up to 5 minutes for each held change"),
    "ko": ("5분은 보류된 변경마다 적용", "보류된 변경마다 최대 5분"),
    "ja": ("5 分は保留された変更ごとに適用", "保留された変更ごとに最大 5 分"),
}
_NOT_REKEYED = {
    "en": ("On a table that is not re-keyed", "On a re-keyed (Composite key) table nothing is skipped"),
    "ko": ("재키잉되지 않은 테이블에서", "재키잉(Composite key) 테이블에서는 아무것도 건너뛰지 않음"),
    "ja": ("リキーされていないテーブルで", "リキーされた (Composite key) テーブルでは何もスキップしません"),
}
_OUTAGE = {
    "en": "so an Aurora DSQL outage that task sees does not use up the wait",
    "ko": "그 태스크가 겪는 Aurora DSQL 장애는 대기를 다 써 버리지 않음",
    "ja": "そのタスクが受ける Aurora DSQL の障害は待機を使い切りません",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_hold_budget_is_per_change_and_the_skip_is_scoped(lang: str) -> None:
    # confirmed[2], [6], [11], [30]: the wait is per held change (conflicts on one partition
    # are waited out one after another), restarts after a transient failure, and only a table
    # that is not re-keyed skips a replaced change.
    row = _unique_row(lang)
    per_change, per_change_ch7 = _PER_CHANGE[lang]
    assert _has(lang, row, per_change), lang
    assert _has(lang, row, _OUTAGE[lang]), lang
    for phrase in _NOT_REKEYED[lang]:
        assert _has(lang, row, phrase), (lang, phrase)
    assert _has(lang, _page(lang, "06-limitations.md"), per_change), lang
    assert _has(lang, _page(lang, "07-performance-and-tuning.md"), per_change_ch7), lang


_RUNBOOK_WAIT = {
    "en": "at least 5 minutes after the last source write",
    "ko": "소스의 마지막 쓰기 후 최소 5분",
    "ja": "ソースの最後の書き込みから少なくとも 5 分",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_cut_over_runbook_waits_five_minutes_after_the_last_source_write(lang: str) -> None:
    # confirmed[5], [8]: a held change is dead-lettered only when its wait ends, so the final
    # drain waits at least that long; the docs say what the UI runbook (step 3) says.
    ui = (ROOT / "src" / "dsql_migrator" / "ui" / "validation.py").read_text(encoding="utf-8")
    assert "at least 5 minutes after " in ui and "the last source write" in ui
    assert _has(lang, _page(lang, "10-conclusion.md"), _RUNBOOK_WAIT[lang]), lang
    assert _has(lang, _page(lang, "05-validation.md"), "§10.3"), lang


@pytest.mark.parametrize("lang", LANGS)
def test_the_verified_notice_is_quoted_as_the_ui_names_it(lang: str) -> None:
    # confirmed[9]: the success header names what was proven, not a reload.
    pages = _all_pages(lang)
    assert not [n for n, t in pages.items() if "Reloaded and verified" in t], lang
    assert _has(lang, pages["06-limitations.md"], "*Verified by CHECKSUM*"), lang


_STALL = {
    "en": "`08xxx`, `57xxx` or `null` means the sink cannot reach the cluster",
    "ko": "`08xxx`, `57xxx` 또는 `null`은 싱크가 클러스터에 닿지 못한다는 뜻입니다",
    "ja": "`08xxx`、`57xxx` または `null` は、シンクがクラスターに到達できないことを",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_other_g25_ui_changes_are_described_where_the_manual_covers_them(lang) -> None:
    # The stall notice read by SQLSTATE, the CDC-only target-side repair, and the GTID refusal
    # shown in full on the CDC card.
    cdc = _page(lang, "04-cdc-and-dsql-constraints.md")
    assert _has(lang, cdc, _STALL[lang]), lang
    assert _has(lang, cdc, "*Stop CDC and repair these tables*"), lang
    assert _has(lang, _page(lang, "06-limitations.md"), "*Start CDC blocked: …*"), lang
    for test in ("DsqlSinkHoldRedeliveryTest.java", "DsqlSinkSupersededSkipTest.java"):
        assert test in _page(lang, "08-testing-and-verification.md"), (lang, test)


def _v0_1_567(lang: str) -> str:
    name = "CHANGELOG.md" if lang == "en" else f"CHANGELOG.{lang}.md"
    text = (ROOT / name).read_text(encoding="utf-8")
    start = text.index("## v0.1.567")
    return _flat(lang, text[start : text.index("## v0.1.566", start)])


_CHANGELOG = {
    "en": (
        ("never continues from a GTID set that stopped advancing",
         "judged as before until its first Start on this version",
         "After a Start on this version, do not start that stack"),
        ("up to and including its first Start on this version",
         "After you deploy or start a CDC stack on this version, do not Start it",
         "(Stop and Delete CDC infrastructure still work)",
         "a Start that failed and was rolled back"),
    ),
    "ko": (
        ("CDC Start가 진행이 멈춘 GTID 집합에서 이어가지 않습니다",
         "이 버전에서 처음 Start할 때까지 예전 방식으로 판단합니다",
         "이 버전으로 Start한 스택은 이전 버전에서"),
        ("그 Start를 포함해 예전 방식으로 판단하고",
         "이 버전으로 배포하거나 Start한 CDC 스택은 이전 버전에서 Start하지 마세요",
         "(Stop과 Delete CDC infrastructure는 그대로 동작합니다)",
         "실패해 롤백된 Start"),
    ),
    "ja": (
        ("CDC の Start が進まなくなった GTID セットから続行しなくなりました",
         "このバージョンで最初に Start するまで従来どおりに判定されます",
         "このバージョンで Start したスタックを、以前のバージョンから"),
        ("その Start を含めて従来どおりに判定され",
         "このバージョンでデプロイまたは Start した CDC スタックを、以前のバージョンから Start しないでください",
         "(Stop と Delete CDC infrastructure はそのまま動作します)",
         "失敗してロールバックされた Start"),
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_0_1_567_notes_scope_the_gtid_record_and_the_downgrade(lang: str) -> None:
    # confirmed[18]: a legacy stack's first 0.1.567 Start is judged the old way (and the record
    # has blind spots); confirmed[19]: a deploy alone adds StartGtidMode, so an older version's
    # Start fails on a stack only deployed on 0.1.567 too (Stop and Delete still work).
    entry = _v0_1_567(lang)
    stale, wanted = _CHANGELOG[lang]
    for phrase in stale:
        assert not _has(lang, entry, phrase), (lang, phrase)
    for phrase in wanted:
        assert _has(lang, entry, phrase), (lang, phrase)


def test_the_sink_readme_and_the_plugin_history_describe_the_g25_hold() -> None:
    readme = _flat("en", _SINK_README.read_text(encoding="utf-8"))
    for phrase in (
        "The 5 minutes apply per held change",
        "resumed only when the put returns normally",
        "counts as `ReplicationLagMs` on its table",
        "A skip is not remembered, for an insert and an update alike",
        "the dead letter carries the describe's own SQLSTATE",
        "how many transactions Aurora DSQL asked it to retry",
    ):
        assert phrase in readme, phrase
    # The logging note said the sink logs only DLQ / dropped records.
    assert "(only `reportOrThrow` logs DLQ/dropped records" not in readme
    source = (ROOT / "src" / "dsql_migrator" / "core" / "s3_provision.py").read_text(
        encoding="utf-8"
    )
    history = " ".join(source.replace("#", " ").split())
    # Rebuilt in place (v53 reached no stack then); v54 later moved past the released v53.
    assert 'PLUGIN_VERSION = "v54"' in source
    assert "v53, rebuilt in place (G25" in history
    assert "lastTransientNanos" in history and "recordLag" in history


# -- Review of the G25 fix ------------------------------------------------------------------

_PLUGIN_BUCKET = "mysql-dsql-migrator-plugins-<account>-<region>"
_RB = f'aws s3 rb "s3://{_PLUGIN_BUCKET}" --force --region "$AWS_REGION"'


def _section(text: str, heading: str, end: str) -> str:
    """The body under the line ``heading``, up to the first line matching ``end``."""
    start = text.index(f"\n{heading}\n") + len(heading) + 2
    stop = re.search(end, text[start:], re.M)
    return text[start : start + stop.start()] if stop else text[start:]


@pytest.mark.parametrize("suffix", ("", ".ko", ".ja"))
def test_the_teardown_guides_delete_the_plugin_bucket_no_stack_owns(suffix: str) -> None:
    # §Teardown promised "nothing — and no cost — is left behind" and the EC2 teardown never
    # named S3, yet the bucket the app creates itself survives every delete-stack and, since
    # G25, keeps one content-addressed copy of each artifact a deploy uploaded. FAQ Q31, which
    # links to §Teardown, already said so.
    from dsql_migrator.core import s3_provision

    assert s3_provision.plugin_bucket_name("<account>", "<region>") == _PLUGIN_BUCKET
    templates = [*DEPLOY.glob("*.yaml"), *DEPLOY.glob("cdc-stack/*.yaml")]
    assert len(templates) >= 4, templates
    for path in templates:
        resources = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("Resources")
        for name, resource in (resources or {}).items():
            if resource.get("Type") == "AWS::S3::Bucket":
                assert "mysql-dsql-migrator-plugins" not in json.dumps(resource), (path, name)
    text = (DEPLOY / f"DEPLOYMENT{suffix}.md").read_text(encoding="utf-8")
    fargate = _section(text, "### Teardown", r"^### ")
    ec2 = _section(text, "### 6. Teardown", r"^---\s*$")
    for block in (fargate, ec2):
        assert _PLUGIN_BUCKET in block, suffix
        assert "cdc-plugins/<hash>/<file>" in block, suffix
        assert _RB in block, suffix
    # The order deletes, then checks for, the DEFAULT cdc-stack name as well as a legacy one.
    assert "--stack-name dsql-cdc-stack" in fargate, suffix
    assert "starts_with(StackName,\\`dsql-cdc-\\`)" in fargate, suffix


@pytest.mark.parametrize("lang", LANGS)
def test_faq_q31_teardown_names_the_default_cdc_stack_and_how_to_remove_the_bucket(lang) -> None:
    text = (MANUAL / lang / "11-customer-faq.md").read_text(encoding="utf-8")
    start = text.index("**Q31.")
    q31 = text[start : text.index("\n---", start)]
    assert "--stack-name dsql-cdc-stack" in q31, lang
    assert "`dsql-cdc-*`" in q31, lang
    assert "`aws s3 rb`" in q31 and "`--force`" in q31, lang
    assert _PLUGIN_BUCKET.replace("<account>-<region>", "") in q31, lang


_ROOT_REPAIR = {
    "en": (
        "at the Full Load on a publication it reuses (before it creates the slot)",
        "at each Start CDC (if there is no publication yet",
        "it creates an empty one with that setting and the connector adds its tables",
        "and before it adds a table selected after the Full Load",
        "at Start CDC a repair that fails, or cannot run because the session has no source "
        "password, does not stop the Start",
    ),
    "ko": (
        "Full Load에서는 재사용하는 publication을(슬롯을 만들기 전에)",
        "Start CDC 때마다(publication이 아직 없으면",
        "그 설정으로 빈 publication을 만들고, 커넥터가 테이블을 추가합니다",
        "그리고 Full Load 이후 선택한 테이블을 추가하기 전에 바로잡습니다",
        "Start CDC에서는 바로잡기가 실패하거나 세션에 소스 비밀번호가 없어 실행되지 못해도 Start가 "
        "멈추지 않으며",
    ),
    "ja": (
        "Full Load では再利用する publication(スロットを作成する前に)",
        "Start CDC のたび(publication がまだない場合",
        "その設定で空の publication を作成し、コネクタがテーブルを追加します",
        "そして Full Load の後に選択したテーブルを追加する前です",
        "Start CDC では、修正が失敗しても、セッションにソースのパスワードがなく修正を実行できなくても "
        "Start は止まらず",
    ),
}
_STALE_ROOT_REPAIR = {
    "en": "`publish_via_partition_root` (on a publication it reuses, before it creates the slot)",
    "ko": "`publish_via_partition_root`를 바로잡으며(재사용하는 publication이면 슬롯을 만들기 전에)",
    "ja": "`publish_via_partition_root` を修正し(再利用する publication では、スロットを作成する前に)",
}
_STALE_CONNECTOR_PUBLICATION = {
    "en": "the source connector creates its own publication and slot",
    "ko": "소스 커넥터가 자체 publication과 슬롯을 만들고",
    "ja": "source connector が自前の publication とスロットを作成し",
}


@pytest.mark.parametrize("lang", LANGS)
def test_3_1_names_every_place_the_partition_root_repair_writes(lang: str) -> None:
    # §3.1 lists every source write ("the only writes the tool ever makes to any source"), but
    # its G25 parenthetical put the publish_via_partition_root repair at the Full Load's reused
    # publication alone. Every PostgreSQL Start CDC repairs it too (creating the publication,
    # empty, when there is none yet), and so does the guard for a table added after the Full
    # Load. The code is read first, so the docs claim fails if a site goes away.
    deployer = ROOT / "src/dsql_migrator/core/cdc_deployer.py"
    assert "_repair_publication_via_partition_root(" in _function(deployer, "run_cdc_start")
    # ...never fatal: no source password in the session, or a failed write, is a warning
    # naming the statement.
    repair = _function(deployer, "_repair_publication_via_partition_root")
    for line in ("WARNING: no source credentials in this session", "WARNING: could not set"):
        assert line in repair, line
    assert "SET (publish_via_partition_root = true);" in repair
    guard = _function(
        ROOT / "src/dsql_migrator/ui/data_migration/_full_load_engine.py", "_guard_pg_on_source"
    )
    assert "ensure_publication_via_partition_root(" in guard
    slot = (ROOT / "src/dsql_migrator/core/cdc_pg_slot.py").read_text(encoding="utf-8")
    assert 'f\'CREATE PUBLICATION "{name}" WITH (publish_via_partition_root = true)\'' in slot

    full_load = _page(lang, "03-full-load.md")
    assert not _has(lang, full_load, _STALE_ROOT_REPAIR[lang]), lang
    for phrase in _ROOT_REPAIR[lang]:
        assert _has(lang, full_load, phrase), (lang, phrase)
    # Chapter 4 no longer says the connector creates the publication on a later CDC start.
    cdc = _page(lang, "04-cdc-and-dsql-constraints.md")
    assert not _has(lang, cdc, _STALE_CONNECTOR_PUBLICATION[lang]), lang


# -- Second review of the G25 fix -----------------------------------------------------------

_STALE_RESTART = {
    "en": (
        "the sink's last transient failure",
        "never uses up the wait",
        "counted again after an Aurora DSQL outage",
    ),
    "ko": (
        "싱크의 마지막 일시적 실패",
        "Aurora DSQL 장애가 대기를 다 써 버리지 않음",
        "Aurora DSQL 장애가 그 시간을 다 써 버리지 않음",
        "그 5분은 Aurora DSQL 장애 뒤 다시 세고",
    ),
    "ja": (
        "sink の最後の一時的な失敗",
        "シンクの最後の一時的な失敗",
        "Aurora DSQL の障害が待機を使い切ることはありません",
        "Aurora DSQL の障害がその時間を使い切ることはありません",
        "その 5 分は Aurora DSQL の障害の後に数え直され",
    ),
}
# The sink task whose own transient failure counts the 5 minutes again (ch. 4 row, ch. 6, ch. 8).
_HOLDING_TASK = {
    "en": dict.fromkeys(("04", "06", "08"), "of the sink task that holds it"),
    "ko": dict.fromkeys(("04", "06", "08"), "그 변경을 보류한 싱크 태스크"),
    "ja": {
        "04": "その変更を保留している sink タスク",
        "06": "その変更を保留しているシンクタスク",
        "08": "その変更を保留しているシンクタスク",
    },
}
# ...and what that leaves out: an outage only another sink task sees still uses the wait up.
_OTHER_TASK = {
    "en": {
        "04": "one that only delays the freeing change on another sink task can",
        "06": "one that only delays the other row's change on another sink task can",
    },
    "ko": {
        "04": "다른 싱크 태스크에서 값을 비우는 변경만 늦추는 장애는 다 써 버릴 수 있음",
        "06": "다른 싱크 태스크에서 다른 행의 변경만 늦추는 장애는 다 써 버릴 수 있음",
    },
    "ja": {
        "04": "値を空ける変更を別の sink タスクで遅らせるだけの障害は、待機を使い切ることがあります",
        "06": "もう一方の行の変更を別のシンクタスクで遅らせるだけの障害は使い切ることがあります",
    },
}


def _unique_texts(lang: str) -> dict:
    """The unique-hold text the manual states the 5 minutes in, keyed by chapter."""
    return {
        "04": _unique_row(lang),
        "06": _page(lang, _PAGE["06"]),
        "08": _page(lang, _PAGE["08"]),
    }


@pytest.mark.parametrize("lang", LANGS)
def test_the_hold_budget_restarts_only_on_the_holding_tasks_transient_failure(lang) -> None:
    # The restart is per sink task: lastTransientNanos is an instance field (Kafka Connect runs
    # one SinkTask instance per task), set only by that task's own transient failures. With
    # SinkTasksMax > 1 the old key's delete and the new key's insert can sit on different tasks,
    # so an outage only the delete's task sees does not restart the insert's hold: it is
    # dead-lettered after 5 minutes and the delete then removes the row. The manual said "the
    # sink's" failure and "never". The code is read first, so this fails if the restart becomes
    # connector-wide.
    source = _SINK_TASK.read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s*private long lastTransientNanos = Long\.MIN_VALUE;", source)
    assert "now - Math.max(hold.firstNanos, lastTransientNanos) < UNIQUE_HOLD_BUDGET_NANOS" in (
        " ".join(source.split())
    )
    pages = _all_pages(lang)
    stale = [
        (name, phrase)
        for name, text in pages.items()
        for phrase in _STALE_RESTART[lang]
        if _has(lang, text, phrase)
    ]
    assert not stale, (lang, stale)
    texts = _unique_texts(lang)
    for chapter, phrase in _HOLDING_TASK[lang].items():
        assert _has(lang, texts[chapter], phrase), (lang, chapter)
    for chapter, phrase in _OTHER_TASK[lang].items():
        assert _has(lang, texts[chapter], phrase), (lang, chapter)


# -- Round 3: the skip that ships (G23's), and the change it can strand ----------------------


def test_the_sink_skips_as_g23_did_and_ends_a_hold_three_ways() -> None:
    # The facts the docs below describe, read from the final source first. A conflicting change
    # of a table that is not re-keyed (and not a move) is skipped when a later change of its row
    # in the put replaces it -- looked for in a held tail too, which only Connect's redelivery
    # of the put that made the hold has -- and a hold ends APPLIED, DEAD_LETTERED or REPLACED.
    # The reverted designs (a converted hold ending MOOT, an earlier skip applied first) are
    # gone.
    source = " ".join(_SINK_TASK.read_text(encoding="utf-8").split())
    for present in (
        "enum HoldEnd { APPLIED, DEAD_LETTERED, REPLACED }",
        "private boolean supersededInPut(Applicable failing)",
        "private List<Applicable> heldTailInPut()",
        "if (!event.hasSourceKey() && !event.isMove() && supersededInPut(a)) {",
        "if (holds.containsKey(partitionOf(failing.record()))) {",
        # A change the one-poll look-ahead finds replaced was held until then: its hold ends.
        "hold.end(HoldEnd.REPLACED, putNanos);",
    ):
        assert present in source, present
    for absent in (
        "hold.converted",
        "HoldEnd.MOOT",
        "HoldEnd.EARLIER_APPLIED",
        "applyEarlier",
        "ownRow && !skipped.readAhead()",
        "record Replaced",
    ):
        assert absent not in source, absent


_STALE_SKIP = {
    "en": (
        "held after all",
        "skipped after all",
        "is held instead",
        "unless another unique conflict on its partition lies between the two",
        "MOOT",
        "EARLIER_APPLIED",
        # A skip is not remembered: Connect's redelivery of its put runs the change again.
        "never written or dead-lettered",
        "a skip that such a redelivery repeats",
        # A change the one-poll look-ahead finds replaced was held until then.
        "neither held nor dead-lettered",
    ),
    "ko": (
        "결국 보류",
        "다른 고유 충돌이 없을 때",
        "쓰지도, dead-letter 처리하지도 않음",
        "보류하지도, dead-letter 처리하지도 않음",
    ),
    "ja": (
        "結局保留",
        "別の一意競合がない場合",
        "書き込まれず、デッドレターにもなりません",
        "保留されず、デッドレターにもなりません",
    ),
}
# Chapter 4's 23505 row: the skip, and the change of another row it can strand.
_SKIP_ROW = {
    "en": (
        "the conflicting change is skipped instead (never dead-lettered)",
        "A skipped change leaves its row's values on the target as they were until its "
        "replacement applies",
        "is dead-lettered after its 5 minutes when the replacement is queued behind it on its "
        "partition, or itself waits for a value that change's row still holds (then both are "
        "dead-lettered)",
    ),
    "ko": (
        "충돌한 변경은 대신 건너뜀(dead-letter 처리하지 않음)",
        "건너뛴 변경의 행은 그 대체 변경이 적용될 때까지 타깃에서 값이 그대로임",
        "그 대체 변경이 그 파티션에서 그 뒤에 줄 서 있거나, 그 변경의 행이 아직 가진 값을 대체 변경 "
        "자신이 기다리면(이때는 둘 다 dead-letter 처리됨) 5분 뒤 dead-letter 처리됨",
    ),
    "ja": (
        "競合した変更は代わりにスキップされます (デッドレターにはなりません)",
        "スキップした変更の行は、その置き換えが適用されるまでターゲット上の値がそのまま残ります",
        "その置き換えがそのパーティション上で後ろに並んでいる場合や、その変更の行がまだ持つ値を置き換え"
        "自身が待つ場合 (このときは両方ともデッドレターになります) は、5 分後にデッドレターになります",
    ),
}
# Chapter 6's list of the cases still dead-lettered.
_STRANDED = {
    "en": "when the change needs a value that a skipped change of another row would have freed "
    "and that row's replacing change is queued behind it on its partition, or itself waits for "
    "a value the change's own row holds (then both are dead-lettered",
    "ko": "다른 행의 건너뛴 변경이 비웠을 값이 그 변경에 필요한데 그 행의 대체 변경이 그 파티션에서 그 "
    "뒤에 줄 서 있거나, 그 대체 변경 자신이 이 변경의 행이 가진 값을 기다리는 경우(이때는 둘 다 "
    "dead-letter 처리됨",
    "ja": "別の行のスキップした変更が空けるはずだった値をその変更が必要とし、その行の置き換えの変更が"
    "そのパーティション上で後ろに並んでいるか、その置き換え自身がこの変更の行の持つ値を待つ場合(この"
    "ときは両方ともデッドレターになります",
}
# Chapter 8's 23505 row.
_STRANDED_TEST = {
    "en": "a change of another row between such a skipped change and its replacement that needs "
    "a value the skipped change would have freed is dead-lettered after its 5 minutes, never "
    "lost silently (`DsqlSinkSupersededSkipTest.java`)",
    "ko": "그런 건너뛴 변경과 그 대체 변경 사이에서 건너뛴 변경이 비웠을 값이 필요한 다른 행의 변경은 "
    "5분 뒤 dead-letter 처리되며 조용히 사라지지 않음(`DsqlSinkSupersededSkipTest.java`)",
    "ja": "そのようなスキップした変更とその置き換えの間にあって、スキップした変更が空けるはずだった値を"
    "必要とする別の行の変更は 5 分後にデッドレターになり、静かに失われることはない "
    "(`DsqlSinkSupersededSkipTest.java`)",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_describes_the_skip_and_the_change_it_can_strand(lang: str) -> None:
    # remaining[2], [12]: ch. 4, 6 and 8 described a skipped UPDATE "held after all" and
    # "skipped then" past the wait, and the XX000 row skipped a landed image only "unless
    # another unique conflict lies between". The final sink skips a replaced change for good
    # (G23), and the known limit is the other row's change it can strand, dead-lettered after
    # its own 5 minutes (the original confirmed[30]).
    pages = _all_pages(lang)
    stale = [(n, p) for n, t in pages.items() for p in _STALE_SKIP[lang] if _has(lang, t, p)]
    assert not stale, (lang, stale)
    row = _unique_row(lang)
    for phrase in _SKIP_ROW[lang]:
        assert _has(lang, row, phrase), (lang, phrase)
    assert _has(lang, pages[_PAGE["06"]], _STRANDED[lang]), lang
    ch8 = pages[_PAGE["08"]]
    assert _has(lang, ch8, _STRANDED_TEST[lang]), lang
    assert "`DsqlSinkHoldRedeliveryTest.java`" in ch8, lang


_CITED_TEST = re.compile(r"`([A-Z][A-Za-z0-9]*Test)(?:\.java)?`")


def test_every_sink_test_class_the_docs_cite_exists() -> None:
    # Round 2 cited DsqlSinkConvertedHoldTest, which never existed; round 3 removed
    # DsqlSinkReplacedSkipTest, DsqlSinkSkipMemoryTest and DsqlSinkHeldBehindSkipTest.
    cited = {
        name
        for path in (*sorted(MANUAL.glob("*/*.md")), _SINK_README)
        for name in _CITED_TEST.findall(path.read_text(encoding="utf-8"))
    }
    assert {"DsqlSinkSupersededSkipTest", "DsqlSinkHoldRedeliveryTest"} <= cited, cited
    missing = sorted(name for name in cited if not (_SINK_TESTS / f"{name}.java").is_file())
    assert not missing, missing


def test_the_readme_and_the_v53_history_describe_the_skip_that_ships() -> None:
    # remaining[3], [13]: both described the reverted design (the skipped change "held after
    # all", ending MOOT past the wait; a hold ending "skipped after all"). The skip is G23's,
    # kept through Connect's redelivery since G25, and its limit is documented.
    readme = _flat("en", _SINK_README.read_text(encoding="utf-8"))
    history = _v53_history()
    for text in (readme, history):
        for stale in (
            "held after all",
            "skipped after all",
            "is held instead",
            "MOOT",
            "EARLIER_APPLIED",
            "remembered until its replacement applies",
            "never skipped again",
            "never uses up",
            # A skip is not remembered (DsqlSinkHoldRedeliveryTest), and a held change's
            # failed tries back off from 500 ms to 10 s (DsqlSinkHoldBudgetAndLagTest).
            "A skip is final",
            "never written, tried again or dead-lettered",
            "waits at least 500 ms before its next try",
            # A change the one-poll look-ahead finds replaced was held until then.
            "the skipped change is not held or dead-lettered",
        ):
            assert stale not in text, stale
    for phrase in (
        "A skip is not remembered, for an insert and an update alike",
        "a change skipped in its put is not held or dead-lettered and never holds the "
        "committed offset back",
        "the hold ends and the change is skipped",
        "Connect's redelivery of its put runs it again with the rest of the batch",
        "or written once its conflict has cleared, its replacement then applied over it",
        "so the redelivery skips the change again instead of dead-lettering it",
        "A hold ends when its change applies, is dead-lettered, or is replaced (look-ahead)",
        "Known limit (documented, not fixed; v52 dead-lettered both changes at once)",
        "is dead-lettered after its 5 minutes when the replacement sits in its held tail",
        "so an Aurora DSQL outage this task sees does not use up the wait",
        "(restarted after a transient failure of that task)",
        "`DsqlSinkSupersededSkipTest`",
        "every hold's next try waits 500 ms, doubling to 10 s while the tries keep failing",
        "WARN that starts like Connect's retry line (`DSQL apply failed (transient sqlstate=…)`)",
        "WARN when one outlasts its wait or a try of it fails transiently",
    ):
        assert phrase in readme, phrase
    for phrase in (
        "The skip of a replaced change stays G23's",
        "and not remembered: Connect's redelivery of the put runs it again",
        "it is looked for there too (heldTailInPut)",
        "is dead-lettered with 23505 after its 5 min, as v52 dead-lettered both changes at once",
        "an Aurora DSQL outage the holding task sees does not use up its hold",
        "hold's next try waits 500 ms, doubling to 10 s while the tries keep failing",
        'logs a WARN that starts like Connect\'s retry line ("DSQL apply failed (transient',
    ):
        assert phrase in history, phrase


def test_a_held_changes_failed_try_is_logged_as_the_docs_quote_it() -> None:
    # The sink's own WARN for a held change's try that Aurora DSQL did not answer (the put no
    # longer throws for it, so Connect logs nothing) starts with the text the stall notice and
    # chapter 4 explain by SQLSTATE, and the next try backs off. The code is read first.
    source = " ".join(_SINK_TASK.read_text(encoding="utf-8").split())
    assert (
        '"DSQL apply failed (transient sqlstate={}); the sink tries the change held on table {} '
        'at "' in source
    )
    assert "long retryIn = holdWaitNanos(unansweredTries);" in source
    for lang in LANGS:
        assert _has(lang, _page(lang, _PAGE["04"]), "`DSQL apply failed (transient sqlstate=…)`")


# -- Round 3: the PostgreSQL and UI changes, where the manual covers them --------------------

_FROM_ONLY_6 = {  # §6.1
    "en": "The Full Load, Validation and the CDC monitor's `MAX(pk)` check read a parent's own "
    "rows only (`FROM ONLY`), as CDC captures them, so a parent whose rows all live in its "
    "children (inheritance used as partitioning) loads none",
    "ko": "Full Load와 Validation, CDC 모니터의 `MAX(pk)` 확인은 부모 자신의 행만 읽으므로(`FROM ONLY`, "
    "CDC가 캡처하는 것과 같음) 모든 행이 자식에 있는 부모(파티셔닝 용도의 상속)는 행을 하나도 로드하지 않고",
    "ja": "Full Load と Validation、CDC モニターの `MAX(pk)` の確認は親テーブル自身の行だけを読むため "
    "(`FROM ONLY`。CDC がキャプチャするものと同じ)、すべての行が子テーブルにある親 (パーティショニング目的"
    "の継承) は 1 行もロードせず",
}
_FROM_ONLY_3 = {  # §3.1
    "en": "just as the Full Load reads only the table's own rows, `FROM ONLY`",
    "ko": "Full Load도 그 테이블 자신의 행만 읽음(`FROM ONLY`",
    "ja": "Full Load もそのテーブル自身の行だけを読みます(`FROM ONLY`",
}
# §6.1: what the refusal means for each migration type. The prerequisite row says so first
# (in every migration type since round 4, pinned in test_g25_docs_r4.py); Start CDC refuses
# the snapshot, and with CDC only the Deploy dialog's own gate does too (the Deploy gate on
# the prerequisites does not read per-table rows).
_SNAPSHOT_REFUSED = {
    "en": (
        "every CDC only Start, a re-snapshot or a Manual start point",
        "and Start CDC refuses that snapshot, as does Deploy CDC infrastructure with CDC only "
        "(*CDC cannot snapshot a table that has inheritance child tables*)",
        "With CDC only, untick the parent (each child still migrates as a table of its own); "
        "with Full load + CDC, CDC starts from the Full Load's snapshot point, with no snapshot",
        "a child needs a primary key of its own (PostgreSQL does not inherit one)",
    ),
    "ko": (
        "모든 CDC only Start, 다시 스냅샷, Manual 시작 지점",
        "실패로 판정하며, Start CDC는 그 스냅샷을 거부하고 CDC only에서는 Deploy CDC infrastructure도 "
        "거부함(*CDC cannot snapshot a table that has inheritance child tables*)",
        "CDC only에서는 부모의 선택을 해제하고(각 자식은 여전히 별개의 테이블로 마이그레이션됨), Full load "
        "+ CDC에서는 CDC가 스냅샷 없이 Full Load의 스냅샷 지점에서 시작함",
        "자식 테이블에는 자체 기본 키가 필요함(PostgreSQL은 기본 키를 상속하지 않음)",
    ),
    "ja": (
        "すべての CDC only の Start、再スナップショット、Manual の開始点",
        "失敗と判定し、Start CDC はそのスナップショットを拒否し、CDC only では Deploy CDC "
        "infrastructure も拒否します (*CDC cannot snapshot a table that has inheritance child "
        "tables*)",
        "CDC only では親テーブルの選択を外し (それぞれの子テーブルは引き続き別々のテーブルとして移行され"
        "ます)、Full load + CDC では CDC がスナップショットなしで Full Load のスナップショット地点から始まり"
        "ます",
        "子テーブルには独自の主キーが必要です (PostgreSQL は主キーを継承しません)",
    ),
}
# The stale justification (§6.2) and the round-3 one before it: a Start CDC that snapshots
# never goes ahead for such a parent, so it never lets Debezium publish the child tables.
_STALE_CHILDREN_PUBLISHED = {
    "en": (
        "lets Debezium publish the child tables too",
        "a Start that re-snapshots lets Debezium set the publication without it",
        "a publication that adds it without `ONLY`",
    ),
    "ko": (
        "Debezium이 자식 테이블도 publication에 넣음",
        "다시 스냅샷하는 Start에서는 Debezium이 `ONLY` 없이 publication을 설정함",
        "테이블을 `ONLY` 없이 추가한 publication",
    ),
    "ja": (
        "Debezium が子テーブルも publication に加えます",
        "再スナップショットする Start では Debezium が `ONLY` なしで publication を設定します",
        "テーブルを `ONLY` なしで追加した publication",
    ),
}


def _inheritance_blocks() -> tuple:
    """The blocks a Start that snapshots gets for a captured inheritance parent: when its
    children were read, and when that read failed. Read from the code (G25)."""
    from dataclasses import replace

    from dsql_migrator.core import cdc_pg_slot as pg

    objects = pg.PgReplicationObjects(
        publication_name="p", slot_name="s", publication_present=True,
        publication_publishes_all_dml=True, publication_tables=frozenset({"app.par"}),
        slot_present_any_database=True, slot_usable=True,
        inheritance_children=(("app.par", 1, ("app.kid",)),),
    )
    kw = dict(resumes_from_slot=False, connector_autocreates_publication=True)
    refused = pg.pg_replication_objects_blocker(objects, ["app.par"], snapshots=True, **kw)
    assert pg.pg_replication_objects_blocker(objects, ["app.par"], snapshots=False, **kw) is None
    unread = pg.pg_replication_objects_blocker(
        replace(objects, inheritance_children=None), ["app.par"], snapshots=True, **kw
    )
    return refused, unread


@pytest.mark.parametrize("lang", LANGS)
def test_an_inheritance_parent_is_read_as_its_own_rows_and_never_snapshotted(lang):
    # remaining[4] and its review: the Full Load copied an inheritance parent's child rows into
    # its table, where CDC (FOR TABLE ONLY) never captured their changes, so the Full Load,
    # Validation and the CDC monitor's MAX(pk) read such a parent FROM ONLY, decided live per
    # read, and each child is a table of its own. A Start CDC that snapshots -- every CDC only
    # Start, a re-snapshot, a Manual start point -- would copy the children's rows back in, so
    # it is refused (and fails closed when the tool reads the source but not the children; one
    # it cannot read at all is warned about instead, test_g25_docs_r4.py); chapter 6 said such a
    # Start "lets Debezium publish the child tables too". The code is read first.
    from dsql_migrator.core import cdc_pg_slot as pg

    core = ROOT / "src/dsql_migrator/core"
    assert "def pg_reads_own_rows_only(" in (core / "source_dialect/postgres.py").read_text(
        encoding="utf-8"
    )
    exporter = (core / "exporter.py").read_text(encoding="utf-8")
    assert 'f"ONLY {table_sql}" if from_only else table_sql' in exporter
    assert "class _OwnRowsSource" in (core / "validator.py").read_text(encoding="utf-8")
    assert "pg_reads_own_rows_only" in _function(core / "watermark.py", "max_pk_source")
    refused, unread = _inheritance_blocks()
    assert refused is not None and refused[0] == pg.INHERITANCE_SNAPSHOT_HEADER
    assert "untick app.par" in refused[1] and '"Full load + CDC"' in refused[1]
    assert unread is not None and unread[0] == pg.INHERITANCE_UNREADABLE_HEADER
    # The prerequisite row: a WARN, and a required FAIL when this run's Start CDC snapshots
    # (Full load only lists the same WARN since round 4: test_g25_docs_r4.py).
    from dsql_migrator.core import prerequisites_postgres as pp
    from dsql_migrator.core.models import ColumnDef, PrerequisiteStatus, TableDef

    table = TableDef(name="app.par", columns=[ColumnDef(name="id", mysql_type="integer")],
                     primary_key=["id"])
    facts = pp.PostgresCdcFacts(inheritance_children={"app.par": (1, ("app.kid",))})
    warn = pp.check_inheritance_children(table, facts, snapshots=False)
    fail = pp.check_inheritance_children(table, facts, snapshots=True)
    assert (warn.status, warn.required) == (PrerequisiteStatus.WARN, False)
    assert (fail.status, fail.required) == (PrerequisiteStatus.FAIL, True)
    # With CDC only, the Deploy dialog runs the same publication/slot gate first (its Start
    # snapshots, so the gate reads the children): Deploy is refused before anything billable.
    from types import SimpleNamespace

    from dsql_migrator.ui.data_migration._cdc_ui import pg_objects_required_before_deploy
    from dsql_migrator.ui.data_migration._models import MigrationType

    cdc_only = SimpleNamespace(migration_type=MigrationType.CDC_ONLY)
    assert pg_objects_required_before_deploy(cdc_only, None) is True
    assert "with_inheritance_children(" in _function(_CDC_UI, "_probe_pg_replication_objects")
    limits = _page(lang, _PAGE["06"])
    assert _has(lang, limits, _FROM_ONLY_6[lang]), lang
    for phrase in _SNAPSHOT_REFUSED[lang]:
        assert _has(lang, limits, phrase), (lang, phrase)
    # The refusal is quoted as the UI names it (English in every manual), in ch. 6 and ch. 4.
    cdc = _page(lang, _PAGE["04"])
    for text in (limits, cdc):
        assert _has(lang, text, f"*{pg.INHERITANCE_SNAPSHOT_HEADER}*"), lang
    assert _has(lang, cdc, f"*{pg.INHERITANCE_UNREADABLE_HEADER}*"), lang
    pages = _all_pages(lang)
    stale = [
        (name, phrase)
        for name, text in pages.items()
        for phrase in _STALE_CHILDREN_PUBLISHED[lang]
        if _has(lang, text, phrase)
    ]
    assert not stale, (lang, stale)
    assert _has(lang, _page(lang, "03-full-load.md"), _FROM_ONLY_3[lang]), lang


# The slot-position block (remaining[5], [7]) is pinned in test_g25_docs_r4.py: round 4
# made it stateless, judged only on a stack's first Start, so the round-3 rule (a Start that
# went ahead exempts later ones) and its way out on a stack started before are gone.


_MID_SNAPSHOT = {
    "en": "with `initial`, though, a source connector that never completed its snapshot (its "
    "task failed, or CDC was stopped, first) snapshots every selected table again",
    "ko": "`initial`에서 초기 스냅샷을 끝내지 못한 소스 커넥터(그 전에 태스크가 실패했거나 CDC를 멈춘 "
    "경우)는 선택한 모든 테이블을 다시 스냅샷합니다",
    "ja": "ただし `initial` で初期スナップショットを完了しなかった source connector (その前にタスクが"
    "失敗したか、CDC を停止した場合) は、選択したすべてのテーブルをもう一度スナップショットします",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_postgres_start_after_a_stop_mid_snapshot_snapshots_again(lang: str) -> None:
    # remaining[10]: the cards promised "streamed before ... no re-load" for a stack that may
    # never have streamed; the UI now says what the start line says, and so does §4.3.
    continues = _function(_CDC_UI, "_pg_resume_continues")
    assert "completed its initial snapshot (its task failed, or CDC was stopped, before then)" in (
        continues
    )
    assert _has(lang, _page(lang, _PAGE["04"]), _MID_SNAPSHOT[lang]), lang


_KEEP_SOURCE_PK = {
    "06": {
        "en": "the prerequisites, the Full Load and the load of a table selected after the Full "
        "Load refuse it before any write: set it back to **Keep source PK** in Schema Conversion "
        "(for such a table the CDC monitor's notice, below, names that fix instead of FULL: then "
        "reload the table, or, with CDC only, migrate it again with **Start over**)",
        "ko": "사전 점검, Full Load, 그리고 Full Load 후 선택한 테이블의 로드가 쓰기 전에 거부합니다: Schema "
        "Conversion에서 그 테이블을 **Keep source PK**로 되돌리세요(그런 테이블에는 아래 CDC 모니터 알림도 "
        "FULL 대신 그 해결 방법을 알려 줌: 그다음 테이블을 리로드하거나, CDC only에서는 **Start over**로 다시 "
        "마이그레이션)",
        "ja": "前提条件チェック、Full Load、そして Full Load の後に選択したテーブルのロードが書き込みの前に"
        "拒否します: Schema Conversion でそのテーブルを **Keep source PK** に戻してください(そのような"
        "テーブルには、下記の CDC モニターの通知も FULL の代わりにその対処法を示します: その後テーブルを"
        "リロードするか、CDC only では **Start over** でもう一度移行します)",
    },
    "04": {  # the red re-check notice: from the last successful read; after a restart, both
        "en": "(on PostgreSQL 18, for a table with a generated column, it names **Keep source PK** "
        "instead of FULL, and a failed re-read keeps that; after an app restart, until the tool "
        "reads the source again, it names FULL and adds that such a table needs **Keep source "
        "PK** instead; see [Chapter 6](06-limitations.md))",
        "ko": "(PostgreSQL 18에서 생성 컬럼이 있는 테이블에는 FULL 대신 **Keep source PK**를 알려 주며, "
        "다시 읽기가 실패해도 그대로임. 앱을 재시작한 뒤 도구가 소스를 다시 읽기 전까지는 FULL을 알려 주면서 "
        "그런 테이블에는 대신 **Keep source PK**가 필요하다고 덧붙임, [6장](06-limitations.md) 참고)",
        "ja": "(PostgreSQL 18 で生成列を持つテーブルには、FULL の代わりに **Keep source PK** を示し、"
        "読み直しが失敗してもそのままです。アプリの再起動後、ツールがソースを読み直すまでは FULL を示し、"
        "そのようなテーブルには代わりに **Keep source PK** が必要だと付け加えます。"
        "[第 6 章](06-limitations.md) を参照)",
    },
    "13": {
        "en": "so a re-keyed table with one is refused and its row says to set it back to **Keep "
        "source PK**",
        "ko": "생성 컬럼이 있는 재키잉 테이블은 거부되고 그 행이 **Keep source PK**로 되돌리라고 알려 줍니다",
        "ja": "生成列を持つ再キー化したテーブルは拒否され、その行が **Keep source PK** に戻すよう示します",
    },
}
_STALE_KEEP_SOURCE_PK = {  # ch. 6 before: a reload for every migration type
    "en": "names that fix and a reload instead of FULL",
    "ko": "FULL 대신 그 해결 방법과 리로드를 알려 줌",
    "ja": "FULL の代わりにその対処法とリロードを示します",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_postgresql_18_rekeyed_table_is_told_keep_source_pk_everywhere(lang: str) -> None:
    # remaining[6] and its review: the guard for a table added after the Full Load and the CDC
    # monitor told a PostgreSQL 18 re-keyed table with a generated column to set FULL, which
    # fails its UPDATE/DELETE there; and the monitor's fix then said "reload it" on CDC only,
    # which has no Full Load step to reload from. The code is read first, through its own
    # function: Start over on CDC only, a reload otherwise.
    from dsql_migrator.ui.cdc_replica_identity_gate import keep_source_pk_fix
    from dsql_migrator.ui.data_migration._models import MigrationType

    engine = (ROOT / "src/dsql_migrator/ui/data_migration/_full_load_engine.py").read_text(
        encoding="utf-8"
    )
    assert "PG_REKEYED_FULL_REFUSED.format(" in engine
    slot = (ROOT / "src/dsql_migrator/core/cdc_pg_slot.py").read_text(encoding="utf-8")
    remedy = "_REKEYED_FULL_REFUSED_REMEDY = 'set it back to \"Keep source PK\" in Schema Conversion'"
    assert remedy in slot
    cdc_only = keep_source_pk_fix(MigrationType.CDC_ONLY)
    assert "Start over" in cdc_only and "reload" not in cdc_only
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.FULL_LOAD_ONLY):
        assert keep_source_pk_fix(mtype).endswith("and reload it"), mtype
    # The notice's fix is the last SUCCESSFUL read's (a failed re-read carries it), and after
    # a restart, before any read, the FULL advice adds that fix for such a table.
    gate = (ROOT / "src/dsql_migrator/ui/cdc_replica_identity_gate.py").read_text(
        encoding="utf-8"
    )
    assert "def full_refused_facts(" in gate and "def _carry_full_refused(" in gate
    assert '" Until the tool reads the source again it cannot tell whether FULL is a fix "' in gate
    for chapter, phrases in _KEEP_SOURCE_PK.items():
        assert _has(lang, _page(lang, _PAGE[chapter]), phrases[lang]), (lang, chapter)
    assert not _has(lang, _page(lang, _PAGE["06"]), _STALE_KEEP_SOURCE_PK[lang]), lang


_ACTIVITY_WHOLE = {
    "en": "the activity log records it whole too, one longer than a log line as consecutive "
    "`(1/2) …`, `(2/2) …` lines",
    "ko": "활동 로그도 알림 전체를 기록하며, 로그 한 줄보다 긴 알림은 `(1/2) …`, `(2/2) …`처럼 이어지는 "
    "줄로 기록함",
    "ja": "アクティビティログも通知全体を記録し、ログの 1 行より長い通知は `(1/2) …`、`(2/2) …` のように"
    "続く行に記録します",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_refused_start_is_recorded_whole_in_the_activity_log(lang: str) -> None:
    # remaining[11]: a long refusal's activity record was cut inside its fix; it is now written
    # whole, as consecutive (i/N) lines when it is longer than one log line.
    assert 'return [f"({i}/{len(lines)}) {line}"' in _function(_CDC_UI, "_activity_lines")
    assert "for line in _activity_lines(" in _function(_CDC_UI, "_start_refusal")
    assert _has(lang, _page(lang, _PAGE["06"]), _ACTIVITY_WHOLE[lang]), lang


_KEY_VALUES = {
    "en": "the dead-letter queue card names each record's primary key but shows a key value only "
    "when it is an integer or a UUID (any other is withheld), so use Start over for a table "
    "whose key values it withholds",
    "ko": "데드레터 큐 카드는 각 레코드의 기본 키를 알려 주지만 키 값은 정수나 UUID일 때만 보여 주므로(그 "
    "밖의 값은 숨김), 키 값이 숨겨지는 테이블에는 Start over를 사용하세요",
    "ja": "デッドレターキューのカードは各レコードの主キーを示しますが、キーの値は整数か UUID のときだけ"
    "表示する（それ以外は伏せる）ため、キーの値が伏せられるテーブルには Start over を使ってください",
}
_STALE_KEY_VALUES = {
    "en": "the dead-letter queue card lists each primary key",
    "ko": "데드레터 큐 카드에 각 기본 키가 나옴",
    "ja": "デッドレターキューのカードに各主キーが表示されます",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_cdc_only_hand_repair_says_which_key_values_the_card_withholds(lang: str) -> None:
    # The sink logs a key value only when it is an integer or a UUID (Property 7), so the
    # CDC-only dead-letter card cannot point at a natural key's rows: the banner and the card
    # send such a table to Start over, and chapter 4 said the card "lists each primary key".
    # The code is read first.
    sink = " ".join(_SINK_TASK.read_text(encoding="utf-8").split())
    assert 'isSurrogate(v) ? String.valueOf(v) : "<withheld>"' in sink
    monitoring = (ROOT / "src/dsql_migrator/ui/data_migration/_cdc_monitoring.py").read_text(
        encoding="utf-8"
    )
    assert "shows a key value only when it is an integer or a " in monitoring
    cdc = _page(lang, _PAGE["04"])
    assert _has(lang, cdc, _KEY_VALUES[lang]), lang
    assert not _has(lang, cdc, _STALE_KEY_VALUES[lang]), lang


_WAY_FORWARD = {  # §5.5: the CDC-only way forward, as Cut over's runbook gives it
    "en": "correct the rows on Aurora DSQL by hand (the dead-letter queue card lists each "
    "record's primary key and Kafka offset, with the key's value when that is an integer or a "
    "UUID) → re-check; for a table whose key values the card withholds, use **Start over**",
    "ko": "Aurora DSQL에서 행을 직접 수정(dead-letter queue 카드에 각 레코드의 기본 키와 Kafka 오프셋이 "
    "나오며, 키 값은 정수나 UUID일 때만 나옴) → 재확인. 카드가 키 값을 숨기는 테이블에는 **Start over**를 "
    "사용하세요",
    "ja": "Aurora DSQL 上の行を手作業で修正（dead-letter queue カードに各レコードの主キーと Kafka オフセット"
    "が表示され、キーの値は整数か UUID のときだけ表示されます）→ 再確認。カードがキーの値を伏せるテーブル"
    "には **Start over** を使ってください",
}
_STALE_WAY_FORWARD = {
    "en": "(the dead-letter queue card lists each record's primary key and Kafka offset)",
    "ko": "(dead-letter queue 카드에 각 레코드의 기본 키와 Kafka 오프셋이 나옴)",
    "ja": "（dead-letter queue カードに各レコードの主キーと Kafka オフセットが表示されます）",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_cut_over_way_forward_says_which_key_values_the_card_withholds(lang: str) -> None:
    # §5.5 repeats Cut over's CDC-only hand repair, which said the card "lists each record's
    # primary key and Kafka offset": a natural key's value is shown as <withheld>, so such a
    # table's rows cannot be found from it, and the runbook now sends it to Start over. The
    # code is read first, through the runbook itself.
    from dsql_migrator.ui.cdc_dlq_gate import way_forward_lines
    from tests.test_cdc_dlq_gate import _cdc_only_gate, _entry

    steps = way_forward_lines(
        _cdc_only_gate({"shop.customers": _entry(kinds=["unique-conflict"])}, "shop.customers")
    )
    (repair,) = [s for s in steps if s.startswith("CDC only has no Full Load step")]
    assert "with the key's value when that is an integer or a UUID" in repair
    assert repair.endswith("For a table whose key values the card withholds, use Start over.")
    validation = _page(lang, "05-validation.md")
    assert _has(lang, validation, _WAY_FORWARD[lang]), lang
    assert not _has(lang, validation, _STALE_WAY_FORWARD[lang]), lang
