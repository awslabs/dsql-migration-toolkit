"""G25 round 4 (docs): the manual, in English, Korean and Japanese alike, and the sink README say
what the closing round's code does.

* f[6]: the README's logging note read as the complete list of what the sink logs ("it logs
  only" a hold's start and end and the retry counts), but the sink also logs a table's drift
  cool-down, a failed read of a table's column types or unique indexes, a failed rollback and a
  failed metrics emit. It now names the hold's lines as the only ones that name one change and
  gives the others as examples.
* Decision (2), closing f[1], f[3], f[7], f[8]: the PostgreSQL slot-position check is stateless.
  It judges only a CDC stack's FIRST Start, one whose CloudFormation parameters record no Start
  of its connectors; a stack started before (stopped since, or with its connectors in place,
  as for Retry CDC) is never judged on the slot's position, and nothing is kept or saved for
  it. Chapter 4 said it held "until a Start CDC after a Full Load has passed every check" and
  named the gapless routes for a stack started before, where it never fires now. Its residual,
  another migration under the same CDC stack name re-creating the slot after this stack has
  streamed, is documented with the advice to give each migration its own CDC stack name.
  The closing step (test_g25_docs_r4c.py) counts a Start whose CloudFormation update rolled
  back as a Start of the connectors, so the pins that say which stacks are started moved there.
* f[2]: a source the tool cannot read at all no longer passes silently, and still does not
  block: the Start CDC dialog warns and the activity log records why. Chapter 4 said Start CDC
  refuses while it cannot read the inheritance facts, which holds only for a source it reads.
* Decision (3), f[0]: an inheritance parent is read FROM ONLY in every migration type, so the
  prerequisite WARN is listed in every migration type, Full load only included; chapter 6 said
  the check is "not run for Full load only".
* f[4]: the oversized-value prerequisite and Check sizes measure an inheritance parent's own
  rows only (FROM ONLY), as the Full Load reads it.
* f[5]: a table whose dead letters a CHECKSUM proved repaired is shown *Verified by CHECKSUM*
  whatever else holds it, and that notice names an open REPLICA IDENTITY or source schema
  finding that still holds it at Cut over; chapter 6 said "unless an open ... finding still
  holds it".

Each check reads the code first, through its own constants or behaviour, then the text with the
hard wrap undone (and, in ja, with no whitespace at all).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import (
    _CDC_UI,
    _PAGE,
    _SINK_README,
    _SINK_TASK,
    LANGS,
    ROOT,
    _all_pages,
    _flat,
    _function,
    _has,
    _page,
)


def _stale(lang: str, phrases) -> list:
    """The pages of the ``lang`` manual that still hold one of ``phrases``."""
    return [
        (name, phrase)
        for name, text in _all_pages(lang).items()
        for phrase in phrases
        if _has(lang, text, phrase)
    ]


# -- f[6]: the README's logging note is not a complete list ---------------------------------


def test_the_sink_readmes_logging_note_does_not_claim_a_complete_list() -> None:
    # The code first: lines the note gives as examples of "its other lines", each per table or
    # per task, and the hold's partition-revoked line it names with the start and the end.
    source = " ".join(_SINK_TASK.read_text(encoding="utf-8").split())
    for literal in (
        '"Table {}: Aurora DSQL rejected a row for a schema reason (SQLSTATE {}); applying "',
        '"Table {}: no further rejections for 60 s; batching resumed."',
        '"Value-fit check is off for table {}: its Aurora DSQL column types could not be "',
        '"Source-key deletes are off for table {}: its Aurora DSQL unique indexes could not be '
        'read "',
        '"Could not roll back the failed attempt before its retry (sqlstate={}); the retry "',
        '"Could not emit CDC monitor metrics (net-rows / replication-lag; best-effort; "',
        '"Since the last offset commit, Aurora DSQL asked this task to retry a transaction {} "',
        '"Table {}: partition {} was revoked while the change at offset {} was held; its commit "',
    ):
        assert literal in source, literal
    readme = _flat("en", _SINK_README.read_text(encoding="utf-8"))
    note = readme[
        readme.index("## Per-record apply logging") : readme.index("**Why a connector-config gate")
    ]
    assert "it logs only" not in note
    for phrase in (
        "Besides `reportOrThrow`'s DLQ/dropped-record lines (WARN/ERROR), the only lines that "
        "name one change are a unique-index hold's: INFO when it starts and when it ends or its "
        "partition is revoked, WARN when one outlasts its wait or a try of it fails transiently",
        "Its other lines are per table or per task, for example a table's drift cool-down "
        "starting and ending (INFO), a failed read of a table's Aurora DSQL column types or "
        "unique indexes (WARN), a failed rollback or metrics emit (WARN) and, at most once per "
        "offset commit, how many transactions Aurora DSQL asked it to retry (INFO)",
        "None of these carries a row value",
    ):
        assert phrase in note, phrase


# -- Decision (2): the slot-position check judges only a stack's first Start ---------------

# What a first Start is, which stacks count as started, what the check cannot see and where the
# gapless routes are named: the closing step counts a Start CloudFormation rolled back as a
# Start, so those pins moved to test_g25_docs_r4c.py (with the round-4 wording as stale).
_SLOT_RULE = {
    "en": (
        "another run with the same CDC stack name re-created or read it, and resuming would skip "
        "the changes in between",
        # G25 round 8 (rule E): the re-run that takes the new start point is a Drop & reload.
        "re-snapshot every table, or Re-run Full Load with **Drop & reload** (under its own CDC "
        "stack name if another migration shares this one)",
        # G25 round 6 (decision A): the resume this once described is now unreachable -- a Full
        # Load that takes a new start point is refused on a stack that has streamed.
        "A Full Load that would take a new start point is refused on such a stack until the CDC "
        "infrastructure is deleted and deployed again",
        "so nothing here describes a resume from a slot a Full Load re-created after this stack "
        "had streamed",
    ),
    "ko": (
        "같은 CDC 스택 이름을 쓰는 다른 실행이 슬롯을 다시 만들었거나 읽었으므로, 재개하면 그 사이의 "
        "변경을 건너뜀",
        "모든 테이블을 다시 스냅샷하거나 **Drop & reload**로 Full Load를 다시 실행하세요(다른 마이그레이션이 "
        "이 스택 이름을 함께 쓰면 고유한 CDC 스택 이름으로)",
        "그런 스택에서 새 시작 지점을 가져가는 Full Load는 CDC 인프라를 삭제하고 다시 배포하기 전까지 "
        "거부되므로",
        "이 스택이 스트리밍한 뒤 Full Load가 다시 만든 슬롯에서의 재개는 어디에서도 설명하지 않습니다",
    ),
    "ja": (
        "同じ CDC スタック名を使う別の実行がスロットを作り直したか読み取ったため、再開するとその間の変更を"
        "スキップします",
        "すべてのテーブルを再スナップショットするか、**Drop & reload** で Full Load を再実行してください "
        "(別のマイグレーションがこのスタック名を共有している場合は独自の CDC スタック名で)",
        "そのようなスタックで新しい開始位置を取る Full Load は、CDC インフラを削除して再デプロイするまで"
        "拒否されるため",
        "このスタックがストリーミングした後に Full Load が作り直したスロットからの再開は、どこでも説明して"
        "いません",
    ),
}
# The round-3 rule (a Start that went ahead exempts later ones, a window kept for it) and its
# way out on a stack started before, where the block no longer fires.
_STALE_SLOT_RULE = {
    "en": (
        # G25 round 6: a Full Load run on a streamed stack no longer re-creates the slot for a
        # resume -- it is refused (decision A), so this promise must not come back.
        "a Full Load run again since then re-creates the slot at its own start point",
        "until a Start CDC after a Full Load has passed every check",
        "until the first Start CDC after a Full Load",
        "on one started before, a re-snapshot cannot be relied on, so the notice names the "
        "gapless routes instead",
    ),
    "ko": (
        "그 뒤 다시 실행한 Full Load는 슬롯을 자신의 시작 지점에 다시 만들기 때문입니다",
        "Full Load 후 Start CDC가 모든 검사를 통과해 진행되기 전까지는",
        "Full Load 후 첫 Start CDC 전까지는",
        "전에 시작한 스택에서는 다시 스냅샷을 믿을 수 없으므로",
    ),
    "ja": (
        "その後に再実行した Full Load はスロットを自身の開始位置に作り直すためです",
        "Full Load の後の Start CDC がすべてのチェックを通過して進むまでは",
        "Full Load の後の最初の Start CDC までは",
        "以前に開始したスタックでは再スナップショットに頼れないため",
    ),
}


def _slot_objects():
    from dsql_migrator.core import cdc_pg_slot as pg

    return pg.PgReplicationObjects(
        publication_name="p", slot_name="s", publication_present=True,
        publication_publishes_all_dml=True, publication_tables=frozenset({"app.t"}),
        slot_present_any_database=True, slot_usable=True, slot_confirmed_flush_lsn="0/2000000",
    )


@pytest.mark.parametrize("lang", LANGS)
def test_the_slot_position_check_judges_only_a_stacks_first_start(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_status import start_resumes_committed_offset
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_stack_started

    # The code first: a moved slot is refused on a stack that records no Start of its
    # connectors, with the re-snapshot route (which does run there), and never on one that does.
    kw = dict(resumes_from_slot=True, start_lsn="0/1000000")
    refused = pg.pg_replication_objects_blocker(_slot_objects(), ["app.t"], stack_started=False,
                                                **kw)
    assert refused is not None and refused[0] == pg.SLOT_POSITION_HEADER
    # Its way out is the route rule's (pg_cdc_route, G25 round 5): the re-snapshot the Start CDC
    # dialog offers when its own checks pass, else the Full Load again under its own CDC stack
    # name; the block itself names none.
    assert refused[1].endswith("silently skip the changes in between.")
    assert pg.pg_cdc_route(pg.SLOT_POSITION_HEADER, started=False, cdc_only=False) == (
        "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
        "re-snapshots passes its own checks), or Re-run Full Load on the Full Load step choosing "
        "'Drop & reload' (it creates the slot again at its own snapshot point; give this "
        "migration its own CDC stack name if another migration shares this one), then Start CDC.")
    assert pg.pg_replication_objects_blocker(_slot_objects(), ["app.t"], stack_started=True,
                                             **kw) is None
    # ...judged from the stack's record only: started then stopped, or connectors in place.
    assert _pg_stack_started(SimpleNamespace(cdc_has_committed_offset=True)) is True
    assert _pg_stack_started(SimpleNamespace(cdc_stack_phase="running")) is True
    assert _pg_stack_started(SimpleNamespace(cdc_has_committed_offset=False,
                                             cdc_stack_phase="infra")) is False
    cdc_ui = _CDC_UI.read_text(encoding="utf-8")
    for gone in ("_PG_START_WINDOW_ATTR", "_hold_pg_start_window", "_note_pg_start_went_ahead",
                 "_pg_started_from", "cdc_ops_window_to_persist", "cdc_pg_start_window"):
        assert gone not in cdc_ui, gone
    persistence = (ROOT / "src/dsql_migrator/ui/session_persistence.py").read_text(
        encoding="utf-8"
    )
    assert 'cdc_ops_window_start=getattr(migration_state, "cdc_ops_window_start", None),' in (
        persistence
    )
    assert "_ops_window_to_persist" not in persistence and "cdc_pg_start_window" not in persistence
    # The gapless routes are named on a stack that was started and then stopped (a PostgreSQL
    # Start resumes exactly when an offset is committed), for every block but the three that
    # name their own fix.
    for committed in (True, False):
        assert start_resumes_committed_offset(
            None, has_committed_offset=committed, deployed_watermark=None,
            source_type=SourceType.POSTGRES,
        ) is committed
    assert pg.BLOCKS_WITH_THEIR_OWN_FIX == {
        pg.PUBLISH_LIST_HEADER, pg.INHERITANCE_SNAPSHOT_HEADER, pg.INHERITANCE_UNREADABLE_HEADER,
    }
    assert "The gapless route: use Delete CDC infrastructure and deploy it again" in (
        pg.pg_cdc_route(pg.PUBLICATION_MISSING_HEADER, started=True, cdc_only=False))

    cdc = _page(lang, _PAGE["04"])
    for phrase in _SLOT_RULE[lang]:
        assert _has(lang, cdc, phrase), (lang, phrase)
    # The block is quoted as the UI names it (English in every manual).
    assert _has(lang, cdc, f"*{pg.SLOT_POSITION_HEADER}*"), lang
    assert not _stale(lang, _STALE_SLOT_RULE[lang]), lang


# -- f[2]: a source the tool cannot read at all: no block, never silent ---------------------

_NOT_RUN = {
    "en": (
        "If the tool can read the source but not which captured tables have any, it refuses "
        "such a Start too (*Whether a captured table has inheritance child tables could not be "
        "read*)",
        "If it cannot read the source at all — for example a Secrets Manager source after an app "
        "restart, before you test the connection again — Start CDC is not blocked, but its "
        "PostgreSQL pre-start checks do not run (the publication, the replication slot and its "
        "position, and the inheritance read)",
        "the Start CDC dialog warns *{header}*, with the reason",
        "the activity log records *{action}* (WARNING), from the dialog and again from the Start "
        "CDC job when it goes ahead without them",
        "Test the source connection on the Connect step, then open Start CDC again so they run",
    ),
    "ko": (
        "도구가 소스는 읽었지만 어떤 캡처 대상 테이블에 자식 테이블이 있는지 읽지 못하면 그런 Start도 "
        "거부됩니다(*Whether a captured table has inheritance child tables could not be read*)",
        "소스를 아예 읽지 못하면 — 예컨대 앱을 재시작한 뒤 연결을 다시 테스트하기 전의 Secrets Manager "
        "소스 — Start CDC는 막히지 않지만 시작 전 PostgreSQL 검사(publication, 복제 슬롯과 그 위치, 상속 "
        "읽기)가 실행되지 않습니다",
        "Start CDC 대화 상자가 이유와 함께 *{header}* 경고를 보여 주고",
        "활동 로그가 *{action}*(WARNING)을 대화 상자에서 한 번, 그리고 Start CDC 작업이 그 검사 없이 "
        "진행하면 그 작업에서 다시 기록합니다",
        "Connect 단계에서 소스 연결을 테스트한 뒤 Start CDC를 다시 열어 검사가 실행되게 하세요",
    ),
    "ja": (
        "ツールがソースは読めても、どのキャプチャ対象テーブルに子テーブルがあるかを読めなければ、そのような"
        " Start も拒否されます (*Whether a captured table has inheritance child tables could not be "
        "read*)",
        "ソースをまったく読めない場合 — たとえばアプリの再起動後、接続をもう一度テストする前の Secrets "
        "Manager のソース — Start CDC はブロックされませんが、開始前の PostgreSQL のチェック (publication、"
        "レプリケーションスロットとその位置、継承の読み取り) は実行されません",
        "Start CDC のダイアログが理由とともに *{header}* という警告を表示し",
        "アクティビティログが *{action}* (WARNING) を、ダイアログから一度、そして Start CDC のジョブが"
        "それらのチェックなしで進んだ場合はそのジョブからもう一度記録します",
        "Connect ステップでソースの接続をテストしてから、もう一度 Start CDC を開いてチェックを実行させて"
        "ください",
    ),
}
_NOT_RUN_6 = {  # §6.1's inheritance row
    "en": "If the tool cannot read the source at all, Start CDC does not refuse the snapshot but "
    "warns that its PostgreSQL checks did not run ([§4.3](04-cdc-and-dsql-constraints.md))",
    "ko": "도구가 소스를 아예 읽지 못하면 Start CDC는 거부하지 않고 PostgreSQL 검사가 실행되지 "
    "않았다고 경고함([§4.3](04-cdc-and-dsql-constraints.md))",
    "ja": "ツールがソースをまったく読めない場合、Start CDC は拒否せず、PostgreSQL のチェックが実行"
    "されなかったことを警告します ([§4.3](04-cdc-and-dsql-constraints.md))",
}
_STALE_FAIL_CLOSED = {  # ch. 4 before: refused "while" the inheritance facts cannot be read
    "en": ("While the tool cannot read which captured tables have any, it refuses such a Start",),
    "ko": ("읽지 못하는 동안에도 그런 Start는 거부됩니다",),
    "ja": ("読めない間も、そのような Start は拒否されます",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_source_the_tool_cannot_read_is_warned_about_not_refused(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the notice, the activity action and the reason, as the UI words them.
    header, action = cu.PG_CHECKS_NOT_RUN_HEADER, cu.PG_CHECKS_NOT_RUN_ACTION
    assert header == "CDC's publication and replication slot could not be checked"
    assert action == "PostgreSQL publication and slot checks not run"
    body = cu.pg_checks_not_run_body("R")
    assert body.startswith("Start CDC could not read the PostgreSQL source (R)")
    assert "Start CDC is not blocked on that" in body
    assert body.endswith("Test the source connection on the Connect step, then open Start CDC "
                         "again so they run.")
    reason = cu._pg_checks_not_run_reason(
        SimpleNamespace(source_password=None, source_secret_id="arn:secret"), RuntimeError("x")
    )
    assert "the Secrets Manager secret is read when the source connection is tested, and an " \
           "app restart clears it" in reason
    source = _CDC_UI.read_text(encoding="utf-8")
    assert source.count("PG_CHECKS_NOT_RUN_ACTION,") == 2  # the dialog's line and the worker's
    assert '"went ahead without them"' in source  # the worker's line: the Start goes ahead
    assert "return _INHERITANCE_UNREADABLE_BLOCK" in (
        ROOT / "src/dsql_migrator/core/cdc_pg_slot.py"
    ).read_text(encoding="utf-8")

    cdc = _page(lang, _PAGE["04"])
    for phrase in _NOT_RUN[lang]:
        phrase = phrase.format(header=header, action=action)
        assert _has(lang, cdc, phrase), (lang, phrase)
    assert _has(lang, _page(lang, _PAGE["06"]), _NOT_RUN_6[lang]), lang
    assert not _stale(lang, _STALE_FAIL_CLOSED[lang]), lang


# -- Decision (3), f[0]: the inheritance WARN in every migration type ------------------------

_EVERY_TYPE = {
    "en": "In every migration type the prerequisite check warns about each such parent "
    "(*{title}*: only the parent's own rows, and each child migrates as a table of its own if it "
    "is selected), and it fails the parent when this migration's Start CDC would snapshot it",
    "ko": "사전 요구사항 검사는 모든 마이그레이션 유형에서 그런 부모마다 경고하고(*{title}*: 부모 자신의 "
    "행만 옮기며, 각 자식은 선택한 경우 별개의 테이블로 마이그레이션됨), 이 마이그레이션의 Start CDC가 "
    "그 부모를 스냅샷하게 되면",
    "ja": "前提条件チェックはすべての移行タイプでそのような親テーブルごとに警告し (*{title}*: 親テーブル"
    "自身の行だけを移し、それぞれの子テーブルは選択した場合に別々のテーブルとして移行されます)、この移行の"
    " Start CDC がその親をスナップショットすることになる場合",
}
_STALE_NOT_RUN_FOR_FULL_LOAD = {
    "en": ("(not run for Full load only)",),
    "ko": ("(Full load only에서는 실행되지 않음)",),
    "ja": ("(Full load only では実行されません)",),
}


class _InheritanceSource:
    """A PostgreSQL prerequisite source probe whose app.par has two INHERITS children."""

    def reachable(self):
        from dsql_migrator.core.models import ConnectionResult

        return ConnectionResult(success=True)

    def grants(self):
        return []

    def variables(self):
        return {}

    def cdc_prerequisites(self, *_a, **_k):
        return None

    def inheritance_children(self, table_names):
        return {"app.par": (2, ("app.par_2026_01", "app.par_2026_02"))}


@pytest.mark.parametrize("lang", LANGS)
def test_the_inheritance_warn_is_listed_in_every_migration_type(lang: str) -> None:
    from dsql_migrator.core.models import (
        ColumnDef,
        MigrationMode,
        PrerequisiteCheckRequest,
        PrerequisiteStatus,
        SourceType,
        TableDef,
    )
    from dsql_migrator.core.prerequisites import PrerequisiteChecker
    from dsql_migrator.core.prerequisites_postgres import _INHERITANCE_TITLE
    from tests.test_prerequisites import _FakeTarget

    # The code first: a Full load only run (MigrationMode.FULL_LOAD) lists the CDC modes' WARN.
    table = TableDef(name="app.par", columns=[ColumnDef(name="id", mysql_type="integer")],
                     primary_key=["id"])
    report = PrerequisiteChecker(
        source_probe=_InheritanceSource(), target_probe=_FakeTarget(existing={"app.par"})
    ).check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, source_type=SourceType.POSTGRES,
                                 tables=["app.par"]),
        tables=[table],
    )
    (row,) = [r for r in report.results if r.title == _INHERITANCE_TITLE]
    assert (row.target, row.status, row.required) == ("app.par", PrerequisiteStatus.WARN, False)

    limits = _page(lang, _PAGE["06"])
    assert _has(lang, limits, _EVERY_TYPE[lang].format(title=_INHERITANCE_TITLE)), lang
    assert not _stale(lang, _STALE_NOT_RUN_FOR_FULL_LOAD[lang]), lang


# -- f[4]: the value-size checks measure an inheritance parent FROM ONLY ---------------------

_SIZE_FROM_ONLY = {
    "en": (
        "On PostgreSQL, an inheritance (`INHERITS`) parent is measured over its own rows only "
        "(`FROM ONLY`), as the Full Load reads it",
        "by the prerequisite checks and Check sizes alike: a value in a child table counts only "
        "for that child",
    ),
    "ko": (
        "PostgreSQL에서 상속(`INHERITS`) 부모는 사전 점검에서도 Check sizes에서도 Full Load가 읽는 "
        "것처럼 자신의 행만으로 측정됩니다(`FROM ONLY`,",
        "자식 테이블의 값은 그 자식의 것으로만 셉니다",
    ),
    "ja": (
        "PostgreSQL では、継承 (`INHERITS`) の親テーブルは、前提条件チェックでも Check sizes でも、"
        "Full Load が読むのと同じく自身の行だけで測定されます (`FROM ONLY`。",
        "子テーブルの値はその子テーブルのものとしてだけ数えられます",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_value_size_checks_measure_an_inheritance_parents_own_rows(lang: str) -> None:
    # The code first: the probe behind the prerequisite size check and Check sizes (the
    # exclusion popup calls the same method) asks the Full Load's own live rule.
    probes = ROOT / "src/dsql_migrator/ui/prerequisite_probes.py"
    measure = _function(probes, "oversized_values")
    assert "if pg_reads_own_rows_only(connection, table_name):" in measure
    assert 'qualified = f"ONLY {qualified}"' in measure
    dialog = (ROOT / "src/dsql_migrator/ui/data_migration/_exclusion_dialog.py").read_text(
        encoding="utf-8"
    )
    assert "probe.oversized_values" in dialog
    excluding = _page(lang, _PAGE["13"])
    for phrase in _SIZE_FROM_ONLY[lang]:
        assert _has(lang, excluding, phrase), (lang, phrase)


# -- f[5]: a CHECKSUM-proven table is Verified whatever else holds it -----------------------

_VERIFIED = {
    "en": "once it does the CDC step shows it as *Verified by CHECKSUM*; if an open source-schema "
    "or REPLICA IDENTITY finding still holds it at Cut over, that notice names the finding, which "
    "keeps its own fix (no reload is needed for the dead letters)",
    "ko": "일치하면 CDC 단계가 그 테이블을 *Verified by CHECKSUM*으로 표시합니다. 열린 소스 스키마나 "
    "REPLICA IDENTITY 발견 사항이 아직 Cut over에서 그 테이블을 붙잡고 있으면 그 알림이 그 발견 사항을 "
    "알려 주며, 그 발견 사항은 자체의 해결 방법을 따릅니다(dead letter 때문에 리로드할 필요는 없음)",
    "ja": "一致すると CDC ステップはそのテーブルを *Verified by CHECKSUM* と表示します。未解決のソース"
    "スキーマや REPLICA IDENTITY の検出がまだ Cut over でそのテーブルを保留している場合は、その通知が"
    "その検出を示し、その検出はそれ自身の対処法に従います (デッドレターのためにリロードする必要はありません)",
}
_STALE_VERIFIED = {
    "en": ("unless an open source-schema or REPLICA IDENTITY finding still holds it",),
    "ko": ("아직 그 테이블을 붙잡고 있지 않는 한",),
    "ja": ("まだそのテーブルを保留していない限り",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_checksum_proven_table_is_verified_and_its_open_finding_named(lang: str) -> None:
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_open_finding_holds, cdc_dlq_resolved_body

    # The code first: an open REPLICA IDENTITY finding no longer keeps a proven table out of
    # the Verified notice; the notice names it.
    state = SimpleNamespace(
        cdc_replica_identity_findings={"app.orders": SimpleNamespace(restored_at=None)},
        cdc_catalog_drift_findings={},
    )
    held = cdc_dlq_open_finding_holds(state, ["app.orders"])
    assert held == {"app.orders": ("REPLICA IDENTITY",)}
    proven = {"app.orders": datetime(2026, 10, 11, 9, 30, tzinfo=timezone.utc)}
    body = cdc_dlq_resolved_body(proven, held)
    assert body.endswith("Cut over still holds app.orders for its open REPLICA IDENTITY finding "
                         "(see that notice above).")
    monitoring = (ROOT / "src/dsql_migrator/ui/data_migration/_cdc_monitoring.py").read_text(
        encoding="utf-8"
    )
    assert "cdc_dlq_verified_tables" not in monitoring  # the open-finding exclusion is gone
    assert 'header="Verified by CHECKSUM",' in monitoring

    limits = _page(lang, _PAGE["06"])
    assert _has(lang, limits, _VERIFIED[lang]), lang
    assert not _stale(lang, _STALE_VERIFIED[lang]), lang
