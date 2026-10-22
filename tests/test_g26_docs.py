# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 (docs): the manual, in English, Korean and Japanese alike, states what G26 shipped, and
what it said before is pinned as stale so it cannot come back.

* Item [2]: a first MySQL Start that CloudFormation rolled back. The rollback restores the
  stack's parameters, but the stack's CloudFormation events still hold the start point that
  Start offered (the offset seeder's properties), and the stack-phase probe reads it back. So
  after an app restart, or for a Start made in another session, the next Start is judged as
  before it: a later point is refused, a retry from the same one is not, and an earlier one is
  refused at the click. Only when the events do not give it (they cannot be read, or the Start
  seeded Kafka in the app, as the single EC2 host does) is every start point refused, under a
  header that does not call that point newer. Chapters 3, 4 and 6 said the offer lived only in
  the app's memory, so that every start point was refused after any restart.
* Item [1]: with CDC only, Start CDC's refusal of the Full Load's watermark (the newer start
  point refusal, and the one for an offer no longer known) names the Drop & reload condition of
  its new-stack route itself, with the type switch a re-run needs. Chapter 3 says so and quotes
  the route's one-line form.
* Item [4]: the chapter 6 lead let the reloaded tables converge in the PostgreSQL case where
  Debezium snapshots every selected table first. There a row deleted on the source between the
  reload and that snapshot stays on the target, as the code's own copy says.
* Items [0] and [3] change copy no manual page describes (the Full load only completion notice
  on an unread CDC state, the Validation AI's grounding), and item [5] is test-only: nothing to
  pin for them.

G26's repair of what its verified review confirmed (task R-DOCS, at the end of this file):

* The Start CDC job reads a rolled-back first Start's start point from the stack's events
  itself, when its own read of the stack shows the rollback and this session holds no such
  point, and then makes the click's earlier-offer check. So chapter 3's "for a Start made in
  another session, as it reads it from those events" holds on the job's path too, and an
  earlier point is refused there by the job when this session had not read that point before
  the click: chapter 3 said only the click refused it. The notice for a start point no longer
  known says this session holds no record of it, names an app restart and a Start made
  outside this session only as possible reasons, and says the events did not give it only
  after a read; chapter 3 quotes only its unchanged start.
* Chapter 3 §3.7 named the Full Load retry "retry failed tables" (in ko and ja, a translation
  of it), a label no button has. The button is Retry unfinished tables, and it resets every
  unfinished table it retries to pending: a table the run never reached too, not only a failed
  one.

Each check reads the CODE first (its own constants and the real derivations, the real probe
over a CloudFormation double) and only then the manual text, with the hard wrap undone (in ja,
with no whitespace at all).
"""

from __future__ import annotations

import re

import pytest

from tests.test_g25_docs import _CDC_UI, DEPLOY, LANGS, ROOT, _has, _page
from tests.test_g25_docs_r4 import _stale
from tests.test_g25_docs_r7 import _joined
from tests.test_g25_docs_r8 import _check

_P03 = "03-full-load.md"
_P04 = "04-cdc-and-dsql-constraints.md"
_P06 = "06-limitations.md"
_UNKNOWN_LEAD = (
    "CloudFormation rolled back this stack's last Start, and the start offset that Start offered "
    "is no longer known"
)


def _entry(template: str, name: str) -> str:
    """The CloudFormation template's top-level entry ``name`` (a condition or a resource), up to
    the next one."""
    body = template[template.index(f"\n  {name}:\n") + 1 :]
    following = re.search(r"\n  [^\s#]", body)
    return body[: following.start()] if following else body


# -- item [2]: a rolled-back first Start's start point, read back from the stack's events ----

_OFFER_READ = {
    _P03: {
        "judged": {
            "en": "After a first Start that CloudFormation rolled back, the rollback restores the "
            "stack's parameters, so they no longer hold the start point that Start offered, but "
            "the stack's CloudFormation events still do (the offset seeder's properties): the app "
            "judges the next Start against that point as it recorded it, or, after an app "
            "restart (or for a Start made in another session), as it reads it from those events.",
            "ko": "CloudFormation이 롤백한 첫 Start 뒤에는 롤백이 스택의 파라미터를 되돌리므로 파라미터에는 "
            "그 Start가 준 시작 지점이 더 이상 없지만, 스택의 CloudFormation 이벤트에는 아직 남아 "
            "있습니다(오프셋 시더의 속성): 앱은 다음 Start를 자신이 기록한 그 지점과 비교하고, 앱을 재시작한 "
            "뒤에는(또는 다른 세션에서 한 Start라면) 그 이벤트에서 읽은 그 지점과 비교합니다.",
            "ja": "CloudFormation がロールバックした初回の Start の後は、ロールバックがスタックのパラメータを"
            "元に戻すため、パラメータにはその Start が渡した開始位置がもう残っていませんが、スタックの "
            "CloudFormation イベントにはまだ残っています (オフセットシーダーのプロパティ): アプリは次の Start "
            "を、自分が記録したその位置と比較し、アプリの再起動後 (または別のセッションで行った Start の"
            "場合) はそのイベントから読み取ったその位置と比較します。",
        },
        # The "verdicts" pin (a later point refused, a retry not, an earlier one refused) moved
        # to _EARLIER_BY_JOB below: the G26 repair added the job's refusal of an earlier point.
        "unknown": {
            "en": "When the events do not give that point either — they cannot be read, for "
            "example, or that Start seeded Kafka in the app instead of through the offset-seeder "
            "Lambda, as the single EC2 host does "
            "([§1.4](01-setup.md#14-run-on-a-single-ec2-host-from-source)) — no start point can "
            "be checked against it, so every start point is refused on that stack, that same one "
            "included (*This CDC stack's last start point is not known*: *CloudFormation rolled "
            "back this stack's last Start, and the start offset that Start offered is no longer "
            "known …*), with the same route.",
            "ko": "이벤트에서도 그 지점을 얻지 못하면 — 예를 들어 이벤트를 읽을 수 없거나, 그 Start가 오프셋 "
            "시더 Lambda 대신 앱 안에서 Kafka를 시드했다면(단일 EC2 호스트가 그렇게 합니다, "
            "[§1.4](01-setup.md#14-단일-ec2-호스트에서-실행-소스에서)) — 어떤 시작 지점도 그 지점과 비교할 수 "
            "없으므로, 그 스택에서는 같은 시작 지점을 포함해 모든 시작 지점이 같은 경로와 함께 "
            "거부됩니다(*This CDC stack's last start point is not known*: *CloudFormation rolled back "
            "this stack's last Start, and the start offset that Start offered is no longer "
            "known …*).",
            "ja": "イベントからもその位置が得られない場合 — たとえばイベントを読み取れない場合や、その Start "
            "がオフセットシーダーの Lambda ではなくアプリの中で Kafka をシードした場合 (単一 EC2 ホストは"
            "そうします。[§1.4](01-setup.md#14-単一-ec2-ホストで実行する-ソースから)) — は、どの開始位置も"
            "その位置と比較できないため、そのスタックでは同じ開始位置も含めてすべての開始位置が同じ方法と"
            "ともに拒否されます (*This CDC stack's last start point is not known*: *CloudFormation "
            "rolled back this stack's last Start, and the start offset that Start offered is no "
            "longer known …*)。",
        },
    },
    _P04: {
        "judged": {
            "en": "After a first Start that CloudFormation rolled back, it refuses a later point "
            "than that Start offered, after an app restart too (the rollback restores the stack's "
            "parameters, but the app reads that point from the stack's CloudFormation events), "
            "and every start point when those events do not give it either.",
            "ko": "CloudFormation이 롤백한 첫 Start 뒤에는 그 Start가 준 지점보다 뒤의 시작 지점을 앱을 "
            "재시작한 뒤에도 거부하고(롤백이 스택의 파라미터를 되돌리지만, 앱은 그 지점을 스택의 "
            "CloudFormation 이벤트에서 읽습니다), 그 이벤트에서도 그 지점을 얻지 못하면 모든 시작 지점을 "
            "거부합니다.",
            "ja": "CloudFormation がロールバックした初回の Start の後は、その Start が渡した位置より後の開始"
            "位置をアプリの再起動後も拒否し (ロールバックがスタックのパラメータを元に戻しますが、アプリは"
            "その位置をスタックの CloudFormation イベントから読み取ります)、そのイベントからもその位置が"
            "得られなければ、すべての開始位置を拒否します。",
        },
    },
    _P06: {
        "judged": {
            "en": "On MySQL, after a first Start that CloudFormation rolled back, the rollback "
            "restores the stack's parameters, so Start CDC judges the next Start against the "
            "start point that Start offered as the app recorded it or, after an app restart, as "
            "the stack's CloudFormation events still record it: it refuses a later point than "
            "that one (a retry from it is not refused), and every start point on that stack when "
            "neither gives it (for example after a restart on the single EC2 host, which seeds "
            "Kafka in the app).",
            "ko": "MySQL에서 CloudFormation이 롤백한 첫 Start 뒤에는 롤백이 스택의 파라미터를 되돌리므로, "
            "Start CDC는 다음 Start를 앱이 기록한 그 Start의 시작 지점과, 앱을 재시작한 뒤에는 스택의 "
            "CloudFormation 이벤트에 아직 기록된 그 지점과 비교합니다: 그 지점보다 뒤의 시작 지점을 "
            "거부하고(그 지점에서 다시 시작하는 것은 거부하지 않음), 둘 다 그 지점을 주지 못하면(예를 들어 "
            "앱 안에서 Kafka를 시드하는 단일 EC2 호스트에서 재시작한 뒤) 그 스택에서 모든 시작 지점을 "
            "거부합니다.",
            "ja": "MySQL では、CloudFormation がロールバックした初回の Start の後、ロールバックがスタックの"
            "パラメータを元に戻すため、Start CDC は次の Start を、アプリが記録したその Start の開始位置と、"
            "アプリの再起動後はスタックの CloudFormation イベントにまだ記録されているその位置と比較します: "
            "その位置より後の開始位置を拒否し (その位置からの再試行は拒否しません)、どちらからもその位置が"
            "得られなければ (たとえばアプリの中で Kafka をシードする単一 EC2 ホストで再起動した後)、その"
            "スタックですべての開始位置を拒否します。",
        },
    },
}
# What G25 round 8 said: the offer lived only in the app's memory, so after any app restart (or
# for a Start made in another session) every start point was refused, a retry included.
_OFFER_READ_STALE = {
    "en": (
        "so the start point that Start offered is kept only in the app's memory",
        "After an app restart (or for a Start made in another session) no start point can be "
        "checked against it",
        "and every start point once the app has restarted: the stack does not record that offer",
        "which the app holds in memory: until the app restarts, Start CDC refuses a later point",
        "after a restart, it refuses every start point on that stack",
    ),
    "ko": (
        "그 Start가 준 시작 지점은 앱의 메모리에만 남습니다",
        "앱을 재시작한 뒤에는(또는 다른 세션에서 한 Start라면) 어떤 시작 지점도 그 지점과 비교할 수 "
        "없으므로",
        "앱을 재시작한 뒤에는 모든 시작 지점을 거부합니다: 스택은 그 지점을 기록하지 않기 때문입니다",
        "앱이 그 지점을 메모리에 들고 있습니다",
        "재시작한 뒤에는 그 스택에서 모든 시작 지점을 거부합니다",
    ),
    "ja": (
        "その Start が渡した開始位置はアプリのメモリにしか残らないからです",
        "アプリの再起動後 (または別のセッションで行った Start の場合) は、どの開始位置もその位置と比較"
        "できないため",
        "アプリの再起動後はすべての開始位置を拒否します: スタックはその位置を記録しないためです",
        "アプリがそれをメモリに保持します",
        "再起動の後は、そのスタックですべての開始位置を拒否します",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_first_starts_point_is_read_back_from_the_stacks_events(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.cdc_stack_deployer import SEEDER_RESOURCE_ID
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER, OFFER_UNKNOWN_HEADER
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_g25_cdc_r7 import _W0, _W1, _W2
    from tests.test_g26_cdc import _restarted_reading, _rolled_back_start

    mysql = SourceType.MYSQL
    # The code first. The offset seeder's custom resource carries the start offset a Start
    # offers as its properties, and only a MySQL Start seeded by the Lambda creates it: the
    # single EC2 host seeds Kafka in the app (SeedMode=External).
    template = (DEPLOY / "cdc-stack" / "cdc-stack.yaml").read_text(encoding="utf-8")
    seeder = _entry(template, SEEDER_RESOURCE_ID)
    assert "Condition: DeployStartPrepResource" in seeder
    for prop in ("WatermarkBinlogFile:", "WatermarkBinlogPos:"):
        assert prop in seeder, prop
    assert "- Condition: SeedByLambda" in _entry(template, "DeployStartPrepResource")
    ec2 = (DEPLOY / "cloudformation-ec2.yaml").read_text(encoding="utf-8")
    assert "DSQL_MIGRATOR_CDC_SEED_MODE=external" in ec2
    # After an app restart (a new state), the REAL stack-phase probe over the REAL deployer
    # reads that offer from the rolled-back update's events, and the next Start is judged
    # against it: the retry is taken, a later point refused, an earlier one refused at the
    # click.
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    assert state.cdc_offered_seed == ("mysql-bin.000090", 4)
    assert _cdc_ui.newer_start_point_refusal(state, _W1, mysql) is None
    assert _cdc_ui.newer_start_point_refusal(state, _W2, mysql)[0][2] == NEWER_POINT_HEADER
    earlier = _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, mysql)
    assert "that offset stays seeded and the seeder never rewinds" in earlier
    assert 'header="This start point cannot be applied", body=_seed_refusal' in _joined(_CDC_UI)
    # Events that do not give it -- unreadable, or a Start seeded in the app (no seeder
    # resource): every start point is refused, that same one included, under a header that does
    # not call it newer.
    for pages in (RuntimeError("AccessDenied"), [_rolled_back_start(seeder=False)]):
        state, _cfn = _restarted_reading(monkeypatch, pages)
        assert state.cdc_offered_seed is None
        for seed in (_W0, _W1, _W2):
            (_tone, _icon, header, body), _line = _cdc_ui.newer_start_point_refusal(
                state, seed, mysql
            )
            assert header == OFFER_UNKNOWN_HEADER and body.startswith(_UNKNOWN_LEAD), seed
            assert "it could not be read from the stack's events" in body
    assert OFFER_UNKNOWN_HEADER == "This CDC stack's last start point is not known"

    _check(lang, _OFFER_READ)
    page = _page(lang, _P03)
    for quoted in (OFFER_UNKNOWN_HEADER, _UNKNOWN_LEAD, "This start point cannot be applied"):
        assert _has(lang, page, f"*{quoted}"), (lang, quoted)
    assert not _stale(lang, _OFFER_READ_STALE[lang]), lang


# -- item [1]: the CDC only refusal names its route's Drop & reload condition ----------------

_CONDITION = {
    _P03: {
        "condition": {
            "en": "The CDC only route starts the new stack from the Full Load's watermark, so when "
            "the refused point is that watermark (Automatic), the refusal adds that route's "
            "condition: if that Full Load appended to rows already on the target, run it again "
            "choosing **Drop & reload** before that Start — CDC only has no Full Load step, so "
            "switch the migration type to Full load + CDC for that run, whose Full Load makes the "
            "CDC start point exact (an append keeps those rows as they were, and CDC from a "
            "watermark carries only the changes after it).",
            "ko": "CDC only 경로는 새 스택을 Full Load의 워터마크에서 시작하므로, 거부된 지점이 그 "
            "워터마크(Automatic)이면 거부가 그 경로의 조건을 덧붙입니다: 그 Full Load가 타깃에 이미 있던 "
            "행에 append했다면 그 Start 전에 **Drop & reload**로 다시 실행하세요. CDC only에는 Full Load "
            "단계가 없으므로 그 실행 동안 마이그레이션 유형을 Full load + CDC로 바꿉니다. 그 실행의 "
            "Full Load가 CDC 시작 지점을 정확하게 맞춥니다(append는 그 행을 그대로 두고, 워터마크에서 "
            "시작한 CDC는 그 뒤의 변경만 가져옵니다).",
            "ja": "CDC only の方法は新しいスタックを Full Load のウォーターマークから開始するため、拒否された"
            "位置がそのウォーターマーク (Automatic) の場合は、拒否がその方法の条件を付け加えます: その "
            "Full Load がターゲットに既にあった行に append した場合は、その Start の前に **Drop & reload** "
            "でもう一度実行してください。CDC only には Full Load ステップがないため、その実行の間は"
            "マイグレーションの種類を Full load + CDC に切り替えます。その実行の Full Load が CDC の"
            "開始位置を正確にします (append はそれらの行をそのまま残し、ウォーターマークからの CDC は"
            "その後の変更しか運びません)。",
        },
        "line": {
            "en": "In the disabled Start's tooltip and the job's error, the route reads",
            "ko": "비활성화된 Start의 툴팁과 작업의 오류에서는 그 경로가",
            "ja": "無効になった Start のツールチップとジョブのエラーでは、その方法は",
        },
    },
}
# G25 round 8's sentence: the condition only after a Full load only re-run, with no word of the
# type switch CDC only needs for that run (it has no Full Load step).
_CONDITION_STALE = {
    "en": (
        "so after a Full load only re-run that appended to rows already on the target, run it "
        "again choosing **Drop & reload** before that Start",
    ),
    "ko": ("Full load only 재실행이 타깃에 이미 있던 행에 append했다면 그 Start 전에 **Drop & reload**로",),
    "ja": (
        "Full load only の再実行がターゲットに既にあった行に append した場合は、その Start の前に "
        "**Drop & reload** でもう一度実行してください",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_cdc_only_refusal_names_its_routes_drop_and_reload_condition(lang: str) -> None:
    from dsql_migrator.ui import cdc_start_point_gate as gate
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._models import MigrationType

    cdc_only, flc = MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC
    point = "the Full Load watermark (binlog mysql-bin.000100:4)"
    # The code first: only CDC only's route from the Full Load's watermark (Automatic) names
    # it -- a Manual point starts the new stack from that point, and Full load + CDC's route
    # already re-runs the Full Load with Drop & reload.
    assert gate.reloads_first(cdc_only, "full_load")
    assert not gate.reloads_first(cdc_only, "manual")
    assert not gate.reloads_first(flc, "full_load")
    assert (
        "run it again choosing 'Drop & reload' before that Start (CDC only has no Full Load step: "
        "switch the migration type to Full load + CDC for that run, whose Full Load makes the CDC "
        "start point exact)"
        in gate.RELOAD_FIRST
    )
    # Both refusals: the newer start point (on a stopped stack or a rolled-back Start) and the
    # one whose offer is no longer known -- the notice the cards, the dialog and the click show,
    # and the one-line form of the tooltip and the job.
    for rolled_back in (False, True):
        body = gate.newer_point_notice(
            point=point, last="binlog mysql-bin.000090:4", rolled_back=rolled_back,
            migration_type=cdc_only,
        )[3]
        assert gate.new_topics_route(cdc_only) in body and gate.RELOAD_FIRST in body
    notice, unknown_line = _cdc_ui._offer_unknown_refusal(point, cdc_only)
    assert gate.RELOAD_FIRST in notice[3]
    route = gate._RELOAD_FIRST_ROUTE
    for line in (gate.newer_point_line(cdc_only), unknown_line):
        assert line.endswith(f": {route}. Nothing was changed."), line
        assert len("Start CDC blocked: " + line) <= 300
    # The job's line chapter 3 quotes still begins as quoted.
    assert gate.newer_point_line(cdc_only).startswith(
        "This CDC stack has streamed, so Start CDC does not move it forward to a newer start point"
    )
    assert gate.RELOAD_FIRST not in gate.newer_point_notice(
        point=point, last="binlog mysql-bin.000090:4", rolled_back=False, migration_type=flc,
    )[3]

    _check(lang, _CONDITION)
    assert _has(lang, _page(lang, _P03), f"*{route}*"), lang
    assert not _stale(lang, _CONDITION_STALE[lang]), lang


# -- item [4]: PostgreSQL, the chapter 6 lead's convergence claim ----------------------------

_PG_LEAD = {
    "en": "so CDC continues from where it stopped if the stack's source connector streamed, "
    "gapless only while the CDC topics still retain what the sink had not applied, and the "
    "reloaded tables converge. Otherwise Debezium snapshots every selected table first, and a "
    "row deleted on the source between the reload and that snapshot stays on the target: "
    "Validation reports it as an EXTRA row.",
    "ko": "스택의 소스 커넥터가 스트리밍했다면 CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지 않은 변경을 "
    "CDC 토픽이 아직 보존하는 동안만 빈틈이 없고, 다시 적재한 테이블은 수렴합니다. 그렇지 않으면 "
    "Debezium이 먼저 선택한 모든 테이블을 스냅샷하므로, 다시 적재한 뒤 그 스냅샷 전에 소스에서 삭제된 "
    "행은 타깃에 남고, Validation이 EXTRA 행으로 보고합니다.",
    "ja": "スタックの source connector がストリーミングしていれば CDC は停止した位置から続き、sink が"
    "適用していなかった変更を CDC トピックがまだ保持している間だけギャップがなく、再ロードしたテーブル"
    "は収束します。そうでなければ Debezium が先に選択したすべてのテーブルをスナップショットするため、"
    "再ロードの後、そのスナップショットの前にソースで削除された行はターゲットに残り、Validation が "
    "EXTRA の行として報告します。",
}
# G25 round 8's sentence: the snapshot case inside the parenthesis, the convergence (and, in ko and
# ja, the retained-topics condition) after it, so both read as holding in the snapshot case too.
_PG_LEAD_STALE = {
    "en": (
        "(otherwise Debezium snapshots every selected table first), gapless only while the CDC "
        "topics still retain what the sink had not applied, and the reloaded tables converge.",
    ),
    "ko": (
        "(그렇지 않으면 Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다. 싱크가 적용하지 않은 변경을 "
        "CDC 토픽이 아직 보존하는 동안만 빈틈이 없음) 다시 적재한 테이블은 수렴합니다.",
    ),
    "ja": (
        "(そうでなければ Debezium が先に選択したすべてのテーブルをスナップショットします。sink が適用"
        "していなかった変更を CDC トピックがまだ保持している間だけギャップがありません)、再ロードした"
        "テーブルは収束します。",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_6_lets_only_a_streamed_source_connectors_reload_converge(lang: str) -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route
    from dsql_migrator.core.cdc_postgres import pg_snapshot_mode
    from dsql_migrator.ui.data_migration import _cdc_ui as cu
    from tests.test_g12_table_set_after_load import _loaded

    # The code first: a Full load only run records no slot, so its watermark's Start sends
    # snapshot.mode=initial, which picks up where streaming stopped only if the source connector
    # streamed, and otherwise snapshots every selected table first. A snapshot carries no
    # delete: a row deleted on the source in between stays on the target, an EXTRA row to
    # Validation -- what the code says of every snapshot of tables already loaded.
    assert pg_snapshot_mode(_loaded().watermark) == "initial"
    card = cu._pg_resume_continues("s", snapshot_never=False)
    assert "If its source connector streamed, it picks up where streaming stopped." in card
    assert "Debezium snapshots every selected table first" in card
    assert (
        "which snapshots every selected table (a row deleted on the source in between stays on "
        "the target; Validation reports it as an EXTRA row)"
        in pg_cdc_route(None, started=True, cdc_only=True)
    )
    screen = _joined(ROOT / "src" / "dsql_migrator" / "ui" / "data_migration" / "__init__.py")
    assert (
        "a row DELETED between the load and the start is in neither the snapshot nor the stream, "
        "so it stays on the target (Validation reports it as an extra row" in screen
    )

    assert _has(lang, _page(lang, _P06), _PG_LEAD[lang]), lang
    assert not _stale(lang, _PG_LEAD_STALE[lang]), lang


# =================================================================================================
# G26 repair (task R-DOCS): what the repair of G26's verified findings made false in the manual
# =================================================================================================

# -- [A] of the repair: the Start CDC job reads the start point and refuses an earlier one ----

_EARLIER_BY_JOB = {
    _P03: {
        "verdicts": {
            "en": "A start point later than that one is refused, a retry from the same one is not, "
            "and an earlier one is refused at the click on Start CDC, or by the Start CDC job when "
            "this session had not read that point yet (*This start point cannot be applied*: that "
            "offset stays seeded, and the seeder never rewinds).",
            "ko": "그보다 뒤의 시작 지점은 거부되고, 같은 시작 지점에서 다시 시작하는 것은 거부되지 않으며, "
            "그보다 이전의 시작 지점은 Start CDC를 클릭할 때, 또는 이 세션이 그 지점을 아직 읽지 않았을 "
            "때는 Start CDC 작업에서 거부됩니다(*This start point cannot be applied*: 그 오프셋은 시드된 "
            "채로 남고, 시더는 되감지 않습니다).",
            "ja": "それより後の開始位置は拒否され、同じ開始位置からの再試行は拒否されず、それより前の開始"
            "位置は Start CDC をクリックした時点で、またはこのセッションがその位置をまだ読み取っていなかった"
            "場合は Start CDC のジョブで拒否されます (*This start point cannot be applied*: そのオフセットは"
            "シードされたまま残り、シーダーは巻き戻さないためです)。",
        },
    },
}
# G26's sentence: the click as the only place that refuses an earlier point. A job submitted from
# a page whose last read of the stack predates the rollback (another session's Start) passes the
# click, and the job refuses it.
_EARLIER_BY_JOB_STALE = {
    "en": (
        "and an earlier one is refused at the click on Start CDC (*This start point cannot be "
        "applied*",
    ),
    "ko": (
        "그보다 이전의 시작 지점은 Start CDC를 클릭할 때 거부됩니다(*This start point cannot be "
        "applied*",
    ),
    "ja": (
        "それより前の開始位置は Start CDC をクリックした時点で拒否されます (*This start point "
        "cannot be applied*",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_an_earlier_point_is_refused_by_the_job_when_this_session_had_not_read_it(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.ui.cdc_start_point_gate import OFFER_UNKNOWN_HEADER, offer_unknown_line
    from dsql_migrator.ui.data_migration import _cdc_status as st
    from tests.test_g25_cdc_r7 import _W0, _W1
    from tests.test_g26_repair_cdc import (
        FL_CDC,
        _EARLIER,
        _LEAD,
        _OLD_CAUSE,
        _click,
        _refusal,
        _stale_page,
    )

    # The code first: the REAL click, job and worker over the REAL deployer and probe (the CDC
    # repair's harness). This session's last read of the stack saw it never started; another
    # session's first Start from W1 has rolled back since, so the click judges without W1. The
    # Start CDC job's own read of the stack shows the rollback, and it reads W1 from the stack's
    # events (one read-only DescribeStackEvents page): the retry from W1 goes ahead.
    cfn, state = _stale_page(monkeypatch)
    run = _click(monkeypatch, cfn, state, _W1)
    assert run["error"] is None and run["submitted"] and cfn.event_calls == [None]
    seed = run["kwargs"]["watermark"]
    assert (seed.binlog_file, seed.binlog_position) == ("mysql-bin.000090", 4)
    # An earlier point passes the click (the page still reads the stack as never started) and
    # the job refuses it, under the header chapter 3 quotes, for the reason it gives.
    cfn, state = _stale_page(monkeypatch)
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W0), state)
    assert not st.cdc_stack_has_streamed(state)
    assert _EARLIER == "This start point cannot be applied"
    assert (tone, header) == ("error", _EARLIER)
    assert first.startswith(f"Start CDC blocked: {_EARLIER}. ")
    assert "that offset stays seeded and the seeder never rewinds" in body
    # When the job's read does not give the point, the notice it records says a read did not
    # give it and asserts no cause: an app restart and a Start made outside this session are
    # named only as possible reasons. Chapter 3 quotes the header and the unchanged start of it.
    cfn, state = _stale_page(monkeypatch)
    cfn.events_error = RuntimeError("Throttling: Rate exceeded")
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W1), state)
    assert cfn.event_calls == [None] and state.cdc_offered_seed is None
    assert first == "Start CDC blocked: " + offer_unknown_line(FL_CDC)
    assert header == OFFER_UNKNOWN_HEADER and body.startswith(_LEAD) and _OLD_CAUSE not in body
    assert _LEAD.startswith(_UNKNOWN_LEAD)
    assert (
        "this session holds no record of that offset (the app may have restarted since that "
        "Start, or that Start may have been made outside this session), and it could not be read "
        "from the stack's events." in _LEAD
    )

    _check(lang, _EARLIER_BY_JOB)
    # The sentence before it still holds on the job's path: it reads the point from the events.
    _check(lang, {_P03: {"judged": _OFFER_READ[_P03]["judged"]}})
    page = _page(lang, _P03)
    for quoted in (OFFER_UNKNOWN_HEADER, _UNKNOWN_LEAD, _EARLIER):
        assert _has(lang, page, f"*{quoted}"), (lang, quoted)
    assert not _stale(lang, _EARLIER_BY_JOB_STALE[lang]), lang
    # No page quotes the cause the notice used to assert.
    assert not _stale(lang, ("does not survive an app restart",)), lang


# -- §3.7: the Full Load retry is named by its button, Retry unfinished tables ---------------

_RETRY_PATH = {
    _P03: {
        "retry-path": {
            # G131: "never redo work" said too much -- an unfinished table reloads from its
            # start; a finished one is what the retry never reloads.
            "en": "The **Retry unfinished tables** path resets only the unfinished tables to "
            "pending, **reuses the original watermark**, and keeps the completed tables — so you "
            "never reload a finished table or lose the CDC handoff point.",
            "ko": "**Retry unfinished tables** 경로는 미완료 테이블만 대기 상태로 리셋하고, **원래 "
            "워터마크를 재사용**하며, 완료된 테이블은 유지합니다 — 끝난 테이블을 다시 적재하거나 CDC "
            "핸드오프 지점을 잃지 않습니다.",
            "ja": "**Retry unfinished tables** の経路は、未完了のテーブルのみを保留(pending)状態にリセット"
            "し、**元のウォーターマークを再利用**し、完了したテーブルはそのまま保持します — したがって、"
            "完了したテーブルをロードし直したり、CDC の引き継ぎ地点を失ったりすることはありません。",
        },
    },
}
# The label no button has (en), and its ko / ja translations, which the manual put in its place.
_RETRY_PATH_STALE = {
    "en": ('The "retry failed tables" path', "resets only the failed tables to pending"),
    "ko": ('"실패 테이블 재시도" 경로', "실패 테이블만 대기 상태로 리셋하고"),
    "ja": ("「失敗したテーブルをリトライ」する経路", "失敗したテーブルのみを保留(pending)状態にリセット"),
}
# Every doc page that tells an operator how to drive the tool. The changelogs are a record of
# what each release shipped, under the label of its day, so they are not scanned.
_OPERATOR_DOCS = (
    *sorted((ROOT / "docs").rglob("*.md")),
    *sorted(DEPLOY.glob("*.md")),
    *sorted(ROOT.glob("README*.md")),
    ROOT / "CONTRIBUTING.md",
    *sorted((ROOT / "connectors").glob("*/README.md")),
)


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_3_names_the_full_load_retry_by_its_button(monkeypatch, lang: str) -> None:
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import MigrationType
    from dsql_migrator.ui.data_migration._full_load_engine import _seed_retry_chunks
    from dsql_migrator.ui.data_migration._models import unsettled_table_names
    from tests.test_g12_table_set_after_load import _c, _job
    from tests.test_g25_fl_r6 import MY, _btn, _failed_b, _render, _state

    # The code first. The Full Load step's recovery row draws the retry as "Retry unfinished
    # tables (N)" (here app.b failed, app.a loaded); no button there names failed tables.
    state = _state(monkeypatch, None, mtype=MigrationType.FULL_LOAD_ONLY)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=MY,
                       status=StepStatus.FAILED)
    assert _btn(ui, "Retry unfinished tables").text == "Retry unfinished tables (1)"
    assert not [b.text for b in ui.btns if "retry failed" in b.text.lower()]
    # Its tables are every one that did not finish (a failed one, and one the run never reached
    # before it ended), and its run resets exactly those to pending and keeps the loaded ones.
    prior = _job("FAILED", [_c("app.a"), _c("app.b", "FAILED"), _c("app.c", "PENDING", attempts=0)])
    unfinished = unsettled_table_names(prior)
    assert unfinished == ["app.b", "app.c"]
    retry = _job("RUNNING", job_id="job-2")
    _seed_retry_chunks(retry, prior.chunks, set(unfinished))
    assert {c.chunk_id: c.status for c in retry.chunks} == {
        "app.a": "DONE", "app.b": "PENDING", "app.c": "PENDING",
    }

    _check(lang, _RETRY_PATH)
    assert not _stale(lang, _RETRY_PATH_STALE[lang]), lang


def test_no_operator_doc_names_the_full_load_retry_failed_tables() -> None:
    # The manual in every language, the deploy guides and the READMEs: none names a "Retry
    # failed tables" button (the Full Load step's is Retry unfinished tables; Schema
    # Conversion's "Retry failed (N)" is another button, and no doc quotes it this way).
    named = [
        path.relative_to(ROOT).as_posix()
        for path in _OPERATOR_DOCS
        if path.is_file()
        and "retry failed tables" in " ".join(path.read_text(encoding="utf-8").split()).lower()
    ]
    assert named == [], named
    assert any(path.name == "03-full-load.md" for path in _OPERATOR_DOCS)
