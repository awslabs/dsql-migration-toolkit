# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation's ``CDC_TOPIC_NAME``: the tables CDC cannot stream because of their Kafka topic.

CDC names each table's Kafka topic ``dsqlcdc.<database or schema>.<table>``, and Kafka
refuses a name holding a character other than ASCII letters, digits, '.', '_' and '-', and
the second of two names that differ only in a '.' against a '_'. Data Migration refused
such a capture only once CDC was being set up; Evaluation said nothing, so the report read
"no issues" over a table CDC could not stream. The finding says it at planning time, as
advice (RECOMMENDATION, like FK_PRESERVED): a Full load only migration is not affected, so
the table stays AUTO, is not an AI candidate and adds no effort.

Covers:
- a '.'/'_' pair (both engines), a group of three, and a name Kafka refuses: one advisory
  finding on each table, worded as the design says, the item AUTO with no effort;
- a real gap on the same table still governs the item and its effort;
- a '.' inside a table name alone gets nothing (it streams on sink plugin v57);
- not an AI candidate, registered for both engines;
- it names exactly the tables Data Migration refuses on a v57 stack (the real builders);
- the Evaluation screen and the text and HTML exports render it as they render FK_PRESERVED.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import (
    CdcTopicNameRule,
    CompatibilityAssessor,
    default_rules,
    render_html_report,
    render_text_report,
)
from dsql_migrator.core.cdc import topic_name_refusal, topic_name_refused_tables
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    EffortLevel,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.ui.schema_conversion_apply import ai_candidate_object_names
from tests.test_g128_deploy_review import _start
from tests.test_ui_evaluation import _ItemUi

RULE = "CDC_TOPIC_NAME"
_ENGINES = {"mysql": SourceType.MYSQL, "postgres": SourceType.POSTGRES}

_NOT_FULL_LOAD = "A Full load only migration is not affected."
_PAIR_FIX = (
    "With Full load + CDC or CDC only, capture only one of them: copy the other in a Full "
    "load only migration once its writes stop, or rename one at the source."
)
_ILLEGAL_FIX = (
    "With Full load + CDC or CDC only, leave it out of the CDC pipeline: copy it in a Full "
    "load only migration once its writes stop, or rename it at the source."
)


def _collision(listed: str) -> str:
    return (
        f"CDC can stream only one of {listed}: each table's Kafka topic is named "
        "dsqlcdc.<database or schema>.<table>, and Kafka does not allow two topics whose "
        f"names differ only in a '.' against a '_'. {_NOT_FULL_LOAD}"
    )


def _illegal(shown: str) -> str:
    return (
        f"CDC cannot stream {shown}: its Kafka topic is named after its database or schema "
        "and its table, and Kafka allows only ASCII letters, digits, '.', '_' and '-'. "
        f"{_NOT_FULL_LOAD}"
    )


def _table(name: str, *, primary_key=("id",)) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=list(primary_key),
    )


def _assess(names, engine="mysql", **tables):
    inventory = SourceInventory(tables=[tables.get(n) or _table(n) for n in names])
    return CompatibilityAssessor(source_type=_ENGINES[engine]).assess(inventory)


def _item(report, name):
    (item,) = [item for item in report.items if item.object_name == name]
    return item


def _topic_concerns(report, name):
    return [c for c in _item(report, name).concerns if c.rule_id == RULE]


def _flagged(report) -> set:
    return {
        item.object_name
        for item in report.items
        if any(c.rule_id == RULE for c in item.concerns)
    }


# ---------------------------------------------------------------------------
# 1. The finding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_a_dot_underscore_pair_is_advice_on_each_member_and_both_stay_automatic(engine) -> None:
    # FAILS WITHOUT THE FIX: no rule judged a table's topic name, so both members read AUTO
    # "No DSQL compatibility issues detected." and the pair surfaced only at Deploy / Start.
    db = "shop" if engine == "mysql" else "app"
    dotted, underscored, plain = f"{db}.orders.v2", f"{db}.orders_v2", f"{db}.orders"
    report = _assess([dotted, underscored, plain], engine)
    for name in (dotted, underscored):
        item = _item(report, name)
        (concern,) = _topic_concerns(report, name)
        assert concern.risk == _collision(f"{dotted} and {underscored}")
        assert concern.recommendation == _PAIR_FIX
        # Advice, filed as FK_PRESERVED is: MANUAL in name only, RECOMMENDATION in kind.
        assert concern.note_kind is ConversionNoteKind.RECOMMENDATION and concern.is_advisory
        assert concern.classification is Classification.MANUAL
        assert concern.effort is None
        # ... so the table stays AUTO with no effort to budget.
        assert item.classification is Classification.AUTO
        assert item.effort is None
        assert [c.rule_id for c in item.concerns] == [RULE]
    # The plain name has no twin: nothing to say.
    assert _item(report, plain).rule_id == "COMPATIBLE"
    assert report.summary[Classification.AUTO] == 3
    assert sum(report.effort_summary.values()) == 0


def test_a_group_of_three_names_every_member_and_asks_to_keep_one() -> None:
    # FAILS WITHOUT THE FIX: no finding at all.
    names = ["a.b_c", "a_b.c", "a.b.c"]
    report = _assess(names)
    for name in names:
        (concern,) = _topic_concerns(report, name)
        assert concern.risk == _collision("a.b.c, a.b_c and a_b.c")
        assert concern.recommendation == (
            "With Full load + CDC or CDC only, capture only one of them: copy the others in a "
            "Full load only migration once their writes stop, or rename all but one at the "
            "source."
        )


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_a_name_kafka_refuses_is_advice_that_shows_what_does_not_show(engine) -> None:
    # FAILS WITHOUT THE FIX: no finding, so the report read "Ready" over a table CDC cannot
    # stream at all.
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.order$items", f"{db}.a b", f"{db}.주문", f"{db}.or\u200bders", f"{db}.ok-2"]
    report = _assess(names, engine)
    shown = {
        f"{db}.order$items": f"{db}.order$items",
        f"{db}.a b": f"{db}.a<U+0020>b",
        f"{db}.주문": f"{db}.주문",
        f"{db}.or\u200bders": f"{db}.or<U+200B>ders",
    }
    for name, text in shown.items():
        (concern,) = _topic_concerns(report, name)
        assert concern.risk == _illegal(text)
        assert concern.recommendation == _ILLEGAL_FIX
        assert concern.is_advisory and concern.effort is None
        assert _item(report, name).classification is Classification.AUTO
    # '-' is a character Kafka allows.
    assert not _topic_concerns(report, f"{db}.ok-2")


def test_a_real_gap_on_the_same_table_still_governs_it_and_its_effort() -> None:
    # The advice sorts after the gap and adds nothing to the object's effort.
    report = _assess(
        ["shop.orders.v2", "shop.orders_v2"],
        **{"shop.orders_v2": _table("shop.orders_v2", primary_key=())},
    )
    item = _item(report, "shop.orders_v2")
    assert item.classification is Classification.UNSUPPORTED
    assert item.rule_id == "NO_PRIMARY_KEY"
    assert [c.rule_id for c in item.concerns][-1] == RULE
    gap_efforts = [c.effort for c in item.concerns if not c.is_advisory and c.effort]
    assert item.effort is not None and item.effort in gap_efforts


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_a_dot_inside_a_table_name_alone_gets_no_finding(engine) -> None:
    # Sink plugin v57 streams it, and a '.' inside a database or schema name is
    # DOTTED_SCHEMA_NAME's (the catalog read's), not this rule's.
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders.v2", f"{db}.{db}.orders", f"{db}.a.b.c", f"{db}.orders"]
    report = _assess(names, engine)
    assert _flagged(report) == set()
    for name in names:
        assert _item(report, name).rule_id == "COMPATIBLE"
    assert CdcTopicNameRule().evaluate(SourceInventory(tables=[_table(n) for n in names])) == []


def test_views_are_not_judged() -> None:
    # CDC captures tables only.
    from dsql_migrator.core.models import ViewDef

    inventory = SourceInventory(
        tables=[_table("shop.orders")],
        views=[ViewDef(name="shop.a b", definition="SELECT 1"),
               ViewDef(name="shop.orders_x", definition="SELECT 1")],
    )
    assert CdcTopicNameRule().evaluate(inventory) == []


# ---------------------------------------------------------------------------
# 2. Where it counts: AI candidates, registration, Data Migration's refusal
# ---------------------------------------------------------------------------


def test_a_table_with_only_this_advice_is_not_an_ai_candidate() -> None:
    report = _assess(["shop.orders.v2", "shop.orders_v2", "shop.a b"])
    assert _flagged(report) == {"shop.orders.v2", "shop.orders_v2", "shop.a b"}
    assert ai_candidate_object_names(report) == []


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_the_rule_is_registered_once_for_each_engine(engine) -> None:
    # FAILS WITHOUT THE FIX: neither list had it.
    rules = default_rules(_ENGINES[engine])
    assert [type(r) for r in rules].count(CdcTopicNameRule) == 1
    assert [r.rule_id for r in rules].count(RULE) == 1


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_it_names_exactly_the_tables_data_migration_refuses_on_a_v57_stack(engine) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = [
        f"{db}.orders", f"{db}.orders.v2", f"{db}.orders_v2", f"{db}.order$items",
        f"{db}.a b", f"{db}.ok_2", f"{db}.a.b", f"{db}.v2_users", f"{db}_v2.users",
        f"{db}.x.y.z",
    ]
    report = _assess(names, engine)
    # SinkTopics as the real Start CDC builder sends them, judged as Data Migration judges
    # CDC infrastructure deployed with this version.
    filled = _start(engine, names).filled
    refused = topic_name_refused_tables(filled, sink_plugin="v57")
    assert _flagged(report) == set(refused) == {
        f"{db}.orders.v2", f"{db}.orders_v2", f"{db}.order$items", f"{db}.a b",
        f"{db}.v2_users", f"{db}_v2.users",
    }
    # And a capture of the tables with no finding is one Data Migration accepts.
    clean = [name for name in names if name not in _flagged(report)]
    assert topic_name_refusal(_start(engine, clean).filled, sink_plugin="v57") is None


# ---------------------------------------------------------------------------
# 3. It reads as FK_PRESERVED does: the screen and both exports
# ---------------------------------------------------------------------------


def test_the_evaluation_screen_renders_it_as_a_recommendation() -> None:
    from dsql_migrator.ui.evaluation import _render_assessment_item, assessment_concern_counts

    item = _item(_assess(["shop.orders.v2", "shop.orders_v2"]), "shop.orders_v2")
    # The collapsed row: one "Recommended" badge in the calm info tone, as FK_PRESERVED.
    assert assessment_concern_counts(item) == [("Recommended", 1, "info")]
    ui = _ItemUi()
    _render_assessment_item(ui, item)
    ui.open(item.object_name)
    (concern,) = item.concerns
    # The card: RECOMMENDED, a "Note" (never "Risk"), its recommendation, no effort badge.
    assert "RECOMMENDED" in ui.badges and RULE in ui.texts
    assert "Note" in ui.texts and "Risk" not in ui.texts
    assert concern.risk in ui.texts and concern.recommendation in ui.texts
    assert "Recommendation" in ui.texts
    assert not [b for b in ui.badges if b.startswith("effort")]
    assert sum("bg-sky-50" in c for c in ui.classes) == 1


def test_the_text_and_html_exports_label_it_recommended() -> None:
    report = _assess(["shop.orders.v2", "shop.orders_v2"])
    text = render_text_report(report)
    assert f"- shop.orders_v2 [AUTO] (effort: NONE) ({RULE})" in text
    assert f"1. [RECOMMENDED] {RULE}\n" in text
    assert f"Note: {_collision('shop.orders.v2 and shop.orders_v2')}" in text
    assert f"Fix:  {_PAIR_FIX}" in text
    html = render_html_report(report)
    assert ">RECOMMENDED</td>" in html and f"<td>{RULE}</td>" in html
    assert "<td>-</td>" in html  # no effort of its own
