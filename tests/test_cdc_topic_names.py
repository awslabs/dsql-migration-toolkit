# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC topic names: a captured table whose name CDC cannot stream is refused before anything
is uploaded or created, and warned about before a Full load + CDC load -- as the CDC table cap
is (G128, G131).

CDC names each captured table's Kafka topic ``dsqlcdc.<db>.<table>`` (MySQL) or
``dsqlcdc.<schema>.<table>`` (PostgreSQL) as written. Three kinds of name failed:

- a character Kafka refuses in a topic name (it allows only ASCII letters, digits, '.', '_' and
  '-'). Both topic pre-create paths caught only an existing topic, so such a table failed Start
  CDC with an opaque Kafka error after the infrastructure was deployed (v0.1.578's Known
  issue): a rollback on the Lambda path, about 2 minutes of retries blaming the network on the
  External one. And a comma, or whitespace ending a PostgreSQL table name, was trimmed into a
  legal name, so Start succeeded and the table was never captured (read from the code);
- a '.' inside the database, schema or table name: Kafka takes it, but a sink plugin older than
  v57 splits a qualified name on every '.', so every change to such a table failed, and takes a
  tombstone's table from the topic's last two segments, so a delete on public."public.orders"
  removed the same-keyed row of public.orders (read from the sink's code). From v57 the sink
  reads a qualified name at its first '.' and skips every tombstone, and TableIncludeList
  carries the exact escaped entry (shop\\.orders\\.v2), so a '.' inside a TABLE name streams
  there; on an older or unidentified plugin it stays refused, naming the plugin and Delete +
  Deploy. A table of a database or schema whose name holds a '.' (Evaluation's
  DOTTED_SCHEMA_NAME) and a name with an empty part before its first '.' stay refused on any
  plugin;
- two names that differ only in a '.' against a '_' (app.v2_users, app_v2.users): Kafka refuses
  the second topic, which both seeders pre-create, so Start CDC failed after the deploy.

A character that does not show is written as its code point. A refused name that may be a table
of a database or schema whose name holds a '.', which no step supports (DOTTED_SCHEMA_NAME) --
one of Evaluation's DOTTED_SCHEMA_NAME tables when they are known, else any with two or more '.'
-- gets a sentence saying so before the remedy, and the load-then-untick route before a Full
load + CDC load excepts such a table. Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re
import string
import unicodedata
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core import s3_provision
from dsql_migrator.core.cdc import (
    CDC_DEFAULT_DLQ_TOPIC,
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_TOPIC_NAME_DOTTED_SCHEMA,
    CDC_TOPIC_NAME_REMEDY,
    DOTTED_TABLE_MIN_PLUGIN_VERSION,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    cdc_expected_connector_names,
    cdc_stack_name_is_valid,
    cdc_table_list_params,
    plugin_reads_dotted_tables,
    plugin_supports_sink_keys,
    table_include_entry,
    topic_name_problems,
    topic_name_refusal,
    topic_name_refused_tables,
    unescape_include_entry,
)
from dsql_migrator.core.cdc_deployer import CdcDeployError, run_cdc_start
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType, StepStatus, Watermark
from dsql_migrator.ui.data_migration import (
    MigrationType,
    data_migration_exclusion_block,
    pre_load_topic_name_warning,
    table_set_editable_after_load,
)
from dsql_migrator.ui.data_migration._models import (
    CDC_TABLE_LIST_HEADER,
    CDC_TOPIC_NAME_HEADER,
    PRE_LOAD_SPLIT_CARVE_OUT,
    cdc_pre_load_topic_name_routes,
    cdc_pre_load_topic_name_warning,
    cdc_topic_name_refusal,
    cdc_topic_name_refused,
    topic_name_hold_remedy,
)

from tests.test_cdc_deploy_stages import (
    SINK,
    SRC,
    STACK,
    _FakeDeployer,
    _FakeHandle,
    _FakeS3,
    _logs,
    _run_infra,
    _start_with,
    _statuses,
)
from tests.test_g12_table_set_after_load import _Jobs
from tests.test_g128_deploy_review import _band, _infra, _inventory, _start, _state
from tests.test_g131_table_list_warning import (
    _click_deploy,
    _deploy_ready,
    _fake_nicegui,
    _loaded,
    _render_deploy,
)
from tests.test_ui_data_migration import _RecordingUi

FLC = MigrationType.FULL_LOAD_AND_CDC
_TS = datetime(2026, 10, 1, tzinfo=timezone.utc)
_KAFKA = frozenset(string.ascii_letters + string.digits + "._-")

# One table CDC cannot stream beside one it can (its space is written as its code point).
_NAMES = ["shop.orders", "shop.a b"]
_FACT = (
    "CDC cannot stream shop.a<U+0020>b: each table's Kafka topic is named after its database or "
    "schema and its table, and Kafka allows only ASCII letters, digits, '.', '_' and '-' in a "
    "topic name."
)
_ILLEGAL_FACT = _FACT.removeprefix("CDC cannot stream shop.a<U+0020>b")  # ": each table's ..."


def _old_plugin_fact(version=None):
    """What a table whose name holds a '.' gets on a stack whose sink plugin, ``version``,
    predates v57 (``None``: one this version cannot identify), after its names."""
    deployed = (
        f"it was deployed with CDC sink plugin {version}, which reads" if version else
        "it was deployed with a CDC sink plugin this version cannot identify, which may read"
    )
    return (
        f" on this CDC infrastructure: {deployed} every '.' in a table's qualified name as the "
        "one between its database or schema and its table, so each change to such a table would "
        "fail or land in another table. CDC infrastructure deployed with this version (sink "
        "plugin v57 or later) streams it: Delete CDC infrastructure and deploy it again (on "
        "MySQL, CDC then starts again from the Full Load's start point or your Manual start "
        "point; on PostgreSQL the teardown also drops the replication slot, and the Delete "
        "confirmation says what Start CDC then needs)."
    )


# ... with no plugin given, as every caller that passes none judges it (an unknown plugin is
# judged as the oldest).
_OLD_PLUGIN_FACT = _old_plugin_fact()
_EMPTY_PART_FACT = (
    ": the part of its name before or after its first '.' is empty, so CDC cannot tell its "
    "database or schema from its table."
)
_DOTTED_SCHEMA_FACT = (
    ": Evaluation found a database or schema whose name holds a '.' that each of these names "
    "begins with (DOTTED_SCHEMA_NAME), so CDC cannot tell its changes apart from those of "
    "another table read under the same name."
)
_COLLISION_FACT = (
    ": Kafka does not allow two topics whose names differ only in a '.' against a '_', so CDC "
    "can stream only one of"
)
_REMEDY = (
    "Leave such a table out of this CDC pipeline (it stays on Aurora DSQL as it is, and CDC "
    "and Validation skip it) and copy it in a Full load only migration once its writes stop, "
    "or rename it at the source before migrating it."
)
_REFUSAL = f"{_FACT} {_REMEDY}"
# Said after the facts when a refused name holds two or more '.': it may be a table of a
# database or schema whose name holds a '.', which no step supports (DOTTED_SCHEMA_NAME).
_DOTTED_SCHEMA = (
    "A '.' inside a database or schema name is not supported at any step, so a table of such "
    "a database or schema cannot be copied in a Full load only migration either (Evaluation "
    "reports it Unsupported, DOTTED_SCHEMA_NAME): rename that database or schema at the "
    "source, or leave its tables out of the migration."
)
_ROUTES = (
    "The Full Load can still load such a table: after it, until CDC starts, untick it in the "
    "table list (it stays on Aurora DSQL as loaded; CDC and Validation skip it) and copy it "
    "again in a Full load only migration once its writes stop."
)
_ROUTES_DOTTED = (
    "The Full Load can still load such a table (not one with a '.' inside its database or "
    "schema name): after it, until CDC starts, untick it in the table list (it stays on Aurora "
    "DSQL as loaded; CDC and Validation skip it) and copy it again in a Full load only "
    "migration once its writes stop."
)
_ROUTES_OR = (
    " Or, before the load, rename it at the source and re-run Step 1 (Evaluation), or leave it "
    "out: "
)
_OPEN = "Deselect tables in the table picker."
_FULL_LOAD_LOCK = (
    "Locked — a Full Load has already run for this table set. Use 'Start over' to migrate a "
    "different set of tables."
)
_INFRA_LOCK = (
    "Locked — CDC infrastructure is deployed for this table set, and each table's Kafka topic "
    "partitions are fixed when it is created (so is the sink's task budget), sized for the "
    "tables selected then. A table added now would stream on the plan's lowest partition "
    "count. To change the set, delete the CDC infrastructure on the CDC step first."
)
_MISSING_HEADER = "A source column is missing from the Aurora DSQL table"


def _lists(names, prefix=CDC_DEFAULT_TOPIC_PREFIX):
    """TopicPrefix and SinkTopics as the parameter builders fill them for ``names``."""
    return [("TopicPrefix", prefix), ("SinkTopics", ",".join(f"{prefix}.{n}" for n in names))]


def _counts(names):
    return {name: 1_000 for name in names}


# ---------------------------------------------------------------------------
# 1. The check on its own: Kafka's character set, a '.' inside a name, and Kafka's '.'/'_'
#    collision rule, judged on SinkTopics as sent
# ---------------------------------------------------------------------------

_ACCEPTED = ["shop.orders", "Shop.Order_Items-2", "db2024.t0123456789"]
_REFUSED = [
    "shop.order$items", "shop.order items", "shop.pct%off", "shop.a:b",
    "shop.주문", "shop.注文", "shop.テーブル", "shop.t\U0001f600",
    "shop.a,b",           # one name, not two
    "shop.a,,b",
    "public.orders ",     # never stripped (Kafka Connect and Debezium would trim it)
    "shop.orders\n",      # a set test: a regex's "$" lets a trailing newline through
    "shop.or\tders", "shop.or\u00a0ders", "shop.or\u200bders",  # tab, NBSP, zero width
    "shop.\uff11", "shop.\u00e9",  # a fullwidth digit, a Latin-1 letter
    "판매.orders",          # the database name counts as much as the table's
]
# How the refusal writes the characters of _REFUSED that do not show; every other one as is.
_SHOWN = {" ": "<U+0020>", "\n": "<U+000A>", "\t": "<U+0009>", "\u00a0": "<U+00A0>",
          "\u200b": "<U+200B>"}


def _shown(name):
    return "".join(_SHOWN.get(ch, ch) for ch in name)


def test_names_kafka_accepts_are_not_refused() -> None:
    assert topic_name_refused_tables(_lists(_ACCEPTED)) == []
    assert topic_name_refusal(_lists(_ACCEPTED)) is None


@pytest.mark.parametrize("name", _REFUSED)
def test_each_name_kafka_refuses_is_named_exactly(name) -> None:
    # FAILS WITHOUT THE FIX: no check existed, and Start CDC failed after the deploy.
    assert topic_name_refused_tables(_lists(["shop.orders", name, "shop.items"])) == [name]
    assert topic_name_refusal(_lists([name])).startswith(f"CDC cannot stream {_shown(name)}: ")


def test_a_name_holding_the_prefix_after_a_comma_reads_as_two_legal_names() -> None:
    # The known, accepted gap (named in the docstring): ",dsqlcdc." inside a name.
    assert topic_name_refused_tables(_lists(["shop.a,dsqlcdc.b"])) == []


def test_a_character_is_refused_exactly_when_kafka_refuses_it() -> None:
    # Every code point of the BMP and a sample of the astral planes, at both ends of a name
    # and inside it. A '.' there is a '.' inside the name, refused too (below).
    astral = [*range(0x10000, 0x110000, 0x1111), 0x1F600, 0x10FFFF]
    for code in [*range(0x10000), *astral]:
        ch = chr(code)
        for name in (f"shop.t{ch}", f"{ch}shop.t", f"shop.{ch}t"):
            expected = [] if ch in _KAFKA and ch != "." else [name]
            assert topic_name_refused_tables(_lists([name])) == expected, (hex(code), name)


def test_a_character_that_does_not_show_is_written_as_its_code_point() -> None:
    # FAILS WITHOUT THE FIX: "CDC cannot stream shop.a b: ..." did not show which character of
    # the name was refused when it was a space, a tab or a zero-width space. Whitespace,
    # control and format characters, and unassigned or private-use code points (Unicode
    # categories Z* and C*), over every code point of the BMP and a sample of the astral
    # planes. The tables the check returns stay as written.
    astral = [*range(0x10000, 0x110000, 0x1111), 0x1F600, 0xE0001, 0x10FFFF]
    for code in [*range(0x10000), *astral]:
        ch = chr(code)
        if ch in _KAFKA:
            continue
        name = f"shop.t{ch}"
        shown = f"<U+{code:04X}>" if unicodedata.category(ch)[0] in "ZC" else ch
        assert topic_name_refusal(_lists([name])).startswith(
            f"CDC cannot stream shop.t{shown}: each table's Kafka topic"), hex(code)
        assert topic_name_refused_tables(_lists([name])) == [name], hex(code)


# A '.' inside a table name: refused on a sink plugin older than v57 (or one this version
# cannot identify), streamed from v57 on.
_DOTTED = ["shop.a.b", "my.db.t", "public.public.orders", "shop.t.", "shop..t"]
# The part before the first '.' is empty: refused on every plugin.
_EMPTY_PART = [".shop.t", "..t", ".a.b.c"]


@pytest.mark.parametrize("plugin", [None, "v56", "v1", "x", "", "V57"])
@pytest.mark.parametrize("name", _DOTTED)
def test_a_dot_inside_a_name_is_refused_on_an_older_or_unknown_sink_plugin(name, plugin) -> None:
    # FAILS WITHOUT THE FIX: the refusal took no sink_plugin and named neither the deployed
    # plugin nor Delete + Deploy. Such a sink splits a qualified name on every '.', so every
    # change to the table failed, and a tombstone of public."public.orders" deleted from
    # public.orders. The text names the deployed plugin when it is a version, and Delete +
    # Deploy with each engine's cost.
    known = plugin if plugin in ("v56", "v1") else None
    assert topic_name_refused_tables(
        _lists(["shop.orders", name, "shop.items"]), sink_plugin=plugin) == [name]
    assert topic_name_refusal(_lists([name]), sink_plugin=plugin) == (
        f"CDC cannot stream {name}{_old_plugin_fact(known)} {_DOTTED_SCHEMA} {_REMEDY}")
    # No plugin given is an unknown one.
    if plugin is None:
        assert topic_name_refusal(_lists([name])) == topic_name_refusal(
            _lists([name]), sink_plugin=None)


@pytest.mark.parametrize("plugin", ["v57", "v58", " v57 ", "v100"])
@pytest.mark.parametrize("name", _DOTTED)
def test_a_dot_inside_a_table_name_streams_on_sink_plugin_v57_or_later(name, plugin) -> None:
    # FAILS WITHOUT THE FIX: every such name was refused on every stack (there was no
    # sink_plugin). The v57 sink reads a qualified name at its first '.' and skips every
    # tombstone, so it streams there.
    assert topic_name_refused_tables(
        _lists(["shop.orders", name, "shop.items"]), sink_plugin=plugin) == []
    assert topic_name_refusal(_lists(["shop.orders", name]), sink_plugin=plugin) is None
    # Evaluation found no database or schema whose name holds a '.': the same.
    assert topic_name_refusal(
        _lists([name]), sink_plugin=plugin, dotted_schema_tables=frozenset()) is None


@pytest.mark.parametrize("plugin", [None, "v56", "v57", "v58"])
@pytest.mark.parametrize("name", _EMPTY_PART)
def test_a_name_with_an_empty_part_before_its_first_dot_is_refused_on_every_plugin(
    name, plugin
) -> None:
    # FAILS WITHOUT THE FIX: the refusal said the sink reads every '.', which v57 no longer
    # does; such a name holds no database or schema the sink can read (the catalog read never
    # produces one).
    assert topic_name_refused_tables(
        _lists(["shop.orders", name]), sink_plugin=plugin) == [name]
    assert topic_name_refusal(_lists([name]), sink_plugin=plugin) == (
        f"CDC cannot stream {name}{_EMPTY_PART_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # Evaluation found no database or schema whose name holds a '.': no such sentence.
    assert topic_name_refusal(
        _lists([name]), sink_plugin=plugin, dotted_schema_tables=()) == (
        f"CDC cannot stream {name}{_EMPTY_PART_FACT} {_REMEDY}")


def test_only_a_dot_after_the_topic_prefix_is_inside_a_name() -> None:
    # A '.' inside TopicPrefix is not one inside a name; one more after it is.
    assert topic_name_refused_tables([("TopicPrefix", "a.b"), ("SinkTopics", "a.b.shop.t")]) == []
    assert topic_name_refused_tables(
        [("TopicPrefix", "a.b"), ("SinkTopics", "a.b.shop.t.x")]) == ["shop.t.x"]
    assert topic_name_refused_tables(
        [("TopicPrefix", "a.b"), ("SinkTopics", "a.b.shop.t.x")], sink_plugin="v57") == []
    # A topic under another prefix than the one sent is judged on its characters only.
    assert topic_name_refused_tables(
        [("TopicPrefix", "other"), ("SinkTopics", "dsqlcdc.shop.a")]) == []
    assert topic_name_refused_tables(
        [("TopicPrefix", "other"), ("SinkTopics", "dsqlcdc..shop.a")]) == []
    # ...and Kafka compares whole topics, so it is not alike a name that only resembles it.
    assert topic_name_refused_tables(
        [("TopicPrefix", "other"), ("SinkTopics", "dsqlcdc.shop.a,other.dsqlcdc_shop.a")]) == []
    # All of them in one sentence each: sorted, five named and the rest counted; the empty
    # part's first, as it holds on every plugin.
    assert topic_name_refusal(_lists([*_DOTTED, "shop.x.y", *_EMPTY_PART])) == (
        f"CDC cannot stream ..t, .a.b.c, .shop.t{_EMPTY_PART_FACT} "
        "CDC cannot stream my.db.t, public.public.orders, shop..t, shop.a.b, shop.t. and 1 more"
        f"{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY}")


def test_names_whose_topics_differ_only_in_a_dot_against_an_underscore_are_refused() -> None:
    # FAILS WITHOUT THE FIX: Kafka refuses the second of two such topics, and both seeders
    # pre-create every SinkTopics entry, so Start CDC failed after the infrastructure was
    # deployed. Every member is returned, in SinkTopics order; the text sorts each group's
    # members, then the groups.
    pair = ["shop.orders", "app_v2.users", "shop.items", "app.v2_users"]
    assert topic_name_refused_tables(_lists(pair)) == ["app_v2.users", "app.v2_users"]
    assert topic_name_refusal(_lists(pair)) == (
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        + _REMEDY)
    three = ["x_a_b.c", "x.a_b_c", "x_a.b_c"]
    assert topic_name_refused_tables(_lists(three)) == three
    assert topic_name_refusal(_lists(three)) == (
        f"CDC cannot stream x.a_b_c together with x_a.b_c and x_a_b.c{_COLLISION_FACT} them. "
        + _REMEDY)
    four = ["a_b_c_d.e", "a.b_c_d_e", "a_b.c_d_e", "a_b_c.d_e"]
    assert topic_name_refusal(_lists(four)) == (
        "CDC cannot stream a.b_c_d_e together with a_b.c_d_e, a_b_c.d_e and a_b_c_d.e"
        f"{_COLLISION_FACT} them. {_REMEDY}")
    two = ["k_l.m", "app.v2_users", "k.l_m", "app_v2.users"]
    assert topic_name_refused_tables(_lists(two)) == two
    assert topic_name_refusal(_lists(two)) == (
        "CDC cannot stream app.v2_users together with app_v2.users, or k.l_m together with k_l.m"
        f"{_COLLISION_FACT} each. {_REMEDY}")
    seven = [name for c in "gfedcba" for name in (f"s_{c}.t", f"s.{c}_t")]
    assert topic_name_refused_tables(_lists(seven)) == seven
    assert topic_name_refusal(_lists(seven)) == (
        "CDC cannot stream "
        + ", or ".join(f"s.{c}_t together with s_{c}.t" for c in "abcde")
        + f", and 2 more like them{_COLLISION_FACT} each. {_REMEDY}")


def test_names_kafka_tells_apart_are_not_refused() -> None:
    for names in (
        ["App.v2_users", "app_v2.users"],  # case counts
        ["a.b-c", "a_b.c"],                # '-' is neither '.' nor '_'
        ["shop.a_b", "shop.a_b"],          # the same name twice is one topic
    ):
        assert topic_name_refused_tables(_lists(names)) == [], names
        assert topic_name_refusal(_lists(names)) is None, names


def test_a_refused_name_is_never_a_collision_partner() -> None:
    # On an older sink plugin a dotted name is refused for its '.', and its look-alike streams.
    assert topic_name_refused_tables(_lists(["shop.a.b", "shop.a_b"])) == ["shop.a.b"]
    assert topic_name_refusal(_lists(["shop.a.b", "shop.a_b"])) == (
        f"CDC cannot stream shop.a.b{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # Two dotted names alike once '.' is '_' are named as dotted only; two illegal ones, as
    # illegal only.
    assert topic_name_refusal(_lists(["shop.a.b_c", "shop.a_b.c"])) == (
        f"CDC cannot stream shop.a.b_c, shop.a_b.c{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # A table of a database or schema whose name holds a '.' is refused for that on every
    # plugin, and its look-alike streams.
    for plugin in (None, "v56", "v57"):
        assert topic_name_refused_tables(
            _lists(["my.db.t", "my_db.t"]), sink_plugin=plugin,
            dotted_schema_tables={"my.db.t"}) == ["my.db.t"], plugin
    # So is a name with an empty part before its first '.'.
    assert topic_name_refused_tables(_lists([".a.b", "_a_b"]), sink_plugin="v57") == [".a.b"]
    assert topic_name_refusal(_lists(["a.b c_d", "a_b c.d"])) == (
        "CDC cannot stream a.b<U+0020>c_d, a_b<U+0020>c.d: each table's Kafka topic is named "
        "after its database or schema and its table, and Kafka allows only ASCII letters, "
        "digits, '.', '_' and '-' in a topic name. " + _REMEDY)


def test_the_stacks_own_topics_cannot_collide_with_a_table_topic() -> None:
    # Kafka compares a table topic with every topic on the cluster, the stack's own ones too,
    # which the check never sees. None can be equal to a table topic once '.' is '_': that one
    # unifies to <TopicPrefix>_<db>_<table>, with at least two '_' after the prefix.
    template = (
        pathlib.Path(__file__).resolve().parents[1] / "deploy" / "cdc-stack" / "cdc-stack.yaml"
    ).read_text(encoding="utf-8")
    # The dead-letter topic and the source offsets topic hold neither '.' nor '_' (nor can the
    # stack name the offsets topic starts with).
    (dlq,) = re.findall(r"\n  DlqTopicName:\n    Type: String\n    Default: (\S+)\n", template)
    assert dlq == CDC_DEFAULT_DLQ_TOPIC
    assert set(re.findall(r"offset\.storage\.topic=(\S+)", template)) == {
        "${AWS::StackName}-debezium-source-offsets"}
    for fixed in (CDC_DEFAULT_DLQ_TOPIC, "-debezium-source-offsets"):
        assert not {".", "_"} & set(fixed), fixed
    assert not cdc_stack_name_is_valid("dsql-cdc-a.b")
    assert not cdc_stack_name_is_valid("dsql-cdc-a_b")
    # The schema history topic unifies to a name with one '_' after the prefix.
    (history,) = set(re.findall(
        r"schema\.history\.internal\.kafka\.topic:\s*\n\s*Fn::Sub: (\S+)\n", template))
    assert history == "${TopicPrefix}.schema-history"
    assert history.removeprefix("${TopicPrefix}").replace(".", "_").count("_") == 1


def test_every_kind_in_one_capture_is_one_message(monkeypatch) -> None:
    names = ["shop.orders", "app_v2.users", "shop.a b", "shop.a.b", "app.v2_users"]
    dotted = f"CDC cannot stream shop.a.b{_OLD_PLUGIN_FACT}"
    collision = f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them."
    facts = f"{_FACT} {dotted} {collision} {_DOTTED_SCHEMA}"
    refusal = topic_name_refusal(_lists(names))
    assert refusal == f"{facts} {_REMEDY}"
    assert refusal.count(_REMEDY) == 1
    assert topic_name_refused_tables(_lists(names)) == [
        "app_v2.users", "shop.a b", "shop.a.b", "app.v2_users"]
    # Before a Full load + CDC load the facts are everything before the remedy: the Full Load
    # card's warning and the early deploy follow them with the routes.
    assert cdc_pre_load_topic_name_warning(names, planned=False, table_lock=None) == (
        CDC_TOPIC_NAME_HEADER,
        f"{facts} Deploy CDC infrastructure and Start CDC refuse a capture holding such a "
        "table. " + _ROUTES_DOTTED + _ROUTES_OR + _OPEN,
    )
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    # The early deploy judges the names for the plugin this version bundles (pinned here).
    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v56")
    bundled = facts.replace(_OLD_PLUGIN_FACT, _old_plugin_fact("v56"))
    assert _deploy_block_notice(
        _deploy_ready(names, FLC), _Jobs({}), _inventory(names), None
    ) == (CDC_TOPIC_NAME_HEADER, f"{bundled} {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}")


def test_every_kind_on_one_stack_is_one_message_in_its_order() -> None:
    # Illegal, then the names no plugin streams (a database or schema whose name holds a '.',
    # an empty part), then the older plugin's, then the collisions; the dotted-schema sentence
    # once (a refused name is one of Evaluation's DOTTED_SCHEMA_NAME tables), the remedy last.
    names = ["shop.orders", "app_v2.users", "shop.a b", "my.db.t", ".x.t", "shop.a.b",
             "app.v2_users"]
    kw = dict(sink_plugin="v56", dotted_schema_tables={"my.db.t"})
    refusal = topic_name_refusal(_lists(names), **kw)
    assert refusal == (
        f"{_FACT} CDC cannot stream my.db.t{_DOTTED_SCHEMA_FACT} "
        f"CDC cannot stream .x.t{_EMPTY_PART_FACT} "
        f"CDC cannot stream shop.a.b{_old_plugin_fact('v56')} "
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        f"{_DOTTED_SCHEMA} {_REMEDY}")
    assert refusal.endswith(CDC_TOPIC_NAME_REMEDY) and refusal.count(_REMEDY) == 1
    assert topic_name_refused_tables(_lists(names), **kw) == names[1:]
    # On v57 only the '.' inside the table name streams.
    kw["sink_plugin"] = "v57"
    assert topic_name_refused_tables(_lists(names), **kw) == [
        "app_v2.users", "shop.a b", "my.db.t", ".x.t", "app.v2_users"]
    assert "shop.a.b" not in topic_name_refusal(_lists(names), **kw)


def test_the_prefix_is_read_from_the_parameters_and_sinktopics_order_is_kept() -> None:
    other = [("TopicPrefix", "other"),
             ("SinkTopics", "other.shop.z z,other.shop.ok,other.shop.a,b")]
    assert topic_name_refused_tables(other) == ["shop.z z", "shop.a,b"]
    # Without TopicPrefix, the default one.
    assert topic_name_refused_tables([("SinkTopics", "dsqlcdc.shop.a,b,dsqlcdc.shop.c")]) == [
        "shop.a,b"]
    # Topics under another prefix than the one sent are one name, judged whole.
    assert topic_name_refused_tables(
        [("TopicPrefix", "other"), ("SinkTopics", "dsqlcdc.shop.a,dsqlcdc.shop.b")]
    ) == ["dsqlcdc.shop.a,dsqlcdc.shop.b"]
    # Each table once.
    assert topic_name_refused_tables(_lists(["shop.a b", "shop.ok", "shop.a b"])) == ["shop.a b"]
    # Any iterable of parameters: an iterator is not left empty for the check's second read.
    assert topic_name_refused_tables(iter(_lists(["shop.ok", "shop.a b"]))) == ["shop.a b"]
    assert topic_name_refusal(_lists(["shop.a b", "shop.a b"])).startswith(
        "CDC cannot stream shop.a<U+0020>b: ")
    # No topics, nothing to refuse.
    for filled in ([], [("SinkTopics", "")], [("SinkTopics", None)], [("TopicPrefix", "x")]):
        assert topic_name_refused_tables(filled) == [], filled
        assert topic_name_refusal(filled) is None, filled


def test_the_refusal_names_the_tables_the_rule_and_the_remedy() -> None:
    assert CDC_TOPIC_NAME_REMEDY == _REMEDY
    assert topic_name_refusal(_lists(_NAMES)) == _REFUSAL
    # Sorted, five named and the rest counted.
    seven = [f"shop.t{c} x" for c in "gfedcba"]
    assert topic_name_refusal(_lists(seven)) == (
        "CDC cannot stream shop.ta<U+0020>x, shop.tb<U+0020>x, shop.tc<U+0020>x, "
        "shop.td<U+0020>x, shop.te<U+0020>x and 2 more: each table's Kafka topic is named after "
        "its database or schema and its table, and Kafka allows only ASCII letters, digits, '.', "
        "'_' and '-' in a topic name. " + _REMEDY
    )


def test_a_name_that_may_be_of_a_dotted_schema_is_not_sent_to_full_load_only() -> None:
    # FAILS WITHOUT THE FIX: the remedy offered a Full load only copy even for a table of a
    # database or schema whose name holds a '.', which no step supports (DOTTED_SCHEMA_NAME).
    # The names alone cannot tell my.db.orders (database my.db) from shop.a.b (a '.' inside the
    # table name), so every refused name with two or more '.' gets the sentence, once, after
    # the facts and before the remedy; a name with one '.' never does.
    assert CDC_TOPIC_NAME_DOTTED_SCHEMA == _DOTTED_SCHEMA
    assert topic_name_refusal(_lists(["my.db.orders"])) == (
        f"CDC cannot stream my.db.orders{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # A name Kafka refuses, too (database "my.d b").
    assert topic_name_refusal(_lists(["my.d b.orders"])) == (
        f"CDC cannot stream my.d<U+0020>b.orders{_ILLEGAL_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # One '.': a name Kafka refuses and a colliding pair keep their text.
    assert topic_name_refusal(_lists(["shop.a b"])) == _REFUSAL
    assert topic_name_refusal(_lists(["app.v2_users", "app_v2.users"])) == (
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        + _REMEDY)
    # Several such names of both kinds: one sentence, after every fact.
    refusal = topic_name_refusal(_lists(
        ["my.db.orders", "my.d b.orders", "shop.a.b", "x.y z.w", "app.v2_users", "app_v2.users"]))
    assert refusal == (
        f"CDC cannot stream my.d<U+0020>b.orders, x.y<U+0020>z.w{_ILLEGAL_FACT} "
        f"CDC cannot stream my.db.orders, shop.a.b{_OLD_PLUGIN_FACT} "
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        f"{_DOTTED_SCHEMA} {_REMEDY}")
    assert refusal.count(_DOTTED_SCHEMA) == 1
    # Where a '.' inside a table name streams (v57), a colliding pair holding one gets it too:
    # without Evaluation's tables, shop.orders.v2 may be table v2 of database shop.orders.
    assert topic_name_refusal(_lists(["shop.orders.v2", "shop.orders_v2"]), sink_plugin="v57") == (
        f"CDC cannot stream shop.orders.v2 together with shop.orders_v2{_COLLISION_FACT} them. "
        f"{_DOTTED_SCHEMA} {_REMEDY}")
    assert topic_name_refused_tables(
        _lists(["shop.orders_v2", "shop.items", "shop.orders.v2"]), sink_plugin="v57") == [
        "shop.orders_v2", "shop.orders.v2"]


@pytest.mark.parametrize("plugin", [None, "v56", "v57"])
def test_evaluations_dotted_schema_tables_are_refused_with_their_own_fact(plugin) -> None:
    # FAILS WITHOUT THE FIX: there was no dotted_schema_tables (TypeError). On MySQL, table t of
    # database my.db and table db.t of database my have the same table id, which the escaped
    # include entry captures both of, so a table Evaluation reported DOTTED_SCHEMA_NAME on is
    # refused on every plugin, followed by the sentence that no step supports it.
    assert topic_name_refusal(
        _lists(["shop.orders", "my.db.t"]), sink_plugin=plugin, dotted_schema_tables={"my.db.t"}
    ) == f"CDC cannot stream my.db.t{_DOTTED_SCHEMA_FACT} {_DOTTED_SCHEMA} {_REMEDY}"
    assert topic_name_refused_tables(
        _lists(["shop.orders", "my.db.t"]), sink_plugin=plugin, dotted_schema_tables=["my.db.t"]
    ) == ["my.db.t"]
    # Sorted, five named and the rest counted, in one sentence.
    seven = [f"my.db.t{c}" for c in "gfedcba"]
    assert topic_name_refusal(
        _lists(seven), sink_plugin=plugin, dotted_schema_tables=seven
    ) == ("CDC cannot stream my.db.ta, my.db.tb, my.db.tc, my.db.td, my.db.te and 2 more"
          f"{_DOTTED_SCHEMA_FACT} {_DOTTED_SCHEMA} {_REMEDY}")
    # A name Kafka refuses that is one of them: its own fact, and the sentence.
    assert topic_name_refusal(
        _lists(["my.d b.t"]), sink_plugin=plugin, dotted_schema_tables={"my.d b.t"}
    ) == f"CDC cannot stream my.d<U+0020>b.t{_ILLEGAL_FACT} {_DOTTED_SCHEMA} {_REMEDY}"


def test_known_dotted_schema_tables_say_the_sentence_only_for_one_of_them() -> None:
    # FAILS WITHOUT THE FIX: every refused name with two or more '.' got the dotted-schema
    # sentence, also when Evaluation had found no database or schema whose name holds a '.'.
    for known in (frozenset(), {"other.x.y"}, ()):
        assert topic_name_refusal(
            _lists(["shop.a.b"]), sink_plugin="v56", dotted_schema_tables=known
        ) == f"CDC cannot stream shop.a.b{_old_plugin_fact('v56')} {_REMEDY}", known
        assert topic_name_refusal(
            _lists(["my.d b.orders"]), dotted_schema_tables=known
        ) == f"CDC cannot stream my.d<U+0020>b.orders{_ILLEGAL_FACT} {_REMEDY}", known
        assert topic_name_refusal(
            _lists(["shop.orders.v2", "shop.orders_v2"]), sink_plugin="v57",
            dotted_schema_tables=known,
        ) == (f"CDC cannot stream shop.orders.v2 together with shop.orders_v2{_COLLISION_FACT} "
              f"them. {_REMEDY}"), known
    # Unknown (None, the default): today's rule, any refused name with two or more '.'.
    assert topic_name_refusal(_lists(["shop.a.b"]), sink_plugin="v56") == (
        f"CDC cannot stream shop.a.b{_old_plugin_fact('v56')} {_DOTTED_SCHEMA} {_REMEDY}")
    # One of them among other refused names: the sentence once, after every fact.
    refusal = topic_name_refusal(
        _lists(["shop.a.b", "my.db.t", "app.v2_users", "app_v2.users"]), sink_plugin="v56",
        dotted_schema_tables={"my.db.t"})
    assert refusal == (
        f"CDC cannot stream my.db.t{_DOTTED_SCHEMA_FACT} "
        f"CDC cannot stream shop.a.b{_old_plugin_fact('v56')} "
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        f"{_DOTTED_SCHEMA} {_REMEDY}")
    assert refusal.count(_DOTTED_SCHEMA) == 1


# ---------------------------------------------------------------------------
# 1b. The include entry, the sink plugin gate and Evaluation's view of the same rule
# ---------------------------------------------------------------------------


def _sent(names, message_key_columns=None):
    return dict(cdc_table_list_params(
        DebeziumSourceConfig(name="s", table_include_list=names,
                             message_key_columns=message_key_columns or {}),
        SinkConnectorConfig(name="k", topics=names, dlq_topic="dsql-sink-dlq"),
    ))


def test_plain_names_send_byte_identical_lists() -> None:
    # Guard (passes before and after): for every name CDC streamed before, TableIncludeList,
    # SinkTopics and MessageKeyColumns are 0.1.588's bytes, so a deployed stack's Start sees
    # no configuration change. Spelled out from 0.1.588's output, not built with the helpers.
    sent = _sent(_ACCEPTED, {"Shop.Order_Items-2": ["tenant_id", "id"],
                             "shop.orders": ["region", "id"]})
    assert sent["TableIncludeList"] == "shop.orders,Shop.Order_Items-2,db2024.t0123456789"
    assert sent["SinkTopics"] == (
        "dsqlcdc.shop.orders,dsqlcdc.Shop.Order_Items-2,dsqlcdc.db2024.t0123456789")
    assert sent["MessageKeyColumns"] == (
        "Shop\\.Order_Items-2:tenant_id,id;shop\\.orders:region,id")
    # Every name Kafka allows with one '.' (or none) is its own entry, as written: each
    # character Kafka allows but '.', at each end of either part and inside it.
    for ch in sorted(_KAFKA - {"."}):
        for name in (f"shop.t{ch}", f"{ch}shop.t", f"shop.{ch}t", f"s{ch}p.{ch}", f"t{ch}"):
            assert table_include_entry(name) == name, name
            assert unescape_include_entry(name) == name, name


@pytest.mark.parametrize("name, entry", [
    ("shop.orders.v2", "shop\\.orders\\.v2"),
    ("app.app.orders", "app\\.app\\.orders"),
    ("shop.t.", "shop\\.t\\."),
    ("shop..t", "shop\\.\\.t"),
    ("Shop.Order.Items-2_x", "Shop\\.Order\\.Items-2_x"),
])
def test_a_dot_inside_a_table_name_is_escaped_in_table_include_list(name, entry) -> None:
    # FAILS WITHOUT THE FIX: TableIncludeList carried shop.orders.v2 as written, a regex whose
    # '.' matches any character, so shop.orders_v2, shopXorders.v2 and shop_orders.v2 were
    # captured too. SinkTopics keeps the topic as Debezium names it.
    sent = _sent(["shop.orders", name])
    assert sent["TableIncludeList"] == f"shop.orders,{entry}"
    assert sent["SinkTopics"] == f"dsqlcdc.shop.orders,dsqlcdc.{name}"
    assert table_include_entry(name) == entry
    # The readback of a deployed TableIncludeList gives the name back.
    assert unescape_include_entry(entry) == name
    assert [unescape_include_entry(e) for e in sent["TableIncludeList"].split(",")] == [
        "shop.orders", name]


def test_every_name_cdc_streams_reads_back_from_its_entry() -> None:
    # A name CDC streams holds only ASCII letters, digits, '.', '_' and '-', so its entry holds
    # no other escape and reads back exactly.
    alphabet = sorted(_KAFKA)
    for i, a in enumerate(alphabet):
        b = alphabet[(i * 7 + 3) % len(alphabet)]
        for name in (f"s{a}.t{b}", f"s.{a}.{b}", f"{a}{b}.x.{a}", f"d.{b}{a}.{a}"):
            if topic_name_refused_tables(_lists([name]), sink_plugin="v57"):
                continue
            assert unescape_include_entry(table_include_entry(name)) == name, name


def test_the_sink_plugin_gate() -> None:
    # FAILS WITHOUT THE FIX: no such gate.
    assert DOTTED_TABLE_MIN_PLUGIN_VERSION == "v57"
    for plugin in ("v56", "v1", "v0", None, "", "x", "V57", "v57a", "57", "v 57"):
        assert plugin_reads_dotted_tables(plugin) is False, plugin
    for plugin in ("v57", "v58", "v100", " v57 "):
        assert plugin_reads_dotted_tables(plugin) is True, plugin
    # The sink-key gate it was factored out of is unchanged.
    for plugin in ("v50", None, "", "x"):
        assert plugin_supports_sink_keys(plugin) is False, plugin
    for plugin in ("v51", "v57", " v51 "):
        assert plugin_supports_sink_keys(plugin) is True, plugin


def test_the_new_parameters_are_keyword_only_and_unknown_by_default() -> None:
    # None means unknown, judged as today's refusal: a caller that passes neither fails safe.
    for fn in (topic_name_refusal, topic_name_refused_tables):
        params = inspect.signature(fn).parameters
        for name in ("sink_plugin", "dotted_schema_tables"):
            assert params[name].kind is inspect.Parameter.KEYWORD_ONLY, (fn, name)
            assert params[name].default is None, (fn, name)


def test_evaluation_judges_names_as_a_v57_stack_does() -> None:
    # topic_name_problems: the names Kafka refuses, and the '.'/'_' groups, in input order, a
    # '.' inside a table name no problem on its own and joining the groups.
    names = ["shop.orders", "shop.orders.v2", "shop.a b", "shop.orders_v2", "app_v2.users",
             "app.v2_users", "shop.a b", "shop.x.y"]
    assert topic_name_problems(names) == (
        ["shop.a b"], [["shop.orders.v2", "shop.orders_v2"], ["app_v2.users", "app.v2_users"]])
    assert topic_name_problems(["shop.orders", "shop.a.b"]) == ([], [])
    assert topic_name_problems([]) == ([], [])
    # The prefix is the topic's: a '.' inside it is not one inside a name.
    assert topic_name_problems(["a.b_c", "a_b.c"], prefix="p.q") == ([], [["a.b_c", "a_b.c"]])
    # The same tables topic_name_refusal names on a v57 stack, illegal ones and groups alike.
    refused = topic_name_refused_tables(_lists(names), sink_plugin="v57")
    illegal, groups = topic_name_problems(names)
    assert set(refused) == {*illegal, *(n for g in groups for n in g)}


# ---------------------------------------------------------------------------
# 2. The UI judges exactly what the builders send
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_the_ui_judges_exactly_what_the_builders_send(engine) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders", f"{db}.order$items", f"{db}.a,b", f"{db}.ok_2", f"{db}.a.b",
             f"{db}.v2_users", f"{db}_v2.users"]
    refusal = cdc_topic_name_refusal(names)
    assert refusal is not None
    assert refusal == topic_name_refusal(_start(engine, names).filled) == (
        topic_name_refusal(_infra(engine, names, _counts(names)).filled))
    assert cdc_topic_name_refused(names) == topic_name_refused_tables(
        _start(engine, names).filled) == topic_name_refused_tables(
        _infra(engine, names, _counts(names)).filled) == [
        f"{db}.order$items", f"{db}.a,b", f"{db}.a.b", f"{db}.v2_users", f"{db}_v2.users"]
    legal = [f"{db}.orders", f"{db}.ok_2"]
    assert cdc_topic_name_refusal(legal) is None
    assert topic_name_refusal(_start(engine, legal).filled) is None
    assert topic_name_refusal(_infra(engine, legal).filled) is None


# ---------------------------------------------------------------------------
# 3. What the UI says
# ---------------------------------------------------------------------------


def _split_job(names, split):
    """A finished MySQL Full load + CDC record that read ``split`` in parts."""
    return MigrationJob(
        job_id="job-1",
        status="DONE",
        chunks=[
            ChunkState(chunk_id=n, status="DONE", attempts=1,
                       reader_shards=4 if n in split else None)
            for n in names
        ],
        watermark=Watermark(
            binlog_file="mysql-bin.000042", binlog_position=120, snapshot_timestamp=_TS,
            table_row_counts=_counts(names),
        ),
    )


def _loaded_state(names, **kw):
    state = _state(names, phase="infra", **kw)
    state.job_id = "job-1"
    return state


def test_the_hold_remedy_follows_the_table_picker() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _table_lock, _topic_name_hold_remedy

    assert topic_name_hold_remedy(_REFUSAL, None) == f"{_REFUSAL} {_OPEN}"
    # A Full Load ran and CDC started for this migration: the picker's Full Load lock.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    started = _loaded_state(_NAMES)
    started.set_cdc_ops_window_start(_TS)
    assert _table_lock(started, jobs, None) == _FULL_LOAD_LOCK
    assert _topic_name_hold_remedy(
        _REFUSAL, started, jobs, _table_lock(started, jobs, None), deploy=False) == (
        f"{_REFUSAL} The table picker is locked. {_FULL_LOAD_LOCK}")
    # CDC only with its infrastructure deployed: the infrastructure lock.
    cdc_only = _state(_NAMES, MigrationType.CDC_ONLY, phase="infra")
    assert _table_lock(cdc_only, _Jobs({}), None) == _INFRA_LOCK
    assert topic_name_hold_remedy(_REFUSAL, _INFRA_LOCK) == (
        f"{_REFUSAL} The table picker is locked. {_INFRA_LOCK}")


def test_a_refused_table_read_in_parts_is_named_as_one_that_cannot_be_deselected() -> None:
    # G137: a refused table the Full Load read in parts is named with its reload route; one
    # read in parts that CDC can stream is not this refusal's business.
    from dsql_migrator.ui.data_migration._cdc_ui import _topic_name_hold_remedy

    names = ["app.users", "app.ev ents", "app.big"]
    job = _split_job(names, split={"app.ev ents", "app.big"})
    state = _loaded_state(names)
    refusal = cdc_topic_name_refusal(names)
    split_text = (
        "The Full Load read app.ev ents in parts, so it cannot be deselected: to deselect it, "
        "set Reader shards per large table to 1 in Settings and reload it (Reload specific "
        "tables, which drops and recreates it) first. Once CDC starts, the table set is fixed "
        "(Start over to change it). Nor can the migration switch to Full load only until that "
        "reload, or until a match in Validation in CHECKSUM mode proves CDC's replay of it."
    )
    assert _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": job}), None, deploy=False) == (
        f"{refusal} {_OPEN} {split_text}")
    assert topic_name_hold_remedy(refusal, None, ["app.ev ents"], job) == (
        f"{refusal} {_OPEN} {split_text}")
    # No refused table read in parts: the picker's remedy alone.
    legal_split = _split_job(names, split={"app.big"})
    assert _topic_name_hold_remedy(
        refusal, state, _Jobs({"job-1": legal_split}), None, deploy=False) == f"{refusal} {_OPEN}"
    # CDC only has no Full Load step: the reload goes through Full load + CDC.
    through = (
        "to deselect it, switch the migration type to Full load + CDC, set Reader shards per "
        "large table to 1 in Settings and reload it on its Full Load step (Reload specific "
        "tables, which drops and recreates it) first."
    )
    cdc_only = _state(names, MigrationType.CDC_ONLY, phase="infra")
    cdc_only.job_id = "job-1"
    assert through in _topic_name_hold_remedy(
        refusal, cdc_only, _Jobs({"job-1": job}), None, deploy=False)
    assert through in topic_name_hold_remedy(refusal, None, ["app.ev ents"], job, cdc_only=True)
    assert through not in topic_name_hold_remedy(refusal, None, ["app.ev ents"], job)


def test_a_colliding_name_read_in_parts_is_named_only_beside_its_partner() -> None:
    # The tables read in parts are judged on their own, on purpose: of two names that differ
    # only in a '.' against a '_', one read in parts is named with its reload route only when
    # the other was read in parts too -- otherwise deselecting the other one is enough.
    from dsql_migrator.ui.data_migration._cdc_ui import _topic_name_hold_remedy

    names = ["app.users", "app.v2_users", "app_v2.users"]
    state = _loaded_state(names)
    refusal = cdc_topic_name_refusal(names)
    one = _split_job(names, split={"app.v2_users"})
    assert _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": one}), None, deploy=False) == (
        f"{refusal} {_OPEN}")
    both = _split_job(names, split={"app.v2_users", "app_v2.users"})
    text = _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": both}), None, deploy=False)
    assert text == topic_name_hold_remedy(refusal, None, ["app.v2_users", "app_v2.users"], both)
    # Two or more are "they", never "them", as the subject ("so them cannot be deselected").
    assert text == (
        f"{refusal} {_OPEN} The Full Load read app.v2_users, app_v2.users in parts, so they "
        "cannot be deselected: to deselect them, set Reader shards per large table to 1 in "
        "Settings and reload them (Reload specific tables, which drops and recreates them) "
        "first. Once CDC starts, the table set is fixed (Start over to change it). Nor can the "
        "migration switch to Full load only until that reload, or until a match in Validation "
        "in CHECKSUM mode proves CDC's replay of them.")


def test_the_pre_load_routes_and_warning() -> None:
    assert cdc_pre_load_topic_name_routes(None) == _ROUTES + _ROUTES_OR + _OPEN
    assert cdc_pre_load_topic_name_routes("Locked — X.", split=True) == (
        _ROUTES + PRE_LOAD_SPLIT_CARVE_OUT + _ROUTES_OR
        + "The table picker is locked. Locked — X.")
    assert cdc_pre_load_topic_name_warning(_NAMES, planned=False, table_lock=None) == (
        CDC_TOPIC_NAME_HEADER,
        f"{_FACT} Deploy CDC infrastructure and Start CDC refuse a capture holding such a "
        "table. " + _ROUTES + _ROUTES_OR + _OPEN,
    )
    assert cdc_pre_load_topic_name_warning(_NAMES, planned=True, table_lock="Locked — X.") == (
        CDC_TOPIC_NAME_HEADER,
        f"{_FACT} Start CDC refuses a capture holding such a table. " + _ROUTES + _ROUTES_OR
        + "The table picker is locked. Locked — X.",
    )
    assert cdc_pre_load_topic_name_warning(["shop.orders"], planned=False, table_lock=None) is None


def test_the_pre_load_routes_except_a_dotted_database_or_schema() -> None:
    # FAILS WITHOUT THE FIX: the load-then-untick route ends in a Full load only copy, which
    # no step supports for a table of a database or schema whose name holds a '.'.
    assert cdc_pre_load_topic_name_routes(None, dotted_schema=True) == (
        _ROUTES_DOTTED + _ROUTES_OR + _OPEN)
    assert cdc_pre_load_topic_name_routes("Locked — X.", split=True, dotted_schema=True) == (
        _ROUTES_DOTTED + PRE_LOAD_SPLIT_CARVE_OUT + _ROUTES_OR
        + "The table picker is locked. Locked — X.")


# ---------------------------------------------------------------------------
# 4. Where the UI says it: the Full Load card (a warning), Deploy and Start (holds)
# ---------------------------------------------------------------------------


def test_start_full_load_is_warned_never_held() -> None:
    from dsql_migrator.ui.data_migration import full_load_run_guard_reason

    state, inv = _state(_NAMES), _inventory(_NAMES)
    assert data_migration_exclusion_block(state, _Jobs({}), None, inv) is None
    assert full_load_run_guard_reason(state, inv, has_run=True, exclusion_block=None) is None
    assert pre_load_topic_name_warning(state, _Jobs({}), None, inv) == (
        cdc_pre_load_topic_name_warning(_NAMES, planned=False, table_lock=None))


def test_the_pre_load_topic_warning_follows_the_state() -> None:
    inv = _inventory(_NAMES)
    jobs = _Jobs({})
    header, body = pre_load_topic_name_warning(_state(_NAMES), jobs, None, inv)
    assert header == CDC_TOPIC_NAME_HEADER
    assert "Deploy CDC infrastructure and Start CDC refuse a capture" in body
    # A stack deployed before the load: only Start CDC is still to refuse; the picker is
    # locked by that stack.
    _header, deployed = pre_load_topic_name_warning(_state(_NAMES, phase="infra"), jobs, None, inv)
    assert f"{_FACT} Start CDC refuses a capture holding such a table." in deployed
    assert deployed.endswith(f"leave it out: The table picker is locked. {_INFRA_LOCK}")
    # Nothing to say: Full load only, CDC only, after a load, once CDC started, no inventory,
    # and a selection that leaves the table out.
    for mtype in (MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY):
        assert pre_load_topic_name_warning(_state(_NAMES, mtype), jobs, None, inv) is None, mtype
    loaded = _state(_NAMES)
    loaded.job_id = "job-1"
    assert pre_load_topic_name_warning(loaded, jobs, None, inv) is None
    started = _state(_NAMES)
    started.set_cdc_ops_window_start(_TS)
    assert pre_load_topic_name_warning(started, jobs, None, inv) is None
    assert pre_load_topic_name_warning(_state(_NAMES), jobs, None, None) is None
    assert pre_load_topic_name_warning(_state(["shop.orders"]), jobs, None, inv) is None


def _render_card(monkeypatch, *, job, warnings):
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_g12_table_set_after_load import _VerifiedSession
    from tests.test_ui_data_migration import _ConfirmDialogUi

    notices: list = []
    real_notice = fl.render_notice

    def _notice(ui, *, tone, header, body="", **k):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    monkeypatch.setattr(fl, "render_notice", _notice)
    monkeypatch.setattr(fl, "_render_full_load_progress", lambda *a, **k: None)
    state = _state(_NAMES)
    if job is not None:
        state.job_id = job.job_id
    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, _Jobs({} if job is None else {job.job_id: job}), _VerifiedSession(),
        job=job, status=StepStatus.NOT_STARTED if job is None else StepStatus.DONE,
        selected_names=_NAMES, guard_reason=None, pre_load_warning=warnings[0],
        pre_load_topic_warning=warnings[1],
        start_full_load=noop, retry_failed_load=noop, reload_table=noop,
        accept_quarantine_and_continue=noop, stop_full_load=noop, refresh=noop,
        retry_tables=noop, schema_recreate_candidates=lambda: [],
    )
    return ui, [n for n in notices if n[1] in ("H", CDC_TOPIC_NAME_HEADER)]


def test_the_full_load_card_warns_before_the_load_beside_the_list_warning(monkeypatch) -> None:
    topic = pre_load_topic_name_warning(_state(_NAMES), _Jobs({}), None, _inventory(_NAMES))
    ui, alone = _render_card(monkeypatch, job=None, warnings=(None, topic))
    assert alone == [("warning", CDC_TOPIC_NAME_HEADER, topic[1])]
    assert "Start Full Load" in ui.buttons
    # Both apply: two warnings, the table list's first.
    _ui, both = _render_card(monkeypatch, job=None, warnings=(("H", "W"), topic))
    assert both == [("warning", "H", "W"), ("warning", CDC_TOPIC_NAME_HEADER, topic[1])]
    # Once a load ran the CDC step's holds speak instead.
    _ui, after = _render_card(
        monkeypatch, job=_loaded(_NAMES, _counts(_NAMES)), warnings=(("H", "W"), topic))
    assert after == []


def test_the_screen_hands_the_topic_warning_to_the_full_load_card() -> None:
    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    (step,) = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "_render_full_load_step"]
    (kw,) = [k for k in step.keywords if k.arg == "pre_load_topic_warning"]
    assert ast.unparse(kw.value) == "_topic_name_warning"
    (assign,) = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                 and any(getattr(t, "id", None) == "_topic_name_warning" for t in n.targets)]
    assert ast.unparse(assign.value.func) == "pre_load_topic_name_warning"
    # The gate every Full Load entry point reads never judges the names.
    assert "cdc_topic_name_refusal" not in inspect.getsource(dm.data_migration_exclusion_block)


def test_the_deploy_form_holds_with_the_route_that_is_open(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    inv = _inventory(_NAMES)
    # Before a Full load + CDC load (the early deploy on Prerequisites): the load-then-untick
    # route of the Full Load card's warning, in place of the refusal's own remedy.
    before = _deploy_ready(_NAMES, FLC)
    routes = f"{_FACT} {_ROUTES}{_ROUTES_OR}{_OPEN}"
    assert _deploy_block_notice(before, _Jobs({}), inv, None) == (CDC_TOPIC_NAME_HEADER, routes)
    ui, notices = _render_deploy(monkeypatch, before, _Jobs({}), inv)
    assert ("error", CDC_TOPIC_NAME_HEADER, routes) in notices
    assert ui.deploy_button().enabled is False
    # The Prerequisites card's early Deploy is that same form (render_notice still records).
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_ui_data_migration import _GateUi

    seen = len(notices)
    early = _GateUi()
    _cdc_ui._render_cdc_infra_prep_section(
        early, before, _Jobs({}), lambda: None, inventory=inv, session=None)
    assert ("error", CDC_TOPIC_NAME_HEADER, routes) in notices[seen:]
    assert early.deploy_button().enabled is False
    # CDC only, and after the load: the refusal with the table picker's remedy.
    after = _deploy_ready(_NAMES, FLC, job_id="job-1")
    loaded = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    for state, jobs in ((_deploy_ready(_NAMES), _Jobs({})), (after, loaded)):
        assert _deploy_block_notice(state, jobs, inv, None) == (
            CDC_TOPIC_NAME_HEADER, f"{_REFUSAL} {_OPEN}"), state.migration_type
        ui, notices = _render_deploy(monkeypatch, state, jobs, inv)
        assert ("error", CDC_TOPIC_NAME_HEADER, f"{_REFUSAL} {_OPEN}") in notices
        assert ui.deploy_button().enabled is False
    # Attach sends no parameter: not held.
    assert _deploy_block_notice(
        before, _Jobs({}), inv, None, include_missing=False, include_capture=False) is None
    # Names Kafka accepts: not held.
    legal = ["shop.orders", "shop.items"]
    assert _deploy_block_notice(
        _deploy_ready(legal, FLC), _Jobs({}), _inventory(legal), None) is None


def test_the_card_and_the_early_deploy_carve_out_a_table_read_in_parts(monkeypatch) -> None:
    # G137: while the load may read a large MySQL table in parts, the load-then-untick route
    # says such a table cannot be unticked until it is reloaded with one reader.
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    inv = _inventory(_NAMES)

    def _texts(source_type):
        session = SimpleNamespace(source_config=SimpleNamespace(source_type=source_type),
                                  restored_source_type=None)
        _h, card = pre_load_topic_name_warning(_state(_NAMES), _Jobs({}), session, inv)
        _h, deploy = _deploy_block_notice(_deploy_ready(_NAMES, FLC), _Jobs({}), inv, session)
        return card, deploy

    for knob in ("READER_SHARDS", "TABLE_PARALLELISM"):
        monkeypatch.delenv(f"DSQL_MIGRATOR_FULL_LOAD_{knob}", raising=False)
    assert not [t for t in _texts(SourceType.MYSQL) if PRE_LOAD_SPLIT_CARVE_OUT in t]
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    assert all(PRE_LOAD_SPLIT_CARVE_OUT in t for t in _texts(SourceType.MYSQL))
    assert not [t for t in _texts(SourceType.POSTGRES) if PRE_LOAD_SPLIT_CARVE_OUT in t]


def test_the_deploy_click_refuses_before_any_read(monkeypatch) -> None:
    calls = _fake_nicegui(monkeypatch, None)
    inv = _inventory(_NAMES)
    assert _click_deploy(_deploy_ready(_NAMES, FLC), inv) == [
        (f"{_FACT} {_ROUTES}{_ROUTES_OR}{_OPEN}", "negative")]
    assert _click_deploy(_deploy_ready(_NAMES), inv) == [(f"{_REFUSAL} {_OPEN}", "negative")]
    assert calls == []  # not even the source's row estimates


def test_the_card_and_the_early_deploy_except_a_dotted_database_or_schema(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: before a Full load + CDC load, the Full Load card's warning, the
    # early Deploy form and its click offered my.db.orders (database my.db) the
    # load-then-untick route and a Full load only copy, which no step supports for it.
    # Before any stack they judge the names for the plugin this version bundles, pinned here
    # to one older than v57 (with no Evaluation report, a name with two '.' is refused there).
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v56")
    names = ["shop.orders", "my.db.orders"]
    inv = _inventory(names)
    facts = f"CDC cannot stream my.db.orders{_old_plugin_fact('v56')} {_DOTTED_SCHEMA}"
    assert pre_load_topic_name_warning(_state(names), _Jobs({}), None, inv) == (
        CDC_TOPIC_NAME_HEADER,
        f"{facts} Deploy CDC infrastructure and Start CDC refuse a capture holding such a "
        f"table. {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}",
    )
    early = f"{facts} {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}"
    assert _deploy_block_notice(_deploy_ready(names, FLC), _Jobs({}), inv, None) == (
        CDC_TOPIC_NAME_HEADER, early)
    calls = _fake_nicegui(monkeypatch, None)
    assert _click_deploy(_deploy_ready(names, FLC), inv) == [(early, "negative")]
    assert calls == []


def test_the_same_click_with_names_cdc_can_stream_goes_on_to_the_reads(monkeypatch) -> None:
    # Guards the guard: with names CDC can stream, the click reads the source's row estimates
    # and goes on to an AWS call, which the fake fails, so the refused click's empty record is
    # the refusal's doing.
    calls = _fake_nicegui(monkeypatch, None)
    legal = ["shop.orders", "shop.items"]
    with pytest.raises(AssertionError, match="no AWS call may run before the refusal"):
        _click_deploy(_deploy_ready(legal), _inventory(legal))
    assert len(calls) == 2, calls  # the row estimates, then the AWS call


def test_the_start_section_holds_start_with_the_refusal(monkeypatch) -> None:
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui

    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    # A source column missing from the target waits its turn: one notice at a time.
    monkeypatch.setattr(_cdc_ui, "missing_column_block_reason", lambda *a, **k: "MISSING")
    state = _state(_NAMES, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)  # the CDC checks passed (they gate first)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _Jobs({}), lambda: None, inventory=_inventory(_NAMES), session=None,
    )
    (hit,) = [n for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER]
    assert hit["tone"] == "error"
    assert hit["body"] == f"{_REFUSAL} The table picker is locked. {_INFRA_LOCK}"
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in start.props_seen
    assert hit["body"] in ui.tooltips and "MISSING" not in ui.tooltips
    assert not [n for n in notices if n.get("header") == _MISSING_HEADER]


_SESSION = SimpleNamespace(source_config=None, target_config=None, aws_profile=None,
                           restored_source_type=None, source_password=None)


def _start_click(monkeypatch, state, jobs, names, *, retry=False, header=CDC_TOPIC_NAME_HEADER):
    """Start CDC's handler (Start click, or the Retry CDC button of a partial pipeline),
    returning ``(notices under header, submitted)``."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    ui = _RecordingUi()
    if retry:
        source, sink = cdc_expected_connector_names(state.cdc_stack_name)
        _cdc_ui._render_cdc_partial_actions(
            ui, state, jobs, lambda: None, names=[source], expected=(source, sink),
            inventory=_inventory(names), session=_SESSION,
        )
        (button,) = [b for b in ui.buttons if b.text == "Retry CDC"]
        button.on_click()
    else:
        _cdc_ui._start_cdc_deploy(
            ui, state, jobs, lambda: None, inventory=_inventory(names), session=_SESSION,
        )
    return [n for n in notices if n.get("header") == header], jobs.submitted


@pytest.mark.parametrize("retry", [False, True], ids=["start", "retry"])
def test_start_and_retry_refuse_before_the_submit_that_fixes_the_set(monkeypatch, retry) -> None:
    # FAILS WITHOUT THE FIX: the Start was submitted, which stamps the CDC operations window,
    # and from then on the table set was fixed -- so the failed Start left only Start over.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    state = _loaded_state(_NAMES)
    assert table_set_editable_after_load(state, jobs) is True
    hits, submitted = _start_click(monkeypatch, state, jobs, _NAMES, retry=retry)
    assert [h["body"] for h in hits] == [f"{_REFUSAL} {_OPEN}"]
    assert hits[0]["tone"] == "error"
    assert submitted == []
    assert state.cdc_ops_window_start is None
    # So the table can still be unticked.
    assert table_set_editable_after_load(state, jobs) is True


@pytest.mark.parametrize("retry", [False, True], ids=["start", "retry"])
def test_start_and_retry_except_a_dotted_database_or_schema(monkeypatch, retry) -> None:
    # FAILS WITHOUT THE FIX: after the load, Start and Retry CDC told my.db.orders (database
    # my.db) to be copied in a Full load only migration, which no step supports for it.
    names = ["shop.orders", "my.db.orders"]
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    hits, submitted = _start_click(monkeypatch, _loaded_state(names), jobs, names, retry=retry)
    assert [h["body"] for h in hits] == [
        f"CDC cannot stream my.db.orders{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY} {_OPEN}"]
    assert submitted == []


def test_a_set_fixed_by_an_earlier_failed_start_is_refused_with_start_over(monkeypatch) -> None:
    # v0.1.578: the first Start failed on such a name after its submit fixed the table set.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    state = _loaded_state(_NAMES)
    state.set_cdc_ops_window_start(_TS)
    hits, submitted = _start_click(monkeypatch, state, jobs, _NAMES)
    assert [h["body"] for h in hits] == [
        f"{_REFUSAL} The table picker is locked. {_FULL_LOAD_LOCK}"]
    assert submitted == []


def test_a_pipeline_that_streamed_with_a_comma_in_a_name_is_refused_at_its_next_start(
    monkeypatch,
) -> None:
    # Accepted (and to be announced): a v0.1.578 Start trimmed "shop.a,b" into two legal names
    # and never streamed it; its next Start is refused, with the lock's own remedy.
    names = ["shop.orders", "shop.a,b"]
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    state = _loaded_state(names)
    state.set_cdc_has_committed_offset(True)
    state.set_cdc_deployed_watermark(("mysql-bin.000042", "120"))
    hits, submitted = _start_click(monkeypatch, state, jobs, names)
    (hit,) = hits
    assert hit["body"].startswith("CDC cannot stream shop.a,b: ")
    assert hit["body"].endswith(f"The table picker is locked. {_FULL_LOAD_LOCK}")
    assert submitted == []


@pytest.mark.parametrize("retry", [False, True], ids=["start", "retry"])
def test_start_and_retry_refuse_a_colliding_pair_before_the_submit(monkeypatch, retry) -> None:
    # FAILS WITHOUT THE FIX: the Start was submitted (fixing the table set), then the seeder's
    # pre-create of the second topic failed.
    names = ["shop.orders", "app.v2_users", "app_v2.users"]
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    state = _loaded_state(names)
    hits, submitted = _start_click(monkeypatch, state, jobs, names, retry=retry)
    refusal = (
        f"CDC cannot stream app.v2_users together with app_v2.users{_COLLISION_FACT} them. "
        + _REMEDY)
    assert [h["body"] for h in hits] == [f"{refusal} {_OPEN}"]
    assert hits[0]["tone"] == "error"
    assert submitted == []
    assert state.cdc_ops_window_start is None
    assert table_set_editable_after_load(state, jobs) is True


def test_a_pipeline_that_streamed_with_a_dot_in_a_name_is_refused_at_its_next_start(
    monkeypatch,
) -> None:
    # Accepted (and to be announced): a v0.1.578 Start streamed "shop.a.b", whose every change
    # failed in the sink and whose deletes reached another table; its next Start is refused,
    # with the lock's own remedy (Start over).
    names = ["shop.orders", "shop.a.b"]
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    state = _loaded_state(names)
    state.set_cdc_has_committed_offset(True)
    state.set_cdc_deployed_watermark(("mysql-bin.000042", "120"))
    hits, submitted = _start_click(monkeypatch, state, jobs, names)
    refusal = f"CDC cannot stream shop.a.b{_OLD_PLUGIN_FACT} {_DOTTED_SCHEMA} {_REMEDY}"
    assert [h["body"] for h in hits] == [
        f"{refusal} The table picker is locked. {_FULL_LOAD_LOCK}"]
    assert submitted == []


def test_start_cdc_builds_the_connector_config_only_for_names_it_can_stream(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _Reached(Exception):
        pass

    def _spy(*_a, **_k):
        raise _Reached()

    monkeypatch.setattr(_cdc_ui, "dispatch_source_config", _spy)
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    hits, _submitted = _start_click(monkeypatch, _loaded_state(_NAMES), jobs, _NAMES)
    assert len(hits) == 1  # refused before the config is built
    legal = ["shop.orders", "shop.items"]  # guards the guard: these reach it
    jobs = _Jobs({"job-1": _loaded(legal, _counts(legal))})
    with pytest.raises(_Reached):
        _start_click(monkeypatch, _loaded_state(legal), jobs, legal)


def test_the_table_cap_speaks_first_when_both_apply(monkeypatch) -> None:
    # One notice at a time: a capture over one pipeline's table cap that also holds a name CDC
    # cannot stream is refused for the cap first, on every surface that holds both.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    many = [*_band(300), "shop.a b"]
    inv = _inventory(many)
    assert cdc_topic_name_refusal(many) is not None
    assert _deploy_block_notice(_deploy_ready(many), _Jobs({}), inv, None)[0] == (
        CDC_TABLE_LIST_HEADER)
    calls = _fake_nicegui(monkeypatch, None)
    ((text, _tone),) = _click_deploy(_deploy_ready(many), inv)
    assert "(SinkTopics), over CloudFormation's" in text and calls == []
    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    state = _state(many, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _Jobs({}), lambda: None, inventory=inv, session=None,
    )
    (cap,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert not [n for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER]
    assert cap["body"] in ui.tooltips
    jobs = _Jobs({"job-1": _loaded(many, _counts(many))})
    hits, submitted = _start_click(
        monkeypatch, _loaded_state(many), jobs, many, header=CDC_TABLE_LIST_HEADER)
    assert len(hits) == 1 and submitted == []


# ---------------------------------------------------------------------------
# 4b. Who each surface judges the capture for: Deploy CDC infrastructure (and the Full Load
#     card while no stack is deployed, or while this session's deploy creates one) for the
#     sink plugin this version bundles, which the deploy uploads; Start CDC and Retry CDC (and
#     the card once a stack is deployed) for the deployed stack's, which Start keeps. Every
#     surface with Evaluation's DOTTED_SCHEMA_NAME tables.
# ---------------------------------------------------------------------------

# A '.' inside a table name (MySQL table orders.v2 of database shop), beside a plain table.
_DOTTED_TABLE = ["shop.orders", "shop.orders.v2"]
# What it gets where it is refused: with no Evaluation report, a refused name with two '.'
# may be a table of a database whose name holds one, so the sentence follows.
_DOTTED_TABLE_FACTS = "CDC cannot stream shop.orders.v2{fact} " + _DOTTED_SCHEMA
# Table t of database my.db, which Evaluation reported DOTTED_SCHEMA_NAME on.
_DOTTED_DB = ["shop.orders", "my.db.t"]
_DOTTED_DB_FACTS = f"CDC cannot stream my.db.t{_DOTTED_SCHEMA_FACT} {_DOTTED_SCHEMA}"


def _ui_calls(path):
    """``(enclosing function, callee, call)`` of each topic-name call in a UI module."""
    tree = ast.parse(path.read_text())
    wanted = {"cdc_topic_name_refusal", "cdc_topic_name_refused",
              "cdc_pre_load_topic_name_warning", "_topic_name_context",
              "_topic_name_hold_remedy", "warning"}
    found = []
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(fn):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) in wanted:
                    found.append((fn.name, node.func.id, node))
    return found


def test_every_topic_name_check_of_the_ui_says_who_it_judges_for() -> None:
    # FAILS WITHOUT THE FIX: no UI check passed a sink plugin or Evaluation's tables, so each
    # judged an unknown plugin -- the oldest: a '.' inside a table name was refused even where
    # the stack's sink reads it, an older plugin went unnamed, and a table of a database or
    # schema whose name holds a '.' was not refused where the sink reads such a name.
    import dsql_migrator.ui as ui_pkg

    root = pathlib.Path(ui_pkg.__file__).parent
    checks = {"cdc_topic_name_refusal", "cdc_topic_name_refused",
              "cdc_pre_load_topic_name_warning"}
    contexts: dict = {}
    for path in sorted(root.rglob("*.py")):
        for fn, callee, call in _ui_calls(path):
            if callee in checks:
                keywords = {k.arg for k in call.keywords}
                spread = [ast.unparse(k.value) for k in call.keywords if k.arg is None]
                assert {"sink_plugin", "dotted_schema_tables"} <= keywords or [
                    text for text in spread if text.startswith("_topic_name_context(")
                ], (path.name, fn, call.lineno)
            if callee == "_topic_name_context":
                (deploy,) = [ast.unparse(k.value) for k in call.keywords if k.arg == "deploy"]
                contexts.setdefault(fn, set()).add(deploy)
            if callee == "_topic_name_hold_remedy":
                (deploy,) = [ast.unparse(k.value) for k in call.keywords if k.arg == "deploy"]
                contexts.setdefault(f"remedy in {fn}", set()).add(deploy)
            if callee == "warning" and fn == "_pre_load_cdc_warning":
                assert [k for k in call.keywords if k.arg is None
                        and ast.unparse(k.value) == "context"], call.lineno
    # Deploy for the bundled plugin; Start and Retry (the Start click) for the deployed one; the
    # split-read remedy as its surface; the card for the stack the next operation judges for.
    assert contexts == {
        "_deploy_block_notice": {"True"},
        "_start_cdc_infra_deploy": {"True"},
        "remedy in _deploy_topic_name_refused": {"True"},
        "_render_cdc_start_button": {"False"},
        "remedy in _render_cdc_start_button": {"False"},
        "_start_cdc_deploy": {"False"},
        "remedy in _start_cdc_deploy": {"False"},
        "_topic_name_hold_remedy": {"deploy"},
        "_pre_load_cdc_warning": {"not (planned and prep == 'ready')"},
    }


def test_the_context_names_the_bundled_or_the_deployed_plugin(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _topic_name_context

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v99")
    state = _state(_NAMES, phase="infra")
    assert _topic_name_context(state, deploy=True) == {
        "sink_plugin": "v99", "dotted_schema_tables": None}
    assert _topic_name_context(state, deploy=False) == {
        "sink_plugin": None, "dotted_schema_tables": None}
    state.set_cdc_deployed_plugin_version("v56")
    state.set_cdc_dotted_schema_tables(["my.db.t"])
    assert _topic_name_context(state, deploy=True) == {
        "sink_plugin": "v99", "dotted_schema_tables": frozenset({"my.db.t"})}
    assert _topic_name_context(state, deploy=False) == {
        "sink_plugin": "v56", "dotted_schema_tables": frozenset({"my.db.t"})}


@pytest.mark.parametrize("bundled", ["v56", "v57"])
def test_the_deploy_surfaces_judge_the_bundled_sink_plugin(monkeypatch, bundled) -> None:
    # FAILS WITHOUT THE FIX: the Deploy form, its click and the Full Load card before any stack
    # judged an unknown plugin: shop.orders.v2 was refused although the plugin the deploy
    # uploads (v57) streams it, and an older bundled one went unnamed. A stale deployed
    # version (the other one) is not what a deploy judges.
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", bundled)
    stale = "v56" if bundled == "v57" else "v57"
    names, inv = _DOTTED_TABLE, _inventory(_DOTTED_TABLE)
    facts = _DOTTED_TABLE_FACTS.format(fact=_old_plugin_fact(bundled))
    before, cdc_only = _deploy_ready(names, FLC), _deploy_ready(names)
    card = _state(names)
    for state in (before, cdc_only, card):
        state.set_cdc_deployed_plugin_version(stale)
    if bundled == "v57":
        assert _deploy_block_notice(before, _Jobs({}), inv, None) is None
        assert _deploy_block_notice(cdc_only, _Jobs({}), inv, None) is None
        ui, notices = _render_deploy(monkeypatch, cdc_only, _Jobs({}), inv)
        assert not [n for n in notices if n[1] == CDC_TOPIC_NAME_HEADER]
        assert ui.deploy_button().enabled is True
        assert pre_load_topic_name_warning(card, _Jobs({}), None, inv) is None
        # The click goes on to the source's row estimates, then to an AWS call (which the fake
        # fails): the names did not stop it.
        calls = _fake_nicegui(monkeypatch, None)
        with pytest.raises(AssertionError, match="no AWS call may run before the refusal"):
            _click_deploy(cdc_only, inv)
        assert len(calls) == 2, calls
        return
    early = f"{facts} {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}"
    assert _deploy_block_notice(before, _Jobs({}), inv, None) == (CDC_TOPIC_NAME_HEADER, early)
    after = f"{facts} {_REMEDY} {_OPEN}"
    assert _deploy_block_notice(cdc_only, _Jobs({}), inv, None) == (CDC_TOPIC_NAME_HEADER, after)
    ui, notices = _render_deploy(monkeypatch, cdc_only, _Jobs({}), inv)
    assert ("error", CDC_TOPIC_NAME_HEADER, after) in notices
    assert ui.deploy_button().enabled is False
    assert pre_load_topic_name_warning(card, _Jobs({}), None, inv) == (
        CDC_TOPIC_NAME_HEADER,
        f"{facts} Deploy CDC infrastructure and Start CDC refuse a capture holding such a "
        f"table. {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}",
    )
    calls = _fake_nicegui(monkeypatch, None)
    assert _click_deploy(before, inv) == [(early, "negative")]
    assert _click_deploy(cdc_only, inv) == [(after, "negative")]
    assert calls == []


class _Reached(Exception):
    pass


@pytest.mark.parametrize("deployed", [None, "v56", "v57", "v58"])
@pytest.mark.parametrize("retry", [False, True], ids=["start", "retry"])
def test_start_and_retry_judge_the_deployed_sink_plugin(monkeypatch, deployed, retry) -> None:
    # FAILS WITHOUT THE FIX: Start CDC and Retry CDC judged an unknown plugin, so
    # shop.orders.v2 was refused on a stack whose sink streams it (v57 or later), and an older
    # one went unnamed. The bundled plugin (pinned to the other kind) is not what they judge:
    # Start CDC keeps the plugin the stack was deployed with. None (not read) is judged as the
    # oldest, as before: that case is a guard and passes before and after.
    from dsql_migrator.ui.data_migration import _cdc_ui

    reads = plugin_reads_dotted_tables(deployed)
    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v56" if reads else "v57")
    names = _DOTTED_TABLE
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    state = _loaded_state(names)
    state.set_cdc_deployed_plugin_version(deployed)
    if reads:
        # Past the names, the Start builds the connector configuration (spied on here).
        monkeypatch.setattr(
            _cdc_ui, "dispatch_source_config", lambda *_a, **_k: (_ for _ in ()).throw(_Reached()))
        with pytest.raises(_Reached):
            _start_click(monkeypatch, state, jobs, names, retry=retry)
        return
    hits, submitted = _start_click(monkeypatch, state, jobs, names, retry=retry)
    facts = _DOTTED_TABLE_FACTS.format(fact=_old_plugin_fact(deployed))
    assert [h["body"] for h in hits] == [f"{facts} {_REMEDY} {_OPEN}"]
    assert submitted == []


@pytest.mark.parametrize("deployed", [None, "v56", "v57"])
def test_the_start_section_judges_the_deployed_sink_plugin(monkeypatch, deployed) -> None:
    # FAILS WITHOUT THE FIX: the Start section held Start CDC for shop.orders.v2 on a v57 stack,
    # and could not name an older one. None (not read): judged as the oldest, as before (a
    # guard).
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v56" if deployed == "v57" else "v57")
    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    names = _DOTTED_TABLE
    state = _state(names, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_cdc_deployed_plugin_version(deployed)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _Jobs({}), lambda: None, inventory=_inventory(names), session=None,
    )
    hits = [n for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER]
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    if deployed == "v57":
        assert hits == []
        assert not [t for t in ui.tooltips if "CDC cannot stream" in t]
        return
    facts = _DOTTED_TABLE_FACTS.format(fact=_old_plugin_fact(deployed))
    assert [h["body"] for h in hits] == [
        f"{facts} {_REMEDY} The table picker is locked. {_INFRA_LOCK}"]
    assert "disable" in start.props_seen


def test_the_full_load_card_judges_for_the_stack_the_next_cdc_operation_judges_for(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the card judged an unknown plugin whatever the state. A deployed
    # stack is judged on its own plugin (Start CDC keeps it); no stack, one being deleted, or
    # the one this session's deploy is creating, on the bundled one (the deploy records it).
    from dsql_migrator.core.models import MigrationJob as _Job

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v56")
    names, inv = _DOTTED_TABLE, _inventory(_DOTTED_TABLE)
    v56 = _DOTTED_TABLE_FACTS.format(fact=_old_plugin_fact("v56"))
    ready = _state(names, phase="infra")
    ready.set_cdc_deployed_plugin_version("v57")
    assert pre_load_topic_name_warning(ready, _Jobs({}), None, inv) is None
    ready.set_cdc_deployed_plugin_version("v56")
    _h, body = pre_load_topic_name_warning(ready, _Jobs({}), None, inv)
    assert body.startswith(f"{v56} Start CDC refuses a capture holding such a table. ")
    # This session's deploy is creating the stack: the bundled plugin, whatever was read last.
    deploying = _state(names)
    deploying.set_cdc_deployed_plugin_version("v57")
    deploying.set_cdc_deploy_job_id("d-1", kind="infra")
    running = _Jobs({"d-1": _Job(job_id="d-1", status="RUNNING", chunks=[])})
    _h, body = pre_load_topic_name_warning(deploying, running, None, inv)
    assert body.startswith(f"{v56} Start CDC refuses a capture holding such a table. ")
    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v57")
    assert pre_load_topic_name_warning(deploying, running, None, inv) is None
    # A deployed stack being deleted: the next one is deployed with the bundled plugin.
    deleting = _state(names, phase=None)
    deleting.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
    deleting.set_cdc_deployed_plugin_version("v56")
    assert pre_load_topic_name_warning(deleting, _Jobs({}), None, inv) is None


def test_evaluations_dotted_schema_tables_are_refused_on_every_surface(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: no surface passed Evaluation's DOTTED_SCHEMA_NAME tables, so on a
    # sink plugin that reads a '.' inside a table name, my.db.t (table t of database my.db) was
    # taken for table db.t of database my and streamed: on MySQL the escaped entry captures
    # both, and the sink dead-letters every change of the one whose database holds a '.'.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v57")
    names, inv = _DOTTED_DB, _inventory(_DOTTED_DB)

    def _known(state, tables=("my.db.t",)):
        state.set_cdc_deployed_plugin_version("v57")
        state.set_cdc_dotted_schema_tables(tables)
        return state

    early = f"{_DOTTED_DB_FACTS} {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}"
    assert _deploy_block_notice(_known(_deploy_ready(names, FLC)), _Jobs({}), inv, None) == (
        CDC_TOPIC_NAME_HEADER, early)
    assert pre_load_topic_name_warning(_known(_state(names)), _Jobs({}), None, inv) == (
        CDC_TOPIC_NAME_HEADER,
        f"{_DOTTED_DB_FACTS} Deploy CDC infrastructure and Start CDC refuse a capture holding "
        f"such a table. {_ROUTES_DOTTED}{_ROUTES_OR}{_OPEN}",
    )
    calls = _fake_nicegui(monkeypatch, None)
    assert _click_deploy(_known(_deploy_ready(names, FLC)), inv) == [(early, "negative")]
    assert calls == []
    for retry in (False, True):
        jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
        hits, submitted = _start_click(
            monkeypatch, _known(_loaded_state(names)), jobs, names, retry=retry)
        assert [h["body"] for h in hits] == [f"{_DOTTED_DB_FACTS} {_REMEDY} {_OPEN}"], retry
        assert submitted == []
    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    section = _known(_state(names, phase="infra"))
    section.set_prereq_gated_mode(MigrationMode.CDC)
    _cdc_ui._render_cdc_start_button(
        _RecordingUi(), section, _Jobs({}), lambda: None, inventory=inv, session=None)
    assert [n["body"] for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER] == [
        f"{_DOTTED_DB_FACTS} {_REMEDY} The table picker is locked. {_INFRA_LOCK}"]
    # Guards the guard: Evaluation found no such database (a known, empty set), so my.db.t is
    # a '.' inside a table name, which a v57 stack streams; with no Evaluation report the same
    # (the sink dead-letters a change whose database holds a '.', the backstop).
    for tables in ((), None):
        assert _deploy_block_notice(
            _known(_deploy_ready(names, FLC), tables), _Jobs({}), inv, None) is None, tables
        assert pre_load_topic_name_warning(
            _known(_state(names), tables), _Jobs({}), None, inv) is None, tables


def test_a_table_read_in_parts_is_named_only_where_its_surface_refuses_it() -> None:
    # FAILS WITHOUT THE FIX: the split-read remedy judged its tables for an unknown plugin, so
    # on a v57 stack it named shop.orders.v2, which the refusal before it does not name.
    from dsql_migrator.ui.data_migration._cdc_ui import _topic_name_hold_remedy

    names = ["app.users", "app.ev ents", "shop.orders.v2"]
    job = _split_job(names, split={"app.ev ents", "shop.orders.v2"})
    state = _loaded_state(names)
    state.set_cdc_deployed_plugin_version("v57")
    refusal = cdc_topic_name_refusal(names, sink_plugin="v57")
    assert "shop.orders.v2" not in refusal
    text = _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": job}), None, deploy=False)
    assert "The Full Load read app.ev ents in parts, so it cannot be deselected" in text
    assert text.count("shop.orders.v2") == 0
    # On an older stack the refusal names both, and so does the remedy.
    state.set_cdc_deployed_plugin_version("v56")
    refusal = cdc_topic_name_refusal(names, sink_plugin="v56")
    text = _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": job}), None, deploy=False)
    assert "The Full Load read app.ev ents, shop.orders.v2 in parts, so they cannot" in text


def test_the_screen_hands_evaluations_dotted_schema_tables_to_the_state(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: nothing recorded Evaluation's DOTTED_SCHEMA_NAME tables, so every
    # surface judged without them. Each render reads the current report.
    from dsql_migrator.core.assessor import (
        CompatibilityAssessor,
        dotted_schema_findings,
        with_object_findings,
    )
    from dsql_migrator.core.models import TargetInventory
    from dsql_migrator.ui.evaluation import EvaluationResult

    from tests.test_g12_table_set_after_load import _loaded as _g12_loaded
    from tests.test_g12_table_set_after_load import _screen

    s = _screen(monkeypatch, job=_g12_loaded(), selected=["app.a", "app.b"])
    s.content(lambda: None)
    assert s.state.cdc_dotted_schema_tables == frozenset()  # a report with no such finding
    inventory = _inventory(["app.a", "my.db.t", "my.db.v2.x", "shop.a.b"])
    report = with_object_findings(
        CompatibilityAssessor(source_type=SourceType.MYSQL).assess(inventory),
        dotted_schema_findings(inventory, ["my.db"]),
    )
    s.evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=report, target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[]))
    s.content(lambda: None)
    assert s.state.cdc_dotted_schema_tables == frozenset({"my.db.t", "my.db.v2.x"})
    s.evals.get_or_create("s1").clear_outputs()
    s.content(lambda: None)
    assert s.state.cdc_dotted_schema_tables is None  # no report: unknown


def test_the_dotted_schema_tables_are_an_evaluation_fact_not_a_stacks() -> None:
    # A stack change (another name, an absent stack, an adopted one) keeps them -- they come
    # from the Evaluation report, which it does not change; Start over resets them.
    from dsql_migrator.ui.data_migration import DataMigrationStore

    store = DataMigrationStore()
    state = store.get_or_create("s1")
    assert state.cdc_dotted_schema_tables is None
    state.set_cdc_dotted_schema_tables(["my.db.t", "my.db.t"])
    assert state.cdc_dotted_schema_tables == frozenset({"my.db.t"})
    state.set_cdc_stack_name("dsql-cdc-other")
    state.set_cdc_stack_phase("absent")
    state.adopt_cdc_stack("dsql-cdc-third")
    assert state.cdc_dotted_schema_tables == frozenset({"my.db.t"})
    state.set_cdc_dotted_schema_tables(())
    assert state.cdc_dotted_schema_tables == frozenset()
    state.set_cdc_dotted_schema_tables(["my.db.t"])
    store.reset_in_place("s1")
    assert state.cdc_dotted_schema_tables is None
    state.set_cdc_dotted_schema_tables(None)
    assert state.cdc_dotted_schema_tables is None


def test_the_stack_probe_reads_an_escaped_include_entry_back_as_its_table(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the reconciled table set (an adopted pipeline's tables) and each
    # attach candidate's held shop\.orders\.v2, which names no table, so the adopted
    # pipeline's dotted table was unresolved and the attach scope check saw another set.
    import dsql_migrator.core.cdc_deployer as deployer_mod
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    deployed = "shop\\.orders\\.v2,shop.items"
    assert deployed == ",".join(table_include_entry(n) for n in ("shop.orders.v2", "shop.items"))
    state = DataMigrationState()

    def _disc(params):
        return SimpleNamespace(stack_status="UPDATE_COMPLETE", is_stable=True,
                               current_parameters=params)

    class _Deployer:
        def describe_stack_or_none(self, name):
            return _disc({"TableIncludeList": deployed if name == state.cdc_stack_name
                          else f" {deployed} ,"})

        def list_cdc_stacks(self):
            return [("dsql-cdc-other", "UPDATE_COMPLETE")]

    monkeypatch.setattr(deployer_mod, "build_cdc_stack_deployer", lambda *a, **k: _Deployer())
    session = SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
    _probe_cdc_stack_phase(state, session)
    assert state.cdc_reconciled_table_names == ["shop.orders.v2", "shop.items"]
    assert state.cdc_other_stack_tables == {"dsql-cdc-other": ["shop.orders.v2", "shop.items"]}


def test_cdc_rule_tables_reads_any_finding_of_the_rule() -> None:
    from dsql_migrator.ui.data_migration import cdc_cascade_gap_tables, cdc_rule_tables

    def _item(name, rule, *concerns):
        return SimpleNamespace(object_name=name, rule_id=rule,
                               concerns=[SimpleNamespace(rule_id=c) for c in concerns])

    report = SimpleNamespace(items=[
        _item("my.db.t", "DOTTED_SCHEMA_NAME", "DOTTED_SCHEMA_NAME"),
        _item("my.db.u", "PK_MISSING", "PK_MISSING", "DOTTED_SCHEMA_NAME"),
        _item("shop.c", "FK_CASCADE_CDC_GAP"),
        _item("my.db.t", "DOTTED_SCHEMA_NAME"),
    ])
    assert cdc_rule_tables(report, "DOTTED_SCHEMA_NAME") == ["my.db.t", "my.db.u"]
    assert cdc_rule_tables(report, "DOTTED_SCHEMA_NAME", ["my.db.u"]) == ["my.db.u"]
    assert cdc_rule_tables(None, "DOTTED_SCHEMA_NAME") == []
    assert cdc_cascade_gap_tables(report) == ["shop.c"]


# ---------------------------------------------------------------------------
# 5. The core backstops refuse before anything is changed, judged for the sink plugin of the
#    stack the capture is for: Deploy CDC infrastructure for the bundled one, the one it
#    uploads and records as the new stack's PluginVersion; Start CDC and Retry CDC for the
#    deployed one, read from the same describe.
# ---------------------------------------------------------------------------


def _refused_of_kind(kind, db, illegal):
    """Names of one kind CDC cannot stream (``illegal`` for that kind)."""
    return {"illegal": [illegal], "dotted": [f"{db}.a.b"],
            "collision": [f"{db}.v2_users", f"{db}_v2.users"],
            # Refused on every sink plugin, v57 included.
            "empty": [f".{db}.t"],
            "dotted_collision": [f"{db}.orders.v2", f"{db}.orders_v2"]}[kind]


# (sink plugin, kind of name it refuses): a '.' inside a table name is refused on a plugin
# older than v57 or one this version cannot identify (None), and only there.
_PLUGIN_KINDS = [
    *((plugin, kind) for plugin in (None, "v56") for kind in ("illegal", "dotted", "collision")),
    *(("v57", kind) for kind in ("illegal", "empty", "collision", "dotted_collision")),
]


def _dotted_name(engine):
    """A table whose name holds a '.', one of each engine's shapes: MySQL `shop`.`orders.v2`,
    PostgreSQL public."public.orders" (an older sink read its tombstone as public.orders)."""
    return "shop.orders.v2" if engine == "mysql" else "public.public.orders"


@pytest.mark.parametrize(("bundled", "kind"), [pk for pk in _PLUGIN_KINDS if pk[0]])
@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_deploy_refuses_before_the_bucket_or_any_upload(monkeypatch, engine, bundled, kind) -> None:
    # FAILS WITHOUT THE FIX for a '.' inside a table name: the deploy judged every name for a
    # plugin it could not identify, not for the one it bundles (here pinned, v56 or v57).
    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", bundled)
    db = "shop" if engine == "mysql" else "public"
    refused = _refused_of_kind(kind, db, f"{db}.order$items")
    names = [f"{db}.orders", *refused]
    params = _infra(engine, names, _counts(names))
    handle = _FakeHandle()
    logs, on_log = _logs()
    s3 = _FakeS3(absent=True)
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log, s3_client=s3, stack_name="dsql-cdc-shop",
            template_body="T", params=params,
            create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        )
    assert str(exc.value) == topic_name_refusal(params.filled, sink_plugin=bundled)
    if kind == "dotted":
        # The fact names the plugin the new stack would get, and Delete + Deploy.
        assert f"CDC cannot stream {db}.a.b{_old_plugin_fact(bundled)}" in str(exc.value)
    if kind in ("illegal", "collision"):
        # Judged alike on every plugin, and as the Data Migration step judges them.
        assert str(exc.value) == cdc_topic_name_refusal(names)
    assert s3.calls == [] and deployer.calls == [] and deployer.created == []
    assert set(_statuses(handle).values()) == {"PENDING"}
    assert handle.job.error_count == 1
    assert any(m.startswith(f"ERROR: CDC cannot stream {refused[0]}") for m in logs)


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_deploy_takes_a_dot_inside_a_table_name_with_a_bundled_plugin_that_reads_it(
    monkeypatch, engine
) -> None:
    # FAILS WITHOUT THE FIX: the deploy refused it, judged for a plugin it could not identify,
    # although the stack it creates gets the bundled plugin (here pinned to v57).
    monkeypatch.setattr(s3_provision, "PLUGIN_VERSION", "v57")
    db = "shop" if engine == "mysql" else "public"
    dotted = _dotted_name(engine)
    names = [f"{db}.orders", dotted]
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    _run_infra(
        handle, deployer, on_log, stack_name="dsql-cdc-shop", template_body="T",
        params=_infra(engine, names, _counts(names)),
        create_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert handle.job.error_count == 0
    (_stack, created), = deployer.created
    sent = dict(created)
    # The new stack records the plugin the names were judged for, so Start judges the same.
    assert sent["PluginVersion"] == "v57"
    assert sent["SinkTopics"] == f"dsqlcdc.{db}.orders,dsqlcdc.{dotted}"
    assert sent["TableIncludeList"] == f"{db}.orders,{table_include_entry(dotted)}"


@pytest.mark.parametrize(("plugin", "kind"), _PLUGIN_KINDS)
@pytest.mark.parametrize("engine", ["mysql", "postgres"])
@pytest.mark.parametrize("seed_mode", ["lambda", "external"])
@pytest.mark.parametrize("running", [False, True], ids=["create", "both_running"])
def test_start_refuses_before_bootstrap_seed_or_update(
    engine, seed_mode, running, plugin, kind
) -> None:
    # FAILS WITHOUT THE FIX for a deployed v56 (its plugin named) and for a pair a '.' inside
    # a table name collides in on v57: Start judged every name for an unknown plugin.
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders", *_refused_of_kind(kind, db, f"{db}.a,b")]
    params = _start(engine, names)
    states = ["RUNNING"] if running else ["CREATING", "RUNNING"]
    deployer = _FakeDeployer(
        connector_states={SRC: list(states), SINK: list(states)},
        # Both RUNNING on this very config: the fast path that sends nothing.
        discovery_params={
            **(dict(params.filled) if running else {}),
            **({"PluginVersion": plugin} if plugin else {}),
        },
    )
    seeds: list = []
    handle = _FakeHandle()
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(
            handle, stack_name=STACK, params=params, deployer=deployer,
            on_log=lambda ts, msg: None, sleep=lambda _s: None,
            connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
            seed_mode=seed_mode, seed_fn=lambda **kw: seeds.append(kw) or "true",
        )
    assert str(exc.value) == topic_name_refusal(params.filled, sink_plugin=plugin)
    if kind == "dotted":
        # The fact names the deployed plugin (or says it cannot tell), and Delete + Deploy.
        assert f"CDC cannot stream {db}.a.b{_old_plugin_fact(plugin)}" in str(exc.value)
    if kind == "dotted_collision":
        # On v57 a '.' inside a table name streams, so it collides like any other name.
        assert str(exc.value).startswith(
            f"CDC cannot stream {db}.orders.v2 together with {db}.orders_v2{_COLLISION_FACT}")
    if plugin is None:
        # A stack with no PluginVersion is judged as the Data Migration step judges a stack
        # whose plugin it does not know.
        assert str(exc.value) == cdc_topic_name_refusal(names)
    assert deployer.updates == []
    assert "submit_update" not in deployer.calls
    assert "get_bootstrap_brokers" not in deployer.calls
    assert seeds == []
    # The stage the job failed in: validate_params (discover_stack only reads).
    statuses = _statuses(handle)
    assert statuses["discover_stack"] == "DONE"
    assert statuses["validate_params"] == "IN_PROGRESS"
    assert statuses["fetch_bootstrap"] == "PENDING"
    assert handle.job.error_count == 1


def test_names_cdc_can_stream_reach_the_stack_update() -> None:
    # Guards the guard: names CDC can stream go on to submit_update, their topics in SinkTopics,
    # so the backstops above see no update because they refuse.
    legal = ["shop.orders", "shop.items"]
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    _start_with(_start("mysql", legal), deployer)
    assert "submit_update" in deployer.calls
    assert dict(deployer.updates[0])["SinkTopics"] == "dsqlcdc.shop.orders,dsqlcdc.shop.items"


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
@pytest.mark.parametrize("seed_mode", ["lambda", "external"])
def test_a_dot_inside_a_table_name_reaches_the_stack_update_on_a_v57_stack(
    engine, seed_mode
) -> None:
    # FAILS WITHOUT THE FIX: Start refused it, judged for a plugin it could not identify,
    # although the describe it had just read says v57.
    db = "shop" if engine == "mysql" else "public"
    dotted = _dotted_name(engine)
    params = _start(engine, [f"{db}.orders", dotted])
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]},
        discovery_params={"PluginVersion": "v57"},
    )
    seeds: list = []
    handle = _FakeHandle()
    run_cdc_start(
        handle, stack_name=STACK, params=params, deployer=deployer,
        on_log=lambda ts, msg: None, sleep=lambda _s: None,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
        seed_mode=seed_mode, seed_fn=lambda **kw: seeds.append(kw) or "true",
    )
    assert handle.job.error_count == 0
    assert deployer.calls.count("submit_update") == 1
    sent = dict(deployer.updates[0])
    topics = [f"dsqlcdc.{db}.orders", f"dsqlcdc.{dotted}"]
    assert sent["SinkTopics"] == ",".join(topics)
    # The exact escaped entry, so the source connector captures only that table.
    assert sent["TableIncludeList"] == f"{db}.orders,{table_include_entry(dotted)}"
    if seed_mode == "external":
        # The in-process seed pre-creates the dotted table's topic by its literal name.
        (seed,) = seeds
        assert seed["sink_topics"] == topics
    else:
        assert seeds == []


# ---------------------------------------------------------------------------
# 6. The manual says it, in English, Korean and Japanese alike
# ---------------------------------------------------------------------------

_DOC_FACTS = {
    "en": ("`.`, `_` and `-`", "**A table whose name CDC cannot stream**"),
    "ko": ("`.`, `_`, `-`", "**CDC가 스트리밍할 수 없는 이름의 테이블**"),
    "ja": ("`.`、`_`、`-`", "**CDC がストリーミングできない名前のテーブル**"),
}
# Chapter 6 names, in every language, a pair that differs only in a '.' against a '_', and
# how the refusal writes a character that does not show.
_DOC_NAMES = ("`app.v2_users`", "`app_v2.users`", "`<U+0020>`")


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_names_the_refusal_and_the_rule(lang) -> None:
    from tests.test_g25_docs import _has, _page

    chars, row = _DOC_FACTS[lang]
    limitations = _page(lang, "06-limitations.md")
    assert _has(lang, limitations, f"*{CDC_TOPIC_NAME_HEADER}*")
    assert _has(lang, limitations, chars)
    for name in _DOC_NAMES:
        assert _has(lang, limitations, name), name
    assert _has(lang, _page(lang, "13-excluding-tables-and-columns.md"), row)


# Chapter 6.2 says that a '.' inside a database or schema name is supported at no step, then
# links the findings reference (§2.4, which chapter 6 links twice before 6.2 too, so it is
# looked for after the finding), before the load-then-untick route, which it gives once, for
# any other such table only.
_DOC_FINDINGS_LINK = {
    "en": "(02-evaluation-and-schema-conversion.md#24-findings-reference)",
    "ko": "(02-evaluation-and-schema-conversion.md#24-evaluation-항목-참조)",
    "ja": "(02-evaluation-and-schema-conversion.md#24-検出結果リファレンス)",
}
_DOC_UNTICK_ROUTE = {
    "en": "load it, then untick it before CDC starts",
    "ko": "적재한 뒤 CDC를 시작하기 전에 선택을 해제하세요",
    "ja": "ロードしてから CDC を開始する前に選択を外してください",
}
_DOC_OTHER_TABLE = {
    "en": "For any other such table, with Full load + CDC, ",
    "ko": "그 밖의 그런 테이블은, Full load + CDC에서는 ",
    "ja": "それ以外のそのようなテーブルは、Full load + CDC では、",
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_excepts_a_dotted_database_or_schema_before_the_routes(lang) -> None:
    # FAILS WITHOUT THE FIX: the routes came first, as if they held for every such table, and
    # the dotted database or schema followed them without a link to its finding.
    from tests.test_g25_docs import _flat, _page

    part = _page(lang, "06-limitations.md").split(_flat(lang, "## 6.2"), 1)[1].split(
        _flat(lang, "## 6.3"), 1)[0]
    route = _flat(lang, _DOC_UNTICK_ROUTE[lang])
    dotted = part.find("(`DOTTED_SCHEMA_NAME`)")
    link = part.find(_flat(lang, _DOC_FINDINGS_LINK[lang]), max(dotted, 0))
    assert part.count(route) == 1, lang
    assert _flat(lang, _DOC_OTHER_TABLE[lang] + _DOC_UNTICK_ROUTE[lang]) in part, lang
    assert -1 < dotted < link < part.index(route), (lang, dotted, link)
