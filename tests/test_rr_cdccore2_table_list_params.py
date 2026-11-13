# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""cdccore-2: the cdc-stack's six table/topic parameters are derived in ONE place.

``TableIncludeList``, ``TopicPrefix``, ``SinkTopics``, ``DlqTopicName``,
``ColumnExcludeList`` and ``MessageKeyColumns`` were spelled out, in that order, in both
``build_cdc_stack_params`` (Start CDC) and ``build_cdc_infra_params`` (Deploy CDC
infrastructure), and the Data Migration step re-derived two of them
(``_models._cdc_table_lists``: TableIncludeList and SinkTopics "as the parameter builders
send them") for its pre-deploy size and topic-name refusals. ``core.cdc.cdc_table_list_params``
now owns the block; each builder splices it in at the position it had, and the UI reads its
two lists back out of it.

Pinned here:

* golden bytes: every builder's full ``filled`` list (keys, values, order) and the UI's two
  lists are exactly what they were before the refactor, for a populated config (custom
  topic prefix, column exclusions, a composite re-key, more sink topics than captured
  tables) and an empty one;
* the shared block on its own;
* structure: no function in ``core/cdc.py`` but the shared one spells more than one of the
  block's parameter tuples (the builders none: each splices the block in once), the UI's
  ``_cdc_table_lists`` calls the block and joins nothing itself, and no UI module builds
  ``<CDC_DEFAULT_TOPIC_PREFIX>.<table>`` in an f-string.

Pure: no AWS.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from dsql_migrator.core.cdc import (
    CDC_ENV_MCU_COUNT,
    CDC_ENV_SINK_TASKS_MAX,
    CDC_ENV_TOPIC_PARTITIONS,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
)
from dsql_migrator.ui.data_migration._models import (
    _cdc_deploy_lists,
    _cdc_table_lists,
    cdc_table_list_refusal,
    cdc_topic_name_refusal,
    cdc_topic_name_refused,
)

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"


@pytest.fixture(autouse=True)
def _no_scaling_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # The infra builder's scaling knobs are env-overridable; the golden is the default.
    for key in (CDC_ENV_MCU_COUNT, CDC_ENV_SINK_TASKS_MAX, CDC_ENV_TOPIC_PARTITIONS):
        monkeypatch.delenv(key, raising=False)


def _populated() -> tuple[DebeziumSourceConfig, SinkConnectorConfig]:
    source = DebeziumSourceConfig(
        name="mysql-source",
        table_include_list=["shop.orders", "shop.items"],
        column_exclude_list=["shop.orders.blob_col", "shop.items.doc"],
        message_key_columns={"shop.items": ["tenant_id", "id"]},
        snapshot_mode="schema_only",
    )
    # More sink topics than captured tables: the two lists come from different configs.
    sink = SinkConnectorConfig(
        name="mysql-sink", topics=["shop.orders", "shop.items", "shop.extra"], dlq_topic="my-dlq"
    )
    return source, sink


_INFRA_KW = dict(
    vpc_id="vpc-1", plugin_bucket_arn="arn:b", debezium_plugin_s3_key="d",
    dsql_sink_plugin_s3_key="s", source_db_hostname="h", source_secret_arn="arn:s",
    source_secret_name="n", dsql_cluster_arn="arn:c", target_endpoint="ep",
)

# Spelled out by hand from the pre-refactor builders' output (not built with the helper).
_BLOCK = [
    ("TableIncludeList", "shop.orders,shop.items"),
    ("TopicPrefix", "pfx"),
    ("SinkTopics", "pfx.shop.orders,pfx.shop.items,pfx.shop.extra"),
    ("DlqTopicName", "my-dlq"),
    ("ColumnExcludeList", "shop.orders.blob_col,shop.items.doc"),
    ("MessageKeyColumns", "shop\\.items:tenant_id,id"),
]

_STACK_GOLDEN = [
    *_BLOCK,
    ("SinkKeyColumns", ""),
    ("DsqlClusterEndpoint", "ep"),
    ("DsqlDatabaseName", "postgres"),
    ("DsqlConnectUser", "admin"),
    ("SnapshotMode", "schema_only"),
    ("SinkMcuCount", "2"),
    ("DeploySink", "false"),
]

_INFRA_GOLDEN = [
    ("VpcId", "vpc-1"), ("ConnectorSubnetIds", ""), ("NatPublicSubnetId", ""),
    ("PrivateSubnetCidrA", ""), ("PrivateSubnetCidrB", ""), ("PrivateSubnetAzA", ""),
    ("PrivateSubnetAzB", ""), ("SourceDbSecurityGroupId", ""), ("PluginBucketArn", "arn:b"),
    ("DebeziumPluginS3Key", "d"), ("DsqlSinkPluginS3Key", "s"), ("LambdaSeederS3Key", ""),
    ("SourceDbHostname", "h"), ("SourceDbPort", "3306"), ("SourceDbServerId", "54321"),
    ("SourceSecretArn", "arn:s"), ("SourceSecretName", "n"), ("DsqlClusterArn", "arn:c"),
    ("DsqlClusterEndpoint", "ep"), ("DsqlDatabaseName", "postgres"),
    ("DsqlConnectUser", "admin"),
    *_BLOCK,
    ("SnapshotMode", "schema_only"), ("PluginVersion", "v1"),
    ("TopicDefaultPartitions", "3"), ("SinkTasksMax", "8"), ("TopicCreationGroups", ""),
    ("TopicGroupInclude2", ""), ("TopicGroupInclude4", ""), ("SinkTopicPartitions", ""),
    ("ConnectorMcuCount", "2"), ("SinkMcuCount", "4"), ("MskBootstrapServers", ""),
    ("DeploySink", "false"), ("SeedMode", "Lambda"), ("HostSubnetCidr", ""),
]


# ---------------------------------------------------------------------------
# Golden bytes
# ---------------------------------------------------------------------------


def test_start_cdc_params_are_byte_identical() -> None:
    source, sink = _populated()
    params = build_cdc_stack_params(
        source, sink, target_endpoint="ep", topic_prefix="pfx", deploy_sink=False,
        sink_mcu_count=2,
    )
    assert params.filled == _STACK_GOLDEN


def test_deploy_infra_params_are_byte_identical() -> None:
    source, sink = _populated()
    params = build_cdc_infra_params(source, sink, topic_prefix="pfx", **_INFRA_KW)
    assert params.filled == _INFRA_GOLDEN


def test_an_empty_config_is_byte_identical() -> None:
    source = DebeziumSourceConfig(name="x")
    sink = SinkConnectorConfig(name="y", dlq_topic="d")
    assert build_cdc_stack_params(source, sink, target_endpoint="ep").filled == [
        ("TableIncludeList", ""), ("TopicPrefix", "dsqlcdc"), ("SinkTopics", ""),
        ("DlqTopicName", "d"), ("ColumnExcludeList", ""), ("MessageKeyColumns", ""),
        ("SinkKeyColumns", ""), ("DsqlClusterEndpoint", "ep"),
        ("DsqlDatabaseName", "postgres"), ("DsqlConnectUser", "admin"),
        ("SnapshotMode", "recovery"), ("SinkMcuCount", "4"), ("DeploySink", "true"),
    ]
    block = build_cdc_infra_params(source, sink, **_INFRA_KW).filled[21:27]
    assert block == [
        ("TableIncludeList", ""), ("TopicPrefix", "dsqlcdc"), ("SinkTopics", ""),
        ("DlqTopicName", "d"), ("ColumnExcludeList", ""), ("MessageKeyColumns", ""),
    ]


def test_the_ui_lists_are_byte_identical() -> None:
    assert _cdc_table_lists(["a.b", "c.d"]) == [
        ("TableIncludeList", "a.b,c.d"), ("SinkTopics", "dsqlcdc.a.b,dsqlcdc.c.d"),
    ]
    assert _cdc_table_lists([]) == [("TableIncludeList", ""), ("SinkTopics", "")]
    # A name the topic-name refusal is for still reaches it verbatim in SinkTopics, which it
    # reads. TableIncludeList carries each name's exact entry (core.cdc.table_include_entry):
    # "s.x,y" escapes its ',' and its separator, which only a refused name holds, so no
    # deployed value changes.
    assert _cdc_table_lists(["s.a b", "s.x,y"]) == [
        ("TableIncludeList", "s.a b,s\\.x\\,y"), ("SinkTopics", "dsqlcdc.s.a b,dsqlcdc.s.x,y"),
    ]
    # A '.' inside a table name is escaped in TableIncludeList, and the topic is as Debezium
    # names it (FAILS WITHOUT THE FIX: "shop.orders.v2" also captured shop.orders_v2).
    assert _cdc_table_lists(["shop.orders.v2", "shop.items"]) == [
        ("TableIncludeList", "shop\\.orders\\.v2,shop.items"),
        ("SinkTopics", "dsqlcdc.shop.orders.v2,dsqlcdc.shop.items"),
    ]
    assert _cdc_deploy_lists(["a.b"], None) == [
        ("TableIncludeList", "a.b"), ("SinkTopics", "dsqlcdc.a.b"),
        ("TopicDefaultPartitions", "8"), ("SinkTasksMax", "8"), ("TopicCreationGroups", ""),
        ("TopicGroupInclude2", ""), ("TopicGroupInclude4", ""), ("SinkTopicPartitions", ""),
    ]


def test_the_ui_lists_match_what_the_builders_send() -> None:
    """The UI's refusals judge exactly the TableIncludeList / SinkTopics the builders send."""
    names = ["shop.orders", "shop.order_items", "app.x"]
    source = DebeziumSourceConfig(name="mysql-source", table_include_list=names)
    sink = SinkConnectorConfig(name="mysql-sink", topics=names, dlq_topic="dsql-sink-dlq")
    sent = dict(build_cdc_stack_params(source, sink, target_endpoint="ep").filled)
    assert _cdc_table_lists(names) == [
        ("TableIncludeList", sent["TableIncludeList"]), ("SinkTopics", sent["SinkTopics"]),
    ]


def test_the_ui_refusals_are_unchanged() -> None:
    assert cdc_table_list_refusal(["a.b"], deploy=True) is None
    many = [f"shop.table_{i:04d}" for i in range(400)]
    assert "(SinkTopics)" in cdc_table_list_refusal(many, deploy=False)
    assert cdc_topic_name_refused(["s.ok", "s.a b", "s.x.y"]) == ["s.a b", "s.x.y"]
    assert cdc_topic_name_refusal(["s.ok"]) is None


# ---------------------------------------------------------------------------
# The shared block, and structure
# ---------------------------------------------------------------------------


def test_the_shared_block_on_its_own() -> None:
    from dsql_migrator.core.cdc import cdc_table_list_params

    source, sink = _populated()
    assert cdc_table_list_params(source, sink, "pfx") == _BLOCK
    assert cdc_table_list_params(source, sink)[1:3] == [
        ("TopicPrefix", "dsqlcdc"),
        ("SinkTopics", "dsqlcdc.shop.orders,dsqlcdc.shop.items,dsqlcdc.shop.extra"),
    ]


def test_the_shared_block_escapes_a_dot_inside_a_table_name() -> None:
    # FAILS WITHOUT THE FIX: TableIncludeList sent shop.orders.v2 as written, a Debezium regex
    # whose '.' matches any character. Both builders send the escaped entry; SinkTopics and the
    # re-key's MessageKeyColumns (already escaped) are unchanged.
    from dsql_migrator.core.cdc import cdc_table_list_params

    names = ["shop.orders.v2", "shop.items"]
    source = DebeziumSourceConfig(
        name="mysql-source", table_include_list=names,
        message_key_columns={"shop.orders.v2": ["tenant_id", "id"]},
    )
    sink = SinkConnectorConfig(name="mysql-sink", topics=names, dlq_topic="d")
    block = dict(cdc_table_list_params(source, sink))
    assert block["TableIncludeList"] == "shop\\.orders\\.v2,shop.items"
    assert block["SinkTopics"] == "dsqlcdc.shop.orders.v2,dsqlcdc.shop.items"
    assert block["MessageKeyColumns"] == "shop\\.orders\\.v2:tenant_id,id"
    for filled in (
        build_cdc_stack_params(source, sink, target_endpoint="ep").filled,
        build_cdc_infra_params(source, sink, **_INFRA_KW).filled,
    ):
        assert dict(filled)["TableIncludeList"] == block["TableIncludeList"]


_BLOCK_KEYS = {key for key, _ in _BLOCK}


def _block_tuples(tree: ast.AST) -> list[str]:
    """The first element of every ``("<block key>", ...)`` 2-tuple literal under ``tree``."""
    return [
        node.elts[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Tuple)
        and len(node.elts) == 2
        and isinstance(node.elts[0], ast.Constant)
        and node.elts[0].value in _BLOCK_KEYS
    ]


def _function(tree: ast.AST, name: str) -> ast.FunctionDef:
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _body_without_docstring(fn: ast.FunctionDef) -> list:
    body = list(fn.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(
        getattr(body[0], "value", None), ast.Constant
    ) and isinstance(body[0].value.value, str):
        body = body[1:]
    return body


def test_only_the_shared_block_spells_the_six_parameters() -> None:
    tree = ast.parse((_SRC / "core" / "cdc.py").read_text(encoding="utf-8"))
    assert _block_tuples(_function(tree, "cdc_table_list_params")) == [k for k, _ in _BLOCK]
    # No other function in cdc.py spells the block, or a piece of it beyond a single key
    # (rekey_parameter_size_refusal names MessageKeyColumns for its own size refusal).
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name != "cdc_table_list_params":
            assert len(set(_block_tuples(fn))) <= 1, fn.name
    for builder in ("build_cdc_stack_params", "build_cdc_infra_params"):
        assert _block_tuples(_function(tree, builder)) == [], builder
        # ... and each splices the block in exactly once.
        spliced = [
            node for node in ast.walk(_function(tree, builder))
            if isinstance(node, ast.Starred)
            and isinstance(node.value, ast.Call)
            and getattr(node.value.func, "id", None) == "cdc_table_list_params"
        ]
        assert len(spliced) == 1, builder


def test_the_ui_does_not_rederive_the_lists() -> None:
    tree = ast.parse(
        (_SRC / "ui" / "data_migration" / "_models.py").read_text(encoding="utf-8")
    )
    lists = _function(tree, "_cdc_table_lists")
    body = _body_without_docstring(lists)  # the docstring names the helper: not a call
    calls = [
        node for stmt in body for node in ast.walk(stmt) if isinstance(node, ast.Call)
    ]
    called = {
        getattr(node.func, "id", None) or getattr(node.func, "attr", None) for node in calls
    }
    assert "cdc_table_list_params" in called
    assert "join" not in called  # the lists come out of the block, never re-joined here
    # No module under ui/ joins the topic prefix onto table names itself: no f-string
    # holds {CDC_DEFAULT_TOPIC_PREFIX} followed by a '.', however it is spelled or wrapped.
    for path in (_SRC / "ui").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.JoinedStr):
                continue
            parts = node.values
            for i, part in enumerate(parts[:-1]):
                if (
                    isinstance(part, ast.FormattedValue)
                    and getattr(part.value, "id", None) == "CDC_DEFAULT_TOPIC_PREFIX"
                    and isinstance(parts[i + 1], ast.Constant)
                    and str(parts[i + 1].value).startswith(".")
                ):
                    raise AssertionError(f"{path}:{node.lineno} joins the topic prefix")
