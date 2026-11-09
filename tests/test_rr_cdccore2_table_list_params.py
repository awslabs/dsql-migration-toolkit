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
* structure: no builder and no UI module spells the block (``"TopicPrefix"`` /
  ``"SinkTopics"`` tuples, the ``<prefix>.<table>`` join) again.

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
    # A name the topic-name refusal is for still reaches it verbatim.
    assert _cdc_table_lists(["s.a b", "s.x,y"]) == [
        ("TableIncludeList", "s.a b,s.x,y"), ("SinkTopics", "dsqlcdc.s.a b,dsqlcdc.s.x,y"),
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


def test_only_the_shared_block_spells_the_six_parameters() -> None:
    tree = ast.parse((_SRC / "core" / "cdc.py").read_text(encoding="utf-8"))
    assert _block_tuples(_function(tree, "cdc_table_list_params")) == [k for k, _ in _BLOCK]
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
    assert "cdc_table_list_params" in ast.unparse(lists)
    assert '",".join(' not in ast.unparse(lists)
    # No module under ui/ joins the topic prefix onto table names itself.
    for path in (_SRC / "ui").rglob("*.py"):
        assert "{CDC_DEFAULT_TOPIC_PREFIX}." not in path.read_text(encoding="utf-8"), path
