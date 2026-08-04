# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for cdc-stack parameter generation (pure, no AWS/NiceGUI).

Covers the keystone that turns the user's CDC settings into a deployable
cdc-stack parameter set: tool-known values are filled, customer-environment
values become labeled placeholders, and the one transformation the template
delegates to the caller -- SinkTopics = <prefix>.<db>.<table> -- is applied here.
"""

from __future__ import annotations

import json

from datetime import datetime, timezone

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_MCU_COUNT,
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_ENV_MCU_COUNT,
    CDC_ENV_SINK_TASKS_MAX,
    CDC_ENV_TOPIC_PARTITIONS,
    CDC_MAX_SINK_PARALLELISM,
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_MAX_LEN,
    CDC_STACK_NAME_PREFIX,
    CDC_WATERMARK_PARAM_KEYS,
    CdcCostEstimate,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
    build_watermark_params,
    cdc_expected_connector_names,
    cdc_stack_name_is_valid,
    cdc_scaling_params,
    cdc_stack_params_to_json,
    compute_cdc_partition_plan,
    compute_cdc_scaling_defaults,
    estimate_cdc_hourly_cost,
)
from dsql_migrator.core.models import Watermark


def _source(tables, *, gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", exclude=None):
    return DebeziumSourceConfig(
        name="mysql-source",
        table_include_list=list(tables),
        start_gtid=gtid,
        column_exclude_list=list(exclude or []),
    )


def _sink(tables, *, dlq="dsql-sink-dlq"):
    return SinkConnectorConfig(name="mysql-sink", topics=list(tables), dlq_topic=dlq)


def _params(tables=("app.orders", "app.customers"), **kw):
    return build_cdc_stack_params(
        _source(tables, exclude=kw.pop("exclude", None)),
        _sink(tables, dlq=kw.pop("dlq", "dsql-sink-dlq")),
        target_endpoint=kw.pop("endpoint", "c.dsql.us-east-1.on.aws"),
        **kw,
    )


# ---------------------------------------------------------------------------
# cdc_expected_connector_names
# ---------------------------------------------------------------------------


def test_expected_connector_names_default() -> None:
    assert cdc_expected_connector_names() == (
        "mysql-dsql-cdc-stack-debezium-source",
        "mysql-dsql-cdc-stack-dsql-sink",
    )


def test_expected_connector_names_custom_stack() -> None:
    assert cdc_expected_connector_names("my-mig") == (
        "my-mig-debezium-source",
        "my-mig-dsql-sink",
    )


# ---------------------------------------------------------------------------
# build_cdc_stack_params — filled values
# ---------------------------------------------------------------------------


def test_table_include_list_comma_joined() -> None:
    filled = dict(_params(("db.a", "db.b")).filled)
    assert filled["TableIncludeList"] == "db.a,db.b"


def test_sink_topics_prefixed_per_table() -> None:
    # The transformation the template delegates to the caller.
    filled = dict(_params(("db.t1", "db.t2", "db.t3")).filled)
    assert filled["SinkTopics"] == "dsqlcdc.db.t1,dsqlcdc.db.t2,dsqlcdc.db.t3"


def test_sink_topics_uses_custom_prefix() -> None:
    filled = dict(_params(("db.t",), topic_prefix="myprefix").filled)
    assert filled["SinkTopics"] == "myprefix.db.t"
    assert filled["TopicPrefix"] == "myprefix"


def test_column_exclude_list_empty_when_none() -> None:
    assert dict(_params(exclude=[]).filled)["ColumnExcludeList"] == ""


def test_column_exclude_list_comma_joined() -> None:
    filled = dict(_params(exclude=["db.t.notes", "db.t.blob"]).filled)
    assert filled["ColumnExcludeList"] == "db.t.notes,db.t.blob"


def test_dlq_topic_name_from_sink_config() -> None:
    assert dict(_params(dlq="custom-dlq").filled)["DlqTopicName"] == "custom-dlq"


def test_message_key_columns_empty_when_no_composite() -> None:
    # No composite re-key -> empty value (Debezium keys on the source PK).
    assert dict(_params().filled)["MessageKeyColumns"] == ""


def test_message_key_columns_rendered_for_composite_tables() -> None:
    source = DebeziumSourceConfig(
        name="mysql-source",
        table_include_list=["app.orders"],
        message_key_columns={"app.orders": ["customer_id", "id"]},
    )
    params = build_cdc_stack_params(
        source, _sink(["app.orders"]), target_endpoint="c.dsql.us-east-1.on.aws"
    )
    assert dict(params.filled)["MessageKeyColumns"] == r"app\.orders:customer_id,id"


def test_every_emitted_param_is_declared_in_the_template() -> None:
    # Guard: a CFN deploy fails ("Parameters [X] do not exist in the template") if
    # the tool emits a parameter the template does not declare. Cross-check every
    # key emitted by the stack/infra/watermark builders against cdc-stack.yaml so a
    # future param addition can't silently break the deploy. Parse the template
    # tolerantly (it uses Fn:: long form plus occasional short tags).
    import pathlib

    import pytest

    yaml = pytest.importorskip("yaml")

    class _L(yaml.SafeLoader):
        pass

    def _any(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    _L.add_multi_constructor("!", _any)
    template_path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "deploy" / "cdc-stack" / "cdc-stack.yaml"
    )
    doc = yaml.load(template_path.read_text(encoding="utf-8"), Loader=_L)
    declared = set(doc["Parameters"])

    source = DebeziumSourceConfig(
        name="s",
        table_include_list=["app.orders"],
        message_key_columns={"app.orders": ["customer_id", "id"]},
    )
    sink = _sink(["app.orders"])
    stack = build_cdc_stack_params(source, sink, target_endpoint="ep.on.aws")
    infra = build_cdc_infra_params(
        source, sink, vpc_id="vpc-1", plugin_bucket_arn="arn",
        debezium_plugin_s3_key="d", dsql_sink_plugin_s3_key="s",
        source_db_hostname="h", source_secret_arn="a", source_secret_name="n",
        dsql_cluster_arn="c", target_endpoint="ep", plugin_version="v12",
    )
    emitted = (
        {k for k, _ in stack.filled}
        | {k for k, _ in stack.placeholders}
        | {k for k, _ in infra.filled}
        | set(CDC_WATERMARK_PARAM_KEYS)
    )
    undeclared = emitted - declared
    assert not undeclared, (
        f"tool emits cdc-stack params the template does not declare: "
        f"{sorted(undeclared)}"
    )


def test_target_connection_filled() -> None:
    p = build_cdc_stack_params(
        _source(("db.t",)),
        _sink(("db.t",)),
        target_endpoint="ep.on.aws",
        target_database="postgres",
        target_username="admin",
    )
    filled = dict(p.filled)
    assert filled["DsqlClusterEndpoint"] == "ep.on.aws"
    assert filled["DsqlDatabaseName"] == "postgres"
    assert filled["DsqlConnectUser"] == "admin"


# ---------------------------------------------------------------------------
# build_cdc_stack_params — placeholders
# ---------------------------------------------------------------------------


def test_placeholders_all_marked() -> None:
    p = _params()
    assert p.placeholders, "expected customer-environment placeholders"
    for key, value in p.placeholders:
        assert value.startswith(CDC_PLACEHOLDER_PREFIX), key
        assert value.endswith(">"), key
        assert key in value  # the key is named in its own placeholder


def test_known_infra_params_are_placeholders_not_filled() -> None:
    p = _params()
    filled_keys = {k for k, _ in p.filled}
    ph_keys = {k for k, _ in p.placeholders}
    for key in ("VpcId", "MskBootstrapServers", "PluginBucketArn", "DsqlClusterArn"):
        assert key in ph_keys
        assert key not in filled_keys


def test_no_gtid_or_binlog_param() -> None:
    # Start offset is seeded via connect-offsets, not a CFN parameter.
    p = _params()
    all_keys = {k for k, _ in (*p.filled, *p.placeholders)}
    assert not any("gtid" in k.lower() or "binlog" in k.lower() for k in all_keys)


def test_stack_name_echoed() -> None:
    assert _params().stack_name == CDC_DEFAULT_STACK_NAME
    assert _params(stack_name="x").stack_name == "x"
    assert _params().topic_prefix == CDC_DEFAULT_TOPIC_PREFIX


def test_deploy_sink_default_true() -> None:
    assert dict(_params().filled)["DeploySink"] == "true"


def test_deploy_sink_false_when_requested() -> None:
    assert dict(_params(deploy_sink=False).filled)["DeploySink"] == "false"


# ---------------------------------------------------------------------------
# cdc_stack_params_to_json
# ---------------------------------------------------------------------------


def test_json_round_trip_shape() -> None:
    p = _params()
    parsed = json.loads(cdc_stack_params_to_json(p))
    keys = [item["ParameterKey"] for item in parsed]
    # Every key appears exactly once; filled + placeholders all present.
    assert len(keys) == len(set(keys))
    assert "SinkTopics" in keys
    assert "VpcId" in keys
    # The VpcId value is an unfilled placeholder in the JSON.
    by_key = {item["ParameterKey"]: item["ParameterValue"] for item in parsed}
    assert by_key["VpcId"].startswith(CDC_PLACEHOLDER_PREFIX)
    assert by_key["TableIncludeList"] == "app.orders,app.customers"


# ---------------------------------------------------------------------------
# build_cdc_infra_params — the full create_stack parameter set
# ---------------------------------------------------------------------------


def _infra(tables=("app.orders", "app.customers"), **kw):
    return build_cdc_infra_params(
        _source(tables, exclude=kw.pop("exclude", None)),
        _sink(tables, dlq=kw.pop("dlq", "dsql-sink-dlq")),
        vpc_id=kw.pop("vpc_id", "vpc-1"),
        connector_subnet_ids=kw.pop("connector_subnet_ids", "subnet-a,subnet-b"),
        plugin_bucket_arn=kw.pop("plugin_bucket_arn", "arn:aws:s3:::b"),
        debezium_plugin_s3_key=kw.pop("debezium_plugin_s3_key", "deb.zip"),
        dsql_sink_plugin_s3_key=kw.pop("dsql_sink_plugin_s3_key", "sink.jar"),
        source_db_hostname=kw.pop("source_db_hostname", "db.host"),
        source_secret_arn=kw.pop("source_secret_arn", "arn:sec"),
        source_secret_name=kw.pop("source_secret_name", "my/secret"),
        dsql_cluster_arn=kw.pop("dsql_cluster_arn", "arn:dsql"),
        target_endpoint=kw.pop("endpoint", "c.dsql.us-east-1.on.aws"),
        **kw,
    )


def test_infra_params_all_filled_no_placeholders() -> None:
    p = _infra()
    # CdcInfraParams has no placeholders field at all -- everything is filled.
    assert not hasattr(p, "placeholders")
    for key, value in p.filled:
        assert not str(value).startswith(CDC_PLACEHOLDER_PREFIX), key


def test_infra_params_pins_no_connectors() -> None:
    by_key = dict(_infra().filled)
    assert by_key["MskBootstrapServers"] == ""
    assert by_key["DeploySink"] == "false"


def test_infra_params_carries_byo_vpc_inputs() -> None:
    # Manual-override (provided subnets) mode.
    by_key = dict(_infra(connector_subnet_ids="subnet-a,subnet-b").filled)
    assert by_key["VpcId"] == "vpc-1"
    # Subnets are a comma-separated string (template's list type), never a list.
    assert by_key["ConnectorSubnetIds"] == "subnet-a,subnet-b"
    assert isinstance(by_key["ConnectorSubnetIds"], str)
    assert by_key["DsqlClusterArn"] == "arn:dsql"
    assert by_key["SourceDbHostname"] == "db.host"


def test_infra_params_manual_mode_leaves_owned_network_empty() -> None:
    by_key = dict(_infra(connector_subnet_ids="subnet-a,subnet-b").filled)
    # When subnets are provided, the tool-owned NAT params stay empty so the
    # template's CreateOwnedNetwork condition is false (no new networking).
    for k in ("NatPublicSubnetId", "PrivateSubnetCidrA", "PrivateSubnetCidrB",
              "PrivateSubnetAzA", "PrivateSubnetAzB"):
        assert by_key[k] == "", k


def test_infra_params_owned_network_mode_carries_nat_inputs() -> None:
    by_key = dict(
        _infra(
            connector_subnet_ids="",
            nat_public_subnet_id="subnet-pub",
            private_subnet_cidr_a="10.0.2.0/24",
            private_subnet_cidr_b="10.0.3.0/24",
            private_subnet_az_a="us-east-1a",
            private_subnet_az_b="us-east-1b",
        ).filled
    )
    assert by_key["ConnectorSubnetIds"] == ""
    assert by_key["NatPublicSubnetId"] == "subnet-pub"
    assert by_key["PrivateSubnetCidrA"] == "10.0.2.0/24"
    assert by_key["PrivateSubnetCidrB"] == "10.0.3.0/24"
    assert by_key["PrivateSubnetAzA"] == "us-east-1a"
    assert by_key["PrivateSubnetAzB"] == "us-east-1b"


def test_infra_params_derives_table_and_topics() -> None:
    by_key = dict(_infra().filled)
    assert by_key["TableIncludeList"] == "app.orders,app.customers"
    assert by_key["SinkTopics"] == (
        f"{CDC_DEFAULT_TOPIC_PREFIX}.app.orders,{CDC_DEFAULT_TOPIC_PREFIX}.app.customers"
    )


def test_infra_params_optional_security_group_default_empty() -> None:
    by_key = dict(_infra().filled)
    assert by_key["SourceDbSecurityGroupId"] == ""


def test_infra_params_unique_keys() -> None:
    keys = [k for k, _ in _infra().filled]
    assert len(keys) == len(set(keys))


def test_infra_params_lambda_seeder_key_default_empty() -> None:
    # Left empty by default; the deploy job patches it from the upload result, so
    # the build-time value is empty (the seeder still works -- it is filled before
    # create_stack runs).
    by_key = dict(_infra().filled)
    assert by_key["LambdaSeederS3Key"] == ""


def test_infra_params_lambda_seeder_key_passes_through() -> None:
    by_key = dict(_infra(lambda_seeder_s3_key="cdc-plugins/offset-seeder-lambda.zip").filled)
    assert by_key["LambdaSeederS3Key"] == "cdc-plugins/offset-seeder-lambda.zip"


# ---------------------------------------------------------------------------
# build_watermark_params — the offset-seeder Watermark* parameters
# ---------------------------------------------------------------------------


def _wm(**kw):
    base = dict(
        binlog_file="mysql-bin.000042",
        binlog_position=15324,
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-9",
        snapshot_timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )
    base.update(kw)
    return Watermark(**base)


def test_watermark_params_keys_in_template_order() -> None:
    assert [k for k, _ in build_watermark_params(_wm())] == list(CDC_WATERMARK_PARAM_KEYS)


def test_watermark_params_filled_from_coordinates() -> None:
    by_key = dict(build_watermark_params(_wm()))
    assert by_key["WatermarkBinlogFile"] == "mysql-bin.000042"
    assert by_key["WatermarkBinlogPos"] == "15324"
    assert by_key["WatermarkGtids"] == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-9"
    # ts_sec is the unix seconds of the snapshot timestamp, as a string.
    assert by_key["WatermarkTsSec"] == str(int(_wm().snapshot_timestamp.timestamp()))
    # Every value is a string (CFN parameter type).
    assert all(isinstance(v, str) for v in by_key.values())


def test_watermark_params_gtidless_source_empty_gtids() -> None:
    by_key = dict(build_watermark_params(_wm(gtid_executed=None)))
    assert by_key["WatermarkGtids"] == ""
    # file/pos still present so SeedOffset (file-based) is still satisfiable.
    assert by_key["WatermarkBinlogFile"] == "mysql-bin.000042"


def test_watermark_params_none_watermark_all_empty() -> None:
    by_key = dict(build_watermark_params(None))
    assert set(by_key) == set(CDC_WATERMARK_PARAM_KEYS)
    assert all(v == "" for v in by_key.values())


def test_watermark_params_no_binlog_coords_all_empty() -> None:
    # A watermark with no binlog file:position (binlog/GTID disabled on the source)
    # blanks every param so the template's SeedOffset condition stays false.
    by_key = dict(build_watermark_params(_wm(binlog_file=None, binlog_position=None)))
    assert all(v == "" for v in by_key.values())


# ---------------------------------------------------------------------------
# cdc_stack_name_is_valid — the mysql-dsql-cdc-* family gate (multi-DB support)
# ---------------------------------------------------------------------------


def test_default_stack_name_is_valid_and_in_family() -> None:
    assert CDC_DEFAULT_STACK_NAME.startswith(CDC_STACK_NAME_PREFIX)
    assert cdc_stack_name_is_valid(CDC_DEFAULT_STACK_NAME)


def test_per_db_stack_names_are_valid() -> None:
    # The whole point: several cdc-stacks (one per source DB) in one account.
    for name in ("mysql-dsql-cdc-orders", "mysql-dsql-cdc-billing-prod", "mysql-dsql-cdc-a1"):
        assert cdc_stack_name_is_valid(name), name


def test_stack_name_must_carry_the_family_prefix() -> None:
    for name in ("my-cdc-stack", "cdc-orders", "orders", "dsqlcdc-orders"):
        assert not cdc_stack_name_is_valid(name), name


def test_stack_name_prefix_alone_is_rejected() -> None:
    # Needs a distinguishing suffix after the prefix.
    assert not cdc_stack_name_is_valid(CDC_STACK_NAME_PREFIX)


def test_stack_name_rejects_cfn_charset_violations() -> None:
    for name in ("mysql-dsql-cdc-bad_name", "mysql-dsql-cdc-has space", "mysql-dsql-cdc-dot.name"):
        assert not cdc_stack_name_is_valid(name), name


def test_stack_name_must_start_with_letter() -> None:
    # CloudFormation stack names must start with a letter; the prefix does, so any
    # accepted name does too. A leading digit can only appear by dropping the prefix.
    assert not cdc_stack_name_is_valid("1mysql-dsql-cdc-x")


def test_stack_name_length_bound() -> None:
    longest = CDC_STACK_NAME_PREFIX + "a" * (CDC_STACK_NAME_MAX_LEN - len(CDC_STACK_NAME_PREFIX))
    assert len(longest) == CDC_STACK_NAME_MAX_LEN
    assert cdc_stack_name_is_valid(longest)
    assert not cdc_stack_name_is_valid(longest + "a")  # one over


def test_stack_name_empty_or_blank_rejected() -> None:
    assert not cdc_stack_name_is_valid("")


# build_cdc_stack_name / cdc_stack_name_suffix — the suffix-only UI field. The user
# edits only the part after the fixed prefix, so a bare word can never escape the
# mysql-dsql-cdc-* family (the old post-hoc reject-and-revert UX is gone).


def test_build_cdc_stack_name_prepends_prefix_and_validates() -> None:
    from dsql_migrator.core.cdc import build_cdc_stack_name

    # A bare suffix (the reported "abcde" case) becomes a VALID full name.
    assert build_cdc_stack_name("abcde") == "mysql-dsql-cdc-abcde"
    assert build_cdc_stack_name("orders") == "mysql-dsql-cdc-orders"
    assert build_cdc_stack_name(" orders ") == "mysql-dsql-cdc-orders"  # trimmed
    # Empty / illegal-charset suffix -> None (caller keeps the current name).
    assert build_cdc_stack_name("") is None
    assert build_cdc_stack_name("a b") is None  # space is not allowed
    assert build_cdc_stack_name("a/b") is None


def test_cdc_stack_name_suffix_round_trips_with_build() -> None:
    from dsql_migrator.core.cdc import (
        build_cdc_stack_name,
        cdc_stack_name_suffix,
    )

    assert cdc_stack_name_suffix(CDC_DEFAULT_STACK_NAME) == "stack"
    assert cdc_stack_name_suffix("mysql-dsql-cdc-orders") == "orders"
    assert cdc_stack_name_suffix(None) == "stack"  # default
    # suffix(build(x)) == x for a valid suffix (what the field relies on).
    assert cdc_stack_name_suffix(build_cdc_stack_name("orders")) == "orders"


def test_custom_stack_name_flows_into_params_and_connector_names() -> None:
    # A per-DB stack name must propagate to the deployable params + the connector
    # names the monitor scopes to (so two stacks never see each other's connectors).
    p = build_cdc_stack_params(
        _source(("db.t",)), _sink(("db.t",)),
        target_endpoint="ep.on.aws", stack_name="mysql-dsql-cdc-orders",
    )
    assert p.stack_name == "mysql-dsql-cdc-orders"
    assert cdc_expected_connector_names("mysql-dsql-cdc-orders") == (
        "mysql-dsql-cdc-orders-debezium-source",
        "mysql-dsql-cdc-orders-dsql-sink",
    )


# ---------------------------------------------------------------------------
# estimate_cdc_hourly_cost — ballpark figure for the deploy dialog
# ---------------------------------------------------------------------------


def test_cost_estimate_includes_nat_costs_more() -> None:
    with_nat = estimate_cdc_hourly_cost(includes_nat=True)
    no_nat = estimate_cdc_hourly_cost(includes_nat=False)
    assert isinstance(with_nat, CdcCostEstimate)
    # A NAT gateway adds to the bill.
    assert with_nat.hourly_low_usd > no_nat.hourly_low_usd
    assert with_nat.includes_nat is True
    assert no_nat.includes_nat is False


def test_cost_estimate_range_is_ordered_and_positive() -> None:
    est = estimate_cdc_hourly_cost(includes_nat=True)
    assert 0 < est.hourly_low_usd < est.hourly_high_usd
    # Order-of-magnitude sanity: MSK Serverless + Connect + NAT base is ~$1/hr.
    assert 0.5 < est.hourly_low_usd < 5


def test_cost_estimate_caveat_is_clear_about_nature_and_billing() -> None:
    est = estimate_cdc_hourly_cost()
    assert "estimate" in est.caveat.lower()
    assert "varies" in est.caveat.lower()
    # Reminds the operator it bills until teardown.
    assert "delete" in est.caveat.lower()
    # Glue is not used by this pipeline -> must not be listed as a cost driver.
    assert "glue" not in est.caveat.lower()


# ---------------------------------------------------------------------------
# compute_cdc_scaling_defaults — CDC connector-scaling smart defaults
# ---------------------------------------------------------------------------


def test_scaling_few_tables_raises_partitions_to_reach_cap() -> None:
    # 3 tables -> ceil(8/3)=3 partitions each, so total parallelism hits the cap.
    d = compute_cdc_scaling_defaults(3, env={})
    assert d.partitions_per_topic == 3
    assert d.sink_tasks_max == CDC_MAX_SINK_PARALLELISM
    assert d.mcu_count == CDC_DEFAULT_MCU_COUNT


def test_scaling_single_table_uses_full_cap_on_one_topic() -> None:
    d = compute_cdc_scaling_defaults(1, env={})
    assert d.partitions_per_topic == CDC_MAX_SINK_PARALLELISM
    assert d.sink_tasks_max == CDC_MAX_SINK_PARALLELISM


def test_scaling_many_tables_use_one_partition_each() -> None:
    # tables >= cap: the tables themselves provide the parallelism.
    for n in (8, 10, 50, 100):
        d = compute_cdc_scaling_defaults(n, env={})
        assert d.partitions_per_topic == 1, n
        assert d.sink_tasks_max == CDC_MAX_SINK_PARALLELISM, n


def test_scaling_zero_or_negative_tables_treated_as_one() -> None:
    d = compute_cdc_scaling_defaults(0, env={})
    assert d.num_tables == 1
    assert d.partitions_per_topic == CDC_MAX_SINK_PARALLELISM


def test_scaling_env_overrides_take_precedence() -> None:
    d = compute_cdc_scaling_defaults(
        3,
        env={
            CDC_ENV_TOPIC_PARTITIONS: "2",
            CDC_ENV_SINK_TASKS_MAX: "6",
            CDC_ENV_MCU_COUNT: "4",
        },
    )
    assert d.partitions_per_topic == 2
    assert d.sink_tasks_max == 6
    assert d.mcu_count == 4


def test_scaling_invalid_mcu_override_falls_back_to_default() -> None:
    # 3 is not in the template's AllowedValues (1/2/4/8) -> ignored.
    d = compute_cdc_scaling_defaults(2, env={CDC_ENV_MCU_COUNT: "3"})
    assert d.mcu_count == CDC_DEFAULT_MCU_COUNT


def test_scaling_blank_and_nonint_env_ignored() -> None:
    d = compute_cdc_scaling_defaults(
        4, env={CDC_ENV_SINK_TASKS_MAX: "", CDC_ENV_TOPIC_PARTITIONS: "abc"}
    )
    # Falls back to smart default: 4 tables -> 2 partitions each, cap tasks.
    assert d.partitions_per_topic == 2
    assert d.sink_tasks_max == CDC_MAX_SINK_PARALLELISM


def test_infra_params_emit_inferred_scaling_knobs() -> None:
    # 2 tables -> ceil(8/2)=4 partitions each; tasks at the cap; default MCU.
    by_key = dict(_infra(tables=("app.orders", "app.customers")).filled)
    assert by_key["TopicDefaultPartitions"] == "4"
    assert by_key["SinkTasksMax"] == str(CDC_MAX_SINK_PARALLELISM)
    assert by_key["ConnectorMcuCount"] == str(CDC_DEFAULT_MCU_COUNT)


def test_infra_params_scaling_knobs_are_declared_in_template() -> None:
    # Guard: the inferred knobs must be real template parameters (else CFn rejects).
    import pathlib

    template = pathlib.Path("deploy/cdc-stack/cdc-stack.yaml").read_text()
    for key in (
        "TopicDefaultPartitions",
        "SinkTasksMax",
        "ConnectorMcuCount",
        "SinkMcuCount",
    ):
        assert f"\n  {key}:" in template, key


# ---------------------------------------------------------------------------
# SinkMcuCount — the operator-tunable sink compute knob
# ---------------------------------------------------------------------------


def test_start_cdc_params_carry_sink_mcu_so_the_knob_can_reach_the_connector() -> None:
    """The Start CDC pass must SEND SinkMcuCount, not just the infra create.

    This is the whole reason the knob works. ``DsqlSinkConnector`` is gated on
    ``DeploySink=true``, so the sink connector is created by Start CDC -- and
    ``submit_update`` forwards every parameter the tool does NOT override as
    ``UsePreviousValue=True``. Omitting it here would silently pin the sink to the
    template default forever, no matter what the operator configured.
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT

    by_key = dict(_params().filled)
    assert by_key["SinkMcuCount"] == str(CDC_DEFAULT_SINK_MCU_COUNT)
    # An operator-raised value reaches the parameter set unchanged.
    assert dict(_params(sink_mcu_count=8).filled)["SinkMcuCount"] == "8"


def test_infra_params_carry_sink_mcu_separately_from_the_source_mcu() -> None:
    """Sink and source compute are INDEPENDENT parameters.

    The sink is the CPU-bound half; the single-task Debezium source has spare CPU. A
    single shared MCU value would force the operator to pay for source compute they
    do not need in order to give the sink what it does.
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT

    by_key = dict(_infra(tables=("app.orders", "app.customers")).filled)
    assert by_key["SinkMcuCount"] == str(CDC_DEFAULT_SINK_MCU_COUNT)
    assert by_key["ConnectorMcuCount"] == str(CDC_DEFAULT_MCU_COUNT)
    # They are genuinely different knobs, not one value written twice.
    assert by_key["SinkMcuCount"] != by_key["ConnectorMcuCount"]


def test_sink_mcu_default_equals_the_template_default() -> None:
    """A mismatch here would bounce both RUNNING connectors on the next Start CDC.

    For a cdc-stack deployed BEFORE the tool passed this parameter, CloudFormation
    reports the template's default. ``run_cdc_start`` compares the desired connector
    overrides against the deployed parameters to decide whether anything changed, so
    a tool default that differed from the template default would read as a real
    config change on every Start CDC and needlessly recreate the connectors --
    burning MSK partition quota that is never reclaimed.
    """
    import pathlib
    import re

    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT

    template = pathlib.Path("deploy/cdc-stack/cdc-stack.yaml").read_text()
    block = template.split("\n  SinkMcuCount:", 1)[1]
    default = re.search(r"\n\s+Default:\s*(\d+)", block)
    assert default is not None, "SinkMcuCount has no Default in the template"
    assert int(default.group(1)) == CDC_DEFAULT_SINK_MCU_COUNT
    # And the tool only ever sends a value the template will accept.
    allowed = re.search(r"\n\s+AllowedValues:\s*\[([^\]]*)\]", block)
    assert allowed is not None
    legal = {int(v.strip()) for v in allowed.group(1).split(",")}
    assert CDC_DEFAULT_SINK_MCU_COUNT in legal
    # The config knob's enum must match the template's AllowedValues exactly.
    from dsql_migrator.config import TUNABLE_KNOBS

    knob = {k.field: k for k in TUNABLE_KNOBS}["cdc_sink_mcu_count"]
    assert set(knob.allowed) == legal


# ---------------------------------------------------------------------------
# compute_cdc_partition_plan — size-proportional partitions (skewed workload)
# ---------------------------------------------------------------------------


def _row_counts(hot, cold, *, hot_rows=750_000, cold_rows=10_000):
    """{topic: rows} with ``hot`` hot tables and ``cold`` small ones."""
    counts = {f"pfx.app.hot{i}": hot_rows for i in range(hot)}
    counts.update({f"pfx.app.cold{i}": cold_rows for i in range(cold)})
    return counts


def test_partition_plan_elevates_hot_tables_in_skewed_many_table_capture() -> None:
    # 4 hot (~750k) + 5 cold (~10k), 9 tables total (>= cap so uniform would give 1
    # each). The hot tables are ~2.2x the average -> tier 2; cold stay at 1.
    plan = compute_cdc_partition_plan(_row_counts(4, 5), env={})
    assert plan is not None
    assert all(plan.partitions_by_topic[f"pfx.app.hot{i}"] == 2 for i in range(4))
    assert all(plan.partitions_by_topic[f"pfx.app.cold{i}"] == 1 for i in range(5))
    assert plan.default_partitions == 1
    # One elevated group (p2) listing exactly the four hot topics.
    assert [g.name for g in plan.groups] == ["p2"]
    assert plan.groups[0].partitions == 2
    assert set(plan.groups[0].topics) == {f"pfx.app.hot{i}" for i in range(4)}
    assert plan.total_partitions == 4 * 2 + 5  # 13
    assert plan.sink_tasks_max == CDC_MAX_SINK_PARALLELISM  # min(13, cap)


def test_partition_plan_gives_a_dominant_table_the_top_tier() -> None:
    # One table dwarfs the other eight (>= 4x average) -> tier 4.
    counts = {"pfx.app.big": 5_000_000}
    counts.update({f"pfx.app.s{i}": 10_000 for i in range(8)})
    plan = compute_cdc_partition_plan(counts, env={})
    assert plan is not None
    assert plan.partitions_by_topic["pfx.app.big"] == 4
    assert all(plan.partitions_by_topic[f"pfx.app.s{i}"] == 1 for i in range(8))
    assert [g.name for g in plan.groups] == ["p4"]
    assert plan.groups[0].topics == ("pfx.app.big",)


def test_partition_plan_none_for_uniform_load() -> None:
    # Every table the same size -> nothing exceeds fair share -> uniform is right.
    counts = {f"pfx.app.t{i}": 100_000 for i in range(9)}
    assert compute_cdc_partition_plan(counts, env={}) is None


def test_partition_plan_none_below_cap_table_count() -> None:
    # Few tables: the uniform default already parallelises, even with skew.
    counts = {"pfx.app.big": 5_000_000, "pfx.app.a": 1, "pfx.app.b": 1}
    assert compute_cdc_partition_plan(counts, env={}) is None


def test_partition_plan_none_without_size_signal() -> None:
    assert compute_cdc_partition_plan({}, env={}) is None
    all_zero = {f"pfx.app.t{i}": 0 for i in range(9)}
    assert compute_cdc_partition_plan(all_zero, env={}) is None


def test_partition_plan_none_when_partition_override_set() -> None:
    # An explicit uniform partition override wins over the proportional plan.
    plan = compute_cdc_partition_plan(
        _row_counts(4, 5), env={CDC_ENV_TOPIC_PARTITIONS: "4"}
    )
    assert plan is None


def test_partition_plan_sink_tasks_env_override_respected() -> None:
    plan = compute_cdc_partition_plan(
        _row_counts(4, 5), env={CDC_ENV_SINK_TASKS_MAX: "3"}
    )
    assert plan is not None
    assert plan.sink_tasks_max == 3


# ---------------------------------------------------------------------------
# cdc_scaling_params — the CFN param tuples (uniform vs size-proportional)
# ---------------------------------------------------------------------------


def test_scaling_params_uniform_when_no_row_counts() -> None:
    # No size signal -> uniform default, empty topic.creation groups.
    params = dict(cdc_scaling_params(["db.a", "db.b"], "pfx", env={}))
    assert params["TopicDefaultPartitions"] == "4"  # 2 tables -> ceil(8/2)
    assert params["SinkTasksMax"] == str(CDC_MAX_SINK_PARALLELISM)
    assert params["TopicCreationGroups"] == ""
    assert params["TopicGroupInclude2"] == ""
    assert params["TopicGroupInclude4"] == ""
    # Uniform plan -> no per-topic map; the seeder uses the flat TopicDefaultPartitions.
    assert params["SinkTopicPartitions"] == ""


def test_scaling_params_size_proportional_groups_hot_tables() -> None:
    # 9 tables, 4 hot -> the "p2" group lists the four hot topics (regex-escaped,
    # prefixed), default drops to 1, and no p4 group.
    tables = [f"hot{i}" for i in range(4)] + [f"cold{i}" for i in range(5)]
    row_counts = {f"hot{i}": 750_000 for i in range(4)}
    row_counts.update({f"cold{i}": 10_000 for i in range(5)})
    params = dict(cdc_scaling_params(tables, "pfx", row_counts_by_table=row_counts))
    assert params["TopicDefaultPartitions"] == "1"
    assert params["TopicCreationGroups"] == "p2"
    assert params["TopicGroupInclude4"] == ""
    # Regex-escaped, prefixed topic names, comma-joined.
    inc2 = params["TopicGroupInclude2"]
    assert set(inc2.split(",")) == {rf"pfx\.hot{i}" for i in range(4)}
    # The seeder pre-creates the topics, so it needs the EXPLICIT per-topic partition
    # map (Debezium topic.creation groups only apply to topics Debezium creates).
    # Hot topics = 2 partitions (the p2 tier), cold = 1 (the default).
    part_map = dict(
        pair.rsplit(":", 1) for pair in params["SinkTopicPartitions"].split(",")
    )
    assert part_map == {
        **{f"pfx.hot{i}": "2" for i in range(4)},
        **{f"pfx.cold{i}": "1" for i in range(5)},
    }


def test_scaling_params_ignores_row_counts_for_untracked_tables() -> None:
    # Counts for tables not in the capture list must not create phantom groups.
    tables = [f"t{i}" for i in range(9)]
    row_counts = {"not_captured": 9_000_000}
    params = dict(cdc_scaling_params(tables, "pfx", row_counts_by_table=row_counts))
    # No captured table has a size signal -> uniform (1 each for 9 tables), no groups.
    assert params["TopicCreationGroups"] == ""


def test_infra_params_size_proportional_partitions_from_row_counts() -> None:
    # End-to-end through build_cdc_infra_params: a skewed capture yields the p2
    # group + default 1, all declared params.
    tables = tuple([f"app.hot{i}" for i in range(4)] + [f"app.cold{i}" for i in range(5)])
    row_counts = {f"app.hot{i}": 750_000 for i in range(4)}
    row_counts.update({f"app.cold{i}": 10_000 for i in range(5)})
    by_key = dict(_infra(tables=tables, row_counts_by_table=row_counts).filled)
    assert by_key["TopicDefaultPartitions"] == "1"
    assert by_key["TopicCreationGroups"] == "p2"
    assert by_key["TopicGroupInclude2"]  # non-empty
    # 13 total partitions capped at the sink-parallelism ceiling.
    assert by_key["SinkTasksMax"] == str(CDC_MAX_SINK_PARALLELISM)


def test_partition_tier_params_and_template_group_blocks_agree() -> None:
    # Guard the Python<->template coupling: each elevated tier N in
    # CDC_PARTITION_TIERS must have a matching topic.creation.pN group in the
    # template whose partition literal is N, and the group-include params the tool
    # emits must be declared. A tier change that misses the template would silently
    # drop the group (Debezium would use the default partition count).
    import pathlib

    from dsql_migrator.core.cdc import CDC_PARTITION_TIERS

    template = pathlib.Path("deploy/cdc-stack/cdc-stack.yaml").read_text()
    for tier in CDC_PARTITION_TIERS:
        assert f"topic.creation.p{tier}.include" in template, tier
        assert f'[HasTopicGroup{tier}, "{tier}", Ref: AWS::NoValue]' in template, tier
        assert f"\n  TopicGroupInclude{tier}:" in template, tier
    for key in ("TopicCreationGroups",):
        assert f"\n  {key}:" in template, key



# EC2 accepts only these characters in a security-group RULE description (this is
# narrower than the free-form text allowed elsewhere in the template -- notably it
# excludes the apostrophe), max 255 chars.
_SG_RULE_DESCRIPTION_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    " ._-:/()#,@[]+=&;{}!$*"
)


def _load_cdc_template():
    """Parse cdc-stack.yaml tolerantly (Fn:: long form plus occasional short tags)."""
    import pathlib

    import pytest

    yaml = pytest.importorskip("yaml")

    class _L(yaml.SafeLoader):
        pass

    def _any(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    _L.add_multi_constructor("!", _any)
    path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "deploy" / "cdc-stack" / "cdc-stack.yaml"
    )
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_L)


def _security_group_rule_descriptions() -> list[tuple[str, str]]:
    """Every SG rule Description in the template, as ``(resource_name, text)``.

    Covers both shapes: inline ``SecurityGroupIngress``/``SecurityGroupEgress`` lists on
    an ``AWS::EC2::SecurityGroup``, and standalone
    ``AWS::EC2::SecurityGroup{Ingress,Egress}`` resources.
    """
    doc = _load_cdc_template()
    found: list[tuple[str, str]] = []
    for name, resource in doc["Resources"].items():
        kind = resource.get("Type")
        props = resource.get("Properties") or {}
        if kind == "AWS::EC2::SecurityGroup":
            for key in ("SecurityGroupIngress", "SecurityGroupEgress"):
                for rule in props.get(key) or []:
                    if isinstance(rule, dict) and isinstance(
                        rule.get("Description"), str
                    ):
                        found.append((f"{name}.{key}", rule["Description"]))
        elif kind in (
            "AWS::EC2::SecurityGroupIngress",
            "AWS::EC2::SecurityGroupEgress",
        ):
            if isinstance(props.get("Description"), str):
                found.append((name, props["Description"]))
    return found


def test_security_group_rule_descriptions_use_only_characters_ec2_accepts() -> None:
    """A stray apostrophe in a rule description fails the whole CDC deploy.

    Observed on mysql-dsql-cdc-stack-0729: "customer's own NAT" in the inline 443 egress
    description made ConnectorSecurityGroup CREATE_FAILED with *"Invalid rule
    description"*, which rolled the stack back -- and the rollback itself then hit
    ROLLBACK_FAILED, because the two CustomPlugins were still CREATING and MSK Connect
    refuses to delete a plugin in that state. So one bad character costs a manual
    cleanup, not just a retry. EC2's accepted set here is narrower than the free-form
    text allowed in Parameter/resource descriptions elsewhere in this template.
    """
    descriptions = _security_group_rule_descriptions()
    # Guard the guard: if the scan finds nothing, the test would pass vacuously.
    assert len(descriptions) >= 5, descriptions

    offenders = {
        name: sorted(set(text) - _SG_RULE_DESCRIPTION_CHARS)
        for name, text in descriptions
        if set(text) - _SG_RULE_DESCRIPTION_CHARS
    }
    assert not offenders, f"disallowed characters in SG rule descriptions: {offenders}"


def test_security_group_rule_descriptions_are_within_the_length_limit() -> None:
    # EC2 caps a rule description at 255 characters; a long wrapped YAML block scalar
    # can cross that without looking long in the source.
    too_long = {
        name: len(text)
        for name, text in _security_group_rule_descriptions()
        if len(text) > 255
    }
    assert not too_long, f"SG rule descriptions over 255 chars: {too_long}"


def test_no_iam_role_makes_the_msk_cluster_wait_on_teardown() -> None:
    """No IAM role may reach the cluster ARN with ``Fn::GetAtt: [MskCluster, Arn]``.

    CloudFormation treats that as a dependency and deletes in reverse, so MskCluster
    waits for every role that names it. The in-VPC OffsetSeederFunction heads one such
    chain (OffsetSeederFunction -> OffsetSeederRole -> MskCluster) and its
    Lambda-managed ENIs take ~15-20 min to reclaim: a measured teardown sat 18m30s on
    the seeder before the cluster's own delete (93s) even started, and the UI showed
    "Deleting infrastructure" the whole time. Roles must build the ARN by NAME
    (Fn::Sub + a wildcard for the UUID suffix) instead -- same authorization, but the
    cluster and the seeder tear down in parallel.

    Outputs are exempt: they are not evaluated at delete time.
    """
    import json

    doc = _load_cdc_template()
    offenders = []
    for name, resource in doc["Resources"].items():
        if resource.get("Type") != "AWS::IAM::Role":
            continue
        blob = json.dumps(resource)
        if '"MskCluster"' in blob:
            offenders.append(name)
    assert not offenders, (
        "these IAM roles reference MskCluster, forcing the cluster to wait for them "
        f"on delete (build the ARN with Fn::Sub instead): {offenders}"
    )


def test_roles_still_authorize_cluster_level_msk_actions() -> None:
    """Dropping the Fn::GetAtt must not drop the grant it carried.

    The control for the test above: cluster-level actions (Connect/DescribeCluster,
    and the seeder's WriteDataIdempotently) still need a cluster-scoped Resource, so
    each role must name the cluster ARN by pattern. A "fix" that simply deleted the
    statement would break every connector start.
    """
    import json

    doc = _load_cdc_template()
    for role in ("ConnectorExecutionRole", "OffsetSeederRole"):
        blob = json.dumps(doc["Resources"][role])
        assert "kafka-cluster:Connect" in blob, f"{role} lost its MSK Connect grant"
        assert ":cluster/${AWS::StackName}-msk/*" in blob, (
            f"{role} must scope cluster-level actions to this stack's cluster ARN "
            "(built by name, with a wildcard for the UUID suffix)"
        )


def test_connectors_still_wait_for_the_cluster_at_create_time() -> None:
    """Creation order must be unaffected -- only the DELETE order changes.

    The connectors cannot be created before the cluster exists, and that ordering was
    NOT provided by the IAM Fn::GetAtt (it comes from an explicit DependsOn). Pinned
    so a later cleanup of "redundant" DependsOn entries cannot silently reintroduce
    the race the removed reference never guarded against.
    """
    doc = _load_cdc_template()
    for connector in ("DebeziumSourceConnector", "DsqlSinkConnector"):
        depends = doc["Resources"][connector].get("DependsOn") or []
        assert "MskCluster" in depends, (
            f"{connector} must DependsOn MskCluster so it is created after the cluster"
        )
