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
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
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
    cdc_stack_params_to_json,
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
