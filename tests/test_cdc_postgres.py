# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the PostgreSQL CDC control plane (core/cdc_postgres.py).

Covers the PostgreSQL source-config + cdc-stack parameter builders, mirroring the
MySQL tests in test_cdc_pipeline.py / test_cdc_stack_params.py:
- CdcResumePoint.wal_lsn (the PG resume coordinate) round-trips from the watermark.
- snapshot.mode selection: never (gapless, wal_lsn present) vs initial (stand-alone).
- The PG param set is EngineType=postgres + pgoutput slot/publication params, drops
  the MySQL-only source params, and inherits the engine-neutral base unchanged.
- Every emitted PG parameter is declared in cdc-stack.yaml (the keystone guard).
- The MySQL builders stay byte-identical (no PG leakage).
- _patch_plugin_params fills the source plugin key matching the set's engine only.
Pure: no AWS, no I/O.
"""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import (
    CdcResumePoint,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
)
from dsql_migrator.core.cdc_deployer import _patch_plugin_params
from dsql_migrator.core.cdc_postgres import (
    PostgresSourceConfig,
    build_pg_cdc_infra_params,
    build_pg_cdc_stack_params,
    build_pg_source_config,
)
from dsql_migrator.core.models import TableDef, Watermark


def _tables() -> list[TableDef]:
    return [
        TableDef(name="app.orders", primary_key=["id"]),
        TableDef(name="app.customers", primary_key=["id"]),
    ]


def _sink() -> SinkConnectorConfig:
    return SinkConnectorConfig(
        name="pg-sink", topics=["app.orders", "app.customers"], dlq_topic="dsql-sink-dlq"
    )


def _wm(*, wal_lsn: str | None = None) -> Watermark:
    return Watermark(snapshot_timestamp=datetime.now(timezone.utc), wal_lsn=wal_lsn)


def _neutral_infra_kwargs() -> dict:
    return dict(
        vpc_id="vpc-1",
        plugin_bucket_arn="arn:aws:s3:::mysql-dsql-migrator-plugins-1-us-east-1",
        dsql_sink_plugin_s3_key="cdc-plugins/dsql-sink-connector.zip",
        source_db_hostname="pg.example.com",
        source_secret_arn="arn:aws:secretsmanager:us-east-1:1:secret:s",
        source_secret_name="s",
        dsql_cluster_arn="arn:aws:dsql:us-east-1:1:cluster/c",
        target_endpoint="ep.dsql.example.com",
        plugin_version="v35",
    )


# ---------------------------------------------------------------------------
# CdcResumePoint.wal_lsn
# ---------------------------------------------------------------------------


def test_resume_point_carries_wal_lsn_from_watermark() -> None:
    resume = CdcResumePoint.from_watermark(_wm(wal_lsn="3/AF012B8"))
    assert resume.wal_lsn == "3/AF012B8"
    assert resume.can_resume_from_lsn() is True
    # A watermark with no LSN (e.g. a MySQL one) cannot resume via LSN.
    assert CdcResumePoint.from_watermark(_wm()).can_resume_from_lsn() is False


# ---------------------------------------------------------------------------
# build_pg_source_config -- snapshot.mode selection
# ---------------------------------------------------------------------------


def test_pg_source_config_gapless_uses_snapshot_never() -> None:
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="dsqlmig_s1", publication_name="dsqlmig_pub1",
    )
    assert src.snapshot_mode == "never"  # slot holds the start LSN -> no snapshot
    assert src.table_include_list == ["app.orders", "app.customers"]
    assert src.database_name == "app"
    assert src.slot_name == "dsqlmig_s1"
    assert src.publication_name == "dsqlmig_pub1"
    assert src.publication_autocreate_mode == "disabled"


def test_pg_source_config_standalone_uses_snapshot_initial() -> None:
    # No WAL LSN to resume from -> stand-alone CDC snapshots the tables first.
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(),
        database_name="app", slot_name="s1", publication_name="p1",
    )
    assert src.snapshot_mode == "initial"


def test_pg_source_config_manual_override_snapshots_initial() -> None:
    # A manual resume override (no watermark LSN path) also snapshots first.
    override = CdcResumePoint(wal_lsn="3/AF012B8")
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="9/12"),
        database_name="app", slot_name="s1", publication_name="p1",
        resume_override=override,
    )
    assert src.snapshot_mode == "initial"


# ---------------------------------------------------------------------------
# build_pg_cdc_infra_params
# ---------------------------------------------------------------------------


def _pg_infra():
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="dsqlmig_s1", publication_name="dsqlmig_pub1",
    )
    return build_pg_cdc_infra_params(src, _sink(), **_neutral_infra_kwargs())


def test_pg_infra_params_are_postgres_shaped() -> None:
    d = dict(_pg_infra().filled)
    assert d["EngineType"] == "postgres"
    assert d["SourceDbPort"] == "5432"  # PG default, not 3306
    assert d["PgDatabaseName"] == "app"
    assert d["PgSlotName"] == "dsqlmig_s1"
    assert d["PgPublicationName"] == "dsqlmig_pub1"
    assert d["PgPublicationAutocreateMode"] == "disabled"
    assert d["PgSnapshotMode"] == "never"
    assert "DebeziumPostgresPluginS3Key" in d


def test_pg_infra_params_drop_mysql_only_source_params() -> None:
    d = dict(_pg_infra().filled)
    for bad in ("DebeziumPluginS3Key", "SourceDbServerId", "SnapshotMode"):
        assert bad not in d, ("MySQL-only param leaked into PG infra set", bad)


def test_pg_infra_params_inherit_the_neutral_base() -> None:
    # The engine-neutral MSK/VPC/DSQL/topic/scaling params come from the shared
    # MySQL builder unchanged.
    d = dict(_pg_infra().filled)
    for k in (
        "VpcId", "PluginBucketArn", "DsqlSinkPluginS3Key", "LambdaSeederS3Key",
        "SourceDbHostname", "SourceSecretArn", "SourceSecretName",
        "DsqlClusterArn", "DsqlClusterEndpoint",
        "TableIncludeList", "TopicPrefix", "SinkTopics", "DlqTopicName",
        "PluginVersion", "ConnectorMcuCount", "SinkMcuCount",
        "MskBootstrapServers", "DeploySink", "SeedMode",
    ):
        assert k in d, k
    assert d["TableIncludeList"] == "app.orders,app.customers"
    assert d["MskBootstrapServers"] == ""  # no connectors on the create pass
    assert d["DeploySink"] == "false"


def test_every_emitted_pg_infra_param_is_declared_in_the_template() -> None:
    # Keystone guard (PG variant of the MySQL one): a new PG param that is emitted
    # but not declared in cdc-stack.yaml would fail the deploy -- catch it here.
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
    declared = set(yaml.load(path.read_text(encoding="utf-8"), Loader=_L)["Parameters"])
    emitted = {k for k, _ in _pg_infra().filled}
    assert not (emitted - declared), emitted - declared


# ---------------------------------------------------------------------------
# build_pg_cdc_stack_params (Start CDC)
# ---------------------------------------------------------------------------


def test_pg_stack_params_swap_snapshot_for_engine_and_pg_snapshot() -> None:
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="s1", publication_name="p1",
    )
    params = build_pg_cdc_stack_params(src, _sink(), target_endpoint="ep")
    d = dict(params.filled)
    assert d["EngineType"] == "postgres"
    assert d["PgSnapshotMode"] == "never"
    assert "SnapshotMode" not in d  # MySQL enum dropped
    # Neutral connector params still present.
    assert d["TableIncludeList"] == "app.orders,app.customers"
    assert d["DeploySink"] == "true"
    # The manual-deploy placeholder names the PostgreSQL plugin, not the MySQL one.
    placeholder_keys = {k for k, _ in params.placeholders}
    assert "DebeziumPostgresPluginS3Key" in placeholder_keys
    assert "DebeziumPluginS3Key" not in placeholder_keys


# ---------------------------------------------------------------------------
# The MySQL builders stay byte-identical (no PG leakage)
# ---------------------------------------------------------------------------


def test_mysql_infra_params_unchanged_by_pg_addition() -> None:
    msrc = DebeziumSourceConfig(name="mysql-source", table_include_list=["app.orders"])
    d = dict(
        build_cdc_infra_params(
            msrc, _sink(),
            debezium_plugin_s3_key="cdc-plugins/debezium-mysql-plugin.zip",
            **_neutral_infra_kwargs(),
        ).filled
    )
    assert "EngineType" not in d  # MySQL set never carries the engine selector
    assert d["SourceDbServerId"] == "54321"
    assert d["SourceDbPort"] == "3306"
    assert d["DebeziumPluginS3Key"].endswith("debezium-mysql-plugin.zip")
    assert "DebeziumPostgresPluginS3Key" not in d


# ---------------------------------------------------------------------------
# _patch_plugin_params is engine-aware (no cross-add)
# ---------------------------------------------------------------------------


def _upload():
    return SimpleNamespace(
        bucket_arn="arn:aws:s3:::mysql-dsql-migrator-plugins-1-us-east-1",
        debezium_key="cdc-plugins/debezium-mysql-plugin.zip",
        debezium_pg_key="cdc-plugins/debezium-postgres-plugin.zip",
        dsql_sink_key="cdc-plugins/dsql-sink-connector.zip",
        lambda_seeder_key="cdc-plugins/offset-seeder-lambda.zip",
        plugin_version="v35",
    )


def test_patch_plugin_params_fills_pg_key_only_for_a_pg_set() -> None:
    patched = dict(_patch_plugin_params(_pg_infra(), _upload()).filled)
    assert patched["DebeziumPostgresPluginS3Key"].endswith("debezium-postgres-plugin.zip")
    # The MySQL source plugin key must NOT be cross-added to a PostgreSQL set.
    assert "DebeziumPluginS3Key" not in patched
    assert patched["DsqlSinkPluginS3Key"].endswith("dsql-sink-connector.zip")
    assert patched["PluginBucketArn"].endswith("us-east-1")


def test_patch_plugin_params_fills_mysql_key_only_for_a_mysql_set() -> None:
    msrc = DebeziumSourceConfig(name="mysql-source", table_include_list=["app.orders"])
    mi = build_cdc_infra_params(
        msrc, _sink(),
        debezium_plugin_s3_key="cdc-plugins/debezium-mysql-plugin.zip",
        **_neutral_infra_kwargs(),
    )
    patched = dict(_patch_plugin_params(mi, _upload()).filled)
    assert patched["DebeziumPluginS3Key"].endswith("debezium-mysql-plugin.zip")
    # The PostgreSQL plugin key must NOT be cross-added to a MySQL set.
    assert "DebeziumPostgresPluginS3Key" not in patched
