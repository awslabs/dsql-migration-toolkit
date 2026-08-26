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
    dispatch_cdc_infra_params,
    dispatch_cdc_stack_params,
    dispatch_source_config,
)
from dsql_migrator.core.models import SourceType, TableDef, Watermark


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


def test_pg_source_config_force_initial_snapshot_overrides_gapless() -> None:
    # The PostgreSQL Manual start choice = re-snapshot: force_initial_snapshot forces
    # snapshot.mode=initial even though the watermark carries a gapless WAL LSN (which
    # would otherwise select `never`). Debezium PG resumes only from the slot, so Manual
    # cannot supply a start LSN -- it re-snapshots instead.
    forced = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="s1", publication_name="p1",
        force_initial_snapshot=True,
    )
    assert forced.snapshot_mode == "initial"
    # Without the flag the same gapless watermark -> never (Automatic).
    auto = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="s1", publication_name="p1",
    )
    assert auto.snapshot_mode == "never"


def test_dispatch_force_initial_snapshot_is_postgres_only() -> None:
    tables = _tables()
    # PG: the flag flows to build_pg_source_config -> initial even with a gapless LSN.
    pg = dispatch_source_config(
        SourceType.POSTGRES, tables, _wm(wal_lsn="3/AF012B8"),
        database="app", stack_name="mysql-dsql-cdc-stack", force_initial_snapshot=True,
    )
    assert isinstance(pg, PostgresSourceConfig)
    assert pg.snapshot_mode == "initial"
    # MySQL accepts the kwarg for a uniform call site but IGNORES it (no such concept);
    # byte-identical to the flag-less MySQL build.
    mysql = dispatch_source_config(SourceType.MYSQL, tables, _wm(), force_initial_snapshot=True)
    assert isinstance(mysql, DebeziumSourceConfig)


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


def test_pg_infra_params_force_seed_mode_external() -> None:
    # PostgreSQL resumes from a logical replication slot, not the Lambda binlog
    # offset seeder. The PG builder MUST force SeedMode=External so the template does
    # not create CdcStartPrepResource (which GetAtt/DependsOn the IsMySqlSource-gated
    # OffsetSeederFunction/Role -> a CloudFormation rollback on a PG stack).
    d = dict(_pg_infra().filled)
    assert d["SeedMode"] == "External", d.get("SeedMode")


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
    # The plugin key is a PLACEHOLDER ONLY -- it must NOT appear in .filled (as an empty
    # value). On the Start update_stack a filled-empty plugin key blanks the CustomPlugin's
    # FileKey and forces CloudFormation to REPLACE the custom-named AWS::KafkaConnect::
    # CustomPlugin, which it refuses ("custom-named resource requires replacing"). It must
    # be UsePreviousValue on Start, exactly like the MySQL DebeziumPluginS3Key.
    assert "DebeziumPostgresPluginS3Key" not in d
    # ...but the INFRA create set DOES carry it (empty, for _patch_plugin_params to fill).
    assert "DebeziumPostgresPluginS3Key" in dict(_pg_infra().filled)


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


def test_dispatch_source_config_branches_by_engine() -> None:
    tables = _tables()
    # MySQL -> the orchestrator's DebeziumSourceConfig (name unchanged).
    mysql_cfg = dispatch_source_config(SourceType.MYSQL, tables, _wm())
    assert isinstance(mysql_cfg, DebeziumSourceConfig)
    assert mysql_cfg.name == "mysql-source"
    # PostgreSQL -> a PostgresSourceConfig with the database + deterministic slot/pub
    # names from the stack, snapshot.mode=never (a WAL LSN is present).
    pg_cfg = dispatch_source_config(
        SourceType.POSTGRES, tables, _wm(wal_lsn="3/AF012B8"),
        database="app", stack_name="mysql-dsql-cdc-stack",
    )
    assert isinstance(pg_cfg, PostgresSourceConfig)
    assert pg_cfg.database_name == "app"
    # No recorded names on this watermark -> derived from the stack (hash-suffixed).
    from dsql_migrator.core.cdc_pg_slot import pg_publication_name, pg_slot_name

    assert pg_cfg.slot_name == pg_slot_name("mysql-dsql-cdc-stack")
    assert pg_cfg.publication_name == pg_publication_name("mysql-dsql-cdc-stack")
    assert pg_cfg.snapshot_mode == "never"
    assert pg_cfg.table_include_list == ["app.orders", "app.customers"]


def test_dispatch_source_config_uses_recorded_watermark_names() -> None:
    # The connector must resume from the EXACT slot the Full Load created, so when the
    # watermark carries recorded slot/publication names, dispatch uses THOSE -- not a name
    # re-derived from the (mutable) live stack name (which could point at a missing slot).
    wm = Watermark(
        snapshot_timestamp=datetime.now(timezone.utc),
        wal_lsn="3/AF012B8",
        slot_name="dsqlmig_recorded_slot",
        publication_name="dsqlmig_pub_recorded",
    )
    cfg = dispatch_source_config(
        SourceType.POSTGRES, _tables(), wm,
        database="app", stack_name="a-totally-different-stack-name",
    )
    assert cfg.slot_name == "dsqlmig_recorded_slot"
    assert cfg.publication_name == "dsqlmig_pub_recorded"


def test_dispatch_params_route_to_the_matching_engine_builder() -> None:
    tables = _tables()
    sink = _sink()
    mysql_cfg = dispatch_source_config(SourceType.MYSQL, tables, _wm())
    pg_cfg = dispatch_source_config(
        SourceType.POSTGRES, tables, _wm(wal_lsn="3/AF012B8"),
        database="app", stack_name="mysql-dsql-cdc-stack",
    )
    # Stack params: PG carries EngineType=postgres; MySQL does not.
    assert dict(dispatch_cdc_stack_params(mysql_cfg, sink, target_endpoint="ep").filled).get(
        "EngineType"
    ) is None
    assert dict(
        dispatch_cdc_stack_params(pg_cfg, sink, target_endpoint="ep").filled
    )["EngineType"] == "postgres"
    # Infra params: same routing, and PG drops the MySQL-only source params. The MySQL
    # builder requires debezium_plugin_s3_key; the PG builder pops it (harmless to pass).
    deb = "cdc-plugins/debezium-mysql-plugin.zip"
    mysql_infra = dict(
        dispatch_cdc_infra_params(
            mysql_cfg, sink, debezium_plugin_s3_key=deb, **_neutral_infra_kwargs()
        ).filled
    )
    pg_infra = dict(
        dispatch_cdc_infra_params(
            pg_cfg, sink, debezium_plugin_s3_key=deb, **_neutral_infra_kwargs()
        ).filled
    )
    assert "SourceDbServerId" in mysql_infra and "EngineType" not in mysql_infra
    assert pg_infra["EngineType"] == "postgres" and "SourceDbServerId" not in pg_infra


def test_classify_slot_health_tones() -> None:
    from dsql_migrator.core.cdc_postgres import SlotHealth, classify_slot_health

    # Unknown / missing -> info (never a false all-clear).
    assert classify_slot_health(None)[0] == "info"
    assert classify_slot_health(SlotHealth("s", exists=False))[0] == "info"
    # Invalidated slot -> error (gapless resume broken).
    assert classify_slot_health(
        SlotHealth("s", exists=True, active=True, wal_status="lost")
    )[0] == "error"
    # WAL pressure -> warning (unreserved / extended / no headroom).
    for h in (
        SlotHealth("s", exists=True, active=True, wal_status="unreserved"),
        SlotHealth("s", exists=True, active=True, wal_status="extended"),
        SlotHealth("s", exists=True, active=True, wal_status="reserved", safe_wal_size=-1),
    ):
        assert classify_slot_health(h)[0] == "warning"
    # Inactive slot (no consumer) -> warning (WAL accumulating).
    assert classify_slot_health(
        SlotHealth("s", exists=True, active=False, wal_status="reserved")
    )[0] == "warning"
    # Active + reserved + headroom -> success.
    assert classify_slot_health(
        SlotHealth("s", exists=True, active=True, wal_status="reserved", safe_wal_size=1000)
    )[0] == "success"


def test_read_replication_slot_health_is_postgres_only() -> None:
    from sqlalchemy import text  # noqa: F401 - documents the SELECT the fake matches
    from dsql_migrator.core.source_dialect import dialect_for

    # MySQL: no slot to watch.
    assert (
        dialect_for(SourceType.MYSQL).read_replication_slot_health(object(), "s") is None
    )

    class _R:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    class _Conn:
        def __init__(self, row):
            self._row = row

        def execute(self, statement, params=None):
            assert "pg_replication_slots" in str(statement)
            return _R(self._row)

    pg = dialect_for(SourceType.POSTGRES)
    health = pg.read_replication_slot_health(
        _Conn((True, "reserved", 12345, "0/16B3748", "0/16B3800")), "dsqlmig_s"
    )
    assert health.exists and health.active and health.wal_status == "reserved"
    assert health.safe_wal_size == 12345 and health.restart_lsn == "0/16B3748"
    # 0 rows -> the slot does not exist.
    assert pg.read_replication_slot_health(_Conn(None), "dsqlmig_s").exists is False


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


def test_pg_stack_params_filled_carries_no_plugin_location_or_immutable_param() -> None:
    # Tier-3 #23: NO plugin-Location / immutable param may appear in the Start (stack) params'
    # .filled -- sending one on the Start update_stack blanks/changes an immutable CustomPlugin
    # property and forces CloudFormation to REPLACE the custom-named plugin (the rollback that
    # bit at Phase F). They must be placeholder-only (UsePreviousValue). Guards ALL siblings.
    src = build_pg_source_config(
        "pg-source", _tables(), _wm(wal_lsn="3/AF012B8"),
        database_name="app", slot_name="s1", publication_name="p1",
    )
    filled_keys = {k for k, _ in build_pg_cdc_stack_params(src, _sink(), target_endpoint="ep").filled}
    assert filled_keys.isdisjoint({
        "DebeziumPostgresPluginS3Key", "DebeziumPluginS3Key", "PluginBucketArn",
        "DsqlSinkPluginS3Key", "LambdaSeederS3Key", "PluginVersion",
    }), filled_keys
