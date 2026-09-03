# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Structural guards for the PostgreSQL source connector in cdc-stack.yaml.

Phase B adds a dedicated Debezium PostgreSQL source connector (pgoutput) beside the
MySQL one, gated by an ``EngineType`` parameter. The MySQL default must stay
byte-identical (the PostgreSQL resources are gated off), and the PostgreSQL connector
must use logical replication (publication + replication slot), never the MySQL-only
knobs (server.id / GTID / schema-history). These tests parse the template (tolerating
CFN long/short intrinsic tags) and assert that wiring; they touch no AWS.
"""

from __future__ import annotations

import json
import pathlib

import pytest

yaml = pytest.importorskip("yaml")


def _load_template():
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


def test_engine_type_param_defaults_to_mysql() -> None:
    # Default mysql keeps every existing deploy byte-identical.
    p = _load_template()["Parameters"]["EngineType"]
    assert p["Default"] == "mysql"
    assert set(p["AllowedValues"]) == {"mysql", "postgres"}


def test_postgres_source_params_present_and_inert_by_default() -> None:
    params = _load_template()["Parameters"]
    # The PG plugin key + source params exist and default to empty/safe so a MySQL
    # deploy (which does not pass them) is unaffected.
    assert params["DebeziumPostgresPluginS3Key"]["Default"] == ""
    for p in ("PgDatabaseName", "PgSlotName", "PgPublicationName", "PgHeartbeatQuery"):
        assert params[p]["Default"] == "", p
    assert params["PgPublicationAutocreateMode"]["Default"] == "disabled"
    assert params["PgSnapshotMode"]["Default"] == "never"
    assert set(params["PgSnapshotMode"]["AllowedValues"]) == {
        "never", "initial", "no_data"
    }


def test_engine_source_conditions_present() -> None:
    conds = _load_template()["Conditions"]
    for name in (
        "IsMySqlSource", "IsPostgresSource",
        "DeployMysqlSource", "DeployPostgresSource",
        "HasPgHeartbeatQuery",
    ):
        assert name in conds, name
    # The two source connectors are mutually exclusive and each ANDs the shared
    # bootstrap gate, so the MySQL default path is unchanged.
    for name in ("DeployMysqlSource", "DeployPostgresSource"):
        blob = json.dumps(conds[name])
        assert "HasBootstrapServers" in blob, name


def test_mysql_source_connector_gated_on_engine() -> None:
    res = _load_template()["Resources"]
    # The MySQL source connector now requires EngineType=mysql (via DeployMysqlSource),
    # so a PostgreSQL stack does not create it.
    assert res["DebeziumSourceConnector"]["Condition"] == "DeployMysqlSource"


def test_postgres_source_plugin_gated_on_engine() -> None:
    plugin = _load_template()["Resources"]["PostgresSourcePlugin"]
    assert plugin["Condition"] == "IsPostgresSource"
    assert plugin["Type"] == "AWS::KafkaConnect::CustomPlugin"
    fixed = json.dumps(plugin["Properties"])
    assert "DebeziumPostgresPluginS3Key" in fixed
    assert "debezium-postgres" in fixed  # distinct resource name from the MySQL plugin


def test_postgres_source_connector_uses_logical_replication() -> None:
    res = _load_template()["Resources"]
    pg = res["PostgresSourceConnector"]
    assert pg["Condition"] == "DeployPostgresSource"
    assert pg["Type"] == "AWS::KafkaConnect::Connector"
    cfg = pg["Properties"]["ConnectorConfiguration"]
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["plugin.name"] == "pgoutput"
    # Logical-replication identity: slot + publication (pre-created), not server.id.
    assert cfg["slot.name"] == {"Ref": "PgSlotName"}
    assert cfg["publication.name"] == {"Ref": "PgPublicationName"}
    assert cfg["publication.autocreate.mode"] == {"Ref": "PgPublicationAutocreateMode"}
    assert cfg["database.dbname"] == {"Ref": "PgDatabaseName"}
    assert cfg["snapshot.mode"] == {"Ref": "PgSnapshotMode"}
    # Keep the slot on stop so a Stop/Start resumes gaplessly from its LSN.
    assert cfg["slot.drop.on.stop"] == "false"
    # Deterministic encodings the sink relies on (hardened-plan knobs).
    assert cfg["interval.handling.mode"] == "string"
    assert cfg["decimal.handling.mode"] == "precise"
    assert cfg["binary.handling.mode"] == "bytes"


def test_postgres_source_connector_enables_reselect_post_processor() -> None:
    # A PG UPDATE of an UNCHANGED TOASTed NON-string value emits the unavailable-value
    # placeholder the sink can only detect for string/bytea, so it would silently overwrite
    # the real value. The reselect post-processor re-queries the toasted column by PK on the
    # SOURCE before emitting the after-image, scoped to unavailable values only (never a
    # legitimate SET col=NULL).
    cfg = _load_template()["Resources"]["PostgresSourceConnector"][
        "Properties"
    ]["ConnectorConfiguration"]
    assert cfg["post.processors"] == "reselector"
    assert (
        cfg["reselector.type"]
        == "io.debezium.processors.reselect.ReselectColumnsPostProcessor"
    )
    # Trigger on toasted (unavailable) values; do NOT reselect genuine NULLs (that would
    # discard a real SET col=NULL).
    assert cfg["reselector.reselect.unavailable.values"] == "true"
    assert cfg["reselector.reselect.null.values"] == "false"


def test_mysql_source_connector_has_no_reselect_post_processor() -> None:
    # MySQL's binlog carries the full before/after image, so it needs no reselect; the
    # post-processor is a PostgreSQL-only addition and must NOT leak into the MySQL source.
    cfg = _load_template()["Resources"]["DebeziumSourceConnector"][
        "Properties"
    ]["ConnectorConfiguration"]
    assert "post.processors" not in cfg
    assert not any(k.startswith("reselector.") for k in cfg)


def test_postgres_source_connector_omits_mysql_only_keys() -> None:
    cfg = _load_template()["Resources"]["PostgresSourceConnector"][
        "Properties"
    ]["ConnectorConfiguration"]
    for bad in (
        "database.server.id",
        "gtid.source.filter.dml.events",
        "gtid.source.excludes",
        "schema.history.internal.kafka.topic",
        "schema.history.internal.kafka.bootstrap.servers",
        "bigint.unsigned.handling.mode",
    ):
        assert bad not in cfg, ("MySQL-only key leaked into PG connector", bad)


def test_both_source_connectors_share_one_connector_name() -> None:
    # Same ConnectorName -> monitoring/alarms (keyed on the connector name) stay
    # engine-agnostic; only one is ever created (mutually exclusive by condition).
    res = _load_template()["Resources"]
    mysql_name = res["DebeziumSourceConnector"]["Properties"]["ConnectorName"]
    pg_name = res["PostgresSourceConnector"]["Properties"]["ConnectorName"]
    assert mysql_name == pg_name


def test_seed_conditions_are_engine_aware() -> None:
    # SeedByLambda/SeedByExternal are the single source of truth for the seed path and
    # MUST be engine-aware: a PostgreSQL stack resumes from a replication slot (not a
    # Kafka connect-offset), so it is ALWAYS the external (no-Lambda) path regardless
    # of SeedMode. Otherwise a PG stack in SeedMode=Lambda would create the Lambda
    # seeder / CdcStartPrepResource / Lambda sink variant, all referencing the
    # IsMySqlSource-gated seeder -> a CloudFormation rollback.
    conds = _load_template()["Conditions"]
    lam = json.dumps(conds["SeedByLambda"])
    ext = json.dumps(conds["SeedByExternal"])
    assert "IsMySqlSource" in lam, lam  # Lambda path only for MySQL
    assert "IsPostgresSource" in ext, ext  # PG is always external-seeded
    # Every SeedMode-gated resource ANDs one of these, so this makes CdcStartPrepResource
    # and the Lambda sink variant off for PostgreSQL in one place.


def test_start_prep_resource_is_gated_off_for_postgres() -> None:
    # Belt-and-braces: DeployStartPrepResource still names IsMySqlSource directly so
    # CdcStartPrepResource is obviously MySQL-only at its own definition.
    conds = _load_template()["Conditions"]
    gate = json.dumps(conds["DeployStartPrepResource"])
    assert "IsMySqlSource" in gate, gate
    assert "SeedByLambda" in gate
    assert "HasBootstrapServers" in gate


def test_source_connector_arn_output_refs_the_active_engine() -> None:
    # The single source-connector ARN output is gated on HasBootstrapServers but must
    # Ref whichever engine's connector exists (both are conditional). Without the
    # Fn::If it would Ref the IsMySqlSource-gated DebeziumSourceConnector on a
    # PostgreSQL stack -> a CloudFormation reference-to-absent-resource error.
    out = _load_template()["Outputs"]["DebeziumSourceConnectorArn"]
    value = json.dumps(out["Value"])
    assert "Fn::If" in value
    assert "IsMySqlSource" in value
    assert "DebeziumSourceConnector" in value
    assert "PostgresSourceConnector" in value


def test_postgres_connector_reuses_the_neutral_sink_untouched() -> None:
    # The DSQL sink is engine-neutral: it must be unchanged (still the custom
    # connector), so a PostgreSQL pipeline reuses it verbatim.
    res = _load_template()["Resources"]
    sink = res["DsqlSinkConnector"]["Properties"]["ConnectorConfiguration"]
    assert sink["connector.class"] == "dev.dsqlmigrator.connect.DsqlSinkConnector"
