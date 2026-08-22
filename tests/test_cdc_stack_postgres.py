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


def test_postgres_connector_reuses_the_neutral_sink_untouched() -> None:
    # The DSQL sink is engine-neutral: it must be unchanged (still the custom
    # connector), so a PostgreSQL pipeline reuses it verbatim.
    res = _load_template()["Resources"]
    sink = res["DsqlSinkConnector"]["Properties"]["ConnectorConfiguration"]
    assert sink["connector.class"] == "dev.dsqlmigrator.connect.DsqlSinkConnector"
