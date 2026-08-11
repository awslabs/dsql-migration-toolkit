# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Structural guards for the SeedMode=Lambda|External gating in cdc-stack.yaml.

Option ② adds a SeedMode parameter that, when "External", omits the in-VPC
offset-seeder Lambda resources and selects a sink-connector variant without the
CdcStartPrepResource DependsOn. The DEFAULT (Lambda) must stay identical to today.
These tests parse the template (tolerating CFN long/short intrinsic tags) and
assert the gating wiring is intact and the two sink variants share ONE body (via a
YAML anchor) so they cannot drift.
"""

from __future__ import annotations

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


def test_seedmode_param_defaults_to_lambda() -> None:
    doc = _load_template()
    p = doc["Parameters"]["SeedMode"]
    assert p["Default"] == "Lambda"
    assert set(p["AllowedValues"]) == {"Lambda", "External"}


def test_seedmode_conditions_present() -> None:
    conds = _load_template()["Conditions"]
    for name in (
        "SeedByLambda", "SeedByExternal",
        "DeploySeederFunctionLambda", "DeployStartPrepResource",
        "DeploySinkConnectorLambda", "DeploySinkConnectorExternal",
    ):
        assert name in conds, name


def test_seeder_resources_gated_on_seedbylambda() -> None:
    # In External mode the three in-VPC seeder resources must be absent, so each
    # AND-gates its existing condition with SeedByLambda (named conditions).
    res = _load_template()["Resources"]
    assert res["OffsetSeederRole"]["Condition"] == "DeploySeederFunctionLambda"
    assert res["OffsetSeederFunction"]["Condition"] == "DeploySeederFunctionLambda"
    assert res["CdcStartPrepResource"]["Condition"] == "DeployStartPrepResource"


def test_sink_variants_share_one_body_and_differ_only_in_dependson() -> None:
    res = _load_template()["Resources"]
    lam = res["DsqlSinkConnector"]
    ext = res["DsqlSinkConnectorExternal"]
    # Same condition family, mutually exclusive by SeedMode.
    assert lam["Condition"] == "DeploySinkConnectorLambda"
    assert ext["Condition"] == "DeploySinkConnectorExternal"
    # The YAML anchor makes both Properties bodies the SAME object content -> they
    # cannot drift.
    assert lam["Properties"] == ext["Properties"]
    # The ONLY structural difference is the Lambda variant's extra DependsOn on the
    # in-VPC prep resource.
    assert "CdcStartPrepResource" in lam["DependsOn"]
    assert "CdcStartPrepResource" not in ext["DependsOn"]
    assert set(ext["DependsOn"]) == {"MskCluster", "ConnectorSelfIngress"}


def test_source_connector_offset_seed_tag_is_conditional() -> None:
    # The source's ordering edge is a Tag GetAtt on CdcStartPrepResource, wrapped in
    # Fn::If SeedByLambda so it drops (AWS::NoValue) in External mode. Assert the
    # Tags property is an Fn::If keyed on SeedByLambda.
    res = _load_template()["Resources"]
    tags = res["DebeziumSourceConnector"]["Properties"]["Tags"]
    assert isinstance(tags, dict) and "Fn::If" in tags
    assert tags["Fn::If"][0] == "SeedByLambda"


def test_sink_output_refs_active_variant() -> None:
    out = _load_template()["Outputs"]["DsqlSinkConnectorArn"]
    # Still gated on DeploySinkConnector (present in both modes); value is an Fn::If
    # selecting the variant that exists.
    assert out["Condition"] == "DeploySinkConnector"
    assert "Fn::If" in out["Value"]
    assert out["Value"]["Fn::If"][0] == "SeedByLambda"


# --------------------------------------------------------------------------- #
# Option 3 reachability: the Lambda-free EC2 host is admitted to MSK on 9098 by
# subnet CIDR (mirroring the bastion rule), condition-gated + empty default so the
# rule is absent (byte-identical) unless a host CIDR is supplied.
# --------------------------------------------------------------------------- #
def test_host_subnet_cidr_param_defaults_empty() -> None:
    doc = _load_template()
    p = doc["Parameters"]["HostSubnetCidr"]
    assert p["Default"] == ""  # empty -> no ingress rule -> unchanged default deploy
    assert "HasHostSubnetCidr" in doc["Conditions"]


def test_host_diagnostics_ingress_is_9098_by_cidr_and_gated() -> None:
    res = _load_template()["Resources"]
    rule = res["ConnectorHostDiagnosticsIngress"]
    assert rule["Type"] == "AWS::EC2::SecurityGroupIngress"
    assert rule["Condition"] == "HasHostSubnetCidr"
    props = rule["Properties"]
    assert props["FromPort"] == 9098 and props["ToPort"] == 9098
    assert props["IpProtocol"] == "tcp"
    # By CIDR (the host subnet), NOT an SG id -> keeps the two stacks decoupled.
    assert props["CidrIp"] == {"Ref": "HostSubnetCidr"}
    assert props["GroupId"] == {"Fn::GetAtt": ["ConnectorSecurityGroup", "GroupId"]}


def test_bastion_diagnostics_literal_cidr_unchanged() -> None:
    # The pre-existing bastion rule's hardcoded /20 must NOT be touched or widened.
    rule = _load_template()["Resources"]["ConnectorBastionDiagnosticsIngress"]
    assert rule["Properties"]["CidrIp"] == "172.31.0.0/20"
