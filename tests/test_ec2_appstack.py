# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Structural guards for the Lambda-free "EC2 + MSK only" app-stack
(deploy/cloudformation-ec2.yaml, Option 3).

The EC2 stack forks the Fargate app-stack into a single in-VPC host reached via
SSM port-forward: no ALB/ACM/Cognito/ECS, state on a retained EBS volume, and the
in-process CDC seed (SeedMode=External) enabled by user-data. These tests parse
the template (tolerating CFN intrinsic tags) and assert the shape the design
requires, so a future edit can't silently reintroduce the ALB tier, drop the EBS
retain policy, or lose the data-plane MSK IAM the External seed needs.
"""

from __future__ import annotations

import pathlib

import pytest

yaml = pytest.importorskip("yaml")


def _load():
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
        / "deploy" / "cloudformation-ec2.yaml"
    )
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_L)


def test_top_level_description_within_cfn_1024_byte_limit() -> None:
    # CloudFormation caps the template-level Description at 1024 bytes and rejects
    # the template at ValidateTemplate/CreateStack otherwise — a hard deploy blocker.
    desc = _load()["Description"]
    assert len(desc.encode("utf-8")) <= 1024, len(desc.encode("utf-8"))


def test_parses_and_has_no_alb_cognito_ecs() -> None:
    res = _load()["Resources"]
    forbidden = {
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        "AWS::ElasticLoadBalancingV2::TargetGroup",
        "AWS::ElasticLoadBalancingV2::Listener",
        "AWS::Cognito::UserPool",
        "AWS::Cognito::UserPoolClient",
        "AWS::Cognito::UserPoolDomain",
        "AWS::ECS::Cluster",
        "AWS::ECS::Service",
        "AWS::ECS::TaskDefinition",
    }
    types = {v.get("Type") for v in res.values() if isinstance(v, dict)}
    assert not (types & forbidden), types & forbidden


def test_core_resources_present() -> None:
    res = _load()["Resources"]
    for name, typ in [
        ("AppHost", "AWS::EC2::Instance"),
        ("StateVolume", "AWS::EC2::Volume"),
        ("StateVolumeAttachment", "AWS::EC2::VolumeAttachment"),
        ("HostInstanceProfile", "AWS::IAM::InstanceProfile"),
        ("HostRole", "AWS::IAM::Role"),
        ("CdcDeployRole", "AWS::IAM::Role"),
        ("HostSecurityGroup", "AWS::EC2::SecurityGroup"),
    ]:
        assert name in res, name
        assert res[name]["Type"] == typ, (name, res[name]["Type"])


def test_state_volume_is_retained() -> None:
    # The accepted single-host failure model still requires state integrity: the
    # EBS volume must survive instance replacement so job/session SQLite resumes.
    sv = _load()["Resources"]["StateVolume"]
    assert sv.get("DeletionPolicy") == "Retain"
    assert sv.get("UpdateReplacePolicy") == "Retain"


def test_security_group_has_no_ingress_and_egress_9098() -> None:
    sg = _load()["Resources"]["HostSecurityGroup"]["Properties"]
    # No inbound: SSM port-forward uses the agent's outbound channel.
    assert not sg.get("SecurityGroupIngress")
    egress_ports = {e.get("FromPort") for e in sg.get("SecurityGroupEgress", [])}
    assert {443, 5432, 9098} <= egress_ports, egress_ports


def test_host_role_has_dataplane_msk_and_ssm() -> None:
    role = _load()["Resources"]["HostRole"]["Properties"]
    # SSM Session Manager managed policy (for the port-forward).
    managed = str(role.get("ManagedPolicyArns"))
    assert "AmazonSSMManagedInstanceCore" in managed
    # Data-plane kafka-cluster actions for the in-process seed (mirrors OffsetSeederRole).
    actions = str(role["Policies"])
    for act in (
        "kafka-cluster:Connect",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
    ):
        assert act in actions, act


def test_host_role_reuses_taskrole_sids() -> None:
    # The instance profile must carry the same app permissions the Fargate TaskRole
    # had, or the app breaks on EC2 (DSQL connect, plugin bucket, assume deploy role).
    actions = str(_load()["Resources"]["HostRole"]["Properties"]["Policies"])
    for sid in ("DsqlConnect", "ProvisionCdcSourceSecret", "DiscoverCdcConnectors",
                "ManagePluginBucket", "AssumeCdcDeployRole"):
        assert sid in actions, sid


def test_cdc_deploy_role_trusts_host_role() -> None:
    trust = _load()["Resources"]["CdcDeployRole"]["Properties"][
        "AssumeRolePolicyDocument"
    ]["Statement"][0]["Principal"]
    assert trust == {"AWS": {"Fn::GetAtt": ["HostRole", "Arn"]}}


def test_user_data_sets_external_seed_and_ebs_state() -> None:
    ud = _load()["Resources"]["AppHost"]["Properties"]["UserData"]
    # UserData is Fn::Base64 of an Fn::Sub; flatten to text to assert content.
    text = str(ud)
    assert "DSQL_MIGRATOR_CDC_SEED_MODE=external" in text
    assert "AWS_STS_REGIONAL_ENDPOINTS=regional" in text
    assert "/state/job_state.sqlite" in text
    assert "/state/session_state.sqlite" in text
    # State buckets intentionally NOT passed as env (EBS-SQLite branch selected).
    # (A comment may mention them; assert they are not SET as docker -e vars.)
    assert "-e DSQL_MIGRATOR_JOB_STATE_BUCKET" not in text
    assert "-e DSQL_MIGRATOR_SESSION_STATE_BUCKET" not in text


def test_params_add_ec2_and_drop_alb_cognito() -> None:
    params = _load()["Parameters"]
    for added in ("InstanceType", "HostSubnetId", "StateVolumeSizeGiB", "MskEgressCidr",
                  "LatestAl2023Ami"):
        assert added in params, added
    for dropped in ("CertificateArn", "AlbScheme", "AssignPublicIp",
                    "EnableCognitoAuth", "ContainerCpu", "ContainerMemory"):
        assert dropped not in params, dropped
    assert "VpcId" in params  # the one irreducible operator input, reused
