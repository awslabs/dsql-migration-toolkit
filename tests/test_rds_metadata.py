# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for best-effort source RDS/Aurora metadata (overview diagram).

Covers endpoint parsing (identifier, cluster detection, region) and the
``DescribeDBInstances`` lookup for instance and Aurora cluster endpoints,
including the best-effort fallbacks (non-RDS host, not found, API error).
"""

from __future__ import annotations

from dsql_migrator.core.rds_metadata import (
    SourceInstanceInfo,
    describe_source_instance,
    fetch_source_security_group_id,
    is_cluster_endpoint,
    parse_db_identifier,
    parse_rds_region,
)

_INSTANCE = "myinstance.abc123.us-east-1.rds.amazonaws.com"
_CLUSTER = "mycluster.cluster-abc123.us-east-1.rds.amazonaws.com"


def test_parse_db_identifier() -> None:
    assert parse_db_identifier(_INSTANCE) == "myinstance"
    assert parse_db_identifier(_CLUSTER) == "mycluster"
    assert parse_db_identifier("localhost") is None
    assert parse_db_identifier("") is None


def test_is_cluster_endpoint() -> None:
    assert is_cluster_endpoint(_CLUSTER) is True
    assert is_cluster_endpoint(_INSTANCE) is False
    assert is_cluster_endpoint("host.cluster-ro-x.eu-west-1.rds.amazonaws.com") is True


def test_parse_rds_region() -> None:
    assert parse_rds_region(_INSTANCE) == "us-east-1"
    assert parse_rds_region(_CLUSTER) == "us-east-1"
    assert parse_rds_region("db.internal.example.com") is None


class _FakeRds:
    def __init__(
        self, *, by_id=None, by_cluster=None, cluster_members=None, raises=False
    ) -> None:  # noqa: ANN001
        self._by_id = by_id or {}
        self._by_cluster = by_cluster or {}
        self._cluster_members = cluster_members or {}
        self._raises = raises
        self.calls: list[dict] = []

    def describe_db_instances(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(kwargs)
        if self._raises:
            raise RuntimeError("AccessDenied: rds:DescribeDBInstances")
        if "DBInstanceIdentifier" in kwargs:
            instances = self._by_id.get(kwargs["DBInstanceIdentifier"], [])
        else:  # cluster filter
            cluster = kwargs["Filters"][0]["Values"][0]
            instances = self._by_cluster.get(cluster, [])
        return {"DBInstances": instances}

    def describe_db_clusters(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(kwargs)
        members = self._cluster_members.get(kwargs.get("DBClusterIdentifier"), [])
        return {"DBClusters": [{"DBClusterMembers": members}]}


def test_describe_source_instance_prefers_cluster_writer() -> None:
    # An Aurora cluster reports all members with no writer flag on
    # DescribeDBInstances; the writer is resolved via DescribeDBClusters so an
    # asymmetric writer/reader topology reports the WRITER's class, not a reader's.
    rds = _FakeRds(
        by_cluster={
            "mycluster": [
                {"DBInstanceIdentifier": "reader-1", "DBInstanceClass": "db.r6g.large"},
                {"DBInstanceIdentifier": "writer-1", "DBInstanceClass": "db.r6g.4xlarge"},
            ]
        },
        cluster_members={
            "mycluster": [
                {"DBInstanceIdentifier": "reader-1", "IsClusterWriter": False},
                {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
            ]
        },
    )
    info = describe_source_instance(rds, _CLUSTER)
    assert info is not None
    assert info.instance_class == "db.r6g.4xlarge"


def test_describe_source_instance_cluster_falls_back_to_first_member() -> None:
    # No writer info (or missing rds:DescribeDBClusters) -> first member, safely.
    rds = _FakeRds(
        by_cluster={
            "mycluster": [
                {"DBInstanceIdentifier": "only-1", "DBInstanceClass": "db.r6g.large"},
            ]
        }
    )
    info = describe_source_instance(rds, _CLUSTER)
    assert info is not None
    assert info.instance_class == "db.r6g.large"


def test_describe_source_instance_for_instance_endpoint() -> None:
    rds = _FakeRds(
        by_id={
            "myinstance": [
                {
                    "DBInstanceClass": "db.r6g.large",
                    "Engine": "mysql",
                    "EngineVersion": "8.0.35",
                }
            ]
        }
    )
    info = describe_source_instance(rds, _INSTANCE)
    assert info == SourceInstanceInfo(
        instance_class="db.r6g.large", engine="mysql", engine_version="8.0.35",
        # The identifier the lookup resolved, kept so the CDC screen can show the
        # PROVENANCE of a derived VpcId rather than silently substituting a value.
        db_identifier="myinstance",
    )


def test_describe_source_instance_for_cluster_endpoint() -> None:
    rds = _FakeRds(
        by_cluster={
            "mycluster": [
                {"DBInstanceClass": "db.r6g.xlarge", "Engine": "aurora-mysql"}
            ]
        }
    )
    info = describe_source_instance(rds, _CLUSTER)
    assert info is not None
    assert info.instance_class == "db.r6g.xlarge"
    # Looked up via the db-cluster-id filter, not a direct instance id.
    assert rds.calls[0]["Filters"][0]["Name"] == "db-cluster-id"


def test_describe_source_instance_best_effort_returns_none() -> None:
    assert describe_source_instance(_FakeRds(), "localhost") is None  # non-RDS host
    assert describe_source_instance(_FakeRds(), _INSTANCE) is None  # not found
    assert describe_source_instance(_FakeRds(raises=True), _INSTANCE) is None  # error


def test_describe_source_instance_reads_active_security_groups() -> None:
    rds = _FakeRds(
        by_id={
            "myinstance": [
                {
                    "DBInstanceClass": "db.r6g.large",
                    "VpcSecurityGroups": [
                        {"VpcSecurityGroupId": "sg-active", "Status": "active"},
                        {"VpcSecurityGroupId": "sg-removing", "Status": "removing"},
                    ],
                }
            ]
        }
    )
    info = describe_source_instance(rds, _INSTANCE)
    assert info is not None
    # Only the active membership is kept; the removing one is dropped.
    assert info.security_group_ids == ("sg-active",)


def test_fetch_source_security_group_id_returns_first_active() -> None:
    rds = _FakeRds(
        by_id={
            "myinstance": [
                {
                    "VpcSecurityGroups": [
                        {"VpcSecurityGroupId": "sg-one", "Status": "active"},
                        {"VpcSecurityGroupId": "sg-two", "Status": "active"},
                    ]
                }
            ]
        }
    )
    assert fetch_source_security_group_id(rds, _INSTANCE) == "sg-one"


def test_fetch_source_security_group_id_best_effort_none() -> None:
    # Non-RDS host, not found, and API error all yield None (open-egress fallback).
    assert fetch_source_security_group_id(_FakeRds(), "localhost") is None
    assert fetch_source_security_group_id(_FakeRds(), _INSTANCE) is None
    assert fetch_source_security_group_id(_FakeRds(raises=True), _INSTANCE) is None
    # An instance with no security groups also yields None (not an empty string).
    rds = _FakeRds(by_id={"myinstance": [{"DBInstanceClass": "db.t3.medium"}]})
    assert fetch_source_security_group_id(rds, _INSTANCE) is None


def test_source_network_comes_from_the_response_the_tool_already_reads() -> None:
    """VpcId/subnets are more fields off the SAME DescribeDBInstances response.

    The tool already calls it to scope the CDC connector's egress rule to the source DB's
    security group, so deriving the network placement needs no extra API call and no extra
    IAM -- which is why the comment claiming "VpcId is the one input the tool cannot infer"
    was wrong.
    """
    from dsql_migrator.core.rds_metadata import fetch_source_network

    rds = _FakeRds(
        by_id={
            "myinstance": [{
                "DBInstanceClass": "db.r6g.large",
                "Engine": "postgres",
                "VpcSecurityGroups": [
                    {"VpcSecurityGroupId": "sg-0a2f", "Status": "active"}
                ],
                "DBSubnetGroup": {
                    "DBSubnetGroupName": "workshop-base-dbsubnetgroup",
                    "VpcId": "vpc-0ea46c134d00458bc",
                    "Subnets": [
                        {"SubnetIdentifier": "subnet-0fbf", "SubnetStatus": "Available"},
                        {"SubnetIdentifier": "subnet-0dfe", "SubnetStatus": "Available"},
                        # Mid-modification subnets are poor placement targets.
                        {"SubnetIdentifier": "subnet-bad", "SubnetStatus": "Modifying"},
                    ],
                },
            }],
        }
    )
    info = fetch_source_network(rds, _INSTANCE)
    assert info is not None
    assert info.vpc_id == "vpc-0ea46c134d00458bc"
    assert info.subnet_ids == ("subnet-0fbf", "subnet-0dfe")
    assert info.db_subnet_group == "workshop-base-dbsubnetgroup"
    assert info.db_identifier == "myinstance"
    # The security group the tool already used is untouched.
    assert info.security_group_ids == ("sg-0a2f",)

    # Best effort: a DB with no subnet group (non-VPC / unresolvable) yields None, so the
    # caller falls back to the manual field rather than inventing a value.
    bare = _FakeRds(by_id={"myinstance": [{"Engine": "postgres"}]})
    assert fetch_source_network(bare, _INSTANCE) is None


def test_a_vpc_that_is_not_the_sources_warns_but_never_blocks() -> None:
    """A mismatch is the shape of a typo, and today it costs ~20 minutes to discover.

    The MSK cluster is created first, so a VpcId that cannot reach the source fails only
    after the deploy is well under way, leaving a failed stack to tear down. It must warn
    BEFORE Deploy -- and only warn, because peering / Transit Gateway / PrivateLink make a
    different VPC legitimate.
    """
    from dsql_migrator.core.rds_metadata import (
        SourceInstanceInfo,
        source_vpc_mismatch_warning,
    )

    source = SourceInstanceInfo(
        vpc_id="vpc-source", db_identifier="pgtest-ecommerce"
    )

    warning = source_vpc_mismatch_warning("vpc-elsewhere", source)
    assert warning is not None
    assert "vpc-elsewhere" in warning and "vpc-source" in warning
    assert "pgtest-ecommerce" in warning, "the operator needs to know which source"
    # It must name what to verify, not just object.
    assert "peering" in warning.lower()

    # Matching -> silent.
    assert source_vpc_mismatch_warning("vpc-source", source) is None
    # Unknowable -> silent. Absence of information must never manufacture a warning:
    # a non-RDS host, a cross-account endpoint or a missing permission all land here.
    assert source_vpc_mismatch_warning("vpc-elsewhere", None) is None
    assert source_vpc_mismatch_warning(
        "vpc-elsewhere", SourceInstanceInfo(vpc_id=None)
    ) is None
    assert source_vpc_mismatch_warning("", source) is None
    assert source_vpc_mismatch_warning(None, source) is None
