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
    def __init__(self, *, by_id=None, by_cluster=None, raises=False) -> None:  # noqa: ANN001
        self._by_id = by_id or {}
        self._by_cluster = by_cluster or {}
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
        instance_class="db.r6g.large", engine="mysql", engine_version="8.0.35"
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
