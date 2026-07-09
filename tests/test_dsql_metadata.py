# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the best-effort DSQL cluster metadata helper."""

from __future__ import annotations

from dsql_migrator.core.dsql_metadata import (
    fetch_dsql_cluster_name,
    parse_dsql_cluster_id,
)


def test_parse_dsql_cluster_id_variants() -> None:
    assert parse_dsql_cluster_id("abc123.dsql.us-east-1.on.aws") == "abc123"
    assert (
        parse_dsql_cluster_id("mycluster.cluster-abc123.us-east-1.rds.amazonaws.com")
        == "mycluster"
    )
    assert parse_dsql_cluster_id("") is None
    assert parse_dsql_cluster_id("bare") == "bare"


class _FakeDsql:
    """A fake DSQL client returning a cluster ARN and a tags payload."""

    def __init__(self, tags: object, *, arn: str = "arn:aws:dsql:us-east-1:1:cluster/abc") -> None:
        self._tags = tags
        self._arn = arn
        self.tagged_arn: str | None = None

    def get_cluster(self, identifier: str) -> dict:  # noqa: D401
        return {"identifier": identifier, "arn": self._arn, "status": "ACTIVE"}

    def list_tags_for_resource(self, resourceArn: str) -> dict:  # noqa: N803
        self.tagged_arn = resourceArn
        return {"tags": self._tags}


def test_fetch_dsql_cluster_name_from_dict_tags() -> None:
    client = _FakeDsql({"Name": "prod-orders", "env": "prod"})
    name = fetch_dsql_cluster_name(client, "abc.dsql.us-east-1.on.aws")
    assert name == "prod-orders"
    assert client.tagged_arn == "arn:aws:dsql:us-east-1:1:cluster/abc"


def test_fetch_dsql_cluster_name_from_list_tags() -> None:
    client = _FakeDsql([{"Key": "Name", "Value": "analytics"}])
    assert fetch_dsql_cluster_name(client, "abc.dsql.us-east-1.on.aws") == "analytics"


def test_fetch_dsql_cluster_name_none_when_untagged() -> None:
    client = _FakeDsql({"env": "prod"})  # no Name tag
    assert fetch_dsql_cluster_name(client, "abc.dsql.us-east-1.on.aws") is None


def test_fetch_dsql_cluster_name_none_on_error() -> None:
    class _Boom:
        def get_cluster(self, identifier: str) -> dict:
            raise RuntimeError("AccessDenied")

    assert fetch_dsql_cluster_name(_Boom(), "abc.dsql.us-east-1.on.aws") is None
    assert fetch_dsql_cluster_name(_FakeDsql({}), "") is None
