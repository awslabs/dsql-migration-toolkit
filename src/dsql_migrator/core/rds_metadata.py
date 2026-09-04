# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Best-effort source RDS/Aurora metadata for the overview diagram.

The source server version is read over the SQL connection
(:meth:`SourceIntrospector.test_connection`), but the **instance class** (e.g.
``db.r6g.large``) is RDS control-plane metadata that is not available over the
database connection. This module derives the DB identifier and region from the
source endpoint and looks the instance up via the RDS ``DescribeDBInstances``
API (``DescribeDBClusters`` membership for an Aurora cluster endpoint), sharing
the single ``boto3`` session/credential context (Requirements 9.5/9.7).

Everything is **best effort**: a non-RDS host, a missing ``rds:DescribeDBInstances``
permission, or any API error yields ``None`` so the diagram simply omits the
instance size rather than failing. No credentials are read or logged here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SourceInstanceInfo:
    """Best-effort RDS/Aurora instance metadata for the source endpoint."""

    instance_class: Optional[str] = None
    engine: Optional[str] = None
    engine_version: Optional[str] = None
    # The source's own VPC security group ids (from ``VpcSecurityGroups``). Used to
    # scope the CDC connector's egress-to-source rule to the source DB's SG instead
    # of falling back to an open ``0.0.0.0/0`` egress. Empty when unknown.
    security_group_ids: tuple[str, ...] = field(default_factory=tuple)


def parse_db_identifier(endpoint: str) -> Optional[str]:
    """Return the leading DB (instance or cluster) identifier from an endpoint.

    RDS/Aurora endpoints start with the identifier label, e.g.
    ``myinstance.abc.us-east-1.rds.amazonaws.com`` or
    ``mycluster.cluster-abc.us-east-1.rds.amazonaws.com`` -> ``myinstance`` /
    ``mycluster``. Returns ``None`` for an empty/dotless host.
    """
    host = (endpoint or "").strip()
    if not host or "." not in host:
        return None
    label = host.split(".", 1)[0].strip()
    return label or None


def is_cluster_endpoint(endpoint: str) -> bool:
    """Return whether ``endpoint`` is an Aurora cluster endpoint.

    Aurora cluster endpoints carry a ``cluster-`` (or ``cluster-ro-``) second
    label: ``mycluster.cluster-abc.us-east-1.rds.amazonaws.com``.
    """
    parts = (endpoint or "").split(".")
    return len(parts) >= 2 and parts[1].startswith("cluster-")


def parse_rds_region(endpoint: str) -> Optional[str]:
    """Return the region from an ``*.rds.amazonaws.com`` endpoint, or ``None``.

    The region is the label immediately preceding ``rds`` in
    ``...<region>.rds.amazonaws.com``. Returns ``None`` for non-RDS hosts.
    """
    parts = (endpoint or "").split(".")
    if "rds" not in parts:
        return None
    index = parts.index("rds")
    if index == 0:
        return None
    region = parts[index - 1].strip()
    return region or None


def describe_source_instance(
    rds_client: object, endpoint: str
) -> Optional[SourceInstanceInfo]:
    """Look up the source instance class/engine via RDS (best effort).

    For a cluster endpoint, the cluster's member instances are queried (the first
    member's class represents the cluster's size); for an instance endpoint the
    instance is queried directly. Returns ``None`` on any failure (non-RDS host,
    missing permission, not found), so callers can omit the metadata silently.
    """
    identifier = parse_db_identifier(endpoint)
    if not identifier:
        return None
    try:
        if is_cluster_endpoint(endpoint):
            response = rds_client.describe_db_instances(  # type: ignore[attr-defined]
                Filters=[{"Name": "db-cluster-id", "Values": [identifier]}]
            )
        else:
            response = rds_client.describe_db_instances(  # type: ignore[attr-defined]
                DBInstanceIdentifier=identifier
            )
        instances = response.get("DBInstances", []) if response else []
        if not instances:
            return None
        instance = (
            _prefer_cluster_writer(rds_client, identifier, instances)
            if is_cluster_endpoint(endpoint)
            else instances[0]
        )
        return SourceInstanceInfo(
            instance_class=instance.get("DBInstanceClass"),
            engine=instance.get("Engine"),
            engine_version=instance.get("EngineVersion"),
            security_group_ids=_active_security_group_ids(instance),
        )
    except Exception:  # noqa: BLE001 - metadata is optional, never fatal
        return None


def _prefer_cluster_writer(
    rds_client: object, cluster_id: str, instances: list
) -> dict:
    """Return the cluster's writer instance, or the first member as a fallback.

    ``describe_db_instances`` does not flag which member is the writer, so an
    Aurora cluster with asymmetric writer/reader sizing could otherwise report a
    reader's ``DBInstanceClass`` on the overview diagram. Resolve the writer's id
    from ``describe_db_clusters`` (``DBClusterMembers[].IsClusterWriter``) and
    return that instance. Best-effort: any failure (e.g. missing
    ``rds:DescribeDBClusters``) or a no-match falls back to the first member.
    """
    try:
        clusters = rds_client.describe_db_clusters(  # type: ignore[attr-defined]
            DBClusterIdentifier=cluster_id
        )
        members = (clusters.get("DBClusters") or [{}])[0].get("DBClusterMembers") or []
        writer_id = next(
            (
                member.get("DBInstanceIdentifier")
                for member in members
                if member.get("IsClusterWriter")
            ),
            None,
        )
        if writer_id:
            for instance in instances:
                if instance.get("DBInstanceIdentifier") == writer_id:
                    return instance
    except Exception:  # noqa: BLE001 - best-effort; fall back to the first member
        pass
    return instances[0]


def _active_security_group_ids(instance: dict) -> tuple[str, ...]:
    """Return the ``active`` VPC security group ids attached to an RDS instance.

    RDS reports each membership with a ``Status`` (``active`` / ``adding`` /
    ``removing``); we keep only ``active`` ones (falling back to any with an id if
    none report a status). Order is preserved and de-duplicated. Empty on any gap.
    """
    memberships = instance.get("VpcSecurityGroups") or []
    ids: list[str] = []
    for member in memberships:
        sg_id = (member.get("VpcSecurityGroupId") or "").strip()
        status = (member.get("Status") or "").strip().lower()
        if sg_id and status in ("", "active") and sg_id not in ids:
            ids.append(sg_id)
    return tuple(ids)


def fetch_source_security_group_id(
    rds_client: object, endpoint: str
) -> Optional[str]:
    """Return the source DB's first active VPC security group id (best effort).

    Used at CDC deploy time to scope the connector's egress-to-source rule to the
    source DB's own security group, so the stack does not fall back to an open
    ``0.0.0.0/0`` egress. Returns ``None`` for a non-RDS host, missing permission,
    or a source with no discoverable security group.
    """
    info = describe_source_instance(rds_client, endpoint)
    if info is None or not info.security_group_ids:
        return None
    return info.security_group_ids[0]


def build_rds_client(aws_profile: Optional[str], region: Optional[str]) -> object:
    """Build an RDS client from the shared session (honoring the global profile)."""
    from dsql_migrator.core.aws_session import build_session

    return build_session(aws_profile).client("rds", region_name=region)


__all__ = [
    "SourceInstanceInfo",
    "parse_db_identifier",
    "is_cluster_endpoint",
    "parse_rds_region",
    "describe_source_instance",
    "fetch_source_security_group_id",
    "build_rds_client",
]
