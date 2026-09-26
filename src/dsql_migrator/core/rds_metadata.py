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
    # The source's own network placement, from the SAME ``DBSubnetGroup`` this response
    # already carries -- no extra API call and no extra IAM. Used to PREFILL the CDC
    # infrastructure's VpcId (and the advanced connector subnets) and, more importantly, to
    # warn when the VpcId an operator typed is not the source's: today a wrong VpcId is only
    # discovered after ~20 minutes of MSK deployment. ``None``/empty when the host is not an
    # RDS endpoint, the permission is missing, or the DB is not in a VPC.
    vpc_id: Optional[str] = None
    subnet_ids: tuple[str, ...] = field(default_factory=tuple)
    db_subnet_group: Optional[str] = None
    # The identifier the values were derived FROM, so the UI can show its provenance
    # ("Derived from the source cluster <id>") rather than silently substituting a value.
    db_identifier: Optional[str] = None


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
        subnet_group = instance.get("DBSubnetGroup") or {}
        return SourceInstanceInfo(
            instance_class=instance.get("DBInstanceClass"),
            engine=instance.get("Engine"),
            engine_version=instance.get("EngineVersion"),
            security_group_ids=_active_security_group_ids(instance),
            vpc_id=(subnet_group.get("VpcId") or None),
            subnet_ids=_subnet_ids(subnet_group),
            db_subnet_group=(subnet_group.get("DBSubnetGroupName") or None),
            db_identifier=identifier,
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


def _subnet_ids(subnet_group: dict) -> tuple[str, ...]:
    """The ``Available`` subnet ids of an RDS ``DBSubnetGroup``, order preserved.

    Only ``Available`` subnets are usable placement targets; a subnet mid-modification
    would be a poor default for the CDC connector. Falls back to any subnet carrying an id
    when none report a status, mirroring :func:`_active_security_group_ids`.
    """
    ids: list[str] = []
    for subnet in subnet_group.get("Subnets") or []:
        subnet_id = (subnet.get("SubnetIdentifier") or "").strip()
        status = (subnet.get("SubnetStatus") or "").strip().lower()
        if subnet_id and status in ("", "available") and subnet_id not in ids:
            ids.append(subnet_id)
    return tuple(ids)


def fetch_source_network(
    rds_client: object, endpoint: str
) -> Optional["SourceInstanceInfo"]:
    """The source DB's VPC / subnets / subnet group (best effort, ``None`` on any gap).

    A thin alias of :func:`describe_source_instance` named for the CDC call site, which
    wants the network placement rather than the instance size. Same best-effort contract as
    every other function here: a non-RDS host (self-managed PostgreSQL on EC2), a
    cross-account endpoint, or a missing ``rds:DescribeDBInstances`` yields ``None`` and the
    caller falls back to the manual field -- the flow this feature had before it existed.
    """
    info = describe_source_instance(rds_client, endpoint)
    if info is None or not info.vpc_id:
        return None
    return info


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


def source_vpc_mismatch_warning(
    entered_vpc_id: Optional[str], source: Optional["SourceInstanceInfo"]
) -> Optional[str]:
    """Warn when the CDC VpcId is not the source DB's VPC. ``None`` when it matches or is
    unknowable.

    Pure, so the decision is testable without AWS. A mismatch is NOT an error -- the CDC
    pipeline may legitimately run in another VPC reached by peering, Transit Gateway or
    PrivateLink -- but it is the shape of a typo, and today a wrong VpcId is only discovered
    after roughly twenty minutes of MSK deployment, leaving a failed stack to clean up. So
    this warns BEFORE Deploy and says what to confirm, rather than blocking.

    Silent (``None``) whenever the answer is not known: no VpcId typed yet, or no source
    network resolved (a non-RDS host, a cross-account endpoint, a missing
    ``rds:DescribeDBInstances``). Same best-effort contract as the rest of this module --
    absence of information must never manufacture a warning.
    """
    entered = (entered_vpc_id or "").strip()
    if not entered or source is None:
        return None
    source_vpc = (source.vpc_id or "").strip()
    if not source_vpc or source_vpc == entered:
        return None
    where = source.db_identifier or "the source database"
    return (
        f"The VPC you entered ({entered}) is not the source's: {where} is in "
        f"{source_vpc}. That is valid only if this VPC can actually reach the source -- "
        "VPC peering, a Transit Gateway attachment or PrivateLink, with routes and "
        "security groups to match. If it cannot, the connectors will fail to reach the "
        "source AFTER the MSK cluster has been created. Confirm the path, or use "
        f"{source_vpc}."
    )


def source_inbound_rule_hint(
    source: Optional["SourceInstanceInfo"], *, port: Optional[int]
) -> Optional[str]:
    """The INBOUND rule the operator must open on THEIR source DB, or ``None``. Pure.

    WHY this has to be said, and said BEFORE Deploy: the cdc-stack can only ever add rules
    to its OWN connector security group. Its source-facing rule is EGRESS (the stack has no
    authority over the customer's database SG, and the tool will not modify a customer
    resource), and the template's own comment concedes that "the route table + the source SG
    inbound rules still gate what is actually reachable". So the reciprocal ingress is the
    operator's job -- and nothing told them. When it is missing, the Debezium worker cannot
    open a connection and the only signal is a connector that never reaches RUNNING, after a
    billable MSK Serverless cluster and both connectors already exist.

    Deliberately NOT a block and NOT a probe. The rule is frequently already satisfied (a
    VPC-wide or broadly-open source SG), and proving otherwise would need to read the
    customer's security groups and then trust the result -- new IAM, new machinery, and a
    false block, which is the worse defect. This is an ``info``: it states the exact rule,
    with the source's own SG ids filled in, so it can be checked in one look.

    Silent whenever the answer is not known (no source resolved, no SG ids, no port), the
    same best-effort contract as the rest of this module: absence of information must never
    manufacture advice the operator cannot act on.
    """
    if source is None or not port:
        return None
    groups = tuple(g for g in (source.security_group_ids or ()) if g)
    if not groups:
        return None
    where = source.db_identifier or "the source database"
    listed = ", ".join(groups)
    plural = "groups" if len(groups) > 1 else "group"
    return (
        f"The connectors run in this VPC on the cdc-stack's own security group, and the "
        f"stack can only open its own OUTBOUND rule -- the matching INBOUND rule on your "
        f"database is yours to set. Confirm that {where}'s security {plural} ({listed}) "
        f"allows inbound TCP {port} from the connector subnets; allowing the VPC's CIDR is "
        f"the simplest rule that works. If it is already open (a VPC-wide rule, for "
        f"example) there is nothing to do. Without it the connectors are created, billed, "
        f"and then never reach RUNNING."
    )


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
    "fetch_source_network",
    "source_vpc_mismatch_warning",
    "fetch_source_security_group_id",
    "build_rds_client",
]
