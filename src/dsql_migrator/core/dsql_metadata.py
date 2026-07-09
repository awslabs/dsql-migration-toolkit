# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Best-effort target Aurora DSQL metadata for the overview diagram.

The DSQL cluster *endpoint* carries the cluster **id** (e.g. ``mycluster`` in
``mycluster.cluster-...dsql.<region>.on.aws``), but the human-friendly cluster
**name** is stored as the resource's ``Name`` tag, which is only available via
the DSQL control plane. This module derives the cluster id from the endpoint and
looks the ``Name`` tag up via ``GetCluster`` + ``ListTagsForResource``, sharing
the single ``boto3`` session/credential context (Requirements 9.5/9.7).

Everything is **best effort**: a missing ``dsql:GetCluster`` /
``dsql:ListTagsForResource`` permission, an untagged cluster, or any API error
yields ``None`` so the diagram simply falls back to the cluster id. No
credentials are read or logged here.
"""

from __future__ import annotations

from typing import Optional

# Marker separating the cluster id from the rest of a DSQL endpoint, e.g.
# ``<cluster-id>.dsql.<region>.on.aws``.
_DSQL_ENDPOINT_MARKER = ".dsql."


def parse_dsql_cluster_id(endpoint: str) -> Optional[str]:
    """Return the cluster id (the leading label) from a DSQL endpoint.

    ``mycluster.cluster-abc123.us-east-1.rds.amazonaws.com`` or
    ``abc123.dsql.us-east-1.on.aws`` -> the label before ``.dsql.`` (or the
    first label otherwise). Returns ``None`` for an empty endpoint.
    """
    host = (endpoint or "").strip()
    if not host:
        return None
    head, marker, _rest = host.partition(_DSQL_ENDPOINT_MARKER)
    cluster_id = head if marker else host.split(".", 1)[0]
    return cluster_id or None


def _normalize_tags(raw: object) -> dict:
    """Coerce a tags payload (dict or list of {Key,Value}) into a plain dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        return {
            (item.get("Key") or item.get("key")): (
                item.get("Value") or item.get("value")
            )
            for item in raw
            if isinstance(item, dict)
        }
    return {}


def fetch_dsql_cluster_name(dsql_client: object, endpoint: str) -> Optional[str]:
    """Look up the DSQL cluster's ``Name`` tag (best effort; None on any miss).

    Resolves the cluster ARN via ``GetCluster`` then reads its tags via
    ``ListTagsForResource``, returning the ``Name`` tag value. Returns ``None``
    on any failure (missing permission, untagged, not found) so the caller can
    fall back to the cluster id silently.
    """
    cluster_id = parse_dsql_cluster_id(endpoint)
    if not cluster_id:
        return None
    try:
        info = dsql_client.get_cluster(identifier=cluster_id)  # type: ignore[attr-defined]
        arn = info.get("arn") or info.get("Arn")
        if not arn:
            return None
        response = dsql_client.list_tags_for_resource(resourceArn=arn)  # type: ignore[attr-defined]
        tags = _normalize_tags(response.get("tags") or response.get("Tags"))
        name = tags.get("Name")
        return name or None
    except Exception:  # noqa: BLE001 - metadata is optional, never fatal
        return None


def fetch_dsql_cluster_arn(dsql_client: object, endpoint: str) -> Optional[str]:
    """Resolve the DSQL cluster ARN from its endpoint (best effort; None on miss).

    Parses the cluster id from the endpoint then calls ``GetCluster``. Returns the
    cluster ARN, or ``None`` on any failure (missing permission, not found, bad
    endpoint) so the caller can fall back to a manual ARN entry silently. Used to
    auto-fill the BYO-VPC infra form's ``DsqlClusterArn`` (not derivable from the
    endpoint hostname alone).
    """
    cluster_id = parse_dsql_cluster_id(endpoint)
    if not cluster_id:
        return None
    try:
        info = dsql_client.get_cluster(identifier=cluster_id)  # type: ignore[attr-defined]
        arn = info.get("arn") or info.get("Arn")
        return arn or None
    except Exception:  # noqa: BLE001 - metadata is optional, never fatal
        return None


def build_dsql_client(aws_profile: Optional[str], region: Optional[str]) -> object:
    """Build a DSQL client from the shared session (honoring the global profile)."""
    from dsql_migrator.core.aws_session import build_session

    return build_session(aws_profile).client("dsql", region_name=region)


__all__ = [
    "parse_dsql_cluster_id",
    "fetch_dsql_cluster_name",
    "fetch_dsql_cluster_arn",
    "build_dsql_client",
]
