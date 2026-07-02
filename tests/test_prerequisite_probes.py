"""Unit tests for the session-wired prerequisite probes (UI adapter layer).

Focus on :class:`SessionMskProbe`: it must report MSK / MSK Connect availability
from the target region's real state via read-only ``kafka:ListClustersV2`` /
``kafkaconnect:ListConnectors`` calls, and fail closed (``False``) on any access
error -- without ever reaching AWS in the test (a fake ``boto3.Session`` is
injected). Property 1 (read-only) and Property 7 (no credential values) hold:
the probe only lists, and only the non-secret region/profile flow through.
"""

from __future__ import annotations

from typing import Any

from dsql_migrator.ui.prerequisite_probes import (
    SessionMskProbe,
    UnavailableMskProbe,
)


class _FakeClient:
    """A fake MSK / MSK Connect client returning canned list responses."""

    def __init__(self, responses: dict[str, Any], *, raise_on: str | None = None):
        self._responses = responses
        self._raise_on = raise_on
        self.calls: list[str] = []

    def list_clusters_v2(self, **_kwargs: Any) -> Any:
        self.calls.append("list_clusters_v2")
        if self._raise_on == "list_clusters_v2":
            raise RuntimeError("boom")
        return self._responses.get("list_clusters_v2", {})

    def list_connectors(self, **_kwargs: Any) -> Any:
        self.calls.append("list_connectors")
        if self._raise_on == "list_connectors":
            raise RuntimeError("boom")
        return self._responses.get("list_connectors", {})


class _FakeSession:
    """Fake ``boto3.Session`` that hands out a fixed client and records region."""

    def __init__(self, client: _FakeClient):
        self._client = client
        self.client_calls: list[tuple[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClient:
        self.client_calls.append((service_name, kwargs.get("region_name")))
        return self._client


def _probe(client: _FakeClient, region: str = "us-east-1") -> SessionMskProbe:
    return SessionMskProbe(region, session=_FakeSession(client))


# ---------------------------------------------------------------------------
# cluster_available
# ---------------------------------------------------------------------------


def test_cluster_available_true_when_active_cluster_present() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"ClusterName": "c", "State": "ACTIVE"}]}}
    )
    assert _probe(client).cluster_available() is True
    # Read-only: only the list call was made.
    assert client.calls == ["list_clusters_v2"]


def test_cluster_available_false_when_cluster_not_active() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"State": "CREATING"}]}}
    )
    assert _probe(client).cluster_available() is False


def test_cluster_available_false_when_no_clusters() -> None:
    client = _FakeClient({"list_clusters_v2": {"ClusterInfoList": []}})
    assert _probe(client).cluster_available() is False


def test_cluster_available_false_on_aws_error() -> None:
    client = _FakeClient({}, raise_on="list_clusters_v2")
    assert _probe(client).cluster_available() is False


def test_cluster_available_passes_region_to_client() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"State": "ACTIVE"}]}}
    )
    session = _FakeSession(client)
    probe = SessionMskProbe("eu-west-1", session=session)
    probe.cluster_available()
    assert session.client_calls == [("kafka", "eu-west-1")]


# ---------------------------------------------------------------------------
# connect_available
# ---------------------------------------------------------------------------


def test_connect_available_true_when_a_connector_exists() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "sink", "connectorState": "RUNNING"}]}}
    )
    assert _probe(client).connect_available() is True
    assert client.calls == ["list_connectors"]


def test_connect_available_false_when_no_connectors() -> None:
    client = _FakeClient({"list_connectors": {"connectors": []}})
    assert _probe(client).connect_available() is False


def test_connect_available_false_on_aws_error() -> None:
    client = _FakeClient({}, raise_on="list_connectors")
    assert _probe(client).connect_available() is False


def test_connect_available_uses_kafkaconnect_client() -> None:
    client = _FakeClient({"list_connectors": {"connectors": [{"connectorName": "x"}]}})
    session = _FakeSession(client)
    SessionMskProbe("us-east-1", session=session).connect_available()
    assert session.client_calls == [("kafkaconnect", "us-east-1")]


# ---------------------------------------------------------------------------
# UnavailableMskProbe fallback
# ---------------------------------------------------------------------------


def test_unavailable_probe_always_false() -> None:
    probe = UnavailableMskProbe()
    assert probe.cluster_available() is False
    assert probe.connect_available() is False
