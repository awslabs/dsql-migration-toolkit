# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for MskConnectController (CDC connector control + monitoring).

Read-only status (list/describe), CloudWatch lag, and the single guarded
mutation (delete_connector). A fake boto3 session is injected so no AWS is
reached; all read methods fail closed on error, and region is forwarded to every
client. Mirrors the fake-session pattern in tests/test_prerequisite_probes.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dsql_migrator.core.cdc import ConnectorState
from dsql_migrator.core.msk_connect_controller import (
    MskConnectController,
    target_lag_seconds,
)


class _FakeClient:
    """Fake kafkaconnect / cloudwatch client returning canned responses."""

    def __init__(self, responses: dict[str, Any], *, raise_on: str | None = None):
        self._responses = responses
        self._raise_on = raise_on
        self.calls: list[tuple[str, dict]] = []

    def list_connectors(self, **kwargs: Any) -> Any:
        self.calls.append(("list_connectors", kwargs))
        if self._raise_on == "list_connectors":
            raise RuntimeError("boom")
        return self._responses.get("list_connectors", {})

    def describe_connector(self, **kwargs: Any) -> Any:
        self.calls.append(("describe_connector", kwargs))
        if self._raise_on == "describe_connector":
            raise RuntimeError("boom")
        return self._responses.get("describe_connector", {})

    def get_metric_data(self, **kwargs: Any) -> Any:
        self.calls.append(("get_metric_data", kwargs))
        if self._raise_on == "get_metric_data":
            raise RuntimeError("boom")
        return self._responses.get("get_metric_data", {})

    def list_metrics(self, **kwargs: Any) -> Any:
        self.calls.append(("list_metrics", kwargs))
        if self._raise_on == "list_metrics":
            raise RuntimeError("boom")
        return self._responses.get("list_metrics", {})

    def delete_connector(self, **kwargs: Any) -> Any:
        self.calls.append(("delete_connector", kwargs))
        if self._raise_on == "delete_connector":
            raise RuntimeError("boom")
        return self._responses.get("delete_connector", {})

    def filter_log_events(self, **kwargs: Any) -> Any:
        self.calls.append(("filter_log_events", kwargs))
        if self._raise_on == "filter_log_events":
            raise RuntimeError("boom")
        return self._responses.get("filter_log_events", {})


class _FakeSession:
    """Fake boto3.Session handing out a fixed client and recording region."""

    def __init__(self, client: _FakeClient):
        self._client = client
        self.client_calls: list[tuple[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClient:
        self.client_calls.append((service_name, kwargs.get("region_name")))
        return self._client


def _controller(client: _FakeClient, region: str = "us-east-1") -> MskConnectController:
    return MskConnectController(region, session=_FakeSession(client))


# ---------------------------------------------------------------------------
# list / describe
# ---------------------------------------------------------------------------


def test_list_connectors_returns_list() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "a"}, {"connectorName": "b"}]}}
    )
    assert len(_controller(client).list_connectors()) == 2


def test_list_connectors_empty_on_error() -> None:
    client = _FakeClient({}, raise_on="list_connectors")
    assert _controller(client).list_connectors() == []


def test_describe_connector_returns_response() -> None:
    client = _FakeClient({"describe_connector": {"connectorState": "RUNNING"}})
    assert _controller(client).describe_connector("arn:x")["connectorState"] == "RUNNING"


def test_describe_connector_none_on_error() -> None:
    client = _FakeClient({}, raise_on="describe_connector")
    assert _controller(client).describe_connector("arn:x") is None


# ---------------------------------------------------------------------------
# connector_statuses (state mapping)
# ---------------------------------------------------------------------------


def test_connector_statuses_maps_running() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "sink", "connectorState": "RUNNING"}]}}
    )
    statuses = _controller(client).connector_statuses(["sink"])
    assert len(statuses) == 1
    assert statuses[0].name == "sink"
    assert statuses[0].state is ConnectorState.RUNNING


def test_connector_statuses_maps_creating_to_unassigned() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "src", "connectorState": "CREATING"}]}}
    )
    statuses = _controller(client).connector_statuses(["src"])
    assert statuses[0].state is ConnectorState.UNASSIGNED


def test_connector_statuses_maps_failed() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "src", "connectorState": "FAILED"}]}}
    )
    assert _controller(client).connector_statuses(["src"])[0].state is ConnectorState.FAILED


def test_connector_statuses_filters_by_name() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [
            {"connectorName": "wanted", "connectorState": "RUNNING"},
            {"connectorName": "other", "connectorState": "RUNNING"},
        ]}}
    )
    statuses = _controller(client).connector_statuses(["wanted"])
    assert [s.name for s in statuses] == ["wanted"]


def test_connector_statuses_empty_names_makes_no_call() -> None:
    client = _FakeClient({"list_connectors": {"connectors": [{"connectorName": "x"}]}})
    assert _controller(client).connector_statuses([]) == []
    assert client.calls == []  # zero AWS calls when nothing requested


def test_connector_statuses_empty_on_error() -> None:
    client = _FakeClient({}, raise_on="list_connectors")
    assert _controller(client).connector_statuses(["x"]) == []


# ---------------------------------------------------------------------------
# connector_health (CloudWatch task health/throughput)
# ---------------------------------------------------------------------------


def test_health_reads_running_errored_send_and_poll_rate() -> None:
    # Query ids are m{connectorIndex}_{metricIndex}:
    # 0=running, 1=errored, 2=send_rate, 3=poll_rate.
    client = _FakeClient(
        {"get_metric_data": {"MetricDataResults": [
            {"Id": "m0_0", "Values": [2.0]},
            {"Id": "m0_1", "Values": [0.0]},
            {"Id": "m0_2", "Values": [12.5]},
            {"Id": "m0_3", "Values": [3.0]},
        ]}}
    )
    health = _controller(client).connector_health(["sink"])
    assert health["sink"].running_tasks == 2
    assert health["sink"].errored_tasks == 0
    assert health["sink"].send_rate == 12.5
    assert health["sink"].poll_rate == 3.0


def test_health_poll_rate_unknown_when_absent() -> None:
    # No poll_rate datapoint -> stays None (never coerced to 0).
    client = _FakeClient(
        {"get_metric_data": {"MetricDataResults": [{"Id": "m0_2", "Values": [1.0]}]}}
    )
    assert _controller(client).connector_health(["sink"])["sink"].poll_rate is None


def test_health_unknown_when_no_datapoints() -> None:
    client = _FakeClient(
        {"get_metric_data": {"MetricDataResults": [{"Id": "m0_0", "Values": []}]}}
    )
    h = _controller(client).connector_health(["sink"])["sink"]
    assert h.running_tasks is None
    assert h.errored_tasks is None
    assert h.send_rate is None


def test_health_unknown_on_error() -> None:
    client = _FakeClient({}, raise_on="get_metric_data")
    h = _controller(client).connector_health(["sink"])["sink"]
    assert h.running_tasks is None


def test_health_empty_names_makes_no_call() -> None:
    client = _FakeClient({})
    assert _controller(client).connector_health([]) == {}


def _net_rows_responses(list_tables: list[str], data: dict[str, list[float]]) -> dict:
    """Canned ListMetrics (discovered Table dims) + GetMetricData (n{i} -> values)."""
    return {
        "list_metrics": {
            "Metrics": [
                {"Dimensions": [{"Name": "Stack", "Value": "stk"},
                                {"Name": "Table", "Value": t}]}
                for t in list_tables
            ]
        },
        "get_metric_data": {
            "MetricDataResults": [
                {"Id": f"n{i}", "Values": vals} for i, vals in enumerate(data.values())
            ]
        },
    }


def test_net_rows_by_table_exact_match_cluster_mode() -> None:
    # Cluster mode: the tool's names are already schema-qualified and match the
    # sink's Table dimension exactly. Query ids are n{i}; per-commit deltas summed.
    client = _FakeClient(
        _net_rows_responses(
            ["cdc_demo.orders", "cdc_demo.customers"],
            {"cdc_demo.orders": [5.0, 3.0], "cdc_demo.customers": []},
        )
    )
    got = _controller(client).net_rows_by_table(
        "stk", ["cdc_demo.orders", "cdc_demo.customers"]
    )
    assert got == {"cdc_demo.orders": 8}


def test_net_rows_by_table_suffix_match_single_db_mode() -> None:
    # Single-db mode: the tool uses bare names ("orders") but the sink always
    # publishes schema-qualified ("ecommerce_demo.orders"). The bare name must still
    # match the one published value (this is the whole point of discover-then-match).
    client = _FakeClient(
        _net_rows_responses(["ecommerce_demo.orders"], {"orders": [10.0, -2.0]})
    )
    got = _controller(client).net_rows_by_table("stk", ["orders"])
    assert got == {"orders": 8}
    # It discovered via ListMetrics before querying data.
    assert [c[0] for c in client.calls] == ["list_metrics", "get_metric_data"]


def test_net_rows_by_table_ambiguous_bare_name_skipped() -> None:
    # A bare name matching two schemas' tables is ambiguous -> skipped (falls back to
    # COUNT) rather than attributing another schema's rows to it.
    client = _FakeClient(
        {"list_metrics": {"Metrics": [
            {"Dimensions": [{"Name": "Table", "Value": "a.orders"}]},
            {"Dimensions": [{"Name": "Table", "Value": "b.orders"}]},
        ]}}
    )
    got = _controller(client).net_rows_by_table("stk", ["orders"])
    assert got == {}
    # No data query when nothing matched unambiguously.
    assert [c[0] for c in client.calls] == ["list_metrics"]


def test_net_rows_by_table_empty_when_nothing_published() -> None:
    # No metrics discovered -> return {} without a GetMetricData call.
    client = _FakeClient({"list_metrics": {"Metrics": []}})
    assert _controller(client).net_rows_by_table("stk", ["orders"]) == {}
    assert [c[0] for c in client.calls] == ["list_metrics"]


def test_net_rows_by_table_empty_on_error() -> None:
    # Fail closed on either the discovery or the data read.
    assert _controller(_FakeClient({}, raise_on="list_metrics")).net_rows_by_table(
        "stk", ["orders"]
    ) == {}
    client = _FakeClient(
        {"list_metrics": {"Metrics": [
            {"Dimensions": [{"Name": "Table", "Value": "orders"}]}]}},
        raise_on="get_metric_data",
    )
    assert client and _controller(client).net_rows_by_table("stk", ["orders"]) == {}


def test_net_rows_by_table_no_call_without_stack_or_tables() -> None:
    client = _FakeClient({"list_metrics": {"Metrics": []}})
    assert _controller(client).net_rows_by_table("", ["orders"]) == {}
    assert _controller(client).net_rows_by_table("stk", []) == {}
    assert client.calls == []  # never hit CloudWatch


def _lag_responses(list_tables: list[str], values_by_table: dict[str, list[float]]) -> dict:
    """ListMetrics (discovered dims) + GetMetricData (l{i} -> lag-ms values, newest first)."""
    return {
        "list_metrics": {
            "Metrics": [
                {"Dimensions": [{"Name": "Stack", "Value": "stk"},
                                {"Name": "Table", "Value": t}]}
                for t in list_tables
            ]
        },
        "get_metric_data": {
            "MetricDataResults": [
                {"Id": f"l{i}", "Values": vals}
                for i, vals in enumerate(values_by_table.values())
            ]
        },
    }


def test_replication_lag_by_table_takes_most_recent_ms() -> None:
    # Stat=Maximum, ScanBy=TimestampDescending -> Values[0] is the newest minute's
    # worst lag (current lag), rounded to int ms. Idle table (no datapoint) -> absent.
    client = _FakeClient(_lag_responses(
        ["ecommerce_demo.orders", "ecommerce_demo.customers"],
        {"ecommerce_demo.orders": [8500.0, 12000.0], "ecommerce_demo.customers": []},
    ))
    got = _controller(client).replication_lag_by_table(
        "stk", ["ecommerce_demo.orders", "ecommerce_demo.customers"])
    assert got == {"ecommerce_demo.orders": 8500}  # newest value; customers idle -> absent
    assert [c[0] for c in client.calls] == ["list_metrics", "get_metric_data"]
    lm = [c for c in client.calls if c[0] == "list_metrics"][0][1]
    assert lm["MetricName"] == "ReplicationLagMs"


def test_replication_lag_by_table_suffix_match_single_db() -> None:
    # Single-db bare name resolves to the sink's schema-qualified Table dimension.
    client = _FakeClient(_lag_responses(["ecommerce_demo.orders"], {"orders": [3200.0]}))
    assert _controller(client).replication_lag_by_table("stk", ["orders"]) == {"orders": 3200}


def test_replication_lag_by_table_empty_on_error() -> None:
    assert _controller(_FakeClient({}, raise_on="list_metrics")).replication_lag_by_table(
        "stk", ["orders"]) == {}


def test_replication_lag_by_table_no_call_without_stack_or_tables() -> None:
    client = _FakeClient({"list_metrics": {"Metrics": []}})
    assert _controller(client).replication_lag_by_table("", ["orders"]) == {}
    assert _controller(client).replication_lag_by_table("stk", []) == {}
    assert client.calls == []


def test_match_metric_tables_prefers_exact_then_unambiguous_bare() -> None:
    from dsql_migrator.core.msk_connect_controller import _match_metric_tables

    discovered = ["cdc_demo.orders", "cdc_demo.customers", "other.orders"]
    # Exact qualified name -> itself.
    assert _match_metric_tables(["cdc_demo.customers"], discovered) == {
        "cdc_demo.customers": "cdc_demo.customers"
    }
    # Bare "customers" is unambiguous -> the one qualified value.
    assert _match_metric_tables(["customers"], discovered) == {
        "customers": "cdc_demo.customers"
    }
    # Bare "orders" is ambiguous (two schemas) -> skipped.
    assert _match_metric_tables(["orders"], discovered) == {}
    # Unknown table -> skipped.
    assert _match_metric_tables(["nope"], discovered) == {}


# ---------------------------------------------------------------------------
# target_lag_seconds (DSQL end-to-end lag)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, row: Any, *, raise_on_execute: bool = False):
        self._row = row
        self._raise = raise_on_execute
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if self._raise:
            raise RuntimeError("boom")

    def fetchone(self) -> Any:
        return self._row


_NOW = datetime(2026, 6, 22, 12, 0, 30, tzinfo=timezone.utc)


def test_target_lag_computes_seconds_behind() -> None:
    max_ts = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    cur = _FakeCursor((max_ts,))
    lag = target_lag_seconds(cur, "cdc_demo.orders", "created_at", now=_NOW)
    assert lag == 30.0
    # Schema-qualified table is quoted part-wise.
    assert 'FROM "cdc_demo"."orders"' in cur.executed[0]
    assert 'max("created_at")' in cur.executed[0]


def test_target_lag_naive_timestamp_treated_as_utc() -> None:
    cur = _FakeCursor((datetime(2026, 6, 22, 12, 0, 0),))  # naive
    assert target_lag_seconds(cur, "public.heartbeat", "ts", now=_NOW) == 30.0


def test_target_lag_none_when_empty_table() -> None:
    cur = _FakeCursor((None,))
    assert target_lag_seconds(cur, "cdc_demo.orders", "created_at", now=_NOW) is None


def test_target_lag_none_on_query_error() -> None:
    cur = _FakeCursor((None,), raise_on_execute=True)
    assert target_lag_seconds(cur, "cdc_demo.orders", "created_at", now=_NOW) is None


def test_target_lag_never_negative() -> None:
    # A target timestamp slightly ahead of now (clock skew) clamps to 0.
    future = datetime(2026, 6, 22, 12, 1, 0, tzinfo=timezone.utc)
    cur = _FakeCursor((future,))
    assert target_lag_seconds(cur, "cdc_demo.orders", "created_at", now=_NOW) == 0.0


# ---------------------------------------------------------------------------
# delete_connector (guarded mutation)
# ---------------------------------------------------------------------------


def test_delete_connector_true_on_success() -> None:
    client = _FakeClient({"delete_connector": {}})
    assert _controller(client).delete_connector("arn:x", "v1") is True
    assert client.calls[0][0] == "delete_connector"
    assert client.calls[0][1]["connectorArn"] == "arn:x"
    assert client.calls[0][1]["currentVersion"] == "v1"


def test_delete_connector_false_on_error() -> None:
    client = _FakeClient({}, raise_on="delete_connector")
    assert _controller(client).delete_connector("arn:x", "v1") is False


# ---------------------------------------------------------------------------
# region forwarding
# ---------------------------------------------------------------------------


def test_region_forwarded_to_kafkaconnect_client() -> None:
    client = _FakeClient({"list_connectors": {"connectors": []}})
    session = _FakeSession(client)
    MskConnectController("eu-west-1", session=session).list_connectors()
    assert session.client_calls == [("kafkaconnect", "eu-west-1")]


def test_region_forwarded_to_cloudwatch_client() -> None:
    client = _FakeClient({"get_metric_data": {"MetricDataResults": []}})
    session = _FakeSession(client)
    MskConnectController("ap-southeast-2", session=session).connector_health(["x"])
    assert session.client_calls == [("cloudwatch", "ap-southeast-2")]


# ---------------------------------------------------------------------------
# dlq_errors: CloudWatch DLQ log read (parse + cursor de-dup + fail-closed)
# ---------------------------------------------------------------------------


def test_dlq_errors_parses_new_events_and_dedups() -> None:
    events = {
        "filter_log_events": {
            "events": [
                {
                    "eventId": "e1",
                    "timestamp": 1000,
                    "message": (
                        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, "
                        "partition=0, offset=42): apply failed (sqlstate=42804)"
                    ),
                },
                {"eventId": "e2", "timestamp": 2000, "message": "routine INFO line"},
                {
                    "eventId": "e3",
                    "timestamp": 3000,
                    "message": (
                        "Dropping unapplicable record (no DLQ configured) "
                        "topic=dsqlcdc.shop.payments, partition=1, offset=7: bad type"
                    ),
                },
            ]
        }
    }
    client = _FakeClient(events)
    controller = _controller(client)

    first = controller.dlq_errors("/msk-connect/mysql-dsql-cdc-stack-cdc")
    assert [e.table for e in first] == ["orders", "payments"]  # INFO noise skipped
    assert first[0].error_code == "42804"
    assert "offset=42" in first[0].message

    # Same events re-returned by the fake, but the eventId de-dup yields nothing.
    second = controller.dlq_errors("/msk-connect/mysql-dsql-cdc-stack-cdc")
    assert second == []


def test_dlq_errors_fail_closed_on_access_error() -> None:
    client = _FakeClient({}, raise_on="filter_log_events")
    assert _controller(client).dlq_errors("/msk-connect/x-cdc") == []
