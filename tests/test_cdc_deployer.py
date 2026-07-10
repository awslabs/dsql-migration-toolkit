# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for CdcStackDeployer (cdc-stack CloudFormation updater).

Read-only discovery + the single guarded update_stack mutation, all driven
through a fake boto3 session so no AWS is reached. Mirrors the fake-session
pattern in tests/test_msk_connect_controller.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dsql_migrator.core.cdc_deployer import (
    CdcDeployError,
    CdcStackDeployer,
)


class _FakeClient:
    def __init__(self, responses: dict[str, Any], *, raise_on: dict[str, Exception] | None = None):
        self._responses = responses
        self._raise_on = raise_on or {}
        self.calls: list[tuple[str, dict]] = []

    def _maybe_raise(self, op: str) -> None:
        if op in self._raise_on:
            raise self._raise_on[op]

    def describe_stacks(self, **kw: Any) -> Any:
        self.calls.append(("describe_stacks", kw))
        self._maybe_raise("describe_stacks")
        # A list under "describe_stacks" is a scripted sequence of responses (one
        # per call, last repeats) so a test can model a status transition (e.g.
        # UPDATE_ROLLBACK_FAILED -> UPDATE_ROLLBACK_COMPLETE across the recover flow).
        resp = self._responses.get("describe_stacks", {})
        if isinstance(resp, list):
            idx = min(
                sum(1 for c in self.calls if c[0] == "describe_stacks") - 1,
                len(resp) - 1,
            )
            return resp[idx]
        return resp

    def describe_stack_resources(self, **kw: Any) -> Any:
        self.calls.append(("describe_stack_resources", kw))
        self._maybe_raise("describe_stack_resources")
        return self._responses.get("describe_stack_resources", {})

    def continue_update_rollback(self, **kw: Any) -> Any:
        self.calls.append(("continue_update_rollback", kw))
        self._maybe_raise("continue_update_rollback")
        return self._responses.get("continue_update_rollback", {})

    def describe_network_interfaces(self, **kw: Any) -> Any:
        self.calls.append(("describe_network_interfaces", kw))
        self._maybe_raise("describe_network_interfaces")
        return self._responses.get("describe_network_interfaces", {})

    def delete_network_interface(self, **kw: Any) -> Any:
        self.calls.append(("delete_network_interface", kw))
        self._maybe_raise("delete_network_interface")
        return self._responses.get("delete_network_interface", {})

    def update_stack(self, **kw: Any) -> Any:
        self.calls.append(("update_stack", kw))
        self._maybe_raise("update_stack")
        return self._responses.get("update_stack", {})

    def describe_stack_events(self, **kw: Any) -> Any:
        self.calls.append(("describe_stack_events", kw))
        self._maybe_raise("describe_stack_events")
        return self._responses.get("describe_stack_events", {})

    def list_connectors(self, **kw: Any) -> Any:
        self.calls.append(("list_connectors", kw))
        self._maybe_raise("list_connectors")
        return self._responses.get("list_connectors", {})

    def create_stack(self, **kw: Any) -> Any:
        self.calls.append(("create_stack", kw))
        self._maybe_raise("create_stack")
        return self._responses.get("create_stack", {})

    def put_object(self, **kw: Any) -> Any:
        self.calls.append(("put_object", kw))
        self._maybe_raise("put_object")
        return self._responses.get("put_object", {})

    def delete_stack(self, **kw: Any) -> Any:
        self.calls.append(("delete_stack", kw))
        self._maybe_raise("delete_stack")
        return self._responses.get("delete_stack", {})

    def get_bootstrap_brokers(self, **kw: Any) -> Any:
        self.calls.append(("get_bootstrap_brokers", kw))
        self._maybe_raise("get_bootstrap_brokers")
        return self._responses.get("get_bootstrap_brokers", {})


class _FakeSession:
    def __init__(self, client: _FakeClient):
        self._client = client
        self.client_calls: list[tuple[str, Any]] = []

    def client(self, service_name: str, **kw: Any) -> _FakeClient:
        self.client_calls.append((service_name, kw.get("region_name")))
        return self._client


def _dep(client: _FakeClient) -> CdcStackDeployer:
    return CdcStackDeployer("us-east-1", session=_FakeSession(client))


def _stack(status="UPDATE_COMPLETE", params=None, outputs=None):
    stack: dict[str, Any] = {
        "StackStatus": status,
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in (params or {}).items()
        ],
    }
    if outputs is not None:
        stack["Outputs"] = [
            {"OutputKey": k, "OutputValue": v} for k, v in outputs.items()
        ]
    return {"Stacks": [stack]}


# ---------------------------------------------------------------------------
# discover_stack
# ---------------------------------------------------------------------------


def test_discover_ok_stable_stack() -> None:
    client = _FakeClient({"describe_stacks": _stack(params={"VpcId": "vpc-1"})})
    disc = _dep(client).discover_stack("mysql-dsql-cdc-stack")
    assert disc.is_stable is True
    assert disc.current_parameters["VpcId"] == "vpc-1"


def test_discover_missing_stack_raises() -> None:
    client = _FakeClient({}, raise_on={"describe_stacks": RuntimeError("not found")})
    with pytest.raises(CdcDeployError):
        _dep(client).discover_stack("nope")


def test_discover_unstable_state_raises() -> None:
    client = _FakeClient({"describe_stacks": _stack(status="UPDATE_IN_PROGRESS")})
    with pytest.raises(CdcDeployError):
        _dep(client).discover_stack("mysql-dsql-cdc-stack")


def test_discover_rejects_unfilled_placeholder() -> None:
    client = _FakeClient(
        {"describe_stacks": _stack(params={"VpcId": "<FILL_ME: VpcId — your VPC>"})}
    )
    with pytest.raises(CdcDeployError):
        _dep(client).discover_stack("mysql-dsql-cdc-stack")


def test_discover_auto_recovers_rollback_failed() -> None:
    # A wedged UPDATE_ROLLBACK_FAILED stack auto-recovers: discover_stack continues
    # the rollback skipping the stuck resource, then returns the now-stable stack.
    client = _FakeClient(
        {
            # 1st read: FAILED. Recover polls status; then final read is COMPLETE.
            "describe_stacks": [
                _stack(status="UPDATE_ROLLBACK_FAILED", params={"VpcId": "vpc-1"}),
                _stack(status="UPDATE_ROLLBACK_COMPLETE", params={"VpcId": "vpc-1"}),
                _stack(status="UPDATE_ROLLBACK_COMPLETE", params={"VpcId": "vpc-1"}),
            ],
            "describe_stack_resources": {
                "StackResources": [
                    {
                        "LogicalResourceId": "DebeziumSourceConnector",
                        "ResourceStatus": "UPDATE_FAILED",
                    },
                    {
                        "LogicalResourceId": "MskCluster",
                        "ResourceStatus": "UPDATE_COMPLETE",
                    },
                ]
            },
        }
    )
    disc = _dep(client).discover_stack("mysql-dsql-cdc-stack")
    assert disc.is_stable is True
    # It skipped exactly the FAILED resource when continuing the rollback.
    cont = [c for c in client.calls if c[0] == "continue_update_rollback"]
    assert cont, "expected continue_update_rollback to be called"
    assert cont[0][1].get("ResourcesToSkip") == ["DebeziumSourceConnector"]


def test_discover_rollback_failed_stays_failed_when_recovery_errors() -> None:
    # If continue_update_rollback itself errors, discovery surfaces the normal
    # not-stable error rather than silently proceeding.
    client = _FakeClient(
        {
            "describe_stacks": _stack(status="UPDATE_ROLLBACK_FAILED"),
            "describe_stack_resources": {"StackResources": []},
        },
        raise_on={"continue_update_rollback": RuntimeError("no can do")},
    )
    with pytest.raises(CdcDeployError):
        _dep(client).discover_stack("mysql-dsql-cdc-stack")


# ---------------------------------------------------------------------------
# submit_update
# ---------------------------------------------------------------------------


def test_submit_update_overrides_and_carries_forward() -> None:
    client = _FakeClient(
        {"describe_stacks": _stack(params={"VpcId": "vpc-1", "TableIncludeList": "old"})}
    )
    changed = _dep(client).submit_update(
        "mysql-dsql-cdc-stack", [("TableIncludeList", "cdc_demo.orders")]
    )
    assert changed is True
    update_call = next(c for c in client.calls if c[0] == "update_stack")
    params = {p["ParameterKey"]: p for p in update_call[1]["Parameters"]}
    # Tool-known override carries a new value...
    assert params["TableIncludeList"]["ParameterValue"] == "cdc_demo.orders"
    # ...infra param is carried forward unchanged.
    assert params["VpcId"].get("UsePreviousValue") is True
    assert update_call[1]["UsePreviousTemplate"] is True


def test_submit_update_no_changes_returns_false() -> None:
    client = _FakeClient(
        {"describe_stacks": _stack(params={"TableIncludeList": "x"})},
        raise_on={"update_stack": RuntimeError("No updates are to be performed.")},
    )
    assert _dep(client).submit_update("mysql-dsql-cdc-stack", [("TableIncludeList", "x")]) is False


def test_submit_update_other_error_raises() -> None:
    client = _FakeClient(
        {"describe_stacks": _stack(params={})},
        raise_on={"update_stack": RuntimeError("AccessDenied")},
    )
    with pytest.raises(CdcDeployError):
        _dep(client).submit_update("mysql-dsql-cdc-stack", [("TableIncludeList", "x")])


# ---------------------------------------------------------------------------
# poll_events
# ---------------------------------------------------------------------------


def test_poll_events_chronological_and_after_since() -> None:
    t0 = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    events = {
        "StackEvents": [  # API returns newest-first
            {"Timestamp": t0 + timedelta(seconds=20), "LogicalResourceId": "Sink", "ResourceStatus": "CREATE_COMPLETE"},
            {"Timestamp": t0 + timedelta(seconds=10), "LogicalResourceId": "Src", "ResourceStatus": "CREATE_IN_PROGRESS", "ResourceStatusReason": "Resource creation Initiated"},
            {"Timestamp": t0 - timedelta(seconds=5), "LogicalResourceId": "Old", "ResourceStatus": "STALE"},
        ]
    }
    client = _FakeClient({"describe_stack_events": events})
    out = _dep(client).poll_events("mysql-dsql-cdc-stack", since=t0)
    # Only events after `since`, oldest-first.
    assert [m for _, m in out] == [
        "Src CREATE_IN_PROGRESS — Resource creation Initiated",
        "Sink CREATE_COMPLETE",
    ]


def test_poll_events_empty_on_error() -> None:
    client = _FakeClient({}, raise_on={"describe_stack_events": RuntimeError("x")})
    assert _dep(client).poll_events("s", since=datetime.now(timezone.utc)) == []


# ---------------------------------------------------------------------------
# connector_state / stack_status
# ---------------------------------------------------------------------------


def test_connector_state_found() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "c", "connectorState": "RUNNING"}]}}
    )
    assert _dep(client).connector_state("c") == "RUNNING"


def test_connector_state_absent_is_none() -> None:
    client = _FakeClient({"list_connectors": {"connectors": []}})
    assert _dep(client).connector_state("c") is None


def test_connector_state_raises_on_read_error() -> None:
    # A read/API error must PROPAGATE (it was previously swallowed to None, which is
    # indistinguishable from "connector absent" and made a RUNNING-wait loop on
    # "creating…" forever with no surfaced cause). None is reserved for a connector
    # genuinely absent from a SUCCESSFULLY-read list.
    client = _FakeClient({}, raise_on={"list_connectors": RuntimeError("throttled")})
    with pytest.raises(RuntimeError):
        _dep(client).connector_state("c")


def test_stack_status_returns_value() -> None:
    client = _FakeClient({"describe_stacks": _stack(status="UPDATE_COMPLETE")})
    assert _dep(client).stack_status("s") == "UPDATE_COMPLETE"


def test_region_forwarded() -> None:
    client = _FakeClient({"describe_stacks": _stack()})
    session = _FakeSession(client)
    CdcStackDeployer("eu-west-1", session=session).discover_stack("s")
    assert session.client_calls[0] == ("cloudformation", "eu-west-1")


# ---------------------------------------------------------------------------
# describe_stack_or_none (non-raising existence probe)
# ---------------------------------------------------------------------------


def test_describe_stack_or_none_present() -> None:
    client = _FakeClient({"describe_stacks": _stack(params={"VpcId": "vpc-1"})})
    disc = _dep(client).describe_stack_or_none("mysql-dsql-cdc-stack")
    assert disc is not None
    assert disc.current_parameters["VpcId"] == "vpc-1"
    assert disc.is_stable is True


def test_describe_stack_or_none_absent_returns_none() -> None:
    client = _FakeClient(
        {},
        raise_on={
            "describe_stacks": RuntimeError(
                "Stack with id mysql-dsql-cdc-stack does not exist"
            )
        },
    )
    assert _dep(client).describe_stack_or_none("mysql-dsql-cdc-stack") is None


def test_describe_stack_or_none_empty_list_returns_none() -> None:
    client = _FakeClient({"describe_stacks": {"Stacks": []}})
    assert _dep(client).describe_stack_or_none("mysql-dsql-cdc-stack") is None


def test_describe_stack_or_none_unexpected_error_raises() -> None:
    client = _FakeClient(
        {}, raise_on={"describe_stacks": RuntimeError("AccessDenied")}
    )
    with pytest.raises(CdcDeployError):
        _dep(client).describe_stack_or_none("mysql-dsql-cdc-stack")


def test_describe_stack_or_none_unstable_marks_not_stable() -> None:
    client = _FakeClient({"describe_stacks": _stack(status="ROLLBACK_COMPLETE")})
    disc = _dep(client).describe_stack_or_none("mysql-dsql-cdc-stack")
    assert disc is not None and disc.is_stable is False
    assert disc.stack_status == "ROLLBACK_COMPLETE"


# ---------------------------------------------------------------------------
# create_stack
# ---------------------------------------------------------------------------


def test_create_stack_sends_full_params_and_named_iam() -> None:
    client = _FakeClient({"create_stack": {"StackId": "arn:stack"}})
    _dep(client).create_stack(
        "mysql-dsql-cdc-stack",
        "TEMPLATE_BODY",
        [("VpcId", "vpc-1"), ("DeploySink", "false")],
    )
    call = next(c for c in client.calls if c[0] == "create_stack")
    kw = call[1]
    assert kw["TemplateBody"] == "TEMPLATE_BODY"
    assert kw["Capabilities"] == ["CAPABILITY_NAMED_IAM"]
    # All parameters carry an explicit value (no UsePreviousValue on a create).
    params = {p["ParameterKey"]: p for p in kw["Parameters"]}
    assert params["VpcId"]["ParameterValue"] == "vpc-1"
    assert params["DeploySink"]["ParameterValue"] == "false"
    assert all("UsePreviousValue" not in p for p in kw["Parameters"])


def test_create_stack_oversize_template_uses_template_url() -> None:
    # A template over the 51,200-byte inline limit is staged in the plugin bucket
    # and passed as TemplateURL (not TemplateBody), with a put_object upload.
    client = _FakeClient({"create_stack": {}})
    dep = _dep(client)
    dep.template_s3_bucket = "mysql-dsql-migrator-plugins-acct-us-east-1"
    big_template = "x" * 60000
    dep.create_stack("mysql-dsql-cdc-stack", big_template, [("VpcId", "vpc-1")])
    put = next(c for c in client.calls if c[0] == "put_object")
    assert put[1]["Bucket"] == "mysql-dsql-migrator-plugins-acct-us-east-1"
    assert put[1]["Key"] == "cdc-plugins/cdc-stack.yaml"
    create = next(c for c in client.calls if c[0] == "create_stack")
    assert "TemplateBody" not in create[1]
    assert create[1]["TemplateURL"].endswith("cdc-plugins/cdc-stack.yaml")
    # Region-specific virtual-hosted endpoint (deployer region us-east-1), NOT the
    # global s3.amazonaws.com which would PermanentRedirect a non-us-east-1 bucket.
    assert create[1]["TemplateURL"] == (
        "https://mysql-dsql-migrator-plugins-acct-us-east-1.s3.us-east-1.amazonaws.com/"
        "cdc-plugins/cdc-stack.yaml"
    )


def test_create_stack_template_url_uses_deployer_region() -> None:
    # Cross-region portability: the TemplateURL must carry the deployer's region so
    # a Seoul (ap-northeast-2) bucket is addressed via its own regional endpoint.
    client = _FakeClient({"create_stack": {}})
    dep = CdcStackDeployer("ap-northeast-2", session=_FakeSession(client))
    dep.template_s3_bucket = "mysql-dsql-migrator-plugins-acct-ap-northeast-2"
    dep.create_stack("mysql-dsql-cdc-stack", "y" * 60000, [("VpcId", "vpc-1")])
    create = next(c for c in client.calls if c[0] == "create_stack")
    assert create[1]["TemplateURL"] == (
        "https://mysql-dsql-migrator-plugins-acct-ap-northeast-2.s3.ap-northeast-2."
        "amazonaws.com/cdc-plugins/cdc-stack.yaml"
    )


def test_create_stack_oversize_template_without_bucket_raises() -> None:
    client = _FakeClient({"create_stack": {}})
    dep = _dep(client)  # no template_s3_bucket set
    with pytest.raises(CdcDeployError) as exc:
        dep.create_stack("mysql-dsql-cdc-stack", "y" * 60000, [("VpcId", "vpc-1")])
    assert "inline limit" in str(exc.value)


def test_create_stack_already_exists_raises() -> None:
    client = _FakeClient(
        {}, raise_on={"create_stack": RuntimeError("AlreadyExistsException: exists")}
    )
    with pytest.raises(CdcDeployError) as exc:
        _dep(client).create_stack("mysql-dsql-cdc-stack", "T", [("VpcId", "vpc-1")])
    assert "already exists" in str(exc.value)


def test_create_stack_other_error_raises() -> None:
    client = _FakeClient(
        {}, raise_on={"create_stack": RuntimeError("ValidationError: bad")}
    )
    with pytest.raises(CdcDeployError):
        _dep(client).create_stack("mysql-dsql-cdc-stack", "T", [("VpcId", "vpc-1")])


def test_create_stack_uses_cloudformation_client() -> None:
    client = _FakeClient({"create_stack": {}})
    session = _FakeSession(client)
    CdcStackDeployer("us-east-1", session=session).create_stack(
        "s", "T", [("VpcId", "vpc-1")]
    )
    assert session.client_calls[0] == ("cloudformation", "us-east-1")


# ---------------------------------------------------------------------------
# delete_stack
# ---------------------------------------------------------------------------


def test_delete_stack_calls_api() -> None:
    client = _FakeClient({"delete_stack": {}})
    _dep(client).delete_stack("mysql-dsql-cdc-stack")
    call = next(c for c in client.calls if c[0] == "delete_stack")
    assert call[1]["StackName"] == "mysql-dsql-cdc-stack"


def test_delete_stack_error_raises() -> None:
    client = _FakeClient(
        {}, raise_on={"delete_stack": RuntimeError("AccessDenied")}
    )
    with pytest.raises(CdcDeployError):
        _dep(client).delete_stack("mysql-dsql-cdc-stack")


def test_recover_delete_failed_clears_enis_and_redeletes() -> None:
    # A DELETE_FAILED stack blocked by leftover (detached) offset-seeder ENIs on
    # the connector subnet auto-recovers: the available ENI is deleted, the delete
    # is re-issued (retaining the still-stuck resources), and the stack ends gone.
    client = _FakeClient(
        {
            "describe_stack_resources": {
                "StackResources": [
                    {
                        "LogicalResourceId": "ConnectorSubnetA",
                        "ResourceType": "AWS::EC2::Subnet",
                        "PhysicalResourceId": "subnet-abc",
                        "ResourceStatus": "DELETE_FAILED",
                    },
                    {
                        "LogicalResourceId": "ConnectorSecurityGroup",
                        "ResourceType": "AWS::EC2::SecurityGroup",
                        "PhysicalResourceId": "sg-xyz",
                        "ResourceStatus": "DELETE_FAILED",
                    },
                ]
            },
            "describe_network_interfaces": {
                "NetworkInterfaces": [
                    {"NetworkInterfaceId": "eni-1", "Status": "available"},
                    {"NetworkInterfaceId": "eni-2", "Status": "in-use"},
                ]
            },
            # After the re-delete the stack is gone -> describe_stacks raises.
            "describe_stacks": {},
        },
        raise_on={"describe_stacks": RuntimeError("does not exist")},
    )
    result = _dep(client).recover_delete_failed("mysql-dsql-cdc-stack")
    assert result == ""  # gone = recovered
    # Deleted only the detached ENI, never the in-use one.
    deleted = [c for c in client.calls if c[0] == "delete_network_interface"]
    assert [c[1]["NetworkInterfaceId"] for c in deleted] == ["eni-1"]
    # Re-issued delete retaining the still-stuck logical resources.
    redelete = next(c for c in client.calls if c[0] == "delete_stack")
    assert set(redelete[1].get("RetainResources", [])) == {
        "ConnectorSubnetA",
        "ConnectorSecurityGroup",
    }


def test_recover_delete_failed_best_effort_on_error() -> None:
    # If resource discovery errors, recovery returns DELETE_FAILED (caller surfaces
    # the normal failure) rather than raising.
    client = _FakeClient(
        {}, raise_on={"describe_stack_resources": RuntimeError("boom")}
    )
    assert _dep(client).recover_delete_failed("mysql-dsql-cdc-stack") == "DELETE_FAILED"


# ---------------------------------------------------------------------------
# get_stack_output / get_bootstrap_brokers
# ---------------------------------------------------------------------------


def test_get_stack_output_found() -> None:
    client = _FakeClient(
        {"describe_stacks": _stack(outputs={"MskClusterArn": "arn:msk"})}
    )
    assert _dep(client).get_stack_output("s", "MskClusterArn") == "arn:msk"


def test_get_stack_output_absent_is_none() -> None:
    client = _FakeClient({"describe_stacks": _stack(outputs={"Other": "x"})})
    assert _dep(client).get_stack_output("s", "MskClusterArn") is None


def test_get_stack_output_error_is_none() -> None:
    client = _FakeClient({}, raise_on={"describe_stacks": RuntimeError("boom")})
    assert _dep(client).get_stack_output("s", "MskClusterArn") is None


def test_get_bootstrap_brokers_returns_sasl_iam() -> None:
    client = _FakeClient(
        {"get_bootstrap_brokers": {"BootstrapBrokerStringSaslIam": "b-1:9098"}}
    )
    assert _dep(client).get_bootstrap_brokers("arn:msk") == "b-1:9098"


def test_get_bootstrap_brokers_uses_kafka_client() -> None:
    client = _FakeClient(
        {"get_bootstrap_brokers": {"BootstrapBrokerStringSaslIam": "b-1:9098"}}
    )
    session = _FakeSession(client)
    CdcStackDeployer("us-east-1", session=session).get_bootstrap_brokers("arn:msk")
    assert session.client_calls[0] == ("kafka", "us-east-1")
    call = next(c for c in client.calls if c[0] == "get_bootstrap_brokers")
    assert call[1]["ClusterArn"] == "arn:msk"


def test_get_bootstrap_brokers_empty_raises() -> None:
    client = _FakeClient({"get_bootstrap_brokers": {}})
    with pytest.raises(CdcDeployError):
        _dep(client).get_bootstrap_brokers("arn:msk")


def test_get_bootstrap_brokers_error_raises() -> None:
    client = _FakeClient(
        {}, raise_on={"get_bootstrap_brokers": RuntimeError("not active")}
    )
    with pytest.raises(CdcDeployError):
        _dep(client).get_bootstrap_brokers("arn:msk")


# ---------------------------------------------------------------------------
# build_cdc_stack_deployer: optional assume-role
# ---------------------------------------------------------------------------


def test_build_cdc_stack_deployer_without_assume_role_uses_profile() -> None:
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer

    dep = build_cdc_stack_deployer("us-east-1", aws_profile="myprofile")
    assert dep._aws_profile == "myprofile"
    assert dep._session is None  # no assumed session; builds on demand


def test_build_cdc_stack_deployer_with_assume_role_injects_session() -> None:
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer

    class _FakeSts:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def assume_role(self, **kw):
            self.calls.append(kw)
            return {
                "Credentials": {
                    "AccessKeyId": "A",
                    "SecretAccessKey": "S",
                    "SessionToken": "T",
                }
            }

    factory_calls: list[dict] = []

    class _Sess:
        def client(self, *a, **k):
            return object()

    def factory(**kw):
        factory_calls.append(kw)
        return _Sess()

    sts = _FakeSts()
    dep = build_cdc_stack_deployer(
        "us-east-1",
        assume_role_arn="arn:aws:iam::1:role/CdcDeploy",
        sts_client=sts,
        session_factory=factory,
    )
    # The deployer holds the assumed-role session; profile path is bypassed.
    assert dep._session is not None
    assert dep._aws_profile is None
    assert sts.calls[0]["RoleArn"] == "arn:aws:iam::1:role/CdcDeploy"
    # The session was built from the temp credentials.
    assert factory_calls[-1]["aws_session_token"] == "T"


def test_build_cdc_stack_deployer_none_assume_role_makes_no_sts_call() -> None:
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer

    # With assume_role_arn=None, no STS/session_factory is touched at all.
    sentinel: list = []
    dep = build_cdc_stack_deployer(
        "us-east-1",
        aws_profile=None,
        assume_role_arn=None,
        session_factory=lambda **kw: sentinel.append(kw),
    )
    assert dep._session is None
    assert sentinel == []  # factory never invoked


# ---------------------------------------------------------------------------
# Stall-watchdog liveness: a long CDC wait must heartbeat so a healthy,
# still-provisioning job is never reaped (audit fix #9).
# ---------------------------------------------------------------------------


class _SlowSettleDeployer:
    """Fake deployer whose stack stays *_IN_PROGRESS for N polls, then settles.

    Emits NO stack events (the realistic worst case: MSK can pass many minutes
    between events), so the only thing that can keep the job's liveness fresh
    across the wait is _StageDriver.heartbeat().
    """

    def __init__(self, *, in_progress_polls: int) -> None:
        self._remaining = in_progress_polls

    def poll_events(self, stack_name: str, since: datetime) -> list:
        return []  # no events between polls -- liveness must come from heartbeat

    def stack_status(self, stack_name: str) -> str:
        if self._remaining > 0:
            self._remaining -= 1
            return "CREATE_IN_PROGRESS"
        return "CREATE_COMPLETE"


def test_cdc_wait_heartbeats_so_watchdog_does_not_reap_healthy_job() -> None:
    from dsql_migrator.core.cdc_deployer import _StageDriver, _wait_stack_settles
    from dsql_migrator.core.job_manager import JobManager

    # A controllable monotonic clock. Real CDC waits poll far more often than the
    # stall window is long (interval ~15-30s vs a 900s timeout), so each poll's
    # heartbeat keeps the job fresh. Model that here: stall window 10s, each poll
    # advances 4s (< 10s) and reaps. WITHOUT the per-poll heartbeat the cumulative
    # idle across 3 polls (12s) would exceed 10s and reap the healthy job; WITH it,
    # the max idle between a heartbeat and the next reap is one 4s step, so it
    # survives until the stack settles.
    now = [1000.0]
    manager = JobManager(stall_timeout_seconds=10.0, clock=lambda: now[0])

    settled: dict = {}

    def work(handle) -> None:
        driver = _StageDriver(
            handle,
            stages=(("stack_create", "Creating"),),
            on_log=lambda ts, msg: None,
            # Each "sleep" advances the clock one poll interval (< stall window)
            # and runs a reap, exactly as the real watchdog would mid-wait.
            sleep=lambda _interval: (
                now.__setitem__(0, now[0] + 4.0),
                manager.reap_stalled_jobs(),
            ),
        )
        status = _wait_stack_settles(
            _SlowSettleDeployer(in_progress_polls=3),
            "mysql-dsql-cdc-stack",
            driver=driver,
            since=datetime.now(timezone.utc),
            timeout=10_000.0,
            interval=1.0,
        )
        settled["status"] = status

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)

    # The wait ran to a clean settle and the job is NOT reaped/FAILED, proving the
    # per-poll heartbeat refreshed liveness across an event-sparse, multi-window
    # provisioning wait.
    assert settled["status"] == "CREATE_COMPLETE"
    assert job.status == "DONE"
    assert manager.get_error(job_id) is None
    manager.shutdown()
