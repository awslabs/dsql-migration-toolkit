# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CDC lifecycle orchestration stage progressions.

Drives the four JobManager work functions -- run_cdc_infra_deploy / run_cdc_start
/ run_cdc_stop / run_cdc_delete -- with a fake CdcStackDeployer and a fake
JobHandle over a real MigrationJob. No AWS, no real sleeps.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.cdc import (
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
)
from dsql_migrator.core.cdc_deployer import (
    CDC_DELETE_STAGES,
    CDC_INFRA_STAGES,
    CDC_START_STAGES,
    CDC_STOP_STAGES,
    CdcDeployError,
    CdcStackDiscovery,
    run_cdc_delete,
    run_cdc_infra_deploy,
    run_cdc_start,
    run_cdc_stop,
)
from dsql_migrator.core.models import MigrationJob

STACK = "mysql-dsql-cdc-stack"
SRC = f"{STACK}-debezium-source"
SINK = f"{STACK}-dsql-sink"


class _FakeHandle:
    """Minimal JobHandle: applies mutators to a real MigrationJob; cancel flag."""

    def __init__(self, *, cancelled: bool = False):
        self.job = MigrationJob(job_id="DEPLOY1")
        self._cancelled = cancelled

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def update(self, mutator) -> None:
        mutator(self.job)


class _FakeDeployer:
    """Fake CdcStackDeployer scripting every lifecycle outcome.

    Records calls (``self.calls``) and the parameter overrides each update/create
    received (``self.updates`` / ``self.created``) so tests can assert the
    two-pass Start sends the right DeploySink / MskBootstrapServers values.
    """

    def __init__(
        self,
        *,
        # discover_stack / describe_stack_or_none
        discover_error: Exception | None = None,
        discovery_params: dict | None = None,
        discovery_status: str = "UPDATE_COMPLETE",
        existing: CdcStackDiscovery | None = "absent",  # type: ignore[assignment]
        # submit_update
        update_changed: bool = True,
        update_error: Exception | None = None,
        # create / delete
        create_error: Exception | None = None,
        # stack status polling: a list consumed left-to-right (last value sticks)
        stack_statuses=("UPDATE_COMPLETE",),
        # connector_state: name -> list of states (last sticks)
        connector_states=None,
        # connector_log_tail: name -> worker-log text (for failure diagnosis)
        connector_logs=None,
        # bootstrap
        cluster_arn: str | None = "arn:msk",
        bootstrap: str = "b-1:9098",
        bootstrap_error: Exception | None = None,
        # poll_events: list of (logical_id, status, reason) emitted once
        events=None,
    ):
        self._discover_error = discover_error
        self._discovery_params = discovery_params or {}
        self._discovery_status = discovery_status
        # "absent" sentinel -> describe_stack_or_none returns None
        self._existing = existing
        self._update_changed = update_changed
        self._update_error = update_error
        self._create_error = create_error
        self._stack_statuses = list(stack_statuses)
        self._connector_states = connector_states or {}
        self._connector_logs = connector_logs or {}
        self._cluster_arn = cluster_arn
        self._bootstrap = bootstrap
        self._bootstrap_error = bootstrap_error
        self._events = list(events or [])
        self._events_emitted = False
        self.calls: list[str] = []
        self.updates: list[list[tuple[str, str]]] = []
        self.created: list[tuple[str, list[tuple[str, str]]]] = []

    def discover_stack(self, stack_name):
        self.calls.append("discover_stack")
        if self._discover_error:
            raise self._discover_error
        return CdcStackDiscovery(
            stack_status=self._discovery_status,
            current_parameters=dict(self._discovery_params),
            is_stable=True,
        )

    def describe_stack_or_none(self, stack_name):
        self.calls.append("describe_stack_or_none")
        if self._existing == "absent":
            return None
        return self._existing

    def create_stack(self, stack_name, template_body, parameters):
        self.calls.append("create_stack")
        if self._create_error:
            raise self._create_error
        self.created.append((stack_name, list(parameters)))

    def delete_stack(self, stack_name):
        self.calls.append("delete_stack")

    def get_stack_output(self, stack_name, key):
        self.calls.append("get_stack_output")
        return self._cluster_arn

    def get_bootstrap_brokers(self, cluster_arn):
        self.calls.append("get_bootstrap_brokers")
        if self._bootstrap_error:
            raise self._bootstrap_error
        return self._bootstrap

    def submit_update(self, stack_name, overrides, *, template_body=None):
        self.calls.append("submit_update")
        self.updates.append(list(overrides))
        if self._update_error:
            raise self._update_error
        return self._update_changed

    def poll_events(self, stack_name, since):
        if self._events_emitted or not self._events:
            return []
        self._events_emitted = True
        # Mirror the real deployer's "<res> <state> — <reason>" formatting.
        out = []
        for res, state, reason in self._events:
            msg = f"{res} {state}" + (f" — {reason}" if reason else "")
            out.append((since, msg))
        return out

    def stack_status(self, stack_name):
        if len(self._stack_statuses) > 1:
            return self._stack_statuses.pop(0)
        return self._stack_statuses[0]

    def connector_state(self, connector_name):
        seq = self._connector_states.get(connector_name)
        if not seq:
            return "RUNNING"
        return seq.pop(0) if len(seq) > 1 else seq[0]

    def connector_log_tail(self, stack_name, connector_name, *, limit=400):
        return self._connector_logs.get(connector_name, "")


def _start_params():
    src = DebeziumSourceConfig(name="mysql-source", table_include_list=["cdc_demo.orders"])
    sink = SinkConnectorConfig(name="mysql-sink", topics=["cdc_demo.orders"], dlq_topic="dsql-sink-dlq")
    return build_cdc_stack_params(src, sink, target_endpoint="ep.on.aws")


def _infra_params():
    src = DebeziumSourceConfig(name="mysql-source", table_include_list=["cdc_demo.orders"])
    sink = SinkConnectorConfig(name="mysql-sink", topics=["cdc_demo.orders"], dlq_topic="dsql-sink-dlq")
    return build_cdc_infra_params(
        src, sink,
        vpc_id="vpc-1", connector_subnet_ids="subnet-a,subnet-b",
        plugin_bucket_arn="arn:aws:s3:::b", debezium_plugin_s3_key="deb.zip",
        dsql_sink_plugin_s3_key="sink.jar", source_db_hostname="db.host",
        source_secret_arn="arn:sec", source_secret_name="my/secret",
        dsql_cluster_arn="arn:dsql", target_endpoint="ep.on.aws",
    )


def _statuses(handle):
    return {c.chunk_id: c.status for c in handle.job.chunks}


def _logs():
    captured: list[str] = []
    return captured, (lambda ts, msg: captured.append(msg))


class _FakeS3:
    """Fake S3 for the infra-deploy bucket+upload stages (always 'exists', skip)."""

    def __init__(self, *, fail_create=None):
        self._fail_create = fail_create
        self.calls: list[str] = []

    def head_bucket(self, **kw):
        self.calls.append("head_bucket")
        return {}  # exists → no create

    def create_bucket(self, **kw):
        self.calls.append("create_bucket")
        if self._fail_create:
            raise RuntimeError(self._fail_create)
        return {}

    def head_object(self, **kw):
        self.calls.append("head_object")
        # Report a matching object so upload is skipped (no real file needed).
        return {"ContentLength": 0, "ETag": '"x-1"'}

    def put_object(self, **kw):
        self.calls.append("put_object")
        return {}


class _FakeSts:
    def get_caller_identity(self):
        return {"Account": "123456789012"}


def _run_infra(handle, deployer, on_log, **kw):
    """Drive run_cdc_infra_deploy with injected fake S3/STS clients.

    Patches s3_provision._artifact_paths to three real temp files (upload_plugin
    requires the local file to exist); the fake S3 reports a size-0/composite
    match so the actual upload is skipped.
    """
    import tempfile

    import dsql_migrator.core.s3_provision as s3p

    orig = s3p._artifact_paths
    with tempfile.TemporaryDirectory() as d:
        deb = Path(d) / "debezium-mysql-plugin.zip"
        sink = Path(d) / "dsql-sink-connector.zip"
        seeder = Path(d) / "offset-seeder-lambda.zip"
        deb.write_bytes(b"")  # size 0 → matches the fake head_object skip
        sink.write_bytes(b"")
        seeder.write_bytes(b"")
        s3p._artifact_paths = lambda: (deb, sink, seeder)
        try:
            run_cdc_infra_deploy(
                handle, deployer=deployer, on_log=on_log,
                region="us-east-1", s3_client=_FakeS3(), sts_client=_FakeSts(),
                sleep=lambda _s: None, **kw,
            )
        finally:
            s3p._artifact_paths = orig


# ---------------------------------------------------------------------------
# run_cdc_infra_deploy
# ---------------------------------------------------------------------------


def test_infra_deploy_happy_path() -> None:
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    _run_infra(
        handle, deployer, on_log,
        stack_name=STACK, template_body="T", params=_infra_params(),
        create_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert "create_stack" in deployer.calls
    # created with the full infra param set including the no-connectors pins;
    # plugin params were patched from the (fake) upload.
    _, params = deployer.created[0]
    pdict = dict(params)
    assert pdict["MskBootstrapServers"] == ""
    assert pdict["DeploySink"] == "false"
    assert pdict["VpcId"] == "vpc-1"
    assert pdict["PluginBucketArn"].startswith("arn:aws:s3:::mysql-dsql-migrator-plugins-")
    assert pdict["DebeziumPluginS3Key"].endswith("debezium-mysql-plugin.zip")


def test_infra_deploy_refuses_existing_stack() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing)
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log,
            stack_name=STACK, template_body="T", params=_infra_params(),
        )
    assert "already exists" in str(exc.value)
    assert "create_stack" not in deployer.calls
    assert _statuses(handle)["check_existing"] == "IN_PROGRESS"


def test_infra_deploy_refuses_rollback_complete() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    existing = CdcStackDiscovery("ROLLBACK_COMPLETE", {}, False)
    deployer = _FakeDeployer(existing=existing)
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log,
            stack_name=STACK, template_body="T", params=_infra_params(),
        )
    assert "Delete" in str(exc.value)


def test_infra_deploy_create_rollback_fails_stage() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "ROLLBACK_COMPLETE"))
    with pytest.raises(CdcDeployError):
        _run_infra(
            handle, deployer, on_log,
            stack_name=STACK, template_body="T", params=_infra_params(),
            create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        )
    assert _statuses(handle)["stack_create"] == "FAILED"


def test_infra_deploy_runs_upload_stages_first() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    _run_infra(
        handle, deployer, on_log,
        stack_name=STACK, template_body="T", params=_infra_params(),
        create_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    statuses = _statuses(handle)
    assert statuses["ensure_bucket"] == "DONE"
    assert statuses["upload_plugins"] == "DONE"


def test_infra_deploy_no_region_fails_bucket_stage() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    deployer = _FakeDeployer()
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_infra_deploy(
            handle, stack_name=STACK, template_body="T", params=_infra_params(),
            deployer=deployer, on_log=on_log, sleep=lambda _s: None,
            region=None,
        )
    assert "region" in str(exc.value).lower()
    assert _statuses(handle)["ensure_bucket"] == "IN_PROGRESS"


# ---------------------------------------------------------------------------
# run_cdc_start (two-pass)
# ---------------------------------------------------------------------------


def _run_start(handle, deployer, logs=None):
    captured = logs if logs is not None else []
    run_cdc_start(
        handle, stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: captured.append(msg), sleep=lambda _s: None,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    return captured


def test_start_happy_path_two_passes() -> None:
    handle = _FakeHandle()
    # Neither connector running yet, then each reaches RUNNING.
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    # Two updates: Pass A (source) sets bootstrap + DeploySink=false;
    # Pass B (sink) sets only DeploySink=true.
    assert len(deployer.updates) == 2
    pass_a = dict(deployer.updates[0])
    assert pass_a["MskBootstrapServers"] == "b-1:9098"
    assert pass_a["DeploySink"] == "false"
    pass_b = dict(deployer.updates[1])
    assert pass_b == {"DeploySink": "true"}


def _connector_overrides() -> dict:
    """The connector-config params (TableIncludeList/SinkTopics/...) for the start
    params -- i.e. what the deployed stack must already carry to count as
    'unchanged'."""
    params = _start_params()
    return {
        k: v
        for k, v in params.filled
        if k not in {"MskBootstrapServers", "DeploySink"}
    }


def test_start_idempotent_both_running_same_config_skips_updates() -> None:
    handle = _FakeHandle()
    # Both connectors RUNNING AND the deployed config matches the request -> skip.
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
        discovery_params=_connector_overrides(),
    )
    logs = _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert deployer.updates == []
    assert any("already RUNNING with the requested configuration" in m for m in logs)


def test_start_running_but_config_changed_updates_connectors() -> None:
    handle = _FakeHandle()
    # Both connectors RUNNING but the deployed table set DIFFERS from the request
    # -> the connectors MUST be updated (not skipped) so the new tables replicate.
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
        discovery_params={"TableIncludeList": "cdc_demo.something_else"},
    )
    logs = _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert len(deployer.updates) == 2  # both passes ran despite RUNNING
    assert any("configuration changed" in m for m in logs)


def test_start_source_only_running_does_sink_pass() -> None:
    handle = _FakeHandle()
    # Source already up (with matching config), sink missing -> one update (sink).
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["CREATING", "RUNNING"]},
        discovery_params=_connector_overrides(),
    )
    _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert len(deployer.updates) == 1
    assert dict(deployer.updates[0]) == {"DeploySink": "true"}


def test_start_source_failed_raises_before_sink() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(connector_states={SRC: ["FAILED"], SINK: ["RUNNING"]})
    with pytest.raises(CdcDeployError):
        _run_start(handle, deployer)
    assert _statuses(handle)["source_connector"] == "IN_PROGRESS"
    assert _statuses(handle)["sink_connector"] == "PENDING"


def _watermark():
    from datetime import datetime, timezone

    from dsql_migrator.core.models import Watermark

    return Watermark(
        binlog_file="mysql-bin.000042",
        binlog_position=15324,
        gtid_executed="UUID:1-9",
        snapshot_timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )


def test_start_passes_watermark_params_on_source_pass_only() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    captured: list[str] = []
    run_cdc_start(
        handle, stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: captured.append(msg), sleep=lambda _s: None,
        watermark=_watermark(),
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    pass_a = dict(deployer.updates[0])
    # Pass A carries the watermark coordinates so the in-VPC seeder runs.
    assert pass_a["WatermarkBinlogFile"] == "mysql-bin.000042"
    assert pass_a["WatermarkBinlogPos"] == "15324"
    assert pass_a["WatermarkGtids"] == "UUID:1-9"
    # Pass B (sink) is watermark-free -- it is purely DeploySink=true.
    assert dict(deployer.updates[1]) == {"DeploySink": "true"}
    assert any("Seeding gapless CDC start offset" in m for m in captured)


def test_start_watermark_only_change_does_not_bounce_running_source() -> None:
    # A RUNNING source whose connector config already matches must NOT be torn down
    # just because a watermark is now supplied: the Watermark* params are kept out
    # of the config-changed comparison (a watermark-only change is a create-time
    # seed concern; the seeder's no-clobber guard handles a live connector anyway).
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
        discovery_params=_connector_overrides(),
    )
    captured: list[str] = []
    run_cdc_start(
        handle, stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: captured.append(msg), sleep=lambda _s: None,
        watermark=_watermark(),
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    # No updates submitted -> the RUNNING source was not rewound by the watermark.
    assert deployer.updates == []
    assert any("already RUNNING with the requested configuration" in m for m in captured)


def test_start_bootstrap_fetch_failure_raises() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(bootstrap_error=CdcDeployError("not active"))
    with pytest.raises(CdcDeployError):
        _run_start(handle, deployer)
    assert _statuses(handle)["fetch_bootstrap"] == "IN_PROGRESS"
    assert deployer.updates == []  # never got to an update


def test_start_missing_cluster_arn_raises() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(cluster_arn=None)
    with pytest.raises(CdcDeployError):
        _run_start(handle, deployer)
    assert _statuses(handle)["fetch_bootstrap"] == "IN_PROGRESS"


def test_start_discover_failure_fails_first_stage_only() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(discover_error=CdcDeployError("missing"))
    with pytest.raises(CdcDeployError):
        _run_start(handle, deployer)
    statuses = _statuses(handle)
    assert statuses["discover_stack"] == "IN_PROGRESS"
    assert statuses["pipeline_running"] == "PENDING"


# ---------------------------------------------------------------------------
# run_cdc_stop
# ---------------------------------------------------------------------------


def test_stop_happy_path() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    deployer = _FakeDeployer(discovery_params={"MskBootstrapServers": "b-1:9098"})
    run_cdc_stop(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, stop_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    # Stop blanks the bootstrap.
    assert dict(deployer.updates[0]) == {"MskBootstrapServers": ""}


def test_stop_already_stopped_submits_nothing() -> None:
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(discovery_params={"MskBootstrapServers": ""})
    run_cdc_stop(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert deployer.updates == []
    assert any("already stopped" in m for m in logs)


# ---------------------------------------------------------------------------
# run_cdc_delete
# ---------------------------------------------------------------------------


def test_delete_happy_path() -> None:
    handle = _FakeHandle()
    _, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    # No region -> the secret-cleanup stage is skipped (still DONE), no AWS call.
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert "delete_stack" in deployer.calls


def test_delete_absent_stack_is_noop_success() -> None:
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(existing="absent")
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert "delete_stack" not in deployer.calls
    assert any("does not exist" in m for m in logs)


def test_delete_inflight_stack_raises_wait_and_retry() -> None:
    # A stack mid-operation (a create/update/rollback still running) cannot be
    # deleted yet -- submitting a delete now races the live op. Stop with a clear
    # wait-and-retry message and do NOT call delete_stack.
    from dsql_migrator.core.cdc_deployer import CdcDeployError

    handle = _FakeHandle()
    _, on_log = _logs()
    existing = CdcStackDiscovery("UPDATE_ROLLBACK_IN_PROGRESS", {}, False)
    deployer = _FakeDeployer(existing=existing)
    with pytest.raises(CdcDeployError) as ei:
        run_cdc_delete(
            handle, stack_name=STACK, deployer=deployer, on_log=on_log,
            sleep=lambda _s: None,
        )
    assert "still" in str(ei.value) and "Wait" in str(ei.value)
    assert "delete_stack" not in deployer.calls  # no blind, doomed submit


def test_delete_when_already_deleting_skips_submit_and_waits() -> None:
    # If a deletion is ALREADY underway (DELETE_IN_PROGRESS), don't re-submit --
    # just wait for it to finish (it then vanishes -> success).
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("DELETE_IN_PROGRESS", {}, False)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert "delete_stack" not in deployer.calls  # already in flight -> no re-submit
    assert any("already in progress" in m for m in logs)


def test_delete_cleans_up_source_secret_when_region_given(monkeypatch) -> None:
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))

    seen: dict = {}

    def _fake_delete(*, stack_name, aws_profile, region):
        seen.update(stack_name=stack_name, aws_profile=aws_profile, region=region)
        return "deleted"

    monkeypatch.setattr(
        "dsql_migrator.core.secrets.delete_source_secret", _fake_delete
    )

    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1", aws_profile="prof",
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert seen == {"stack_name": STACK, "aws_profile": "prof", "region": "us-east-1"}
    assert any("scheduled for deletion" in m for m in logs)


def test_delete_secret_cleanup_failure_is_not_fatal(monkeypatch) -> None:
    from dsql_migrator.core.secrets import SecretProvisionError

    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))

    def _fail(**_kw):
        raise SecretProvisionError("Could not delete the source secret 's': denied")

    monkeypatch.setattr(
        "dsql_migrator.core.secrets.delete_source_secret", _fail
    )

    # The stack is already gone; a secret-cleanup failure must NOT fail teardown.
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1",
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert any("WARNING" in m and "manually" in m for m in logs)


def test_delete_skips_secret_cleanup_when_disabled(monkeypatch) -> None:
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))

    def _boom(**_kw):  # pragma: no cover - must not be called
        raise AssertionError("delete_source_secret must not run when disabled")

    monkeypatch.setattr(
        "dsql_migrator.core.secrets.delete_source_secret", _boom
    )

    # SM-auth source -> cleanup_source_secret=False; the customer's secret is safe.
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1", cleanup_source_secret=False,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert any("Skipped source-secret cleanup" in m for m in logs)


# ---------------------------------------------------------------------------
# cancellation
# ---------------------------------------------------------------------------


def test_start_cancel_stops_early() -> None:
    handle = _FakeHandle(cancelled=True)
    deployer = _FakeDeployer()
    _run_start(handle, deployer)
    # Cancelled before submitting the source pass.
    assert _statuses(handle)["pipeline_running"] == "PENDING"


def test_stage_lists_have_unique_ids() -> None:
    for stages in (CDC_INFRA_STAGES, CDC_START_STAGES, CDC_STOP_STAGES, CDC_DELETE_STAGES):
        ids = [s[0] for s in stages]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# MSK partition-quota detection + actionable failure
# ---------------------------------------------------------------------------


def test_partition_quota_message_detector() -> None:
    from dsql_migrator.core.cdc_deployer import _is_partition_quota_message

    assert _is_partition_quota_message(
        "Quota exceeded for maximum number of partitions"
    )
    assert _is_partition_quota_message("partition limit reached")
    assert not _is_partition_quota_message("Resource creation cancelled")
    assert not _is_partition_quota_message("")


def test_infra_failure_with_partition_quota_event_is_actionable() -> None:
    # A create that fails AFTER a partition-quota event surfaces the recovery
    # instruction (delete the whole stack + redeploy), not the opaque generic text.
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(
        stack_statuses=("CREATE_IN_PROGRESS", "ROLLBACK_COMPLETE"),
        events=[("MskCluster", "CREATE_FAILED",
                 "Quota exceeded for maximum number of partitions")],
    )
    with pytest.raises(CdcDeployError) as excinfo:
        run_cdc_infra_deploy(
            handle, stack_name=STACK, template_body="t", params=_infra_params(),
            deployer=deployer, on_log=on_log, region="us-east-1",
            s3_client=_FakeS3(), sts_client=_FakeSts(),
            sleep=lambda _s: None, create_timeout_seconds=5.0,
            poll_interval_seconds=0.0,
        )
    msg = str(excinfo.value)
    assert "partition quota" in msg.lower()
    assert "Delete CDC infrastructure" in msg


# ---------------------------------------------------------------------------
# Worker-log failure diagnosis (the real cause is in the connector log, not the
# generic CloudFormation event)
# ---------------------------------------------------------------------------


def test_diagnose_connector_log_classifies_known_signatures() -> None:
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log

    quota = _diagnose_connector_log(
        SRC, "...Quota exceeded for maximum number of partitions..."
    )
    assert quota is not None and "Delete CDC infrastructure" in quota

    conn = _diagnose_connector_log(SRC, "com.mysql.cj... Communications link failure")
    assert conn is not None and "could not reach the source MySQL" in conn

    sdk = _diagnose_connector_log(SRC, "java.lang.NoSuchFieldError: AUTH_SCHEME_PROVIDER")
    assert sdk is not None and "plugin-packaging defect" in sdk

    denied = _diagnose_connector_log(SRC, "Access denied for user 'cdc'@'10.0.0.1'")
    assert denied is not None and "access denied" in denied.lower()

    # Nothing recognizable -> None (caller falls back to the raw state).
    assert _diagnose_connector_log(SRC, "some unrelated info line") is None
    assert _diagnose_connector_log(SRC, "") is None


def test_connector_failed_surfaces_worker_log_guidance() -> None:
    # A connector that flips to FAILED with a partition-quota worker log surfaces
    # the actionable recovery instruction, not the opaque "entered FAILED state".
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "FAILED"]},
        connector_logs={SRC: "ERROR ... Quota exceeded for maximum number of partitions"},
    )
    with pytest.raises(CdcDeployError) as excinfo:
        _run_start(handle, deployer)
    msg = str(excinfo.value)
    assert "partition quota" in msg.lower()
    assert "Delete CDC infrastructure" in msg


def test_connector_failed_without_known_log_falls_back_to_generic() -> None:
    # No recognizable worker-log signature -> the generic FAILED message (still
    # names the connector), never a crash.
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "FAILED"]},
        connector_logs={SRC: "just some unremarkable log output"},
    )
    with pytest.raises(CdcDeployError) as excinfo:
        _run_start(handle, deployer)
    assert "FAILED" in str(excinfo.value)
