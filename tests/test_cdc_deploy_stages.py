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
    _MAX_STATE_READ_FAILURES,
    _parse_unsupported_azs,
    _wait_connector_running,
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
        # find_unsupported_azs: AZ names MSK rejected (reactive-retry trigger)
        unsupported_azs=None,
        # seeder_eni_count: list of counts consumed left-to-right (last sticks); the
        # delete wait logs the seeder-ENI reclamation from these.
        seeder_eni_counts=None,
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
        self._unsupported_azs = set(unsupported_azs or set())
        self._seeder_eni_counts = list(seeder_eni_counts or [])
        self.calls: list[str] = []
        self.updates: list[list[tuple[str, str]]] = []
        self.created: list[tuple[str, list[tuple[str, str]]]] = []

    # The in-process External seed reads the deployer's region.
    region = "us-east-1"

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

    def find_unsupported_azs(self, stack_name):
        self.calls.append("find_unsupported_azs")
        return set(self._unsupported_azs)

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

    def stack_status_checked(self, stack_name):
        # The raising variant _wait_stack_settles now uses. This fake never simulates
        # a read error, so it just mirrors the best-effort sequence (a read error is
        # exercised by a dedicated fake in test_cdc_deployer.py).
        return self.stack_status(stack_name)

    def seeder_eni_count(self, stack_name):
        # Default: unknown (None) so the delete wait logs no ENI line unless a test
        # opts in by setting self._seeder_eni_counts to a drainable sequence.
        seq = getattr(self, "_seeder_eni_counts", None)
        if not seq:
            return None
        return seq.pop(0) if len(seq) > 1 else seq[0]

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
        deb_pg = Path(d) / "debezium-postgres-plugin.zip"
        sink = Path(d) / "dsql-sink-connector.zip"
        seeder = Path(d) / "offset-seeder-lambda.zip"
        deb.write_bytes(b"")  # size 0 → matches the fake head_object skip
        deb_pg.write_bytes(b"")
        sink.write_bytes(b"")
        seeder.write_bytes(b"")
        s3p._artifact_paths = lambda: (deb, deb_pg, sink, seeder)
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


@pytest.mark.parametrize(
    "reason,expected",
    [
        (
            "Resource handler returned message: unsupported availability zones: "
            "[ap-northeast-2d]",
            {"ap-northeast-2d"},
        ),
        (
            "unsupported availability zones: [ap-northeast-2d, ap-northeast-2c]",
            {"ap-northeast-2d", "ap-northeast-2c"},
        ),
        ("Unsupported Availability Zone: [us-east-1e]", {"us-east-1e"}),
        ("Some unrelated failure reason", set()),
        ("", set()),
    ],
)
def test_parse_unsupported_azs(reason, expected) -> None:
    assert _parse_unsupported_azs(reason) == expected


class _FakeEc2ForRetry:
    """Fake EC2 for the reactive-retry subnet re-selection (3 NAT AZs)."""

    def describe_subnets(self, **kw):
        return {
            "Subnets": [
                {"SubnetId": "subnet-x", "AvailabilityZone": "us-east-1a"},
                {"SubnetId": "subnet-y", "AvailabilityZone": "us-east-1b"},
                {"SubnetId": "subnet-d", "AvailabilityZone": "us-east-1d"},
            ]
        }

    def describe_route_tables(self, **kw):
        return {
            "RouteTables": [
                {
                    "Associations": [{"Main": True}],
                    "Routes": [
                        {"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": "nat-1"}
                    ],
                }
            ]
        }


def test_infra_deploy_retries_excluding_unsupported_az() -> None:
    # First create rolls back with an unsupported-AZ reason; the deploy deletes
    # the stack, re-selects subnets excluding that AZ, and the retry succeeds.
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(
        stack_statuses=("ROLLBACK_COMPLETE", None, "CREATE_COMPLETE"),
        unsupported_azs={"us-east-1d"},
    )
    _run_infra(
        handle, deployer, on_log,
        stack_name=STACK, template_body="T", params=_infra_params(),
        ec2_client=_FakeEc2ForRetry(),
        create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        delete_timeout_seconds=5.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    # Two create attempts, with a delete in between.
    assert deployer.calls.count("create_stack") == 2
    assert "delete_stack" in deployer.calls
    # The retry re-selected subnets across the two supported AZs (d excluded).
    _, retried = deployer.created[1]
    assert dict(retried)["ConnectorSubnetIds"] == "subnet-x,subnet-y"
    assert any("does not support" in m for m in logs)


def test_infra_deploy_gives_up_when_exclusion_leaves_one_az() -> None:
    # Only two NAT AZs and MSK rejects one → re-select can't reach >=2 → fail.
    class _TwoAzEc2(_FakeEc2ForRetry):
        def describe_subnets(self, **kw):
            return {
                "Subnets": [
                    {"SubnetId": "subnet-x", "AvailabilityZone": "us-east-1a"},
                    {"SubnetId": "subnet-d", "AvailabilityZone": "us-east-1d"},
                ]
            }

    handle = _FakeHandle()
    _, on_log = _logs()
    deployer = _FakeDeployer(
        stack_statuses=("ROLLBACK_COMPLETE", None),
        unsupported_azs={"us-east-1d"},
    )
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log,
            stack_name=STACK, template_body="T", params=_infra_params(),
            ec2_client=_TwoAzEc2(),
            create_timeout_seconds=5.0, poll_interval_seconds=0.0,
            delete_timeout_seconds=5.0,
        )
    assert "us-east-1d" in str(exc.value)
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


def test_start_happy_path_single_pass() -> None:
    handle = _FakeHandle()
    # Neither connector running yet, then each reaches RUNNING.
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    # ONE update creates BOTH connectors: bootstrap + DeploySink=true. The stack's
    # CdcStartPrepResource pre-creates the topics so source + sink deploy in parallel
    # (no source-then-sink two-pass).
    assert len(deployer.updates) == 1
    only = dict(deployer.updates[0])
    assert only["MskBootstrapServers"] == "b-1:9098"
    assert only["DeploySink"] == "true"


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


# ---------------------------------------------------------------------------
# run_cdc_start — SeedMode=External (Lambda-free in-process seed)
# ---------------------------------------------------------------------------


def _run_start_external(handle, deployer, seed_calls, logs=None):
    """Run start in External mode with an injected seed_fn that records its call."""
    def _seed_fn(**kwargs):
        seed_calls.append(kwargs)
        return "true"

    captured = logs if logs is not None else []
    run_cdc_start(
        handle, stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: captured.append(msg), sleep=lambda _s: None,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
        seed_mode="external", seed_fn=_seed_fn,
    )
    return captured


def test_start_external_seeds_before_submit_and_appends_seedmode() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    seed_calls: list[dict] = []
    _run_start_external(handle, deployer, seed_calls)
    assert all(s == "DONE" for s in _statuses(handle).values())
    # The in-process seed ran exactly once, BEFORE the single connector-creating
    # update (fake records order via deployer.calls: seed is not a deployer call, so
    # assert the seed happened and the update carries the CAPITALIZED SeedMode token
    # the template's AllowedValues ["Lambda","External"] require -- real CFN
    # validates case-sensitively, so this must be "External", not "external").
    assert len(seed_calls) == 1
    assert len(deployer.updates) == 1
    only = dict(deployer.updates[0])
    assert only["SeedMode"] == "External"
    assert only["MskBootstrapServers"] == "b-1:9098"
    # The seed used the fetched bootstrap + the fixed offset topic name.
    call = seed_calls[0]
    assert call["bootstrap"] == "b-1:9098"
    assert call["offset_topic"] == f"{STACK}-debezium-source-offsets"
    assert call["connector_name"] == SRC


def test_start_default_lambda_does_not_seed_or_set_seedmode() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    seed_calls: list[dict] = []

    def _seed_fn(**kwargs):
        seed_calls.append(kwargs)
        return "true"

    # Default seed_mode='lambda': the seed_fn must never be called and SeedMode must
    # NOT appear in the update (the template keeps its Default=Lambda).
    run_cdc_start(
        handle, stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: None, sleep=lambda _s: None,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
        seed_fn=_seed_fn,
    )
    assert seed_calls == []
    only = dict(deployer.updates[0])
    assert "SeedMode" not in only


def test_start_external_fastpath_both_running_skips_seed() -> None:
    handle = _FakeHandle()
    # Both connectors RUNNING with matching config -> the whole else branch is
    # skipped, so the in-process seed must NOT run (quota-safe idempotent no-op).
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
        discovery_params=_connector_overrides(),
    )
    seed_calls: list[dict] = []
    _run_start_external(handle, deployer, seed_calls)
    assert deployer.updates == []
    assert seed_calls == []  # seed skipped along with the update


def test_start_external_seed_failure_raises_before_submit() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )

    def _boom(**kwargs):
        raise RuntimeError("cluster unreachable")

    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(
            handle, stack_name=STACK, params=_start_params(), deployer=deployer,
            on_log=lambda ts, msg: None, sleep=lambda _s: None,
            connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
            seed_mode="external", seed_fn=_boom,
        )
    # Fails loudly and NO connector-creating update was submitted (no silent gap).
    assert "No connectors were created" in str(exc.value)
    assert deployer.updates == []


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
    assert len(deployer.updates) == 1  # single pass updates both, despite RUNNING
    assert any("configuration changed" in m for m in logs)


def test_start_sink_missing_runs_single_pass() -> None:
    handle = _FakeHandle()
    # Source already up (matching config) but sink missing -> NOT both RUNNING, so
    # the single pass runs (reconciling both). One update with DeploySink=true.
    deployer = _FakeDeployer(
        connector_states={SRC: ["RUNNING"], SINK: ["CREATING", "RUNNING"]},
        discovery_params=_connector_overrides(),
    )
    _run_start(handle, deployer)
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert len(deployer.updates) == 1
    assert dict(deployer.updates[0])["DeploySink"] == "true"


def test_start_connector_failed_raises() -> None:
    handle = _FakeHandle()
    deployer = _FakeDeployer(connector_states={SRC: ["FAILED"], SINK: ["RUNNING"]})
    with pytest.raises(CdcDeployError):
        _run_start(handle, deployer)
    # The single connectors_running stage never completes; pipeline stays PENDING.
    assert _statuses(handle)["connectors_running"] != "DONE"


def test_start_rollback_diagnoses_from_real_connector_logs() -> None:
    # A connector CREATE_FAILED rolls the stack back, and the connectors-pass
    # _wait_stack_settles raises BEFORE the per-connector RUNNING-waits run -- so its
    # rollback diagnosis is the only chance to surface the cause. It must scan the REAL
    # connector names' worker logs: a synthetic "src + sink" pseudo-name matched no log
    # stream (connector_log_tail matches by substring), leaving the whole diagnosis dead
    # on this -- the primary -- connector-failure path. A source-unreachable signature
    # in the source connector's log must surface as actionable guidance, not the opaque
    # "Stack operation ended in 'UPDATE_ROLLBACK_COMPLETE'".
    handle = _FakeHandle()
    deployer = _FakeDeployer(
        stack_statuses=("UPDATE_ROLLBACK_COMPLETE",),
        connector_logs={SRC: "com.mysql.cj... Communications link failure"},
    )
    with pytest.raises(CdcDeployError) as exc:
        _run_start(handle, deployer)
    msg = str(exc.value)
    assert "could not reach the source MySQL" in msg  # the diagnosed real cause
    assert "Stack operation ended in" not in msg  # NOT the opaque fallback
    assert _statuses(handle)["pipeline_running"] == "PENDING"


def _watermark():
    from datetime import datetime, timezone

    from dsql_migrator.core.models import Watermark

    return Watermark(
        binlog_file="mysql-bin.000042",
        binlog_position=15324,
        gtid_executed="UUID:1-9",
        snapshot_timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )


def test_start_passes_watermark_params_on_single_pass() -> None:
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
    # The single pass carries the watermark coordinates + DeploySink=true, so the
    # in-VPC CdcStartPrepResource seeds the offset and both connectors are created.
    assert len(deployer.updates) == 1
    only = dict(deployer.updates[0])
    assert only["WatermarkBinlogFile"] == "mysql-bin.000042"
    assert only["WatermarkBinlogPos"] == "15324"
    assert only["WatermarkGtids"] == "UUID:1-9"
    assert only["DeploySink"] == "true"
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


class _SlotRes:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):  # noqa: ANN201
        return self._rows[0] if self._rows else None


class _SlotConn:
    """Fake source connection: slot+publication exist, records the drop statements."""

    def __init__(self):
        self.sql: list[str] = []
        self.params: list[dict] = []

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_a):  # noqa: ANN204
        return False

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        s = str(statement)
        self.sql.append(s)
        self.params.append(params or {})
        up = " ".join(s.upper().split())
        if "FROM PG_REPLICATION_SLOTS" in up or "FROM PG_PUBLICATION" in up:
            return _SlotRes([(1,)])  # both exist -> drops are issued
        return _SlotRes([])


def _patch_pg_teardown(monkeypatch, *, engine):
    """Patch the PG source-write engine + secret resolution/deletion for a delete test."""
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core import cdc_pg_slot
    import dsql_migrator.core.secrets as secrets

    monkeypatch.setattr(
        cdc_pg_slot, "build_pg_source_write_engine", lambda src, pw: engine
    )
    monkeypatch.setattr(
        secrets, "resolve_source_secret",
        lambda sid, prof, region=None: ("cdc_user", SecretValue("pw")),
    )
    monkeypatch.setattr(
        secrets, "delete_source_secret",
        lambda **k: "scheduled",  # avoid a real Secrets Manager call in cleanup_secret
    )


_PG_DELETE_PARAMS = {
    "EngineType": "postgres",
    "SourceDbHostname": "pg.example.com",
    "SourceDbPort": "5432",
    "PgDatabaseName": "app",
}


def test_delete_drops_pg_replication_slot_before_secret_cleanup(monkeypatch) -> None:
    # A PostgreSQL cdc-stack teardown must drop the logical replication slot +
    # publication on the source (after the stack/connectors are gone, before the
    # secret is deleted). A slot left behind pins source WAL.
    conn = _SlotConn()

    class _Eng:
        def connect(self):  # noqa: ANN201
            return conn

        def dispose(self) -> None:
            self.disposed = True

    engine = _Eng()
    _patch_pg_teardown(monkeypatch, engine=engine)

    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", dict(_PG_DELETE_PARAMS), True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1", sleep=lambda _s: None,
        delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    writes = " ".join(conn.sql).upper()
    assert "PG_DROP_REPLICATION_SLOT" in writes
    assert "DROP PUBLICATION IF EXISTS" in writes
    assert engine.disposed is True  # source write engine disposed (no leak)


def test_delete_uses_deployed_slot_name_and_secret_fallback(monkeypatch) -> None:
    # Teardown must drop the DEPLOYED slot name (PgSlotName from the stack params, which
    # is the exact name the connector used even if the session's stack name drifted), and
    # -- for a Secrets-Manager-auth source with no tool-managed secret -- fall back to the
    # stack's own SourceSecretArn.
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core import cdc_pg_slot
    import dsql_migrator.core.secrets as secrets
    from dsql_migrator.core.secrets import SecretResolutionError

    conn = _SlotConn()

    class _Eng:
        def connect(self):  # noqa: ANN201
            return conn

        def dispose(self) -> None:
            self.disposed = True

    engine = _Eng()
    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine", lambda src, pw: engine)
    monkeypatch.setattr(secrets, "delete_source_secret", lambda **k: "absent")
    resolved = []

    def _resolve(secret_id, prof, region=None):
        resolved.append(secret_id)
        if secret_id.startswith("mysql-dsql-migrator/cdc/"):
            raise SecretResolutionError("no tool secret")  # SM-auth source: none created
        return ("cdc_user", SecretValue("pw"))

    monkeypatch.setattr(secrets, "resolve_source_secret", _resolve)

    handle = _FakeHandle()
    _, on_log = _logs()
    params = dict(_PG_DELETE_PARAMS)
    params["PgSlotName"] = "dsqlmig_deployed_slot"
    params["PgPublicationName"] = "dsqlmig_pub_deployed"
    params["SourceSecretArn"] = "arn:aws:secretsmanager:us-east-1:1:secret:customer-abc"
    existing = CdcStackDiscovery("CREATE_COMPLETE", params, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1", sleep=lambda _s: None,
        delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    # Dropped the DEPLOYED slot name (a bind param) and publication (inlined), not
    # re-derived ones.
    slot_params = [p.get("name") for p in conn.params if p.get("name")]
    assert "dsqlmig_deployed_slot" in slot_params
    assert "dsqlmig_pub_deployed" in " ".join(conn.sql)
    # Fell back to the stack's customer secret after the tool secret was absent.
    assert any(s.startswith("arn:aws:secretsmanager") for s in resolved), resolved


def test_delete_absent_stack_warns_about_a_possible_orphan_slot() -> None:
    # When the stack is gone (deleted out-of-band), the drop can't run; a loud reminder
    # must name the slot that may still pin WAL on the source.
    handle = _FakeHandle()
    logs, on_log = _logs()
    deployer = _FakeDeployer(existing="absent")
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log, sleep=lambda _s: None,
    )
    assert any("pg_drop_replication_slot" in m for m in logs), logs


def test_delete_mysql_stack_never_touches_the_source(monkeypatch) -> None:
    # A MySQL teardown must be byte-identical: no source-write engine is ever built.
    from dsql_migrator.core import cdc_pg_slot

    built: list = []
    monkeypatch.setattr(
        cdc_pg_slot, "build_pg_source_write_engine",
        lambda src, pw: built.append((src, pw)),
    )
    handle = _FakeHandle()
    _, on_log = _logs()
    # No EngineType (or "mysql") -> the PG slot-drop branch never runs.
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert built == []  # source never contacted for a MySQL teardown


def test_delete_pg_slot_drop_failure_is_loud_but_not_fatal(monkeypatch) -> None:
    # A surviving slot is a production-disk hazard, so a drop failure logs a prominent
    # manual-drop reminder -- but never fails the (already-complete) infra teardown.
    from dsql_migrator.core import cdc_pg_slot
    import dsql_migrator.core.secrets as secrets

    def _boom(src, pw):
        raise RuntimeError("cannot reach source")

    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine", _boom)
    monkeypatch.setattr(
        secrets, "resolve_source_secret",
        lambda sid, prof, region=None: ("cdc_user", None),
    )
    monkeypatch.setattr(secrets, "delete_source_secret", lambda **k: "scheduled")

    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", dict(_PG_DELETE_PARAMS), True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        region="us-east-1", sleep=lambda _s: None,
        delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    # Teardown still completes...
    assert all(s == "DONE" for s in _statuses(handle).values())
    # ...with a loud manual-drop reminder naming pg_drop_replication_slot.
    assert any(
        "WARNING" in m and "pg_drop_replication_slot" in m for m in logs
    ), logs


def test_delete_pg_missing_host_skips_drop_gracefully(monkeypatch) -> None:
    # A PostgreSQL teardown whose captured stack params carry no source host cannot
    # reconstruct the source connection, so the slot drop is skipped BEFORE any engine
    # is built -- but it must log a loud manual-drop reminder (a surviving slot pins
    # WAL) and let the (already-complete) teardown finish.
    from dsql_migrator.core import cdc_pg_slot

    built: list = []
    monkeypatch.setattr(
        cdc_pg_slot, "build_pg_source_write_engine",
        lambda src, pw: built.append((src, pw)),
    )
    handle = _FakeHandle()
    logs, on_log = _logs()
    params = dict(_PG_DELETE_PARAMS)
    params["SourceDbHostname"] = ""
    existing = CdcStackDiscovery("CREATE_COMPLETE", params, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert built == []  # no source engine built when the host is unknown
    assert any(
        "WARNING" in m and "source host" in m and "pg_drop_replication_slot" in m
        for m in logs
    ), logs


def test_delete_pg_no_credentials_skips_drop_gracefully(monkeypatch) -> None:
    # A PostgreSQL teardown where the source credentials can't be resolved (no
    # tool-managed secret and no stack SourceSecretArn/Name fallback) must catch the
    # SecretResolutionError, never build a source engine, log a loud manual-drop
    # reminder, and let the (already-complete) teardown finish.
    from dsql_migrator.core import cdc_pg_slot
    import dsql_migrator.core.secrets as secrets
    from dsql_migrator.core.secrets import SecretResolutionError

    built: list = []
    monkeypatch.setattr(
        cdc_pg_slot, "build_pg_source_write_engine",
        lambda src, pw: built.append((src, pw)),
    )
    monkeypatch.setattr(
        secrets, "resolve_source_secret",
        lambda secret_id, prof, region=None: (_ for _ in ()).throw(
            SecretResolutionError("none")
        ),
    )
    monkeypatch.setattr(secrets, "delete_source_secret", lambda **k: "absent")

    handle = _FakeHandle()
    logs, on_log = _logs()
    params = dict(_PG_DELETE_PARAMS)  # no SourceSecretArn/Name -> no fallback
    existing = CdcStackDiscovery("CREATE_COMPLETE", params, True)
    deployer = _FakeDeployer(existing=existing, stack_statuses=("DELETE_IN_PROGRESS", None))
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log, region="us-east-1",
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert all(s == "DONE" for s in _statuses(handle).values())
    assert built == []  # creds unresolved -> engine never built
    assert any(
        "WARNING" in m and "pg_drop_replication_slot" in m for m in logs
    ), logs


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


def test_delete_logs_seeder_eni_reclamation() -> None:
    """The delete wait must report seeder-ENI reclamation, which CFN is silent about.

    CloudFormation emits no event during the ~15-20 min AWS spends releasing the
    in-VPC seeder Lambda's ENIs before MskCluster can go, so the log looked frozen. The
    wait now polls the count and logs the wait, then the release.
    """
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    # Two ENIs pending for two polls, then released, then the stack vanishes.
    deployer = _FakeDeployer(
        existing=existing,
        stack_statuses=("DELETE_IN_PROGRESS", "DELETE_IN_PROGRESS", None),
        seeder_eni_counts=[2, 2, 0],
    )
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    joined = " ".join(logs)
    assert "reclaim 2 seeder network interfaces" in joined
    assert "Seeder network interfaces released." in joined


def test_delete_eni_wait_not_logged_on_every_poll() -> None:
    """Rapid polls within the re-report interval must not each emit a line.

    The wait reports on CHANGE plus at most every _ENI_REPORT_INTERVAL_SECONDS, so a
    burst of polls (as here, with no wall-clock advance) yields ONE line rather than one
    per poll -- otherwise the ENI reporting would drown the stack events.
    """
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(
        existing=existing,
        stack_statuses=("DELETE_IN_PROGRESS",) * 4 + (None,),
        seeder_eni_counts=[1, 1, 1, 1, 0],
    )
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    waiting_lines = [m for m in logs if "reclaim 1 seeder network interface" in m]
    assert len(waiting_lines) == 1, f"expected one waiting line, got {waiting_lines}"


def test_delete_eni_wait_is_re_reported_while_it_drags_on() -> None:
    """The reported defect: an 18m30s silent gap before "released".

    Logging only on CHANGE meant that during the ~15-20 min reclamation -- which emits
    no CloudFormation events -- the log went completely silent, so the teardown read as
    hung and the user only learned what happened after it finished. The wait must
    re-report periodically, with elapsed minutes, while the count is unchanged.
    """
    import itertools
    import time

    import dsql_migrator.core.cdc_deployer as cdc_deployer

    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    # ENIs stay pending for many polls, then clear.
    deployer = _FakeDeployer(
        existing=existing,
        stack_statuses=("DELETE_IN_PROGRESS",) * 20 + (None,),
        seeder_eni_counts=[2] * 20 + [0],
    )
    # Advance the monotonic clock 30s per read, mirroring the real 30s poll interval.
    clock = itertools.count(0.0, 30.0)
    original = time.monotonic
    time.monotonic = lambda: next(clock)
    try:
        run_cdc_delete(
            handle, stack_name=STACK, deployer=deployer, on_log=on_log,
            sleep=lambda _s: None,
            delete_timeout_seconds=1e9, poll_interval_seconds=0.0,
        )
    finally:
        time.monotonic = original

    waiting = [m for m in logs if "seeder network interface" in m]
    assert len(waiting) > 1, (
        f"the wait must be re-reported while it drags on, got {len(waiting)}: {waiting}"
    )
    # And the re-reports must carry elapsed time, so it reads as progressing.
    assert any("min so far" in m for m in waiting), (
        f"re-reports must include elapsed minutes; got {waiting}"
    )
    # The first line stays clean (no "0 min so far").
    assert "min so far" not in waiting[0]


def test_delete_no_release_line_when_enis_were_never_pending() -> None:
    """"Released" must only fire after we actually saw some pending.

    If the count is 0 from the first poll (a stack with no seeder, or already
    reclaimed), announcing "released" would be a message about something that never
    happened. The line is gated on having previously seen a positive count.
    """
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(
        existing=existing,
        stack_statuses=("DELETE_IN_PROGRESS", None),
        seeder_eni_counts=[0, 0],  # never any pending
    )
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert not any("released" in m.lower() for m in logs), (
        "must not claim ENIs were released when none were ever pending"
    )


def test_delete_without_eni_data_logs_no_eni_line() -> None:
    # seeder_eni_count returning None (SG unresolved / read error) must not emit a
    # line at all -- absence of data is not "released".
    handle = _FakeHandle()
    logs, on_log = _logs()
    existing = CdcStackDiscovery("CREATE_COMPLETE", {}, True)
    deployer = _FakeDeployer(
        existing=existing,
        stack_statuses=("DELETE_IN_PROGRESS", None),
        seeder_eni_counts=None,  # -> seeder_eni_count returns None
    )
    run_cdc_delete(
        handle, stack_name=STACK, deployer=deployer, on_log=on_log,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    assert not any("seeder network interface" in m for m in logs)


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


# ---------------------------------------------------------------------------
# _wait_connector_running: a connector-state READ failure must be surfaced, not
# masqueraded as "still creating" (the bug where a RUNNING source connector never
# advanced to the sink pass because connector_state kept returning None).
# ---------------------------------------------------------------------------


class _WaitDriver:
    """Minimal _StageDriver double for _wait_connector_running (no real sleeps)."""

    cancelled = False

    def __init__(self) -> None:
        self.logs: list[str] = []

    def heartbeat(self) -> None:
        pass

    def log(self, msg: str) -> None:
        self.logs.append(msg)

    def sleep(self, _seconds: float) -> None:
        pass  # never wait for real in tests


class _StateSeqDeployer:
    """Scripts connector_state as a sequence; an Exception item is RAISED (read error)."""

    def __init__(self, seq: list) -> None:
        self._seq = list(seq)

    def connector_state(self, _name: str):
        value = self._seq.pop(0) if len(self._seq) > 1 else self._seq[0]
        if isinstance(value, Exception):
            raise value
        return value

    def connector_log_tail(self, *_a, **_k) -> str:
        return ""


def _wait(deployer, *, timeout=1e9):
    _wait_connector_running(deployer, "c", "stage", _WaitDriver(), timeout, 0)


def test_wait_connector_running_returns_on_running() -> None:
    _wait(_StateSeqDeployer(["CREATING", "RUNNING"]))  # no raise = success


def test_wait_connector_running_tolerates_a_transient_read_blip() -> None:
    # One read error then RUNNING: a transient blip is tolerated (streak resets).
    _wait(_StateSeqDeployer([RuntimeError("throttled"), "RUNNING"]))


def test_wait_connector_running_surfaces_persistent_read_failure() -> None:
    # Every read fails (non-terminal): after the tolerance budget, fail WITH the
    # cause instead of looping on "creating…" forever.
    seq = [RuntimeError("kafkaconnect unreachable")] * (_MAX_STATE_READ_FAILURES + 1)
    with pytest.raises(CdcDeployError) as ei:
        _wait(_StateSeqDeployer(seq))
    assert "Could not read" in str(ei.value)


def test_wait_connector_running_fails_fast_on_expired_credentials() -> None:
    # A credential-expiry read error will not self-heal -> fail immediately (not
    # after burning the whole retry budget) with an actionable, retryable message.
    err = RuntimeError("ExpiredTokenException: the security token is expired")
    with pytest.raises(CdcDeployError) as ei:
        _wait(_StateSeqDeployer([err]))
    msg = str(ei.value)
    assert "Could not read" in msg and ("expired" in msg.lower() or "Credentials" in msg)
