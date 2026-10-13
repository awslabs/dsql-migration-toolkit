# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 S3 / deploy: how the CDC plugin artifacts reach the bucket and the cdc-stack.

* Every artifact goes up as ONE PutObject. An interrupted multipart upload leaves billed
  parts in the bucket that the app's roles cannot abort (no s3:AbortMultipartUpload); a
  single PUT that fails or is killed leaves nothing.
* Every artifact goes to a CONTENT-ADDRESSED key, ``cdc-plugins/<sha256[:16]>/<name>``.
  The bucket is shared by every tool build in an account and region and a CustomPlugin
  names no object version, so with one fixed key per artifact another build's upload
  could replace the bytes between a deploy's upload and its CustomPlugin's creation --
  including the minutes before the AZ retry re-creates the stack.
* The keys reach the stack at create, and an EXISTING stack keeps the keys it was created
  with: Start and Stop send every plugin parameter as UsePreviousValue. A changed FileKey
  makes CloudFormation replace the custom-named CustomPlugin, which it refuses.

Fakes only: no AWS and no network (the one real boto3 client is short-circuited before any
HTTP request is made, and fails its test if anything opens a connection).
"""

from __future__ import annotations

import hashlib
import re
import socket
from pathlib import Path

import pytest

from dsql_migrator.core import s3_provision
from dsql_migrator.core.cdc import (
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
    cdc_expected_connector_names,
)
from dsql_migrator.core.cdc_deployer import (
    run_cdc_infra_deploy,
    run_cdc_start,
    run_cdc_stop,
)
from dsql_migrator.core.cdc_postgres import (
    build_pg_cdc_infra_params,
    build_pg_cdc_stack_params,
    build_pg_source_config,
)
from dsql_migrator.core.cdc_stack_deployer import CdcStackDeployer
from dsql_migrator.core.models import MigrationJob, SourceType, TableDef
from dsql_migrator.core.s3_provision import (
    DEBEZIUM_PG_PLUGIN_KEY,
    DEBEZIUM_PLUGIN_KEY,
    DSQL_SINK_PLUGIN_KEY,
    LAMBDA_SEEDER_KEY,
    ensure_and_upload_plugins,
    upload_plugin,
)

_REGION = "us-east-1"
_ACCOUNT = "123456789012"
_BUCKET = s3_provision.plugin_bucket_name(_ACCOUNT, _REGION)
_STACK = "dsql-cdc-g25"
_BASE_KEYS = (DEBEZIUM_PLUGIN_KEY, DEBEZIUM_PG_PLUGIN_KEY, DSQL_SINK_PLUGIN_KEY, LAMBDA_SEEDER_KEY)
# The cdc-stack parameters that locate (or name) a plugin artifact.
_KEY_PARAMS = (
    "DebeziumPluginS3Key",
    "DebeziumPostgresPluginS3Key",
    "DsqlSinkPluginS3Key",
    "LambdaSeederS3Key",
)
_PLUGIN_PARAMS = ("PluginBucketArn", *_KEY_PARAMS, "PluginVersion")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _expected_key(base_key: str, path: Path) -> str:
    """The key format itself, spelled out: cdc-plugins/<sha256[:16]>/<artifact name>."""
    return f"cdc-plugins/{_sha256(path.read_bytes())[:16]}/{base_key.rpartition('/')[2]}"


class _Bucket:
    """One unversioned bucket shared by every build: a write replaces the object."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict]] = {}
        self.puts: list[str] = []

    def head_bucket(self, **_kw):
        return {}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise RuntimeError("404 Not Found")
        body, metadata = self.objects[Key]
        # A multipart-style ETag, so a skip has to rest on the stamped SHA-256.
        return {"ContentLength": len(body), "ETag": '"0f-2"', "Metadata": dict(metadata)}

    def put_object(self, Bucket, Key, Body, Metadata=None):  # noqa: N803
        self.objects[Key] = (bytes(Body), dict(Metadata or {}))
        self.puts.append(Key)
        return {}


class _Sts:
    def get_caller_identity(self):
        return {"Account": _ACCOUNT}


def _build(tmp_path: Path, name: str) -> dict[str, Path]:
    """One tool build's four artifacts, by base key; each build has its own bytes."""
    root = tmp_path / name
    root.mkdir()
    files = {}
    for base_key in _BASE_KEYS:
        path = root / base_key.rpartition("/")[2]
        path.write_bytes(f"{name}:{base_key};".encode() * 64)
        files[base_key] = path
    return files


def _use(monkeypatch, files: dict[str, Path]) -> None:
    """Make the deploy read ``files`` (it finds its artifacts through _artifact_paths)."""
    monkeypatch.setattr(
        s3_provision, "_artifact_paths", lambda: tuple(files[k] for k in _BASE_KEYS)
    )


# ---------------------------------------------------------------------------
# One PutObject per artifact (residual: s3:AbortMultipartUpload)
# ---------------------------------------------------------------------------


def test_a_managed_upload_is_told_to_use_one_put(tmp_path: Path) -> None:
    received = []

    class _Managed(_Bucket):
        def upload_file(self, filename, bucket, key, ExtraArgs=None, Config=None):  # noqa: N803
            received.append((key, ExtraArgs, Config))

    artifact = tmp_path / "a.zip"
    artifact.write_bytes(b"x" * 10)
    upload_plugin(_Managed(), _BUCKET, "cdc-plugins/0123/a.zip", artifact)
    ((key, extra_args, config),) = received
    assert key == "cdc-plugins/0123/a.zip"
    assert extra_args == {"Metadata": {"sha256": _sha256(b"x" * 10)}}
    assert config is not None
    assert config.multipart_threshold == s3_provision._SINGLE_PUT_THRESHOLD


class _Http:
    def __init__(self, status: int) -> None:
        self.status_code = status
        self.headers: dict = {}


@pytest.mark.parametrize(
    "exported",
    [
        {},
        # A developer's named profile, or the one a credentials launcher exports -- even
        # "default": the session below reads no config file, so botocore found no such
        # profile and raised ProfileNotFound before the upload ran.
        {"AWS_PROFILE": "default"},
        {"AWS_DEFAULT_PROFILE": "g25-dev-profile"},
        # "auto" makes client creation ask the instance metadata service for the region.
        {"AWS_DEFAULTS_MODE": "auto"},
    ],
    ids=["clean-shell", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_DEFAULTS_MODE"],
)
def test_a_real_boto3_upload_above_8_mib_is_one_put_object(
    exported, tmp_path: Path, monkeypatch
) -> None:
    """Through the INSTALLED boto3/s3transfer: boto3's default multipart_threshold is 8 MiB,
    so before G25 this 9 MiB upload was CreateMultipartUpload + 2 UploadPart + Complete.
    Whatever the shell running pytest exports, the test passes and opens no connection."""
    import boto3
    import botocore.session

    for name, value in exported.items():
        monkeypatch.setenv(name, value)
    connects: list = []

    def _no_network(_sock, address):
        connects.append(address)
        raise OSError("this test opens no connection")

    monkeypatch.setattr(socket.socket, "connect", _no_network)

    calls: list[tuple[str, dict]] = []

    def _short_circuit(model, params, **_kw):
        # before-call returns a response, so botocore never sends an HTTP request.
        calls.append((model.name, params))
        if model.name == "HeadObject":
            return _Http(404), {
                "Error": {"Code": "404", "Message": "Not Found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }
        parsed = {"ResponseMetadata": {"HTTPStatusCode": 200}, "ETag": '"e"'}
        if model.name == "CreateMultipartUpload":
            parsed["UploadId"] = "u-1"
        return _Http(200), parsed

    # A session that reads no AWS config or credentials file from this machine, and none of
    # the variables that would send it looking: a profile named by AWS_PROFILE or
    # AWS_DEFAULT_PROFILE (not in the absent config file) and AWS_DEFAULTS_MODE=auto.
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_DEFAULTS_MODE"):
        monkeypatch.delenv(name, raising=False)
    core = botocore.session.Session()
    core.set_config_variable("config_file", str(tmp_path / "no-config"))
    core.set_config_variable("credentials_file", str(tmp_path / "no-credentials"))
    client = boto3.session.Session(
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name=_REGION,
        botocore_session=core,
    ).client("s3")
    client.meta.events.register("before-call.s3", _short_circuit)

    artifact = tmp_path / "dsql-sink-connector.zip"
    with artifact.open("wb") as fh:
        fh.truncate(9 * 1024 * 1024)
    upload_plugin(client, _BUCKET, "cdc-plugins/0123/dsql-sink-connector.zip", artifact)

    assert [name for name, _ in calls] == ["HeadObject", "PutObject"]
    put = calls[1][1]
    assert put["headers"]["x-amz-meta-sha256"] == _sha256(artifact.read_bytes())
    # Streamed from the file, never read into memory whole.
    assert not isinstance(put["body"], (bytes, bytearray))
    assert connects == []


def test_every_bundled_artifact_is_below_the_single_put_threshold() -> None:
    plugins = s3_provision._repo_root() / "connectors" / "plugins"
    bundled = sorted(plugins.glob("*.zip"))
    uploaded = {
        Path(rel).name
        for rel in (
            s3_provision._DEBEZIUM_PLUGIN_RELPATH,
            s3_provision._DEBEZIUM_PG_PLUGIN_RELPATH,
            s3_provision._DSQL_SINK_PLUGIN_RELPATH,
            s3_provision._LAMBDA_SEEDER_RELPATH,
        )
    }
    # The four the deploy uploads, the seeder zip among them, are all checked here.
    assert uploaded <= {p.name for p in bundled}
    for artifact in bundled:
        size = artifact.stat().st_size
        # At or over the threshold the upload silently becomes multipart again: raise the
        # threshold (S3 takes up to 5 GiB in one PUT) or grant s3:AbortMultipartUpload.
        assert size < s3_provision._SINGLE_PUT_THRESHOLD, (artifact.name, size)
    assert s3_provision._SINGLE_PUT_THRESHOLD < 5 * 1024**3


# ---------------------------------------------------------------------------
# Content-addressed keys (confirmed[17])
# ---------------------------------------------------------------------------


def test_each_artifact_is_uploaded_to_the_key_its_bytes_name(
    tmp_path: Path, monkeypatch
) -> None:
    files = _build(tmp_path, "a")
    _use(monkeypatch, files)
    bucket = _Bucket()
    result = ensure_and_upload_plugins(bucket, _Sts(), _REGION)  # engine unknown: all four
    returned = {
        DEBEZIUM_PLUGIN_KEY: result.debezium_key,
        DEBEZIUM_PG_PLUGIN_KEY: result.debezium_pg_key,
        DSQL_SINK_PLUGIN_KEY: result.dsql_sink_key,
        LAMBDA_SEEDER_KEY: result.lambda_seeder_key,
    }
    for base_key, path in files.items():
        key = _expected_key(base_key, path)
        assert returned[base_key] == key
        body, metadata = bucket.objects[key]
        assert body == path.read_bytes()
        assert metadata == {"sha256": _sha256(body)}
    assert sorted(bucket.puts) == sorted(returned.values())
    # Nothing writes the fixed pre-G25 keys any more (a pre-G25 stack still names them).
    assert not set(_BASE_KEYS) & set(bucket.objects)


def test_a_rebuilt_artifact_gets_a_new_key_and_never_replaces_the_old_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    files = _build(tmp_path, "a")
    _use(monkeypatch, files)
    bucket = _Bucket()
    first = ensure_and_upload_plugins(bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL)
    old_sink = files[DSQL_SINK_PLUGIN_KEY].read_bytes()
    # Rebuilt in place, same size, other bytes: how v52 and v53 were each rebuilt.
    new_sink = bytes(reversed(old_sink))
    assert new_sink != old_sink and len(new_sink) == len(old_sink)
    files[DSQL_SINK_PLUGIN_KEY].write_bytes(new_sink)
    bucket.puts.clear()

    second = ensure_and_upload_plugins(bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL)
    assert second.dsql_sink_key != first.dsql_sink_key
    assert bucket.puts == [second.dsql_sink_key]  # only the changed artifact goes up
    # A stack created from the first upload still finds exactly its own bytes.
    assert bucket.objects[first.dsql_sink_key][0] == old_sink
    assert bucket.objects[second.dsql_sink_key][0] == new_sink
    assert (second.debezium_key, second.lambda_seeder_key) == (
        first.debezium_key,
        first.lambda_seeder_key,
    )


def test_a_content_key_is_skipped_only_when_its_object_is_proven(
    tmp_path: Path, monkeypatch
) -> None:
    # The G21 skip rule, unchanged on the content-addressed key: existence + size + the
    # stamped SHA-256 (or a single-part MD5 ETag). The key's name proves nothing by itself.
    files = _build(tmp_path, "a")
    _use(monkeypatch, files)
    bucket = _Bucket()
    first = ensure_and_upload_plugins(bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL)
    bucket.puts.clear()
    logs: list[str] = []
    again = ensure_and_upload_plugins(
        bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL, on_progress=logs.append
    )
    assert again == first
    assert bucket.puts == []
    assert f"{first.dsql_sink_key}: already up to date — skipping upload." in logs

    sink = files[DSQL_SINK_PLUGIN_KEY].read_bytes()
    bucket.objects[first.dsql_sink_key] = (b"?" * len(sink), {})  # same size, unproven
    ensure_and_upload_plugins(bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL)
    assert bucket.puts == [first.dsql_sink_key]
    assert bucket.objects[first.dsql_sink_key] == (sink, {"sha256": _sha256(sink)})


# --- The race itself: another build's upload during this deploy's AZ retry ----------


class _Handle:
    def __init__(self) -> None:
        self.job = MigrationJob(job_id="G25")

    cancelled = False

    def update(self, mutator) -> None:
        mutator(self.job)


class _Ec2:
    """Three NAT-egress AZs, so the AZ retry can re-select two supported ones."""

    def describe_subnets(self, **_kw):
        return {
            "Subnets": [
                {"SubnetId": f"subnet-{z}", "AvailabilityZone": f"{_REGION}{z}"}
                for z in "abd"
            ]
        }

    def describe_route_tables(self, **_kw):
        return {
            "RouteTables": [
                {
                    "Associations": [{"Main": True}],
                    "Routes": [{"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": "nat-1"}],
                }
            ]
        }


class _CfnRegisteringPlugins:
    """CloudFormation + MSK Connect as far as the plugin bytes go: create_stack registers
    whatever the bucket holds at each key it names (a CustomPlugin names no object
    version). The first create rolls back on an unsupported AZ, another build's deploy
    uploads while it does, and the AZ retry deletes the stack and creates it again."""

    region = _REGION
    template_s3_bucket = None

    def __init__(self, bucket: _Bucket, other_build_deploys) -> None:
        self._bucket = bucket
        self._other_build_deploys = other_build_deploys
        self._statuses = ["ROLLBACK_COMPLETE", None, "CREATE_COMPLETE"]
        self.calls: list[str] = []
        self.created: list[dict[str, str]] = []
        self.registered: list[dict[str, bytes]] = []

    def describe_stack_or_none(self, _name):
        return None

    def create_stack(self, _name, _template_body, parameters) -> None:
        self.calls.append("create_stack")
        params = dict(parameters)
        self.created.append(params)
        self.registered.append(
            {k: self._bucket.objects[params[k]][0] for k in _KEY_PARAMS if params.get(k)}
        )

    def delete_stack(self, _name) -> None:
        self.calls.append("delete_stack")

    def find_unsupported_azs(self, _name):
        return {f"{_REGION}d"}

    def poll_events(self, _name, _since):
        return []

    def seeder_eni_count(self, _name):
        return None

    def connector_log_tail(self, *_args, **_kw):
        return ""

    def stack_status_checked(self, _name):
        status = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        if status == "ROLLBACK_COMPLETE" and self._other_build_deploys is not None:
            self._other_build_deploys()  # while this deploy's first attempt rolls back
            self._other_build_deploys = None
        return status

    stack_status = stack_status_checked


def _mysql_infra_params():
    src = DebeziumSourceConfig(name="mysql-source", table_include_list=["shop.orders"])
    sink = SinkConnectorConfig(
        name="mysql-sink", topics=["shop.orders"], dlq_topic="dsql-sink-dlq"
    )
    # As the UI builds them: plugin bucket and keys empty, filled in by the deploy job.
    return build_cdc_infra_params(
        src, sink,
        vpc_id="vpc-1", connector_subnet_ids="subnet-a,subnet-d",
        plugin_bucket_arn="", debezium_plugin_s3_key="", dsql_sink_plugin_s3_key="",
        source_db_hostname="db.example.com",
        source_secret_arn=f"arn:aws:secretsmanager:{_REGION}:{_ACCOUNT}:secret:s",
        source_secret_name="s",
        dsql_cluster_arn=f"arn:aws:dsql:{_REGION}:{_ACCOUNT}:cluster/c",
        target_endpoint="c.dsql.us-east-1.on.aws",
        stack_name=_STACK,
    )


@pytest.mark.parametrize("other_build", ["pre-G25 fixed keys", "G25 content keys"])
def test_another_builds_upload_cannot_change_what_the_az_retry_registers(
    other_build, tmp_path: Path, monkeypatch
) -> None:
    mine = _build(tmp_path, "this-build")
    theirs = _build(tmp_path, "other-build")
    bucket = _Bucket()

    def other_build_deploys() -> None:
        if other_build == "pre-G25 fixed keys":
            # The deployed older app: it uploads to the fixed, shared keys.
            for base_key in (DEBEZIUM_PLUGIN_KEY, DSQL_SINK_PLUGIN_KEY, LAMBDA_SEEDER_KEY):
                upload_plugin(bucket, _BUCKET, base_key, theirs[base_key])
        else:
            _use(monkeypatch, theirs)
            try:
                ensure_and_upload_plugins(
                    bucket, _Sts(), _REGION, source_engine=SourceType.MYSQL
                )
            finally:
                _use(monkeypatch, mine)

    _use(monkeypatch, mine)
    deployer = _CfnRegisteringPlugins(bucket, other_build_deploys)
    run_cdc_infra_deploy(
        _Handle(),
        stack_name=_STACK, template_body="T", params=_mysql_infra_params(),
        deployer=deployer, on_log=lambda _ts, _msg: None,
        region=_REGION, s3_client=bucket, sts_client=_Sts(), ec2_client=_Ec2(),
        create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        delete_timeout_seconds=5.0, sleep=lambda _s: None,
    )

    assert deployer.calls == ["create_stack", "delete_stack", "create_stack"]
    # The other build really did upload different bytes in between.
    their_sink = theirs[DSQL_SINK_PLUGIN_KEY].read_bytes()
    assert any(body == their_sink for body, _ in bucket.objects.values())
    expected = {
        "DebeziumPluginS3Key": _expected_key(DEBEZIUM_PLUGIN_KEY, mine[DEBEZIUM_PLUGIN_KEY]),
        "DsqlSinkPluginS3Key": _expected_key(DSQL_SINK_PLUGIN_KEY, mine[DSQL_SINK_PLUGIN_KEY]),
        "LambdaSeederS3Key": _expected_key(LAMBDA_SEEDER_KEY, mine[LAMBDA_SEEDER_KEY]),
    }
    mine_bytes = {param: mine[base].read_bytes() for param, base in (
        ("DebeziumPluginS3Key", DEBEZIUM_PLUGIN_KEY),
        ("DsqlSinkPluginS3Key", DSQL_SINK_PLUGIN_KEY),
        ("LambdaSeederS3Key", LAMBDA_SEEDER_KEY),
    )}
    # Both attempts register exactly the bytes THIS deploy hashed (before G25 the retry
    # registered the other build's sink under this build's plugin name, with no error)...
    assert deployer.registered == [mine_bytes, mine_bytes]
    for params in deployer.created:
        # ...because both name the same content-addressed keys.
        assert {k: params.get(k, "") for k in expected} == expected
        assert params["PluginBucketArn"] == f"arn:aws:s3:::{_BUCKET}"


# ---------------------------------------------------------------------------
# An existing stack keeps its plugin keys: Start and Stop through the REAL deployer
# ---------------------------------------------------------------------------


class _FakeCfn:
    def __init__(self, parameters: dict[str, str]) -> None:
        self.parameters = dict(parameters)
        self.status = "CREATE_COMPLETE"
        self.updates: list[list[dict]] = []

    def describe_stacks(self, StackName):  # noqa: N803
        return {
            "Stacks": [
                {
                    "StackName": StackName,
                    "StackStatus": self.status,
                    "Parameters": [
                        {"ParameterKey": k, "ParameterValue": v}
                        for k, v in self.parameters.items()
                    ],
                    "Outputs": [
                        {
                            "OutputKey": "MskClusterArn",
                            "OutputValue": f"arn:aws:kafka:{_REGION}:{_ACCOUNT}:cluster/x/y",
                        }
                    ],
                }
            ]
        }

    def update_stack(self, StackName, Parameters, Capabilities, **_template):  # noqa: N803
        self.updates.append(list(Parameters))
        for p in Parameters:
            if "ParameterValue" in p:
                self.parameters[p["ParameterKey"]] = p["ParameterValue"]
        self.status = "UPDATE_COMPLETE"
        return {}

    def describe_stack_events(self, StackName):  # noqa: N803
        return {"StackEvents": []}


class _FakeKafka:
    def get_bootstrap_brokers(self, ClusterArn):  # noqa: N803
        return {"BootstrapBrokerStringSaslIam": "b-1.example:9098"}


class _FakeKafkaConnect:
    """RUNNING connectors while the stack has a bootstrap string (Start), none after Stop."""

    def __init__(self, cfn: _FakeCfn) -> None:
        self._cfn = cfn

    def list_connectors(self, **_kw):
        if not self._cfn.parameters.get("MskBootstrapServers"):
            return {"connectors": []}
        return {
            "connectors": [
                {"connectorName": name, "connectorState": "RUNNING"}
                for name in cdc_expected_connector_names(_STACK)
            ]
        }


class _FakeSession:
    def __init__(self, cfn: _FakeCfn) -> None:
        self._clients = {
            "cloudformation": cfn,
            "kafka": _FakeKafka(),
            "kafkaconnect": _FakeKafkaConnect(cfn),
            "logs": object(),
        }

    def client(self, service_name, region_name=None):
        return self._clients[service_name]


def _pg_source():
    tables = [TableDef(name="app.orders", primary_key=["id"])]
    return build_pg_source_config(
        "pg-source", tables, None,
        database_name="app", slot_name="dsqlmig_s1", publication_name="dsqlmig_p1",
    )


def _pg_sink():
    return SinkConnectorConfig(name="pg-sink", topics=["app.orders"], dlq_topic="dsql-sink-dlq")


def _deployed(engine: str, keys: str) -> dict[str, str]:
    """A created stack's parameters, its plugin keys ``"fixed"`` (a stack deployed before
    G25) or ``"content"``-addressed. DescribeStacks lists every template parameter, the
    other engine's empty plugin key included."""
    if engine == "mysql":
        params = dict(_mysql_infra_params().filled)
    else:
        params = dict(
            build_pg_cdc_infra_params(
                _pg_source(), _pg_sink(),
                vpc_id="vpc-1", plugin_bucket_arn="", dsql_sink_plugin_s3_key="",
                source_db_hostname="pg.example.com",
                source_secret_arn=f"arn:aws:secretsmanager:{_REGION}:{_ACCOUNT}:secret:s",
                source_secret_name="s",
                dsql_cluster_arn=f"arn:aws:dsql:{_REGION}:{_ACCOUNT}:cluster/c",
                target_endpoint="c.dsql.us-east-1.on.aws", stack_name=_STACK,
            ).filled
        )
    content = "cdc-plugins/0123456789abcdef/" if keys == "content" else "cdc-plugins/"
    params.update(
        PluginBucketArn=f"arn:aws:s3:::{_BUCKET}",
        DebeziumPluginS3Key=(content + "debezium-mysql-plugin.zip") if engine == "mysql" else "",
        DebeziumPostgresPluginS3Key=(
            (content + "debezium-postgres-plugin.zip") if engine == "postgres" else ""
        ),
        DsqlSinkPluginS3Key=content + "dsql-sink-connector.zip",
        LambdaSeederS3Key=(content + "offset-seeder-lambda.zip") if engine == "mysql" else "",
        PluginVersion="v53" if keys == "content" else "v52",
    )
    return params


@pytest.mark.parametrize("keys", ["fixed", "content"], ids=["pre-G25 stack", "G25 stack"])
@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_start_and_stop_send_every_plugin_parameter_as_use_previous_value(
    engine, keys, monkeypatch
) -> None:
    from dsql_migrator.core import ec2_metadata

    # The PostgreSQL Start's in-process seed asks for this host's address: none, here.
    monkeypatch.setattr(ec2_metadata, "local_ipv4", lambda: None)
    deployed = _deployed(engine, keys)
    cfn = _FakeCfn(deployed)
    deployer = CdcStackDeployer(_REGION, session=_FakeSession(cfn))
    if engine == "mysql":
        src = DebeziumSourceConfig(name="mysql-source", table_include_list=["shop.orders"])
        sink = SinkConnectorConfig(
            name="mysql-sink", topics=["shop.orders"], dlq_topic="dsql-sink-dlq"
        )
        start_params = build_cdc_stack_params(
            src, sink, target_endpoint="c.dsql.us-east-1.on.aws", stack_name=_STACK
        )
        seed = {}
    else:
        start_params = build_pg_cdc_stack_params(
            _pg_source(), _pg_sink(), target_endpoint="c.dsql.us-east-1.on.aws",
            stack_name=_STACK,
        )
        # PostgreSQL stacks are SeedMode=External; the in-process Kafka prep is stubbed.
        seed = {"seed_mode": "external", "seed_fn": lambda **_kw: "stubbed"}
    common = dict(stack_name=_STACK, deployer=deployer, on_log=lambda _ts, _msg: None,
                  poll_interval_seconds=0.0, sleep=lambda _s: None)

    run_cdc_start(_Handle(), params=start_params, connector_timeout_seconds=5.0, **seed, **common)
    run_cdc_stop(_Handle(), stop_timeout_seconds=5.0, **common)

    assert len(cfn.updates) == 2  # the Start pass, then the Stop
    for update in cfn.updates:
        sent = {p["ParameterKey"]: p for p in update}
        for key in _PLUGIN_PARAMS:
            assert sent[key] == {"ParameterKey": key, "UsePreviousValue": True}, key
    # So the stack still names exactly the plugin objects it was created with.
    assert {k: cfn.parameters[k] for k in _PLUGIN_PARAMS} == {
        k: deployed[k] for k in _PLUGIN_PARAMS
    }


# ---------------------------------------------------------------------------
# IAM: every identity that writes or reads the plugins covers the nested keys
# ---------------------------------------------------------------------------


def _load_template(relpath: str) -> dict:
    yaml = pytest.importorskip("yaml")

    class _Loader(yaml.SafeLoader):
        pass

    def _any(loader, _suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    _Loader.add_multi_constructor("!", _any)
    path = s3_provision._repo_root() / "deploy" / relpath
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_Loader)


def _statements(template: dict, role: str) -> list[dict]:
    out = []
    for policy in template["Resources"][role]["Properties"].get("Policies", []) or []:
        if isinstance(policy, dict) and "Fn::If" in policy:
            policy = policy["Fn::If"][1]  # the policy that is present when the condition holds
        out.extend(policy["PolicyDocument"]["Statement"])
    return out


def _resolved(resource) -> str:
    if isinstance(resource, dict):  # only Fn::Sub can name these objects
        resource = resource.get("Fn::Sub", "")
        if isinstance(resource, list):  # Fn::Sub: [template, {variables}]
            resource = resource[0]
    text = str(resource)
    names = {
        "AWS::Partition": "aws",
        "AWS::AccountId": _ACCOUNT,
        "AWS::Region": _REGION,
        "PluginBucketArn": f"arn:aws:s3:::{_BUCKET}",
    }
    for name, value in names.items():
        text = text.replace("${" + name + "}", value)
    return text


def _iam_matches(pattern: str, arn: str) -> bool:
    """IAM resource matching: '*' is any run of characters, '/' included; '?' is one."""
    regex = "".join(
        ".*" if ch == "*" else "." if ch == "?" else re.escape(ch) for ch in pattern
    )
    return re.fullmatch(regex, arn) is not None


@pytest.mark.parametrize(
    "template,role,action",
    [
        # The app's own identity uploads (PutObject) and proves (HeadObject = GetObject).
        ("cloudformation.yaml", "TaskRole", "s3:PutObject"),
        ("cloudformation.yaml", "TaskRole", "s3:GetObject"),
        ("cloudformation-ec2.yaml", "HostRole", "s3:PutObject"),
        ("cloudformation-ec2.yaml", "HostRole", "s3:GetObject"),
        # CloudFormation creates the CustomPlugins and the seeder Lambda as CdcDeployRole.
        ("cloudformation.yaml", "CdcDeployRole", "s3:GetObject"),
        ("cloudformation-ec2.yaml", "CdcDeployRole", "s3:GetObject"),
        # The connectors' execution role reads the plugin objects too.
        ("cdc-stack/cdc-stack.yaml", "ConnectorExecutionRole", "s3:GetObject"),
    ],
)
def test_the_plugin_roles_cover_the_content_addressed_keys(template, role, action) -> None:
    statements = _statements(_load_template(template), role)
    for base_key in _BASE_KEYS:
        arn = f"arn:aws:s3:::{_BUCKET}/{s3_provision._content_key(base_key, '0' * 64)}"
        granted = False
        for st in statements:
            actions = st.get("Action")
            actions = actions if isinstance(actions, list) else [actions]
            resources = st.get("Resource")
            resources = resources if isinstance(resources, list) else [resources]
            if st.get("Effect") == "Allow" and action in actions and any(
                _iam_matches(_resolved(r), arn) for r in resources
            ):
                granted = True
        assert granted, (template, role, action, arn)
