# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Structural guards for the Lambda-free "EC2 + MSK only" app-stack
(deploy/cloudformation-ec2.yaml, Option 3).

The EC2 stack forks the Fargate app-stack into a single in-VPC host reached via
SSM port-forward: no ALB/ACM/Cognito/ECS, state on a retained EBS volume, and the
in-process CDC seed (SeedMode=External) enabled by user-data. These tests parse
the template (tolerating CFN intrinsic tags) and assert the shape the design
requires, so a future edit can't silently reintroduce the ALB tier, drop the EBS
retain policy, lose the data-plane MSK IAM the External seed needs, write a
secret the user data reads into its trace, or mount the state volume by a device
name that can change from one boot to the next.
"""

from __future__ import annotations

import pathlib
import re

import pytest

yaml = pytest.importorskip("yaml")


def _load():
    class _L(yaml.SafeLoader):
        pass

    def _any(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    _L.add_multi_constructor("!", _any)
    path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "deploy" / "cloudformation-ec2.yaml"
    )
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_L)


def test_top_level_description_within_cfn_1024_byte_limit() -> None:
    # CloudFormation caps the template-level Description at 1024 bytes and rejects
    # the template at ValidateTemplate/CreateStack otherwise — a hard deploy blocker.
    desc = _load()["Description"]
    assert len(desc.encode("utf-8")) <= 1024, len(desc.encode("utf-8"))


def test_parses_and_has_no_alb_cognito_ecs() -> None:
    res = _load()["Resources"]
    forbidden = {
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        "AWS::ElasticLoadBalancingV2::TargetGroup",
        "AWS::ElasticLoadBalancingV2::Listener",
        "AWS::Cognito::UserPool",
        "AWS::Cognito::UserPoolClient",
        "AWS::Cognito::UserPoolDomain",
        "AWS::ECS::Cluster",
        "AWS::ECS::Service",
        "AWS::ECS::TaskDefinition",
    }
    types = {v.get("Type") for v in res.values() if isinstance(v, dict)}
    assert not (types & forbidden), types & forbidden


def test_core_resources_present() -> None:
    res = _load()["Resources"]
    for name, typ in [
        ("AppHost", "AWS::EC2::Instance"),
        ("StateVolume", "AWS::EC2::Volume"),
        ("StateVolumeAttachment", "AWS::EC2::VolumeAttachment"),
        ("HostInstanceProfile", "AWS::IAM::InstanceProfile"),
        ("HostRole", "AWS::IAM::Role"),
        ("CdcDeployRole", "AWS::IAM::Role"),
        ("HostSecurityGroup", "AWS::EC2::SecurityGroup"),
    ]:
        assert name in res, name
        assert res[name]["Type"] == typ, (name, res[name]["Type"])


def test_state_volume_is_retained() -> None:
    # The accepted single-host failure model still requires state integrity: the
    # EBS volume must survive instance replacement so job/session SQLite resumes.
    sv = _load()["Resources"]["StateVolume"]
    assert sv.get("DeletionPolicy") == "Retain"
    assert sv.get("UpdateReplacePolicy") == "Retain"


def test_security_group_has_no_ingress_and_egress_9098() -> None:
    sg = _load()["Resources"]["HostSecurityGroup"]["Properties"]
    # No inbound: SSM port-forward uses the agent's outbound channel.
    assert not sg.get("SecurityGroupIngress")
    egress_ports = {e.get("FromPort") for e in sg.get("SecurityGroupEgress", [])}
    assert {443, 5432, 9098} <= egress_ports, egress_ports


def test_host_role_has_dataplane_msk_and_ssm() -> None:
    role = _load()["Resources"]["HostRole"]["Properties"]
    # SSM Session Manager managed policy (for the port-forward).
    managed = str(role.get("ManagedPolicyArns"))
    assert "AmazonSSMManagedInstanceCore" in managed
    # Data-plane kafka-cluster actions for the in-process seed (mirrors OffsetSeederRole).
    actions = str(role["Policies"])
    for act in (
        "kafka-cluster:Connect",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
    ):
        assert act in actions, act


def test_host_role_reuses_taskrole_sids() -> None:
    # The instance profile must carry the same app permissions the Fargate TaskRole
    # had, or the app breaks on EC2 (DSQL connect, plugin bucket, assume deploy role).
    actions = str(_load()["Resources"]["HostRole"]["Properties"]["Policies"])
    for sid in ("DsqlConnect", "ProvisionCdcSourceSecret", "DiscoverCdcConnectors",
                "ManagePluginBucket", "AssumeCdcDeployRole"):
        assert sid in actions, sid


def test_cdc_deploy_role_trusts_host_role() -> None:
    trust = _load()["Resources"]["CdcDeployRole"]["Properties"][
        "AssumeRolePolicyDocument"
    ]["Statement"][0]["Principal"]
    assert trust == {"AWS": {"Fn::GetAtt": ["HostRole", "Arn"]}}


def test_user_data_sets_external_seed_and_ebs_state() -> None:
    ud = _load()["Resources"]["AppHost"]["Properties"]["UserData"]
    # UserData is Fn::Base64 of an Fn::Sub; flatten to text to assert content.
    text = str(ud)
    assert "DSQL_MIGRATOR_CDC_SEED_MODE=external" in text
    assert "AWS_STS_REGIONAL_ENDPOINTS=regional" in text
    # State on the retained EBS mount (host paths, not the old container /state bind).
    assert "/var/lib/dsql-migrator/job_state.sqlite" in text
    assert "/var/lib/dsql-migrator/session_state.sqlite" in text
    # State buckets intentionally NOT set (EBS-SQLite branch selected).
    assert "DSQL_MIGRATOR_JOB_STATE_BUCKET" not in text
    assert "DSQL_MIGRATOR_SESSION_STATE_BUCKET" not in text


def test_user_data_runs_from_source_not_docker() -> None:
    # The minimal mode runs from source (git clone / S3 tarball) via uv + systemd,
    # with NO Docker and NO ECR image.
    ud = str(_load()["Resources"]["AppHost"]["Properties"]["UserData"])
    assert "docker" not in ud  # no Docker install / run
    assert "uv sync" in ud and "--extra cdc-external" in ud  # deps incl. seed client
    assert "git clone" in ud
    assert "dsql-migrator.service" in ud  # systemd unit
    assert "/opt/dsql-migrator/.venv/bin/mysql-dsql-migrator ui" in ud  # ExecStart


def test_user_data_source_mode_branches() -> None:
    # SourceMode selects S3-tarball (restricted network / local copy) vs git clone.
    ud = str(_load()["Resources"]["AppHost"]["Properties"]["UserData"])
    assert 'if [ "${!SOURCE_MODE}" = "s3" ]' in ud  # S3 tarball branch
    # ...downloaded only from a bucket this account owns (test_s3_bucket_owner runs it).
    assert "aws s3api get-object" in ud and "--expected-bucket-owner ${AWS::AccountId}" in ud
    assert "git clone" in ud  # public-HTTPS git branch


def test_no_internal_gitlab_deploy_key_path() -> None:
    # The pre-publication internal-GitLab SSH clone path was removed for the public
    # release; guard against a silent reintroduction. Source acquisition is public
    # HTTPS git clone or an S3 tarball only -- no deploy key, no port-22 egress.
    doc = _load()
    assert "HasDeployKey" not in doc.get("Conditions", {})
    assert "GitSshEgress" not in doc["Resources"]
    assert "DeployKeySsmParam" not in doc["Parameters"]
    sg = doc["Resources"]["HostSecurityGroup"]["Properties"]
    egress_ports = {e.get("FromPort") for e in sg.get("SecurityGroupEgress", [])}
    assert 22 not in egress_ports, "port-22 SSH egress must not be reintroduced"
    ud = str(doc["Resources"]["AppHost"]["Properties"]["UserData"])
    assert "gitlab" not in ud.lower()
    assert "get-parameter --with-decryption" not in ud


def test_params_source_model_dropped_and_added() -> None:
    params = _load()["Parameters"]
    # New source-acquisition params.
    for added in ("SourceMode", "SourceS3Uri", "SourceRepoUrl", "SourceRepoRef",
                  "InstanceType", "HostSubnetId",
                  "StateVolumeSizeGiB", "MskEgressCidr", "LatestAl2023Ami"):
        assert added in params, added
    # Docker/ECR + ALB/Cognito/Fargate params gone.
    for dropped in ("ContainerImageUri", "CertificateArn", "AlbScheme",
                    "AssignPublicIp", "EnableCognitoAuth", "ContainerCpu",
                    "ContainerMemory"):
        assert dropped not in params, dropped
    assert "VpcId" in params  # the one irreducible operator input, reused
    assert params["SourceMode"]["Default"] == "git"


# -- the user data's trace and the files it writes ------------------------------

_XTRACE_ON = re.compile(r"^\{?\s*set\s+(?:-[a-z]*x|-o\s+xtrace\b)")
_XTRACE_OFF = re.compile(r"^\{?\s*set\s+(?:\+[a-z]*x|\+o\s+xtrace\b)")
_HEREDOC = re.compile(r"(?<!<)<<(?!<)-?\s*['\"]?(\w+)")
# The commands that read a secret: their output, assigned to a variable, is the secret.
_SECRET_SOURCES = ("secretsmanager get-secret-value", "get-random-password", "--with-decryption")


def _user_data() -> str:
    """The host's user-data script as the template writes it (before Fn::Sub)."""
    return _load()["Resources"]["AppHost"]["Properties"]["UserData"]["Fn::Base64"]["Fn::Sub"]


def _traced(script: str) -> list[str]:
    """The command lines bash runs with xtrace on, each of which it writes to the trace
    with its expanded arguments and the values it assigns.

    xtrace turns on at ``set -...x`` / ``set -o xtrace`` and off at ``set +x`` /
    ``set +o xtrace``. A here-document's body is not traced (bash prints ``+ cat``), nor
    is a comment."""
    traced: list[str] = []
    xtrace, terminator = False, None
    for line in script.split("\n"):
        stripped = line.strip()
        if terminator is not None:  # a here-document's body, up to its terminator line
            if stripped == terminator:
                terminator = None
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if _XTRACE_ON.search(stripped):
            xtrace = True
        elif _XTRACE_OFF.search(stripped):
            xtrace = False
        elif xtrace:
            traced.append(stripped)
        heredoc = _HEREDOC.search(stripped)
        if heredoc:
            terminator = heredoc.group(1)
    return traced


def test_no_secret_reaches_the_user_data_trace() -> None:
    # The script runs under set -x with its output sent to /var/log/dsql-migrator-userdata.log,
    # so the trace of STORAGE_SECRET=$(aws secretsmanager get-secret-value ...) wrote the
    # cookie-signing secret there, as "+ STORAGE_SECRET=<value>". A secret is read with
    # xtrace off, and no traced command expands it.
    script = _user_data()
    sources = "|".join(re.escape(source) for source in _SECRET_SOURCES)
    secrets = {
        match.group(1)
        for match in re.finditer(
            rf"(?m)^\s*(?:export\s+)?(\w+)=[\"']?\$\(.*(?:{sources})", script
        )
    }
    assert secrets == {"STORAGE_SECRET"}, secrets  # the scan found the secret it reads
    traced = _traced(script)
    leaks = [
        line
        for line in traced
        if any(source in line for source in _SECRET_SOURCES)
        or any(re.search(rf"\$\{{!?{name}\}}|\${name}(?!\w)", line) for name in secrets)
    ]
    assert not leaks, leaks
    # The env file still gets it (a here-document's body is not traced) ...
    env_file = re.search(
        r"cat > /etc/dsql-migrator\.env <<EOF\n(.*?)\nEOF\n", script, re.S
    ).group(1)
    assert "DSQL_MIGRATOR_STORAGE_SECRET=${!STORAGE_SECRET}" in env_file.split("\n")
    # ... and the trace comes back on after the read.
    assert "systemctl enable --now dsql-migrator.service" in traced


def test_the_user_data_log_and_env_file_are_created_owner_only() -> None:
    # Created by a redirection under the default umask (022), each would be world-readable:
    # the log holds the trace of every command, the env file the cookie-signing secret (it
    # was made 0600 only after it was written). Each is created 0600 first, right before
    # the redirection that writes it, which truncates the file and keeps its mode.
    commands = [
        line.strip()
        for line in _user_data().split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]

    def at(command: str) -> int:
        assert commands.count(command) == 1, command
        return commands.index(command)

    for path, writer in (
        ("/var/log/dsql-migrator-userdata.log", "exec > {path} 2>&1"),
        ("/etc/dsql-migrator.env", "cat > {path} <<EOF"),
    ):
        created = at(f"install -m 600 /dev/null {path}")
        assert at(writer.format(path=path)) == created + 1, path
        assert not any(path in command for command in commands[:created]), path


# -- the state volume's mount ---------------------------------------------------


def test_the_state_volume_is_mounted_by_uuid_and_the_service_requires_it() -> None:
    # /etc/fstab named the state volume by its NVMe device name (readlink -f /dev/sdf), which
    # follows the order the devices answer at boot and can change from one boot to the next:
    # the volume was then not mounted (nofail lets the host boot), and the app, which did not
    # wait for it, restarted every 5 s unable to open its state. The line names the file
    # system's UUID, read after mkfs.xfs (before it, blkid exits 2 on a new volume and set -e
    # stops every new host's script), and the unit requires the mount that holds the state.
    script = _user_data()
    commands = [
        line.strip()
        for line in script.split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]
    fstab = re.compile(r'echo "(\S+) (\S+) (\S+) (\S+) (\d) (\d)" >> /etc/fstab')
    (echoed,) = [i for i, command in enumerate(commands) if fstab.fullmatch(command)]
    source, mount_point, _type, options, _dump, _pass = fstab.fullmatch(commands[echoed]).groups()
    assert source == "UUID=${!STATE_UUID}", source
    assert "nofail" in options.split(","), options  # a boot without the volume still completes

    def at(command: str) -> int:
        assert commands.count(command) == 1, command
        return commands.index(command)

    # Read once the volume has a file system: after the block that formats a new volume, so
    # also for a re-attached one, which that block leaves as it is.
    formatted = at('mkfs.xfs "${!DEV}"')
    assert commands[formatted + 1] == "fi", commands[formatted + 1]
    read = at('STATE_UUID=$(blkid -s UUID -o value "${!DEV}")')
    assert formatted + 1 < read < echoed, (formatted, read, echoed)
    unit = re.search(
        r"cat > /etc/systemd/system/dsql-migrator\.service <<'EOF'\n(.*?)\nEOF\n", script, re.S
    ).group(1)
    unit_section = unit.split("\n[Service]\n")[0]
    assert f"RequiresMountsFor={mount_point}" in unit_section.split("\n"), unit_section
    # The mount the unit requires holds every state file the env file points the app at.
    env_file = re.search(
        r"cat > /etc/dsql-migrator\.env <<EOF\n(.*?)\nEOF\n", script, re.S
    ).group(1)
    paths = dict(re.findall(r"(?m)^(DSQL_MIGRATOR_\w+_PATH)=(\S+)$", env_file))
    assert {"DSQL_MIGRATOR_JOB_STATE_PATH", "DSQL_MIGRATOR_SESSION_STATE_PATH"} <= set(paths)
    for name, path in paths.items():
        assert path.startswith(f"{mount_point}/"), (name, path)
