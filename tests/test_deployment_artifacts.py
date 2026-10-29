# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the deployment artifacts.

These validate the container image definition and the CloudFormation app-stack
without requiring AWS access:

- The Dockerfile installs version-pinned dependencies (``uv sync --frozen``),
  runs the NiceGUI app as a non-root user, and binds to all interfaces.
- The CloudFormation template parses as YAML and provisions the Fargate
  app-stack (ECS cluster/service/task, ALB + HTTPS listener, optional Cognito)
  with least-privilege IAM (task role scoped to dsql:DbConnect + the source
  secret; execution role for ECR pull + logs), restricted security groups, and
  opt-in scoped bedrock:InvokeModel (Property 10).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

DEPLOY_DIR = Path(__file__).resolve().parent.parent / "deploy"
DOCKERFILE = DEPLOY_DIR / "Dockerfile"
CFN_TEMPLATE = DEPLOY_DIR / "cloudformation.yaml"
CODEBUILD_TEMPLATE = DEPLOY_DIR / "codebuild.yaml"
BUILDSPEC = DEPLOY_DIR / "buildspec.yml"


# --- Container image (Dockerfile) --------------------------------------------


def _dockerfile_text() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def test_dockerfile_exists() -> None:
    assert DOCKERFILE.is_file()


def test_dockerfile_installs_version_pinned_dependencies() -> None:
    text = _dockerfile_text()
    # --frozen installs exactly the pinned versions from the committed lockfile.
    assert "uv sync --frozen" in text


def test_dockerfile_bakes_cdc_external_extra() -> None:
    # The image must include the [cdc-external] extra (kafka-python + MSK IAM SASL
    # signer) so the Lambda-free "EC2 + MSK only" host can run SeedMode=External.
    # The wheels are pure-Python and imported lazily only under External, so they
    # are inert for the default Fargate / Lambda-mode deployment.
    text = _dockerfile_text()
    assert "--extra cdc-external" in text


def test_dockerfile_runs_nicegui_ui_entrypoint() -> None:
    text = _dockerfile_text()
    assert 'ENTRYPOINT ["mysql-dsql-migrator"]' in text
    assert 'CMD ["ui"]' in text


def test_dockerfile_runs_as_non_root() -> None:
    text = _dockerfile_text()
    assert "useradd" in text
    assert "USER appuser" in text


def test_dockerfile_binds_all_interfaces_for_alb() -> None:
    # Behind the ALB the container must listen on 0.0.0.0, not localhost.
    text = _dockerfile_text()
    assert "DSQL_MIGRATOR_APP_HOST=0.0.0.0" in text
    assert "EXPOSE 8080" in text


def test_dockerfile_has_no_hardcoded_credentials() -> None:
    text = _dockerfile_text()
    assert "AKIA" not in text
    assert "aws_secret_access_key" not in text.lower()
    assert "password=" not in text.lower()


def test_dockerfile_bundles_runtime_cdc_artifacts() -> None:
    # The "Deploy CDC infrastructure" path reads these files at runtime, relative to
    # the image's /app root. If the cdc-stack template is not bundled, a clean task
    # fails with "Could not read the cdc-stack template"; if a plugin zip is missing
    # the plugin-upload stage fails. Guard the full runtime-required set.
    text = _dockerfile_text()
    assert "deploy/cdc-stack/cdc-stack.yaml" in text, (
        "Dockerfile must COPY deploy/cdc-stack/cdc-stack.yaml; the runtime reads it "
        "to deploy CDC infrastructure (_read_cdc_template_body)."
    )
    for plugin in (
        "connectors/plugins/debezium-mysql-plugin.zip",
        # Now that the upload stage is engine-aware, a missing PostgreSQL plugin no
        # longer breaks EVERY deploy (it used to, because the upload stat'd all four
        # artifacts and raised on any absent one) -- it would break only PostgreSQL
        # deploys. That accidental coverage disappeared exactly when it started to
        # matter, so guard this one explicitly.
        "connectors/plugins/debezium-postgres-plugin.zip",
        "connectors/plugins/dsql-sink-plugin.zip",
        "connectors/plugins/offset-seeder-lambda.zip",
    ):
        assert plugin in text, f"Dockerfile must bundle {plugin}"

    # A COPY only works if the file is in the build context: when .dockerignore
    # excludes deploy/, it MUST re-include the template, or the COPY fails with
    # "not found" at build time.
    dockerignore = (DEPLOY_DIR.parent / ".dockerignore")
    if dockerignore.is_file():
        ignore = dockerignore.read_text(encoding="utf-8")
        if "deploy/" in ignore:
            assert "!deploy/cdc-stack/cdc-stack.yaml" in ignore, (
                ".dockerignore excludes deploy/ but does not re-include "
                "deploy/cdc-stack/cdc-stack.yaml; the Dockerfile COPY will fail."
            )


# --- CloudFormation template --------------------------------------------------


@pytest.fixture(scope="module")
def template() -> dict:
    return yaml.safe_load(CFN_TEMPLATE.read_text(encoding="utf-8"))


def test_cdc_deploy_role_can_manage_connector_enis(template: dict) -> None:
    # When MSK Connect creates the connector it places the connector's ENIs in the
    # connector subnets using the CALLER'S credentials -- CloudTrail shows
    # CreateNetworkInterface invoked by kafkaconnect.amazonaws.com but authorized
    # against the deploy role (<stack>-cdc-deploy), NOT the connector's
    # ServiceExecutionRole and NOT the MSK Connect service-linked role. Without
    # ec2:CreateNetworkInterface (+ Describe/Delete) on the CdcDeployRole, the
    # connector CREATE fails with "not authorized to perform
    # ec2:CreateNetworkInterface" (AWS::KafkaConnect::Connector InvalidRequest).
    actions = _cdc_deploy_role_actions(template)
    assert {
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
    } <= actions


def test_task_role_can_read_connector_state(template: dict) -> None:
    # The TASK role itself (not the assumed deploy role) polls connector state to
    # drive the CDC UI: it lists connectors and waits for RUNNING before starting
    # the sink pass. Without kafkaconnect:ListConnectors the read is AccessDenied
    # and silently swallowed (returns None), so a connector that is actually
    # RUNNING shows "creating…" forever and the sink pass never starts.
    actions = _all_task_role_actions(template)
    assert "kafkaconnect:ListConnectors" in actions
    assert "kafkaconnect:DescribeConnector" in actions


def test_cdc_deploy_role_can_poll_connector_operations(template: dict) -> None:
    # UpdateConnector is asynchronous: it returns a connector-operation id that the
    # CFN handler polls with DescribeConnectorOperation. Without it, updating a
    # RUNNING connector (reapplying the table set on a CDC retry) fails with "not
    # authorized to perform kafkaconnect:DescribeConnectorOperation" and the stack
    # rolls back. Required alongside kafkaconnect:UpdateConnector.
    actions = _cdc_deploy_role_actions(template)
    assert "kafkaconnect:UpdateConnector" in actions
    assert "kafkaconnect:DescribeConnectorOperation" in actions


def test_cdc_deploy_role_can_configure_connector_log_delivery(template: dict) -> None:
    # The connector enables CloudWatch worker-log delivery, which MSK Connect sets
    # up via the CloudWatch Logs vended-logs *delivery* API using the deploy role's
    # credentials. Without logs:ListLogDeliveries / CreateLogDelivery the connector
    # goes to FAILED with InvalidInput.WorkerLogsError ("not authorized to perform
    # logs:ListLogDeliveries") and no worker logs are ever written.
    actions = _cdc_deploy_role_actions(template)
    assert {
        "logs:CreateLogDelivery",
        "logs:ListLogDeliveries",
    } <= actions


def test_cdc_deploy_role_can_manage_connector_alarms(template: dict) -> None:
    # The cdc-stack creates a CloudWatch alarm per connector on ErroredTaskCount, so
    # the deploy role must be able to create/read/delete those alarms -- else the
    # cdc-stack deploy fails creating the alarm with AccessDenied
    # (cloudwatch:PutMetricAlarm) and its rollback fails on cloudwatch:DeleteAlarms.
    actions = _cdc_deploy_role_actions(template)
    assert {
        "cloudwatch:PutMetricAlarm",
        "cloudwatch:DeleteAlarms",
        "cloudwatch:DescribeAlarms",
    } <= actions


def test_cloudformation_parses_as_yaml(template: dict) -> None:
    assert template["AWSTemplateFormatVersion"] == "2010-09-09"
    assert "Resources" in template


def test_alb_is_named_so_its_dns_name_is_lower_case(template: dict) -> None:
    # An unnamed ALB gets a CloudFormation-generated MIXED-CASE name
    # ("mysql--LoadB-u9DQdeKlckt9") and its DNSName inherits that casing. The ALB
    # sends the Cognito OAuth redirect_uri with the host LOWER-CASED, while the app
    # client's CallbackURLs come from GetAtt DNSName -- so the strings differ and
    # sign-in fails with the misleading "Client is not enabled for OAuth2.0 flows."
    # Naming the ALB from the (lower-case) stack name keeps DNSName lower case.
    props = template["Resources"]["LoadBalancer"]["Properties"]
    assert "Name" in props, (
        "LoadBalancer must set Name; without it CloudFormation generates a mixed-case "
        "name, DNSName inherits the casing, and Cognito rejects the login because the "
        "ALB lower-cases the redirect_uri host it sends."
    )
    assert props["Name"] == {"Fn::Sub": "${AWS::StackName}-alb"}, (
        f"unexpected ALB Name {props['Name']!r}; it must derive from the stack name so "
        "the operator controls the casing (documented as lower-case, <=28 chars)."
    )


def test_target_group_is_named_so_an_alb_replacement_can_be_upgraded(
    template: dict,
) -> None:
    """Naming the ALB replaces it, and an UNNAMED target group makes that unupgradeable.

    CloudFormation replacement is create-new -> repoint -> delete-old. An unnamed target
    group has no changed property, so CFN reuses the same physical group and attaches it
    to the NEW listener while the OLD ALB still holds it -- and ELBv2 allows a target
    group on only one load balancer:

        The following target groups cannot be associated with more than one load balancer
        (HandlerErrorCode: ServiceLimitExceeded)

    That rolled back the first 0.1.229 upgrade of a live stack. TargetGroup ``Name`` is
    create-only (confirmed against the CloudFormation registry), so naming it replaces
    the group in the same update and the new listener gets an unattached one.
    """
    tg = template["Resources"]["TargetGroup"]["Properties"]
    assert "Name" in tg, (
        "TargetGroup must set Name. Without it the group is REUSED across an ALB "
        "replacement and the new listener cannot attach it while the old ALB holds it, "
        "so upgrading an existing stack fails with ServiceLimitExceeded."
    )
    assert tg["Name"] == {"Fn::Sub": "${AWS::StackName}-tg"}, (
        f"unexpected TargetGroup Name {tg['Name']!r}; derive it from the stack name so "
        "it inherits the documented <=28-char, lower-case budget"
    )


def test_alb_and_target_group_are_named_together(template: dict) -> None:
    # The coupling IS the fix: a named ALB with an unnamed target group is the broken
    # upgrade path, and an unnamed ALB re-breaks Cognito login. Pin them as a pair so
    # removing either one fails here rather than during someone's production upgrade.
    resources = template["Resources"]
    lb_named = "Name" in resources["LoadBalancer"]["Properties"]
    tg_named = "Name" in resources["TargetGroup"]["Properties"]
    assert lb_named == tg_named, (
        f"LoadBalancer Name={lb_named} but TargetGroup Name={tg_named} -- these must be "
        "named together: naming only the ALB breaks upgrades of existing stacks "
        "(target group reused across the replacement), and naming neither breaks Cognito "
        "login (mixed-case DNS name vs the lower-cased redirect_uri the ALB sends)."
    )


def test_listener_forwards_to_the_target_group_by_ref(template: dict) -> None:
    # Why the fix works at all: the listener references the group with Ref, making the
    # NEW group a dependency of the NEW listener, so CFN must create it first. If this
    # ever became a hard-coded ARN or an import, the ordering guarantee would be gone
    # and naming the group would no longer save the upgrade.
    def _refs(node) -> int:
        """Count {"Ref": "TargetGroup"} occurrences anywhere under ``node``."""
        if isinstance(node, dict):
            if node.get("Ref") == "TargetGroup":
                return 1
            return sum(_refs(v) for v in node.values())
        if isinstance(node, list):
            return sum(_refs(v) for v in node)
        return 0

    # DefaultActions is an Fn::If (Cognito on/off), each branch ending in a forward, so
    # BOTH branches must use the Ref: a hard-coded ARN in one branch would break that
    # branch's ordering guarantee while the other kept a laxer test green.
    actions = template["Resources"]["HttpsListener"]["Properties"]["DefaultActions"]
    branches = _refs(actions)
    assert branches >= 2, (
        f"expected the Ref in BOTH Fn::If branches (Cognito on and off), found "
        f"{branches}. A hard-coded ARN or an import in either branch removes the "
        "dependency that forces CloudFormation to create the replacement target group "
        "before the replacement listener -- which is what avoids the "
        "one-ALB-per-target-group conflict."
    )
    # The ECS service must point at the same logical group, or it would keep registering
    # into the old one after a replacement.
    service = template["Resources"]["Service"]["Properties"]
    assert _refs(service.get("LoadBalancers", [])) >= 1, (
        "Service.LoadBalancers must reference TargetGroup by Ref so it re-points at the "
        "replacement group in the same update"
    )


def test_alb_drops_invalid_headers_and_pins_a_tls13_policy(template: dict) -> None:
    """Both edge-hardening properties are OFF by default, so pin them here.

    A content security review flagged them (CKV_AWS_103 and
    ELBV2_LISTENER_SSL_POLICY_RULE): an ALB forwards malformed HTTP headers to the
    target unless told not to (the HTTP desync / request-smuggling vector), and an
    HTTPS listener with no ``SslPolicy`` inherits ``ELBSecurityPolicy-2016-08``,
    which still negotiates TLS 1.0/1.1. Neither shows up in normal use, so nothing
    but this test would notice them being removed.
    """
    attrs = {
        a["Key"]: a["Value"]
        for a in template["Resources"]["LoadBalancer"]["Properties"][
            "LoadBalancerAttributes"
        ]
    }
    assert attrs.get("routing.http.drop_invalid_header_fields.enabled") == "true", (
        "LoadBalancerAttributes must keep routing.http.drop_invalid_header_fields."
        "enabled=true; without it the ALB passes malformed headers straight to the "
        f"container (found {attrs!r})"
    )
    listener = template["Resources"]["HttpsListener"]["Properties"]
    policy = listener.get("SslPolicy", "")
    assert policy.startswith("ELBSecurityPolicy-TLS13-"), (
        f"HttpsListener SslPolicy is {policy!r}; pin an ELBSecurityPolicy-TLS13-* "
        "policy, since omitting SslPolicy falls back to the 2016-08 default that "
        "still allows TLS 1.0/1.1"
    )


def test_cognito_callback_url_is_built_from_the_alb_dns_name(template: dict) -> None:
    # The pairing that makes the test above matter: the callback is GetAtt DNSName, so
    # DNSName's casing IS the callback's casing. If this ever switches to a hand-built
    # string, the ALB Name guard above no longer protects the login flow.
    client = template["Resources"]["UserPoolClient"]["Properties"]
    rendered = json.dumps(client["CallbackURLs"])
    assert "LoadBalancer" in rendered and "DNSName" in rendered, (
        "UserPoolClient.CallbackURLs no longer derives from the ALB DNSName; re-check "
        "that the callback host still matches what the ALB sends as redirect_uri."
    )


def test_deployment_docs_state_the_stack_name_constraint(template: dict) -> None:
    # The 32-char ALB name cap is only discoverable via a ~2 minute rollback, and the
    # lower-case requirement is invisible until Cognito login fails. Both must be
    # documented up-front, in every language, next to the deploy command.
    # Assert on the ALB error text itself, not a bare "32" -- these docs already say
    # "32" for /32 CIDRs and token_urlsafe(32), so a loose substring check passes even
    # after the constraint paragraph is deleted (confirmed by mutation).
    for name in ("DEPLOYMENT.md", "DEPLOYMENT.ko.md", "DEPLOYMENT.ja.md"):
        text = (DEPLOY_DIR / name).read_text(encoding="utf-8")
        assert "-alb" in text, f"{name} must say the ALB is named <stack-name>-alb"
        assert "cannot be longer than '32' characters" in text, (
            f"{name} must quote the actual ALB failure text so an operator can match "
            "it: The load balancer name '<stack>-alb' cannot be longer than '32' "
            "characters. Without it, the 32-char cap is only discoverable by a "
            "~2 minute rollback."
        )
        assert "28" in text, (
            f"{name} must state the resulting stack-name budget (28 characters)."
        )
        assert "redirect_uri" in text, (
            f"{name} must explain WHY the stack name has to be lower case -- the ALB "
            "lower-cases the redirect_uri host it sends to Cognito, so a mixed-case "
            "ALB DNS name breaks login."
        )


def test_cloudformation_provisions_fargate_app_stack(template: dict) -> None:
    types = {res["Type"] for res in template["Resources"].values()}
    assert "AWS::ECS::Cluster" in types
    assert "AWS::ECS::TaskDefinition" in types
    assert "AWS::ECS::Service" in types
    assert "AWS::ElasticLoadBalancingV2::LoadBalancer" in types
    assert "AWS::ElasticLoadBalancingV2::TargetGroup" in types
    assert "AWS::ElasticLoadBalancingV2::Listener" in types
    assert "AWS::IAM::Role" in types
    assert "AWS::Logs::LogGroup" in types


def test_session_resume_wiring_storage_secret_and_state_path(template: dict) -> None:
    """The task injects a stable cookie-signing secret and a session-state path
    so a reconnecting browser resumes its workbench state across restarts."""
    container = template["Resources"]["TaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]
    # The storage secret is injected from Secrets Manager (never plaintext env).
    secrets = {s["Name"]: s["ValueFrom"] for s in container.get("Secrets", [])}
    assert secrets.get("DSQL_MIGRATOR_STORAGE_SECRET") == {"Ref": "StorageSecret"}
    # It is an auto-generated Secrets Manager secret (no operator input needed).
    assert (
        template["Resources"]["StorageSecret"]["Type"]
        == "AWS::SecretsManager::Secret"
    )
    # Per-session and job state paths are set (on /tmp ephemeral storage).
    env = {e["Name"]: e["Value"] for e in container["Environment"]}
    assert env["DSQL_MIGRATOR_SESSION_STATE_PATH"] == "/tmp/session_state.sqlite"
    assert env["DSQL_MIGRATOR_JOB_STATE_PATH"] == "/tmp/job_state.sqlite"
    # The activity log shares /tmp; it is size-capped + rotated in code so it
    # cannot grow without bound on the task's ephemeral storage.
    assert env["DSQL_MIGRATOR_ACTIVITY_LOG_PATH"] == "/tmp/migration_activity.log"
    # Log level is NOT a deploy-time parameter (adjusted at runtime on the app's
    # Settings -> Activity log tab); the template ships a safe default.
    assert env["DSQL_MIGRATOR_LOG_LEVEL"] == "INFO"
    assert "LogLevel" not in template["Parameters"]
    # CloudWatch mirroring of the activity log IS a deploy-time parameter so the
    # durable audit trail can be enabled from the start (the /tmp file is lost on
    # task replacement); it defaults off and the env is driven by the parameter.
    assert template["Parameters"]["EnableActivityLogCloudWatch"]["Default"] == "false"
    assert env["DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT"] == {
        "Ref": "EnableActivityLogCloudWatch"
    }


def test_service_is_fargate_single_task(template: dict) -> None:
    svc = template["Resources"]["Service"]["Properties"]
    assert svc["LaunchType"] == "FARGATE"
    # Single-tenant control plane: exactly one task (no horizontal scaling).
    assert svc["DesiredCount"] == 1
    # Public IP assignment is parameterized; defaults to DISABLED (private).
    assert svc["NetworkConfiguration"]["AwsvpcConfiguration"]["AssignPublicIp"] == {
        "Ref": "AssignPublicIp"
    }
    assert template["Parameters"]["AssignPublicIp"]["Default"] == "DISABLED"


def _task_role_statements(template: dict) -> dict:
    """Flatten the TaskRole's policy statements by Sid.

    Some policy entries are conditional (wrapped in ``Fn::If`` -- e.g. the
    read-source-secret policy, present only when SourceSecretArn is set). Unwrap
    an ``Fn::If`` to its "true" branch so the conditional statements are included.
    """
    role = template["Resources"]["TaskRole"]
    statements = {}
    for policy in role["Properties"]["Policies"]:
        if isinstance(policy, dict) and "Fn::If" in policy:
            policy = policy["Fn::If"][1]  # the present-branch policy
        for stmt in policy["PolicyDocument"]["Statement"]:
            statements[stmt["Sid"]] = stmt
    return statements


def test_task_role_grants_least_privilege_dsql_and_secrets(template: dict) -> None:
    statements = _task_role_statements(template)

    dsql = statements["DsqlConnect"]
    # The app connects as the DSQL admin role by default, so both connect
    # actions are granted, scoped to the cluster only.
    assert dsql["Action"] == ["dsql:DbConnect", "dsql:DbConnectAdmin"]
    assert dsql["Resource"] == {"Ref": "DsqlClusterArn"}

    # read-source-secret is conditional (reuse path); when present it's scoped.
    secret = statements["GetSourceSecret"]
    assert secret["Action"] == "secretsmanager:GetSecretValue"
    assert secret["Resource"] == {"Ref": "SourceSecretArn"}

    # The discovery/status actions that AWS cannot resource-scope (EC2 Describe*,
    # sts:GetCallerIdentity, kafkaconnect:ListConnectors, cloudwatch:GetMetricData)
    # are the ONLY statements allowed to use "*", and they must be strictly
    # read-only. Everything else stays scoped.
    _UNSCOPABLE_READONLY_SIDS = {
        "DiscoverConnectorSubnets",
        "CallerIdentity",
        "DiscoverCdcConnectors",
        "ReadConnectorMetrics",
        # The CDC VpcId prefill describes the SOURCE database, which is an arbitrary
        # endpoint the operator types -- not a resource this stack owns, so there is
        # nothing to scope the ARN to. Strictly read-only (Describe* only), enforced by
        # the verb check below.
        "DiscoverSourceNetwork",
    }
    for sid, stmt in statements.items():
        if sid in _UNSCOPABLE_READONLY_SIDS:
            assert stmt["Resource"] == "*"
            actions = stmt["Action"]
            actions = actions if isinstance(actions, list) else [actions]
            # Read-only verbs only (Describe* / List* / GetCallerIdentity /
            # GetMetricData — CloudWatch's metric read has no resource-level scoping).
            assert all(
                a.split(":", 1)[1].startswith(
                    ("Describe", "List", "GetCallerIdentity", "GetMetricData")
                )
                for a in actions
            )
        else:
            assert stmt["Resource"] != "*"


def test_source_secret_is_optional_and_conditionally_scoped(template: dict) -> None:
    """SourceSecretArn is optional (username/password auth is the common case):
    it defaults to empty, and the GetSecretValue policy that reads it is wrapped
    in the HasSourceSecret condition so it's omitted when no secret is provided."""
    assert template["Parameters"]["SourceSecretArn"]["Default"] == ""
    cond = template["Conditions"]["HasSourceSecret"]
    assert cond == {"Fn::Not": [{"Fn::Equals": [{"Ref": "SourceSecretArn"}, ""]}]}
    # The read-source-secret policy entry is gated by that condition.
    policies = template["Resources"]["TaskRole"]["Properties"]["Policies"]
    conditional = [p for p in policies if isinstance(p, dict) and "Fn::If" in p]
    assert any(p["Fn::If"][0] == "HasSourceSecret" for p in conditional)


def test_task_role_cdc_deploy_discovery_scoped_to_plugin_bucket(template: dict) -> None:
    statements = _task_role_statements(template)
    # S3 plugin bucket management is scoped to the single managed bucket (its name
    # embeds the account + region), not "*".
    bucket = statements["ManagePluginBucket"]["Resource"]
    upload = statements["UploadPlugins"]["Resource"]
    assert "mysql-dsql-migrator-plugins-" in str(bucket)
    assert str(upload).endswith("/*}") or "mysql-dsql-migrator-plugins-" in str(upload)


def test_task_role_provisions_cdc_source_secret_scoped_to_prefix(template: dict) -> None:
    statements = _task_role_statements(template)
    # When the source was connected with username/password, the app creates/upserts
    # a tool-managed source-credentials secret for the CDC connector. Write access is
    # scoped to the deterministic mysql-dsql-migrator/cdc/* name prefix -- never "*" and
    # never the customer's other secrets.
    stmt = statements["ProvisionCdcSourceSecret"]
    actions = stmt["Action"]
    assert set(actions) == {
        "secretsmanager:CreateSecret",
        "secretsmanager:PutSecretValue",
        "secretsmanager:DescribeSecret",
        # RestoreSecret so the upsert can recover a same-named secret that a prior
        # teardown scheduled for deletion (recovery window) before writing anew.
        "secretsmanager:RestoreSecret",
        # DeleteSecret so a full teardown removes the credentials the tool stored.
        "secretsmanager:DeleteSecret",
        # GetSecretValue for ONE job: a PostgreSQL teardown drops the logical replication
        # slot on the source and needs the credentials it stored here to connect. Without it
        # the role could Create, Put, Describe, Restore and DELETE this secret but not READ
        # it -- and a real teardown failed with "Access denied reading the secret", left the
        # slot pinning the source WAL, then deleted that same secret one second later.
        "secretsmanager:GetSecretValue",
    }
    resource = str(stmt["Resource"])
    assert "secret:mysql-dsql-migrator/cdc/*" in resource
    # Still the prefix, never "*" -- read access must not widen the blast radius.
    assert resource.count("*") >= 1 and ":secret:*" not in resource
    assert resource != "*"


def test_task_role_can_filter_connector_logs_for_dlq(template: dict) -> None:
    # The data-migration status page surfaces DLQ depth / "Quarantined records" by
    # running MskConnectController.dlq_errors, which calls logs:FilterLogEvents against
    # the /msk-connect/<cdc-stack>-cdc log group. The reader fails closed (swallows
    # AccessDenied and returns []), so a missing grant is INVISIBLE at runtime -- the UI
    # just shows an empty DLQ regardless of what CloudWatch holds. Pin the action here so
    # it can't be dropped again (it was absent through v0.1.301; added in v0.1.302).
    stmt = _task_role_statements(template)["ReadConnectorWorkerLogs"]
    actions = stmt["Action"]
    actions = actions if isinstance(actions, list) else [actions]
    assert "logs:FilterLogEvents" in actions
    # The worker-log-tail read (connector FAILED diagnosis) still needs these two.
    assert "logs:DescribeLogStreams" in actions
    assert "logs:GetLogEvents" in actions
    # Scoped to the CDC log-group family, never "*".
    resource = str(stmt["Resource"])
    assert "log-group:/msk-connect/mysql-dsql-cdc-*" in resource
    assert stmt["Resource"] != "*"


# --- Dedicated CDC deploy role (privilege separation) ------------------------


def _all_task_role_actions(template: dict) -> set[str]:
    actions: set[str] = set()
    for stmt in _task_role_statements(template).values():
        a = stmt["Action"]
        actions.update(a if isinstance(a, list) else [a])
    return actions


def _cdc_deploy_role_actions(template: dict) -> set[str]:
    actions: set[str] = set()
    role = template["Resources"]["CdcDeployRole"]
    for policy in role["Properties"]["Policies"]:
        for stmt in policy["PolicyDocument"]["Statement"]:
            a = stmt["Action"]
            actions.update(a if isinstance(a, list) else [a])
    return actions


_PRIVILEGED_CDC_ACTIONS = {
    "cloudformation:CreateStack",
    "cloudformation:DeleteStack",
    "kafka:CreateClusterV2",
    "kafka:DeleteCluster",
    "kafkaconnect:CreateConnector",
    "iam:CreateRole",
    "iam:PassRole",
}


def test_cdc_deploy_role_exists_and_is_iam_role(template: dict) -> None:
    role = template["Resources"]["CdcDeployRole"]
    assert role["Type"] == "AWS::IAM::Role"
    # Explicit RoleName (breaks the trust-policy dependency cycle with TaskRole).
    assert role["Properties"]["RoleName"] == {
        "Fn::Sub": "${AWS::StackName}-cdc-deploy"
    }


def test_cdc_deploy_role_can_stage_oversize_template_in_plugin_bucket(
    template: dict,
) -> None:
    # The cdc-stack template exceeds CloudFormation's 51,200-byte inline limit, so
    # the deployer stages it in the managed plugin bucket and passes TemplateURL.
    # The assumed CdcDeployRole does the PutObject AND CloudFormation reads it back
    # (GetObject) under that role, so it must allow both -- scoped to the plugin
    # bucket's cdc-plugins/ prefix. (Without this, deploy fails with AccessDenied on
    # s3:PutObject at the create-stack stage and no stack is created.)
    role = template["Resources"]["CdcDeployRole"]
    stmts = [
        s
        for p in role["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    staging = [s for s in stmts if s.get("Sid") == "StageCdcStackTemplate"]
    assert staging, "CdcDeployRole must allow staging the cdc-stack template in S3"
    acts = staging[0]["Action"]
    acts = acts if isinstance(acts, list) else [acts]
    assert "s3:PutObject" in acts and "s3:GetObject" in acts
    resource = str(staging[0]["Resource"])
    assert "mysql-dsql-migrator-plugins-" in resource and "cdc-plugins/" in resource


def test_cdc_deploy_role_trusts_the_task_role(template: dict) -> None:
    trust = template["Resources"]["CdcDeployRole"]["Properties"][
        "AssumeRolePolicyDocument"
    ]["Statement"]
    assert len(trust) == 1
    stmt = trust[0]
    assert stmt["Effect"] == "Allow"
    assert stmt["Action"] == "sts:AssumeRole"
    # The TaskRole (by GetAtt Arn) is the only principal allowed to assume it.
    assert stmt["Principal"] == {"AWS": {"Fn::GetAtt": ["TaskRole", "Arn"]}}


def test_cdc_deploy_role_has_the_privileged_actions(template: dict) -> None:
    actions = _cdc_deploy_role_actions(template)
    assert _PRIVILEGED_CDC_ACTIONS <= actions


def test_kafkaconnect_create_connector_is_unscoped(template: dict) -> None:
    # kafkaconnect:CreateConnector has NO resource-level support: at create time the
    # connector ARN does not exist, so a scoped connector ARN makes CreateConnector
    # fail with "Access denied for operation 'AWS::KafkaConnect::Connector'" (the
    # DebeziumSourceConnector CREATE_FAILED symptom). It must therefore be granted on
    # Resource "*", like CreateCustomPlugin / CreateWorkerConfiguration. The scoped
    # connector-ARN statement must NOT carry CreateConnector.
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]

    def _actions(stmt) -> list:
        a = stmt["Action"]
        return a if isinstance(a, list) else [a]

    create_stmts = [
        s for s in stmts if "kafkaconnect:CreateConnector" in _actions(s)
    ]
    assert create_stmts, "CreateConnector must be granted somewhere"
    for s in create_stmts:
        assert s["Resource"] == "*", (
            "kafkaconnect:CreateConnector must be on Resource '*' (no resource-level "
            f"support at create time); got {s['Resource']!r}"
        )

    # The scoped connector-ARN statement (Delete/Describe/Update/...) must not
    # smuggle CreateConnector back in under a narrow ARN.
    scoped = [
        s
        for s in stmts
        if isinstance(s.get("Resource"), dict)
        and "connector/mysql-dsql-cdc-" in str(s["Resource"])
    ]
    for s in scoped:
        assert "kafkaconnect:CreateConnector" not in _actions(s), (
            "CreateConnector must not be under a scoped connector ARN"
        )


def test_cdc_deploy_role_can_deploy_offset_seeder(template: dict) -> None:
    # The gapless Full Load -> CDC handoff (SeedOffset) makes the cdc-stack create
    # an in-VPC offset-seeder Lambda + its own IAM role, invoked by a custom
    # resource. The assumed CdcDeployRole must therefore (1) manage that Lambda,
    # (2) manage the seeder's auto-named role -- not only the connector role -- and
    # (3) PassRole it to lambda.amazonaws.com. Without these the SeedOffset deploy
    # fails with AccessDenied and rolls back.
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    by_sid = {s.get("Sid"): s for s in stmts}

    # (1) Lambda lifecycle, scoped to the cdc-stack's function-name family.
    lam = by_sid.get("LambdaOffsetSeeder")
    assert lam is not None, "CdcDeployRole must allow managing the offset-seeder Lambda"
    lam_acts = lam["Action"] if isinstance(lam["Action"], list) else [lam["Action"]]
    assert {
        "lambda:CreateFunction",
        "lambda:DeleteFunction",
        "lambda:InvokeFunction",
    } <= set(lam_acts)
    assert "function:mysql-dsql-cdc-*" in str(lam["Resource"])

    # (2) Role management covers ANY mysql-dsql-cdc-* role (seeder + connector),
    # not just the connector role.
    roles_stmt = by_sid.get("IamCdcStackRoles")
    assert roles_stmt is not None, "expected the broadened cdc-stack role-management statement"
    res = str(roles_stmt["Resource"])
    assert "role/mysql-dsql-cdc-*" in res and "ConnectorExecutionRole" not in res, (
        "role management must cover all cdc-stack roles (incl. OffsetSeederRole)"
    )

    # (3) PassRole allows lambda (for the seeder) in addition to MSK Connect.
    passrole = by_sid.get("IamPassRoleToCdcServices")
    assert passrole is not None
    passed_to = passrole["Condition"]["StringEquals"]["iam:PassedToService"]
    passed_to = passed_to if isinstance(passed_to, list) else [passed_to]
    assert "lambda.amazonaws.com" in passed_to
    assert "kafkaconnect.amazonaws.com" in passed_to


_EC2_SCOPED_TYPES = (
    "security-group/*",
    "security-group-rule/*",
    "subnet/*",
    "route-table/*",
    "vpc/*",
    "natgateway/*",
    "elastic-ip/*",
    "vpc-endpoint/*",
    "network-interface/*",
)

# Every statement still on Resource "*" and the reason it has to be. Each entry was
# checked against AWS's machine-readable service reference: these actions list NO
# resource types, so a scoped ARN would silently grant nothing.
_EXPECTED_UNSCOPED_SIDS = {
    "CloudFormationAccountReads",   # ValidateTemplate / ListStacks: account-level
    "KafkaConnectCreateConnector",  # create + tag-on-create: ARN does not exist yet
    "KafkaConnectListConnectors",   # List*: authorized against .../v1/connectors
    "KafkaConnectCreateAndTag",     # ditto for CustomPlugin / WorkerConfiguration
    "Ec2NetworkReads",              # ec2:Describe*: no resource-level form
    "CloudWatchLogsLogDelivery",    # logs:*LogDelivery / PutResourcePolicy
}


def test_cdc_deploy_role_ec2_writes_are_scoped_to_the_types_it_creates(
    template: dict,
) -> None:
    """EC2 network writes must be pinned to resource-type ARNs, not "*".

    Every action in this statement DOES support resource-level permissions (AWS
    service reference), and iam:SimulateCustomPolicy confirms all of them resolve to
    "allowed" against an ARN of the type they create while another account, another
    region or another resource type resolves to implicitDeny. A specific VPC/subnet
    ARN is impossible here -- the operator picks the VPC as a cdc-stack parameter long
    after this role exists -- so account+region+type is the narrowest available form,
    and dropping back to "*" would widen a privileged role by ~28 write actions.
    """
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    by_sid = {s.get("Sid"): s for s in stmts}
    writes = by_sid["Ec2NetworkResources"]
    assert writes["Resource"] != "*", "EC2 network writes must not be account-wide"
    rendered = json.dumps(writes["Resource"])
    for suffix in _EC2_SCOPED_TYPES:
        assert suffix in rendered, f"missing {suffix} -- the deploy would AccessDeny"
    # No Describe* in the scoped statement except DescribeVpcAttribute, which is the
    # only one that takes an ARN. Any other Describe here would never match.
    describes = [a for a in writes["Action"] if a.startswith("ec2:Describe")]
    assert describes == ["ec2:DescribeVpcAttribute"], describes
    # ... and the account-wide statement carries ONLY reads.
    reads = by_sid["Ec2NetworkReads"]
    assert reads["Resource"] == "*"
    assert all(a.startswith("ec2:Describe") for a in reads["Action"]), reads["Action"]


def test_cdc_deploy_role_cannot_attach_an_arbitrary_managed_policy(
    template: dict,
) -> None:
    """The escalation path this closes is real, so pin it rather than trust review.

    With `iam:AttachRolePolicy` unrestricted on `role/mysql-dsql-cdc-*`, this role could
    create a role in that family, attach `AdministratorAccess`, pass it to a Lambda it may
    also create (`IamPassRoleToCdcServices` + `LambdaOffsetSeeder`), and invoke it --
    account administrator. The cdc-stack attaches exactly ONE managed policy
    (`AWSLambdaVPCAccessExecutionRole`, the only `ManagedPolicyArns` in cdc-stack.yaml), so
    the grant is conditioned on that policy ARN. No scanner rule flags this now that the
    wildcards are scoped, which is precisely why it needs a test.
    """
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    attach = [
        s
        for s in stmts
        if "iam:AttachRolePolicy"
        in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    ]
    assert len(attach) == 1, "iam:AttachRolePolicy must live in exactly one statement"
    condition = attach[0].get("Condition") or {}
    rendered = json.dumps(condition)
    assert "iam:PolicyARN" in rendered, (
        "iam:AttachRolePolicy must be conditioned on iam:PolicyARN, or this role can "
        f"attach AdministratorAccess to a mysql-dsql-cdc-* role (condition={condition!r})"
    )
    assert "AWSLambdaVPCAccessExecutionRole" in rendered, rendered
    # The unconditioned role-management statement must NOT carry Attach/Detach.
    others = [s for s in stmts if s.get("Sid") == "IamCdcStackRoles"]
    assert others, "IamCdcStackRoles statement missing"
    for s in others:
        actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
        assert "iam:AttachRolePolicy" not in actions
        assert "iam:DetachRolePolicy" not in actions


def test_cdc_deploy_role_grants_the_reads_cloudformation_actually_makes(
    template: dict,
) -> None:
    # CloudTrail on a live cdc-stack create+delete showed CloudFormation calling
    # ec2:DescribeSecurityGroupRules (once per SecurityGroupIngress/Egress resource) and
    # ec2:DescribeNetworkAcls, both denied because neither was granted. The deploy survives
    # on a CFN fallback; granting them removes an AccessDenied from every customer's trail.
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    reads = next(s for s in stmts if s.get("Sid") == "Ec2NetworkReads")
    for action in ("ec2:DescribeSecurityGroupRules", "ec2:DescribeNetworkAcls"):
        assert action in reads["Action"], f"{action} was denied on every real deploy"


def test_cdc_deploy_role_wildcard_statements_are_an_explicit_allowlist(
    template: dict,
) -> None:
    # The blast radius of this privileged role is exactly "what is still on *". Pin
    # that set so adding a wildcard statement is a deliberate, reviewed act rather
    # than something a scanner finds later.
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    unscoped = {s.get("Sid") for s in stmts if s.get("Resource") == "*"}
    assert unscoped == _EXPECTED_UNSCOPED_SIDS, (
        f"unexpected wildcard statements: {unscoped ^ _EXPECTED_UNSCOPED_SIDS}. If an "
        "action genuinely has no resource-level form, add it here with that reason; "
        "otherwise scope it."
    )


def test_both_app_templates_share_one_cdc_deploy_role_policy() -> None:
    # cloudformation.yaml (Fargate) and cloudformation-ec2.yaml (single host) grant
    # the SAME privileged cdc-deploy role. They have drifted apart in review before;
    # comparing the parsed policy keeps a scoping fix from landing in only one.
    ec2_template = yaml.safe_load(
        (DEPLOY_DIR / "cloudformation-ec2.yaml").read_text(encoding="utf-8")
    )
    fargate = yaml.safe_load(CFN_TEMPLATE.read_text(encoding="utf-8"))
    a = fargate["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
    b = ec2_template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True), (
        "CdcDeployRole policies differ between cloudformation.yaml and "
        "cloudformation-ec2.yaml -- apply the change to both"
    )


def test_cdc_deploy_role_logs_describe_unscoped_and_can_delete_msk(template: dict) -> None:
    # Two gaps that only surface on the assumed-role path (admin creds bypass them):
    # (1) logs:DescribeLogGroups has NO resource-level support -- CFN calls it to
    #     resolve a LogGroup Arn for !GetAtt -- so it is its own statement on a broad
    #     log-group resource, not pinned to the connector log group.
    # (2) MSK delete is kafka:DeleteCluster (there is no DeleteClusterV2), needed so
    #     rollback/teardown can remove the Serverless cluster.
    stmts = [
        s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    ]
    by_sid = {s.get("Sid"): s for s in stmts}

    desc = by_sid.get("LogGroupDescribe")
    assert desc is not None, "logs:DescribeLogGroups must be its own statement"
    assert desc["Action"] == "logs:DescribeLogGroups"
    assert ":log-group:*" in str(desc["Resource"]), (
        "DescribeLogGroups cannot be pinned to one log group (no resource-level support)"
    )
    lg = by_sid["ConnectorLogGroup"]
    lg_acts = lg["Action"] if isinstance(lg["Action"], list) else [lg["Action"]]
    assert "logs:DescribeLogGroups" not in lg_acts

    actions = _cdc_deploy_role_actions(template)
    assert "kafka:DeleteCluster" in actions
    assert "kafka:DeleteClusterV2" not in actions, "DeleteClusterV2 is not a real IAM action"
    # Glue Schema Registry is no longer in the architecture (JSON converter, v0.1.5);
    # no Glue permissions should remain on the deploy role.
    assert not any(a.startswith("glue:") for a in actions)


def test_task_role_does_not_hold_privileged_cdc_actions(template: dict) -> None:
    # The security guarantee: those broad privileges live ONLY on the deploy role.
    leaked = _PRIVILEGED_CDC_ACTIONS & _all_task_role_actions(template)
    assert leaked == set(), f"TaskRole must not hold privileged CDC actions: {leaked}"


def test_task_role_can_assume_cdc_deploy_role_scoped(template: dict) -> None:
    statements = _task_role_statements(template)
    assume = statements["AssumeCdcDeployRole"]
    assert assume["Action"] == "sts:AssumeRole"
    # Scoped to the deploy role by name (Sub), never "*".
    assert assume["Resource"] != "*"
    assert "cdc-deploy" in str(assume["Resource"])


def test_cdc_deploy_role_passrole_is_conditioned_to_msk_connect(template: dict) -> None:
    stmts = {
        s["Sid"]: s
        for p in template["Resources"]["CdcDeployRole"]["Properties"]["Policies"]
        for s in p["PolicyDocument"]["Statement"]
    }
    passrole = stmts["IamPassRoleToCdcServices"]
    assert passrole["Action"] == "iam:PassRole"
    # Least privilege: only the two services that consume cdc-stack roles -- MSK
    # Connect (connector exec role) and Lambda (offset-seeder role), nothing else.
    passed_to = passrole["Condition"]["StringEquals"]["iam:PassedToService"]
    assert sorted(passed_to) == [
        "kafkaconnect.amazonaws.com",
        "lambda.amazonaws.com",
    ]


def test_container_injects_cdc_deploy_role_arn(template: dict) -> None:
    container = template["Resources"]["TaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]
    envs = {e["Name"]: e["Value"] for e in container["Environment"]}
    assert envs["DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN"] == {
        "Fn::GetAtt": ["CdcDeployRole", "Arn"]
    }


def test_cdc_deploy_role_arn_output_exists(template: dict) -> None:
    out = template["Outputs"]["CdcDeployRoleArn"]
    assert out["Value"] == {"Fn::GetAtt": ["CdcDeployRole", "Arn"]}


def test_cdc_deploy_role_has_nat_networking_actions(template: dict) -> None:
    actions = _cdc_deploy_role_actions(template)
    for action in (
        "ec2:CreateSubnet", "ec2:DeleteSubnet",
        "ec2:CreateNatGateway", "ec2:DeleteNatGateway",
        "ec2:AllocateAddress", "ec2:ReleaseAddress",
        "ec2:CreateRouteTable", "ec2:CreateRoute", "ec2:AssociateRouteTable",
        "ec2:DescribeVpcs",
    ):
        assert action in actions, f"CdcDeployRole missing {action}"


def _cdc_deploy_role_resource_subs(template: dict) -> list[str]:
    """All Fn::Sub resource strings on the CdcDeployRole's scoped statements."""
    subs: list[str] = []
    role = template["Resources"]["CdcDeployRole"]
    for policy in role["Properties"]["Policies"]:
        for stmt in policy["PolicyDocument"]["Statement"]:
            res = stmt.get("Resource")
            for item in res if isinstance(res, list) else [res]:
                if isinstance(item, dict) and "Fn::Sub" in item:
                    subs.append(item["Fn::Sub"])
    return subs


def test_cdc_deploy_role_scopes_the_dsql_cdc_family_not_a_fixed_name(template: dict) -> None:
    # Multi-DB support: the deploy role must scope the mysql-dsql-cdc-* naming family so
    # one app can manage many concurrent cdc-stacks (mysql-dsql-cdc-orders, …). A scope
    # pinned to the single literal "mysql-dsql-cdc-stack-" would AccessDeny custom names.
    subs = _cdc_deploy_role_resource_subs(template)
    # No statement may pin the old fixed per-stack prefix.
    assert not any("mysql-dsql-cdc-stack-" in s for s in subs), (
        "CdcDeployRole still has a fixed mysql-dsql-cdc-stack- scope; use the mysql-dsql-cdc-* family"
    )
    # The CFN stack, MSK cluster, connectors, logs, and exec-role scopes
    # all use the family wildcard.
    joined = "\n".join(subs)
    assert "stack/mysql-dsql-cdc-*" in joined
    assert "cluster/mysql-dsql-cdc-*-msk/" in joined
    assert "connector/mysql-dsql-cdc-*/" in joined
    assert "log-group:/msk-connect/mysql-dsql-cdc-*-cdc" in joined
    assert "role/mysql-dsql-cdc-*" in joined


def test_cdc_stack_uses_json_converter_not_glue(cdc_template: dict) -> None:
    # The pipeline uses the built-in JSON converter (schemas.enable=true), the
    # spike's proven configuration -- NOT the Glue Avro converter. Reverting to JSON
    # removed the ~59 MiB Glue jar from both plugins and the SDK-conflict surface,
    # and aligns with the only config that reached RUNNING end-to-end.
    resources = cdc_template["Resources"]
    # No Glue Schema Registry resource / policy / output.
    assert "SchemaRegistry" not in resources
    assert "SchemaRegistryArn" not in cdc_template.get("Outputs", {})
    role = resources["ConnectorExecutionRole"]["Properties"]["Policies"]
    assert not any(p.get("PolicyName") == "glue-schema-registry" for p in role)
    # Both worker configs declare the JSON converter with schemas.enable=true (the
    # custom sink's DebeziumEvents parser requires a Connect Struct envelope).
    for cfg_name in ("WorkerConfiguration", "SinkWorkerConfiguration"):
        body = resources[cfg_name]["Properties"]["PropertiesFileContent"]
        text = json.dumps(body)
        assert "org.apache.kafka.connect.json.JsonConverter" in text, cfg_name
        assert "schemas.enable=true" in text, cfg_name
        assert "AWSKafkaAvroConverter" not in text, cfg_name
        assert "schemaregistry" not in text.lower(), cfg_name


def test_cdc_worker_configs_disable_null_to_default_replacement(
    cdc_template: dict,
) -> None:
    """Neither worker's converters may swap a null for the field's schema default.

    Kafka's ``JsonConverter`` substitutes ``schema.defaultValue()`` for a null in BOTH
    directions when ``replace.null.with.default`` is true -- and true is its default.
    Debezium sets that field default from the SOURCE COLUMN's ``DEFAULT`` clause, so
    leaving the flag alone silently wrote the column's default wherever CDC wrote NULL, on
    both source engines, with no error and no dead letter (live-reproduced: ``n int DEFAULT
    7`` set to NULL by CDC landed on Aurora DSQL as 7).

    BOTH worker configs are asserted, and the source one is the easy miss: a source
    connector only SERIALIZES, but the serialize path (``convertToJson``) substitutes too,
    so the default was baked into the JSON on the wire and the NULL never left the source
    connector. Pinning only the sink was live-verified NOT to fix it. The KEY converters
    matter for a re-keyed table, where a fabricated non-NULL key component stopped the
    sink's null-key DELETE guard from ever firing.
    """
    resources = cdc_template["Resources"]
    for cfg_name in ("WorkerConfiguration", "SinkWorkerConfiguration"):
        body = json.dumps(resources[cfg_name]["Properties"]["PropertiesFileContent"])
        for prefix in ("key", "value"):
            assert (
                f"{prefix}.converter.replace.null.with.default=false" in body
            ), f"{cfg_name}/{prefix}"
        # Never the substituting value -- an explicit `=true` reintroduces the corruption.
        assert "replace.null.with.default=true" not in body, cfg_name


def test_cdc_stack_worker_configs_shrink_internal_topic_partitions(
    cdc_template: dict,
) -> None:
    # MSK Connect creates 3 COMPACTED internal topics per connector instance
    # (offsets 25 + status 5 + configs 1 by default = 31). MSK Serverless caps
    # COMPACTED partitions at 120 cluster-wide, and each (re)deploy creates a fresh
    # set (named by connector UUID) that is not reused -- so a few redeploys exhaust
    # the cap and the next connector fails to create. Both worker configs pin the
    # offset/status internal topics to 1 partition (31 -> 3 per connector) so the
    # quota is not exhausted; these carry only low-frequency metadata, so 1 has no
    # effect on replication throughput.
    resources = cdc_template["Resources"]
    for cfg_name in ("WorkerConfiguration", "SinkWorkerConfiguration"):
        text = json.dumps(
            resources[cfg_name]["Properties"]["PropertiesFileContent"]
        )
        assert "offset.storage.partitions=1" in text, cfg_name
        assert "status.storage.partitions=1" in text, cfg_name


def test_cdc_stack_worker_configs_size_message_limits_for_oversized_records(
    cdc_template: dict,
) -> None:
    # H13: a 1-4 MiB change event must be PRODUCED to Kafka by the source and
    # FETCHED + dead-lettered by the sink, so the message-size limits must be
    # raised above the 1 MiB Kafka client default. MSK Connect supports these as
    # WORKER-config "producer."/"consumer." keys (applied to the connector's task
    # clients); it does NOT support per-connector ".override." keys -- it excludes
    # connector.client.config.override.policy entirely (CreateWorkerConfiguration
    # rejects it: "Unsupported key"). So the limits live in the worker configs, and
    # the connector configs must NOT carry ".override." keys.
    resources = cdc_template["Resources"]
    # Source worker config raises the producer (it produces the data-topic records).
    src_wc = json.dumps(resources["WorkerConfiguration"]["Properties"]["PropertiesFileContent"])
    assert "producer.max.request.size=" in src_wc
    # Sink worker config raises consumer fetch + producer (fetch oversized, DLQ it).
    sink_wc = json.dumps(
        resources["SinkWorkerConfiguration"]["Properties"]["PropertiesFileContent"]
    )
    assert "consumer.max.partition.fetch.bytes=" in sink_wc
    assert "consumer.fetch.max.bytes=" in sink_wc
    assert "producer.max.request.size=" in sink_wc

    # The excluded key must NOT appear anywhere (it rolls back the stack create).
    for cfg_name in ("WorkerConfiguration", "SinkWorkerConfiguration"):
        text = json.dumps(resources[cfg_name]["Properties"]["PropertiesFileContent"])
        assert "connector.client.config.override.policy" not in text, cfg_name
    # Connector configs must carry no ".override." keys (unsupported on MSK Connect).
    for conn in ("DebeziumSourceConnector", "DsqlSinkConnector"):
        cfg = resources[conn]["Properties"]["ConnectorConfiguration"]
        assert not any(".override." in k for k in cfg), conn


def test_cdc_sink_worker_config_sets_consumer_poll_and_commit_timeouts(
    cdc_template: dict,
) -> None:
    # This sink's put() is slow and unbounded BY DESIGN: a permanent SQL error in a
    # chunk makes DsqlSinkTask re-apply that chunk row by row (one DSQL transaction
    # per row, up to SinkMaxPollRecords=3000 of them), and a transient failure is
    # retried indefinitely. On Kafka's defaults (max.poll.interval.ms=300000,
    # offset.flush.timeout.ms=5000) one long put() gets the consumer EJECTED from its
    # group; offsets can then never commit, and since errors.tolerance=all and the
    # resulting "Commit of offsets timed out" is only a WARN, the task never fails --
    # the connector reports RUNNING while replication is permanently dead. So these
    # four keys are load-bearing, not tuning, and must not silently disappear.
    sink_wc = json.dumps(
        cdc_template["Resources"]["SinkWorkerConfiguration"]["Properties"][
            "PropertiesFileContent"
        ]
    )
    assert "consumer.max.poll.interval.ms=" in sink_wc
    assert "consumer.session.timeout.ms=" in sink_wc
    assert "consumer.heartbeat.interval.ms=" in sink_wc
    assert "offset.flush.timeout.ms=" in sink_wc

    params = cdc_template["Parameters"]
    # max.poll.interval must exceed Kafka's 300000 default -- that default is the bug.
    assert params["SinkMaxPollIntervalMs"]["Default"] > 300000
    # offset.flush.timeout must exceed Connect's 5000 default (flush() makes a
    # synchronous CloudWatch call inside the commit budget).
    assert params["SinkOffsetFlushTimeoutMs"]["Default"] > 5000
    # Kafka requires heartbeat.interval.ms < session.timeout.ms (guidance: <= 1/3).
    heartbeat = params["SinkHeartbeatIntervalMs"]["Default"]
    session = params["SinkSessionTimeoutMs"]["Default"]
    assert heartbeat * 3 <= session
    # A slow put() must not be able to trip the session timeout on its own: the
    # heartbeat runs on a background thread, so session.timeout covers worker pauses
    # only and stays well below the poll interval.
    assert session < params["SinkMaxPollIntervalMs"]["Default"]


def test_cdc_stack_worker_config_names_carry_plugin_version(cdc_template: dict) -> None:
    # AWS::KafkaConnect::WorkerConfiguration is custom-named and immutable: changing
    # its PropertiesFileContent forces a replacement that collides on a fixed name
    # ("cannot update a stack when a custom-named resource requires replacing").
    # Versioning the name with ${PluginVersion} lets a normal update_stack swap it,
    # the same fix used for the custom plugins (SPIKE gotcha #5).
    resources = cdc_template["Resources"]
    for cfg_name in ("WorkerConfiguration", "SinkWorkerConfiguration"):
        name = resources[cfg_name]["Properties"]["Name"]
        assert "${PluginVersion}" in json.dumps(name), cfg_name


def test_a_plugin_version_bump_renames_only_the_plugins_and_worker_configs(
    cdc_template: dict,
) -> None:
    # What a PLUGIN_VERSION bump replaces: the three CustomPlugins and the two worker
    # configurations carry ${PluginVersion}; nothing else does -- the offset-seeder Lambda keeps
    # ${AWS::StackName}-offset-seeder and a fixed S3 key, so a bump does not reload its code.
    # The PLUGIN_VERSION history in core/s3_provision.py says so (G21 review: the v52 note
    # said the bump renamed the seeder Lambda's resources too).
    versioned = {
        name for name, resource in cdc_template["Resources"].items()
        if "PluginVersion" in json.dumps(resource)
    }
    assert versioned == {
        "DebeziumSourcePlugin", "PostgresSourcePlugin", "DsqlSinkPlugin",
        "WorkerConfiguration", "SinkWorkerConfiguration",
    }
    source = Path(__file__).resolve().parent.parent / "src" / "dsql_migrator" / "core"
    history = " ".join(
        (source / "s3_provision.py").read_text(encoding="utf-8").replace("#", " ").split()
    )
    assert "the bump only renames their resources" not in history
    assert (
        "the bump renames only the CustomPlugins and the worker configurations; the seeder "
        "Lambda keeps its name" in history
    )


def test_cdc_source_connector_has_heartbeat_without_source_write(cdc_template: dict) -> None:
    # A heartbeat keeps Debezium's committed binlog offset advancing while the
    # CAPTURED tables are idle (other tables churn the binlog), so source binlog
    # retention cannot purge past a stale offset and break gapless resume. It must
    # NOT set heartbeat.action.query -- that would WRITE to the read-only source.
    cfg = cdc_template["Resources"]["DebeziumSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    assert cfg.get("heartbeat.interval.ms"), "source connector must set a heartbeat interval"
    assert int(cfg["heartbeat.interval.ms"]) > 0
    assert "heartbeat.action.query" not in cfg, (
        "heartbeat.action.query would write to the READ-ONLY source"
    )


def test_both_source_connectors_emit_source_truncates_parsed(cdc_template: dict) -> None:
    # G3: Debezium's `skipped.operations` defaults to "t", so without this key a source
    # TRUNCATE produces NO record -- the target silently keeps every truncated row. Both
    # engines must carry exactly `none` (the only value that skips nothing; Debezium rejects
    # `none` combined with other codes, and any other value skips exactly the op codes it
    # lists -- the default "t" is this bug), so the sink can dead-letter the event by name
    # and the `source-truncate` banner + Cut over hold can fire. Checked on the PARSED
    # template, as CloudFormation will read it.
    for name in ("DebeziumSourceConnector", "PostgresSourceConnector"):
        cfg = cdc_template["Resources"][name]["Properties"]["ConnectorConfiguration"]
        value = cfg["skipped.operations"]
        assert isinstance(value, str) and value == "none", (name, value)


def test_mysql_source_connector_keeps_a_year_0_to_99_as_it_is(cdc_template: dict) -> None:
    # G126: Debezium's `enable.time.adjuster` defaults to true, and debezium-connector-binlog
    # 2.7.4 (BinlogValueConverters.adjustTemporal) then reads a DATETIME / DATE year 0-69 as
    # 2000-2069 and 70-99 as 1970-1999 -- a 2-digit year MySQL already resolved when the row
    # was written. A .NET DateTime.MinValue (0001-01-01) streamed as 2001-01-01 while Full Load
    # wrote it as it is. MySQL only: the PostgreSQL connector has no such key.
    cfg = cdc_template["Resources"]["DebeziumSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    assert cfg["enable.time.adjuster"] == "false"  # a string, as every connector value


def test_cdc_stack_alarms_on_connector_errored_tasks(cdc_template: dict) -> None:
    # Each connector gets a CloudWatch alarm on AWS/KafkaConnect ErroredTaskCount
    # (dimension ConnectorName) so a FAILED task is surfaced automatically. The alarm
    # is gated to the SAME condition as the connector it watches, so a not-yet-created
    # connector has no permanently-INSUFFICIENT_DATA alarm.
    resources = cdc_template["Resources"]
    expected = {
        "DebeziumSourceErroredTaskAlarm": (
            "HasBootstrapServers", "${AWS::StackName}-debezium-source"),
        "DsqlSinkErroredTaskAlarm": (
            "DeploySinkConnector", "${AWS::StackName}-dsql-sink"),
    }
    for name, (cond, connector_name) in expected.items():
        alarm = resources[name]
        assert alarm["Type"] == "AWS::CloudWatch::Alarm", name
        assert alarm["Condition"] == cond, name
        props = alarm["Properties"]
        assert props["Namespace"] == "AWS/KafkaConnect", name
        assert props["MetricName"] == "ErroredTaskCount", name
        assert props["ComparisonOperator"] == "GreaterThanThreshold", name
        assert int(props["Threshold"]) == 0, name
        dim = props["Dimensions"][0]
        # Exact dimension name per the MSK Connect monitoring docs -- a wrong name
        # means the alarm silently never fires (stuck INSUFFICIENT_DATA).
        assert dim["Name"] == "ConnectorName", name
        assert dim["Value"]["Fn::Sub"] == connector_name, name


def test_cdc_stack_alarm_notification_topic_is_optional(cdc_template: dict) -> None:
    # Notifications are opt-in (deployment convenience -- no SNS wiring needed to
    # deploy). The param defaults empty and the alarm actions are gated on HasAlarmTopic
    # so an empty ARN yields NO action (AWS::NoValue) rather than an invalid action.
    tpl = cdc_template
    assert tpl["Parameters"]["AlarmNotificationTopicArn"]["Default"] == ""
    assert "HasAlarmTopic" in tpl["Conditions"]
    for name in ("DebeziumSourceErroredTaskAlarm", "DsqlSinkErroredTaskAlarm"):
        actions = tpl["Resources"][name]["Properties"]["AlarmActions"]
        assert actions["Fn::If"][0] == "HasAlarmTopic", name
        assert actions["Fn::If"][2] == {"Ref": "AWS::NoValue"}, name


# --- cdc-stack template: tool-owned NAT networking ---------------------------

CDC_STACK_TEMPLATE = DEPLOY_DIR / "cdc-stack" / "cdc-stack.yaml"

_OWNED_NETWORK_RESOURCES = (
    "ConnectorEip",
    "ConnectorNatGateway",
    "ConnectorSubnetA",
    "ConnectorSubnetB",
    "ConnectorPrivateRouteTable",
    "ConnectorNatRoute",
    "ConnectorSubnetARouteTableAssociation",
    "ConnectorSubnetBRouteTableAssociation",
)


@pytest.fixture(scope="module")
def cdc_template() -> dict:
    return yaml.safe_load(CDC_STACK_TEMPLATE.read_text(encoding="utf-8"))


def _collect_refs(node) -> list:
    """Recursively collect every {"Ref": <name>} logical id under ``node``."""
    found: list = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "Ref" and isinstance(value, str):
                found.append(value)
            else:
                found.extend(_collect_refs(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_collect_refs(item))
    return found


def test_cdc_stack_parses_as_yaml(cdc_template: dict) -> None:
    assert cdc_template["AWSTemplateFormatVersion"] == "2010-09-09"
    assert "Resources" in cdc_template


def test_cdc_stack_has_owned_network_resources(cdc_template: dict) -> None:
    resources = cdc_template["Resources"]
    for name in _OWNED_NETWORK_RESOURCES:
        assert name in resources, f"Missing networking resource: {name}"


def test_cdc_stack_owned_network_resources_are_conditional(cdc_template: dict) -> None:
    # All owned-network resources gate on CreateOwnedNetwork so they appear ONLY
    # when ConnectorSubnetIds is empty (and regardless of the connector conditions).
    for name in _OWNED_NETWORK_RESOURCES:
        assert (
            cdc_template["Resources"][name].get("Condition") == "CreateOwnedNetwork"
        ), name


def test_cdc_stack_create_owned_network_condition(cdc_template: dict) -> None:
    assert "CreateOwnedNetwork" in cdc_template["Conditions"]


def test_cdc_stack_connector_has_source_db_egress_with_and_without_sg(
    cdc_template: dict,
) -> None:
    # Regression: the Debezium source connector must always have egress on the
    # MySQL port, or its task cannot open a TCP connection to the source and the
    # connector CREATE_FAILs (SocketTimeoutException) -> stack rollback. The
    # precise SG-to-SG rule only exists when SourceDbSecurityGroupId is supplied,
    # so a fallback (open to 0.0.0.0/0 on SourceDbPort) must cover the empty case.
    resources = cdc_template["Resources"]
    conditions = cdc_template["Conditions"]

    by_sg = resources["ConnectorSourceDbEgress"]
    assert by_sg["Type"] == "AWS::EC2::SecurityGroupEgress"
    assert by_sg["Condition"] == "UseSourceDbSecurityGroup"
    assert by_sg["Properties"]["DestinationSecurityGroupId"] == {
        "Ref": "SourceDbSecurityGroupId"
    }

    fallback = resources["ConnectorSourceDbEgressOpen"]
    assert fallback["Type"] == "AWS::EC2::SecurityGroupEgress"
    assert fallback["Condition"] == "NoSourceDbSecurityGroup"
    assert fallback["Properties"]["CidrIp"] == "0.0.0.0/0"
    assert fallback["Properties"]["FromPort"] == {"Ref": "SourceDbPort"}
    assert fallback["Properties"]["ToPort"] == {"Ref": "SourceDbPort"}

    # The two conditions are exact complements, so EXACTLY one egress rule exists
    # for every value of SourceDbSecurityGroupId (never zero -> never a silent gap).
    assert conditions["UseSourceDbSecurityGroup"] == {
        "Fn::Not": [{"Fn::Equals": [{"Ref": "SourceDbSecurityGroupId"}, ""]}]
    }
    assert conditions["NoSourceDbSecurityGroup"] == {
        "Fn::Equals": [{"Ref": "SourceDbSecurityGroupId"}, ""]
    }


def test_cdc_stack_source_creates_data_topics_sized_for_oversized_records(
    cdc_template: dict,
) -> None:
    # H13 (the BROKER half): raising producer.max.request.size is not enough. MSK
    # Serverless auto-creates each per-table data topic with max.message.bytes at
    # the broker default (~1 MiB), so the broker itself rejects a 1-4 MiB change
    # event ("RecordTooLargeException: ... larger than the max message size the
    # server will accept") and the source task drops it BEFORE it reaches the sink
    # -- silent loss, no DLQ. The source must therefore size the topics it creates
    # via topic.creation.default.max.message.bytes (capped at the 8 MiB Serverless
    # max). Without this the sink-side DLQ for an oversized value is unreachable.
    cfg = cdc_template["Resources"]["DebeziumSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    assert "topic.creation.default.max.message.bytes" in cfg
    # Sized from the same MaxMessageBytes parameter as the client limits, so the
    # topic ceiling and the producer ceiling never drift apart.
    value = cfg["topic.creation.default.max.message.bytes"]
    assert json.dumps(value).find("MaxMessageBytes") != -1, value


def test_mysql_source_connector_sets_no_gtid_source_filter(cdc_template: dict) -> None:
    # G19 (live, Seoul, 2026-10-07): gtid.source.excludes ".*" did NOT force file:position
    # mode on a gtid_mode=ON source. Debezium 2.7.4 applies the filter to the offset's GTID
    # set, so the set it registered with MySQL was EMPTY and MySQL streamed its whole
    # retained binary log on every Start and connector restart -- re-applying history and
    # dying on an unparseable DDL from before the start point. With no filter Debezium
    # starts after the offset's own GTID set on ON, and by file:position on any other
    # mode, so no gtid.source.* key belongs in the connector config.
    cfg = cdc_template["Resources"]["DebeziumSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    assert not [k for k in cfg if str(k).startswith("gtid.source.")]
    raw = CDC_STACK_TEMPLATE.read_text(encoding="utf-8")
    assert "forcing pure file:pos mode" not in raw
    assert "gtid_mode is exactly ON" in raw
    # Backward compatible: the parameter Start CDC seeds the GTID set through is kept,
    # with its empty default (an older stack's Start re-sends it as UsePreviousValue).
    assert cdc_template["Parameters"]["WatermarkGtids"]["Default"] == ""


def test_cdc_stack_schema_history_has_complete_iam_jaas(cdc_template: dict) -> None:
    # Regression: the Debezium schema-history is a SEPARATE Kafka client (MSK
    # Connect only auto-injects IAM auth for the connector's own producer). Declaring
    # sasl.mechanism=AWS_MSK_IAM without the JAAS entry + callback handler boots the
    # task with no 'KafkaClient' JAAS config and it dies. The spike template carried
    # all four lines; they must not be dropped from the production template again.
    cfg = cdc_template["Resources"]["DebeziumSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    for role in ("producer", "consumer"):
        assert (
            cfg[f"schema.history.internal.{role}.sasl.mechanism"] == "AWS_MSK_IAM"
        )
        assert (
            cfg[f"schema.history.internal.{role}.security.protocol"] == "SASL_SSL"
        )
        # The two lines that were missing and killed the source task:
        assert (
            "IAMLoginModule"
            in cfg[f"schema.history.internal.{role}.sasl.jaas.config"]
        )
        assert (
            cfg[f"schema.history.internal.{role}.sasl.client.callback.handler.class"]
            == "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        )


def test_cdc_stack_connector_subnet_ids_defaults_empty(cdc_template: dict) -> None:
    p = cdc_template["Parameters"]["ConnectorSubnetIds"]
    assert p["Type"] == "CommaDelimitedList"
    assert p["Default"] == ""


def test_cdc_stack_msk_cluster_uses_fn_if_for_subnets(cdc_template: dict) -> None:
    subnets = cdc_template["Resources"]["MskCluster"]["Properties"]["VpcConfigs"][0][
        "SubnetIds"
    ]
    assert "Fn::If" in subnets
    fn_if = subnets["Fn::If"]
    assert fn_if[0] == "CreateOwnedNetwork"
    assert {"Ref": "ConnectorSubnetA"} in fn_if[1]
    assert {"Ref": "ConnectorSubnetB"} in fn_if[1]
    assert fn_if[2] == {"Ref": "ConnectorSubnetIds"}


def test_cdc_stack_nat_route_depends_on_nat_gateway(cdc_template: dict) -> None:
    dep = cdc_template["Resources"]["ConnectorNatRoute"].get("DependsOn")
    deps = dep if isinstance(dep, list) else [dep]
    assert "ConnectorNatGateway" in deps
    eip_dep = cdc_template["Resources"]["ConnectorNatGateway"].get("DependsOn")
    eip_deps = eip_dep if isinstance(eip_dep, list) else [eip_dep]
    assert "ConnectorEip" in eip_deps


def test_execution_role_is_separate_and_pulls_from_ecr(template: dict) -> None:
    exec_role = template["Resources"]["ExecutionRole"]["Properties"]
    # The managed ECS execution policy grants ECR pull + CloudWatch Logs only.
    assert (
        "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
        in exec_role["ManagedPolicyArns"]
    )
    # The execution role's only inline policy is reading the auto-generated UI
    # storage secret so ECS can inject it into the container at start (a
    # deploy-time/injection concern, not an app runtime permission). It must be
    # scoped to that secret and must NOT carry the app's runtime perms
    # (dsql:* or the source DB secret) -- those belong to the task role.
    policies = exec_role.get("Policies", [])
    assert [p["PolicyName"] for p in policies] == ["read-storage-secret"]
    statements = [
        stmt
        for policy in policies
        for stmt in policy["PolicyDocument"]["Statement"]
    ]
    assert len(statements) == 1
    storage = statements[0]
    assert storage["Action"] == "secretsmanager:GetSecretValue"
    assert storage["Resource"] == {"Ref": "StorageSecret"}
    # No app-runtime permissions leaked onto the execution role.
    for stmt in statements:
        action = stmt["Action"]
        actions = action if isinstance(action, list) else [action]
        assert not any(a.startswith("dsql:") for a in actions)
        assert stmt["Resource"] != {"Ref": "SourceSecretArn"}
        assert stmt["Resource"] != "*"


def test_alb_security_group_allows_only_https_ingress(template: dict) -> None:
    sg = template["Resources"]["AlbSecurityGroup"]["Properties"]
    ingress = sg["SecurityGroupIngress"]
    assert len(ingress) == 1
    assert ingress[0]["FromPort"] == 443 and ingress[0]["ToPort"] == 443


def test_alb_can_reach_cognito_for_the_oidc_token_exchange(template: dict) -> None:
    # Regression: authenticate-cognito makes the ALB itself call the Cognito hosted UI
    # (/oauth2/token, /oauth2/userInfo) from its own ENIs. Declaring ANY
    # SecurityGroupEgress on the ALB SG replaces the default allow-all egress, so with
    # only the app-port rule the login flow reached the callback and then died in the
    # token exchange with a bare 500 on /oauth2/idpresponse -- and no app log at all,
    # because the request never reaches the app. Observed live before this rule existed.
    egress = template["Resources"]["AlbHttpsEgress"]
    assert egress["Type"] == "AWS::EC2::SecurityGroupEgress"
    # Only needed with Cognito: a no-auth ALB needs no outbound internet.
    assert egress["Condition"] == "CognitoEnabled"
    props = egress["Properties"]
    assert props["GroupId"] == {"Fn::GetAtt": ["AlbSecurityGroup", "GroupId"]}
    assert props["FromPort"] == 443 and props["ToPort"] == 443
    assert props["CidrIp"] == {"Ref": "HttpsEgressCidr"}


def test_service_only_accepts_traffic_from_alb(template: dict) -> None:
    sg = template["Resources"]["ServiceSecurityGroup"]["Properties"]
    ingress = sg["SecurityGroupIngress"]
    assert len(ingress) == 1
    # Inbound only from the ALB security group (not a CIDR).
    assert ingress[0]["SourceSecurityGroupId"] == {
        "Fn::GetAtt": ["AlbSecurityGroup", "GroupId"]
    }
    # Egress is scoped to HTTPS; no allow-all (-1) rule.
    for rule in sg["SecurityGroupEgress"]:
        assert rule.get("IpProtocol") != "-1"
    # Egress must allow the Aurora DSQL data-plane port (PostgreSQL wire, 5432)
    # in addition to HTTPS, otherwise the task cannot reach the DSQL endpoint.
    egress_ports = {rule.get("ToPort") for rule in sg["SecurityGroupEgress"]}
    assert 443 in egress_ports
    assert 5432 in egress_ports


def test_source_db_egress_is_scoped(template: dict) -> None:
    by_sg = template["Resources"]["SourceDbEgressBySg"]
    assert by_sg["Type"] == "AWS::EC2::SecurityGroupEgress"
    assert by_sg["Condition"] == "UseSourceDbSecurityGroup"
    assert by_sg["Properties"]["DestinationSecurityGroupId"] == {
        "Ref": "SourceDbSecurityGroupId"
    }
    assert by_sg["Properties"]["FromPort"] == {"Ref": "SourceDbPort"}


def test_cognito_auth_is_conditional(template: dict) -> None:
    resources = template["Resources"]
    assert resources["UserPool"]["Condition"] == "CognitoEnabled"
    assert resources["UserPoolClient"]["Condition"] == "CognitoEnabled"
    assert template["Conditions"]["CognitoEnabled"] == {
        "Fn::Equals": [{"Ref": "EnableCognitoAuth"}, "true"]
    }


def test_one_deploy_convenience_defaults(template: dict) -> None:
    """The minimal 1-deploy path (only env-specific params) must succeed:
    Cognito OFF by default (no UserPoolDomain with an empty prefix), an internal
    ALB by default, and the published ECR Public image as ContainerImageUri."""
    params = template["Parameters"]
    # Cognito off by default -- an internal/scoped ALB is the gate; with it ON by
    # default the minimal deploy created a UserPoolDomain with an empty prefix and
    # failed (InvalidRequest).
    assert params["EnableCognitoAuth"]["Default"] == "false"
    # Internal ALB by default (Well-Architected SEC05-BP02).
    assert params["AlbScheme"]["Default"] == "internal"
    # No image build required: defaults to the published ECR Public image.
    assert params["ContainerImageUri"]["Default"].startswith("public.ecr.aws/")
    # ...and that default must be PINNED to a real tag, not "latest": a floating tag
    # makes a "same template" deploy irreproducible.
    default_tag = params["ContainerImageUri"]["Default"].rsplit(":", 1)[-1]
    assert default_tag != "latest", params["ContainerImageUri"]["Default"]
    assert default_tag[0].isdigit(), default_tag
    # Safety net: Cognito is still REQUIRED when ingress is open to the internet.
    rule = template["Rules"]["CognitoRequiredWhenIngressOpen"]
    assert rule["RuleCondition"] == {"Fn::Equals": [{"Ref": "AllowedIngressCidr"}, "0.0.0.0/0"]}


def test_source_reachability_rule_requires_sg_or_cidr(template: dict) -> None:
    """At least one of SourceDbSecurityGroupId / SourceDbCidr must be provided so
    the task gets egress to the source DB; with both empty no DB-port egress rule
    is created and the source connection times out. Enforced unconditionally."""
    rule = template["Rules"]["SourceReachabilityRequired"]
    # No RuleCondition -> the assertion is always enforced (every deploy).
    assert "RuleCondition" not in rule
    assertion = rule["Assertions"][0]["Assert"]
    refs = set()
    for branch in assertion["Fn::Or"]:
        equals = branch["Fn::Not"][0]["Fn::Equals"]
        assert equals[1] == "", "each branch asserts the param is non-empty"
        refs.add(equals[0]["Ref"])
    assert refs == {"SourceDbSecurityGroupId", "SourceDbCidr"}
    # Both stay individually optional (each has a Default); the constraint is the
    # pair, so the console labels must remain "[Optional]".
    for name in refs:
        assert "Default" in template["Parameters"][name]


def test_console_parameter_interface_groups_and_labels_every_param(
    template: dict,
) -> None:
    """Uploading this template in the CloudFormation console must render a grouped,
    labelled 'Specify stack details' form. The AWS::CloudFormation::Interface must
    cover EVERY parameter exactly once (no orphan in a flat list) and label each."""
    params = set(template["Parameters"].keys())
    iface = template["Metadata"]["AWS::CloudFormation::Interface"]
    grouped: list[str] = []
    for group in iface["ParameterGroups"]:
        assert group["Label"]["default"]  # every group has a heading
        grouped.extend(group["Parameters"])
    grouped_set = set(grouped)
    labels = set(iface["ParameterLabels"].keys())
    # Every parameter is grouped exactly once and labelled; no orphans either way.
    assert grouped_set == params, f"ungrouped/unknown: {grouped_set ^ params}"
    assert len(grouped) == len(grouped_set), "a parameter is in two groups"
    assert labels == params, f"unlabelled/unknown label: {labels ^ params}"

    # Each label must announce Required/Optional, and that tag must match reality:
    # a parameter is REQUIRED exactly when it has no Default (the console then
    # forces a value), OPTIONAL when a Default exists. The tag is a "[Required...]"
    # / "[Optional...]" chip embedded after the human-readable field name (e.g.
    # "VPC  [Required] — ..."), not necessarily the very first characters, so the
    # assertion checks the tag is PRESENT and matches reality rather than the label
    # starting with it.
    #
    # One deliberate exception: a mutually-exclusive pair where EITHER satisfies the
    # requirement (SourceDbSecurityGroupId / SourceDbCidr). Each carries a Default
    # (so it is individually optional), but the label reads "[Required — this OR
    # ...]" to convey that at least one of the pair is required. Allow that form for
    # those two only.
    or_required = {"SourceDbSecurityGroupId", "SourceDbCidr"}
    for name, spec in template["Parameters"].items():
        label = iface["ParameterLabels"][name]["default"]
        has_default = "Default" in spec
        if not has_default:
            assert "[Required" in label and "[Optional" not in label, (
                f"{name}: no Default -> label must carry a [Required...] tag; "
                f"got {label!r}"
            )
        elif name in or_required:
            assert "[Required — this OR" in label, (
                f"{name}: mutually-exclusive pair -> label must read "
                f"'[Required — this OR ...]'; got {label!r}"
            )
        else:
            assert "[Optional" in label and "[Required" not in label, (
                f"{name}: has a Default -> label must carry an [Optional...] tag; "
                f"got {label!r}"
            )


def test_bedrock_invoke_is_opt_in_and_scoped(template: dict) -> None:
    resources = template["Resources"]
    policy = resources["BedrockInvokePolicy"]
    assert policy["Type"] == "AWS::IAM::Policy"
    assert policy["Condition"] == "AiAssistEnabled"

    statement = policy["Properties"]["PolicyDocument"]["Statement"][0]
    assert "bedrock:InvokeModel" in statement["Action"]
    # Resource is an Fn::If: an explicit BedrockModelArns override, or the
    # provider-scoped Anthropic default (inference-profile + foundation-model).
    # Never a blanket "*"; the provider stays pinned to `anthropic.` so a UI model
    # switch among Anthropic models needs no redeploy but non-Anthropic / image /
    # Marketplace models are never invokable.
    resource = statement["Resource"]
    assert resource != "*"
    fn_if = resource["Fn::If"]
    assert fn_if[0] == "HasBedrockModelArnsOverride"
    assert fn_if[1] == {"Ref": "BedrockModelArns"}
    derived = fn_if[2]
    assert isinstance(derived, list) and len(derived) == 2  # profile + foundation-model
    profile_arn = derived[0]["Fn::Sub"]
    fm_arn = derived[1]["Fn::Sub"]
    # Account-scoped Anthropic inference profiles (any geo, any region) + any
    # Anthropic foundation model (region-agnostic, AWS-owned so no account id).
    assert "${AWS::AccountId}:inference-profile/*.anthropic.*" in profile_arn
    assert ":bedrock:*::foundation-model/anthropic.*" in fm_arn
    # Provider PINNED to anthropic. -> never a provider-less wildcard, never "*".
    assert ".anthropic." in profile_arn
    assert "foundation-model/anthropic." in fm_arn
    assert "foundation-model/*" not in str(derived)
    assert "inference-profile/*\"" not in str(derived)  # not a bare inference-profile/*


def test_ai_role_never_needs_aws_marketplace_permissions(template: dict) -> None:
    """The task role can only INVOKE Bedrock models -- by design (G18).

    A third-party model's one-time AWS Marketplace agreement is accepted once per
    account by someone who has AWS Marketplace permissions; the tool tells the operator
    so (MODEL_AGREEMENT_REQUIRED) instead of the app widening its own role. The docs
    state this, so neither deployment template may grow an ``aws-marketplace:`` action
    or a GetFoundationModelAvailability check (which needs one more IAM action and was
    seen reporting AVAILABLE for a model that was still denied).

    An AWS managed policy (e.g. AWSMarketplaceManageSubscriptions, or the usual "quick
    fix" for a Bedrock denial, AmazonBedrockFullAccess) grants such actions without
    naming them in the template, so the managed policies of the role the app's Bedrock
    calls run as are pinned too: none on the Fargate TaskRole, and only the SSM agent's
    on the EC2 HostRole.
    """
    for template_path in (CFN_TEMPLATE, DEPLOY_DIR / "cloudformation-ec2.yaml"):
        text = template_path.read_text(encoding="utf-8")
        assert "aws-marketplace:" not in text, template_path.name
        assert "GetFoundationModelAvailability" not in text, template_path.name
    assert "ManagedPolicyArns" not in template["Resources"]["TaskRole"]["Properties"]
    ec2 = yaml.safe_load(
        (DEPLOY_DIR / "cloudformation-ec2.yaml").read_text(encoding="utf-8")
    )
    assert ec2["Resources"]["HostRole"]["Properties"]["ManagedPolicyArns"] == [
        {"Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"}
    ]


_REPO_ROOT = DEPLOY_DIR.parent
_MANUAL_02 = {
    lang: _REPO_ROOT / "docs" / "manual" / lang / "02-evaluation-and-schema-conversion.md"
    for lang in ("en", "ko", "ja")
}


def _flat(text: str) -> str:
    return " ".join(text.split())


def _block_with(text: str, needle: str) -> str:
    """The blank-line-delimited block containing ``needle``, whitespace-collapsed."""
    for block in text.split("\n\n"):
        if needle in _flat(block):
            return _flat(block)
    raise AssertionError(f"no block contains {needle!r}")


def test_g18_docs_quote_the_ui_copy_of_a_model_access_failure_word_for_word() -> None:
    """A quoted UI message must be findable in the UI, and vice versa (G18).

    The docs quote one sentence for the model-agreement (AWS Marketplace) denial -- as
    what the AI DBA panel and the Connect status say, and as a troubleshooting symptom
    -- so it must be word for word in BOTH fixed copies (the panel shows the chat copy,
    Verify AI access the preflight copy). And the manual must not promise that sentence
    for every subscription failure: only a denial that names AWS Marketplace produces
    it; a missing first-time use form can instead surface as Verify AI access's
    *not enabled or not available*, which the manual now says, quoting that copy.
    """
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS, _UNAVAILABLE_DETAILS

    phrase = "the configured model is not enabled for this AWS account yet"
    for copy in (
        _UNAVAILABLE_DETAILS["MODEL_AGREEMENT_REQUIRED"],
        _ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"],
    ):
        assert phrase.casefold() in _flat(copy).casefold(), copy
    docs = [*_MANUAL_02.values()] + [
        DEPLOY_DIR / name for name in ("DEPLOYMENT.md", "DEPLOYMENT.ko.md", "DEPLOYMENT.ja.md")
    ]
    for path in docs:
        assert f"*{phrase}*" in _flat(path.read_text(encoding="utf-8")), path.name

    form_case = "not enabled or not available"
    assert form_case in _flat(_ACCESS_CHECK_DETAILS["MODEL_NOT_ENABLED"])
    for lang, path in _MANUAL_02.items():
        assert f"*{form_case}*" in _flat(path.read_text(encoding="utf-8")), lang
    en = _flat(_MANUAL_02["en"].read_text(encoding="utf-8"))
    assert "Bedrock then denies the model and the tool says" not in en


def test_g18_docs_describe_the_verify_fallback_instead_of_a_single_call() -> None:
    """Verify AI access is not always "one minimal call to the configured model" (G18).

    When Bedrock rejects the configured model as unavailable, or denies it for a reason
    other than its model agreement, the preflight also tries a fallback model, and
    Connect switches the Model ID to it when it answers. Every README and manual chapter
    2 that describes the preflight must say so, in every language, and none may claim a
    single call.
    """
    described = {
        # path: (the block's anchor, the word for the fallback, the single-call claim
        # with all whitespace removed)
        _REPO_ROOT / "README.md": (
            "**Verify AI access** preflight", "fallback", "oneminimalcall"
        ),
        _REPO_ROOT / "README.ko.md": (
            "**Verify AI access** 사전 점검", "fallback", "호출을한번보내"
        ),
        _REPO_ROOT / "README.ja.md": (
            "**Verify AI access** プリフライト", "フォールバック", "呼び出しを1回行"
        ),
        _MANUAL_02["en"]: (
            "2. Use the **Verify AI access** preflight", "fallback", "oneminimalcall"
        ),
        _MANUAL_02["ko"]: (
            "2. **Verify AI access** 사전 점검", "fallback", "호출을한번보내"
        ),
        _MANUAL_02["ja"]: (
            "2. **Verify AI access** のプリフライト", "フォールバック", "呼び出しを1回行"
        ),
    }
    for path, (anchor, fallback, single_call) in described.items():
        block = _block_with(path.read_text(encoding="utf-8"), anchor)
        assert fallback in block, (path.name, block)
        assert single_call not in "".join(block.split()), (path.name, block)


def test_bedrock_model_id_is_curated_dropdown_with_auto_scope(template: dict) -> None:
    """BedrockModelId is a curated Anthropic picker for the app's DEFAULT model; the
    IAM scope is provider-wide (any Anthropic), not derived from this value, and
    HasBedrockModelArnsOverride is false on the empty default -- so an operator
    enables AI by toggling EnableAiAssist + picking a model, with no ARNs."""
    spec = template["Parameters"]["BedrockModelId"]
    allowed = spec["AllowedValues"]
    assert spec["Default"] in allowed
    # GLOBAL profiles only. A `global.` profile resolves from every commercial region
    # (verified against us-east-1, us-west-2 and ap-northeast-2), so offering the `us.`
    # equivalents alongside them added a value that fails outside three US regions -- a
    # trap, not a choice -- and let the template default drift from the app's.
    assert allowed and all(v.startswith("global.anthropic.") for v in allowed)
    assert not any(v.startswith("us.anthropic.") for v in allowed)
    assert template["Conditions"]["HasBedrockModelArnsOverride"] == {
        "Fn::Not": [{"Fn::Equals": [{"Fn::Join": ["", {"Ref": "BedrockModelArns"}]}, ""]}]
    }
    # BedrockModelArns stays an optional override (has a Default -> [Optional]).
    assert "Default" in template["Parameters"]["BedrockModelArns"]

    # The base task role must NOT grant Bedrock unconditionally.
    base_actions = [
        stmt["Action"] for stmt in _task_role_statements(template).values()
    ]
    flat = [
        a
        for actions in base_actions
        for a in ([actions] if isinstance(actions, str) else actions)
    ]
    assert not any("bedrock" in a for a in flat)


def test_bedrock_model_dropdown_matches_cfn_allowed_values(template: dict) -> None:
    """The Connect Model ID dropdown (SUPPORTED_BEDROCK_MODELS) must equal the CFN
    BedrockModelId AllowedValues, so a model added to one is added to the other and the
    picker never offers a value the parameter would reject (or vice versa)."""
    from dsql_migrator.ui.ai_assist import (
        DEFAULT_BEDROCK_MODEL_ID,
        SUPPORTED_BEDROCK_MODELS,
    )

    allowed = template["Parameters"]["BedrockModelId"]["AllowedValues"]
    assert list(SUPPORTED_BEDROCK_MODELS) == list(allowed)
    # The app default is offered in the picker and is the CFN default.
    assert DEFAULT_BEDROCK_MODEL_ID in SUPPORTED_BEDROCK_MODELS
    assert template["Parameters"]["BedrockModelId"]["Default"] == DEFAULT_BEDROCK_MODEL_ID


def test_ai_assist_condition_defaults_off(template: dict) -> None:
    params = template["Parameters"]
    assert params["EnableAiAssist"]["Default"] == "false"
    assert template["Conditions"]["AiAssistEnabled"] == {
        "Fn::Equals": [{"Ref": "EnableAiAssist"}, "true"]
    }


# --- Cloud build (CodeBuild, no local Docker) --------------------------------


@pytest.fixture(scope="module")
def codebuild_template() -> dict:
    return yaml.safe_load(CODEBUILD_TEMPLATE.read_text(encoding="utf-8"))


def test_codebuild_template_provisions_build_infra(codebuild_template: dict) -> None:
    types = {res["Type"] for res in codebuild_template["Resources"].values()}
    assert "AWS::ECR::Repository" in types
    assert "AWS::S3::Bucket" in types
    assert "AWS::CodeBuild::Project" in types
    assert "AWS::IAM::Role" in types


def test_codebuild_project_uses_privileged_mode_and_buildspec(
    codebuild_template: dict,
) -> None:
    project = codebuild_template["Resources"]["BuildProject"]["Properties"]
    # Docker builds require privileged mode in the managed environment.
    assert project["Environment"]["PrivilegedMode"] is True
    assert project["Source"]["BuildSpec"] == "deploy/buildspec.yml"


def test_codebuild_role_ecr_push_is_repo_scoped(codebuild_template: dict) -> None:
    role = codebuild_template["Resources"]["CodeBuildServiceRole"]["Properties"]
    statements = [
        stmt
        for policy in role["Policies"]
        for stmt in policy["PolicyDocument"]["Statement"]
    ]
    # GetAuthorizationToken is account-wide; layer/image push is repo-scoped.
    push = next(
        s for s in statements if "ecr:PutImage" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    )
    assert push["Resource"] == {"Fn::GetAtt": ["EcrRepository", "Arn"]}


def test_codebuild_role_public_push_is_repo_scoped(codebuild_template: dict) -> None:
    # The ECR PUBLIC publish actions take a repository ARN just like their private
    # ecr: counterparts above, so they must be scoped too -- a release build may push
    # the release image and nothing else. Left on "*" this is an unconstrained write
    # (the finding a content security review raises), and nothing but this test would
    # notice it regressing: the release path works either way.
    role = codebuild_template["Resources"]["CodeBuildServiceRole"]["Properties"]
    statements = [
        stmt
        for policy in role["Policies"]
        for stmt in policy["PolicyDocument"]["Statement"]
    ]
    publish = next(
        s
        for s in statements
        if "ecr-public:PutImage"
        in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    )
    resource = publish["Resource"]
    assert resource != "*", "ecr-public publish must be scoped to the repository ARN"
    rendered = json.dumps(resource)
    assert "ecr-public" in rendered and "${EcrRepoName}" in rendered, rendered
    # Only the two token APIs stay account-wide -- they have no resource-level form.
    unscoped = {
        action
        for s in statements
        if s["Resource"] == "*"
        for action in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    }
    assert unscoped == {
        "ecr:GetAuthorizationToken",
        "ecr-public:GetAuthorizationToken",
        "sts:GetServiceBearerToken",
    }, f"unexpected unscoped actions: {unscoped}"


def test_ecr_repository_is_kms_encrypted(codebuild_template: dict) -> None:
    # ECR defaults to AES256; KMS (with no KmsKey, i.e. the AWS managed aws/ecr key)
    # is what the review asks for and costs nothing. EncryptionConfiguration is
    # immutable, so this is also a reminder that changing it replaces the repo.
    repo = codebuild_template["Resources"]["EcrRepository"]["Properties"]
    assert repo.get("EncryptionConfiguration") == {"EncryptionType": "KMS"}
    # Replacement/teardown must stay possible: the repo empties itself on delete.
    assert repo.get("EmptyOnDelete") is True


def test_buildspec_builds_amd64_and_pushes() -> None:
    text = BUILDSPEC.read_text(encoding="utf-8")
    assert "docker build" in text
    assert "linux/amd64" in text
    assert "docker push" in text
    assert "deploy/Dockerfile" in text


# --- cdc-stack template: automatic gapless offset seeder ---------------------

_OFFSET_SEED_FIXED_TOPIC = "${AWS::StackName}-debezium-source-offsets"


def test_cdc_stack_source_worker_pins_fixed_offset_topic_not_sink(
    cdc_template: dict,
) -> None:
    # The SOURCE worker config pins a fixed offset.storage.topic so the seeder
    # Lambda can create + seed that exact topic before the connector exists. The
    # SINK worker config must NOT pin it (it has its own offsets, and a shared
    # fixed name would collide).
    resources = cdc_template["Resources"]
    src = json.dumps(resources["WorkerConfiguration"]["Properties"]["PropertiesFileContent"])
    sink = json.dumps(resources["SinkWorkerConfiguration"]["Properties"]["PropertiesFileContent"])
    assert f"offset.storage.topic={_OFFSET_SEED_FIXED_TOPIC}" in src
    assert "offset.storage.topic=" not in sink


def test_cdc_stack_seeder_function_persists_across_stop(
    cdc_template: dict,
) -> None:
    # The seeder Role + Function are gated on DeploySeederFunctionLambda = the
    # existing DeploySeederFunction (Fn::Or: bootstrap present OR code key supplied)
    # AND SeedByLambda. In the default (Lambda) mode SeedByLambda is true, so this
    # equals the old DeploySeederFunction gate exactly: the OR still means the code
    # key alone keeps the Function deployed when a Stop blanks MskBootstrapServers
    # (persists across Stop, keeping the slow VPC-Lambda ENI teardown off the Stop
    # path). SeedMode=External drops all three seeder resources instead.
    resources = cdc_template["Resources"]
    for name in ("OffsetSeederRole", "OffsetSeederFunction"):
        assert name in resources, name
        assert resources[name].get("Condition") == "DeploySeederFunctionLambda", name
    # The invoker (pre-creates topics + seeds the offset) is gated on
    # DeployStartPrepResource = HasBootstrapServers AND SeedByLambda, so a Stop
    # removes it (fast) and a Lambda-mode Start re-creates + reruns it.
    assert resources["CdcStartPrepResource"].get("Condition") == "DeployStartPrepResource"

    conds = cdc_template["Conditions"]
    # DeploySeederFunctionLambda ANDs the DeploySeederFunction with SeedByLambda.
    lambda_gate = json.dumps(conds["DeploySeederFunctionLambda"])
    assert "DeploySeederFunction" in lambda_gate
    assert "SeedByLambda" in lambda_gate
    deploy = conds["DeploySeederFunction"]
    deploy_text = json.dumps(deploy)
    # The MySQL binlog offset seeder is gated on IsMySqlSource (a PostgreSQL stack
    # resumes from a replication slot, not a Kafka connect-offset, so it must never
    # create this Lambda). No-op for MySQL (IsMySqlSource is true by default).
    assert "IsMySqlSource" in deploy_text
    # Nested under that AND, the original Fn::Or (bootstrap present OR key present)
    # still keeps the Function deployed across a Stop (which blanks MskBootstrapServers).
    assert "Fn::Or" in deploy_text
    assert "LambdaSeederS3Key" in deploy_text
    assert "MskBootstrapServers" in deploy_text


def test_cdc_stack_seeder_is_in_vpc_python_lambda_from_plugin_bucket(
    cdc_template: dict,
) -> None:
    fn = cdc_template["Resources"]["OffsetSeederFunction"]["Properties"]
    assert fn["Handler"] == "seeder.handler"
    assert str(fn["Runtime"]).startswith("python3")
    # Runs in the connector SG + subnets (already allow 9098 to MSK + 443 egress).
    assert "VpcConfig" in fn
    vpc = fn["VpcConfig"]
    assert "SecurityGroupIds" in vpc and "SubnetIds" in vpc
    # Code is fetched from the managed plugin bucket via the seeder key.
    assert fn["Code"]["S3Key"] == {"Ref": "LambdaSeederS3Key"}
    # Environment carries the fixed topic + connector identity the handler needs.
    env = fn["Environment"]["Variables"]
    assert env["OFFSETS_TOPIC"] == {"Fn::Sub": _OFFSET_SEED_FIXED_TOPIC}
    assert "CONNECTOR_NAME" in env and "MSK_BOOTSTRAP" in env


def test_cdc_stack_seeder_pins_full_s3_egress_path_for_teardown(
    cdc_template: dict,
) -> None:
    # Regression (cdc-stack delete hung ~1h on OffsetSeedResource): the seeder
    # Lambda's Delete handler PUTs to CloudFormation's S3 ResponseURL, which routes
    # via the S3 GATEWAY endpoint (no NAT). For that route to survive teardown, the
    # WHOLE path must be deleted AFTER the custom-resource Delete responds. We force
    # that ordering with an implicit-dependency env var that References every link.
    # Referencing only the endpoint (the earlier, insufficient fix) let CloudFormation
    # delete the subnet<->route-table ASSOCIATIONS in parallel during the Delete,
    # dropping the seeder subnet to the VPC main route table (no S3 route) -> PUT
    # timed out -> hang. So assert all three links are pinned.
    env = cdc_template["Resources"]["OffsetSeederFunction"]["Properties"][
        "Environment"
    ]["Variables"]
    # The ordering var is owned-network-conditional (Fn::If on CreateOwnedNetwork).
    ordering = next(
        (v for k, v in env.items() if "EGRESS" in k or "ENDPOINT_ORDERING" in k), None
    )
    assert ordering is not None, "seeder must carry an S3 egress-ordering env var"
    refs = set(_collect_refs(ordering))
    for link in (
        "ConnectorS3Endpoint",
        "ConnectorSubnetARouteTableAssociation",
        "ConnectorSubnetBRouteTableAssociation",
    ):
        assert link in refs, f"egress-ordering env must Ref {link} (got {sorted(refs)})"


def test_cdc_stack_sg_has_inline_443_egress_for_seeder_teardown(
    cdc_template: dict,
) -> None:
    # Regression / PRIMARY fix for the teardown hang: the offset-seeder's
    # cfnresponse PUT (to S3) needs an SG rule allowing 443 outbound, and that rule
    # MUST be INLINE on ConnectorSecurityGroup -- an inline rule is part of the SG
    # and cannot be deleted while the Lambda's ENI references the SG, which CFN can't
    # delete until the custom-resource Delete is handled. A standalone
    # AWS::EC2::SecurityGroupEgress resource has no such ordering and is deleted in
    # parallel with the Delete -> PUT times out -> ~1h hang, then DELETE_FAILED with a
    # billable MSK cluster left behind.
    #
    # The rule must exist on BOTH network modes. It used to be gated on
    # CreateOwnedNetwork ("customer subnets reach S3 via their own egress"), but the
    # customer's NAT route is irrelevant once the SG stops permitting 443 -- so the
    # BYO-subnet path failed exactly as the owned path had: observed on
    # mysql-dsql-cdc-stack-0727 (ConnectorSubnetIds supplied), where the standalone
    # egress rule was DELETE_COMPLETE while the custom resource was still waiting.
    sg = cdc_template["Resources"]["ConnectorSecurityGroup"]["Properties"]
    inline = sg.get("SecurityGroupEgress")
    assert inline is not None, "ConnectorSecurityGroup must have an inline egress rule"
    assert not (isinstance(inline, dict) and "Fn::If" in inline), (
        "the inline 443 egress must NOT be conditional on the network mode -- a "
        "BYO-subnet deploy needs it for the seeder's teardown response too"
    )
    rule_list = inline
    https = [
        r
        for r in rule_list
        if r.get("FromPort") == 443 and r.get("ToPort") == 443
    ]
    assert https, f"inline SG egress must allow 443 (got {rule_list})"
    # 443 egress uses CidrIp 0.0.0.0/0 (S3 still routes via the gateway endpoint).
    # The S3 managed prefix-list id is NOT a CloudFormation attribute of the VPC
    # endpoint -- a !GetAtt on it fails template validation. The teardown fix is that
    # the rule is INLINE on the SG (asserted above), not its destination scope.
    assert https[0].get("CidrIp") == "0.0.0.0/0", (
        f"inline 443 egress should use CidrIp 0.0.0.0/0 (got {https[0]})"
    )
    assert "DestinationPrefixListId" not in https[0], (
        "must not reference a (nonexistent) AWS::EC2::VPCEndpoint PrefixListId"
    )
    # And the standalone duplicate must be GONE: keeping it would re-introduce the
    # very resource whose parallel deletion broke the teardown (and it is now exactly
    # redundant with the inline rule).
    assert "ConnectorHttpsEgress" not in cdc_template["Resources"], (
        "the standalone 443 egress resource must not come back -- it is deleted in "
        "parallel with the custom-resource Delete"
    )


def test_cdc_stack_seeder_role_scoped_to_the_fixed_offset_topic(
    cdc_template: dict,
) -> None:
    # Least privilege: the seeder may only touch the one fixed offsets topic, not
    # topic/* on the cluster.
    policies = cdc_template["Resources"]["OffsetSeederRole"]["Properties"]["Policies"]
    topic_stmt = next(
        s
        for p in policies
        for s in p["PolicyDocument"]["Statement"]
        if s["Sid"] == "MskSeedTopic"
    )
    resource = json.dumps(topic_stmt["Resource"])
    assert "debezium-source-offsets" in resource
    # The VPC ENI permissions come from the managed AWSLambdaVPCAccessExecutionRole.
    managed = json.dumps(
        cdc_template["Resources"]["OffsetSeederRole"]["Properties"]["ManagedPolicyArns"]
    )
    assert "AWSLambdaVPCAccessExecutionRole" in managed


def test_cdc_stack_connectors_deploy_in_parallel_via_start_prep(
    cdc_template: dict,
) -> None:
    # Both connectors depend on CdcStartPrepResource (pre-created topics), NOT on
    # each other, so they deploy in ONE parallel pass. The source expresses the
    # ordering via the OffsetSeed tag's Fn::GetAtt; SeedMode gating wraps the whole
    # Tags property in Fn::If [SeedByLambda, <the tag>, AWS::NoValue] so it resolves
    # to today's exact tag in the default (Lambda) mode and drops in External. The
    # sink expresses ordering via a hard DependsOn. Crucially the sink must NOT
    # DependsOn the source connector -- that was the old serial ordering.
    resources = cdc_template["Resources"]
    src = resources["DebeziumSourceConnector"]["Properties"]
    tags = src["Tags"]
    assert "Fn::If" in tags and tags["Fn::If"][0] == "SeedByLambda"
    lambda_tags = tags["Fn::If"][1]  # the Lambda-mode Tags list
    seed_tag = next(t for t in lambda_tags if t["Key"] == "OffsetSeed")
    assert seed_tag["Value"] == {"Fn::GetAtt": ["CdcStartPrepResource", "Seeded"]}

    # The Lambda-mode sink variant keeps the hard DependsOn on CdcStartPrepResource.
    sink_depends = resources["DsqlSinkConnector"].get("DependsOn", [])
    assert "CdcStartPrepResource" in sink_depends
    assert "DebeziumSourceConnector" not in sink_depends


def test_cdc_start_prep_passes_partition_map_and_max_bytes(cdc_template: dict) -> None:
    # Pre-creation must reproduce Debezium topic.creation's shaping, so the seeder
    # gets the per-topic partition map (SinkTopicPartitions) and max.message.bytes.
    props = cdc_template["Resources"]["CdcStartPrepResource"]["Properties"]
    assert props["SinkTopics"] == {"Ref": "SinkTopics"}
    assert props["SinkTopicPartitions"] == {"Ref": "SinkTopicPartitions"}
    assert props["MaxMessageBytes"] == {"Ref": "MaxMessageBytes"}
    assert "SinkTopicPartitions" in cdc_template["Parameters"]
    # The seeder also pre-creates the sink DLQ topic at MaxMessageBytes so a >1 MiB
    # dead-lettered record isn't RecordTooLarge'd against a 1 MiB auto-created DLQ.
    assert props["DlqTopicName"] == {"Ref": "DlqTopicName"}
    # G19: the offset seeder reads its start point ONLY from these resource properties.
    # Without WatermarkGtids a gtid_mode=ON source re-reads its whole retained binary log
    # (the seeded offset carries no GTID set); without WatermarkBinlogFile/Pos nothing is
    # seeded. Every key the app emits must be forwarded.
    from dsql_migrator.core.cdc import CDC_WATERMARK_PARAM_KEYS

    for key in CDC_WATERMARK_PARAM_KEYS:
        assert props[key] == {"Ref": key}, key


def test_cdc_stack_seeder_iam_covers_per_table_topics(cdc_template: dict) -> None:
    # The seeder now CREATES the per-table data topics too, so its IAM CreateTopic
    # grant must cover them (scoped to <TopicPrefix>.*), not only the fixed offsets
    # topic -- else every start fails with a Kafka authorization error.
    role = cdc_template["Resources"]["OffsetSeederRole"]["Properties"]
    stmts = role["Policies"][0]["PolicyDocument"]["Statement"]
    seed = next(s for s in stmts if s.get("Sid") == "MskSeedTopic")
    assert "kafka-cluster:CreateTopic" in seed["Action"]
    resources = seed["Resource"]
    assert isinstance(resources, list)
    blob = json.dumps(resources)
    assert "debezium-source-offsets" in blob  # the fixed offsets topic
    assert "${TopicPrefix}." in blob           # the per-table data topics
    assert "${DlqTopicName}" in blob           # the pre-created sink DLQ topic


def test_cdc_stack_declares_watermark_and_seeder_params(cdc_template: dict) -> None:
    params = cdc_template["Parameters"]
    for p in (
        "LambdaSeederS3Key",
        "WatermarkBinlogFile",
        "WatermarkBinlogPos",
        "WatermarkGtids",
        "WatermarkTsSec",
    ):
        assert p in params, p
        # All default to empty so a deploy without a watermark is valid (no seeder).
        assert params[p].get("Default", None) == "", p


def test_cdc_stack_declares_the_start_gtid_mode_record(cdc_template: dict) -> None:
    # G20: Start CDC records the source gtid_mode each start positions under. Default "" so
    # a stack deployed by an earlier version takes it at its next Start (Start sends the
    # bundled template); no AllowedValues, so no recorded value can fail UpdateStack
    # mid-Start; and it is no seeder input (the seeder ZIP is unchanged) -- nothing refers
    # to it at all.
    param = cdc_template["Parameters"]["StartGtidMode"]
    assert param["Type"] == "String" and param["Default"] == ""
    assert "AllowedValues" not in param and "AllowedPattern" not in param
    prep = cdc_template["Resources"]["CdcStartPrepResource"]["Properties"]
    assert "StartGtidMode" not in prep
    assert "StartGtidMode" not in _collect_refs(cdc_template["Resources"])
    assert "StartGtidMode" not in _collect_refs(cdc_template.get("Conditions", {}))
    text = CDC_STACK_TEMPLATE.read_text(encoding="utf-8")
    assert "Ref: StartGtidMode" not in text and "!Ref StartGtidMode" not in text
    assert "${StartGtidMode}" not in text


def test_public_image_default_is_not_left_behind_the_app_version(template: dict) -> None:
    """The ECR Public default tag must track the shipped version.

    This is what a NEW customer gets: CloudFormation pulls this exact tag, so a stale
    default silently hands them an old build. It had drifted to 0.1.34 while the app was
    at 0.1.164 -- 130 releases behind, missing every fix in between -- because publishing
    to ECR Public is an opt-in extra step (PUBLIC_IMAGE_URI) that is easy to skip.

    Enforced as "not far behind" rather than "exactly equal": a patch release does not
    have to be republished, but the default must not be allowed to rot.
    """
    import pathlib
    import re

    pyproject = (
        pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"
    ).read_text(encoding="utf-8")
    app_version = re.search(r'^version = "([^"]+)"', pyproject, re.MULTILINE).group(1)

    default_uri = template["Parameters"]["ContainerImageUri"]["Default"]
    default_tag = default_uri.rsplit(":", 1)[-1]

    def _patch(v: str) -> int:
        return int(v.split(".")[-1])

    # Same major.minor line...
    assert default_tag.rsplit(".", 1)[0] == app_version.rsplit(".", 1)[0], (
        f"published default {default_tag} is on a different line to {app_version}"
    )
    # ...and within a reasonable window of the shipped patch level.
    drift = _patch(app_version) - _patch(default_tag)
    assert 0 <= drift <= 20, (
        f"ECR Public default is {drift} patch releases behind the app "
        f"({default_tag} vs {app_version}) -- republish with "
        f"PUBLIC_IMAGE_URI=public.ecr.aws/<alias>/mysql-dsql-migrator and bump the "
        f"ContainerImageUri default in deploy/cloudformation.yaml"
    )


# ---------------------------------------------------------------------------
# Manual cross-links. The KO/JA chapters had 16 links pointing at ENGLISH
# anchors, which can never resolve: a GitHub anchor is derived from the heading
# text, so a translated page has no English slug. They were silently dead.
# ---------------------------------------------------------------------------


def _github_anchor_slugs(path) -> set:
    """Return the anchor slugs GitHub generates for a markdown file's headings.

    GitHub lowercases the heading, drops everything that is not a word character,
    whitespace or hyphen (so `§`, `*`, `(`, `.` and `—` all vanish), then turns runs of
    whitespace into hyphens. ``re.UNICODE`` matters: `\\w` must keep Hangul and Kana, or
    every translated heading would collapse to an empty slug and the check would pass
    vacuously.
    """
    import re

    slugs = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("#"):
            heading = line.lstrip("#").strip()
            cleaned = re.sub(r"[^\w\s-]", "", heading.lower(), flags=re.UNICODE)
            slugs.add(re.sub(r"\s", "-", cleaned))
    return slugs


def test_every_manual_anchor_link_resolves() -> None:
    """Each `](file.md#anchor)` in the manual must point at a heading that exists.

    Catches two failure modes that are invisible in review: a translated page linking to
    an English anchor, and a link left behind when a section is renamed or moved between
    chapters (which is exactly what happened moving the type reference from chapter 4 to
    chapter 2).
    """
    import pathlib
    import re
    import urllib.parse

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    broken: list[str] = []
    checked = 0

    for md in sorted(manual.rglob("*.md")):
        for match in re.finditer(
            r"\]\((0[0-9][^)#]*\.md)?#([^)]+)\)", md.read_text(encoding="utf-8")
        ):
            checked += 1
            target = md.parent / match.group(1) if match.group(1) else md
            anchor = urllib.parse.unquote(match.group(2))
            where = md.relative_to(manual)
            if not target.exists():
                broken.append(f"{where}: {match.group(0)} (no such file)")
            elif anchor not in _github_anchor_slugs(target):
                broken.append(f"{where}: {match.group(0)} (no such anchor)")

    # Guard the guard: if the scan found nothing, it would pass vacuously.
    assert checked > 50, f"only {checked} anchor links found -- is the scan still right?"
    assert not broken, "broken manual links:\n  " + "\n  ".join(broken)


def test_anchor_slug_helper_matches_github_for_a_known_heading() -> None:
    # The whole check rests on this slug rule, so pin it against real headings in all
    # three languages -- including a Korean and a Japanese one, since a `\w` without
    # re.UNICODE would silently drop those characters.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"

    en = _github_anchor_slugs(manual / "en" / "03-full-load.md")
    assert "35-the-watermark--the-bridge-to-cdc" in en

    ko = _github_anchor_slugs(manual / "ko" / "01-setup.md")
    assert "11-사전-요구사항" in ko

    ja = _github_anchor_slugs(manual / "ja" / "01-setup.md")
    assert "11-前提条件" in ja


def test_the_cut_over_runbook_in_every_language_covers_a_cdc_only_truncate() -> None:
    # G3: section 5.5 "The way forward" is the written form of the Cut over step's runbook,
    # whose CDC only branch now has a separate TRUNCATE line (cdc_dlq_gate.way_forward_lines):
    # that dead letter names no row, so "the card lists each record's primary key" cannot
    # find what to delete. Each language must give the same steps and point back to 4.2.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    for lang in ("en", "ko", "ja"):
        text = (manual / lang / "05-validation.md").read_text(encoding="utf-8")
        section = text[text.index("## 5.5") : text.index("## 5.6")]
        (bullet,) = [line for line in section.split("\n- ") if line.startswith("**CDC only:**")]
        assert "`TRUNCATE`" in bullet, lang
        assert bullet.index("Stop CDC") < bullet.index("3,000") < bullet.index("Start CDC"), lang
        assert "(04-cdc-and-dsql-constraints.md#42-" in bullet, lang


def test_the_drift_lag_paragraph_promises_only_what_the_sink_cool_down_does() -> None:
    # G10: DsqlSinkTask's per-table cool-down (isStructuralDrift) starts only for a missing
    # column/table/schema, a key, type or NOT NULL change; the other tables keep batching but
    # still share the task thread, so their lag is NOT "normal". A value-level rejection
    # (23505/23514/22001/22003/07006) starts no cool-down: its chunk still replays per row.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    cases = {
        "en": ("**Lag while a table drifts.**", "### What gets dead-lettered",
               ("stays normal", "normal speed", "for a schema reason")),
        "ko": ("**드리프트 중의 지연.**", "### DLQ로 가는 것",
               ("정상으로 유지", "정상 속도", "스키마 문제로")),
        "ja": ("**ドリフト中の遅延。**", "### デッドレター化される対象",
               ("通常どおり", "通常の速度", "スキーマ上の理由で")),
    }
    for lang, (lag_head, dlq_head, overclaims) in cases.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        start = text.index(lag_head)
        lag = text[start : text.index("\n\n", start)]
        dlq_start = text.index(dlq_head)
        dlq = text[dlq_start : text.index("\n### ", dlq_start + 1)]
        for part, name in ((lag, "lag"), (dlq, "dlq")):
            for claim in overclaims:
                assert claim not in part, (lang, name, claim)
            # The value-level exception is spelled out where the isolation is described.
            assert "`CHECK`" in part, (lang, name)
        assert "`NOT NULL`" in lag and "60" in lag, lang


def test_manual_4_2_scopes_schema_changes_flag_and_does_not_route_ddl_through_schema_conversion() -> None:
    # G11: include.schema.changes=false is a MySQL-connector setting (pgoutput carries no
    # DDL at all), and Schema Conversion's Apply -- Replace included -- is blocked while CDC
    # streams (Replace drops the table, so the loaded rows would be gone). The 4.2 intro
    # used to say the flag applied to Debezium in general and to apply DDL "via Schema
    # Conversion first".
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    forbidden = {
        "en": "(via Schema Conversion) **first**",
        "ko": "(Schema Conversion으로) **먼저**",
        "ja": "(Schema Conversion 経由で) **先に**",
    }
    for lang, old in forbidden.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        start = text.index("\n## 4.2")
        intro = text[start : text.index("\n### ", start)]
        # The manual no longer names the connector settings (include.schema.changes, the
        # pgoutput stream): it says CDC replicates row changes, not DDL, which holds for both.
        assert "Fix target schema" in intro, lang
        assert old not in text, lang


def test_no_manual_or_readme_page_routes_live_ddl_through_schema_conversion() -> None:
    # G11: the same "re-apply it via Schema Conversion" / "not lost" advice lived outside
    # the chapters G1-G10 touched. Apply is blocked while CDC streams, and some source DDL
    # dead-letters nothing, so neither claim holds.
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    manual = root / "docs" / "manual"
    stale = {
        root / "README.md": ["go through Schema Conversion"],
        root / "README.ko.md": ["Schema Conversion으로 처리"],
        root / "README.ja.md": ["ではなく Schema Conversion"],
        manual / "en" / "00-before-you-begin.md": ["re-apply them yourself via Schema"],
        manual / "ko" / "00-before-you-begin.md": ["Schema Conversion으로 직접 다시 적용"],
        manual / "ja" / "00-before-you-begin.md": ["Schema Conversion によって自分で再適用"],
        manual / "en" / "02-evaluation-and-schema-conversion.md": [
            "re-apply the equivalent DDL here yourself"
        ],
        manual / "ko" / "02-evaluation-and-schema-conversion.md": ["동일한 DDL을 여기서 직접 다시 적용"],
        manual / "ja" / "02-evaluation-and-schema-conversion.md": ["同等の DDL をここで"],
        manual / "en" / "08-testing-and-verification.md": ["goes to the **DLQ**, not lost"],
        manual / "ja" / "08-testing-and-verification.md": ["失われずに **DLQ**"],
        manual / "en" / "11-customer-faq.md": ["(dead-letter), not lost"],
        manual / "ko" / "11-customer-faq.md": ["조용히 사라지지 않고"],
        manual / "ja" / "11-customer-faq.md": ["失われるのではなく"],
    }
    for path, phrases in stale.items():
        text = path.read_text(encoding="utf-8")
        for phrase in phrases:
            assert phrase not in text, (path.name, phrase)


def test_06_recovery_restart_window_covers_the_whole_run() -> None:
    # G11: a connector created in recovery keeps that mode until Stop CDC
    # (core.cdc.mysql_snapshot_mode rule 1), so the automatic-restart risk window is the
    # whole run, not "that first run" -- and a restart after Stop resumes in schema_only.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    old = {"en": "restart of that first run", "ko": "그 첫 실행이", "ja": "その初回実行の"}
    for lang, phrase in old.items():
        text = (manual / lang / "06-limitations.md").read_text(encoding="utf-8")
        assert phrase not in text, lang
        # (2026-10-26, answer-first: 6.2 no longer names the snapshot modes; the window is still "until Stop CDC")
        assert "Stop CDC" in text, lang
    en = " ".join((manual / "en" / "06-limitations.md").read_text(encoding="utf-8").split())
    assert "until Stop CDC" in en


def test_sink_retry_timeout_comment_does_not_claim_it_retries_put() -> None:
    # G11: errors.retry.timeout wraps only Connect's conversion stage; DsqlSinkTask.put()
    # retries transient errors indefinitely (RetriableException), and OccRetry re-runs a
    # whole TRANSACTION (applyChunk: the chunk; applyRecordByRecord: one row) -- nothing in
    # the plugin retries a single statement.
    text = CDC_STACK_TEMPLATE.read_text(encoding="utf-8")
    assert "absorbed by the" not in text
    assert text.count("CONVERSION stage only") == 2
    assert "retried per statement" not in text
    assert text.count("retried per transaction") == 2


def test_manual_4_says_the_sink_retries_occ_per_transaction_not_per_statement() -> None:
    # G11 review: OccRetry wraps the whole chunk transaction (DsqlSinkTask.applyChunk) or
    # one row's (applyRecordByRecord); 4.1 / 4.4 claimed a "statement level" retry.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    stale = {"en": "**statement level**", "ko": "**문장 단위**로", "ja": "**ステートメント単位で**"}
    new = {"en": "**transaction**", "ko": "**트랜잭션**", "ja": "**トランザクション**"}
    for lang, phrase in stale.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        assert phrase not in text, lang
        occ_rows = [line for line in text.splitlines() if "`SQLSTATE 40001`" in line]
        # 4.1 (why a custom sink) and the 4.4 table row both name the transaction.
        assert text.count(new[lang]) >= 2, lang
        assert any(new[lang] in row for row in occ_rows), lang


def _manual_4_2_drift_region(lang: str) -> str:
    """4.2's drift sub-section, up to the next ### heading (the banner-row table leads it, and
    the 'which dead letters get a banner row' paragraph and its bullets follow the table)."""
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
    start = text.index("\n### ", text.index("\n## 4.2"))
    return text[start : text.index("\n### ", start + 1)]


def test_manual_4_2_banner_row_scope_matches_the_drift_classifier() -> None:
    # G11 review: the paragraph after the Fix target schema bullets claimed EVERY rejection
    # with a SQLSTATE gets a banner row and that "no SQLSTATE" is why the rest do not. The
    # classifier is the single source of truth: 22P02 / 23503 / 54000 stay unmapped (no
    # banner row), while 42P01 / 23505 / 07006 and the sink's synthetic codes ARE mapped.
    # Every code the page puts on either side must agree with classify_schema_drift.
    import re

    from dsql_migrator.core.cdc import classify_schema_drift
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_CODE

    assert classify_schema_drift(OVERSIZED_VALUE_CODE) is None  # the "too large" bullet
    code = re.compile(r"`([0-9][0-9A-Z]{4})`")
    stale = {
        "en": [
            "Every other rejection that Aurora DSQL reports with a SQLSTATE",
            "carry no SQLSTATE",
            "on MySQL, a change record with no key",
            "is caught by the source schema re-check below",
        ],
        "ko": ["SQLSTATE와 함께 보고하는 그 밖의 모든 거부", "SQLSTATE가 없으므로", "MySQL에서, 소스 기본 키가"],
        "ja": ["SQLSTATE 付きで報告するそのほかの拒否", "SQLSTATE がないため", "MySQL で、ソースの主キー"],
    }
    for lang, phrases in stale.items():
        region = _manual_4_2_drift_region(lang)
        for phrase in phrases:
            assert phrase not in region, (lang, phrase)
        blocks = region.split("\n\n")
        (banner,) = [b for b in blocks if "`42P01`" in b]
        mapped = set(code.findall(banner))
        assert {"42P01", "23505", "07006"} <= mapped, lang
        assert all(classify_schema_drift(c) is not None for c in mapped), (lang, mapped)
        (no_banner_list,) = [b for b in blocks if "`22P02`" in b]
        bullets = no_banner_list.split("\n- ")
        (unmapped_bullet,) = [b for b in bullets if "`22P02`" in b]
        unmapped = set(code.findall(unmapped_bullet))
        assert {"22P02", "23503", "54000"} <= unmapped, lang
        assert all(classify_schema_drift(c) is None for c in unmapped), (lang, unmapped)
        # The other no-banner dead letters, and what to do after any source DDL.
        assert "`timetz[]`" in no_banner_list and "`bytea[]`" in no_banner_list, lang
        assert "#45-" in no_banner_list, lang  # the oversized-value bullet links 4.5
        # The no-key record is not MySQL-only, and the re-check does report the key drop.
        assert "primary key changed at the source" in " ".join(no_banner_list.split()), lang
        assert "PostgreSQL" in [b for b in bullets if "primary key changed" in b][0], lang
        assert "06-limitations.md#62-" in region and "CHECKSUM" in region, lang


def test_manual_4_2_does_not_offer_replace_as_a_mode_apply_does_not_block() -> None:
    # G11 review: "Its Apply is blocked ... Its other mode, Replace, ..." read as if Replace
    # were a way around the block (it is an ApplyMode; _cdc_blocks_apply returns before the
    # REPLACE branch), and it named the wrong failure: after the recreate the upserts
    # SUCCEED into an empty table -- the loaded rows are simply gone.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    stale = {"en": "Its other mode", "ko": "다른 모드인", "ja": "もう一つのモード"}
    included = {"en": "**Replace** included", "ko": "**Replace**를 포함해", "ja": "**Replace** を含めて"}
    for lang, phrase in stale.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        start = text.index("\n## 4.2")
        intro = text[start : text.index("\n### ", start)]
        assert phrase not in intro, lang
        assert included[lang] in " ".join(intro.split()), lang
        assert "**Drop & reload**" in intro, lang


def test_ddl_advice_names_the_recreate_for_a_type_or_key_change() -> None:
    # G11 review: "Fix target schema… or an ALTER" was offered for every source change, but
    # Aurora DSQL has no ALTER COLUMN … TYPE and cannot change a primary key: those need a
    # recreate with CDC stopped (Replace + Drop & reload), as the 4.2 drift table says.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    markers = {
        "06-limitations.md": {
            "en": "**CDC replicates data, not DDL.**",
            "ko": "**CDC는 데이터를 복제할 뿐 DDL은 복제하지 않음.**",
            "ja": "**CDC が複製するのはデータであり、DDL ではありません。**",
        },
        "00-before-you-begin.md": {
            "en": "**CDC replicates data, not schema.**",
            "ko": "**CDC는 스키마가 아니라 데이터를 복제합니다.**",
            "ja": "**CDC が複製するのはスキーマではなくデータです。**",
        },
        "02-evaluation-and-schema-conversion.md": {
            "en": "not by re-applying it",
            "ko": "다시 적용하는 방식이 아닙니다",
            "ja": "ここで再適用するのではありません",
        },
    }
    needs = {
        "06-limitations.md": ["**Replace** + **Drop & reload**", "DROP NOT NULL"],
        "00-before-you-begin.md": ["DROP NOT NULL", "Fix target schema"],
        "02-evaluation-and-schema-conversion.md": ["**Replace**", "**Drop & reload**"],
    }
    for page, per_lang in markers.items():
        for lang, marker in per_lang.items():
            # whitespace-flattened: the page is hard-wrapped
            text = " ".join((manual / lang / page).read_text(encoding="utf-8").split())
            assert text.count(marker) == 1, (page, lang)
            window = text[text.index(marker) :][:700]
            for phrase in needs[page]:
                assert phrase in window, (page, lang, phrase)


def test_06_pg_ddl_while_stopped_is_its_own_item_not_a_recheck_blind_spot() -> None:
    # G11 review: the PostgreSQL stopped-DDL caveat sat in "What that re-check cannot see",
    # but the re-check DOES see a key / NULL-rule change (cdc_catalog_drift.diff_table emits
    # KIND_KEY_CHANGE / KIND_NULLABILITY_CHANGE for any source). What it cannot see is that
    # the backlog was already applied with the current definition -- so it is a top-level
    # item of its own, not a nested blind-spot bullet.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    titles = {
        "en": "**PostgreSQL: DDL while CDC is stopped.**",
        "ko": "**PostgreSQL: CDC를 멈춘 동안의 DDL.**",
        "ja": "**PostgreSQL: CDC 停止中の DDL。**",
    }
    for lang, title in titles.items():
        text = (manual / lang / "06-limitations.md").read_text(encoding="utf-8")
        assert text.count(title) == 1, lang
        assert f"\n- {title}" in text, lang  # top level, not "  - " under the blind spots
        item = text[text.index(title) :]
        item = item[: item.index("\n- ")]
        assert "`pgoutput`" in item and "CHECKSUM" in item, lang


def test_06_schema_only_restart_names_the_missing_history_exception() -> None:
    # G11 review: mysql_snapshot_mode rule 3 -- a resume whose schema history is missing
    # rebuilds it, so "a restart after Stop CDC is not affected" was wrong for exactly that Start.
    # 2026-10-26 (answer-first): 6.2 no longer names the snapshot modes (`schema_only` / `recovery`); its
    # "A restart after Stop CDC is different" bullet says the recorded history is replayed, what happens when
    # it is missing, and that the stale "is not affected" wording is gone.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    item_titles = {
        "en": ("**A restart after Stop CDC is different.**",
               "If that history is missing, the Start fails saying so", "is not affected"),
        "ko": ("**Stop CDC 후의 재시작은 다릅니다.**",
               "그 히스토리가 없으면 Start가 그렇다고 알리며 실패합니다", "영향을 받지 않습니다"),
        "ja": ("**Stop CDC 後の再起動は別です。**",
               "その履歴が失われていれば Start はそう示して失敗します", "影響を受けません"),
    }
    for lang, (title, missing, stale) in item_titles.items():
        text = (manual / lang / "06-limitations.md").read_text(encoding="utf-8")
        item = text[text.index(title) :]
        item = " ".join(item[: item.index("\n\n")].split())
        if lang == "ja":
            item = item.replace(" ", "")
        assert missing.replace(" ", "" if lang == "ja" else " ") in item, lang
        assert stale.replace(" ", "" if lang == "ja" else " ") not in item, lang


# --- PostgreSQL CDC: the app seeds MSK itself (SeedMode=External) --------------
# A PostgreSQL cdc-stack has no in-VPC offset-seeder Lambda, so THIS task creates the
# Kafka topics and the CDC start offset over the MSK Serverless IAM endpoint on 9098
# before any connector exists. That needs three things from this template, and without
# them Start CDC dies with KafkaTimeoutError -- after the MSK cluster is already billing.


def test_service_can_reach_msk_9098_for_the_external_seed(template: dict) -> None:
    # The task SG declares an explicit inline SecurityGroupEgress list, which SUPPRESSES
    # the default allow-all egress. So 9098 needs its own rule or it is a deny at the
    # source. Asserted on the PARSED rule, not on the text: grepping for "9098" would
    # also match the explanatory comments and the parameter description.
    rule = template["Resources"]["MskEgress"]
    assert rule["Type"] == "AWS::EC2::SecurityGroupEgress"
    props = rule["Properties"]
    assert props["GroupId"] == {"Fn::GetAtt": ["ServiceSecurityGroup", "GroupId"]}
    assert (props["IpProtocol"], props["FromPort"], props["ToPort"]) == ("tcp", 9098, 9098)
    # Its OWN destination parameter, NOT HttpsEgressCidr: that one is documented as the
    # NAT/PrivateLink range for AWS PUBLIC endpoints, while the MSK bootstrap resolves
    # inside the cdc-stack VPC -- an operator narrowing it would silently break PG CDC.
    assert props["CidrIp"] == {"Ref": "MskEgressCidr"}
    assert template["Parameters"]["MskEgressCidr"]["Default"] == "0.0.0.0/0"
    # It must be the TASK security group that gets the rule, not some other SG.
    net = template["Resources"]["Service"]["Properties"]["NetworkConfiguration"]
    assert net["AwsvpcConfiguration"]["SecurityGroups"] == [
        {"Fn::GetAtt": ["ServiceSecurityGroup", "GroupId"]}
    ]


def test_task_role_can_seed_msk_in_process_with_offsets_only_data_access(
    template: dict,
) -> None:
    statements = _task_role_statements(template)
    # It must be the TASK role: the seed runs on the ambient credential chain
    # (core/cdc_kafka_seed.py threads no session), NOT as the assumed CdcDeployRole.
    connect = statements["MskConnect"]
    assert set(connect["Action"]) == {
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:WriteDataIdempotently",
    }
    create = statements["MskSeedCreateTopics"]
    assert set(create["Action"]) == {
        "kafka-cluster:CreateTopic",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:DescribeTopicDynamicConfiguration",
        "kafka-cluster:AlterTopicDynamicConfiguration",
    }

    def _arns(stmt):
        return [r["Fn::Sub"] for r in stmt["Resource"]]

    # Bounded to the tool's cdc-stack family, and BOTH prefixes: the canonical
    # "dsql-cdc-*" and the legacy "mysql-dsql-cdc-*" that existing deploys still use.
    for stmt in (connect, create):
        arns = _arns(stmt)
        assert any("cluster/dsql-cdc-*-msk" in a or "topic/dsql-cdc-*-msk" in a for a in arns)
        assert any("mysql-dsql-cdc-*-msk" in a for a in arns)
        assert not any(a.strip() == "*" for a in arns)

    # The load-bearing narrowing: the seed reads and writes ONLY the offsets topic, so
    # this long-lived, ALB-fronted role must NOT be able to consume the replicated data
    # topics of every cdc-stack in the account. A "topic/<family>/*/*" resource here
    # would be that over-grant, so assert the offsets suffix on EVERY arn.
    data = statements["MskSeedOffsetTopicData"]
    assert set(data["Action"]) == {"kafka-cluster:ReadData", "kafka-cluster:WriteData"}
    assert _arns(data) and all(
        a.endswith("-debezium-source-offsets") for a in _arns(data)
    )
    # No consumer-group action at all: the seed uses consumer.assign() and joins none.
    all_actions = {
        a
        for stmt in statements.values()
        for a in (
            stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        )
    }
    assert not any("Group" in a for a in all_actions if a.startswith("kafka-cluster:"))


def test_container_attests_the_msk_seed_capability(template: dict) -> None:
    # The app cannot observe either of the two grants above at runtime (it holds no
    # iam:Simulate*, and ec2:DescribeSecurityGroups belongs to CdcDeployRole), so this
    # template revision DECLARES the capability. Shipped in the same revision as both
    # grants, so its presence implies them -- which is what lets the pre-flight refuse
    # an un-updated stack in seconds instead of spending 5-20 min of billable MSK.
    container = template["Resources"]["TaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]
    env = {e["Name"]: e["Value"] for e in container["Environment"]}
    assert env["DSQL_MIGRATOR_CDC_MSK_ACCESS"] == "true"
    # NOT via DSQL_MIGRATOR_CDC_SEED_MODE: that would also switch MySQL off the
    # cdc-stack's in-VPC seeder Lambda, which is its live-verified path.
    assert "DSQL_MIGRATOR_CDC_SEED_MODE" not in env


def test_the_task_role_can_read_the_source_network_for_the_vpc_prefill(
    template: dict,
) -> None:
    """Without this grant the CDC VpcId prefill can never work on a managed deployment.

    It reads the source DB's own DBSubnetGroup, and the deployed task role had NO
    ``rds:Describe*`` at all (IAM simulate: ``rds:DescribeDBInstances`` -> implicitDeny;
    neither template mentioned it) -- so the read was AccessDenied, swallowed, and the field
    just stayed empty. It worked from a laptop whose credentials happened to have RDS
    access, which is how a missing grant looked like a lost feature.

    ``DescribeDBClusters`` is load-bearing too: an Aurora source is given as a CLUSTER
    endpoint and the writer member is resolved from the cluster.
    """
    statements = _task_role_statements(template)
    actions: set[str] = set()
    for stmt in statements.values():
        acts = stmt["Action"]
        actions.update(acts if isinstance(acts, list) else [acts])

    assert "rds:DescribeDBInstances" in actions, sorted(
        a for a in actions if a.startswith("rds:")
    )
    assert "rds:DescribeDBClusters" in actions, sorted(
        a for a in actions if a.startswith("rds:")
    )
    # Read-only: no RDS mutation may sneak in beside them.
    assert all(
        a.split(":", 1)[1].startswith("Describe")
        for a in actions
        if a.startswith("rds:")
    ), sorted(a for a in actions if a.startswith("rds:"))


def test_the_committed_sink_zip_contains_the_current_java_sources() -> None:
    """A Java change must not ship without rebuilding the plugin ZIP.

    THE root cause of the v44 defect, in one sentence: v0.1.541 taught Schema Conversion to
    substitute a PostgreSQL array to jsonb and shipped NO connector change, so the sink
    received an array for the first time and dead-lettered every write for the table --
    silently, because an UPDATE that never lands changes no row count.

    The deployed sink is the committed ZIP, not the source tree (MSK Connect registers the
    ZIP), and nothing checked that the two agree. The Lambda already has this guard
    (`test_committed_zip_embeds_current_lambda_sources`); the sink did not, so a rebuilt
    source with a stale ZIP would deploy the OLD behaviour while every unit test passed.

    Checked by MARKER rather than by recompiling: a byte-for-byte comparison would need the
    Maven toolchain (which the "a fresh git clone can deploy" principle deliberately does not
    require -- that is why the ZIPs are committed at all). So this asserts that a distinctive
    string from each behaviour the ZIP must carry is actually inside it. A behaviour whose
    marker is missing means: run `mvn -B -ntp package`, re-zip, and bump PLUGIN_VERSION.
    """
    import io
    import pathlib
    import zipfile

    root = pathlib.Path(__file__).resolve().parents[1]
    zip_path = root / "connectors/plugins/dsql-sink-plugin.zip"
    assert zip_path.is_file(), zip_path

    # A jar is itself a DEFLATE-compressed ZIP, so its raw bytes carry no readable strings --
    # the class entries have to be decompressed before a marker can be found in them.
    classes: dict[str, bytes] = {}
    with zipfile.ZipFile(zip_path) as bundle:
        jars = [n for n in bundle.namelist() if n.endswith(".jar")]
        assert jars, bundle.namelist()
        for name in jars:
            with bundle.open(name) as raw, zipfile.ZipFile(io.BytesIO(raw.read())) as jar:
                for entry in jar.namelist():
                    if entry.endswith(".class"):
                        classes[entry] = classes.get(entry, b"") + jar.read(entry)
    blob = b"".join(classes.values())

    # One marker per behaviour the ZIP must carry. Each is a literal this repo's Java source
    # owns, so it can only be present if that source was compiled into the committed ZIP.
    markers = {
        # v54 (G126, rebuilt in place): a failure to OPEN the connection (the IAM token refused,
        # too many connections, the connection rate) is transient instead of a poison row that
        # dead-lettered every record while it lasted.
        "refused connect is transient": b"Could not open a connection to Aurora DSQL",
        # v54: a PostgreSQL infinity / -infinity date or timestamp is mapped, and any
        # RuntimeException a value's conversion throws is a DataException naming the column
        # (dead-letters one record instead of killing the task).
        "conversion failure names its column": b"Cannot convert the value of column '",
        # v54 (rebuilt in place): a PostgreSQL point is written as its own text.
        "pg point as text": b"A PostgreSQL point arrived without its x/y coordinates",
        # ...a MySQL TIME duration is refused by name from a time column (ValueFit DURATION,
        # and at bind time when the column types could not be read).
        "time duration refused": b"is a duration, which a time-of-day column",
        # v53 (G25, rebuilt in place): a hold that ends -- applied, dead-lettered, or replaced
        # by a later change of its row -- is released only when the put returns normally, and
        # a probe's transient failure no longer fails the put: Connect's redelivery of a failed
        # put's batch wrote the released partition's later records ahead of its never-written
        # tail and committed past it (a row lost silently after a crash). The marker is the
        # nested type that carries the decision.
        "hold released after the put returns": b"DsqlSinkTask$HoldEnd",
        # ...a change skipped because a later change of its row replaces it stays skipped in
        # Connect's redelivery of its put, whose held tail now holds that later change (it was
        # dead-lettered with 23505 once per redelivery). G23's skip itself is kept as is.
        "skip kept through a redelivery": b"heldTailInPut",
        # ...time Aurora DSQL could not answer does not count against the 5-minute budget.
        "hold budget excludes outages": b"lastTransientNanos",
        # ...a held change is reported as replication lag while it waits (no op counted).
        "held change as lag": b"recordLag",
        # ...every logged hold is closed by a line: the look-ahead replacement and a revoke.
        "hold replaced line": b"so the hold on unique index {} ended",
        "hold dropped line": b"next owner reads it again",
        # ...the retries Aurora DSQL asked for are logged once per offset commit.
        "retry summary": b"asked this task to retry a transaction",
        # ...a describe failure inside a move's transaction is reported with its own cause
        # (such as 42703), not as the 25P02 it leads to.
        "describe failure kept": b"DsqlSinkTask$Described",
        # ...a held change's try that Aurora DSQL does not answer is logged (WARN, about once a
        # minute while it lasts) and backed off from 500 ms to 10 s: it no longer fails the put,
        # so Connect logged nothing and the sink reconnected every 500 ms. The marker is the
        # WARN literal of DsqlSinkTask.UNIQUE_HOLD_TRY_FAILED.
        "held try failure logged and backed off": b"less often (up to every 10 s) while this",
        # v53 (G23): a unique-index conflict (23505) is held on its partition -- rewound,
        # paused, its commit capped -- until the earlier change that frees the value (a
        # primary-key change's old-row delete, on another partition) applies, for up to 5 min,
        # instead of being dead-lettered at once (the row was missing). The marker is the hold
        # INFO literal of DsqlSinkTask.UNIQUE_HOLD_START.
        "unique conflict hold": b"until the earlier change that frees the value",
        # v52 (G22, rebuilt in place): Aurora DSQL's XX000 "server unavailable" is retried
        # like a 40001 and, once the budget is spent, redelivered -- never dead-lettered. The
        # marker is OccRetry.SERVER_UNAVAILABLE_SQLSTATE; no class of the pre-G22 jar (shaded
        # dependencies included) holds it.
        "server unavailable retry": b"XX000",
        # v52 (G21): every OCC retry starts on a clean transaction -- the failed attempt is
        # rolled back before the re-run, because Aurora DSQL can report a 40001 (OC001, a
        # stale catalog) while a statement runs, which leaves the transaction aborted; the
        # retry then failed with 25P02 and a healthy row was dead-lettered. The marker is the
        # WARN literal of DsqlSinkTask.rollbackBeforeRetry, reworded in G22 because the
        # failed attempt can now be an XX000.
        "OCC retry rollback": b"failed attempt before its retry",
        # v51 (G15): a re-keyed table's records are keyed on the SOURCE primary key and the
        # sink writes them by the composite target key read from target.key.columns, so a
        # leading-column change is one ordered move and a DELETE never misses its row.
        "source-keyed composite writes": b"target.key.columns",
        # v51 (G15, live follow-up): an UPDATE with no before-image (REPLICA IDENTITY DEFAULT)
        # replaces the row holding its source key once the unique index on it is valid,
        # instead of an upsert the index rejects (23505, the old row kept).
        "unknown old key replace": b"can have an unknown old key",
        # ...and its delete spares a row already at the new composite key, so an UPDATE that
        # kept the key is updated in place (an excluded column keeps its value).
        "replace spares an unmoved row": b" AND NOT (",
        # v51 (G8): a value its Aurora DSQL column cannot hold exactly is REFUSED pre-write
        # with a value-free reason -- not stored as true (boolean) or rounded (numeric/int).
        "value-fit refusal": b"Value does not fit target column '",
        # v51 (G10): a table DSQL keeps rejecting for a schema reason goes one row per
        # transaction for a bounded cool-down, so healthy tables keep batching.
        "drift cool-down": b"batching resumed",
        # v48: field values are read VERBATIM. Struct.get(Field) substitutes the schema's
        # default for a null slot, and Debezium sets that default from the source column's
        # DEFAULT clause -- so a source NULL was written to the target as the default, and a
        # defaulted KEY column fabricated a delete key that matched 0 rows. Both engines.
        "verbatim field read": b"getWithoutDefault",
        # v47: a source TRUNCATE is dead-lettered with the TRUNCATE NAMED. Without the
        # sink's op='t' branch a truncate envelope (no after-image, no key) fell through to
        # buildDelete and dead-lettered as "Cannot build DELETE ... no primary key" -- loud
        # about the wrong thing. Pairs with `skipped.operations: none` on BOTH source blocks
        # (PostgreSQL and MySQL), which is what makes the event reach the sink at all.
        "pg truncate named": b"has no TRUNCATE statement",
        # v46: an unbounded `bit varying` column's declared length is REFUSED instead of
        # allocated. Padding to it asked String.repeat for a multi-gigabyte array and the JVM
        # raised OutOfMemoryError -- an Error, so errors.tolerance/the DLQ could not catch it and
        # the sink task died permanently, halting replication.
        "pg bit length bound": b"that is not a storable column width",
        # v45: each array ELEMENT is rendered from its Debezium LOGICAL TYPE, not its Java
        # class. v44 bound the array as jsonb but rendered elements by class, so a
        # timestamp[]/date[]/time[] element (a raw epoch Long/Integer on the wire) became a
        # bare JSON number where to_jsonb writes a quoted ISO string -- valid jsonb, accepted
        # by DSQL, and detected only by Validation's CHECKSUM after the target already held
        # the wrong bytes. This marker comes from the timetz[] refusal, which is new in v45.
        "pg array element logical types": b"to_jsonb keeps the SOURCE offset",
        # v44: PostgreSQL arrays bind as jsonb (the silent total-loss fix).
        "pg array -> jsonb": b"renders array columns as jsonb",
        # v43: the quarantine line carries the DML op, which is what tells a recoverable
        # dead-letter (re-read the source) from an unrecoverable one (a lost DELETE).
        "quarantine op tag": b"| op: ",
    }
    # A marker a shaded dependency also holds proves nothing blob-wide, so it is looked for
    # in the class that owns it.
    class_markers = {
        # v54 (G65, rebuilt in place): a whole decimal bound into an integer parameter is
        # bound as a long (`"id" = ?` on a numeric(12,0) serial key made a bigint identity
        # became `"id"::numeric = $1`, a full scan per DELETE). The shaded pgjdbc
        # ByteConverter also calls longValueExact, so only DsqlSinkTask's own use counts.
        "whole decimal key bound as a long": (
            "dev/dsqlmigrator/connect/DsqlSinkTask.class", b"longValueExact"
        ),
        # v54 (G126, rebuilt in place): a JSON value (any PGobject) counts its text against the
        # 8 MiB chunk budget -- as a 16-byte scalar, a fetch of large documents became one
        # statement past Aurora DSQL's 10 MiB message limit, redelivered forever.
        "json sized by its text": ("dev/dsqlmigrator/connect/DsqlSinkTask.class", b"utf8Bytes"),
        # ...and a timestamp is bound as a java.time value, which pgjdbc renders in the
        # proleptic calendar (utcDateTime).
        "timestamp as java.time": (
            "dev/dsqlmigrator/connect/DebeziumTypeConverter.class", b"utcDateTime"
        ),
        # ...and (G133) a date as a java.time LocalDate in the proleptic calendar, one before
        # pgjdbc's 4713-01-01 BC floor (which it sends as -infinity) as its own text.
        "date as java.time": (
            "dev/dsqlmigrator/connect/DebeziumTypeConverter.class", b"prolepticDate"
        ),
    }
    # What the ZIP must no longer carry: pgjdbc renders a java.sql.Timestamp in the Julian
    # calendar before 1582-10-15, so binding one wrote every earlier timestamp days off; and a
    # java.sql.Date through the same calendar, whose switch skips 1582-10-05 .. 1582-10-14, so
    # a date in those ten days was written ten days later (G133).
    class_absent = {
        "pre-1582 timestamp shift": (
            "dev/dsqlmigrator/connect/DebeziumTypeConverter.class", b"java/sql/Timestamp"
        ),
        "skipped-days date shift": (
            "dev/dsqlmigrator/connect/DebeziumTypeConverter.class", b"java/sql/Date"
        ),
    }
    missing = (
        [name for name, marker in markers.items() if marker not in blob]
        + [
            name
            for name, (entry, marker) in class_markers.items()
            if marker not in classes.get(entry, b"")
        ]
        + [
            name
            for name, (entry, marker) in class_absent.items()
            if entry not in classes or marker in classes[entry]
        ]
    )
    assert not missing, (
        "connectors/plugins/dsql-sink-plugin.zip is STALE for: "
        + ", ".join(missing)
        + ". Run `mvn -B -ntp package` in connectors/dsql-sink, re-zip "
        "connectors/plugins/dsql-sink-plugin.zip, and bump PLUGIN_VERSION in "
        "core/s3_provision.py (a live cdc-stack needs Delete + Deploy infra to pick it up)."
    )


def test_the_manual_never_names_the_removed_oversized_lob_card() -> None:
    """P3-C22 removed both exclusion cards; exclusions are edited only in the 'Exclude
    tables & columns' popup. The manual (en/ko/ja) must not send users to the old card."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent / "docs" / "manual"
    stale = re.compile(r"oversized-LOB card|特大 LOB カード|초대형 LOB 카드")
    hits = [f"{p.relative_to(root)}:{n}" for p in sorted(root.rglob("*.md"))
            for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
            if stale.search(line)]
    assert hits == []
    for lang in ("en", "ko", "ja"):
        assert "Exclude tables & columns" in (root / lang / "11-customer-faq.md").read_text(
            encoding="utf-8"), lang
    # Never the unqualified "an excluded column is NULL": CDC only keeps the values of rows
    # already on Aurora DSQL before CDC started.
    overclaims = re.compile(r"An excluded column is NULL on Aurora DSQL|제외한 컬럼은 Aurora "
                            r"DSQL에서 NULL입니다|除外した列は Aurora DSQL では NULL になります")
    assert [str(p.relative_to(root)) for p in sorted(root.rglob("*.md"))
            if overclaims.search(p.read_text(encoding="utf-8"))] == []


def test_the_manual_lists_every_mysql_oversized_lob_type() -> None:
    """G13: the manual (en/ko/ja) must list every MySQL type the tool flags as an
    oversized-LOB candidate -- MySQL JSON included -- and no longer tell the reader that
    MySQL JSON is "not highlighted"."""
    from pathlib import Path

    from dsql_migrator.core.assessor import _OVERSIZED_LOB_BASES

    root = Path(__file__).resolve().parent.parent / "docs" / "manual"
    not_highlighted = {"en": "does not highlight", "ko": "강조 표시하지 않는", "ja": "強調表示しない"}
    for lang, marker in not_highlighted.items():
        ch13 = (root / lang / "13-excluding-tables-and-columns.md").read_text(encoding="utf-8")
        (bullet,) = [ln for ln in ch13.splitlines() if ln.startswith("- **MySQL:**")]
        for base in sorted(_OVERSIZED_LOB_BASES):
            assert f"`{base}`" in bullet, (lang, base)
        paragraph = ch13[ch13.index(marker) :].split("\n\n", 1)[0]
        assert "JSON" not in paragraph, (lang, paragraph)
        ch06 = (root / lang / "06-limitations.md").read_text(encoding="utf-8")
        assert "`LONGTEXT`/`JSON`" in ch06, lang
        ch02 = (root / lang / "02-evaluation-and-schema-conversion.md").read_text(
            encoding="utf-8"
        )
        (row,) = [ln for ln in ch02.splitlines() if ln.startswith("| `JSON` |")]
        assert "OVERSIZED_LOB" in row and "1 MiB" in row, (lang, row)


def test_the_manual_triage_files_oversized_lob_under_manual_not_unsupported() -> None:
    """G13 review: the assessor grades OVERSIZED_LOB MANUAL and tells the reader to check
    sizes first, but ch02 "How to use the report" step 1 (UNSUPPORTED: "these block a
    clean migration") said to "exclude or split oversized LOB columns" -- with MySQL JSON
    now flagged, that told a reader with ordinary small JSON columns to exclude them (they
    would migrate as NULL). ch10 §10.2 listed "oversized LOBs" as UNSUPPORTED too. The
    three languages must file it under MANUAL, with the size check first."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent / "docs" / "manual"
    headings = {
        "en": "### How to use the report",
        "ko": "### 리포트 활용법",
        "ja": "### レポートの活用方法",
    }
    for lang, heading in headings.items():
        ch02 = (root / lang / "02-evaluation-and-schema-conversion.md").read_text(
            encoding="utf-8"
        )
        section = ch02[ch02.index(heading) :].split("\n> ", 1)[0]
        steps = {
            int(m.group(1)): " ".join(m.group(2).split())
            for m in re.finditer(r"^(\d)\. (.*?)(?=^\d\. |\Z)", section, re.S | re.M)
        }
        assert "UNSUPPORTED" in steps[1] and "MANUAL" in steps[2], (lang, steps)
        assert "LOB" not in steps[1], (lang, steps[1])
        assert "LOB" in steps[2] and "OVERSIZED_LOB" in steps[2], (lang, steps[2])
        assert "Check sizes" in steps[2], (lang, steps[2])
        ch10 = (root / lang / "10-conclusion.md").read_text(encoding="utf-8")
        m = re.search(r"\*\*UNSUPPORTED\*\*\s*(?:item|項目|항목)\s*[（(]([^)）]*)[)）]", ch10)
        assert m, lang
        assert "LOB" not in m.group(1), (lang, m.group(1))
        m = re.search(r"\*\*MANUAL\*\*\s*(?:item|項目|항목)\s*[（(]([^)）]*)[)）]", ch10)
        assert m and "LOB/JSON" in m.group(1).replace("LOB·JSON", "LOB/JSON"), lang


def test_the_manual_matches_the_popup_on_size_results_and_typeless_quarantines() -> None:
    """G13 review, ch13 en/ko/ja. (1) The prerequisite run's SOURCE_VALUE_SIZE answers DO
    show in the popup (DataMigrationState.set_prereq_report fills its size cache), but the
    manual said they were "not shown here" and that only Check sizes turns the row line
    warning. (2) A quarantine reason that names no type (DSQL's 54000 "maximum column size
    exceeded") also opens the filtered LOB section, but the manual said that happens only
    when "the database reported only a type"."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent / "docs" / "manual"
    expect = {
        "en": (
            ("whose result is not shown here", "reported only a type"),
            (
                "a column they measured shows that result here too",
                "once a size check (**Check sizes** or the prerequisite checks)",
                "it names no type, as with `maximum column size exceeded`",
            ),
        ),
        "ko": (
            ("그 결과는 여기에 표시되지 않습니다", "타입만 보고"),
            (
                "사전 점검이 측정한 컬럼은 그 결과가 여기에도 표시됩니다",
                "크기 점검(**Check sizes** 또는 사전 점검)이",
                "`maximum column size exceeded`처럼 타입을 밝히지 않거나",
            ),
        ),
        "ja": (
            ("その結果はここには表示されません", "型だけを報告"),
            (
                "前提条件チェックが測定した列は、その結果がここにも表示されます",
                "サイズチェック(**Check sizes** または前提条件チェック)が",
                "`maximum column size exceeded` のように型を示さない",
            ),
        ),
    }
    for lang, (gone, present) in expect.items():
        text = (root / lang / "13-excluding-tables-and-columns.md").read_text(encoding="utf-8")
        # Undo the hard wrap: a line break is a space in en/ko, nothing in ja.
        flat = re.sub(r"[ \t]*\n[ \t]*", "" if lang == "ja" else " ", text)
        for phrase in gone:
            assert phrase not in flat, (lang, phrase)
        for phrase in present:
            assert phrase in flat, (lang, phrase)


# ---------------------------------------------------------------------------
# G15 review: re-keyed tables are keyed on the SOURCE primary key by CDC infrastructure
# deployed by this version. The manual must say what this version does, and what CDC
# infrastructure deployed by an earlier version still does, in every language.
# ---------------------------------------------------------------------------

_G15_SECTION_4_6 = {
    "en": "### Re-keyed PostgreSQL tables",
    "ko": "### 재키잉된 PostgreSQL 테이블",
    "ja": "### リキーされた PostgreSQL テーブル",
}


def _flat_manual_slice(lang: str, page: str, head: str, end: str) -> str:
    """``head`` up to the next ``end`` on one manual page, with the hard wrap undone (a line
    break is a space in en/ko, nothing in ja)."""
    import pathlib
    import re

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    text = (manual / lang / page).read_text(encoding="utf-8")
    start = text.index(head)
    piece = text[start : text.index(end, start + len(head))]
    return re.sub(r"[ \t]*\n[ \t]*", "" if lang == "ja" else " ", piece)


def test_manual_4_6_does_not_close_a_finding_for_keying_on_the_source_primary_key() -> None:
    # The amber bullet said a finding closes when CDC is "redeployed with the source primary
    # key" or the source primary key "covers the record key". This version keys a re-keyed
    # table's change records on the source primary key -- which trivially "covers" it -- yet
    # the finding stays open (deployed_rekey_map unions SinkKeyColumns): only Keep source PK,
    # or a source primary key that covers the COMPOSITE key, closes it.
    stale = {"en": "covers the record key", "ko": "레코드 키를 포함", "ja": "レコードキーを含む"}
    new = {"en": "covers the composite key", "ko": "복합 키를 포함", "ja": "複合キーを含む"}
    for lang, head in _G15_SECTION_4_6.items():
        section = _flat_manual_slice(lang, "04-cdc-and-dsql-constraints.md", head, "\n### ")
        assert stale[lang] not in section, lang
        assert new[lang] in section, lang
        assert "**Keep source PK**" in section, lang


def test_manual_4_6_places_the_source_key_index_build_where_it_happens() -> None:
    # The intro said Composite key's unique index on the source key is "built in the
    # background after the load". Schema Conversion's Apply creates it WITH the table, so in
    # the default flow it is valid before CDC starts; only Drop & reload builds it after the
    # load. And "the monitor re-reads it" followed the UPDATE sentence, so "it" read as the
    # old row: the monitor re-reads REPLICA IDENTITY. (Chapter 4's answer-first §4.6 no longer
    # explains when the index is built -- the notice's fix is the same either way -- so only the
    # stale claims and the explicit "re-reads REPLICA IDENTITY" are pinned.)
    stale = {
        "en": ("built in the background after the load)", "So the monitor re-reads it ("),
        "ko": ("로드 후 백그라운드에서 빌드됩니다)", "그래서 모니터가 이를 다시 읽고"),
        "ja": ("ロード後にバックグラウンドで構築されます)", "そのためモニターがこれを読み直し"),
    }
    new = {
        "en": ("re-reads `REPLICA IDENTITY`",),
        "ko": ("`REPLICA IDENTITY`를 다시 읽고",),
        "ja": ("`REPLICA IDENTITY` を読み直し",),
    }
    for lang, head in _G15_SECTION_4_6.items():
        intro = _flat_manual_slice(lang, "04-cdc-and-dsql-constraints.md", head, "\n|")
        for phrase in stale[lang]:
            assert phrase not in intro, (lang, phrase)
        for phrase in new[lang]:
            assert phrase in intro, (lang, phrase)


def test_manual_06_says_what_redeploying_older_cdc_infrastructure_costs() -> None:
    # The older-infrastructure bullet advised Delete CDC infrastructure and a new deploy with a
    # PostgreSQL cost only, then said "Both engines", so MySQL read as free. The delete also
    # deletes the stream position on MySQL: the bullet must say what the next Start costs on
    # each engine and point at the chapter 13 bullets that explain it.
    heads = {
        "en": "**Re-keyed tables on CDC infrastructure deployed by an earlier version**",
        "ko": "**이전 버전이 배포한 CDC 인프라의 재키잉 테이블**",
        "ja": "**以前のバージョンがデプロイした CDC インフラ上のリキーされたテーブル**",
    }
    stale = {
        "en": "(on PostgreSQL that removes the replication slot, so the tables are read again)",
        "ko": "(PostgreSQL에서는 이 작업이 복제 슬롯을 제거하므로 테이블을 다시 읽습니다)",
        "ja": "(PostgreSQL ではこれによりレプリケーションスロットが削除されるため、テーブルが"
        "再度読み込まれます)",
    }
    must = {
        "en": ("deletes the stream position", "Full Load watermark", "CDC only",
               '**"Re-snapshot every table instead"**',
               "(13-excluding-tables-and-columns.md#why-the-set-locks)"),
        "ko": ("스트림 위치도 삭제", "Full Load 워터마크", "CDC only",
               '**"Re-snapshot every table instead"**',
               "(13-excluding-tables-and-columns.md#집합이-잠기는-이유)"),
        "ja": ("ストリームの位置も削除", "Full Load のウォーターマーク", "CDC only",
               "**「Re-snapshot every table instead」**",
               "(13-excluding-tables-and-columns.md#集合がロックされる理由)"),
    }
    for lang, head in heads.items():
        bullet = _flat_manual_slice(lang, "06-limitations.md", head, "\n- ")
        assert stale[lang] not in bullet, lang
        for phrase in must[lang]:
            assert phrase in bullet, (lang, phrase)


def test_manual_13_gives_a_target_key_reason_true_for_both_cdc_routings() -> None:
    # "CDC builds the record key from it" is false for a re-keyed table on infrastructure
    # deployed by this version (its record key is the source primary key). The column is still
    # needed: CDC writes and deletes each row by the composite key.
    stale = {
        "en": "CDC builds the record key from it",
        "ko": "CDC는 그것으로 레코드 키를 만듭니다",
        "ja": "CDC はそこからレコードキーを構築します",
    }
    new = {
        "en": "CDC needs it to write and delete each row by that key",
        "ko": "CDC는 각 행을 그 키로 쓰고 삭제하므로",
        "ja": "CDC は各行をそのキーで書き込み・削除するため",
    }
    for lang in stale:
        row = _flat_manual_slice(
            lang, "13-excluding-tables-and-columns.md", "| target key |", "\n"
        )
        assert stale[lang] not in row, lang
        assert new[lang] in row, lang


def test_manual_says_how_a_gtid_on_source_starts_cdc() -> None:
    # G19: on gtid_mode=ON Debezium starts after the start point's GTID set; the manual must
    # say so in every language, name the refusals and their remedies, keep gtid_mode until
    # cut over, and tell pipelines run by 0.1.565 or earlier what to do. The old claim that
    # GTID made the resume robust while the template emptied the set is gone.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    heads = {
        "en": ("**MySQL with `gtid_mode=ON`: CDC starts after the start point's GTID set.**",
               "**MySQL: decide `gtid_mode` before the Full Load and keep it until cut over.**",
               "**MySQL with `gtid_mode=ON`: pipelines started by version 0.1.565 or earlier.**"),
        "ko": ("**MySQL에서 `gtid_mode=ON`이면: CDC는 시작 지점의 GTID 셋 다음부터 시작.**",
               "**MySQL: `gtid_mode`는 Full Load 전에 정하고 cut over까지 유지.**",
               "**MySQL에서 `gtid_mode=ON`이면: 버전 0.1.565 이하가 시작한 파이프라인.**"),
        "ja": ("**MySQL で `gtid_mode=ON` の場合: CDC は開始位置の GTID セットの次から開始します。**",
               "**MySQL: `gtid_mode` は Full Load の前に決め、カットオーバーまで維持してください。**",
               "**MySQL で `gtid_mode=ON` の場合: バージョン 0.1.565 以前が開始したパイプライン。**"),
    }
    must = ("**Fetch current position**", "`ON_PERMISSIVE`", "CHECKSUM", "Stop CDC",
            "Start CDC", "**Delete CDC", "0.1.565")
    for lang, three in heads.items():
        bullets = "".join(
            _flat_manual_slice(lang, "06-limitations.md", head, "\n- ") for head in three
        )
        for phrase in must:
            assert phrase in bullets, (lang, phrase)
        for page in ("01-setup.md", "03-full-load.md", "04-cdc-and-dsql-constraints.md"):
            assert "gtid_mode" in (manual / lang / page).read_text(encoding="utf-8"), (
                lang, page
            )
    stale = {
        "en": "CDC\n  resume survives a source failover or replica promotion",
        "ko": "복제본 승격 후에도 CDC 재개가 견고합니다",
        "ja": "レプリカ昇格の後でも CDC の再開が堅牢になります",
    }
    for lang, phrase in stale.items():
        assert phrase not in (manual / lang / "01-setup.md").read_text(encoding="utf-8"), lang


def test_manual_g19_bullets_say_what_the_gtid_start_check_really_covers() -> None:
    # G19 review: the manual must not promise a refusal the check cannot make (it judges the
    # recorded mode and the stack's recorded start offset, not how the stack streamed since),
    # must give the gapless ON_PERMISSIVE resume for an earlier version's Manual-route
    # pipeline, must pair every Manual remedy with the correction it needs, and must say
    # that a Manual point saved by 0.1.565 or earlier comes back without its GTID set.
    heads = {
        "en": ("**MySQL with `gtid_mode=ON`: CDC starts after the start point's GTID set.**",
               "**MySQL: decide `gtid_mode` before the Full Load and keep it until cut over.**",
               "**MySQL with `gtid_mode=ON`: pipelines started by version 0.1.565 or earlier.**"),
        "ko": ("**MySQL에서 `gtid_mode=ON`이면: CDC는 시작 지점의 GTID 셋 다음부터 시작.**",
               "**MySQL: `gtid_mode`는 Full Load 전에 정하고 cut over까지 유지.**",
               "**MySQL에서 `gtid_mode=ON`이면: 버전 0.1.565 이하가 시작한 파이프라인.**"),
        "ja": ("**MySQL で `gtid_mode=ON` の場合: CDC は開始位置の GTID セットの次から開始します。**",
               "**MySQL: `gtid_mode` は Full Load の前に決め、カットオーバーまで維持してください。**",
               "**MySQL で `gtid_mode=ON` の場合: バージョン 0.1.565 以前が開始したパイプライン。**"),
    }
    gone = {
        "en": ("refuses a restart after Stop CDC of a stack that streamed by `file:position`",
               "or a stack that streamed by `file:position`: see the next bullet",
               "(or set a new Manual point)",
               "GTID position still needs*, or have it refused",
               # G20 (DELIBERATE UPDATE): a seedless restart no longer leaves the stack
               # without a record -- the gtid_mode record survives it.
               "which leaves the stack no record",
               # G20 review: after the redeploy, the held point may do (see present).
               "deploy it again and give CDC a new start point as above"),
        "ko": ("`file:position`으로 스트리밍한 스택의 Stop CDC 후 재시작도 거부합니다",
               "또는 `file:position`으로 스트리밍한 스택: 다음", "(또는 새 Manual 지점)",
               "스택에 기록이 남지 않음",
               "후 다시 배포하고 위와 같이 CDC에 새 시작 지점을 주세요"),
        "ja": ("`file:position`でストリーミングしたスタックのStop CDC後の再起動も拒否",
               "または`file:position`でストリーミングしたスタック: 次の項目",
               "(または新しいManualの位置を設定)",
               "スタックに記録が残らない",
               "を実行して再デプロイし、上記のとおりCDCに新しい開始位置を与えてください"),
    }
    # (the explanation of how an old stack is judged "only by what is recorded, not how the stack
    # streamed since" is no longer spelled out: 6.2 says to keep such a source not ON or take a new start point)
    # (2026-10-26: the part-way-failure paragraph -- "rows the sink re-applied keep their older values" -- is gone from 6.2)
    present = {
        "en": ("even when the session no longer holds the start point",
               "set the source's `gtid_mode` to `ON_PERMISSIVE` until cut over",
               "with no gap",
               "changes to the captured tables between the two points are then not replicated",
               "then Start CDC again",
               "comes back without its GTID set",
               "`<server UUID>:<interval>`",
               # G20: the gtid_mode record, and which stacks still lack it.
               "records on the CDC stack the source's `gtid_mode`",
               "version 0.1.566 or earlier holds no record",
               # G20 review: a new stack starts exactly at the held point when it can.
               "the one you hold if `gtid_mode` was already `ON` when it was recorded and "
               "has stayed `ON` since"),
        "ko": ("세션에 더 이상 시작 지점이 없어도",
               "`gtid_mode`를 `ON_PERMISSIVE`로 바꾸고",
               "빈틈 없이",
               "두 지점 사이의 캡처 대상 테이블 변경은 복제되지 않으므로",
               "Start CDC를 다시 실행하세요",
               "GTID 셋 없이 복원됩니다",
               "`<server UUID>:<interval>`",
               "소스의 `gtid_mode`를 CDC 스택에 기록",
               "버전 0.1.566 이하가 마지막으로 시작한 스택에는 기록이 없",
               "가지고 있는 시작 지점이 기록될 때 이미 `gtid_mode`가 `ON`이었고 그 뒤로 계속 "
               "`ON`이었다면 그 지점에서"),
        "ja": ("セッションにもう開始位置がなくても",
               "`gtid_mode`を`ON_PERMISSIVE`に変更し",
               "欠落なく再開します",
               "2つの位置の間のキャプチャ対象テーブルの変更はレプリケートされない",
               "もう一度Start CDCを実行してください",
               "GTIDセットなしで復元されます",
               "`<server UUID>:<interval>`",
               "ソースの `gtid_mode` を CDC スタックに記録",
               "バージョン 0.1.566 以前が最後に開始したスタックには記録がな",
               "保持している開始位置が記録されたときすでに `gtid_mode` が `ON` で、その後も "
               "`ON` のままなら、その位置から"),
    }
    for lang, three in heads.items():
        bullets = "".join(
            _flat_manual_slice(lang, "06-limitations.md", head, "\n- ") for head in three
        )
        if lang == "ja":
            bullets = bullets.replace(" ", "")
        for phrase in gone[lang]:
            probe = phrase.replace(" ", "") if lang == "ja" else phrase
            assert probe not in bullets, (lang, phrase)
        for phrase in present[lang]:
            probe = phrase.replace(" ", "") if lang == "ja" else phrase
            assert probe in bullets, (lang, phrase)
    # Chapter 4's handoff step names the restored-without-a-set Manual point too.
    import pathlib

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    for lang, phrase in {"en": "0.1.565 or earlier comes back", "ko": "0.1.565 이하가 저장한 Manual",
                         "ja": "0.1.565 以前が保存した Manual"}.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        flat = " ".join(text.split())
        assert phrase in flat, lang


def test_occ_retry_is_described_as_a_transaction_not_a_statement() -> None:
    # G20: the Full Load re-runs the whole batch (one multi-row INSERT in its own
    # transaction) and the sink re-runs its whole chunk's transaction (or one row's in the
    # record-by-record fallback) -- nothing retries a single statement. The docs and the
    # comments said "statement-level" in many places. G20 kept one deliberate exception, the
    # plugin's ConfigDef string literal, because it changes only with a plugin rebuild; G21's
    # v52 rebuild changed it too, so it now says what occ.max.retries counts (OCC attempts
    # per sink transaction) and no file is exempt.
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    manual = root / "docs" / "manual"
    sink_src = root / "connectors" / "dsql-sink" / "src" / "main" / "java" / "dev" / (
        "dsqlmigrator") / "connect"

    def flat(path: pathlib.Path) -> str:
        return re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))

    english = [
        root / "README.md", root / "connectors" / "dsql-sink" / "README.md",
        manual / "en" / "07-performance-and-tuning.md",
        manual / "en" / "08-testing-and-verification.md",
        root / "src" / "dsql_migrator" / "core" / "cdc.py", CDC_STACK_TEMPLATE,
        sink_src / "DsqlSinkTask.java", sink_src / "OccRetry.java",
        sink_src / "DsqlSinkConnectorConfig.java",
    ]
    for path in english:
        text = flat(path)
        for stale in ("statement-level", "Statement-level", "statement level",
                      "statement -level"):
            assert stale not in text, (path.name, stale)
    assert "conflicted statement" not in flat(sink_src / "OccRetry.java")
    assert "구문 단위 OCC" not in flat(root / "README.ko.md")
    assert "문장 단위" not in flat(manual / "ko" / "07-performance-and-tuning.md")
    for path in (root / "README.ja.md", manual / "ja" / "07-performance-and-tuning.md",
                 manual / "ja" / "08-testing-and-verification.md"):
        text = flat(path)
        assert "ステートメントレベル" not in text and "ステートメント単位" not in text, path.name
    assert "the tool re-runs the **whole transaction**" in flat(
        manual / "en" / "07-performance-and-tuning.md"
    )
    # G20 review: each path's own attempt budget -- the Full Load's per-batch default
    # (shared with dropped connections) and the sink's occ.max.retries default (the
    # template does not set it) -- not one shared "up to 10".
    from dsql_migrator.config import load_config

    full_load = load_config(env={}).full_load_occ_max_attempts
    sink = int(re.search(
        r"MAX_RETRIES,\s*Type\.INT,\s*(\d+),",
        (sink_src / "DsqlSinkConnectorConfig.java").read_text(encoding="utf-8"),
    ).group(1))
    assert "occ.max.retries" not in CDC_STACK_TEMPLATE.read_text(encoding="utf-8")
    budgets = {
        "en": (f"up to {full_load} attempts per Full Load batch",
               f"up to {sink} per sink transaction", "for up to 10 attempts"),
        "ko": (f"Full Load 배치당 최대 {full_load}회", f"싱크 트랜잭션당 최대 {sink}회",
               "`40001`은 최대 10회까지"),
        "ja": (f"Full Load のバッチごとに最大 {full_load} 回",
               f"シンクのトランザクションごとに最大 {sink} 回", "`40001` は最大 10 回まで"),
    }
    for lang, (fl_phrase, sink_phrase, stale) in budgets.items():
        # Chapter 7 no longer quotes the attempt budgets (retry parameters are not a user knob);
        # it must still not carry the stale shared "up to 10".
        text = (manual / lang / "07-performance-and-tuning.md").read_text(encoding="utf-8")
        text = re.sub(r"[ \t]*\n[ \t]*", "" if lang == "ja" else " ", text)
        assert stale not in text, lang
    # The ConfigDef string literal ships in the plugin jar (v52, G21): the value is the
    # number of attempts, and the retry re-runs a whole sink transaction. G22 (v52 rebuilt in
    # place) names the second SQLSTATE it covers; the literal is split across two Java string
    # literals, so their join is flattened first.
    config_src = re.sub(
        r'"\s*\+\s*"',
        "",
        (sink_src / "DsqlSinkConnectorConfig.java").read_text(encoding="utf-8"),
    )
    assert (
        "Max attempts per sink transaction (a chunk, or one row) on an OCC conflict (40001) or "
        "Aurora DSQL's server unavailable (XX000)." in config_src
    )


def test_manual_says_each_sink_occ_retry_starts_on_a_clean_transaction() -> None:
    # G21 (plugin v52): Aurora DSQL can report a 40001 while a statement runs (OC001, a
    # stale catalog after a schema change elsewhere on the cluster), not only at COMMIT, and
    # the sink now rolls the failed attempt back before each re-run. Chapters 4 and 7 say
    # both, in every language.
    import pathlib
    import re

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    phrases = {
        "en": ("Each re-run starts on a clean transaction", "while a statement runs"),
        "ko": ("깨끗한 트랜잭션에서 시작", "구문을 실행하는 도중에도"),
        "ja": ("クリーンなトランザクションから始まります", "ステートメントの実行中にも"),
    }
    for lang, wanted in phrases.items():
        for page in ("04-cdc-and-dsql-constraints.md",):  # chapter 7 no longer repeats the rollback detail
            text = (manual / lang / page).read_text(encoding="utf-8")
            text = re.sub(r"\s+", "", text) if lang == "ja" else re.sub(r"\s+", " ", text)
            for phrase in wanted:
                probe = phrase.replace(" ", "") if lang == "ja" else phrase
                assert probe in text, (lang, page, phrase)


def test_manual_says_a_stack_deployed_before_plugin_v52_still_needs_a_redeploy() -> None:
    # G21 review: a live cdc-stack keeps its plugin until Delete + Deploy CDC infrastructure
    # (Start CDC carries PluginVersion forward), so the v52 rollback-before-retry is not what
    # an earlier stack does. Chapter 4 says once, in the note under its sink table (4.4), that
    # a stack keeps the plugin it was deployed with and how to get the bundled one (it no
    # longer lists what each older plugin does differently: the CDC step says it is older),
    # chapter 7 names the plugin version, and section 13.10 no longer says, unscoped, that no
    # Delete + Deploy is needed.
    import pathlib
    import re

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    wanted = {
        "en": {
            "04-cdc-and-dsql-constraints.md": (
                "A CDC stack keeps the plugin it was deployed with",
                "until you **Delete CDC infrastructure** and deploy it again.",
            ),
            # (2026-10-28) Section 13.10 (Upgrading from 0.1.561) was deleted: only the check that
            # chapter 13 does not say, unscoped, that no Delete + Deploy is needed remains.
            "13-excluding-tables-and-columns.md": (),
        },
        "ko": {
            "04-cdc-and-dsql-constraints.md": (
                "CDC 스택은 배포될 때의 플러그인을 그대로 씁니다",
                "**Delete CDC infrastructure** 후 다시 배포할 때까지 그대로입니다.",
            ),
            "13-excluding-tables-and-columns.md": (),
        },
        "ja": {
            "04-cdc-and-dsql-constraints.md": (
                "CDC スタックはデプロイ時のプラグインをそのまま使い続けます",
                "**Delete CDC infrastructure** して再デプロイするまでそのままです。",
            ),
            "13-excluding-tables-and-columns.md": (),
        },
    }
    unscoped = {
        "en": "**No Delete + Deploy is needed.**",
        "ko": "**Delete + Deploy는 필요 없습니다.**",
        "ja": "**Delete + Deploy は不要です。**",
    }
    for lang, pages in wanted.items():
        for page, phrases in pages.items():
            text = (manual / lang / page).read_text(encoding="utf-8")
            text = re.sub(r"\s+", "", text) if lang == "ja" else re.sub(r"\s+", " ", text)
            for phrase in phrases:
                probe = phrase.replace(" ", "") if lang == "ja" else phrase
                assert probe in text, (lang, page, phrase)
            if page.startswith("13-"):
                stale = unscoped[lang].replace(" ", "") if lang == "ja" else unscoped[lang]
                assert stale not in text, (lang, page)


def test_plugin_v52_history_records_the_g22_rebuild() -> None:
    # G22 rebuilt the never-deployed v52 ZIP in place instead of bumping the version: the
    # PLUGIN_VERSION history must say so, and name what changed.
    import pathlib

    source = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator" / "core"
    text = (source / "s3_provision.py").read_text(encoding="utf-8")
    history = " ".join(text.replace("#", " ").split())
    assert 'PLUGIN_VERSION = "v54"' in text  # G23 bumped past the released v52, B6 past v53
    assert "rebuilt in place (G22" in history
    assert "XX000" in history
    # G22 also changed paths other than XX000 (a failed error-path rollback drops the
    # session; every transient RetriableException's cause is value-free), so the G21
    # paragraph must not claim they behave as in v51 with only XX000 carved out.
    assert "any other error except XX000 (G22, below)" not in history
    assert "behave as in v51, except as G22 changes them (below)" in history
    # A landed COMMIT's re-run is exact per primary key, not in every case: a secondary
    # UNIQUE index can reject an image the landed chunk already replaced.
    assert "re-runs to the same rows" not in history
    assert "secondary UNIQUE index can reject (23505)" in history


def test_manual_says_server_unavailable_is_retried_never_dead_lettered() -> None:
    # G22: Aurora DSQL documents XX000 "server unavailable" as a temporary condition to be
    # answered by re-running the whole transaction, COMMIT included. The Full Load retries the
    # batch and never quarantines a row for it; the sink (plugin v52) retries the transaction
    # and never dead-letters a row for it. Chapters 3, 4 and 7 say so in every language.
    import pathlib
    import re

    manual = pathlib.Path(__file__).resolve().parents[1] / "docs" / "manual"
    wanted = {
        "en": {
            "04-cdc-and-dsql-constraints.md": (
                "such a row is **never dead-lettered**",
                "If `XX000` keeps recurring, contact AWS Support.",
            ),
            "07-performance-and-tuning.md": (
                "So is Aurora DSQL's `XX000`",
                "no row is quarantined for it",
            ),
            "03-full-load.md": ("no row is quarantined for it",),
        },
        "ko": {
            "04-cdc-and-dsql-constraints.md": ("그런 행은 **dead-letter 처리되지 않음**",),
            "07-performance-and-tuning.md": (
                'Aurora DSQL의 `XX000`("server unavailable")도 재시도합니다',
            ),
            "03-full-load.md": ("그 때문에 quarantine되는 행은 없습니다",),
        },
        "ja": {
            "04-cdc-and-dsql-constraints.md": ("そのような行は **デッドレター化されません**",),
            "07-performance-and-tuning.md": (
                'Aurora DSQL の `XX000` ("server unavailable") もリトライします',
            ),
            "03-full-load.md": ("そのために隔離される行はありません",),
        },
    }
    for lang, pages in wanted.items():
        for page, phrases in pages.items():
            text = (manual / lang / page).read_text(encoding="utf-8")
            text = re.sub(r"\s+", "", text) if lang == "ja" else re.sub(r"\s+", " ", text)
            for phrase in phrases:
                probe = re.sub(r"\s+", "", phrase) if lang == "ja" else phrase
                assert probe in text, (lang, page, phrase)


def test_manual_says_a_landed_commits_rerun_can_meet_a_secondary_unique_index() -> None:
    # G22 review: an XX000 at COMMIT may have landed, and the sink re-runs the whole chunk on
    # the rows it already wrote. That is idempotent per primary key, but a secondary UNIQUE
    # index can reject (23505) an image the landed chunk already replaced, which is then
    # dead-lettered -- on a re-keyed table or a plain one, as on any redelivery of a committed
    # batch. The manual (ch.4 in every language) and the sink README said "the re-run leaves
    # the same rows", unqualified. Chapter 4 (answer-first) no longer carries the landed-commit
    # sentence at all (a user acts on the dead letter it can cause like on any other, and the
    # README keeps the mechanism), so what is pinned there is only that the unqualified claim
    # does not come back.
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    manual = root / "docs" / "manual"
    wanted = {
        "en": ("Aurora DSQL documents `XX000` as temporary",),
        "ko": ("Aurora DSQL은 `XX000`을 일시적인 상태로 문서화하므로",),
        "ja": ("Aurora DSQL は `XX000` を一時的な状態として文書化しているため",),
    }
    stale = {
        "en": "so the re-run leaves the same rows",
        "ko": "PK 기준 upsert·delete이므로 다시 실행해도 같은 행이 남음",
        "ja": "なので、再実行しても同じ行が残ります",
    }
    for lang, phrases in wanted.items():
        text = (manual / lang / "04-cdc-and-dsql-constraints.md").read_text(encoding="utf-8")
        row = next(line for line in text.splitlines() if "XX000" in line and line.startswith("| **"))
        for phrase in phrases:
            assert phrase in row, (lang, phrase)
        assert stale[lang] not in re.sub(r"\s+", " ", text), lang
    readme = re.sub(r"\s+", " ", (root / "connectors" / "dsql-sink" / "README.md").read_text(encoding="utf-8"))
    assert "the PK-keyed upsert/delete re-run is idempotent per key, though a secondary `UNIQUE` index" in readme


def test_manual_3_1_stops_the_run_only_for_the_watermarks_publication_write() -> None:
    # G20 review: §3.1 said an unusable REPLICA IDENTITY stops the run before any of its
    # source writes -- adding a table selected after the Full Load included. That table is
    # refused on its own (not loaded, its row says why) while the other additions still join
    # the publication and load; only the watermark's provisioning stops the run.
    stale = {
        "en": "Before the first of these writes it checks",
        "ko": "이 쓰기 가운데 첫 번째보다 먼저",
        "ja": "これらの最初の書き込みの前に",
    }
    new = {
        "en": ("Before it creates the publication (or adds tables to it) at the watermark",
               "A table selected after the Full Load gets the same check before it is added: "
               "without one it is not loaded, and its row says why"),
        "ko": ("워터마크 시점에 publication을 만들거나 거기에 테이블을 추가하기 전에",
               "Full Load 이후 선택한 테이블도 추가하기 전에 같은 확인을 거치며, 없으면 그 "
               "테이블은 로드되지 않고 그 행에 이유가 표시됩니다"),
        "ja": ("ウォーターマークの地点でpublicationを作成する(またはテーブルを追加する)前に",
               "FullLoadの後に選択したテーブルも、追加する前に同じ確認を受けます。ない場合は"
               "そのテーブルはロードされず、その行に理由が表示されます"),
    }
    for lang in ("en", "ko", "ja"):
        section = _flat_manual_slice(lang, "03-full-load.md", "## 3.1 ", "\n---")
        if lang == "ja":
            section = section.replace(" ", "")
        assert stale[lang] not in section, lang
        for phrase in new[lang]:
            assert phrase in section, (lang, phrase)
