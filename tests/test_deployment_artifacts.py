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
    # Log level / CloudWatch mirroring are NOT deploy-time parameters (kept to a
    # minimal parameter set); they are adjusted at runtime from the app's
    # Diagnostics control. The template ships a safe default log level.
    assert env["DSQL_MIGRATOR_LOG_LEVEL"] == "INFO"
    assert "LogLevel" not in template["Parameters"]
    assert "EnableActivityLogCloudWatch" not in template["Parameters"]
    assert "DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT" not in env


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
    }
    resource = str(stmt["Resource"])
    assert "secret:mysql-dsql-migrator/cdc/*" in resource
    assert resource != "*"


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
    # Resource is an Fn::If: an explicit BedrockModelArns override, or an
    # auto-derived scoped list (profile + foundation-model ARNs). Never "*".
    resource = statement["Resource"]
    assert resource != "*"
    fn_if = resource["Fn::If"]
    assert fn_if[0] == "HasBedrockModelArnsOverride"
    assert fn_if[1] == {"Ref": "BedrockModelArns"}
    derived = fn_if[2]
    # Region-agnostic scope: the inference-profile ARN (this deploy region) + ONE
    # region-agnostic foundation-model ARN (region `*`, exact model id). This works
    # for us./global./apac. profiles without enumerating per-geo member regions.
    assert isinstance(derived, list) and len(derived) == 2  # profile + FM (any region)
    # The only `*` allowed is the REGION field of the foundation-model ARN; the
    # resource must never be a blanket "*" and the model id stays exact.
    assert resource != "*"
    fm_arn = derived[1]["Fn::Sub"][0]
    assert ":bedrock:*::foundation-model/anthropic.${Fm}" in fm_arn
    # No wildcard on the model id / action scope beyond that region field.
    assert "foundation-model/*" not in str(derived)


def test_bedrock_model_id_is_curated_dropdown_with_auto_scope(template: dict) -> None:
    """BedrockModelId is a curated Anthropic picker; the IAM scope is auto-derived
    from it (HasBedrockModelArnsOverride is false on the empty default), so an
    operator enables AI by toggling EnableAiAssist + picking a model -- no ARNs."""
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
    # The seeder Role + Function are gated on DeploySeederFunction, which is now an
    # Fn::Or (bootstrap present OR the code key supplied). The OR means the code key
    # alone keeps the Function deployed when a Stop blanks MskBootstrapServers, so it
    # PERSISTS across a Stop -- keeping the slow VPC-Lambda ENI teardown OFF the Stop
    # path. It also makes HasBootstrapServers imply DeploySeederFunction, so the
    # start-prep invoker (gated on HasBootstrapServers) can always reference it.
    resources = cdc_template["Resources"]
    for name in ("OffsetSeederRole", "OffsetSeederFunction"):
        assert name in resources, name
        assert resources[name].get("Condition") == "DeploySeederFunction", name
    # The invoker (pre-creates topics + seeds the offset) is gated on
    # HasBootstrapServers, so a Stop removes it (fast) and a Start re-creates + reruns it.
    assert resources["CdcStartPrepResource"].get("Condition") == "HasBootstrapServers"

    conds = cdc_template["Conditions"]
    deploy = conds["DeploySeederFunction"]
    assert "Fn::Or" in deploy  # bootstrap present OR key present
    deploy_text = json.dumps(deploy)
    # Key-present alone keeps the Function deployed across a Stop (persist), and the
    # OR includes bootstrap so HasBootstrapServers implies this condition.
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
    # ordering via the OffsetSeed tag's Fn::GetAtt (an implicit ref -- CdcStartPrepResource
    # shares the source's HasBootstrapServers condition, so no Fn::If is needed); the
    # sink via a hard DependsOn. Crucially the sink must NOT DependsOn the source
    # connector -- that was the old serial source-then-sink ordering.
    resources = cdc_template["Resources"]
    src = resources["DebeziumSourceConnector"]["Properties"]
    seed_tag = next(t for t in src["Tags"] if t["Key"] == "OffsetSeed")
    assert seed_tag["Value"] == {"Fn::GetAtt": ["CdcStartPrepResource", "Seeded"]}

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
