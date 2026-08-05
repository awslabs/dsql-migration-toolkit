# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deploy and manage the whole CDC pipeline lifecycle via the cdc-stack.

The UI owns the entire CDC lifecycle through this module, all expressed as
CloudFormation operations on the canonical cdc-stack (``deploy/cdc-stack/
cdc-stack.yaml``), which stays the single source of truth for the gotcha-laden
connector configuration:

* **Deploy infra** (:func:`run_cdc_infra_deploy`) -- ``create_stack`` with the
  customer's BYO-VPC inputs, ``MskBootstrapServers=""`` and
  ``DeploySink="false"`` so MSK / VPC wiring / plugins / IAM are created but no
  connectors yet (the MSK Serverless cluster takes ~15-20 min).
* **Start CDC** (:func:`run_cdc_start`) -- a SINGLE-pass ``update_stack``: fetch
  the cluster bootstrap brokers, then set ``MskBootstrapServers`` +
  ``DeploySink="true"`` so CloudFormation runs ``CdcStartPrepResource`` (which
  PRE-CREATES the per-table topics, and on a gapless handoff seeds the offset) and
  then creates the source AND sink connectors IN PARALLEL -- both DependsOn only
  the pre-created topics, not each other. Pre-creating the topics removes the
  empty-partition-assignment race (formerly gotcha #11, worked around by a serial
  source-then-sink two-pass) at its source and roughly halves the connector wall
  time. We then wait for both connectors to reach RUNNING.
* **Stop CDC** (:func:`run_cdc_stop`) -- ``update_stack`` setting
  ``MskBootstrapServers=""``; both connectors' template conditions
  (``HasBootstrapServers`` / ``DeploySinkConnector``) go false, so
  CloudFormation deletes just the two connectors and keeps MSK / VPC / plugins /
  IAM for a fast restart. MSK Connect has no pause API, so stop == delete.
* **Delete infra** (:func:`run_cdc_delete`) -- ``delete_stack`` to tear the whole
  stack down (also the recovery path for a create that ended in
  ``ROLLBACK_COMPLETE``).

Why CloudFormation and not ``kafkaconnect:CreateConnector`` directly: the
connector config has 20+ keys plus a Secrets Manager config-provider colon
syntax, MSK Serverless topic-creation rules, a self-managed schema-history
topic, and a separate WorkerConfiguration -- all already encoded correctly in
the template. Re-encoding them in Python would create a second source of truth
(drift). CloudFormation keeps one source of truth and gets automatic rollback +
rich stack-event logs for the step-by-step progress UI.

Mutating operations (``create_stack`` / ``update_stack`` / ``delete_stack``) are
each gated behind an explicit UI confirmation. boto3 clients are built from the
shared profile-aware :func:`build_session`, injectable for tests.

WARNING -- partition quota: every connector create/delete consumes MSK
Serverless partition quota that does not come back (topics survive rollback), so
the Start path is idempotent (it skips the update when a connector is already
RUNNING) to avoid burning quota on retries.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.cdc import (
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_PREFIX,
    CDC_WATERMARK_PARAM_KEYS,
    CdcInfraParams,
    CdcStackParams,
    build_watermark_params,
    cdc_expected_connector_names,
)
from dsql_migrator.core.models import ChunkState, MigrationJob, Watermark

# Stack statuses from which an UpdateStack can safely start. Anything in an
# *_IN_PROGRESS / ROLLBACK / FAILED state means a deploy is unsafe right now.
_STABLE_STACK_STATES = frozenset(
    {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "IMPORT_COMPLETE"}
)

# Each lifecycle operation is a list of ordered stages, surfaced as a
# step-by-step progress log. The chunk_id is the first element, the human label
# the second. Stage ids are unique within an operation.

# Deploy CDC infra: ensure the plugin bucket + upload artifacts, then create_stack
# the MSK / VPC wiring / plugins / IAM (no connectors).
CDC_INFRA_STAGES: tuple[tuple[str, str], ...] = (
    ("ensure_bucket", "Ensuring plugin S3 bucket"),
    ("upload_plugins", "Uploading connector plugins"),
    ("check_existing", "Checking for existing stack"),
    ("validate_params", "Validating infrastructure parameters"),
    ("create_stack", "Submitting stack creation"),
    ("stack_create", "Creating infrastructure (~15-20 min)"),
    ("infra_ready", "Infrastructure ready"),
)

# Start CDC: a SINGLE-pass update creates BOTH connectors at once. The stack's
# CdcStartPrepResource pre-creates the per-table topics (so the sink no longer hits
# the empty-partition-assignment race and no longer has to wait for the source to
# auto-create them). Source and sink then deploy IN PARALLEL and are waited on
# together in one "connectors_running" stage -- roughly halving the connector wall
# time vs the old source-then-sink two-pass. The per-connector state (source /sink
# still CREATING vs RUNNING) is shown by the live connector-state chips.
CDC_START_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("validate_params", "Validating configuration"),
    ("fetch_bootstrap", "Fetching MSK bootstrap brokers"),
    ("submit_connectors", "Starting connectors (topics + source + sink)"),
    ("stack_connectors", "Connectors deploying"),
    ("connectors_running", "Waiting for connectors (source + sink)"),
    ("pipeline_running", "Pipeline running"),
)

# Stop CDC: update MskBootstrapServers="" so CloudFormation deletes both connectors.
CDC_STOP_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("submit_stop", "Submitting connector removal"),
    ("stack_stop", "Removing connectors"),
    ("connectors_gone", "CDC stopped"),
)

# Delete CDC infra: delete_stack the whole thing.
CDC_DELETE_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("submit_delete", "Submitting stack deletion"),
    ("stack_delete", "Deleting infrastructure"),
    ("cleanup_secret", "Removing the source-credentials secret"),
    ("deleted", "Infrastructure deleted"),
)

# MSK Connect connector states meaning "fully up" vs "terminally failed".
_CONNECTOR_RUNNING = "RUNNING"
_CONNECTOR_FAILED = "FAILED"

# How many CONSECUTIVE connector-state read failures a RUNNING-wait tolerates before
# it gives up and surfaces the cause. A read failure (throttle, transient network,
# credential expiry) must NOT masquerade as "still creating" and stall the wait
# forever with no error -- the observed failure mode where the source connector was
# RUNNING but the deploy never advanced to the sink pass.
_MAX_STATE_READ_FAILURES = 5

# AWS error codes/markers for a NON-self-healing read failure (credentials expired,
# access lost). These will never recover on retry, so the wait fails immediately
# with an actionable cause instead of burning the whole retry budget.
_TERMINAL_READ_ERROR_MARKERS = (
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
    "SignatureDoesNotMatch",
    "AccessDenied",
    "AccessDeniedException",
    "UnauthorizedOperation",
)


def _is_terminal_read_error(exc: Exception) -> bool:
    """True when ``exc`` is a credential/authorization failure that will not recover
    on retry (so the RUNNING-wait should fail fast with the cause, not keep polling)."""
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
    text = code + " " + str(exc)
    return any(marker in text for marker in _TERMINAL_READ_ERROR_MARKERS)


class CdcDeployError(RuntimeError):
    """A deploy precondition failed (bad stack state, unfilled placeholder, …)."""


@dataclass(frozen=True)
class CdcStackDiscovery:
    """Read-only facts about the target cdc-stack gathered before an update."""

    stack_status: str
    current_parameters: dict[str, str]
    is_stable: bool


@dataclass
class CdcDeployLog:
    """An append-only, timestamped progress log for one deploy run.

    Lives on the session state (transient); the work function appends one line
    per meaningful step / stack event and the UI renders them logging-style.
    """

    lines: list[tuple[datetime, str]] = field(default_factory=list)

    def append(self, when: datetime, message: str) -> None:
        self.lines.append((when, message))


class CdcStackDeployer:
    """CloudFormation-driven cdc-stack updater (reuses existing MSK/VPC/plugins).

    Mirrors :class:`~dsql_migrator.core.msk_connect_controller.MskConnectController`:
    a region + optional profile, an injectable ``session`` for tests, and a tiny
    ``_client`` helper. All methods are read-only except :meth:`submit_update`.
    """

    def __init__(
        self,
        region: str,
        *,
        aws_profile: Optional[str] = None,
        session: Optional[BotoSessionLike] = None,
    ) -> None:
        self._region = region
        self._aws_profile = aws_profile
        self._session = session
        # When the cdc-stack template exceeds CloudFormation's 51,200-byte inline
        # TemplateBody limit, it must be uploaded to S3 and passed as TemplateURL.
        # The caller sets this to the managed plugin bucket NAME (already provisioned
        # for the connector artifacts) so the deployer can stage the template there.
        self.template_s3_bucket: Optional[str] = None

    def _client(self, service_name: str) -> object:
        session = self._session or build_session(self._aws_profile)
        return session.client(service_name, region_name=self._region)

    # CloudFormation's hard limit for an inline TemplateBody (bytes).
    _MAX_INLINE_TEMPLATE_BYTES = 51200

    def _template_kwargs(self, template_body: str) -> dict:
        """Return the CreateStack/UpdateStack template kwarg for ``template_body``.

        Small templates pass inline as ``TemplateBody``. A template over the
        51,200-byte inline limit is uploaded to the managed plugin bucket and passed
        as ``TemplateURL`` (CloudFormation allows up to ~460 KB via S3). Requires
        :attr:`template_s3_bucket` to be set for the oversize path; raises a clear
        :class:`CdcDeployError` otherwise.
        """
        if len(template_body.encode("utf-8")) <= self._MAX_INLINE_TEMPLATE_BYTES:
            return {"TemplateBody": template_body}
        if not self.template_s3_bucket:
            raise CdcDeployError(
                "cdc-stack template exceeds the 51,200-byte inline limit and no S3 "
                "bucket is set to stage it. This is a deploy-wiring bug: the plugin "
                "bucket name must be passed to the deployer for the TemplateURL path."
            )
        key = "cdc-plugins/cdc-stack.yaml"
        s3 = self._client("s3")
        s3.put_object(  # type: ignore[attr-defined]
            Bucket=self.template_s3_bucket,
            Key=key,
            Body=template_body.encode("utf-8"),
        )
        # Use the REGION-SPECIFIC virtual-hosted S3 endpoint, not the global
        # ``s3.amazonaws.com`` (which targets us-east-1): a bucket outside us-east-1
        # (e.g. ap-northeast-2) accessed via the global endpoint returns S3
        # PermanentRedirect, and CloudFormation rejects/relocates the TemplateURL.
        # The managed plugin bucket is created in this deployer's region, so the
        # endpoint must match it.
        url = (
            f"https://{self.template_s3_bucket}.s3.{self._region}.amazonaws.com/{key}"
        )
        return {"TemplateURL": url}

    def discover_stack(self, stack_name: str) -> CdcStackDiscovery:
        """Read the stack's status + current parameters, validating preconditions.

        Raises :class:`CdcDeployError` when the stack is missing, in an unstable
        (in-progress / rollback) state, or still carries an unfilled
        ``<FILL_ME:`` placeholder in its current parameters -- so an update is
        never submitted against a stack that cannot accept one.
        """
        try:
            client = self._client("cloudformation")
            response = client.describe_stacks(StackName=stack_name)
        except Exception as exc:  # noqa: BLE001 - surface as a typed precondition error
            raise CdcDeployError(
                f"Could not find or read the cdc-stack '{stack_name}'. Deploy the "
                f"cdc-stack once first (it creates MSK, the VPC, and the plugins)."
            ) from exc
        stacks = response.get("Stacks", []) or []
        if not stacks:
            raise CdcDeployError(f"cdc-stack '{stack_name}' not found.")
        stack = stacks[0]
        status = str(stack.get("StackStatus", ""))
        params = {
            p.get("ParameterKey", ""): p.get("ParameterValue", "")
            for p in stack.get("Parameters", []) or []
        }
        unfilled = [k for k, v in params.items() if str(v).startswith(CDC_PLACEHOLDER_PREFIX)]
        if unfilled:
            raise CdcDeployError(
                "The cdc-stack still has unfilled placeholder parameters "
                f"({', '.join(sorted(unfilled))}). Fill them and redeploy the "
                "cdc-stack before starting CDC from here."
            )
        # Auto-recover a wedged rollback. A connector UpdateConnector that fails
        # leaves the connector not-RUNNING, and CloudFormation's automatic rollback
        # then also fails on that same resource ("only valid for RUNNING"), parking
        # the stack in UPDATE_ROLLBACK_FAILED -- from which NO further update can be
        # submitted. Recover by continuing the rollback while skipping the stuck
        # resource(s): the connector itself is left as-is (typically still RUNNING
        # from before the failed update), and the stack returns to
        # UPDATE_ROLLBACK_COMPLETE so the next Start/Retry can proceed. This turns a
        # dead-end that previously required manual CLI intervention into a
        # self-healing precondition.
        if status == "UPDATE_ROLLBACK_FAILED":
            status = self._recover_rollback_failed(stack_name)
            params = {
                p.get("ParameterKey", ""): p.get("ParameterValue", "")
                for p in (
                    client.describe_stacks(StackName=stack_name)
                    .get("Stacks", [{}])[0]
                    .get("Parameters", [])
                    or []
                )
            }
        is_stable = status in _STABLE_STACK_STATES
        if not is_stable:
            raise CdcDeployError(
                f"cdc-stack '{stack_name}' is '{status}', not a stable state. "
                "Wait for the current operation to finish, then try again."
            )
        return CdcStackDiscovery(
            stack_status=status, current_parameters=params, is_stable=is_stable
        )

    def _recover_rollback_failed(self, stack_name: str) -> str:
        """Continue a failed rollback, skipping the resource(s) that are stuck.

        Finds the resources currently in ``*_FAILED`` and calls
        ``continue_update_rollback`` with them in ``ResourcesToSkip`` (CloudFormation
        leaves those resources untouched and completes the rollback). Waits for the
        stack to settle and returns the resulting status. Best-effort: on any error
        it returns the original ``UPDATE_ROLLBACK_FAILED`` so the caller's stable
        check raises the normal, actionable message.
        """
        client = self._client("cloudformation")
        try:
            resources = client.describe_stack_resources(StackName=stack_name).get(
                "StackResources", []
            ) or []
            stuck = sorted(
                {
                    r.get("LogicalResourceId", "")
                    for r in resources
                    if str(r.get("ResourceStatus", "")).endswith("FAILED")
                    and r.get("LogicalResourceId")
                }
            )
            kwargs = {"StackName": stack_name}
            if stuck:
                kwargs["ResourcesToSkip"] = stuck
            client.continue_update_rollback(**kwargs)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - best-effort; fall through to status read
            return "UPDATE_ROLLBACK_FAILED"
        # Poll briefly for the cleanup to settle (it is fast -- it only skips).
        import time as _time

        for _ in range(60):
            current = self.stack_status(stack_name)
            if current is None or not current.endswith("IN_PROGRESS"):
                return current or "UPDATE_ROLLBACK_FAILED"
            _time.sleep(5)
        return self.stack_status(stack_name) or "UPDATE_ROLLBACK_FAILED"

    def recover_delete_failed(self, stack_name: str) -> str:
        """Recover a DELETE_FAILED cdc-stack blocked by leftover Lambda ENIs.

        The in-VPC offset-seeder Lambda leaves AWS-managed (hyperplane) ENIs behind
        when it is deleted, and AWS reclaims them asynchronously (minutes to tens of
        minutes). While they linger, deleting the connector subnets / security group
        fails, so the whole stack lands in DELETE_FAILED. Those ENIs are
        ``status=available`` (detached) and safe to delete directly, which unblocks
        the subnet/SG deletion; a re-issued ``delete_stack`` then completes.

        Strategy: find the failed subnet/SG resources, delete any *available* ENIs
        in those subnets (or in the connector SG), then ``delete_stack`` again while
        skipping resources still stuck. Returns the resulting status ("" when the
        stack is gone = success). Best-effort: any error returns "DELETE_FAILED" so
        the caller surfaces the normal failure.
        """
        cfn = self._client("cloudformation")
        try:
            resources = cfn.describe_stack_resources(StackName=stack_name).get(
                "StackResources", []
            ) or []
        except Exception:  # noqa: BLE001
            return "DELETE_FAILED"

        failed = [
            r for r in resources
            if str(r.get("ResourceStatus", "")).endswith("FAILED")
        ]
        subnet_ids = [
            r.get("PhysicalResourceId")
            for r in failed
            if r.get("ResourceType") == "AWS::EC2::Subnet" and r.get("PhysicalResourceId")
        ]
        sg_ids = [
            r.get("PhysicalResourceId")
            for r in failed
            if r.get("ResourceType") == "AWS::EC2::SecurityGroup"
            and r.get("PhysicalResourceId")
        ]

        # Delete the leftover, detached (available) ENIs pinning those subnets / SG.
        try:
            ec2 = self._client("ec2")
            filters = []
            if subnet_ids:
                filters.append({"Name": "subnet-id", "Values": subnet_ids})
            enis: list[dict] = []
            if filters:
                enis += (
                    ec2.describe_network_interfaces(Filters=filters).get(
                        "NetworkInterfaces", []
                    )
                    or []
                )
            if sg_ids:
                enis += (
                    ec2.describe_network_interfaces(
                        Filters=[{"Name": "group-id", "Values": sg_ids}]
                    ).get("NetworkInterfaces", [])
                    or []
                )
            seen: set[str] = set()
            for eni in enis:
                eni_id = eni.get("NetworkInterfaceId")
                # Only detached ENIs can be deleted; an in-use one is still being
                # reclaimed by AWS and will free up shortly (a later retry catches it).
                if (
                    eni_id
                    and eni_id not in seen
                    and eni.get("Status") == "available"
                ):
                    seen.add(eni_id)
                    try:
                        ec2.delete_network_interface(NetworkInterfaceId=eni_id)
                    except Exception:  # noqa: BLE001 - best effort per ENI
                        pass
        except Exception:  # noqa: BLE001 - ENI cleanup is best effort
            pass

        # Re-issue the delete, skipping anything still stuck this pass.
        stuck = sorted(
            {r.get("LogicalResourceId", "") for r in failed if r.get("LogicalResourceId")}
        )
        try:
            kwargs = {"StackName": stack_name}
            if stuck:
                kwargs["RetainResources"] = stuck
            cfn.delete_stack(**kwargs)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return "DELETE_FAILED"
        import time as _time

        for _ in range(60):
            current = self.stack_status(stack_name)
            if current is None:  # stack gone = deleted
                return ""
            if not current.endswith("IN_PROGRESS"):
                return current
            _time.sleep(5)
        return self.stack_status(stack_name) or "DELETE_FAILED"

    def submit_update(
        self,
        stack_name: str,
        overrides: Sequence[tuple[str, str]],
        *,
        template_body: Optional[str] = None,
    ) -> bool:
        """UpdateStack with tool-known overrides; all other params unchanged.

        ``overrides`` are the ``(ParameterKey, ParameterValue)`` pairs the tool
        sets (table list, topics, DLQ, DSQL endpoint, DeploySink, …). Every other
        parameter is passed as ``UsePreviousValue=True`` so infra (VPC, MSK,
        plugins, secrets, role) is untouched. Returns ``True`` when an update was
        submitted, ``False`` when CloudFormation reports no changes (idempotent
        re-click: the connectors already match the requested config).

        When ``template_body`` is supplied, the update uses the new template
        instead of ``UsePreviousTemplate``. This is required when the template
        has new parameters that the deployed stack doesn't know about yet.
        """
        client = self._client("cloudformation")
        override_keys = {k for k, _ in overrides}
        # Read current params to know which keys to carry forward unchanged.
        described = client.describe_stacks(StackName=stack_name)  # type: ignore[attr-defined]
        current = described.get("Stacks", [{}])[0].get("Parameters", []) or []
        param_list = [
            {"ParameterKey": key, "ParameterValue": value} for key, value in overrides
        ]
        for p in current:
            key = p.get("ParameterKey")
            if key and key not in override_keys:
                param_list.append({"ParameterKey": key, "UsePreviousValue": True})
        try:
            if template_body is not None:
                template_kwargs = self._template_kwargs(template_body)
            else:
                template_kwargs = {"UsePreviousTemplate": True}
            client.update_stack(  # type: ignore[attr-defined]
                StackName=stack_name,
                **template_kwargs,
                Parameters=param_list,
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
            return True
        except Exception as exc:  # noqa: BLE001
            # "No updates are to be performed" is success: connectors already match.
            if "No updates are to be performed" in str(exc):
                return False
            raise CdcDeployError(f"UpdateStack failed: {str(exc).splitlines()[0]}") from exc

    def poll_events(self, stack_name: str, since: datetime) -> list[tuple[datetime, str]]:
        """Return stack events newer than ``since``, oldest-first, as log lines.

        ``describe_stack_events`` returns newest-first; this reverses to
        chronological order and formats each as
        ``"<LogicalResourceId> <ResourceStatus> <reason>"`` for the progress log.
        Best-effort: any error yields an empty list (the poll loop keeps going).
        """
        try:
            client = self._client("cloudformation")
            response = client.describe_stack_events(StackName=stack_name)
        except Exception:  # noqa: BLE001 - transient read; skip this poll
            return []
        events = response.get("StackEvents", []) or []
        out: list[tuple[datetime, str]] = []
        for ev in events:
            ts = ev.get("Timestamp")
            if not isinstance(ts, datetime):
                continue
            if ts <= since:
                continue
            res = ev.get("LogicalResourceId", "")
            state = ev.get("ResourceStatus", "")
            reason = ev.get("ResourceStatusReason", "")
            msg = f"{res} {state}" + (f" — {reason}" if reason else "")
            out.append((ts, msg))
        out.sort(key=lambda x: x[0])
        return out

    def seeder_eni_count(self, stack_name: str) -> Optional[int]:
        """Count the in-VPC seeder Lambda's still-present ENIs, or ``None`` if unknown.

        During a cdc-stack DELETE, ``MskCluster`` cannot go until AWS reclaims the
        offset-seeder Lambda's ENIs on the connector security group, which takes
        ~15-20 min and produces NO CloudFormation events -- so the deploy log looks
        frozen through the longest part of the teardown. Counting these ENIs each poll
        lets the progress loop report what it is actually waiting on ("still reclaiming
        N seeder network interface(s)") and detect when they clear.

        Resolves the connector security group from the stack's own resources (the
        stack still exists while the delete runs), then counts interface-type
        ``lambda`` ENIs on it. Read-only and best-effort: ``None`` on any error (the
        caller simply skips the line that poll), ``0`` when the SG is resolvable but
        holds no seeder ENI (reclaimed / never created).
        """
        try:
            cfn = self._client("cloudformation")
            resources = cfn.describe_stack_resources(StackName=stack_name).get(
                "StackResources", []
            ) or []
        except Exception:  # noqa: BLE001 - stack may be gone / transient read
            return None
        sg_id = next(
            (
                r.get("PhysicalResourceId")
                for r in resources
                if r.get("LogicalResourceId") == "ConnectorSecurityGroup"
                and r.get("PhysicalResourceId")
            ),
            None,
        )
        if not sg_id:
            return None
        try:
            ec2 = self._client("ec2")
            enis = ec2.describe_network_interfaces(
                Filters=[
                    {"Name": "group-id", "Values": [sg_id]},
                    {"Name": "interface-type", "Values": ["lambda"]},
                ]
            ).get("NetworkInterfaces", []) or []
        except Exception:  # noqa: BLE001 - ENI read is advisory
            return None
        return len(enis)

    def find_unsupported_azs(self, stack_name: str) -> set[str]:
        """Scan stack events for an MSK "unsupported availability zones" failure.

        Returns the AZ names MSK Serverless rejected (e.g. ``{"ap-northeast-2d"}``)
        across all failure events, or an empty set when no such reason is present.
        Best-effort: any read error yields an empty set. Used by the deploy retry
        to re-select connector subnets with the unsupported AZ(s) excluded.
        """
        try:
            client = self._client("cloudformation")
            response = client.describe_stack_events(StackName=stack_name)
        except Exception:  # noqa: BLE001
            return set()
        azs: set[str] = set()
        for ev in response.get("StackEvents", []) or []:
            azs |= _parse_unsupported_azs(ev.get("ResourceStatusReason", ""))
        return azs

    def stack_status(self, stack_name: str) -> Optional[str]:
        """Return the stack's current StackStatus, or ``None`` on error."""
        try:
            client = self._client("cloudformation")
            stacks = client.describe_stacks(StackName=stack_name).get("Stacks", [])
            return str(stacks[0].get("StackStatus")) if stacks else None
        except Exception:  # noqa: BLE001
            return None

    def connector_state(self, connector_name: str) -> Optional[str]:
        """Return the named connector's ``connectorState``, or ``None`` if it is not
        present in the (successfully read) connector list.

        Raises on an API/read error (credential expiry, throttle, permission) rather
        than swallowing it to ``None``. A swallowed read error is indistinguishable
        from "connector absent"/"still creating", so it made a RUNNING-wait poll
        (:func:`_wait_connector_running`) loop on "creating…" forever with no surfaced
        cause -- the observed failure where a RUNNING source never advanced to the
        sink pass. Callers that only want a best-effort snapshot (fast-path skips)
        wrap this and treat any error as ``None``; the RUNNING-wait treats a raised
        error as a bounded-retry / fail-with-cause signal."""
        client = self._client("kafkaconnect")
        for c in client.list_connectors().get("connectors", []) or []:
            if c.get("connectorName") == connector_name:
                return str(c.get("connectorState"))
        return None

    def connector_log_tail(
        self, stack_name: str, connector_name: str, *, limit: int = 400
    ) -> str:
        """Return recent worker-log lines for ``connector_name`` (best-effort).

        MSK Connect writes the *real* failure cause (e.g. a partition-quota
        rejection, a DB connection timeout, a missing class) to the connector's
        CloudWatch worker log -- the CloudFormation stack event for a CREATE_FAILED
        connector is only a generic ``GeneralServiceException``. This reads the
        newest stream of the cdc-stack log group (/msk-connect/<stack>-cdc) for
        that connector and returns its joined messages, so failure handlers can
        classify the cause. Returns ``""`` on any error (the log may not exist yet,
        or permissions may be missing) -- callers must treat it as advisory.
        """
        log_group = f"/msk-connect/{stack_name}-cdc"
        try:
            logs = self._client("logs")
            streams = logs.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=10,
            ).get("logStreams", [])
            stream = next(
                (
                    s.get("logStreamName")
                    for s in streams
                    if connector_name in str(s.get("logStreamName", ""))
                ),
                None,
            )
            if stream is None:
                return ""
            events = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream,
                limit=limit,
                startFromHead=False,
            ).get("events", [])
            return "\n".join(str(e.get("message", "")) for e in events)
        except Exception:  # noqa: BLE001 - advisory only
            return ""

    # -- First-deploy / teardown (create_stack / delete_stack) ---------------

    def describe_stack_or_none(self, stack_name: str) -> Optional[CdcStackDiscovery]:
        """Return discovery facts for the stack, or ``None`` if it does not exist.

        Unlike :meth:`discover_stack`, this never raises on an absent / unstable
        / placeholder-bearing stack -- it is the "does this stack already exist,
        and in what state?" probe the Deploy-infra and Delete-infra flows use to
        pick the right branch. Returns ``None`` only when the stack truly does
        not exist; re-raises (as :class:`CdcDeployError`) on unexpected API
        errors so real failures are not silently treated as "absent".
        """
        try:
            client = self._client("cloudformation")
            response = client.describe_stacks(StackName=stack_name)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "does not exist" in msg or "Stack with id" in msg:
                return None
            raise CdcDeployError(
                f"Could not check cdc-stack '{stack_name}': {msg.splitlines()[0]}"
            ) from exc
        stacks = response.get("Stacks", []) or []
        if not stacks:
            return None
        stack = stacks[0]
        status = str(stack.get("StackStatus", ""))
        params = {
            p.get("ParameterKey", ""): p.get("ParameterValue", "")
            for p in stack.get("Parameters", []) or []
        }
        return CdcStackDiscovery(
            stack_status=status,
            current_parameters=params,
            is_stable=status in _STABLE_STACK_STATES,
        )

    def list_cdc_stacks(self) -> list[tuple[str, str]]:
        """List every CloudFormation stack in this region in the
        ``mysql-dsql-cdc-*`` family (excluding ``DELETE_COMPLETE``), as
        ``(stack_name, stack_status)`` pairs.

        Account-scoped discovery so the CDC screen can find infrastructure a prior
        or other session already deployed under a name the current session no
        longer remembers -- without it, a reset session (the app is single-task and
        loses in-memory state on an ECS task replacement) shows a fresh "deploy"
        flow and can silently create a SECOND, costly MSK stack. ``ListStacks`` has
        no server-side name filter, so names are filtered client-side by
        :data:`~dsql_migrator.core.cdc.CDC_STACK_NAME_PREFIX`.

        Best-effort: returns ``[]`` on any read error (e.g. the deploy role lacks
        ``cloudformation:ListStacks``), so this discovery is purely additive and
        never blocks the screen. Paginates via ``NextToken``.
        """
        try:
            client = self._client("cloudformation")
            found: list[tuple[str, str]] = []
            token: Optional[str] = None
            while True:
                resp = (
                    client.list_stacks(NextToken=token)  # type: ignore[attr-defined]
                    if token
                    else client.list_stacks()  # type: ignore[attr-defined]
                )
                for summary in resp.get("StackSummaries", []) or []:
                    name = str(summary.get("StackName", ""))
                    status = str(summary.get("StackStatus", ""))
                    if name.startswith(CDC_STACK_NAME_PREFIX) and status != "DELETE_COMPLETE":
                        found.append((name, status))
                token = resp.get("NextToken")
                if not token:
                    break
            return found
        except Exception:  # noqa: BLE001 - best-effort; discovery must never block
            return []

    def create_stack(
        self, stack_name: str, template_body: str, parameters: Sequence[tuple[str, str]]
    ) -> None:
        """``create_stack`` the cdc-stack with the full parameter set.

        ``parameters`` are every ``(ParameterKey, ParameterValue)`` pair -- on a
        create there is no previous state, so each is passed explicitly (no
        ``UsePreviousValue``). ``CAPABILITY_NAMED_IAM`` is required (the template
        creates a named execution role). Raises :class:`CdcDeployError` if the
        stack already exists (the caller should use the Start flow instead) or on
        any other API error. Does not wait; the caller polls ``stack_status``.
        """
        client = self._client("cloudformation")
        param_list = [
            {"ParameterKey": key, "ParameterValue": value} for key, value in parameters
        ]
        try:
            client.create_stack(  # type: ignore[attr-defined]
                StackName=stack_name,
                Parameters=param_list,
                Capabilities=["CAPABILITY_NAMED_IAM"],
                **self._template_kwargs(template_body),
            )
        except Exception as exc:  # noqa: BLE001
            if "AlreadyExistsException" in str(exc) or "already exists" in str(exc):
                raise CdcDeployError(
                    f"cdc-stack '{stack_name}' already exists. Use Start CDC to "
                    "begin replication, or Delete CDC infrastructure first."
                ) from exc
            raise CdcDeployError(
                f"CreateStack failed: {str(exc).splitlines()[0]}"
            ) from exc

    def delete_stack(self, stack_name: str) -> None:
        """``delete_stack`` the whole cdc-stack. Does not wait; caller polls."""
        try:
            client = self._client("cloudformation")
            client.delete_stack(StackName=stack_name)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            raise CdcDeployError(
                f"DeleteStack failed: {str(exc).splitlines()[0]}"
            ) from exc

    def get_stack_output(self, stack_name: str, output_key: str) -> Optional[str]:
        """Return a named stack Output value (e.g. ``MskClusterArn``), or ``None``."""
        try:
            client = self._client("cloudformation")
            stacks = client.describe_stacks(StackName=stack_name).get("Stacks", []) or []
            if not stacks:
                return None
            for out in stacks[0].get("Outputs", []) or []:
                if out.get("OutputKey") == output_key:
                    return str(out.get("OutputValue"))
        except Exception:  # noqa: BLE001
            return None
        return None

    def get_bootstrap_brokers(self, cluster_arn: str) -> str:
        """Return the MSK Serverless IAM bootstrap broker string for the cluster.

        The bootstrap string is NOT available via CloudFormation Ref/GetAtt for a
        serverless cluster, so it must be read from the MSK management API after
        the cluster is ACTIVE. It is stable for the cluster's lifetime, so the
        Start flow re-fetches it each time. Raises :class:`CdcDeployError` if the
        call fails or no SASL/IAM endpoint is returned.
        """
        try:
            client = self._client("kafka")
            response = client.get_bootstrap_brokers(ClusterArn=cluster_arn)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            raise CdcDeployError(
                f"Could not read MSK bootstrap brokers: {str(exc).splitlines()[0]}"
            ) from exc
        brokers = response.get("BootstrapBrokerStringSaslIam") or ""
        if not brokers:
            raise CdcDeployError(
                "MSK cluster returned no SASL/IAM bootstrap endpoint "
                "(is the cluster ACTIVE yet?)."
            )
        return str(brokers)


def build_cdc_stack_deployer(
    region: str,
    *,
    aws_profile: Optional[str] = None,
    assume_role_arn: Optional[str] = None,
    sts_client: Optional[object] = None,
    session_factory: Optional[object] = None,
) -> CdcStackDeployer:
    """Build a deployer for the given region, optionally via an assumed role.

    When ``assume_role_arn`` is set, the deployer's clients act AS that role
    (an ``sts:AssumeRole`` is performed up front) -- this is how the privileged
    cdc-stack CloudFormation/MSK/IAM operations run under a dedicated least-
    privilege deploy role instead of the long-running app's own identity. When it
    is ``None`` (local dev / admin creds), behavior is unchanged: the deployer
    builds clients from the profile-aware shared session on demand.

    ``sts_client`` / ``session_factory`` are test seams forwarded to
    :func:`~dsql_migrator.core.aws_session.build_assumed_role_session`.
    """
    if assume_role_arn:
        from dsql_migrator.core.aws_session import build_assumed_role_session

        session = build_assumed_role_session(
            assume_role_arn,
            sts_client=sts_client,
            aws_profile=aws_profile,
            region=region,
            session_factory=session_factory,  # type: ignore[arg-type]
        )
        return CdcStackDeployer(region, session=session)
    return CdcStackDeployer(region, aws_profile=aws_profile)


# -- Stage progression on a JobManager job ----------------------------------


def _seed_stages(job: MigrationJob, stages: tuple[tuple[str, str], ...]) -> None:
    """Seed one PENDING chunk per stage of an operation (chunk_id = stage name)."""
    job.chunks = [ChunkState(chunk_id=name) for name, _ in stages]


def seed_infra_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Deploy-infra operation."""
    _seed_stages(job, CDC_INFRA_STAGES)


def seed_start_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Start-CDC operation."""
    _seed_stages(job, CDC_START_STAGES)


def seed_stop_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Stop-CDC operation."""
    _seed_stages(job, CDC_STOP_STAGES)


def seed_delete_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Delete-infra operation."""
    _seed_stages(job, CDC_DELETE_STAGES)


def _set_stage(job: MigrationJob, stage: str, status: str) -> None:
    for chunk in job.chunks:
        if chunk.chunk_id == stage:
            chunk.status = status  # type: ignore[assignment]
            return


def _now() -> datetime:
    return datetime.now(timezone.utc)


class _StageDriver:
    """Tiny helper binding a JobManager handle + log to a single operation's stages.

    Centralises the boilerplate every ``run_cdc_*`` shares: seed the chunks, mark
    a stage IN_PROGRESS/DONE/FAILED, append a timestamped log line, and -- on a
    :class:`CdcDeployError` -- log it + bump the job's ``error_count`` before
    re-raising. ``sleep`` is injectable so tests run without real waits.
    """

    def __init__(
        self,
        handle,
        *,
        stages: tuple[tuple[str, str], ...],
        on_log: Callable[[datetime, str], None],
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        import time as _time

        self._handle = handle
        self._stages = stages
        self._on_log = on_log
        self.sleep = sleep or _time.sleep
        handle.update(lambda job: _seed_stages(job, stages))

    @property
    def cancelled(self) -> bool:
        return bool(self._handle.cancelled)

    def log(self, msg: str) -> None:
        self._on_log(_now(), msg)

    def heartbeat(self) -> None:
        """Refresh the job's liveness clock without changing its state.

        CDC lifecycle waits (MSK create ~15-20 min, a connector reaching RUNNING
        up to 45 min) can pass many minutes between stack events, so the
        JobManager's stall watchdog (DEFAULT_STALL_TIMEOUT_SECONDS, 15 min) would
        otherwise reap a perfectly healthy, still-provisioning job. A no-op
        ``handle.update`` refreshes ``last_progress_at`` (apply_update stamps it on
        every mutation), proving liveness each poll so only a genuinely wedged
        operation is ever reaped. Cheap: it mutates nothing and persists the
        unchanged snapshot.
        """
        self._handle.update(lambda job: None)

    def stage(self, name: str, status: str) -> None:
        self._handle.update(lambda job, n=name, s=status: _set_stage(job, n, s))

    def all_done(self) -> None:
        """Mark every stage of this operation DONE (idempotent fast-path)."""
        for name, _ in self._stages:
            self.stage(name, "DONE")

    def fail(self, exc: CdcDeployError) -> None:
        """Log the error and bump the job error count (call before re-raising)."""
        self.log(f"ERROR: {exc}")
        self._handle.update(
            lambda job: setattr(job, "error_count", job.error_count + 1)
        )


# MSK Serverless caps total partitions per cluster. Every connector create/delete
# consumes partitions that are NOT reclaimed (the internal topics survive rollback),
# so repeated Start/iterate cycles can exhaust the quota and wedge the cluster --
# unrecoverable in place. We detect the signature in CloudFormation/MSK event text
# and turn the otherwise-opaque failure into an actionable recovery instruction.
# How often to re-report an unchanged seeder-ENI wait during a delete. The wait has no
# CloudFormation events and routinely runs ~15-20 min, so a change-only line leaves the
# log silent for that whole stretch and the teardown reads as hung. 2 min is frequent
# enough to prove liveness without drowning the stack events (polls are ~30s).
_ENI_REPORT_INTERVAL_SECONDS = 120.0

_PARTITION_QUOTA_GUIDANCE = (
    "Stack operation ended in '{status}' because the MSK Serverless partition "
    "quota is exhausted. Each connector create/delete consumes partitions that "
    "are not reclaimed, so repeated Start/retry cycles can run the cluster out. "
    "This cannot be fixed in place: use Delete CDC infrastructure to tear down the "
    "whole cdc-stack, then deploy again from scratch (CDC resumes from the source "
    "position you set). To avoid recurrence, finalize table selection before "
    "Start so you do not iterate connectors."
)


def _is_partition_quota_message(msg: str) -> bool:
    """True when a stack event signals MSK Serverless partition-quota exhaustion."""
    lowered = (msg or "").lower()
    return "partition" in lowered and (
        "quota" in lowered or "exceeded" in lowered or "limit" in lowered
    )


# MSK Serverless supports only a subset of a region's AZs and offers no API to
# list them; a subnet in an unsupported AZ makes MskCluster CREATE_FAILED with a
# reason like "unsupported availability zones: [ap-northeast-2d]" (one or more,
# comma-separated inside the brackets). This extracts those AZ names so the
# deploy can re-select subnets with them excluded.
_UNSUPPORTED_AZ_RE = re.compile(
    r"unsupported availability zones?:?\s*\[([^\]]*)\]", re.IGNORECASE
)


def _parse_unsupported_azs(reason: str) -> set[str]:
    """Extract the AZ names from an MSK "unsupported availability zones: [..]" reason.

    Returns an empty set when the reason is not that failure. Pure (no AWS)."""
    match = _UNSUPPORTED_AZ_RE.search(reason or "")
    if not match:
        return set()
    return {az.strip() for az in match.group(1).split(",") if az.strip()}


# Known connector-failure signatures and the actionable guidance for each. The
# CloudFormation event for a CREATE_FAILED connector is only a generic
# GeneralServiceException; the real cause is in the worker log. Each entry is
# (substring-matcher, guidance template) -- the first match wins. Matchers are
# lowercase substring checks against the joined worker log.
_CONNECTOR_LOG_DIAGNOSES: tuple[tuple[str, str], ...] = (
    (
        "quota exceeded for maximum number of partitions",
        "{connector} failed: the MSK Serverless partition quota is exhausted. Each "
        "connector create/delete consumes partitions that are not reclaimed, so "
        "repeated Start/retry cycles run the cluster out. This cannot be fixed in "
        "place: use Delete CDC infrastructure to tear down the whole cdc-stack, then "
        "deploy again from scratch (CDC resumes from the source position you set). To "
        "avoid recurrence, finalize your table selection before Start so you do not "
        "iterate connectors.",
    ),
    (
        "could not find a 'kafkaclient' entry in the jaas",
        "{connector} failed: the schema-history Kafka client could not authenticate "
        "to MSK (missing IAM JAAS config). This is a cdc-stack template defect, not an "
        "operational issue -- report it; redeploying will not help until the template "
        "is fixed.",
    ),
    (
        "auth_scheme_provider",
        "{connector} failed with an AWS SDK conflict (NoSuchFieldError "
        "AUTH_SCHEME_PROVIDER) in the connector plugin. This is a plugin-packaging "
        "defect (conflicting bundled SDK jars), not an operational issue -- report it; "
        "redeploying the same plugin will not help.",
    ),
    (
        "communications link failure",
        "{connector} failed: the Debezium worker could not reach the source MySQL "
        "(TCP connection timed out). Check that the source database is reachable from "
        "the connector's network and that its security group allows inbound on the "
        "MySQL port from the connector, then Start CDC again.",
    ),
    (
        "access denied for user",
        "{connector} failed: the source MySQL rejected the connector's credentials "
        "(access denied). Verify the CDC user's username/password and that it has the "
        "required REPLICATION privileges, then Start CDC again.",
    ),
)


def _diagnose_connector_log(connector: str, log_tail: str) -> Optional[str]:
    """Map a connector's worker-log tail to actionable guidance, or ``None``.

    Returns the guidance for the first known failure signature found in
    ``log_tail`` (the recent worker-log messages). ``None`` when nothing matches,
    so the caller can fall back to the raw connector state. Pure (no AWS).
    """
    lowered = (log_tail or "").lower()
    for needle, guidance in _CONNECTOR_LOG_DIAGNOSES:
        if needle in lowered:
            return guidance.format(connector=connector)
    return None


def _wait_stack_settles(
    deployer: CdcStackDeployer,
    stack_name: str,
    *,
    driver: _StageDriver,
    since: datetime,
    timeout: float,
    interval: float,
    vanish_ok: bool = False,
    connector_for_log: Optional[str] = None,
    watch_seeder_enis: bool = False,
) -> Optional[str]:
    """Stream stack events to the log and block until the stack settles.

    Returns the final (non ``*_IN_PROGRESS``) StackStatus. When ``vanish_ok`` is
    set (the Delete path), a stack that no longer exists is a success and returns
    ``None``. Otherwise a transient ``None`` from ``stack_status`` just keeps
    polling. Raises :class:`CdcDeployError` on a ROLLBACK/FAILED terminal state or
    on timeout. Honors cancellation by returning the last seen status.

    A connector's stack op (CREATE_FAILED) rolls back with only a generic
    CloudFormation event -- the real cause lives in the worker log. When
    ``connector_for_log`` names the connector being created, a rollback triggers a
    worker-log scan so a known failure (partition quota, source unreachable, a
    plugin defect, …) becomes actionable guidance instead of an opaque
    "Stack operation ended in 'UPDATE_ROLLBACK_COMPLETE'".
    """
    import time as _time

    deadline = _monotonic_deadline(timeout)
    seen_until = since
    saw_partition_quota = False
    # Seeder-ENI reporting state. ``-2`` is an unused sentinel so the first real
    # reading (including 0) always prints. ``eni_wait_started`` / ``eni_last_logged``
    # drive the periodic re-report: logging ONLY on change left an observed 18m30s gap
    # between "MskCluster DELETE_COMPLETE" and "Seeder network interfaces released.",
    # which reads as a hung teardown -- the very symptom this reporting exists to cure.
    last_eni_count = -2
    eni_wait_started: Optional[float] = None
    eni_last_logged: Optional[float] = None
    while True:
        if driver.cancelled:
            return None
        # Prove liveness each poll: MSK provisioning can pass >15 min between
        # stack events, and without a heartbeat the stall watchdog would reap this
        # healthy job. (A log line below also refreshes liveness, but events can be
        # sparse, so heartbeat unconditionally.)
        driver.heartbeat()
        for ts, msg in deployer.poll_events(stack_name, seen_until):
            driver.log(msg)
            seen_until = ts
            if _is_partition_quota_message(msg):
                saw_partition_quota = True
        # Delete only: report the seeder-ENI reclamation that CloudFormation is silent
        # about, so the long tail of the teardown does not look frozen. Reported on
        # CHANGE *and* periodically while the count is unchanged -- the wait routinely
        # runs ~15-20 min with no stack event, so a change-only line leaves a silent gap
        # that reads as a hang (observed: 18m30s between the last CFN event and
        # "released"). The re-report carries elapsed minutes so the operator can see it
        # is progressing rather than stuck.
        if watch_seeder_enis:
            count = deployer.seeder_eni_count(stack_name)
            if count is not None:
                now_mono = _time.monotonic()
                changed = count != last_eni_count
                if count > 0:
                    if eni_wait_started is None:
                        eni_wait_started = now_mono
                    due = (
                        eni_last_logged is None
                        or (now_mono - eni_last_logged) >= _ENI_REPORT_INTERVAL_SECONDS
                    )
                    if changed or due:
                        noun = "interface" if count == 1 else "interfaces"
                        waited = int((now_mono - eni_wait_started) // 60)
                        # Only mention elapsed time once there is some to mention, so
                        # the first line stays clean.
                        elapsed = f" — {waited} min so far" if waited >= 1 else ""
                        driver.log(
                            f"Waiting for AWS to reclaim {count} seeder network "
                            f"{noun}{elapsed} (no CloudFormation event until done)…"
                        )
                        eni_last_logged = now_mono
                elif changed and last_eni_count > 0:
                    # Only announce release if we had previously seen some pending.
                    driver.log("Seeder network interfaces released.")
                    eni_wait_started = None
                    eni_last_logged = None
                last_eni_count = count
        status = deployer.stack_status(stack_name)
        if status is None:
            if vanish_ok:
                return None  # stack is gone — delete succeeded
            # transient read error mid-operation; keep polling
        elif "IN_PROGRESS" not in status:
            if "ROLLBACK" in status or "FAILED" in status:
                if saw_partition_quota:
                    raise CdcDeployError(_PARTITION_QUOTA_GUIDANCE.format(status=status))
                if connector_for_log:
                    tail = deployer.connector_log_tail(stack_name, connector_for_log)
                    guidance = _diagnose_connector_log(connector_for_log, tail)
                    if guidance:
                        raise CdcDeployError(guidance)
                raise CdcDeployError(f"Stack operation ended in '{status}'.")
            return status
        if _deadline_passed(deadline):
            raise CdcDeployError("Stack operation timed out.")
        driver.sleep(interval)


# -- The four lifecycle operations (JobManager work functions) ---------------


def _patch_plugin_params(params: CdcInfraParams, upload) -> CdcInfraParams:
    """Return a new CdcInfraParams with plugin S3 params filled from the upload.

    ``build_cdc_infra_params`` emits empty placeholders for PluginBucketArn / the
    two plugin keys / PluginVersion when the tool will provision them; after the
    bucket + upload stages run, this fills those four with the upload result so
    ``create_stack`` gets concrete values. Pure (no AWS).
    """
    updates = {
        "PluginBucketArn": upload.bucket_arn,
        "DebeziumPluginS3Key": upload.debezium_key,
        "DsqlSinkPluginS3Key": upload.dsql_sink_key,
        "LambdaSeederS3Key": upload.lambda_seeder_key,
        "PluginVersion": upload.plugin_version,
    }
    new_filled = [(k, updates.get(k, v)) for k, v in params.filled]
    present = {k for k, _ in new_filled}
    for k, v in updates.items():
        if k not in present:
            new_filled.append((k, v))
    return CdcInfraParams(
        filled=new_filled,
        stack_name=params.stack_name,
        topic_prefix=params.topic_prefix,
    )


def _param_value(params: CdcInfraParams, key: str) -> Optional[str]:
    """Return a filled parameter's value by key, or ``None`` if absent."""
    for k, v in params.filled:
        if k == key:
            return str(v)
    return None


def _with_subnet_param(params: CdcInfraParams, subnet_ids: str) -> CdcInfraParams:
    """Return a copy of ``params`` with ConnectorSubnetIds replaced. Pure (no AWS)."""
    new_filled = [
        (k, subnet_ids if k == "ConnectorSubnetIds" else v) for k, v in params.filled
    ]
    return CdcInfraParams(
        filled=new_filled,
        stack_name=params.stack_name,
        topic_prefix=params.topic_prefix,
    )


def _retry_infra_without_azs(
    driver: "_StageDriver",
    deployer: CdcStackDeployer,
    stack_name: str,
    params: CdcInfraParams,
    *,
    excluded_azs: set[str],
    new_azs: set[str],
    ec2_client: Optional[BotoSessionLike],
    aws_profile: Optional[str],
    region: Optional[str],
    delete_timeout: float,
    interval: float,
) -> CdcInfraParams:
    """Recover from an MSK unsupported-AZ CREATE_FAILED by re-selecting subnets.

    Tears down the rolled-back stack, then re-selects NAT-egress connector subnets
    with ``excluded_azs`` removed and returns ``params`` with the new
    ConnectorSubnetIds. Raises :class:`CdcDeployError` when the VPC can no longer
    yield >=2 supported AZs (so the caller stops rather than looping forever).
    """
    from dsql_migrator.core.ec2_metadata import (
        Ec2MetadataError,
        build_ec2_client,
        select_connector_subnets,
    )

    driver.log(
        "MSK Serverless does not support availability zone(s) "
        f"{', '.join(sorted(new_azs))} in this region; excluding them and "
        "retrying with different subnets."
    )

    vpc_id = _param_value(params, "VpcId")
    if not vpc_id:
        raise CdcDeployError(
            "MSK rejected the connector subnets' availability zone(s) "
            f"({', '.join(sorted(new_azs))}) and no VpcId is available to "
            "re-select subnets. Enter subnet ids in supported AZs manually."
        )

    # Tear down the rolled-back stack — CloudFormation will not create over it.
    driver.log(f"Deleting the rolled-back stack '{stack_name}' before retrying…")
    deployer.delete_stack(stack_name)
    _wait_stack_settles(
        deployer, stack_name, driver=driver, since=_now(),
        timeout=delete_timeout, interval=interval, vanish_ok=True,
    )

    client = ec2_client or build_ec2_client(aws_profile, region)
    try:
        selection = select_connector_subnets(
            client, vpc_id, excluded_azs=excluded_azs
        )
    except Ec2MetadataError as exc:
        raise CdcDeployError(str(exc)) from exc
    if not selection.can_auto_select or not selection.subnet_ids:
        raise CdcDeployError(selection.reason)
    driver.log(selection.reason)
    return _with_subnet_param(params, selection.subnet_ids)


def run_cdc_infra_deploy(
    handle,
    *,
    stack_name: str,
    template_body: str,
    params: CdcInfraParams,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    region: Optional[str] = None,
    aws_profile: Optional[str] = None,
    s3_client: Optional[BotoSessionLike] = None,
    sts_client: Optional[BotoSessionLike] = None,
    ec2_client: Optional[BotoSessionLike] = None,
    create_timeout_seconds: float = 1800.0,
    poll_interval_seconds: float = 30.0,
    delete_timeout_seconds: float = 1200.0,
    max_az_retries: int = 3,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Deploy CDC infrastructure with ``create_stack`` (no connectors yet).

    Walks :data:`CDC_INFRA_STAGES`: ensure the managed plugin S3 bucket exists and
    upload the two bundled connector artifacts, then refuse to create over an
    existing stack (directing the user to Start, or to Delete a rolled-back one),
    then create the stack and poll until ``CREATE_COMPLETE``. The MSK Serverless
    cluster takes ~15-20 min, so ``create_timeout_seconds`` defaults to 30 min.

    ``region``/``aws_profile`` (or injected ``s3_client``/``sts_client`` for tests)
    drive the bucket+upload stages; the resulting bucket ARN / keys / version are
    patched into ``params`` before ``create_stack``.
    """
    from dsql_migrator.core.s3_provision import (
        S3ProvisionError,
        build_s3_client,
        build_sts_client,
        ensure_and_upload_plugins,
    )

    driver = _StageDriver(
        handle, stages=CDC_INFRA_STAGES, on_log=on_log, sleep=sleep
    )
    try:
        # 0a. ensure plugin bucket + 0b. upload plugins (background — ~42 MiB).
        driver.stage("ensure_bucket", "IN_PROGRESS")
        if region is None:
            raise CdcDeployError(
                "No AWS region for plugin upload — configure the target connection."
            )
        s3 = s3_client if s3_client is not None else build_s3_client(aws_profile, region)
        sts = sts_client if sts_client is not None else build_sts_client(aws_profile, region)
        driver.stage("ensure_bucket", "DONE")

        driver.stage("upload_plugins", "IN_PROGRESS")
        try:
            upload = ensure_and_upload_plugins(s3, sts, region, on_progress=driver.log)
        except S3ProvisionError as exc:
            driver.stage("upload_plugins", "FAILED")
            raise CdcDeployError(str(exc)) from exc
        params = _patch_plugin_params(params, upload)
        # The cdc-stack template now exceeds CloudFormation's 51,200-byte inline
        # limit, so the deployer stages it in the managed plugin bucket and uses
        # TemplateURL. Reuse the just-provisioned bucket.
        deployer.template_s3_bucket = upload.bucket_name
        driver.stage("upload_plugins", "DONE")

        if driver.cancelled:
            return

        # 1. check existing — never create over a live or rolled-back stack
        driver.stage("check_existing", "IN_PROGRESS")
        driver.log(f"Checking whether cdc-stack '{stack_name}' already exists…")
        existing = deployer.describe_stack_or_none(stack_name)
        if existing is not None:
            status = existing.stack_status
            if "ROLLBACK" in status:
                raise CdcDeployError(
                    f"cdc-stack '{stack_name}' is in '{status}' from a failed "
                    "creation. Delete it (Delete CDC infrastructure) then retry."
                )
            if "IN_PROGRESS" in status:
                raise CdcDeployError(
                    f"cdc-stack '{stack_name}' is '{status}'. Wait for the current "
                    "operation to finish, then retry."
                )
            raise CdcDeployError(
                f"cdc-stack '{stack_name}' already exists ('{status}'). Use Start "
                "CDC to begin replication, or Delete CDC infrastructure first."
            )
        driver.stage("check_existing", "DONE")

        # 2. validate generated params carry no unfilled placeholder
        driver.stage("validate_params", "IN_PROGRESS")
        bad = [k for k, v in params.filled if str(v).startswith(CDC_PLACEHOLDER_PREFIX)]
        if bad:
            raise CdcDeployError(f"Unfilled infrastructure parameters: {bad}")
        driver.stage("validate_params", "DONE")

        if driver.cancelled:
            return

        # 3+4. create_stack, wait for CREATE_COMPLETE, and self-heal the one
        # failure the user cannot pre-empt: MSK Serverless supports only a subset
        # of a region's AZs (no API to list them), so a NAT subnet auto-selected
        # in an unsupported AZ makes MskCluster CREATE_FAILED. On that reason we
        # delete the rolled-back stack, re-select subnets with the AZ excluded,
        # and retry — bounded by max_az_retries so a genuinely stuck deploy stops.
        excluded_azs: set[str] = set()
        attempt = 0
        while True:
            if driver.cancelled:
                return
            attempt += 1

            driver.stage("create_stack", "IN_PROGRESS")
            since = _now()
            deployer.create_stack(stack_name, template_body, params.filled)
            driver.log("Stack creation submitted — this provisions MSK (~15-20 min).")
            driver.stage("create_stack", "DONE")

            driver.stage("stack_create", "IN_PROGRESS")
            try:
                _wait_stack_settles(
                    deployer, stack_name, driver=driver, since=since,
                    timeout=create_timeout_seconds, interval=poll_interval_seconds,
                )
                break
            except CdcDeployError:
                new_azs = deployer.find_unsupported_azs(stack_name) - excluded_azs
                if not new_azs or attempt > max_az_retries:
                    driver.stage("stack_create", "FAILED")
                    raise
                excluded_azs |= new_azs
                try:
                    params = _retry_infra_without_azs(
                        driver, deployer, stack_name, params,
                        excluded_azs=excluded_azs, new_azs=new_azs,
                        ec2_client=ec2_client, aws_profile=aws_profile, region=region,
                        delete_timeout=delete_timeout_seconds,
                        interval=poll_interval_seconds,
                    )
                except CdcDeployError:
                    driver.stage("stack_create", "FAILED")
                    raise
        if driver.cancelled:
            return
        driver.stage("stack_create", "DONE")

        # 5. ready
        driver.stage("infra_ready", "IN_PROGRESS")
        driver.log("Infrastructure ready. Run Start CDC to begin replication.")
        driver.stage("infra_ready", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def run_cdc_start(
    handle,
    *,
    stack_name: str,
    params: CdcStackParams,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    watermark: Optional[Watermark] = None,
    template_body: Optional[str] = None,
    connector_timeout_seconds: float = 2700.0,
    poll_interval_seconds: float = 15.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Start CDC with a SINGLE-pass update that creates both connectors at once.

    Walks :data:`CDC_START_STAGES`. Fetches the cluster bootstrap brokers, then a
    single ``update_stack`` sets ``MskBootstrapServers`` + ``DeploySink="true"`` so
    CloudFormation runs ``CdcStartPrepResource`` (pre-creating the per-table topics
    and, on a gapless handoff, seeding the offset) and then creates the source AND
    sink connectors IN PARALLEL (both DependsOn only the pre-created topics, not each
    other). We then wait for both to reach RUNNING. This roughly halves the
    connector wall time vs the old source-then-sink two-pass, and removes the
    empty-partition-assignment race at its source (topics exist before either
    connector starts). The flow is config-aware idempotent: the whole pass is
    skipped only when BOTH connectors are already RUNNING AND the desired connector
    configuration (notably the table set -> ``TableIncludeList`` / ``SinkTopics``)
    matches the deployed stack. When the configuration changed (e.g. a different set
    of tables), the connectors are updated rather than silently kept on the old
    table set; an identical re-start still no-ops (CloudFormation reports no changes)
    and burns no MSK quota.

    ``connector_timeout_seconds`` is the upper bound PER connector wait (applied to
    each connector's RUNNING wait; because both deploy in parallel the total wait is
    ~max(source, sink), not their sum), NOT the whole operation. It defaults to
    2700s (45 min) because MSK Connect connector creation (Fargate provisioning +
    plugin download of our ~70-90 MiB zips + Kafka Connect worker boot + Glue
    Schema Registry connect) can take well past 15-25 min EACH, and a too-short
    budget would time out and red-flag a connector that is still creating normally.
    A generous bound is essentially free: polling exits the instant a connector
    reaches RUNNING/FAILED, so the full timeout is only ever consumed when the
    connector genuinely never settles.

    ``watermark`` (the Full Load consistency point) drives the automatic gapless
    offset seed: its binlog coordinates are passed as the cdc-stack Watermark*
    parameters on the SOURCE pass only, so the in-VPC seeder Lambda creates +
    seeds the connect-offsets record before the source connector is created. The
    Watermark* params are deliberately kept OUT of the config-changed comparison
    and the shared connector overrides, so a watermark-only change never forces an
    already-RUNNING source connector to be torn down and rebuilt (the seed is a
    create-time concern; rewinding a live connector is exactly what the seeder's
    no-clobber guard prevents anyway). When ``watermark`` is None or lacks binlog
    coordinates the Watermark* params are blanked and the template's SeedOffset
    condition stays false (legacy: start from the current binlog).
    """
    driver = _StageDriver(
        handle, stages=CDC_START_STAGES, on_log=on_log, sleep=sleep
    )
    src_name, sink_name = cdc_expected_connector_names(stack_name)
    try:
        # 1. discover (raises if absent/unstable/placeholder)
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        discovery = deployer.discover_stack(stack_name)
        driver.stage("discover_stack", "DONE")

        # 2. validate connector params
        driver.stage("validate_params", "IN_PROGRESS")
        bad = [k for k, v in params.filled if str(v).startswith(CDC_PLACEHOLDER_PREFIX)]
        if bad:
            raise CdcDeployError(f"Unfilled values in connector parameters: {bad}")
        driver.stage("validate_params", "DONE")

        if driver.cancelled:
            return

        # 3. fetch bootstrap brokers (re-read each Start; stable per cluster)
        driver.stage("fetch_bootstrap", "IN_PROGRESS")
        cluster_arn = deployer.get_stack_output(stack_name, "MskClusterArn")
        if not cluster_arn:
            raise CdcDeployError(
                "Could not read MskClusterArn from the stack outputs — is the "
                "infrastructure deploy complete?"
            )
        bootstrap = deployer.get_bootstrap_brokers(cluster_arn)
        driver.log("Fetched MSK bootstrap brokers.")
        driver.stage("fetch_bootstrap", "DONE")

        # connector-control overrides shared by both passes. Watermark* keys are
        # never part of params.filled (they come from the separate watermark arg),
        # so they cannot leak into config_changed; we also defensively exclude them
        # here so a watermark-only change can never bounce a RUNNING source.
        _seed_keys = set(CDC_WATERMARK_PARAM_KEYS)
        connector_overrides = [
            kv
            for kv in params.filled
            if kv[0] not in {"MskBootstrapServers", "DeploySink"} and kv[0] not in _seed_keys
        ]

        # Watermark -> cdc-stack Watermark* params, applied on the SOURCE pass only
        # (a create-time seed concern). Blank/absent watermark -> SeedOffset false.
        watermark_overrides = build_watermark_params(watermark)
        if watermark is not None and watermark.binlog_file:
            driver.log(
                "Seeding gapless CDC start offset from the Full Load watermark "
                f"({watermark.binlog_file}:{watermark.binlog_position})."
            )

        # Has the desired connector configuration (notably the table set ->
        # TableIncludeList / SinkTopics) changed vs the currently-deployed stack?
        # The RUNNING fast-path below skips only when the config is UNCHANGED;
        # when it differs (e.g. a new/different set of tables for Full load + CDC)
        # the connectors MUST be updated, so we never silently keep the old table
        # set. CloudFormation is the safety net for a true no-op (an identical
        # config update reports no changes and burns no MSK partition quota).
        config_changed = any(
            str(discovery.current_parameters.get(key, "")) != str(value)
            for key, value in connector_overrides
        )

        # -- Single pass: create BOTH connectors at once ----------------------
        # CdcStartPrepResource (in the stack) pre-creates the per-table topics, so
        # the sink no longer waits for the source; both connectors deploy in
        # parallel. Best-effort pre-check: a read error only means "cannot confirm
        # RUNNING", so fall through to the (idempotent) pass. Skip the whole pass
        # only when BOTH connectors are already RUNNING with the requested config.
        try:
            src_state = deployer.connector_state(src_name)
        except Exception:  # noqa: BLE001
            src_state = None
        try:
            sink_state = deployer.connector_state(sink_name)
        except Exception:  # noqa: BLE001
            sink_state = None
        both_running = (
            src_state == _CONNECTOR_RUNNING and sink_state == _CONNECTOR_RUNNING
        )
        if both_running and not config_changed:
            driver.log(
                "Both connectors already RUNNING with the requested configuration "
                "— nothing to start."
            )
            driver.stage("submit_connectors", "DONE")
            driver.stage("stack_connectors", "DONE")
            driver.stage("connectors_running", "DONE")
        else:
            if both_running and config_changed:
                driver.log(
                    "Connectors RUNNING but the configuration changed (e.g. a "
                    "different table set) — updating them."
                )
            driver.stage("submit_connectors", "IN_PROGRESS")
            since = _now()
            start_pass = [
                ("MskBootstrapServers", bootstrap),
                ("DeploySink", "true"),
                *watermark_overrides,
                *connector_overrides,
            ]
            changed = deployer.submit_update(
                stack_name, start_pass, template_body=template_body
            )
            if changed:
                driver.log(
                    "Connector deploy submitted (topics + source + sink in one pass)."
                )
                driver.stage("submit_connectors", "DONE")
                driver.stage("stack_connectors", "IN_PROGRESS")
                try:
                    _wait_stack_settles(
                        deployer, stack_name, driver=driver, since=since,
                        timeout=connector_timeout_seconds, interval=poll_interval_seconds,
                        connector_for_log=f"{src_name} + {sink_name}",
                    )
                except CdcDeployError:
                    driver.stage("stack_connectors", "FAILED")
                    raise
                driver.stage("stack_connectors", "DONE")
            else:
                driver.log("No connector changes needed.")
                driver.stage("submit_connectors", "DONE")
                driver.stage("stack_connectors", "DONE")
            if driver.cancelled:
                return
            # Both connectors were created by the single stack update and deploy in
            # parallel; wait for each to reach RUNNING under the one connectors_running
            # stage. Sequential polling, so the total wait is ~max(source, sink) --
            # not their sum -- because both are already deploying concurrently.
            driver.stage("connectors_running", "IN_PROGRESS")
            _wait_connector_running(
                deployer, src_name, "connectors_running", driver,
                connector_timeout_seconds, poll_interval_seconds,
                stack_name=stack_name,
            )
            _wait_connector_running(
                deployer, sink_name, "connectors_running", driver,
                connector_timeout_seconds, poll_interval_seconds,
                stack_name=stack_name,
            )
            driver.stage("connectors_running", "DONE")

        # final
        driver.stage("pipeline_running", "IN_PROGRESS")
        driver.log("CDC pipeline is running.")
        driver.stage("pipeline_running", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def run_cdc_stop(
    handle,
    *,
    stack_name: str,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    stop_timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 15.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Stop CDC by blanking ``MskBootstrapServers`` so CFN deletes both connectors.

    Walks :data:`CDC_STOP_STAGES`. MSK / VPC / plugins / IAM are preserved for a
    fast restart. Idempotent: if the stack already has ``MskBootstrapServers=""``
    (already stopped, or infra-only) no update is submitted.
    """
    driver = _StageDriver(
        handle, stages=CDC_STOP_STAGES, on_log=on_log, sleep=sleep
    )
    try:
        # 1. discover
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        discovery = deployer.discover_stack(stack_name)
        already_stopped = (discovery.current_parameters.get("MskBootstrapServers", "") == "")
        driver.stage("discover_stack", "DONE")

        if already_stopped:
            driver.log("CDC is already stopped — no connectors to remove.")
            driver.all_done()
            return

        if driver.cancelled:
            return

        # 2. submit stop (blank the bootstrap → both connector conditions go false)
        driver.stage("submit_stop", "IN_PROGRESS")
        since = _now()
        changed = deployer.submit_update(stack_name, [("MskBootstrapServers", "")])
        if not changed:
            driver.log("No changes — connectors already removed.")
            driver.all_done()
            return
        driver.log("Connector removal submitted.")
        driver.stage("submit_stop", "DONE")

        # 3. wait for the update to settle
        driver.stage("stack_stop", "IN_PROGRESS")
        try:
            _wait_stack_settles(
                deployer, stack_name, driver=driver, since=since,
                timeout=stop_timeout_seconds, interval=poll_interval_seconds,
            )
        except CdcDeployError:
            driver.stage("stack_stop", "FAILED")
            raise
        if driver.cancelled:
            return
        driver.stage("stack_stop", "DONE")

        # 4. done
        driver.stage("connectors_gone", "IN_PROGRESS")
        driver.log("CDC stopped. MSK and infrastructure preserved.")
        driver.stage("connectors_gone", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def run_cdc_delete(
    handle,
    *,
    stack_name: str,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    region: Optional[str] = None,
    aws_profile: Optional[str] = None,
    cleanup_source_secret: bool = True,
    delete_timeout_seconds: float = 1800.0,
    poll_interval_seconds: float = 30.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Delete the entire cdc-stack (full teardown / rollback recovery).

    Walks :data:`CDC_DELETE_STAGES`. Uses :meth:`describe_stack_or_none` (not
    ``discover_stack``) so a ``ROLLBACK_COMPLETE`` stack can still be deleted. If
    the stack is already gone the operation succeeds immediately.

    After the stack is gone, the tool-managed source-credentials secret (created
    out-of-band by :func:`ensure_source_secret`, so CloudFormation cannot delete it)
    is scheduled for deletion with a recovery window, so production DB credentials do
    not linger in Secrets Manager. The secret delete uses the app's own credentials
    (``region``/``aws_profile``), NOT the assumed deploy role, mirroring how it was
    created. A cleanup failure does NOT fail the teardown -- the stack is already
    gone -- it logs a manual-deletion reminder. Set ``cleanup_source_secret=False``
    (or omit ``region``) to skip cleanup, e.g. when the source used Secrets Manager
    auth and the tool never created a secret.
    """
    driver = _StageDriver(
        handle, stages=CDC_DELETE_STAGES, on_log=on_log, sleep=sleep
    )
    try:
        # 1. discover (non-raising — a rolled-back stack must still be deletable)
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        existing = deployer.describe_stack_or_none(stack_name)
        if existing is None:
            driver.log("Stack does not exist — nothing to delete.")
            driver.all_done()
            return
        # A stack mid-operation cannot be cleanly deleted yet. If a delete is ALREADY
        # underway (DELETE_IN_PROGRESS) just wait for it -- re-submitting is wasteful
        # and CloudFormation ignores it. For any OTHER in-flight operation (a
        # create/update/rollback still running) submitting a delete now races the
        # live operation and CloudFormation may reject it, so stop with a clear
        # wait-and-retry message instead of a blind, possibly-doomed submit. This
        # mirrors the deploy path's IN_PROGRESS guard.
        status = existing.stack_status
        already_deleting = status == "DELETE_IN_PROGRESS"
        # In-flight == any live CloudFormation operation (statuses end in
        # ``_IN_PROGRESS``, e.g. CREATE_/UPDATE_/UPDATE_ROLLBACK_IN_PROGRESS).
        in_flight = bool(status) and status.upper().endswith("_IN_PROGRESS")
        if in_flight and not already_deleting:
            raise CdcDeployError(
                f"cdc-stack '{stack_name}' is '{status}' — an operation is still "
                "running, so it cannot be deleted yet. Wait for it to finish, then "
                "try Delete CDC infrastructure again. (If it stays stuck, delete the "
                f"'{stack_name}' stack in the AWS CloudFormation console.)"
            )
        driver.stage("discover_stack", "DONE")

        if driver.cancelled:
            return

        # 2. submit delete (skip the submit if a deletion is already in flight; just
        #    fall through to wait for it to settle).
        driver.stage("submit_delete", "IN_PROGRESS")
        since = _now()
        if already_deleting:
            driver.log("Stack deletion already in progress — waiting for it to finish.")
        else:
            deployer.delete_stack(stack_name)
            driver.log("Stack deletion submitted.")
        driver.stage("submit_delete", "DONE")

        # 3. wait for the stack to disappear
        driver.stage("stack_delete", "IN_PROGRESS")
        try:
            _wait_stack_settles(
                deployer, stack_name, driver=driver, since=since,
                timeout=delete_timeout_seconds, interval=poll_interval_seconds,
                vanish_ok=True,
                watch_seeder_enis=True,
            )
        except CdcDeployError:
            # DELETE_FAILED is usually the in-VPC offset-seeder Lambda's leftover
            # (detached) ENIs still pinning the connector subnets / SG while AWS
            # reclaims them. Auto-recover: delete those available ENIs and re-issue
            # the delete (retaining anything still stuck), instead of dead-ending the
            # teardown and leaving billable MSK/NAT behind (previously a manual ENI
            # cleanup + delete-stack from the CLI).
            status = deployer.stack_status(stack_name)
            if status == "DELETE_FAILED":
                driver.log(
                    "Stack delete blocked (likely leftover Lambda ENIs); clearing "
                    "detached ENIs and retrying the delete…"
                )
                recovered = deployer.recover_delete_failed(stack_name)
                if recovered == "" or deployer.stack_status(stack_name) is None:
                    driver.log("Stack deletion completed after ENI cleanup.")
                    driver.stage("stack_delete", "DONE")
                else:
                    driver.stage("stack_delete", "FAILED")
                    raise
            else:
                driver.stage("stack_delete", "FAILED")
                raise
        else:
            driver.stage("stack_delete", "DONE")
        if driver.cancelled:
            return

        # 4. clean up the tool-managed source-credentials secret (best effort).
        #    CloudFormation never owned it, so it must be removed here or it lingers
        #    in Secrets Manager with production DB credentials. A failure here is
        #    logged, not fatal -- the infrastructure is already gone.
        driver.stage("cleanup_secret", "IN_PROGRESS")
        if cleanup_source_secret and region:
            from dsql_migrator.core.secrets import (
                SecretProvisionError,
                cdc_source_secret_name,
                delete_source_secret,
            )

            secret_name = cdc_source_secret_name(stack_name)
            try:
                result = delete_source_secret(
                    stack_name=stack_name, aws_profile=aws_profile, region=region
                )
                if result == "absent":
                    driver.log(
                        f"No tool-managed source secret '{secret_name}' to remove "
                        "(the source likely used Secrets Manager auth)."
                    )
                else:
                    driver.log(
                        f"Source-credentials secret '{secret_name}' scheduled for "
                        "deletion (7-day recovery window)."
                    )
            except SecretProvisionError as exc:
                driver.log(
                    f"WARNING: could not delete the source secret '{secret_name}': "
                    f"{exc}. Delete it manually in Secrets Manager so the database "
                    "credentials do not linger."
                )
        else:
            driver.log("Skipped source-secret cleanup (no region or disabled).")
        driver.stage("cleanup_secret", "DONE")

        # 5. done
        driver.stage("deleted", "IN_PROGRESS")
        driver.log("CDC infrastructure deleted.")
        driver.stage("deleted", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def _wait_connector_running(
    deployer, connector, stage_name, driver: _StageDriver, timeout, interval,
    *, stack_name: Optional[str] = None
) -> None:
    """Poll one connector until RUNNING; raise on FAILED or timeout.

    ``driver`` supplies cancellation, logging, and the injectable ``sleep``. On a
    FAILED connector the CloudFormation event is only a generic error, so when
    ``stack_name`` is given we read the connector's worker log and, if it matches a
    known failure signature (partition-quota exhaustion, source-DB unreachable, a
    plugin/SDK defect, …), raise with the actionable guidance instead of the opaque
    "entered FAILED state".
    """
    deadline = _monotonic_deadline(timeout)
    read_failures = 0
    while True:
        if driver.cancelled:
            return
        # Prove liveness each poll so a connector that legitimately takes many
        # minutes to reach RUNNING is not reaped by the stall watchdog.
        driver.heartbeat()
        try:
            state = deployer.connector_state(connector)
        except Exception as exc:  # noqa: BLE001 - a state READ failure, not a state
            # A read failure must NOT masquerade as "still creating" (that stalled the
            # deploy forever with no surfaced cause). A credential/authorization error
            # will not self-heal -> fail immediately with the cause; a transient error
            # is tolerated for a few consecutive polls, then surfaced.
            read_failures += 1
            cause = str(exc).splitlines()[0]
            terminal = _is_terminal_read_error(exc)
            driver.log(
                f"{connector}: could not read state ({cause}) "
                f"[{read_failures}/{_MAX_STATE_READ_FAILURES}]"
            )
            if terminal or read_failures >= _MAX_STATE_READ_FAILURES:
                hint = (
                    " Credentials may have expired or access was lost; retry Start CDC."
                    if terminal else ""
                )
                raise CdcDeployError(
                    f"Could not read {connector} state: {cause}.{hint}"
                )
            if _deadline_passed(deadline):
                raise CdcDeployError(f"{connector} did not reach RUNNING in time.")
            driver.sleep(interval)
            continue
        read_failures = 0  # a successful read clears the transient-failure streak
        if state == _CONNECTOR_RUNNING:
            driver.log(f"{connector}: RUNNING")
            return
        if state == _CONNECTOR_FAILED:
            raise CdcDeployError(_connector_failure_message(deployer, stack_name, connector))
        driver.log(f"{connector}: {state or 'creating'}…")
        if _deadline_passed(deadline):
            raise CdcDeployError(f"{connector} did not reach RUNNING in time.")
        driver.sleep(interval)


def _connector_failure_message(
    deployer, stack_name: Optional[str], connector: str
) -> str:
    """Best-effort actionable message for a FAILED connector (worker-log driven).

    Falls back to a plain "entered FAILED state" when no stack name is available
    or the worker log matches no known signature.
    """
    if stack_name:
        tail = deployer.connector_log_tail(stack_name, connector)
        guidance = _diagnose_connector_log(connector, tail)
        if guidance:
            return guidance
    return f"{connector} entered FAILED state."


# Monotonic deadline helpers kept tiny + injectable-friendly. Real time is fine
# here (a live deploy genuinely waits); tests pass sleep=lambda _: None and short
# timeouts, or a fake deployer that returns RUNNING immediately.
def _monotonic_deadline(seconds: float) -> float:
    import time as _time

    return _time.monotonic() + seconds


def _deadline_passed(deadline: float) -> bool:
    import time as _time

    return _time.monotonic() >= deadline


__all__ = [
    "CdcDeployError",
    "CdcStackDiscovery",
    "CdcDeployLog",
    "CdcStackDeployer",
    "build_cdc_stack_deployer",
    # stage lists (one per lifecycle operation)
    "CDC_INFRA_STAGES",
    "CDC_START_STAGES",
    "CDC_STOP_STAGES",
    "CDC_DELETE_STAGES",
    # seed helpers
    "seed_infra_stages",
    "seed_start_stages",
    "seed_stop_stages",
    "seed_delete_stages",
    # lifecycle operations (JobManager work functions)
    "run_cdc_infra_deploy",
    "run_cdc_start",
    "run_cdc_stop",
    "run_cdc_delete",
]
