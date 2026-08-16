# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Aurora DSQL CDC-stack CloudFormation deployer (extracted from ``cdc_deployer.py``).

:class:`CdcStackDeployer` is the stateful boto3 wrapper over the cdc-stack's
CloudFormation / MSK / EC2 API surface -- discover / create / update / delete /
poll, ENI recovery, connector state + log tail -- plus its data types
(:class:`CdcStackDiscovery`, :class:`CdcDeployError`), the
:func:`build_cdc_stack_deployer` factory, and the ``_parse_unsupported_azs`` /
``_stack_absent_error`` helpers it uses. The ``run_cdc_*`` deploy ORCHESTRATION
stays in ``cdc_deployer.py`` and depends on this module one-directionally (this
module never imports the orchestration).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from dsql_migrator.core.aws_session import (
    BotoSessionLike,
    build_assumed_role_session,
    build_session,
)
from dsql_migrator.core.cdc import CDC_PLACEHOLDER_PREFIX, CDC_STACK_NAME_PREFIX

# Stack statuses from which an UpdateStack can safely start. Anything in an
# *_IN_PROGRESS / ROLLBACK / FAILED state means a deploy is unsafe right now.
_STABLE_STACK_STATES = frozenset(
    {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "IMPORT_COMPLETE"}
)


def _stack_absent_error(exc: Exception) -> bool:
    """True when a ``describe_stacks`` failure means the stack does not EXIST (as
    opposed to a read/credential error).

    CloudFormation raises a ``ValidationError`` whose message is ``Stack with id
    <name> does not exist`` when the named stack is gone. The Delete path treats that
    as a successful vanish, so it must be told apart from a genuine read error
    (ExpiredToken / throttle / access lost), which has to surface rather than read as
    "the stack is gone" or "still in progress"."""
    response = getattr(exc, "response", None)
    code = ""
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
    return "does not exist" in (code + " " + str(exc)).lower()


class CdcDeployError(RuntimeError):
    """A deploy precondition failed (bad stack state, unfilled placeholder, …)."""


@dataclass(frozen=True)
class CdcStackDiscovery:
    """Read-only facts about the target cdc-stack gathered before an update."""

    stack_status: str
    current_parameters: dict[str, str]
    is_stable: bool


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

    @property
    def region(self) -> str:
        """The AWS region this deployer targets (used by the in-process seed)."""
        return self._region

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

    def stack_status_checked(self, stack_name: str) -> Optional[str]:
        """Return the stack's current StackStatus, RAISING on a read error.

        Returns ``None`` ONLY when the stack genuinely does not exist (a
        CloudFormation ``ValidationError`` "… does not exist"). Any OTHER failure --
        an expired/now-unauthorized credential, throttling, a transient network error
        -- is RAISED so a wait loop can classify it (fail fast on a terminal auth
        error, tolerate a few transient ones) instead of collapsing it to ``None``
        and mistaking it for "still in progress". Mirrors :meth:`connector_state`,
        which raises for the same reason; :meth:`stack_status` wraps this and swallows
        for best-effort callers."""
        client = self._client("cloudformation")
        try:
            stacks = client.describe_stacks(StackName=stack_name).get("Stacks", [])
        except Exception as exc:  # noqa: BLE001 - distinguish "gone" from "read error"
            if _stack_absent_error(exc):
                return None  # genuinely gone (a Delete-path vanish is a success)
            raise
        return str(stacks[0].get("StackStatus")) if stacks else None

    def stack_status(self, stack_name: str) -> Optional[str]:
        """Return the stack's current StackStatus, or ``None`` on ANY error.

        Best-effort snapshot for callers that only want the status if it is cheaply
        readable (fast-path skips). A wait loop that must tell a terminal credential
        error apart from "still in progress" uses :meth:`stack_status_checked`."""
        try:
            return self.stack_status_checked(stack_name)
        except Exception:  # noqa: BLE001 - best-effort snapshot
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
