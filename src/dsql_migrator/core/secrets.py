# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Resolve a source database credential from AWS Secrets Manager.

The Connect screen lets a user authenticate the source database connection either
with a username and password they type, or by pointing at an AWS Secrets Manager
secret (for example the managed secret RDS/Aurora creates). This module resolves
such a secret into a ``(username, password)`` pair using the single shared
``boto3`` session, so Secrets Manager runs in the *same* credential context as
every other AWS client and honors the global AWS profile (see
:mod:`dsql_migrator.core.aws_session`).

Credential confidentiality (Property 7 / Requirement 9.2): the resolved password
is wrapped in a masked :class:`~dsql_migrator.config.SecretValue` so it never
appears in logs, reprs, or exception messages, and only the non-secret username
is returned in plaintext. The secret value is never persisted.

``boto3``/``botocore`` are imported lazily (via the shared session factory) so
importing this module needs no AWS configuration, and the session factory is
injectable so unit tests never reach AWS.
"""

from __future__ import annotations

from datetime import datetime, timezone

import json
import os
from typing import Callable, Optional

from dsql_migrator.config import SecretValue
from dsql_migrator.core.aws_session import SessionFactory, build_session

# A resolver maps (secret_id, aws_profile) -> (username, password). This is the
# injection seam the Connect screen depends on; the default implementation is
# :func:`resolve_source_secret`.
# (secret_id, aws_profile, *, region) -> (username, password). ``region`` is the
# source DB's region for resolving a bare secret NAME in the right region (a full
# ARN carries its own); it is keyword-only and optional so older 2-arg callers are
# unaffected.
SourceSecretResolver = Callable[..., "tuple[Optional[str], SecretValue]"]


class SecretResolutionError(Exception):
    """A source secret could not be resolved into a usable credential.

    The message is credential-free and actionable (what failed and the next
    step), so it is safe to show in the UI and logs (Property 7 /
    Requirement 9.2).
    """


class SecretProvisionError(Exception):
    """A tool-managed source secret could not be created/updated.

    Like :class:`SecretResolutionError`, the message is credential-free and
    actionable so it is safe to surface in the UI and logs (Property 7).
    """


def _region_from_arn(secret_id: str) -> Optional[str]:
    """Return the region embedded in a Secrets Manager ARN, or ``None``.

    A full ARN looks like ``arn:aws:secretsmanager:<region>:<account>:secret:..``
    so the region is the fourth colon-separated field. A plain secret *name*
    (not an ARN) has no embedded region and yields ``None``, in which case the
    shared session's own region applies.
    """
    parts = secret_id.split(":")
    if len(parts) >= 4 and parts[0] == "arn" and parts[3].strip():
        return parts[3].strip()
    return None


# Set by the deploy templates to the app stack's ``SourceSecretArn`` parameter --
# the ONE secret whose ``GetSecretValue`` grant was generated for this deployment.
# Empty string = the parameter was left unset, so NO secret is readable. Absent
# entirely = not a managed deployment (laptop / hand-rolled host), where ambient
# credentials decide and the app must not claim to know the grant.
GRANTED_SECRET_ENV = "DSQL_MIGRATOR_SOURCE_SECRET_ARN"


def granted_source_secret_arn() -> Optional[str]:
    """The secret ARN this deployment can read, ``""`` for none, ``None`` if unknown.

    The app cannot observe its own task role (no ``iam:Simulate*``), so the grant is
    *attested* by the template through an env var -- the same pattern as
    ``DSQL_MIGRATOR_CDC_MSK_ACCESS``. Distinguishing "granted nothing" (``""``) from
    "cannot tell" (``None``) matters: the first is a definite, actionable statement
    and the second must stay silent rather than guess.
    """
    raw = os.environ.get(GRANTED_SECRET_ENV)
    return None if raw is None else raw.strip()


def _access_denied_message(secret_id: str) -> str:
    """The AccessDenied message, aware of what THIS deployment was actually granted.

    "The AWS identity needs secretsmanager:GetSecretValue" is true and useless on a
    managed deployment: the operator cannot edit the task role by hand, because the
    grant is GENERATED from the app stack's ``SourceSecretArn`` parameter -- one ARN,
    fixed at deploy time -- while the Connect screen accepts any ARN they type. So the
    only message the app could produce sent them to IAM when the actionable fix was a
    stack parameter (or username/password auth), and it could not even say that the
    deployment had been granted NOTHING.

    ``DSQL_MIGRATOR_SOURCE_SECRET_ARN`` closes that: the template passes the granted
    ARN (empty when the parameter was left unset), exactly as
    ``DSQL_MIGRATOR_CDC_MSK_ACCESS`` attests the MSK grant the app equally cannot
    observe. Unset entirely means a laptop/EC2 run where the ambient credentials really
    are the thing to fix, so the original wording stands there.
    """
    granted = granted_source_secret_arn()
    iam_advice = (
        "The AWS identity needs secretsmanager:GetSecretValue (and kms:Decrypt when "
        "the secret uses a customer-managed key)."
    )
    if granted is None:
        # Not a managed deployment (no marker): the credentials themselves are the fix.
        return f"Access denied reading the secret. {iam_advice}"
    if not granted:
        return (
            "Access denied reading the secret: this deployment was not granted access "
            "to ANY Secrets Manager secret. Its app stack's SourceSecretArn parameter "
            "is empty, so no read permission was created. Either update the stack with "
            f"SourceSecretArn set to '{secret_id}', or connect with a username and "
            "password instead (no secret is needed for that)."
        )
    if _same_secret(granted, secret_id):
        # The right secret IS granted, so this really is an IAM/KMS problem -- most
        # likely a customer-managed KMS key whose policy omits this role.
        return (
            f"Access denied reading '{secret_id}', which this deployment IS granted. "
            f"{iam_advice} A customer-managed KMS key is the usual cause: add this "
            "task role to the key's policy."
        )
    return (
        f"Access denied reading the secret. This deployment is granted exactly one "
        f"secret -- '{granted}' -- and '{secret_id}' is a different one. Update the app "
        "stack's SourceSecretArn parameter to this secret, or connect with a username "
        "and password instead."
    )


def _same_secret(granted: str, requested: str) -> bool:
    """Do two Secrets Manager references name the same secret? Pure.

    A full ARN carries a random 6-character suffix (``...:my/secret-AbCdEf``) that the
    operator may or may not have pasted, and the Connect screen accepts a bare NAME as
    well as an ARN -- so a plain string compare would call the granted secret "a
    different one" and print advice that contradicts itself.
    """
    def _key(value: str) -> str:
        name = value.strip()
        if name.startswith("arn:"):
            name = name.split(":secret:", 1)[-1]
        # Drop Secrets Manager's 6-char uniqueness suffix when present.
        if len(name) > 7 and name[-7] == "-":
            name = name[:-7]
        return name
    return bool(granted) and _key(granted) == _key(requested)


def _friendly_error(secret_id: str, exc: Exception) -> str:
    """Map a boto3/botocore failure to a credential-free, actionable message."""
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code", "") or ""
    name = exc.__class__.__name__

    if code == "ResourceNotFoundException":
        return (
            f"Secret not found: '{secret_id}'. Check the ARN or name and that it "
            "exists in the resolved region."
        )
    if code in ("AccessDeniedException", "AccessDenied"):
        return _access_denied_message(secret_id)
    if code in ("DecryptionFailure", "DecryptionFailureException"):
        return (
            "Could not decrypt the secret. The AWS identity needs kms:Decrypt on "
            "the secret's KMS key."
        )
    if code in (
        "InvalidParameterException",
        "InvalidRequestException",
        "ValidationException",
    ):
        return (
            f"Invalid Secrets Manager request for '{secret_id}'. Check the ARN or "
            "name."
        )
    if name in ("NoCredentialsError", "PartialCredentialsError"):
        return (
            "No AWS credentials available. Configure your AWS profile or "
            "environment credentials, then try again."
        )
    if name == "ProfileNotFound":
        return (
            "The selected AWS profile was not found in ~/.aws/config. Choose a "
            "valid profile and try again."
        )
    if name in ("EndpointConnectionError", "ConnectTimeoutError"):
        return (
            "Could not reach AWS Secrets Manager. Check network/VPC connectivity "
            "and the region, then try again."
        )
    if code:
        return (
            f"Could not read the secret ({code}). Check the ARN or name, region, "
            "and permissions."
        )
    return (
        "Could not read the secret. Check the ARN or name, region, and that the "
        "AWS identity can call secretsmanager:GetSecretValue."
    )


def resolve_source_secret(
    secret_id: str,
    aws_profile: Optional[str],
    *,
    region: Optional[str] = None,
    session_factory: Optional[SessionFactory] = None,
) -> "tuple[Optional[str], SecretValue]":
    """Resolve a Secrets Manager secret into a ``(username, password)`` pair.

    ``secret_id`` is a Secrets Manager ARN or name (for example an RDS/Aurora
    managed secret). The secret's JSON value must contain a ``password`` field
    and may contain a ``username``; both are the standard fields of an RDS-style
    credential secret. The password is returned wrapped in a masked
    :class:`SecretValue`; the username is returned in plaintext when present,
    else ``None`` (the user can still type one on the form).

    The Secrets Manager client is built from the single shared ``boto3`` session
    honoring the global ``aws_profile`` (Requirement 9.5), so it shares one
    credential context with the DSQL token and Bedrock clients. The region is
    taken from ``region`` when given, else parsed from a full ARN, else left to
    the session's own region.

    Any failure (missing/blank id, AWS error, non-JSON value, or a secret with
    no password) is raised as :class:`SecretResolutionError` with a
    credential-free, actionable message (Property 7).
    """
    secret_id = (secret_id or "").strip()
    if not secret_id:
        raise SecretResolutionError(
            "Enter a Secrets Manager secret ARN or name for the source."
        )

    resolved_region = region or _region_from_arn(secret_id)
    session = build_session(aws_profile, session_factory=session_factory)
    client = session.client("secretsmanager", region_name=resolved_region)

    try:
        response = client.get_secret_value(SecretId=secret_id)
    except SecretResolutionError:
        raise
    except Exception as exc:  # noqa: BLE001  # botocore ClientError and friends
        raise SecretResolutionError(_friendly_error(secret_id, exc)) from exc

    secret_string = response.get("SecretString") if isinstance(response, dict) else None
    if not secret_string:
        raise SecretResolutionError(
            "The secret has no SecretString value. Binary secrets are not "
            "supported for database credentials; use an RDS/Aurora-style secret."
        )

    try:
        data = json.loads(secret_string)
    except (json.JSONDecodeError, TypeError) as exc:
        raise SecretResolutionError(
            "The secret value is not JSON with 'username' and 'password' fields. "
            "Use an RDS/Aurora-style secret."
        ) from exc

    if not isinstance(data, dict):
        raise SecretResolutionError(
            "The secret value is not a JSON object with 'username' and "
            "'password' fields."
        )

    password = data.get("password")
    if not isinstance(password, str) or not password:
        raise SecretResolutionError(
            "The secret JSON has no 'password' field. Use an RDS/Aurora-style "
            "secret that stores the database password."
        )

    username_raw = data.get("username")
    username = username_raw if isinstance(username_raw, str) and username_raw else None
    return username, SecretValue(password)


def cdc_source_secret_name(stack_name: str) -> str:
    """Return the deterministic Secrets Manager name for the CDC source creds.

    A slash-delimited (colon-free) name so it can be used directly as the
    cdc-stack ``SourceSecretName`` (the MSK Connect config provider's
    ``${secretsManager:<name>:<key>}`` syntax forbids colons in the name).
    Deterministic per stack so re-deploys upsert the same secret.
    """
    return f"mysql-dsql-migrator/cdc/{stack_name}/source"


def ensure_source_secret(
    *,
    stack_name: str,
    username: str,
    password: str,
    aws_profile: Optional[str],
    region: Optional[str],
    kms_key_id: Optional[str] = None,
    session_factory: Optional[SessionFactory] = None,
) -> str:
    """Create or update the tool-managed source-credentials secret; return its ARN.

    Used when the source was connected with a username/password (no Secrets
    Manager reference to reuse) but CDC needs a secret: the connector (Debezium)
    can only read source credentials from Secrets Manager, never an in-memory
    password. Stores ``{"username": ..., "password": ...}`` under the deterministic
    :func:`cdc_source_secret_name` -- ``CreateSecret`` when absent, else
    ``PutSecretValue`` (idempotent upsert). If a prior teardown left the secret
    scheduled for deletion (recovery window), it is restored first so the upsert
    succeeds instead of failing the deploy. Returns the secret ARN for the
    cdc-stack ``SourceSecretArn`` parameter.

    Encryption posture: ``kms_key_id`` (a customer-managed KMS key id/ARN/alias)
    is passed to ``CreateSecret`` for stricter key-access control and auditing of
    the production credentials; when ``None`` the secret uses the account's default
    ``aws/secretsmanager`` AWS-managed key. The key is only set at create time
    (changing the key of an existing secret is out of scope for the idempotent
    upsert). The connector's read access is least-privilege-scoped to this exact
    secret ARN by the cdc-stack ConnectorExecutionRole (``read-source-secret``).

    Property 7 trade-off (deliberate, opt-in): the credential is written to
    Secrets Manager only on a password-auth CDC deploy the user explicitly runs.
    The plaintext flows only into the SecretString here; it is never logged and
    never placed in an exception message (only the non-secret secret NAME is).
    """
    name = cdc_source_secret_name(stack_name)
    secret_string = json.dumps({"username": username, "password": password})
    try:
        client = build_session(aws_profile, session_factory=session_factory).client(
            "secretsmanager", region_name=region
        )
    except Exception as exc:  # noqa: BLE001 - build failure, credential-free
        raise SecretProvisionError(
            f"Could not reach Secrets Manager to create the source secret "
            f"'{name}': {str(exc).splitlines()[0]}"
        ) from exc

    create_kwargs: dict = {
        "Name": name,
        "SecretString": secret_string,
        "Description": "Source database credentials for the mysql-dsql-migrator CDC pipeline.",
    }
    if kms_key_id:
        create_kwargs["KmsKeyId"] = kms_key_id
    try:
        resp = client.create_secret(**create_kwargs)  # type: ignore[attr-defined]
        arn = resp.get("ARN")
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        # A prior teardown (:func:`delete_source_secret`) leaves the secret
        # scheduled for deletion within its recovery window. In that state AWS
        # rejects BOTH CreateSecret and PutSecretValue with InvalidRequestException
        # ("...already scheduled for deletion"); the secret must be restored first.
        scheduled_for_deletion = (
            "scheduled for deletion" in msg or "marked for deletion" in msg
        )
        if "ResourceExistsException" not in msg and not scheduled_for_deletion:
            raise SecretProvisionError(
                f"Could not create the source secret '{name}': "
                f"{msg.splitlines()[0]}"
            ) from exc
        # Secret already exists -> upsert its value (idempotent re-deploy). Cancel a
        # pending deletion first so PutSecretValue is accepted; restore is a no-op
        # cost on an active secret but we only call it when actually needed.
        try:
            if scheduled_for_deletion:
                client.restore_secret(SecretId=name)  # type: ignore[attr-defined]
            client.put_secret_value(  # type: ignore[attr-defined]
                SecretId=name, SecretString=secret_string
            )
            described = client.describe_secret(SecretId=name)  # type: ignore[attr-defined]
            arn = described.get("ARN")
        except Exception as exc2:  # noqa: BLE001
            raise SecretProvisionError(
                f"Could not update the existing source secret '{name}': "
                f"{str(exc2).splitlines()[0]}"
            ) from exc2

    if not arn:
        raise SecretProvisionError(
            f"Secrets Manager did not return an ARN for the source secret '{name}'."
        )
    return str(arn)


def delete_source_secret(
    *,
    stack_name: str,
    aws_profile: Optional[str],
    region: Optional[str],
    recovery_window_in_days: int = 7,
    session_factory: Optional[SessionFactory] = None,
    # Only delete a secret NOT touched since the teardown began. The teardown polls the
    # stack every 30 s while the UI re-probes every 5 s, so for up to ~30 s after the stack
    # vanishes the UI can already offer a fresh deploy -- whose ensure_source_secret upserts
    # THIS name. Without the check the still-running cleanup then scheduled the NEW
    # deployment's credentials for deletion, the deploy succeeded, and Start CDC failed
    # minutes later with the Debezium source unable to read its credentials -- reported as a
    # generic "<connector> entered FAILED state" with no self-heal (the Start pass never
    # re-upserts the secret and the connector reads it by name).
    not_modified_since: "Optional[datetime]" = None,
) -> str:
    """Delete the tool-managed source-credentials secret; return a status string.

    Counterpart to :func:`ensure_source_secret`, called on full CDC teardown so the
    production database credentials the tool stored do not linger in Secrets Manager
    after the pipeline is gone. Deletes by the deterministic
    :func:`cdc_source_secret_name` with a recovery window (a soft delete that can be
    restored within ``recovery_window_in_days``), so an accidental teardown is
    recoverable and a secret managed externally is never force-destroyed.

    Returns a short, credential-free status: ``"deleted"`` (scheduled for deletion),
    ``"absent"`` (nothing to delete -- e.g. the source used Secrets Manager auth, so
    the tool never created one), ``"skipped-modified"`` (a newer write exists, so this
    teardown does not own it -- see ``not_modified_since``), ``"skipped-unverified"``
    (ownership could not be read, so the secret is left alone), or raises
    :class:`SecretProvisionError` on a real failure. Idempotent: a missing secret is a
    success, not an error.
    """
    name = cdc_source_secret_name(stack_name)
    try:
        client = build_session(aws_profile, session_factory=session_factory).client(
            "secretsmanager", region_name=region
        )
    except Exception as exc:  # noqa: BLE001 - build failure, credential-free
        raise SecretProvisionError(
            f"Could not reach Secrets Manager to delete the source secret "
            f"'{name}': {str(exc).splitlines()[0]}"
        ) from exc

    if not_modified_since is not None:
        # FRESHNESS, not ownership: a tag or the ARN cannot discriminate here, because the
        # racing upsert is from the SAME tool for the SAME stack name -- any ownership
        # marker matches on both sides. Only "was this written after I started tearing
        # down?" separates the secret this teardown created from one a new deployment just
        # wrote. A read failure leaves the secret alone: skipping a cleanup costs a
        # lingering secret the operator can delete, while deleting the wrong one costs a
        # pipeline that cannot start and gives no reason.
        try:
            described = client.describe_secret(SecretId=name)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            if "ResourceNotFoundException" in str(exc):
                return "absent"
            return "skipped-unverified"
        changed = described.get("LastChangedDate") or described.get("CreatedDate")
        if changed is not None:
            reference = not_modified_since
            # Compare in one frame: boto3 returns tz-aware, a caller may pass naive.
            if changed.tzinfo is not None and reference.tzinfo is None:
                reference = reference.replace(tzinfo=timezone.utc)
            elif changed.tzinfo is None and reference.tzinfo is not None:
                changed = changed.replace(tzinfo=timezone.utc)
            if changed > reference:
                return "skipped-modified"

    try:
        client.delete_secret(  # type: ignore[attr-defined]
            SecretId=name, RecoveryWindowInDays=recovery_window_in_days
        )
        return "deleted"
    except Exception as exc:  # noqa: BLE001
        if "ResourceNotFoundException" in str(exc):
            return "absent"
        raise SecretProvisionError(
            f"Could not delete the source secret '{name}': "
            f"{str(exc).splitlines()[0]}"
        ) from exc


__all__ = [
    "SourceSecretResolver",
    "SecretResolutionError",
    "SecretProvisionError",
    "resolve_source_secret",
    "cdc_source_secret_name",
    "ensure_source_secret",
    "delete_source_secret",
]
