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

import json
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
        return (
            "Access denied reading the secret. The AWS identity needs "
            "secretsmanager:GetSecretValue (and kms:Decrypt when the secret uses "
            "a customer-managed key)."
        )
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
    the tool never created one), or raises :class:`SecretProvisionError` on a real
    failure. Idempotent: a missing secret is a success, not an error.
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
