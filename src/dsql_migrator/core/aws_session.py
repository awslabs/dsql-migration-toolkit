"""Shared ``boto3.Session`` factory for a single AWS credential context.

Every AWS client in the tool (DSQL IAM token generation, Secrets Manager
``GetSecretValue``, and the Bedrock-runtime ``InvokeModel`` client) is created
from one shared ``boto3.Session`` built here. Sharing a single session gives all
AWS calls **one credential context** and prevents per-service / Bedrock-only
credential contexts -- i.e. the "DSQL token uses profile A, Bedrock uses profile
B" mismatch trap (Requirements 9.5, 9.7).

The session honors an optional single global AWS named profile
(:attr:`dsql_migrator.config.AppConfig.aws_profile`): when a profile name is
provided, ``boto3.Session(profile_name=...)`` is used; when it is ``None`` the
standard AWS credential chain (default chain + ``AWS_PROFILE``) applies
(Requirements 9.5, 9.6). Cross-account access for the app's normal operation
stays a concern of the profile's own ``~/.aws/config`` (``role_arn`` /
``source_profile``).

One deliberate exception: :func:`build_assumed_role_session` performs an explicit
``sts:AssumeRole`` so the CDC deploy can run privileged CloudFormation/MSK/IAM
operations under a dedicated, least-privilege role instead of granting those
broad permissions to the long-running web app's own (task) role. It is opt-in --
used only when a CDC deploy-role ARN is configured -- and builds the assumed-role
session from the same injectable factory, so the rest of the credential model is
unchanged.

Credential confidentiality (Property 7): only the non-secret profile *name* ever
flows through this module. No credential value is read, stored, or logged.

``boto3`` is imported lazily (mirroring the convention in ``target_connection``
and ``ai_assistant``) so importing this module needs no AWS configuration, and
the underlying session constructor is injectable so unit tests can supply a fake
and never reach AWS.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Optional, Protocol


class BotoSessionLike(Protocol):
    """Minimal structural seam for a ``boto3.Session``.

    Callers only ever create clients from the shared session (DSQL, Secrets
    Manager, Bedrock-runtime), so depending on ``client`` alone lets a fake stand
    in for a real ``boto3.Session`` in tests without importing ``boto3``.
    """

    def client(self, service_name: str, **kwargs: Any) -> Any:
        """Return a service client for ``service_name``."""


# A session factory mirrors the ``boto3.Session`` constructor: called with no
# arguments for the standard credential chain, or with ``profile_name=`` for a
# named profile. It is injectable so tests pass a fake and never touch AWS.
SessionFactory = Callable[..., BotoSessionLike]


def _default_session_factory() -> SessionFactory:
    """Return the real ``boto3.Session`` constructor, imported lazily.

    ``boto3`` is imported here rather than at module import time so importing
    this module does not require AWS configuration to be present.
    """
    import boto3  # local import: avoid requiring AWS config at import time

    return boto3.Session


def build_session(
    aws_profile: Optional[str],
    *,
    session_factory: Optional[SessionFactory] = None,
) -> BotoSessionLike:
    """Build the single shared ``boto3.Session`` honoring an optional profile.

    When ``aws_profile`` is a non-empty profile name the session is created with
    ``profile_name=aws_profile``; otherwise it is created with no arguments so
    the standard AWS credential chain (default chain + ``AWS_PROFILE``) applies
    (Requirements 9.5, 9.6). Only the non-secret profile name is used; no
    credential value is read or stored (Property 7).

    ``session_factory`` is the dependency-injection seam (defaults to the real
    ``boto3.Session`` constructor, resolved lazily): tests pass a fake to record
    the call and avoid reaching AWS. The returned object is exactly whatever the
    factory produced.
    """
    factory = (
        session_factory if session_factory is not None else _default_session_factory()
    )
    if aws_profile:
        return factory(profile_name=aws_profile)
    return factory()


def ensure_default_region(
    aws_region: Optional[str], *, env: Optional[dict] = None
) -> Optional[str]:
    """Seed a default AWS region into the environment if none is already set.

    On AWS Fargate the ECS task gets credentials from the container credential
    provider but **no region** (no ``~/.aws/config``, no ``AWS_REGION`` in the
    task env unless we set it). Any boto3 client built without an explicit
    ``region_name`` then raises ``NoRegionError`` -- notably the AI-assist
    Bedrock-runtime client when ``BEDROCK_REGION`` is blank (it falls back to the
    session's region, which is unset on Fargate).

    The CloudFormation task definition injects ``DSQL_MIGRATOR_AWS_REGION`` =
    ``${AWS::Region}`` (loaded into ``config.aws_region``), but nothing consumed
    it. This makes it load-bearing: at startup we copy it into
    ``AWS_DEFAULT_REGION`` (only when neither ``AWS_REGION`` nor
    ``AWS_DEFAULT_REGION`` is already set), so every region-less boto3 client in
    the process has a sane default. Clients that derive a region explicitly
    (DSQL token / Secrets / CDC deploy parse it from the endpoint) are unaffected;
    this only supplies a floor for the ones that don't.

    Returns the region that was set (or the pre-existing one), or ``None`` when
    there was nothing to set. ``env`` is the injection seam for tests; it defaults
    to ``os.environ``.
    """
    environ = os.environ if env is None else env
    existing = environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION")
    if existing:
        return existing
    region = (aws_region or "").strip()
    if not region:
        return None
    environ["AWS_DEFAULT_REGION"] = region
    return region


class AssumeRoleError(RuntimeError):
    """An ``sts:AssumeRole`` for the CDC deploy role failed."""


def build_assumed_role_session(
    role_arn: str,
    *,
    role_session_name: str = "mysql-dsql-migrator-cdc-deploy",
    duration_seconds: int = 3600,
    sts_client: Optional[Any] = None,
    aws_profile: Optional[str] = None,
    region: Optional[str] = None,
    session_factory: Optional[SessionFactory] = None,
) -> BotoSessionLike:
    """Return a session whose credentials are from assuming ``role_arn``.

    Calls ``sts:AssumeRole`` with the caller's base credentials (the task role /
    profile) and builds a new :class:`BotoSessionLike` from the returned temporary
    credentials, so downstream clients act AS the assumed role. Used to run the
    privileged cdc-stack CloudFormation operations under a dedicated deploy role
    rather than on the long-running app's own identity.

    ``sts_client`` is the test seam: when ``None`` an STS client is built from
    ``build_session(aws_profile, session_factory=...)``. ``session_factory`` is
    forwarded to both that STS-client build and the final session construction
    from the temporary credentials, so a fake factory + fake STS keep this fully
    unit-testable without reaching AWS.

    Each call performs a FRESH ``AssumeRole``; the returned session's temporary
    credentials expire after ``duration_seconds`` (default 1 h, ample for a single
    ~20 min cdc-stack create). Do NOT cache the returned session across operations.

    Credential confidentiality (Property 7): the temporary credential values flow
    only into the factory call; they are never stored on an attribute or logged.
    Only the non-secret ``role_arn`` appears in any error message. Raises
    :class:`AssumeRoleError` if the AssumeRole call fails.
    """
    if sts_client is None:
        # Pin the STS client to the deploy region. The global STS endpoint works in
        # most accounts, but accounts/partitions that enforce regional STS endpoints
        # (sts_regional_endpoints=regional, or the global endpoint disabled) would
        # fail an unregioned client -- so thread the region like every other client.
        sts_client = build_session(
            aws_profile, session_factory=session_factory
        ).client("sts", region_name=region)
    try:
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            DurationSeconds=duration_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - surface as a typed error
        raise AssumeRoleError(
            f"Could not assume the CDC deploy role {role_arn}: "
            f"{str(exc).splitlines()[0]}"
        ) from exc
    creds = (response or {}).get("Credentials") or {}
    factory = (
        session_factory if session_factory is not None else _default_session_factory()
    )
    return factory(
        aws_access_key_id=creds.get("AccessKeyId"),
        aws_secret_access_key=creds.get("SecretAccessKey"),
        aws_session_token=creds.get("SessionToken"),
    )


__all__ = [
    "BotoSessionLike",
    "SessionFactory",
    "build_session",
    "ensure_default_region",
    "AssumeRoleError",
    "build_assumed_role_session",
]
