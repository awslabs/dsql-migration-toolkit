# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the shared ``boto3.Session`` factory and its wiring.

Covers Task 17.2:

- :func:`build_session` selects an explicit global profile when given a profile
  name and falls back to the standard AWS credential chain (no ``profile_name``)
  when given ``None`` (Requirements 9.5, 9.6).
- The factory returns exactly the object produced by the injected session
  factory and reads no credential values (Property 7).
- The Bedrock-runtime client and the DSQL IAM token client are both built from
  the shared session, honoring the global profile or falling back to the default
  credential chain, so there is a single credential context (Requirements 9.5,
  9.7). All AWS interaction is faked; no test reaches AWS.
"""

from __future__ import annotations

from typing import Any, Optional

from dsql_migrator.core import aws_session
from dsql_migrator.core import ai_assistant
from dsql_migrator.core import target_connection
from dsql_migrator.core.aws_session import build_session
from dsql_migrator.core.ai_assistant import (
    BEDROCK_RUNTIME_SERVICE,
    build_bedrock_runtime_client,
)
from dsql_migrator.core.models import AiAssistConfig, TargetConnectionConfig
from dsql_migrator.core.target_connection import DsqlConnector


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeClient:
    """A stand-in AWS service client that performs no work."""


class _FakeSession:
    """A fake boto3 session that records how ``client`` was called."""

    def __init__(self) -> None:
        self.client_calls: list[tuple[str, dict[str, Any]]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClient:
        self.client_calls.append((service_name, kwargs))
        return _FakeClient()


class _RecordingSessionFactory:
    """A session factory that records the kwargs it was constructed with."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.session = _FakeSession()

    def __call__(self, **kwargs: Any) -> _FakeSession:
        self.calls.append(kwargs)
        return self.session


# ---------------------------------------------------------------------------
# build_session: profile selection vs default credential-chain fallback
# ---------------------------------------------------------------------------


def test_build_session_uses_default_chain_when_profile_is_none() -> None:
    factory = _RecordingSessionFactory()

    session = build_session(None, session_factory=factory)

    # No profile_name is forwarded: the standard AWS credential chain applies.
    assert factory.calls == [{}]
    assert session is factory.session


def test_build_session_uses_default_chain_when_profile_is_blank() -> None:
    factory = _RecordingSessionFactory()

    build_session("", session_factory=factory)

    # A blank profile is treated as "not selected" -> default credential chain.
    assert factory.calls == [{}]


def test_build_session_forwards_named_profile() -> None:
    factory = _RecordingSessionFactory()

    session = build_session("myprofile", session_factory=factory)

    assert factory.calls == [{"profile_name": "myprofile"}]
    assert session is factory.session


def test_build_session_returns_factory_result() -> None:
    sentinel = _FakeSession()

    result = build_session("p", session_factory=lambda **_: sentinel)

    assert result is sentinel


def test_build_session_reads_no_credentials() -> None:
    factory = _RecordingSessionFactory()

    build_session("myprofile", session_factory=factory)

    # Only the non-secret profile name flows through; no credential values.
    (kwargs,) = factory.calls
    assert set(kwargs) == {"profile_name"}
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
    assert "aws_session_token" not in kwargs


def test_module_imports_without_boto3() -> None:
    # Importing the module and reaching this assert proves no AWS SDK import or
    # configuration is required at import time (boto3 is imported lazily).
    assert hasattr(aws_session, "build_session")


# ---------------------------------------------------------------------------
# Bedrock-runtime client is built from the shared session
# ---------------------------------------------------------------------------


def test_bedrock_client_built_from_shared_session_honoring_profile(
    monkeypatch: Any,
) -> None:
    recorded: dict[str, Optional[str]] = {}
    session = _FakeSession()

    def fake_build_session(aws_profile: Optional[str]) -> _FakeSession:
        recorded["aws_profile"] = aws_profile
        return session

    monkeypatch.setattr(ai_assistant, "build_session", fake_build_session)

    client = build_bedrock_runtime_client(
        AiAssistConfig(enabled=True, region="us-east-1"), aws_profile="myprofile"
    )

    # The Bedrock client comes from the shared session built for the global profile.
    assert recorded["aws_profile"] == "myprofile"
    assert isinstance(client, _FakeClient)
    assert session.client_calls == [
        (BEDROCK_RUNTIME_SERVICE, {"region_name": "us-east-1"})
    ]


def test_bedrock_client_falls_back_to_default_shared_session(
    monkeypatch: Any,
) -> None:
    recorded: dict[str, Optional[str]] = {}
    session = _FakeSession()

    def fake_build_session(aws_profile: Optional[str]) -> _FakeSession:
        recorded["aws_profile"] = aws_profile
        return session

    monkeypatch.setattr(ai_assistant, "build_session", fake_build_session)

    # No session and no profile: the default shared session (no profile) is used.
    build_bedrock_runtime_client(AiAssistConfig(enabled=True))

    assert recorded["aws_profile"] is None
    assert session.client_calls == [(BEDROCK_RUNTIME_SERVICE, {})]


def test_bedrock_explicit_session_takes_precedence_over_profile(
    monkeypatch: Any,
) -> None:
    def fail_build_session(_aws_profile: Optional[str]) -> Any:  # pragma: no cover
        raise AssertionError("build_session must not be called when session is given")

    monkeypatch.setattr(ai_assistant, "build_session", fail_build_session)
    injected = _FakeSession()

    build_bedrock_runtime_client(
        AiAssistConfig(enabled=True, region="eu-west-1"),
        session=injected,
        aws_profile="ignored",
    )

    assert injected.client_calls == [
        (BEDROCK_RUNTIME_SERVICE, {"region_name": "eu-west-1"})
    ]


# ---------------------------------------------------------------------------
# DSQL IAM token client is built from the shared session
# ---------------------------------------------------------------------------


def _target_config() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="my-cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
        database="postgres",
        username="admin",
    )


class _FakeDsqlClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def generate_db_connect_admin_auth_token(
        self, hostname: str, region: str, expires_in: int
    ) -> str:
        self.calls.append((hostname, region, expires_in))
        return "iam-token"


def test_dsql_token_client_built_from_shared_session_honoring_profile(
    monkeypatch: Any,
) -> None:
    recorded: dict[str, Optional[str]] = {}
    dsql_client = _FakeDsqlClient()

    class _SessionWithDsql:
        def client(self, service_name: str, **kwargs: Any) -> _FakeDsqlClient:
            assert service_name == "dsql"
            assert kwargs == {"region_name": "us-east-1"}
            return dsql_client

    def fake_build_session(aws_profile: Optional[str]) -> _SessionWithDsql:
        recorded["aws_profile"] = aws_profile
        return _SessionWithDsql()

    monkeypatch.setattr(target_connection, "build_session", fake_build_session)

    connector = DsqlConnector(_target_config(), aws_profile="myprofile")
    token = connector._current_token()  # exercises the default token generator

    assert recorded["aws_profile"] == "myprofile"
    assert token.reveal() == "iam-token"
    assert dsql_client.calls == [
        ("my-cluster.dsql.us-east-1.on.aws", "us-east-1", 900)
    ]


def test_dsql_token_client_falls_back_to_default_shared_session(
    monkeypatch: Any,
) -> None:
    recorded: dict[str, Optional[str]] = {}

    class _SessionWithDsql:
        def client(self, service_name: str, **kwargs: Any) -> _FakeDsqlClient:
            return _FakeDsqlClient()

    def fake_build_session(aws_profile: Optional[str]) -> _SessionWithDsql:
        recorded["aws_profile"] = aws_profile
        return _SessionWithDsql()

    monkeypatch.setattr(target_connection, "build_session", fake_build_session)

    connector = DsqlConnector(_target_config())
    connector._current_token()

    # No profile selected: the shared session falls back to the default chain.
    assert recorded["aws_profile"] is None


# ---------------------------------------------------------------------------
# build_assumed_role_session: sts:AssumeRole -> session from temp creds
# ---------------------------------------------------------------------------


class _FakeStsClient:
    """Records assume_role calls and returns canned temporary credentials."""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self._fail = fail

    def assume_role(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        if self._fail:
            raise RuntimeError("AccessDenied: not authorized to assume role")
        return {
            "Credentials": {
                "AccessKeyId": "AKIDTEMP",
                "SecretAccessKey": "SECRETTEMP",
                "SessionToken": "TOKENTEMP",
            }
        }


def test_build_assumed_role_session_assumes_and_builds_from_temp_creds() -> None:
    from dsql_migrator.core.aws_session import build_assumed_role_session

    sts = _FakeStsClient()
    factory = _RecordingSessionFactory()
    session = build_assumed_role_session(
        "arn:aws:iam::123456789012:role/CdcDeploy",
        sts_client=sts,
        session_factory=factory,
    )
    # STS called with the role + default session name + 1h duration.
    assert sts.calls == [
        {
            "RoleArn": "arn:aws:iam::123456789012:role/CdcDeploy",
            "RoleSessionName": "mysql-dsql-migrator-cdc-deploy",
            "DurationSeconds": 3600,
        }
    ]
    # The returned session was built from the temp credentials (no profile_name).
    assert factory.calls == [
        {
            "aws_access_key_id": "AKIDTEMP",
            "aws_secret_access_key": "SECRETTEMP",
            "aws_session_token": "TOKENTEMP",
        }
    ]
    assert session is factory.session


def test_build_assumed_role_session_custom_name_and_duration() -> None:
    from dsql_migrator.core.aws_session import build_assumed_role_session

    sts = _FakeStsClient()
    build_assumed_role_session(
        "arn:aws:iam::1:role/R",
        role_session_name="custom",
        duration_seconds=900,
        sts_client=sts,
        session_factory=_RecordingSessionFactory(),
    )
    assert sts.calls[0]["RoleSessionName"] == "custom"
    assert sts.calls[0]["DurationSeconds"] == 900


def test_build_assumed_role_session_raises_typed_error_on_failure() -> None:
    from dsql_migrator.core.aws_session import (
        AssumeRoleError,
        build_assumed_role_session,
    )

    sts = _FakeStsClient(fail=True)
    try:
        build_assumed_role_session(
            "arn:aws:iam::1:role/R",
            sts_client=sts,
            session_factory=_RecordingSessionFactory(),
        )
        raise AssertionError("expected AssumeRoleError")
    except AssumeRoleError as exc:
        # The role ARN is surfaced; no temporary credential value leaks.
        assert "arn:aws:iam::1:role/R" in str(exc)
        assert "AccessDenied" in str(exc)
        assert "AKIDTEMP" not in str(exc)


def test_build_assumed_role_session_builds_sts_from_profile_when_not_injected() -> None:
    from dsql_migrator.core.aws_session import build_assumed_role_session

    sts = _FakeStsClient()

    class _StsSession:
        def client(self, service_name: str, **kwargs: Any) -> Any:
            assert service_name == "sts"
            return sts

    calls: list[dict[str, Any]] = []

    def factory(**kwargs: Any):
        calls.append(kwargs)
        # First call (no creds kwargs) builds the STS session; second builds the
        # assumed-role session from the temp creds.
        if "aws_session_token" not in kwargs:
            return _StsSession()
        return _FakeSession()

    build_assumed_role_session(
        "arn:aws:iam::1:role/R", aws_profile="myprof", session_factory=factory
    )
    # The STS session honored the profile.
    assert calls[0] == {"profile_name": "myprof"}
    # The assumed-role session used the temp creds.
    assert calls[1]["aws_session_token"] == "TOKENTEMP"
    assert sts.calls  # assume_role was reached


# ---------------------------------------------------------------------------
# ensure_default_region (Fargate region floor)
# ---------------------------------------------------------------------------


def test_ensure_default_region_seeds_when_unset() -> None:
    env: dict[str, str] = {}
    result = aws_session.ensure_default_region("us-east-1", env=env)
    assert result == "us-east-1"
    assert env["AWS_DEFAULT_REGION"] == "us-east-1"


def test_ensure_default_region_respects_existing_aws_region() -> None:
    env = {"AWS_REGION": "eu-west-1"}
    result = aws_session.ensure_default_region("us-east-1", env=env)
    # Pre-existing region wins; we never override it.
    assert result == "eu-west-1"
    assert "AWS_DEFAULT_REGION" not in env


def test_ensure_default_region_respects_existing_default_region() -> None:
    env = {"AWS_DEFAULT_REGION": "ap-northeast-2"}
    result = aws_session.ensure_default_region("us-east-1", env=env)
    assert result == "ap-northeast-2"
    assert env["AWS_DEFAULT_REGION"] == "ap-northeast-2"


def test_ensure_default_region_noop_when_nothing_to_set() -> None:
    env: dict[str, str] = {}
    assert aws_session.ensure_default_region(None, env=env) is None
    assert aws_session.ensure_default_region("   ", env=env) is None
    assert "AWS_DEFAULT_REGION" not in env
