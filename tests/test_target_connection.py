# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the target Aurora DSQL connection layer.

Covers:
- IAM token generation: the token is generated and used as the connection
  password (Requirement 5.4).
- Token caching/auto-refresh: a valid token is reused; an expired/near-expiry
  token is regenerated automatically (Requirement 5.4).
- DSQL connection specifics: connections are autocommit and use
  ``sslmode=require`` as the configured username/database (Requirement 5.4).
- Credential confidentiality: the IAM token is never exposed in plaintext via
  repr/str and is redacted from failure messages (Property 7 / Requirement 9.2).
- ``test_connection`` returns success/failure ``ConnectionResult`` appropriately.
"""

from __future__ import annotations

from typing import Any

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.models import ConnectionResult, TargetConnectionConfig
from dsql_migrator.core.target_connection import (
    DEFAULT_REFRESH_MARGIN_SECONDS,
    DEFAULT_TOKEN_EXPIRY_SECONDS,
    DsqlConnector,
    target_error_hint,
)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _RecordingTokenGenerator:
    """A fake token generator that records calls and returns sequential tokens."""

    def __init__(self, prefix: str = "iam-token") -> None:
        self.prefix = prefix
        self.calls: list[tuple[str, str, int]] = []

    def __call__(self, hostname: str, region: str, expires_in: int) -> str:
        self.calls.append((hostname, region, expires_in))
        return f"{self.prefix}-{len(self.calls)}"


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection
        self.closed = False

    def execute(self, statement: str, *_: Any) -> None:
        self._connection.executed.append(statement)

    def fetchone(self) -> tuple[int]:
        return (1,)

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    """A minimal psycopg-like connection capturing the kwargs it was built with."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.autocommit = bool(kwargs.get("autocommit", False))
        self.executed: list[str] = []
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class _ConnectRecorder:
    """A connect factory that records connection kwargs and returns fakes."""

    def __init__(self) -> None:
        self.connections: list[_FakeConnection] = []

    def __call__(self, **kwargs: Any) -> _FakeConnection:
        connection = _FakeConnection(**kwargs)
        self.connections.append(connection)
        return connection


class _ManualClock:
    """A controllable monotonic clock for deterministic cache tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _target_config() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="my-cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
        database="postgres",
        username="admin",
    )


def _connector(
    *,
    generator: _RecordingTokenGenerator | None = None,
    connect: _ConnectRecorder | None = None,
    clock: _ManualClock | None = None,
    config: TargetConnectionConfig | None = None,
    ipv4_resolver=None,
) -> DsqlConnector:
    return DsqlConnector(
        config or _target_config(),
        token_generator=generator or _RecordingTokenGenerator(),
        connect_factory=connect or _ConnectRecorder(),
        clock=clock or _ManualClock(),
        # Default to a no-IPv4 resolver so unit tests do NO real DNS (host-only
        # connect, the pre-pinning behavior). Specific tests inject an IPv4.
        ipv4_resolver=ipv4_resolver if ipv4_resolver is not None else (lambda _host: None),
    )


# ---------------------------------------------------------------------------
# Token generation and use as password
# ---------------------------------------------------------------------------


def test_connect_uses_generated_token_as_password() -> None:
    generator = _RecordingTokenGenerator()
    connect = _ConnectRecorder()
    connector = _connector(generator=generator, connect=connect)

    connector.connect()

    assert len(generator.calls) == 1
    hostname, region, expires_in = generator.calls[0]
    assert hostname == "my-cluster.dsql.us-east-1.on.aws"
    assert region == "us-east-1"
    assert expires_in == DEFAULT_TOKEN_EXPIRY_SECONDS

    kwargs = connect.connections[0].kwargs
    assert kwargs["password"] == "iam-token-1"
    assert kwargs["user"] == "admin"
    assert kwargs["dbname"] == "postgres"
    assert kwargs["host"] == "my-cluster.dsql.us-east-1.on.aws"


def test_connect_pins_ipv4_via_hostaddr_when_resolvable() -> None:
    # DSQL is dual-stack; in an IPv4-only network a reconnect to the IPv6 address
    # fails ("Network is unreachable"). The connector pins the TCP target to the
    # resolved IPv4 via `hostaddr`, while `host` stays the DNS name for TLS SNI.
    connect = _ConnectRecorder()
    connector = _connector(connect=connect, ipv4_resolver=lambda _host: "10.1.2.3")
    connector.connect()
    kwargs = connect.connections[0].kwargs
    assert kwargs["hostaddr"] == "10.1.2.3"        # TCP target pinned to IPv4
    assert kwargs["host"] == "my-cluster.dsql.us-east-1.on.aws"  # SNI/cert unchanged


def test_connect_falls_back_to_host_when_no_ipv4() -> None:
    # No IPv4 resolvable (e.g. IPv6-only env) -> no hostaddr, default host resolution.
    connect = _ConnectRecorder()
    connector = _connector(connect=connect, ipv4_resolver=lambda _host: None)
    connector.connect()
    kwargs = connect.connections[0].kwargs
    assert "hostaddr" not in kwargs
    assert kwargs["host"] == "my-cluster.dsql.us-east-1.on.aws"


def test_connect_passes_a_bounded_connect_timeout() -> None:
    # An unreachable endpoint must fail fast, not block the UI test on the OS
    # default; the connector passes a bounded connect_timeout to psycopg.
    connect = _ConnectRecorder()
    _connector(connect=connect).connect()
    kwargs = connect.connections[0].kwargs
    assert "connect_timeout" in kwargs
    assert isinstance(kwargs["connect_timeout"], int) and kwargs["connect_timeout"] > 0


def test_connect_pins_output_formatting_gucs_matching_source() -> None:
    # The DSQL target connection pins the output-formatting GUCs the checksum render
    # depends on (TimeZone / DateStyle / IntervalStyle), matching the PostgreSQL SOURCE's
    # pins, so a byte-identical value renders identically regardless of any role/cluster
    # default.
    #
    # It must NOT pin lc_numeric: Aurora DSQL REJECTS lc_numeric as a startup/session GUC
    # ("FATAL: setting configuration parameter \"lc_numeric\" not supported"), so passing
    # it makes EVERY DSQL connection fail (regressed in v0.1.435, live-verified & fixed in
    # v0.1.438). It is also unnecessary -- the checksum's numeric mask uses a literal '.'
    # (not to_char's locale-aware 'D'), so the decimal point agrees without it.
    connect = _ConnectRecorder()
    _connector(connect=connect).connect()
    options = connect.connections[0].kwargs["options"]
    assert "-c TimeZone=UTC" in options
    assert "-c DateStyle=ISO" in options
    assert "-c IntervalStyle=postgres" in options
    assert "lc_numeric" not in options


def test_admin_username_selects_admin_token_api() -> None:
    captured: dict[str, Any] = {}

    class _FakeDsqlClient:
        def generate_db_connect_admin_auth_token(self, hostname, region, expires_in):
            captured["api"] = "admin"
            return "admin-token"

        def generate_db_connect_auth_token(self, hostname, region, expires_in):
            captured["api"] = "standard"
            return "standard-token"

    class _FakeSession:
        @staticmethod
        def client(service_name, region_name=None):
            assert service_name == "dsql"
            return _FakeDsqlClient()

    class _FakeBoto3:
        @staticmethod
        def Session(profile_name=None):
            # The default token generator builds its dsql client from the shared
            # boto3.Session; no profile is selected here (default credential chain).
            assert profile_name is None
            return _FakeSession()

    # Inject a fake boto3 so the lazy ``import boto3`` resolves to the fake.
    import sys

    original = sys.modules.get("boto3")
    sys.modules["boto3"] = _FakeBoto3  # type: ignore[assignment]
    try:
        connect = _ConnectRecorder()
        admin = DsqlConnector(
            _target_config(), connect_factory=connect, clock=_ManualClock()
        )
        admin.connect()
        assert captured["api"] == "admin"

        standard_config = TargetConnectionConfig(
            cluster_endpoint="c.example.aws",
            region="us-east-1",
            username="app_user",
        )
        standard = DsqlConnector(
            standard_config, connect_factory=_ConnectRecorder(), clock=_ManualClock()
        )
        standard.connect()
        assert captured["api"] == "standard"
    finally:
        if original is not None:
            sys.modules["boto3"] = original
        else:
            sys.modules.pop("boto3", None)


# ---------------------------------------------------------------------------
# Token caching and auto-refresh
# ---------------------------------------------------------------------------


def test_token_is_cached_and_reused_while_valid() -> None:
    generator = _RecordingTokenGenerator()
    clock = _ManualClock()
    connector = _connector(generator=generator, clock=clock)

    connector.connect()
    # Advance, but stay within the valid window (before expiry - margin).
    clock.advance(DEFAULT_TOKEN_EXPIRY_SECONDS - DEFAULT_REFRESH_MARGIN_SECONDS - 1)
    connector.connect()

    assert len(generator.calls) == 1  # token regenerated only once


def test_token_is_regenerated_when_near_expiry() -> None:
    generator = _RecordingTokenGenerator()
    connect = _ConnectRecorder()
    clock = _ManualClock()
    connector = _connector(generator=generator, connect=connect, clock=clock)

    connector.connect()
    # Advance to exactly the refresh deadline (expiry - margin): must refresh.
    clock.advance(DEFAULT_TOKEN_EXPIRY_SECONDS - DEFAULT_REFRESH_MARGIN_SECONDS)
    connector.connect()

    assert len(generator.calls) == 2
    assert connect.connections[0].kwargs["password"] == "iam-token-1"
    assert connect.connections[1].kwargs["password"] == "iam-token-2"


def test_token_is_regenerated_after_full_expiry() -> None:
    generator = _RecordingTokenGenerator()
    clock = _ManualClock()
    connector = _connector(generator=generator, clock=clock)

    connector.connect()
    clock.advance(DEFAULT_TOKEN_EXPIRY_SECONDS + 10)
    connector.connect()

    assert len(generator.calls) == 2


# ---------------------------------------------------------------------------
# DSQL connection specifics: autocommit + TLS
# ---------------------------------------------------------------------------


def test_connection_is_autocommit_and_requires_tls() -> None:
    connect = _ConnectRecorder()
    connector = _connector(connect=connect)

    connection = connector.connect()

    kwargs = connect.connections[0].kwargs
    assert kwargs["autocommit"] is True
    assert kwargs["sslmode"] == "require"
    assert connection.autocommit is True


def test_connection_autocommit_is_enforced_when_driver_ignores_kwarg() -> None:
    class _NonAutocommitConnection(_FakeConnection):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            # Simulate a driver that did not honor the autocommit kwarg.
            self.autocommit = False

    def connect(**kwargs: Any) -> _NonAutocommitConnection:
        return _NonAutocommitConnection(**kwargs)

    connector = DsqlConnector(
        _target_config(),
        token_generator=_RecordingTokenGenerator(),
        connect_factory=connect,
        clock=_ManualClock(),
    )
    connection = connector.connect()
    assert connection.autocommit is True


# ---------------------------------------------------------------------------
# Credential confidentiality (Property 7)
# ---------------------------------------------------------------------------


def test_token_is_never_exposed_in_plaintext() -> None:
    secret = SecretValue("super-secret-iam-token")
    assert "super-secret-iam-token" not in repr(secret)
    assert "super-secret-iam-token" not in str(secret)
    assert secret.reveal() == "super-secret-iam-token"


def test_failure_message_redacts_the_token() -> None:
    token_value = "leaky-iam-token-value"

    def generator(hostname: str, region: str, expires_in: int) -> str:
        return token_value

    def connect(**kwargs: Any) -> Any:
        # Simulate a driver error that echoes the password (token) in its text.
        raise RuntimeError(f"auth failed using password {kwargs['password']}")

    connector = DsqlConnector(
        _target_config(),
        token_generator=generator,
        connect_factory=connect,
        clock=_ManualClock(),
    )
    result = connector.test_connection()

    assert result.success is False
    assert token_value not in result.detail
    assert "***" in result.detail


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success_runs_select_1_and_closes() -> None:
    connect = _ConnectRecorder()
    connector = _connector(connect=connect)

    result = connector.test_connection()

    assert isinstance(result, ConnectionResult)
    assert result.success is True
    assert "successful" in result.detail.lower()

    connection = connect.connections[0]
    assert connection.executed == ["SELECT 1"]
    assert connection.closed is True


def test_test_connection_failure_returns_reason() -> None:
    def connect(**kwargs: Any) -> Any:
        raise RuntimeError("could not reach DSQL endpoint")

    connector = DsqlConnector(
        _target_config(),
        token_generator=_RecordingTokenGenerator(),
        connect_factory=connect,
        clock=_ManualClock(),
    )
    result = connector.test_connection()

    assert result.success is False
    assert "Connection failed" in result.detail
    assert "could not reach DSQL endpoint" in result.detail


def _fail_with(message: str) -> "ConnectionResult":
    def connect(**kwargs: Any) -> Any:
        raise RuntimeError(message)

    connector = DsqlConnector(
        _target_config(),
        token_generator=_RecordingTokenGenerator(),
        connect_factory=connect,
        clock=_ManualClock(),
    )
    return connector.test_connection()


def test_failure_hint_iam_for_auth_errors() -> None:
    result = _fail_with("FATAL: password authentication failed for user admin")
    assert result.success is False
    # The raw reason is preserved AND an actionable IAM hint is appended.
    assert "password authentication failed" in result.detail
    assert "IAM/auth" in result.detail
    assert "dsql:DbConnectAdmin" in result.detail


def test_failure_hint_network_for_unreachable_endpoint() -> None:
    result = _fail_with('could not translate host name "bad.dsql" to address')
    assert "network/endpoint" in result.detail
    assert "reachable" in result.detail


def test_failure_hint_tls_for_ssl_errors() -> None:
    result = _fail_with("server does not support SSL, but SSL was required")
    assert "TLS" in result.detail


def test_failure_hint_absent_for_unclassifiable_error() -> None:
    # An opaque error gets no hint -- just the raw reason (no misleading guess).
    result = _fail_with("some entirely unexpected internal error xyz")
    assert "Connection failed: some entirely unexpected internal error xyz" in result.detail
    assert "—" not in result.detail  # no hint separator appended


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_refresh_margin_must_be_smaller_than_expiry() -> None:
    with pytest.raises(ValueError):
        DsqlConnector(
            _target_config(),
            token_generator=_RecordingTokenGenerator(),
            connect_factory=_ConnectRecorder(),
            token_expiry_seconds=60,
            refresh_margin_seconds=60,
        )


# ---------------------------------------------------------------------------
# target_error_hint — actionable hint for a DSQL-side load/DDL failure (audit U6)
# ---------------------------------------------------------------------------


class _TargetError(Exception):
    """A driver-like error exposing an optional ``sqlstate`` (like psycopg)."""

    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def test_target_error_hint_keys_off_sqlstate() -> None:
    assert "optimistic-concurrency" in target_error_hint(_TargetError("x", "40001"))
    assert "per-table limit" in target_error_hint(_TargetError("x", "54000"))
    assert "target constraint" in target_error_hint(_TargetError("x", "23505"))
    assert "could not be stored" in target_error_hint(_TargetError("x", "22001"))


def test_target_error_hint_falls_back_to_message_text() -> None:
    # A RuntimeError wrapping the sanitized first_error has no sqlstate attr, so the
    # hint must also recognize the text.
    assert "target constraint" in target_error_hint(
        RuntimeError("1 batch(es) failed loading 't': duplicate key value violates "
                     "unique constraint \"u\"")
    )
    assert "optimistic-concurrency" in target_error_hint(RuntimeError("OC001 conflict"))


def test_target_error_hint_none_for_unrecognized() -> None:
    assert target_error_hint(_TargetError("some opaque failure")) is None
    assert target_error_hint(RuntimeError("nothing recognizable here")) is None
