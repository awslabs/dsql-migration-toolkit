# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Target Aurora DSQL connection layer (psycopg v3 + IAM token auth).

The :class:`DsqlConnector` opens ``psycopg`` (v3) connections to an Amazon
Aurora DSQL cluster. DSQL speaks the PostgreSQL wire protocol but authenticates
with a short-lived IAM auth token used in place of a password, and it requires
optimistic-concurrency (OCC) semantics, so connections are opened in
``AUTOCOMMIT`` mode over TLS.

Dependency choice: the IAM auth token is generated directly via ``boto3``'s
``dsql`` client (``generate_db_connect_admin_auth_token`` /
``generate_db_connect_auth_token``). This is the documented, dependency-light
path and avoids adding the separate "Aurora DSQL Connector for Python" package,
in line with the project's minimal-dependency principle. The token generator is
injectable so unit tests can supply a fake and never call real AWS.

Guarantees implemented here:

- IAM token auto-refresh (Requirement 5.4): the token is short-lived, so it is
  cached with its expiry and regenerated automatically when expired or within a
  small safety margin before expiry.
- AUTOCOMMIT + TLS (Requirement 5.4 / DSQL specifics): every connection is set
  to autocommit and connects with ``sslmode=require`` as the configured
  ``username``/``database``.
- Credential confidentiality (Property 7 / Requirement 9.2): the IAM token is a
  short-lived credential. It is wrapped in :class:`~dsql_migrator.config.SecretValue`
  so it is masked in reprs/logs, is never persisted in serialized config, and is
  redacted from any failure message.

OCC retry on ``SQLSTATE 40001`` is intentionally out of scope here; it is added
as a separate utility in a later subtask.
"""

from __future__ import annotations

import socket
import time
from typing import Any, Callable, Optional

from dsql_migrator.config import SecretValue
from dsql_migrator.core.aws_session import build_session
from dsql_migrator.core.models import ConnectionResult, TargetConnectionConfig

# DSQL admin database role; selects the admin token-generation API.
ADMIN_USERNAME = "admin"

# DSQL IAM auth tokens are short-lived. 900s (15 min) is the DSQL default.
DEFAULT_TOKEN_EXPIRY_SECONDS = 900

# Regenerate the token this many seconds before its nominal expiry so an
# in-flight connect never races a just-expired token.
DEFAULT_REFRESH_MARGIN_SECONDS = 60

# Bound the TCP connect so an unreachable DSQL endpoint fails fast rather than
# blocking the UI "Test connection" / prerequisite probe on the OS default.
# Mirrors the source introspector's 10s connect_timeout.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10

# Lowercase substrings of libpq/OpenSSL messages for a transient connection
# failure that carries NO SQLSTATE (the server never answered): a dropped socket,
# TLS teardown, or a connect that timed out / was rate-limited. Used only as a
# fallback when the exception TYPE was lost (a wrapped/re-raised error); a live
# psycopg OperationalError/InterfaceError is classified by type below.
TRANSIENT_CONN_SIGNATURES = (
    "ssl error",
    "unexpected eof",
    "eof detected",
    "server closed the connection",
    "connection already closed",
    "connection is closed",
    "connection is lost",
    "connection reset",
    "consuming input failed",
    "could not receive data",
    "could not send data",
    "terminating connection",
    "broken pipe",
    "no connection to the server",
    "connection not open",
    "connection timeout expired",
    "timeout expired",
    "timed out",
)


def is_transient_connection_error(exc: BaseException) -> bool:
    """True for a transient DSQL connection-level error worth retrying.

    Two recoverable shapes, both fixed by opening a FRESH connection (which
    re-mints a short-lived IAM token) and replaying the idempotent work:

    1. **SQLSTATE class ``08``** -- a connection exception the server reported
       (e.g. an expired token, admin-closed connection).
    2. **No SQLSTATE** -- the server never answered, so this is a
       connection/network failure (dropped socket, TLS teardown, connect timeout,
       unreachable address, or DSQL's new-connection rate limit rejecting the
       connect under a burst). A genuine row/constraint error ALWAYS carries a
       SQLSTATE, so a psycopg error with ``sqlstate=None`` is a connection failure.
       The exception TYPE gates this so a caller's own no-SQLSTATE structural
       error (a ``ValueError`` etc.) is NOT misclassified as retryable.

    Shared by every DSQL connect/execute path (the batched loader's pool leases
    AND the per-table DROP+recreate connect) so a connection storm -- e.g. many
    table workers opening fresh connections at once when a wave of tables finishes
    together -- is absorbed by reconnecting instead of failing the table.
    """
    state = getattr(exc, "sqlstate", None)
    if isinstance(state, str) and state.startswith("08"):
        return True
    if state is None:
        module = type(exc).__module__ or ""
        name = type(exc).__name__
        if module.startswith("psycopg") or name in ("OperationalError", "InterfaceError"):
            return True
        message = str(exc).lower()
        return any(sig in message for sig in TRANSIENT_CONN_SIGNATURES)
    return False

# A token generator takes (hostname, region, expires_in_seconds) and returns the
# token string. It is injectable so tests can supply a fake (no real AWS calls).
TokenGenerator = Callable[[str, str, int], str]

# A connect factory mirrors ``psycopg.connect`` and returns a connection object.
ConnectFactory = Callable[..., Any]

# An IPv4 resolver maps a hostname to one of its IPv4 (A-record) addresses, or
# None when it has no IPv4 / cannot be resolved. Injectable so unit tests never do
# real DNS.
Ipv4Resolver = Callable[[str], Optional[str]]


def _resolve_ipv4(host: str) -> Optional[str]:
    """Return an IPv4 (A-record) address for ``host``, or ``None``.

    Aurora DSQL endpoints are **dual-stack** (both A and AAAA records). In an
    IPv4-only VPC (e.g. an ECS task with no IPv6 egress), a reconnect that libpq
    routes to the AAAA (IPv6) address fails with ``Network is unreachable`` -- and
    when a transient DSQL event forces many reconnects at once, that can fail an
    in-flight Full Load even though IPv4 is perfectly reachable. Resolving the
    IPv4 here lets :meth:`DsqlConnector.connect` pin the TCP target to IPv4 via
    ``hostaddr`` (the DNS ``host`` is still passed for TLS SNI / cert verification),
    so every connect and reconnect stays on the reachable address family. Returns
    ``None`` (caller falls back to default host-based resolution) if there is no
    IPv4 or the lookup fails, so an IPv6-only environment is unaffected.
    """
    try:
        infos = socket.getaddrinfo(host, 5432, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        return None
    for info in infos:
        sockaddr = info[4]
        if sockaddr and sockaddr[0]:
            return sockaddr[0]
    return None


def _default_token_generator(
    username: str, aws_profile: Optional[str] = None
) -> TokenGenerator:
    """Build the default DSQL IAM token generator from the shared session.

    The ``dsql`` client is created from the single shared ``boto3.Session``
    (built via :func:`dsql_migrator.core.aws_session.build_session`, honoring the
    optional global ``aws_profile``), so DSQL token generation shares the same
    credential context as every other AWS client and there is no per-service
    credential context (Requirements 9.5, 9.7). The admin token API is used for
    the ``admin`` role and the standard db-connect token API otherwise. ``boto3``
    is imported lazily inside ``build_session``.
    """

    use_admin = username == ADMIN_USERNAME

    def generate(hostname: str, region: str, expires_in: int) -> str:
        session = build_session(aws_profile)
        client = session.client("dsql", region_name=region)
        if use_admin:
            return client.generate_db_connect_admin_auth_token(
                hostname, region, expires_in
            )
        return client.generate_db_connect_auth_token(hostname, region, expires_in)

    return generate


class _CachedToken:
    """A generated IAM token together with the clock value at which it expires."""

    __slots__ = ("secret", "refresh_at")

    def __init__(self, secret: SecretValue, refresh_at: float) -> None:
        self.secret = secret
        self.refresh_at = refresh_at


class DsqlConnector:
    """Opens autocommit, TLS psycopg connections to Aurora DSQL using IAM tokens.

    The connector caches the generated IAM token and regenerates it automatically
    when it is expired or within ``refresh_margin_seconds`` of expiry, so callers
    never have to manage token lifetime. The token value is wrapped in
    :class:`SecretValue` and is never logged or persisted in plaintext.
    """

    def __init__(
        self,
        config: TargetConnectionConfig,
        *,
        aws_profile: Optional[str] = None,
        token_generator: Optional[TokenGenerator] = None,
        connect_factory: Optional[ConnectFactory] = None,
        token_expiry_seconds: int = DEFAULT_TOKEN_EXPIRY_SECONDS,
        refresh_margin_seconds: int = DEFAULT_REFRESH_MARGIN_SECONDS,
        connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        ipv4_resolver: Optional[Ipv4Resolver] = None,
    ) -> None:
        """Create a connector.

        ``aws_profile`` is the optional single global AWS named profile applied
        when building the default ``boto3``-backed token generator, so DSQL token
        generation uses the same shared credential context as all other AWS
        clients (Requirements 9.5, 9.7); when ``None`` the standard credential
        chain is used. It is ignored when ``token_generator`` is injected.
        ``token_generator`` produces an IAM auth token for
        ``(hostname, region, expires_in)``; the default builds its ``dsql``
        client from the shared :func:`build_session`. ``connect_factory`` opens a
        connection (default ``psycopg.connect``). Both are injectable so unit
        tests never reach real AWS or a real cluster. ``clock`` supplies a
        monotonic time source for the token cache and is injectable for
        deterministic tests.
        """
        if refresh_margin_seconds >= token_expiry_seconds:
            raise ValueError(
                "refresh_margin_seconds must be smaller than token_expiry_seconds"
            )

        self._config = config
        self._token_generator = token_generator or _default_token_generator(
            config.username, aws_profile
        )
        self._connect_factory = connect_factory or _default_connect_factory()
        self._token_expiry_seconds = token_expiry_seconds
        self._refresh_margin_seconds = refresh_margin_seconds
        self._connect_timeout = connect_timeout_seconds
        self._clock = clock
        # Pin DSQL connects to IPv4 (see _resolve_ipv4) so a reconnect in an
        # IPv4-only network never routes to an unreachable IPv6 address. Injectable
        # so unit tests do no real DNS.
        self._ipv4_resolver = ipv4_resolver or _resolve_ipv4
        self._cached: Optional[_CachedToken] = None

    def _current_token(self) -> SecretValue:
        """Return a valid IAM token, regenerating it when expired/near expiry.

        The freshly generated token is cached with a refresh deadline of
        ``expiry - margin`` so it is replaced before it can actually expire
        (Requirement 5.4).
        """
        now = self._clock()
        cached = self._cached
        if cached is not None and now < cached.refresh_at:
            return cached.secret

        raw_token = self._token_generator(
            self._config.cluster_endpoint,
            self._config.region,
            self._token_expiry_seconds,
        )
        secret = SecretValue(raw_token)
        refresh_at = now + self._token_expiry_seconds - self._refresh_margin_seconds
        self._cached = _CachedToken(secret=secret, refresh_at=refresh_at)
        return secret

    def connect(self) -> Any:
        """Open an autocommit, TLS connection to DSQL using an IAM token.

        The token is supplied as the connection password. The connection is set
        to ``autocommit`` (DSQL's OCC model has no multi-statement transactions
        in the usual sense) and uses ``sslmode=require`` (DSQL mandates TLS).
        """
        token = self._current_token()
        # Pin the TCP target to IPv4 when the endpoint resolves to one: DSQL is
        # dual-stack, and in an IPv4-only network libpq may otherwise (re)connect to
        # the unreachable IPv6 address ("Network is unreachable"). ``hostaddr`` sets
        # the connect IP while ``host`` stays the DNS name for TLS SNI / certificate
        # verification. Falls back to host-only resolution when there is no IPv4.
        try:
            ipv4 = self._ipv4_resolver(self._config.cluster_endpoint)
        except Exception:  # noqa: BLE001 - resolution is best-effort; fall back
            ipv4 = None
        connect_kwargs: dict[str, Any] = dict(
            host=self._config.cluster_endpoint,
            port=5432,
            dbname=self._config.database,
            user=self._config.username,
            password=token.reveal(),
            sslmode="require",
            autocommit=True,
            # Bound the TCP connect so an unreachable endpoint (wrong host, VPC the
            # tool can't egress to, security-group filtered) fails fast instead of
            # blocking the UI "Test connection" / prerequisite probe indefinitely on
            # the OS default. Mirrors the source introspector's connect_timeout.
            connect_timeout=self._connect_timeout,
        )
        if ipv4:
            connect_kwargs["hostaddr"] = ipv4
        connection = self._connect_factory(**connect_kwargs)
        # Defensively enforce autocommit even if the driver ignored the kwarg.
        try:
            if getattr(connection, "autocommit", True) is not True:
                connection.autocommit = True
        except Exception:  # noqa: BLE001 - best-effort; connect already succeeded
            pass
        return connection

    def test_connection(self) -> ConnectionResult:
        """Validate connectivity by executing ``SELECT 1`` (Requirement 5.4).

        Returns a success/failure :class:`ConnectionResult`. On failure the
        reason is returned with the IAM token redacted (Property 7 / 9.2).
        """
        connection: Optional[Any] = None
        try:
            connection = self.connect()
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                _safe_close(cursor)
            return ConnectionResult(success=True, detail="Connection successful.")
        except Exception as exc:  # noqa: BLE001 - surfaced as a failure reason
            reason = _sanitize_message(str(exc), self._cached) or exc.__class__.__name__
            hint = _failure_hint(reason)
            detail = f"Connection failed: {reason}"
            if hint:
                detail = f"{detail} — {hint}"
            return ConnectionResult(success=False, detail=detail)
        finally:
            if connection is not None:
                _safe_close(connection)


def _default_connect_factory() -> ConnectFactory:
    """Build the default psycopg (v3) connect factory.

    ``psycopg`` is imported lazily so importing this module does not require the
    binary driver to be importable in environments that only need the class.
    """

    def connect(**kwargs: Any) -> Any:
        import psycopg  # local import: keep module import light

        return psycopg.connect(**kwargs)

    return connect


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


def _sanitize_message(message: str, cached: Optional[_CachedToken]) -> str:
    """Redact the cached IAM token from an error message (Property 7)."""
    if cached is not None:
        token = cached.secret.reveal()
        if token:
            message = message.replace(token, "***")
    return message


def _failure_hint(reason: str) -> str:
    """Classify a DSQL connection failure into one actionable next step.

    A raw psycopg/network error tells a non-expert little about whether the fix
    is IAM, networking, or the endpoint. This maps common signatures to a short,
    specific hint so the UI says *what to fix*, not just *that it failed*. Returns
    "" when the cause is unclear (the raw reason already shows).
    """
    low = reason.lower()
    # IAM / auth: PostgreSQL auth failures + AWS authorization signatures.
    if any(
        s in low
        for s in (
            "password authentication failed",
            "authentication failed",
            "permission denied",
            "accessdenied",
            "not authorized",
            "iam",
            "token",
            "28000",  # invalid authorization specification
            "28p01",  # invalid password
        )
    ):
        return (
            "looks like an IAM/auth issue — confirm your AWS identity has "
            "dsql:DbConnectAdmin on this cluster and the AWS profile/region are "
            "correct"
        )
    # Network / reachability: cannot reach the endpoint at all.
    if any(
        s in low
        for s in (
            "timeout",
            "timed out",
            "could not connect",
            "connection refused",
            "no route to host",
            "network is unreachable",
            "name or service not known",
            "could not translate host name",
            "temporary failure in name resolution",
        )
    ):
        return (
            "looks like a network/endpoint issue — check the cluster endpoint is "
            "correct and reachable (VPC egress / security group / DNS) from where "
            "this tool runs"
        )
    # TLS: DSQL requires SSL.
    if "ssl" in low or "tls" in low or "certificate" in low:
        return "looks like a TLS issue — Aurora DSQL requires an encrypted (SSL) connection"
    return ""


__all__ = [
    "DsqlConnector",
    "TokenGenerator",
    "ConnectFactory",
    "ADMIN_USERNAME",
    "DEFAULT_TOKEN_EXPIRY_SECONDS",
    "DEFAULT_REFRESH_MARGIN_SECONDS",
]
