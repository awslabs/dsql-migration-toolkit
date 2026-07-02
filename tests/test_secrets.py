"""Unit tests for resolving a source credential from AWS Secrets Manager.

These tests exercise :func:`resolve_source_secret` with an injected fake session
so they never reach AWS. They verify the happy path (username/password parsed
from RDS-style secret JSON), region resolution from an ARN, credential
confidentiality (Property 7: the password is wrapped in a masked
:class:`SecretValue`), and that every failure surfaces a clear, credential-free
:class:`SecretResolutionError`.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.secrets import (
    SecretProvisionError,
    SecretResolutionError,
    cdc_source_secret_name,
    delete_source_secret,
    ensure_source_secret,
    resolve_source_secret,
)


# ---------------------------------------------------------------------------
# Fakes (no real AWS)
# ---------------------------------------------------------------------------


class _FakeSecretsClient:
    """Returns a canned secret value, or raises a canned error."""

    def __init__(
        self, *, secret_string: Optional[str] = None, error: Optional[Exception] = None
    ) -> None:
        self._secret_string = secret_string
        self._error = error
        self.requested_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:  # noqa: N803
        self.requested_ids.append(SecretId)
        if self._error is not None:
            raise self._error
        return {"SecretString": self._secret_string}


class _FakeSession:
    """Records client kwargs and hands back a fixed fake client."""

    def __init__(self, client: _FakeSecretsClient) -> None:
        self._client = client
        self.client_calls: list[tuple[str, dict[str, Any]]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeSecretsClient:
        self.client_calls.append((service_name, kwargs))
        return self._client


def _factory_for(session: _FakeSession):
    """Build a session factory (matching ``build_session``) returning ``session``."""

    def factory(**_kwargs: Any) -> _FakeSession:
        return session

    return factory


def _client_error(code: str, message: str = "boom") -> Exception:
    """Build a botocore ``ClientError`` with the given error code."""
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "GetSecretValue"
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_resolve_returns_username_and_masked_password() -> None:
    client = _FakeSecretsClient(
        secret_string=json.dumps({"username": "appuser", "password": "s3cr3t"})
    )
    session = _FakeSession(client)

    username, password = resolve_source_secret(
        "my-db-secret", None, session_factory=_factory_for(session)
    )

    assert username == "appuser"
    assert isinstance(password, SecretValue)
    assert password.reveal() == "s3cr3t"
    assert client.requested_ids == ["my-db-secret"]


def test_resolve_password_is_not_exposed_in_repr() -> None:
    client = _FakeSecretsClient(
        secret_string=json.dumps({"username": "u", "password": "top-secret"})
    )
    _username, password = resolve_source_secret(
        "name", None, session_factory=_factory_for(_FakeSession(client))
    )
    assert "top-secret" not in repr(password)
    assert "top-secret" not in str(password)


def test_resolve_username_optional_returns_none() -> None:
    client = _FakeSecretsClient(secret_string=json.dumps({"password": "pw"}))
    username, password = resolve_source_secret(
        "name", None, session_factory=_factory_for(_FakeSession(client))
    )
    assert username is None
    assert password.reveal() == "pw"


def test_resolve_blank_username_treated_as_none() -> None:
    client = _FakeSecretsClient(
        secret_string=json.dumps({"username": "", "password": "pw"})
    )
    username, _password = resolve_source_secret(
        "name", None, session_factory=_factory_for(_FakeSession(client))
    )
    assert username is None


# ---------------------------------------------------------------------------
# Region resolution and profile passthrough
# ---------------------------------------------------------------------------


def test_region_parsed_from_arn() -> None:
    client = _FakeSecretsClient(secret_string=json.dumps({"password": "pw"}))
    session = _FakeSession(client)
    arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:my-db-AbCdEf"

    resolve_source_secret(arn, None, session_factory=_factory_for(session))

    service, kwargs = session.client_calls[0]
    assert service == "secretsmanager"
    assert kwargs["region_name"] == "eu-west-1"


def test_explicit_region_overrides_arn_region() -> None:
    client = _FakeSecretsClient(secret_string=json.dumps({"password": "pw"}))
    session = _FakeSession(client)
    arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:my-db-AbCdEf"

    resolve_source_secret(
        arn, None, region="us-east-1", session_factory=_factory_for(session)
    )

    _service, kwargs = session.client_calls[0]
    assert kwargs["region_name"] == "us-east-1"


def test_plain_name_leaves_region_to_session() -> None:
    client = _FakeSecretsClient(secret_string=json.dumps({"password": "pw"}))
    session = _FakeSession(client)

    resolve_source_secret("my-db", None, session_factory=_factory_for(session))

    _service, kwargs = session.client_calls[0]
    assert kwargs["region_name"] is None


# ---------------------------------------------------------------------------
# Error handling -- clear, credential-free messages (Property 7)
# ---------------------------------------------------------------------------


def test_blank_secret_id_raises() -> None:
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret("   ", None, session_factory=_factory_for(
            _FakeSession(_FakeSecretsClient())
        ))
    assert "ARN or name" in str(excinfo.value)


def test_not_found_error_message() -> None:
    client = _FakeSecretsClient(error=_client_error("ResourceNotFoundException"))
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "missing", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "not found" in str(excinfo.value).lower()


def test_access_denied_error_message() -> None:
    client = _FakeSecretsClient(error=_client_error("AccessDeniedException"))
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    message = str(excinfo.value)
    assert "Access denied" in message
    assert "secretsmanager:GetSecretValue" in message


def test_decryption_failure_error_message() -> None:
    client = _FakeSecretsClient(error=_client_error("DecryptionFailure"))
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "kms:Decrypt" in str(excinfo.value)


def test_non_json_secret_raises() -> None:
    client = _FakeSecretsClient(secret_string="not-json")
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "JSON" in str(excinfo.value)


def test_secret_without_password_raises() -> None:
    client = _FakeSecretsClient(secret_string=json.dumps({"username": "u"}))
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "password" in str(excinfo.value).lower()


def test_binary_only_secret_raises() -> None:
    client = _FakeSecretsClient(secret_string=None)
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "SecretString" in str(excinfo.value)


def test_error_message_never_leaks_secret_value() -> None:
    # Even a JSON-array (non-object) value must not echo its contents.
    client = _FakeSecretsClient(secret_string=json.dumps(["leaked-value"]))
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_source_secret(
            "name", None, session_factory=_factory_for(_FakeSession(client))
        )
    assert "leaked-value" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# cdc_source_secret_name -- deterministic, colon-free name
# ---------------------------------------------------------------------------


def test_cdc_source_secret_name_is_deterministic_and_colon_free() -> None:
    name = cdc_source_secret_name("mysql-dsql-cdc-stack")
    assert name == "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"
    # Colon-free so it works directly as the cdc-stack SourceSecretName
    # (the MSK Connect config provider's ${secretsManager:<name>:<key>} forbids
    # colons in the name).
    assert ":" not in name
    # Stable across calls -> re-deploys upsert the same secret.
    assert cdc_source_secret_name("mysql-dsql-cdc-stack") == name


# ---------------------------------------------------------------------------
# ensure_source_secret -- idempotent create/upsert, returns ARN (Property 7)
# ---------------------------------------------------------------------------


class _FakeProvisionClient:
    """Fake Secrets Manager client for create/put/describe of a managed secret.

    ``exists`` controls whether ``create_secret`` raises ResourceExistsException
    (forcing the put_secret_value + describe_secret upsert path). ``deletion_pending``
    instead makes ``create_secret`` raise the InvalidRequestException AWS returns when
    the secret is scheduled for deletion -- exercising the restore-then-upsert path.
    """

    def __init__(self, *, exists: bool = False, deletion_pending: bool = False, arn: str = "arn:aws:secretsmanager:us-east-1:111122223333:secret:mysql-dsql-migrator/cdc/s/source-AbCdEf") -> None:
        self._exists = exists
        self._deletion_pending = deletion_pending
        self._arn = arn
        self.create_calls: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self.describe_calls: list[str] = []
        self.restore_calls: list[str] = []

    def create_secret(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        if self._deletion_pending:
            raise _client_error(
                "InvalidRequestException",
                "You can't create this secret because a secret with this name is "
                "already scheduled for deletion.",
            )
        if self._exists:
            raise _client_error("ResourceExistsException", "already exists")
        return {"ARN": self._arn}

    def restore_secret(self, *, SecretId: str) -> dict[str, Any]:  # noqa: N803
        self.restore_calls.append(SecretId)
        return {"ARN": self._arn}

    def put_secret_value(self, *, SecretId: str, SecretString: str) -> dict[str, Any]:  # noqa: N803
        self.put_calls.append({"SecretId": SecretId, "SecretString": SecretString})
        return {"ARN": self._arn}

    def describe_secret(self, *, SecretId: str) -> dict[str, Any]:  # noqa: N803
        self.describe_calls.append(SecretId)
        return {"ARN": self._arn}


def _ensure(client: _FakeProvisionClient, **kw: Any) -> str:
    return ensure_source_secret(
        stack_name=kw.pop("stack_name", "mysql-dsql-cdc-stack"),
        username=kw.pop("username", "appuser"),
        password=kw.pop("password", "p@ss"),
        aws_profile=kw.pop("aws_profile", None),
        region=kw.pop("region", "us-east-1"),
        session_factory=_factory_for(_FakeSession(client)),
        **kw,
    )


def test_ensure_creates_secret_with_username_password_json() -> None:
    client = _FakeProvisionClient(exists=False)
    arn = _ensure(client, username="appuser", password="s3cr3t")

    assert arn == client._arn
    assert len(client.create_calls) == 1
    assert not client.put_calls  # create path, no upsert
    call = client.create_calls[0]
    assert call["Name"] == "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"
    body = json.loads(call["SecretString"])
    assert body == {"username": "appuser", "password": "s3cr3t"}


def test_ensure_upserts_when_secret_already_exists() -> None:
    client = _FakeProvisionClient(exists=True)
    arn = _ensure(client, password="rotated")

    assert arn == client._arn
    # Tried create, hit ResourceExistsException, then put + describe for the ARN.
    assert len(client.create_calls) == 1
    assert len(client.put_calls) == 1
    body = json.loads(client.put_calls[0]["SecretString"])
    assert body["password"] == "rotated"
    assert client.describe_calls == ["mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"]


def test_ensure_does_not_restore_when_secret_is_active() -> None:
    # A plain ResourceExistsException must NOT trigger restore_secret -- the secret
    # is active, only its value needs upserting.
    client = _FakeProvisionClient(exists=True)
    _ensure(client)
    assert client.restore_calls == []


def test_ensure_restores_secret_scheduled_for_deletion_then_upserts() -> None:
    # A prior teardown left the secret scheduled for deletion; CreateSecret fails
    # with InvalidRequestException. We must restore (cancel the pending deletion),
    # then PutSecretValue + describe for the ARN -- not fail the deploy.
    client = _FakeProvisionClient(deletion_pending=True)
    arn = _ensure(client, password="redeployed")

    assert arn == client._arn
    assert len(client.create_calls) == 1
    assert client.restore_calls == ["mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"]
    assert len(client.put_calls) == 1
    body = json.loads(client.put_calls[0]["SecretString"])
    assert body["password"] == "redeployed"
    assert client.describe_calls == ["mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"]


def test_ensure_never_leaks_password_when_restore_fails() -> None:
    # If restore fails on the deletion-pending path, the error must not echo the
    # password we were about to store.
    client = _FakeProvisionClient(deletion_pending=True)
    client.restore_secret = lambda **_kw: (_ for _ in ()).throw(  # type: ignore[assignment]
        _client_error("AccessDeniedException", "no restore perms")
    )
    with pytest.raises(SecretProvisionError) as excinfo:
        _ensure(client, password="leaky-password")
    assert "leaky-password" not in str(excinfo.value)


def test_ensure_uses_deterministic_name_per_stack() -> None:
    client = _FakeProvisionClient(exists=False)
    _ensure(client, stack_name="my-mig")
    assert client.create_calls[0]["Name"] == "mysql-dsql-migrator/cdc/my-mig/source"


def test_ensure_passes_kms_key_when_given() -> None:
    client = _FakeProvisionClient(exists=False)
    _ensure(client, kms_key_id="alias/my-cmk")
    assert client.create_calls[0]["KmsKeyId"] == "alias/my-cmk"


def test_ensure_omits_kms_key_by_default() -> None:
    # No CMK -> CreateSecret carries no KmsKeyId (uses the default managed key).
    client = _FakeProvisionClient(exists=False)
    _ensure(client)
    assert "KmsKeyId" not in client.create_calls[0]


def test_ensure_passes_region_to_client() -> None:
    client = _FakeProvisionClient(exists=False)
    session = _FakeSession(client)
    ensure_source_secret(
        stack_name="s",
        username="u",
        password="pw",
        aws_profile=None,
        region="eu-west-1",
        session_factory=_factory_for(session),
    )
    service, kwargs = session.client_calls[0]
    assert service == "secretsmanager"
    assert kwargs["region_name"] == "eu-west-1"


def test_ensure_wraps_create_error_as_provision_error() -> None:
    client = _FakeProvisionClient(exists=False)
    client.create_secret = lambda **_kw: (_ for _ in ()).throw(  # type: ignore[assignment]
        _client_error("AccessDeniedException", "nope")
    )
    with pytest.raises(SecretProvisionError) as excinfo:
        _ensure(client)
    assert "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source" in str(excinfo.value)


def test_ensure_never_leaks_password_in_error() -> None:
    # An error on the upsert path must not echo the password we tried to store.
    client = _FakeProvisionClient(exists=True)
    client.put_secret_value = lambda **_kw: (_ for _ in ()).throw(  # type: ignore[assignment]
        _client_error("InternalServiceError", "boom")
    )
    with pytest.raises(SecretProvisionError) as excinfo:
        _ensure(client, password="leaky-password")
    assert "leaky-password" not in str(excinfo.value)


def test_ensure_raises_when_no_arn_returned() -> None:
    client = _FakeProvisionClient(exists=False, arn="")
    with pytest.raises(SecretProvisionError) as excinfo:
        _ensure(client)
    assert "ARN" in str(excinfo.value)


# ---------------------------------------------------------------------------
# delete_source_secret -- teardown cleanup, idempotent, recovery window
# ---------------------------------------------------------------------------


class _FakeDeleteClient:
    """Fake Secrets Manager client for delete_secret, optionally raising."""

    def __init__(self, *, error: Optional[Exception] = None) -> None:
        self._error = error
        self.delete_calls: list[dict[str, Any]] = []

    def delete_secret(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return {"ARN": "arn:...", "Name": kwargs["SecretId"]}


def _delete(client: _FakeDeleteClient, **kw: Any) -> str:
    return delete_source_secret(
        stack_name=kw.pop("stack_name", "mysql-dsql-cdc-stack"),
        aws_profile=kw.pop("aws_profile", None),
        region=kw.pop("region", "us-east-1"),
        session_factory=_factory_for(_FakeSession(client)),
        **kw,
    )


def test_delete_schedules_deletion_with_recovery_window() -> None:
    client = _FakeDeleteClient()
    result = _delete(client)
    assert result == "deleted"
    assert len(client.delete_calls) == 1
    call = client.delete_calls[0]
    assert call["SecretId"] == "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"
    # A recovery window (soft delete) -- an accidental teardown is restorable.
    assert call["RecoveryWindowInDays"] == 7


def test_delete_missing_secret_is_absent_not_error() -> None:
    client = _FakeDeleteClient(error=_client_error("ResourceNotFoundException"))
    # Idempotent: nothing to delete (e.g. SM-auth source) is a success.
    assert _delete(client) == "absent"


def test_delete_wraps_other_errors_as_provision_error() -> None:
    client = _FakeDeleteClient(error=_client_error("AccessDeniedException", "nope"))
    with pytest.raises(SecretProvisionError) as excinfo:
        _delete(client)
    assert "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source" in str(excinfo.value)


def test_delete_passes_region_to_client() -> None:
    client = _FakeDeleteClient()
    session = _FakeSession(client)
    delete_source_secret(
        stack_name="s", aws_profile=None, region="eu-west-1",
        session_factory=_factory_for(session),
    )
    service, kwargs = session.client_calls[0]
    assert service == "secretsmanager"
    assert kwargs["region_name"] == "eu-west-1"
