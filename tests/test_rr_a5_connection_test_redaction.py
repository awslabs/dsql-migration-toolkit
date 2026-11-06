# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The source connection test redacts the password on the path the Connect screen runs.

Property 7: a credential never appears in plaintext in a message the tool shows or logs.
``SourceIntrospector.test_connection`` promised to redact the password from a failure
reason, but it looked the password up through ``SourceConnectionConfig.secret`` -- a
reference the Connect screen never sets. The password the screen holds goes to the engine
factory (``make_source_engine_factory``) and nowhere else, so on the real path the
redaction had nothing to redact: a driver error that echoed the credential reached the
Connect screen verbatim.

These tests drive the REAL path: ``check_source_connection`` with no injected
introspector, so the production engine factory receives the password and builds the URL
with it. Only ``create_engine`` is replaced, by a driver whose connect error echoes the
password of the URL it was given -- the case the redaction exists for.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine as _real_create_engine

import dsql_migrator.ui.connect as connect_mod
from dsql_migrator.config import SecretValue
from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.connect import build_source_config, check_source_connection

_PASSWORD = "Sup3r-Secret-Pw!"


def _leaky_create_engine(url, **_kwargs):  # noqa: ANN001, ANN202 - create_engine's shape
    """An engine whose every connect fails with an error that echoes the URL's password."""

    def creator():  # noqa: ANN202
        raise RuntimeError(
            f"access denied for user '{url.username}' using password '{url.password}' "
            f"at {url.host}:{url.port}"
        )

    return _real_create_engine("sqlite://", creator=creator)


@pytest.mark.parametrize(
    ("source_type", "port"), [(SourceType.MYSQL, 3306), (SourceType.POSTGRES, 5432)]
)
def test_the_connect_screen_source_test_redacts_a_password_the_driver_echoes(
    monkeypatch: pytest.MonkeyPatch, source_type: SourceType, port: int
) -> None:
    monkeypatch.setattr(connect_mod, "create_engine", _leaky_create_engine)
    config = build_source_config(
        host="db.example.com",
        port=port,
        database="app",
        username="reader",
        source_type=source_type,
    )

    result = check_source_connection(config, SecretValue(_PASSWORD))

    assert result.success is False
    assert _PASSWORD not in result.detail
    # The password DID reach the driver (the factory built the URL with it) and was
    # replaced in place; the rest of the reason survives for the operator to act on.
    assert "using password '***'" in result.detail
    assert "access denied for user 'reader'" in result.detail
    assert f"db.example.com:{port}" in result.detail


def test_a_source_test_without_a_password_still_reports_the_driver_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No password held (e.g. a passwordless test account): nothing to redact, and the
    # failure reason is passed through unchanged rather than blanked.
    monkeypatch.setattr(connect_mod, "create_engine", _leaky_create_engine)
    config = build_source_config(host="db.example.com", port=3306, username="reader")

    result = check_source_connection(config, None)

    assert result.success is False
    assert "access denied for user 'reader' using password 'None'" in result.detail
