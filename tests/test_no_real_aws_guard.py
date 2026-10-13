# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The suite-wide guard in tests/conftest.py: no unit test reaches real AWS."""

import os
import sys

import pytest

from dsql_migrator.core import aws_session
from dsql_migrator.core.aws_session import build_session
from dsql_migrator.core.models import TargetConnectionConfig
from dsql_migrator.core.target_connection import DsqlConnector


def test_the_guard_refuses_a_real_send_and_never_resolves_this_shells_credentials(
    monkeypatch, tmp_path, no_real_aws
):
    """Every session the tool builds holds the guard's dummy credentials, whatever the shell
    exports, and every real call stops before the HTTP layer, naming what it was."""
    import botocore.credentials
    import botocore.httpsession

    # A shell whose credential chain WOULD answer: exported keys, a profile, both files.
    config, credentials_file = tmp_path / "config", tmp_path / "credentials"
    config.write_text("[profile shell]\nregion = eu-west-1\n")
    credentials_file.write_text("[shell]\naws_access_key_id = AKIDFILE\naws_secret_access_key = s\n")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))
    monkeypatch.setenv("AWS_PROFILE", "shell")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDSHELL")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "shell-secret")
    resolved, sent = [], []
    monkeypatch.setattr(
        botocore.credentials, "create_credential_resolver", lambda *a, **k: resolved.append(a)
    )
    monkeypatch.setattr(
        botocore.httpsession.URLLib3Session, "send", lambda _self, req: sent.append(req.url)
    )

    for profile in (None, "shell", "no-such-profile"):
        session = build_session(profile)
        credentials = session.get_credentials()
        assert (credentials.access_key, credentials.method) == ("testing", "explicit")
        assert session.available_profiles == []  # no config or credentials file was read
        sts = session.client("sts")  # region-less, as product code may build one
        with pytest.raises(RuntimeError, match="from a unit test: sts GetCallerIdentity"):
            sts.get_caller_identity()

    # A DSQL connect stops at its IAM token: no DNS lookup, no connection attempt.
    attempts: list = []
    connector = DsqlConnector(
        TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        connect_factory=lambda **kw: attempts.append(kw),
        ipv4_resolver=lambda host: attempts.append(host),
    )
    with pytest.raises(RuntimeError, match="from a unit test: dsql DbConnectAdmin"):
        connector.connect()

    assert resolved == [] and sent == [] and attempts == []
    assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"
    assert no_real_aws == ["sts GetCallerIdentity"] * 3 + ["dsql DbConnectAdmin"]
    no_real_aws.clear()  # the refusals were the point here; any other test fails on one


def test_a_stubber_still_answers_and_a_tests_own_seam_still_wins(monkeypatch, no_real_aws):
    """The guard is a floor, not a fake: a Stubber, a fake boto3 or a patched factory wins."""
    from botocore.stub import Stubber

    sts = build_session(None).client("sts")
    with Stubber(sts) as stub:
        stub.add_response(
            "get_caller_identity",
            {"UserId": "u", "Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/u"},
        )
        assert sts.get_caller_identity()["Account"] == "123456789012"
    assert no_real_aws == []

    mine = object()

    class _FakeBoto3:  # a fake boto3 module, as tests/test_target_connection.py installs
        Session = staticmethod(lambda **_k: mine)

    monkeypatch.setitem(sys.modules, "boto3", _FakeBoto3)
    assert build_session(None) is mine
    monkeypatch.setattr(aws_session, "_default_session_factory", lambda: lambda **_k: "own")
    assert build_session("p") == "own"


def test_a_session_the_factory_does_not_build_is_refused_at_the_http_layer(no_real_aws):
    """The CDC deploy role's assumed-role session builds its own botocore session, so the
    factory's refusals never reach it; the guard's HTTP-layer refusal does, before anything
    leaves the process."""
    from datetime import datetime, timedelta, timezone

    from dsql_migrator.core.aws_session import build_assumed_role_session

    class _Sts:  # the documented sts_client seam
        def assume_role(self, **_kwargs):
            return {
                "Credentials": {
                    "AccessKeyId": "AKID",
                    "SecretAccessKey": "SEC",
                    "SessionToken": "TOK",
                    "Expiration": datetime.now(timezone.utc) + timedelta(hours=2),
                }
            }

    session = build_assumed_role_session(
        "arn:aws:iam::1:role/R", sts_client=_Sts(), region="us-east-1"
    )
    cfn = session.client("cloudformation")
    with pytest.raises(
        RuntimeError,
        match="refused a real AWS request from a unit test to cloudformation.us-east-1",
    ):
        cfn.describe_stacks(StackName="dsql-cdc-guard")
    assert no_real_aws == ["HTTP send to cloudformation.us-east-1.amazonaws.com"]
    no_real_aws.clear()  # the refusal was the point here; any other test fails on one
