# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Suite-wide guard: no unit test resolves this machine's AWS credentials or reaches AWS.

The repo rule is that tests never touch a real MySQL/DSQL/AWS. The tool builds every
boto3 session that would use the shell's credential chain -- the DSQL IAM token's
included -- through ``build_session``, which takes its constructor from
``aws_session._default_session_factory``. For every test not marked ``integration`` or
``e2e`` (those need real infrastructure, or run the app as a subprocess), the autouse
fixture below replaces that factory with one whose sessions:

* hold STATIC dummy credentials, whatever profile is asked for: no AWS config or
  credentials file is read and no profile variable is honoured, so the shell's credential
  chain is never resolved. They always have a region (``AWS_DEFAULT_REGION``, else
  us-east-1), so a region-less client meets the refusal below in every shell, not a
  NoRegionError in some;
* refuse every real HTTP send (botocore ``before-send``) and every DSQL signature, the
  IAM token's included (``before-sign.dsql``: the token is the password of a real
  connection to the cluster), with an error naming the service and the operation. A
  botocore ``Stubber`` still answers: it returns before anything is signed or sent.

A session the factory does not build -- the CDC deploy role's assumed-role session builds
its own botocore session -- gets neither hook, so the guard also refuses at the one place
every real AWS request leaves the process: botocore's ``URLLib3Session.send``.

A refusal also fails the test at teardown, even when product code caught it, so no test
passes on the guard's error in place of a fake of its own. Instance metadata is off
(``AWS_EC2_METADATA_DISABLED=true``). A test that patches the factory, swaps ``boto3``
for a fake or passes its own session seam still wins: it patches after this fixture.
"""

from __future__ import annotations

import os

import pytest

from dsql_migrator.core import aws_session

_DUMMY_CREDENTIALS = {
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
    "aws_session_token": "testing",
}


class RealAwsCallRefused(RuntimeError):
    """A unit test tried to sign or send a real AWS request."""


def _dummy_credentials_factory(refused: list):
    """Return the ``_default_session_factory`` stand-in described in the module docstring."""
    import boto3.session
    import botocore.session
    from botocore.configprovider import ConstantProvider

    real_session = boto3.session.Session

    def refuse(event_name: str, **_kwargs) -> None:
        service, operation = event_name.split(".")[1:3]
        refused.append(f"{service} {operation}")
        raise RealAwsCallRefused(
            f"tests/conftest.py refused a real AWS call from a unit test: {service} "
            f"{operation}. Tests never touch a real AWS: inject the fake or seam this code "
            "path offers (a fake client or session, session_factory, token_generator, "
            "connection_factory, ...)."
        )

    def session(*_args, **_kwargs):  # profile_name is ignored, like the shell's profile
        core = botocore.session.Session()
        store = core.get_component("config_store")
        store.set_config_provider("profile", ConstantProvider(None))
        for name in ("config_file", "credentials_file"):
            store.set_config_provider(name, ConstantProvider(os.devnull))
        core.register("before-send", refuse)
        core.register("before-sign.dsql", refuse)
        region = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
        return real_session(botocore_session=core, region_name=region, **_DUMMY_CREDENTIALS)

    def default_session_factory():
        import boto3  # a fake boto3 a test installed wins, as it would over the real one

        return session if boto3.Session is real_session else boto3.Session

    return default_session_factory


def _refuse_send(refused: list):
    """Return the ``URLLib3Session.send`` stand-in described in the module docstring."""
    from urllib.parse import urlsplit

    def send(_self, request) -> None:
        host = urlsplit(str(getattr(request, "url", "") or "")).hostname or "an AWS endpoint"
        refused.append(f"HTTP send to {host}")
        raise RealAwsCallRefused(
            f"tests/conftest.py refused a real AWS request from a unit test to {host}. Tests "
            "never touch a real AWS: inject the fake or seam this code path offers (a fake "
            "client or session, sts_client, session_factory, ...)."
        )

    return send


@pytest.fixture(autouse=True)
def no_real_aws(request: pytest.FixtureRequest):
    """Yield the AWS calls refused so far in this test (see the module docstring)."""
    refused: list[str] = []
    if request.node.get_closest_marker("integration") or request.node.get_closest_marker("e2e"):
        yield refused
        return
    # An own MonkeyPatch, not the test's: a test's monkeypatch.undo() must not lift it.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        patch.setattr(
            aws_session, "_default_session_factory", _dummy_credentials_factory(refused)
        )
        import botocore.httpsession

        patch.setattr(botocore.httpsession.URLLib3Session, "send", _refuse_send(refused))
        yield refused
    if refused:
        pytest.fail(
            f"this test tried to reach real AWS ({', '.join(refused)}) and tests/conftest.py "
            "refused it. Inject the fake or seam this code path offers: a refusal that "
            "product code catches lets a test pass on the guard's error, not on a fake of "
            "its own.",
            pytrace=False,
        )
