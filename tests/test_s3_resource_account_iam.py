# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Every IAM grant on the tool's S3 buckets is limited to this account's buckets.

The app asserts ``ExpectedBucketOwner`` on every S3 request to its derived bucket
(``core/s3_ownership.py``). As defense in depth, each IAM statement that grants an S3
action on that bucket name -- or on the plugin bucket the cdc-stack reads, or on the
single-EC2-host source tarball -- also carries ``s3:ResourceAccount = AWS::AccountId``,
so S3 refuses the role on a bucket by that name another account owns even for a request
that would not assert the owner (a future call that forgot it, or an AWS service reading
the object with the role's credentials: CloudFormation's TemplateURL, MSK Connect's
plugin, Lambda's code).

The managed bucket has two names (``s3_provision.managed_bucket_name`` chooses one): the
``-an`` name in this account's S3 account regional namespace, which a new deployment uses,
and the earlier global-namespace name, which an existing deployment keeps. Every grant in
the two app templates names both.

``s3:CreateBucket`` is the one exception to the ResourceAccount condition, in statements of
its own: creating a name another account owns fails with ``BucketAlreadyExists`` whatever
the policy says. CreateBucket on the ``-an`` name requires
``s3:x-amz-bucket-namespace = account-regional`` instead, so the role can create that name
only in this account's namespace; CreateBucket on the earlier name has no condition (an
older app version, me-south-1 and me-central-1, and the fallback when the ``-an`` create is
denied).

YAML only: no AWS. ``_allowed`` evaluates the rendered statements the way IAM's policy
simulator would for these plain Allow statements (exact actions, ``*`` resource wildcards,
``StringEquals`` conditions), so the tests check what a request may do, not only how the
YAML is laid out.
"""

from __future__ import annotations

import fnmatch
import pathlib
import re

import pytest
import yaml

from dsql_migrator.core.s3_ownership import ACCOUNT_REGIONAL_NAMESPACE
from dsql_migrator.core.s3_provision import (
    earlier_plugin_bucket_name,
    regional_plugin_bucket_name,
)

_REPO = pathlib.Path(__file__).resolve().parents[1]
_APP_TEMPLATES = ("deploy/cloudformation.yaml", "deploy/cloudformation-ec2.yaml")
_TEMPLATES = (*_APP_TEMPLATES, "deploy/cdc-stack/cdc-stack.yaml")
# The two names of the managed bucket, as the app templates spell them (in an Fn::Sub).
_EARLIER_SUB = "mysql-dsql-migrator-plugins-${AWS::AccountId}-${AWS::Region}"
_REGIONAL_SUB = f"{_EARLIER_SUB}-an"
# A statement's resource names one of the tool's buckets when its text holds one of these
# (the first already matches the -an name; it is listed on its own so that it stays covered
# if the first is ever narrowed).
_BUCKET_MARKERS = (
    "mysql-dsql-migrator-plugins-",
    _REGIONAL_SUB,
    "${PluginBucketArn}",
    "${SourceObject}",
)
_NAMESPACE_CONDITION = {
    "StringEquals": {"s3:x-amz-bucket-namespace": ACCOUNT_REGIONAL_NAMESPACE}
}
# The app's own role in each app template (the other role that reaches the managed bucket
# is the CdcDeployRole it assumes: template staging, CloudFormation's plugin reads).
_APP_ROLE = {
    "deploy/cloudformation.yaml": "TaskRole",
    "deploy/cloudformation-ec2.yaml": "HostRole",
}

_ME = "111122223333"
_THEM = "444455556666"
_REGION = "ap-northeast-2"
_EARLIER = earlier_plugin_bucket_name(_ME, _REGION)
_REGIONAL = regional_plugin_bucket_name(_ME, _REGION)


def _load(path: str) -> dict:
    class _L(yaml.SafeLoader):
        pass

    def _any(loader, suffix, node):  # short-form intrinsics, if any: keep the value
        if isinstance(node, yaml.ScalarNode):
            return {suffix: loader.construct_scalar(node)}
        if isinstance(node, yaml.SequenceNode):
            return {suffix: loader.construct_sequence(node, deep=True)}
        return {suffix: loader.construct_mapping(node, deep=True)}

    _L.add_multi_constructor("!", _any)
    return yaml.load((_REPO / path).read_text(encoding="utf-8"), Loader=_L)


def _statements(node):
    """Every IAM policy statement (a mapping with Effect and Action) under ``node``."""
    if isinstance(node, dict):
        if "Effect" in node and "Action" in node:
            yield node
        for value in node.values():
            yield from _statements(value)
    elif isinstance(node, list):
        for value in node:
            yield from _statements(value)


def _actions(statement: dict) -> list:
    actions = statement["Action"]
    return [actions] if isinstance(actions, str) else list(actions)


def _bucket_statements(path: str) -> list:
    return [
        s for s in _statements(_load(path))
        if any(a.startswith("s3:") for a in _actions(s))
        and any(marker in repr(s.get("Resource")) for marker in _BUCKET_MARKERS)
    ]


def _resources(statement: dict) -> list:
    """The statement's resources, each as its Fn::Sub text."""
    resource = statement["Resource"]
    items = resource if isinstance(resource, list) else [resource]
    return [item["Fn::Sub"] if isinstance(item, dict) else item for item in items]


def _is_create_only(statement: dict) -> bool:
    return _actions(statement) == ["s3:CreateBucket"]


def _render(node, region: str = _REGION):
    """``node`` with the pseudo parameters the bucket grants use filled in (partition aws,
    account ``_ME``)."""
    if isinstance(node, dict):
        if set(node) == {"Fn::Sub"} and isinstance(node["Fn::Sub"], str):
            return _render(node["Fn::Sub"], region)
        if node == {"Ref": "AWS::AccountId"}:
            return _ME
        return {k: _render(v, region) for k, v in node.items()}
    if isinstance(node, list):
        return [_render(v, region) for v in node]
    if isinstance(node, str):
        return (
            node.replace("${AWS::Partition}", "aws")
            .replace("${AWS::AccountId}", _ME)
            .replace("${AWS::Region}", region)
        )
    return node


def _role_statements(path: str, role: str) -> list:
    return [_render(s) for s in _statements(_load(path)["Resources"][role])]


def _conditions_hold(condition: dict | None, context: dict) -> bool:
    for operator, keys in (condition or {}).items():
        assert operator == "StringEquals", operator  # the only operator these grants use
        if any(context.get(key) != value for key, value in keys.items()):
            return False
    return True


def _allowed(statements: list, action: str, resource: str, context: dict) -> bool:
    """Whether an Allow statement grants ``action`` on ``resource`` in ``context`` (no Deny
    statement touches the bucket, so any matching Allow decides)."""
    for statement in statements:
        if statement["Effect"] != "Allow" or action not in _actions(statement):
            continue
        targets = statement["Resource"]
        targets = targets if isinstance(targets, list) else [targets]
        # A target left unrendered (an Fn::Sub with variables: the EC2 source tarball) is
        # not one of the managed bucket's names.
        targets = [t for t in targets if isinstance(t, str)]
        if any(fnmatch.fnmatchcase(resource, t) for t in targets) and _conditions_hold(
            statement.get("Condition"), context
        ):
            return True
    return False


def _arn(bucket: str, key: str = "") -> str:
    return f"arn:aws:s3:::{bucket}" + (f"/{key}" if key else "")


@pytest.mark.parametrize("path", _TEMPLATES)
def test_every_grant_on_the_tools_buckets_is_this_accounts_only(path: str) -> None:
    statements = _bucket_statements(path)
    assert statements, path
    for statement in statements:
        if _is_create_only(statement):
            # Never s3:ResourceAccount here. The -an name's create carries the namespace
            # condition and nothing else; the earlier name's create carries none.
            if any(r.endswith("-an") for r in _resources(statement)):
                assert statement.get("Condition") == _NAMESPACE_CONDITION, (
                    path, statement.get("Sid"),
                )
            else:
                assert "Condition" not in statement, (path, statement.get("Sid"))
            continue
        assert "s3:CreateBucket" not in _actions(statement), (path, statement.get("Sid"))
        condition = (statement.get("Condition") or {}).get("StringEquals") or {}
        assert condition.get("s3:ResourceAccount") == {"Ref": "AWS::AccountId"}, (
            path, statement.get("Sid"),
        )


@pytest.mark.parametrize("path", _APP_TEMPLATES)
def test_each_app_template_grants_the_bucket_actions_the_app_needs(path: str) -> None:
    # The split did not drop a grant: CreateBucket, ListBucket and the object actions the
    # app (plugins, state) and the CDC deploy role (template staging, plugin reads) use.
    granted = {a for s in _bucket_statements(path) for a in _actions(s)}
    assert {"s3:CreateBucket", "s3:ListBucket", "s3:PutObject", "s3:GetObject",
            "s3:DeleteObject"} <= granted, (path, granted)


@pytest.mark.parametrize("path", _APP_TEMPLATES)
def test_every_managed_bucket_grant_names_both_of_its_names(path: str) -> None:
    # FAILS WITHOUT THE FIX: every grant named only the earlier bucket, so on a new
    # deployment, whose bucket is the -an one, the HeadBucket that chooses it, the plugin
    # upload, the job and session state, the template staging and CloudFormation's read of
    # the plugins were all AccessDenied. An existing deployment keeps the earlier bucket, so
    # no grant may drop that one either.
    covered = set()
    for statement in _bucket_statements(path):
        resources = [r for r in _resources(statement) if "mysql-dsql-migrator-plugins-" in r]
        if not resources or _is_create_only(statement):
            continue
        earlier = {r for r in resources if _REGIONAL_SUB not in r}
        regional = {r.replace(_REGIONAL_SUB, _EARLIER_SUB) for r in resources if _REGIONAL_SUB in r}
        assert earlier and regional == earlier, (path, statement.get("Sid"), resources)
        covered.add(statement.get("Sid"))
    assert {"ManagePluginBucket", "UploadPlugins", "StageCdcStackTemplate",
            "ReadPluginArtifacts"} <= covered, (path, covered)


@pytest.mark.parametrize("path", _APP_TEMPLATES)
def test_create_bucket_on_the_regional_name_requires_the_account_regional_namespace(
    path: str,
) -> None:
    # FAILS WITHOUT THE FIX: the app's role could create only the earlier name, so a new
    # deployment fell back to the guessable global-namespace name instead of the -an one
    # that only this account can create.
    regional_arn = f"arn:${{AWS::Partition}}:s3:::{_REGIONAL_SUB}"
    earlier_arn = f"arn:${{AWS::Partition}}:s3:::{_EARLIER_SUB}"
    creates = {
        tuple(_resources(s)): s
        for s in _bucket_statements(path) if "s3:CreateBucket" in _actions(s)
    }
    assert set(creates) == {(regional_arn,), (earlier_arn,)}, (path, list(creates))
    assert creates[(regional_arn,)]["Condition"] == _NAMESPACE_CONDITION, path
    assert _NAMESPACE_CONDITION == {
        "StringEquals": {"s3:x-amz-bucket-namespace": "account-regional"}
    }
    # The earlier name: an older app version, me-south-1/me-central-1, and the fallback.
    assert "Condition" not in creates[(earlier_arn,)], path


@pytest.mark.parametrize("path", _APP_TEMPLATES)
def test_the_app_role_uses_either_bucket_and_creates_the_regional_one_only_in_its_namespace(
    path: str,
) -> None:
    # FAILS WITHOUT THE FIX: every request on the -an bucket below was denied (the role
    # named only the earlier bucket).
    statements = _role_statements(path, _APP_ROLE[path])
    mine = {"s3:ResourceAccount": _ME}
    theirs = {"s3:ResourceAccount": _THEM}
    namespace = {"s3:x-amz-bucket-namespace": "account-regional"}
    assert _allowed(statements, "s3:CreateBucket", _arn(_REGIONAL), namespace)
    assert _allowed(statements, "s3:CreateBucket", _arn(_EARLIER), {})
    for bucket in (_REGIONAL, _EARLIER):
        assert _allowed(statements, "s3:ListBucket", _arn(bucket), mine), bucket
        for key in ("cdc-plugins/dsql-sink-plugin-abc.zip", "sessions/b.json", "jobs/j.json"):
            for action in ("s3:PutObject", "s3:GetObject", "s3:DeleteObject"):
                assert _allowed(statements, action, _arn(bucket, key), mine), (action, key)
        # A bucket by either name that another account holds: refused.
        assert not _allowed(statements, "s3:ListBucket", _arn(bucket), theirs), bucket
        assert not _allowed(statements, "s3:GetObject", _arn(bucket, "jobs/j.json"), theirs)
    # The -an name only in this account's namespace: a create that leaves the namespace out,
    # or names the global one, is denied.
    assert not _allowed(statements, "s3:CreateBucket", _arn(_REGIONAL), {})
    assert not _allowed(
        statements, "s3:CreateBucket", _arn(_REGIONAL), {"s3:x-amz-bucket-namespace": "global"}
    )
    # Nothing beyond the two names.
    assert not _allowed(
        statements, "s3:CreateBucket", _arn(earlier_plugin_bucket_name(_THEM, _REGION)), {}
    )
    assert not _allowed(statements, "s3:ListBucket", _arn(f"{_REGIONAL}-x"), mine)


@pytest.mark.parametrize("path", _APP_TEMPLATES)
def test_the_cdc_deploy_role_stages_and_reads_plugins_in_either_bucket(path: str) -> None:
    # FAILS WITHOUT THE FIX: Start CDC stages the template in the -an bucket, and
    # CloudFormation reads it and the plugins from there with this role, which named only
    # the earlier bucket (AccessDenied). A CDC stack an earlier version deployed keeps
    # reading the earlier one.
    statements = _role_statements(path, "CdcDeployRole")
    mine = {"s3:ResourceAccount": _ME}
    for bucket in (_REGIONAL, _EARLIER):
        staged = _arn(bucket, "cdc-plugins/cdc-stack.yaml")
        assert _allowed(statements, "s3:PutObject", staged, mine), bucket
        assert _allowed(statements, "s3:GetObject", staged, mine), bucket
        plugin = _arn(bucket, "cdc-plugins/debezium-mysql-plugin-abc.zip")
        assert _allowed(statements, "s3:GetObject", plugin, mine), bucket
        assert not _allowed(statements, "s3:GetObject", plugin, {"s3:ResourceAccount": _THEM})
        # Staging stays inside cdc-plugins/: this role never writes the app's state.
        assert not _allowed(statements, "s3:PutObject", _arn(bucket, "sessions/b.json"), mine)
    assert not any("s3:CreateBucket" in _actions(s) for s in statements)


@pytest.mark.parametrize("path", _APP_TEMPLATES)
@pytest.mark.parametrize("region", ["us-east-1", "ap-northeast-2", "me-central-1"])
def test_the_template_names_are_the_ones_the_app_chooses_between(path: str, region: str) -> None:
    # The grants name exactly the two buckets s3_provision chooses between: a name that
    # drifted from earlier_plugin_bucket_name / regional_plugin_bucket_name would make the
    # app's HeadBucket of it AccessDenied, and the rule would treat that bucket as unusable.
    names = set()
    for statement in _bucket_statements(path):
        for resource in _render(_resources(statement), region=region):
            if not isinstance(resource, str):
                continue  # an Fn::Sub with variables: the EC2 source tarball
            found = re.fullmatch(
                r"arn:aws:s3:::(mysql-dsql-migrator-plugins-[^/]+)(/.*)?", resource
            )
            if found:
                names.add(found.group(1))
    assert names == {
        earlier_plugin_bucket_name(_ME, region),
        regional_plugin_bucket_name(_ME, region),
    }, (path, names)


def test_the_fargate_state_settings_stay_the_earlier_name_the_app_reads_as_managed() -> None:
    # The ECS Fargate template keeps the job and session state settings at the earlier name:
    # an image older than this template keeps using that bucket, and this version reads the
    # value as "the managed bucket" (s3_provision.state_bucket compares it with
    # earlier_plugin_bucket_name) and uses the bucket it chooses. The -an name there would be
    # used as given, outside the rule, and an existing deployment would lose its saved state.
    container = _load("deploy/cloudformation.yaml")["Resources"]["TaskDefinition"][
        "Properties"
    ]["ContainerDefinitions"][0]
    env = {e["Name"]: e.get("Value") for e in container["Environment"]}
    for name in ("DSQL_MIGRATOR_JOB_STATE_BUCKET", "DSQL_MIGRATOR_SESSION_STATE_BUCKET"):
        assert env[name] == {"Fn::Sub": _EARLIER_SUB}, name
        assert _render(env[name]) == _EARLIER, name
