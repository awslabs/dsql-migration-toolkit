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

``s3:CreateBucket`` is the one exception, in a statement of its own: creating a name
another account owns fails with ``BucketAlreadyExists`` whatever the policy says.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

_REPO = pathlib.Path(__file__).resolve().parents[1]
_TEMPLATES = (
    "deploy/cloudformation.yaml",
    "deploy/cloudformation-ec2.yaml",
    "deploy/cdc-stack/cdc-stack.yaml",
)
# A statement's resource names one of the tool's buckets when its text holds one of these.
_BUCKET_MARKERS = ("mysql-dsql-migrator-plugins-", "${PluginBucketArn}", "${SourceObject}")


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


@pytest.mark.parametrize("path", _TEMPLATES)
def test_every_grant_on_the_tools_buckets_is_this_accounts_only(path: str) -> None:
    statements = _bucket_statements(path)
    assert statements, path
    for statement in statements:
        if _actions(statement) == ["s3:CreateBucket"]:
            assert "Condition" not in statement, (path, statement.get("Sid"))
            continue
        assert "s3:CreateBucket" not in _actions(statement), (path, statement.get("Sid"))
        condition = (statement.get("Condition") or {}).get("StringEquals") or {}
        assert condition.get("s3:ResourceAccount") == {"Ref": "AWS::AccountId"}, (
            path, statement.get("Sid"),
        )


@pytest.mark.parametrize("path", ("deploy/cloudformation.yaml", "deploy/cloudformation-ec2.yaml"))
def test_each_app_template_grants_the_bucket_actions_the_app_needs(path: str) -> None:
    # The split did not drop a grant: CreateBucket, ListBucket and the object actions the
    # app (plugins, state) and the CDC deploy role (template staging, plugin reads) use.
    granted = {a for s in _bucket_statements(path) for a in _actions(s)}
    assert {"s3:CreateBucket", "s3:ListBucket", "s3:PutObject", "s3:GetObject",
            "s3:DeleteObject"} <= granted, (path, granted)
