# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""S3 bucket ownership: a derived bucket name another AWS account owns is refused.

The tool names its managed bucket ``mysql-dsql-migrator-plugins-<account>-<region>``, and the
ECS Fargate app template points the job and session state at the same name. Anyone who knows
the account ID can create that bucket first, in their own account, and let the victim's
identities in through its bucket policy. Before this fix the tool adopted such a bucket
(HeadBucket answered 200): it uploaded the CDC connector plugins, the seeder Lambda and the
cdc-stack template there -- MSK Connect, Lambda and CloudFormation then read them back from it
-- and wrote and restored the job, session and overlay-ledger state there.

Now every request names the caller's own account (``sts:GetCallerIdentity``) as the bucket's
``ExpectedBucketOwner`` -- S3 answers 403 unless that account owns the bucket -- and a refusal
is a hard error (or, for the best-effort state stores, nothing saved and nothing restored),
never an adoption.

:class:`OwnerCheckingS3` is S3 as far as ownership goes, and it REQUIRES the owner: a request
without ``ExpectedBucketOwner`` is recorded in ``violations`` and -- as on real S3 -- reaches a
foreign bucket that lets this identity in. So a call that leaves the owner out fails both
``violations == []`` and the "nothing reached the other account's bucket" checks.

Fakes only: no AWS. The one real boto3 client is short-circuited before any request is sent.
"""

from __future__ import annotations

import ast
import hashlib
import io
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib.parse import urlsplit

import pytest
from botocore.exceptions import ClientError

from dsql_migrator.core import s3_provision
from dsql_migrator.core.cdc_stack_deployer import CdcDeployError, CdcStackDeployer
from dsql_migrator.core.job_store import S3JobStore
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType
from dsql_migrator.core.overlay_ledger import (
    OverlayLedgerColumn,
    OverlayLedgerError,
    OverlayLedgerTable,
    S3OverlayLedger,
)
from dsql_migrator.core.s3_ownership import (
    BucketOwnershipError,
    CallerAccount,
    OwnedBucket,
    caller_account_id,
    is_access_denied,
    is_missing,
)
from dsql_migrator.core.s3_provision import (
    S3ProvisionError,
    ensure_and_upload_plugins,
    ensure_plugin_bucket,
    plugin_bucket_name,
    upload_plugin,
)
from dsql_migrator.core.session_state_store import S3SessionStateStore, SessionSnapshot

_ME = "111122223333"
_THEM = "444455556666"
_REGION = "us-east-1"
_BUCKET = plugin_bucket_name(_ME, _REGION)
_SRC = Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"


# ---------------------------------------------------------------------------
# The fake: S3 as far as bucket ownership goes
# ---------------------------------------------------------------------------


def _error(operation: str, code: str, status: int, message: str = "") -> ClientError:
    """A botocore ClientError as S3 sends it (a HEAD has no body: its code is the status)."""
    return ClientError(
        {
            "Error": {"Code": code, "Message": message or code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )


# The IAM action each S3 operation the tool sends is authorized by.
_ACTIONS = {
    "HeadBucket": "s3:ListBucket", "ListObjectsV2": "s3:ListBucket",
    "HeadObject": "s3:GetObject", "GetObject": "s3:GetObject",
    "PutObject": "s3:PutObject", "DeleteObject": "s3:DeleteObject",
}


class _Bucket:
    def __init__(self, owner: str, *, lets_me_in: bool = True) -> None:
        self.owner = owner
        # A squatter's bucket policy lets the victim's identities in, so that a request that
        # does not name the owner reaches it -- the case the reported issue is about.
        self.lets_me_in = lets_me_in
        self.objects: dict[str, tuple[bytes, dict]] = {}

    def last_modified(self, key: str) -> datetime:
        """Write order: the objects dict keeps insertion order, and a write re-inserts."""
        return _EPOCH + timedelta(seconds=list(self.objects).index(key))


_EPOCH = datetime(2026, 11, 1, tzinfo=timezone.utc)


class _Paginator:
    def __init__(self, s3: "OwnerCheckingS3") -> None:
        self._s3 = s3

    def paginate(self, **params):
        bucket = self._s3._reach("ListObjectsV2", params)
        prefix = params.get("Prefix", "")
        contents = [
            {"Key": key, "LastModified": bucket.last_modified(key)}
            for key in bucket.objects
            if key.startswith(prefix)
        ]
        yield {"Contents": contents}


class OwnerCheckingS3:
    """S3 for one identity in account ``caller``: buckets have owners; a request with
    ``ExpectedBucketOwner`` reaches a bucket only when that account owns it (else 403, as S3
    answers); every request but CreateBucket must carry it (else a recorded violation, and
    the request goes through as S3 would let it -- to a foreign bucket that lets us in)."""

    def __init__(self, caller: str = _ME, *, region: str = _REGION) -> None:
        self.caller = caller
        self.region = region
        self.buckets: dict[str, _Bucket] = {}
        self.calls: list[tuple[str, dict]] = []
        self.violations: list[str] = []
        self.reads: list[tuple[str, str]] = []  # (bucket, key) an object was read from
        # Another account creates the bucket between our HeadBucket and our CreateBucket.
        self.squatter_creates_first: Optional[str] = None
        # A bucket just created answers 404 to this many HeadBuckets.
        self.new_bucket_misses = 0
        # CreateBucket answers BucketAlreadyOwnedByYou yet HeadBucket answers 403 (an
        # identity that may create the bucket but not list it).
        self.head_denied = False
        # Operations this identity's IAM policy denies, in a bucket it owns too (403).
        self.iam_denied: set[str] = set()

    # -- set-up ---------------------------------------------------------------
    def bucket(self, name: str, owner: Optional[str] = None, **kw) -> _Bucket:
        self.buckets[name] = _Bucket(owner or self.caller, **kw)
        return self.buckets[name]

    def ops(self) -> list[str]:
        return [op for op, _ in self.calls]

    # -- the check S3 makes ---------------------------------------------------
    def _reach(self, operation: str, params: dict, *, head: bool = False) -> _Bucket:
        self.calls.append((operation, dict(params)))
        expected = params.get("ExpectedBucketOwner")
        if expected is None:
            self.violations.append(operation)
        bucket = self.buckets.get(params["Bucket"])
        if bucket is None:
            raise _error(operation, "404" if head else "NoSuchBucket", 404)
        denied = (expected is not None and expected != bucket.owner) or (
            bucket.owner != self.caller and not bucket.lets_me_in
        )
        if denied or (head and self.head_denied and operation == "HeadBucket"):
            raise _error(operation, "403" if head else "AccessDenied", 403)
        if operation in self.iam_denied:
            action = _ACTIONS[operation]
            raise _error(
                operation, "403" if head else "AccessDenied", 403,
                "" if head else (
                    f"User: arn:aws:sts::{self.caller}:assumed-role/app/task is not authorized "
                    f"to perform: {action} because no identity-based policy allows the "
                    f"{action} action"
                ),
            )
        return bucket

    # -- the S3 API the tool uses --------------------------------------------
    def head_bucket(self, **params):
        self._reach("HeadBucket", params, head=True)
        if self.new_bucket_misses and params["Bucket"] in getattr(self, "_created", ()):
            self.new_bucket_misses -= 1
            raise _error("HeadBucket", "404", 404)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def create_bucket(self, **params):
        self.calls.append(("CreateBucket", dict(params)))
        name = params["Bucket"]
        location = (params.get("CreateBucketConfiguration") or {}).get("LocationConstraint")
        if (location or "us-east-1") != self.region:
            raise _error("CreateBucket", "IllegalLocationConstraintException", 400)
        if self.squatter_creates_first and name not in self.buckets:
            self.buckets[name] = _Bucket(self.squatter_creates_first)
        existing = self.buckets.get(name)
        if existing is not None:
            if existing.owner != self.caller:
                raise _error("CreateBucket", "BucketAlreadyExists", 409)
            if self.region == "us-east-1":
                return {}  # the legacy 200 that us-east-1 answers a re-create with
            raise _error("CreateBucket", "BucketAlreadyOwnedByYou", 409)
        self.buckets[name] = _Bucket(self.caller)
        self._created = {*getattr(self, "_created", ()), name}
        return {}

    def head_object(self, **params):
        bucket = self._reach("HeadObject", params, head=True)
        if params["Key"] not in bucket.objects:
            raise _error("HeadObject", "404", 404)
        body, metadata = bucket.objects[params["Key"]]
        return {"ContentLength": len(body), "ETag": '"0f-2"', "Metadata": dict(metadata)}

    def get_object(self, **params):
        bucket = self._reach("GetObject", params)
        if params["Key"] not in bucket.objects:
            raise _error("GetObject", "NoSuchKey", 404)
        self.reads.append((params["Bucket"], params["Key"]))
        return {"Body": io.BytesIO(bucket.objects[params["Key"]][0])}

    def put_object(self, **params):
        bucket = self._reach("PutObject", params)
        body = params["Body"]
        body = body.read() if hasattr(body, "read") else body
        bucket.objects.pop(params["Key"], None)  # a write moves the object last
        bucket.objects[params["Key"]] = (bytes(body), dict(params.get("Metadata") or {}))
        return {}

    def delete_object(self, **params):
        bucket = self._reach("DeleteObject", params)
        bucket.objects.pop(params["Key"], None)
        return {}

    def get_paginator(self, name: str):
        assert name == "list_objects_v2", name
        return _Paginator(self)


class ManagedOwnerCheckingS3(OwnerCheckingS3):
    """As boto3's S3 client: with ``upload_file`` (s3transfer sends one PutObject with the
    ExtraArgs), so upload_plugin takes its managed-upload path."""

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None, Config=None):  # noqa: N803
        from boto3.exceptions import S3UploadFailedError

        params = {"Bucket": Bucket, "Key": Key, **(ExtraArgs or {})}
        try:
            bucket = self._reach("PutObject", params)
        except ClientError as exc:  # as boto3 reports a failed managed upload
            raise S3UploadFailedError(f"Failed to upload {Filename} to {Bucket}/{Key}: {exc}")
        bucket.objects.pop(Key, None)
        bucket.objects[Key] = (Path(Filename).read_bytes(), dict(params.get("Metadata") or {}))


class _Sts:
    def __init__(self, account: str = _ME) -> None:
        self.account = account
        self.calls = 0

    def get_caller_identity(self):
        self.calls += 1
        arn = f"arn:aws:iam::{self.account}:role/r"
        return {"Account": self.account, "Arn": arn, "UserId": "u"}


class _NoSts:
    calls = 0

    def get_caller_identity(self):
        self.calls += 1
        raise ClientError(
            {"Error": {"Code": "ExpiredToken", "Message": "The security token is expired"}},
            "GetCallerIdentity",
        )


def _foreign(s3: OwnerCheckingS3, name: str = _BUCKET) -> _Bucket:
    """The squatter's bucket: same name, another account, and it lets this identity in."""
    return s3.bucket(name, _THEM, lets_me_in=True)


def _assert_refusal(text: str, bucket: str = _BUCKET) -> None:
    """What every refusal must tell the operator."""
    assert f"'{bucket}'" in text, text
    assert "another AWS account" in text, text
    assert "AWS Support" in text, text
    assert "delete it there" in text, text


# ---------------------------------------------------------------------------
# The classification and the owner itself
# ---------------------------------------------------------------------------


def test_the_error_classes_follow_s3s_answers() -> None:
    assert is_access_denied(_error("HeadBucket", "403", 403))
    assert is_access_denied(_error("PutObject", "AccessDenied", 403))
    assert not is_access_denied(_error("HeadBucket", "404", 404))
    assert is_missing(_error("HeadBucket", "404", 404))
    assert is_missing(_error("PutObject", "NoSuchBucket", 404))
    assert not is_missing(_error("PutObject", "SlowDown", 503))
    assert not is_access_denied(_error("PutObject", "SlowDown", 503))
    # A status code inside the account ID in a message is not a status.
    unreachable = f"could not connect to {plugin_bucket_name('123404567890', 'x')}"
    assert not is_missing(RuntimeError(unreachable))
    assert is_missing(RuntimeError("404 Not Found"))


def test_the_owner_is_the_callers_account_and_is_read_once() -> None:
    sts = _Sts()
    owner = CallerAccount(lambda: sts)
    assert (owner.get(), owner.get()) == (_ME, _ME)
    assert sts.calls == 1
    assert caller_account_id(_Sts()) == _ME


def test_an_unreadable_account_is_a_clear_error_and_is_tried_again() -> None:
    sts = _NoSts()
    owner = CallerAccount(lambda: sts)
    for _ in range(2):
        with pytest.raises(BucketOwnershipError) as exc:
            owner.get()
    assert sts.calls == 2  # a failure is not kept
    assert "sts:GetCallerIdentity" in str(exc.value) and "expired" in str(exc.value)
    with pytest.raises(BucketOwnershipError):
        caller_account_id(SimpleNamespace(get_caller_identity=lambda: {"Account": "acct"}))
    with pytest.raises(BucketOwnershipError):
        CallerAccount(lambda: (_ for _ in ()).throw(RuntimeError("no region"))).get()


def test_an_owned_bucket_takes_only_a_12_digit_owner() -> None:
    for owner in ("", None, "1", "acct", "1111222233334"):
        with pytest.raises(BucketOwnershipError):
            OwnedBucket(OwnerCheckingS3(), _BUCKET, owner)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Every S3 call on a derived bucket asserts the owner (structure)
# ---------------------------------------------------------------------------

# S3 client operations that address a bucket. ``create_bucket`` takes no expected owner.
_S3_OPERATIONS = frozenset({
    "abort_multipart_upload", "complete_multipart_upload", "copy_object",
    "create_bucket", "create_multipart_upload", "delete_bucket", "delete_bucket_policy",
    "delete_object", "delete_objects", "download_file", "download_fileobj",
    "generate_presigned_post", "generate_presigned_url", "get_bucket_policy",
    "get_object", "get_object_attributes", "head_bucket", "head_object",
    "list_object_versions", "list_objects", "list_objects_v2", "put_bucket_policy",
    "put_bucket_versioning", "put_object", "restore_object", "upload_file",
    "upload_fileobj", "upload_part",
})
# boto3's managed transfers take request parameters only through ``ExtraArgs``.
_TRANSFER_OPERATIONS = frozenset(
    {"upload_file", "upload_fileobj", "download_file", "download_fileobj"}
)
_OWNER = "ExpectedBucketOwner"


def _is_s3_paginator(call: ast.Call) -> bool:
    return (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "get_paginator"
        and bool(call.args)
        and isinstance(call.args[0], ast.Constant)
        and str(call.args[0].value).startswith(("list_objects", "list_object_versions"))
    )


def _names_owner(call: ast.Call) -> bool:
    """The call asserts the owner where its API takes it: a keyword of the operation, or a
    key of a managed transfer's ``ExtraArgs``."""
    assert isinstance(call.func, ast.Attribute)
    if call.func.attr not in _TRANSFER_OPERATIONS:
        return any(k.arg == _OWNER for k in call.keywords)
    for keyword in call.keywords:
        if keyword.arg == "ExtraArgs" and isinstance(keyword.value, ast.Dict):
            return any(
                isinstance(k, ast.Constant) and k.value == _OWNER for k in keyword.value.keys
            )
    return False


def direct_s3_calls(source: str) -> list[str]:
    """The S3 operations ``source`` calls on a client directly (``client.put_object(...)``,
    an S3 ``get_paginator``). Outside core/s3_ownership.py there must be none."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in _S3_OPERATIONS or _is_s3_paginator(node):
                found.append(f"{node.func.attr}@{node.lineno}")
    return found


def calls_without_owner(source: str) -> list[str]:
    """In core/s3_ownership.py: each S3 call that does not assert the owner -- an operation
    with no ``ExpectedBucketOwner`` (a managed transfer: none among its ``ExtraArgs``), a
    ``paginate`` without it, or a ``create_bucket`` outside ``OwnedBucket.ensure`` (the one
    place that checks the bucket's owner after it)."""
    tree = ast.parse(source)
    missing = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                continue
            name = node.func.attr
            if name == "create_bucket":
                if func.name != "ensure":
                    missing.append(f"{name}@{node.lineno}")
            elif name in _S3_OPERATIONS or name == "paginate":
                if not _names_owner(node):
                    missing.append(f"{name}@{node.lineno}")
    return sorted(set(missing))


def test_no_module_calls_s3_except_through_the_owned_bucket() -> None:
    # FAILS WITHOUT THE FIX: s3_provision, cdc_stack_deployer, job_store, session_state_store
    # and overlay_ledger each called head_bucket / put_object / get_object ... directly, with
    # no expected owner. Every module of the shipped package is scanned, so a new S3 call
    # anywhere has to go through OwnedBucket (or be argued for here).
    modules = sorted(_SRC.rglob("*.py"))
    assert len(modules) > 100, modules
    offenders = {}
    for path in modules:
        if path.name == "s3_ownership.py":
            continue
        calls = direct_s3_calls(path.read_text(encoding="utf-8"))
        if calls:
            offenders[str(path.relative_to(_SRC))] = calls
    assert offenders == {}
    # ...and the modules that reach the derived bucket do go through it.
    for name in ("s3_provision", "cdc_stack_deployer", "job_store", "session_state_store",
                 "overlay_ledger"):
        assert "OwnedBucket(" in (_SRC / "core" / f"{name}.py").read_text(encoding="utf-8"), name


def test_every_s3_call_in_the_owned_bucket_asserts_the_owner() -> None:
    source = (_SRC / "core" / "s3_ownership.py").read_text(encoding="utf-8")
    assert calls_without_owner(source) == []
    # The scan sees the calls it is meant to see.
    assert len(direct_s3_calls(source)) >= 8


def test_the_scans_flag_a_call_without_the_owner() -> None:
    """The scans themselves, fed each shape of call they must catch."""
    assert direct_s3_calls("self._s3().put_object(Bucket=b, Key=k, Body=x)") == ["put_object@1"]
    assert direct_s3_calls("client.get_paginator('list_objects_v2')") == ["get_paginator@1"]
    assert direct_s3_calls("client.get_paginator('list_stacks')") == []
    assert direct_s3_calls("bucket.put(k, body)") == []

    def wrap(body: str, name: str = "head") -> str:
        return f"def {name}(self):\n    {body}\n"

    assert calls_without_owner(wrap("self.client.put_object(Bucket=b, Key=k, Body=x)")) == [
        "put_object@2"
    ]
    assert calls_without_owner(
        wrap("self.client.put_object(Bucket=b, Key=k, Body=x, ExpectedBucketOwner=o)")
    ) == []
    assert calls_without_owner(
        wrap("self.client.upload_file(f, b, k, ExtraArgs={'Metadata': m})")
    ) == ["upload_file@2"]
    assert calls_without_owner(
        wrap("self.client.upload_file(f, b, k, ExtraArgs={**e, 'ExpectedBucketOwner': o})")
    ) == []
    # ...and in the place each API takes it: upload_file has no such keyword of its own.
    assert calls_without_owner(
        wrap("self.client.upload_file(f, b, k, ExpectedBucketOwner=o)")
    ) == ["upload_file@2"]
    assert calls_without_owner(
        wrap("self.client.put_object(Bucket=b, ExtraArgs={'ExpectedBucketOwner': o})")
    ) == ["put_object@2"]
    assert calls_without_owner(wrap("p.paginate(Bucket=b, Prefix=x)")) == ["paginate@2"]
    assert calls_without_owner(wrap("self.client.create_bucket(Bucket=b)")) == [
        "create_bucket@2"
    ]
    assert calls_without_owner(wrap("self.client.create_bucket(Bucket=b)", "ensure")) == []


# ---------------------------------------------------------------------------
# Plugin provisioning (Deploy CDC infrastructure)
# ---------------------------------------------------------------------------


def _artifacts(tmp_path: Path, monkeypatch) -> dict[str, Path]:
    keys = (
        s3_provision.DEBEZIUM_PLUGIN_KEY,
        s3_provision.DEBEZIUM_PG_PLUGIN_KEY,
        s3_provision.DSQL_SINK_PLUGIN_KEY,
        s3_provision.LAMBDA_SEEDER_KEY,
    )
    files = {}
    for key in keys:
        path = tmp_path / key.rpartition("/")[2]
        path.write_bytes(f"real:{key}".encode() * 32)
        files[key] = path
    monkeypatch.setattr(s3_provision, "_artifact_paths", lambda: tuple(files[k] for k in keys))
    return files


@pytest.mark.parametrize("client", [OwnerCheckingS3, ManagedOwnerCheckingS3])
def test_a_plugin_bucket_another_account_owns_is_refused_not_adopted(
    client, tmp_path: Path, monkeypatch
) -> None:
    # FAILS WITHOUT THE FIX: HeadBucket answered 200 (the squatter's policy lets us in) and the
    # bucket was adopted; the four artifacts went into it and their keys into the cdc-stack.
    _artifacts(tmp_path, monkeypatch)
    s3 = client()
    theirs = _foreign(s3)
    with pytest.raises(S3ProvisionError) as exc:
        ensure_and_upload_plugins(s3, _Sts(), _REGION, source_engine=SourceType.MYSQL)
    _assert_refusal(str(exc.value))
    assert s3.ops() == ["HeadBucket"]  # no create, no upload
    assert theirs.objects == {}
    assert s3.violations == []


def test_a_pre_seeded_plugin_in_another_accounts_bucket_is_never_used(
    tmp_path: Path, monkeypatch
) -> None:
    # The keys are content-addressed and the bundled ZIPs are public, so a squatter can place a
    # same-size object stamped with the real SHA-256 at each key: before, HeadObject proved it
    # "up to date" and the deploy registered the squatter's bytes without uploading a thing.
    files = _artifacts(tmp_path, monkeypatch)
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    for base_key, path in files.items():
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        theirs.objects[s3_provision._content_key(base_key, digest)] = (
            b"!" * len(data), {"sha256": digest}
        )
    planted = dict(theirs.objects)
    with pytest.raises(S3ProvisionError):
        ensure_and_upload_plugins(s3, _Sts(), _REGION)
    assert theirs.objects == planted
    assert "HeadObject" not in s3.ops()
    assert s3.violations == []


def test_bucket_already_exists_at_create_is_refused(tmp_path: Path, monkeypatch) -> None:
    # HeadBucket found nothing, and another account created the name before our CreateBucket.
    _artifacts(tmp_path, monkeypatch)
    s3 = OwnerCheckingS3()
    s3.squatter_creates_first = _THEM
    with pytest.raises(S3ProvisionError) as exc:
        ensure_and_upload_plugins(s3, _Sts(), _REGION)
    _assert_refusal(str(exc.value))
    assert "BucketAlreadyExists" in str(exc.value)
    assert s3.ops() == ["HeadBucket", "CreateBucket"]
    assert s3.buckets[_BUCKET].objects == {}


@pytest.mark.parametrize("region", ["eu-west-1", "us-east-1"])
def test_a_bucket_this_account_already_owns_is_accepted_after_the_owner_check(region) -> None:
    # BucketAlreadyOwnedByYou (us-east-1: a 200) is ours, and passes the owner-checked
    # HeadBucket after it like a new bucket. Here the first HeadBucket was a 404 (a race).
    s3 = OwnerCheckingS3(region=region)
    name = plugin_bucket_name(_ME, region)
    real_head = s3.head_bucket
    first = {"done": False}

    def racing_head(**params):
        if not first["done"]:
            first["done"] = True
            s3.calls.append(("HeadBucket", dict(params)))
            s3.bucket(name)  # created by an earlier deploy of ours, just now
            raise _error("HeadBucket", "404", 404)
        return real_head(**params)

    s3.head_bucket = racing_head  # type: ignore[method-assign]
    assert ensure_plugin_bucket(s3, _ME, region) == name
    assert s3.ops() == ["HeadBucket", "CreateBucket", "HeadBucket"]
    assert s3.calls[0][1]["ExpectedBucketOwner"] == s3.calls[-1][1]["ExpectedBucketOwner"] == _ME
    assert s3.violations == []


def test_a_bucket_this_account_may_create_but_not_reach_is_refused() -> None:
    s3 = OwnerCheckingS3(region="eu-west-1")
    name = plugin_bucket_name(_ME, "eu-west-1")
    s3.bucket(name)
    s3.head_denied = True
    with pytest.raises(S3ProvisionError) as exc:
        ensure_plugin_bucket(s3, _ME, "eu-west-1")
    _assert_refusal(str(exc.value), name)
    assert "s3:ListBucket" in str(exc.value)
    assert "CreateBucket" not in s3.ops()


def _refused_after_create(already_owned: bool) -> tuple[OwnerCheckingS3, str]:
    """A bucket whose first HeadBucket answers 404 and whose HeadBucket after CreateBucket
    answers 403: created by this call (and this identity may not list it), or there already,
    in this account (CreateBucket: BucketAlreadyOwnedByYou), yet refused to that check."""
    region = "eu-west-1"
    s3 = OwnerCheckingS3(region=region)
    name = plugin_bucket_name(_ME, region)
    if not already_owned:
        s3.iam_denied = {"HeadBucket"}  # the missing bucket answers 404 before IAM is asked
        return s3, name
    s3.bucket(name)
    real_head = s3.head_bucket

    def head(**params):
        if not s3.head_denied:  # the first one misses it; every one after is refused
            s3.calls.append(("HeadBucket", dict(params)))
            s3.head_denied = True
            raise _error("HeadBucket", "404", 404)
        return real_head(**params)

    s3.head_bucket = head  # type: ignore[method-assign]
    return s3, name


@pytest.mark.parametrize("already_owned", [False, True])
def test_a_bucket_the_check_after_create_refuses_is_refused_not_adopted(already_owned) -> None:
    # The HeadBucket after CreateBucket is what shows the bucket is this account's: a create
    # answered BucketAlreadyOwnedByYou, or a race with another create, proves nothing until
    # it answers. Its 403 is a hard error -- the bucket is not adopted, and the deploy and
    # the state store built on it write nothing there.
    s3, name = _refused_after_create(already_owned)
    owned = OwnedBucket(s3, name, _ME)
    with pytest.raises(BucketOwnershipError) as exc:
        owned.ensure("eu-west-1", sleep=lambda _s: None)
    _assert_refusal(str(exc.value), name)
    assert "a HeadBucket request" in str(exc.value)
    assert s3.ops() == ["HeadBucket", "CreateBucket", "HeadBucket"]
    assert owned.confirmed is False and s3.violations == []

    s3, name = _refused_after_create(already_owned)
    with pytest.raises(S3ProvisionError):
        ensure_plugin_bucket(s3, _ME, "eu-west-1")
    s3, name = _refused_after_create(already_owned)
    store = S3JobStore(name, region="eu-west-1", s3_client=s3, sts_client=_Sts())
    store.save(_job("j1"), None)
    assert "PutObject" not in s3.ops() and s3.buckets[name].objects == {}
    _assert_refusal(store.bucket_refusal or "", name)


@pytest.mark.parametrize("region", ["us-east-1", "ap-northeast-2"])
def test_a_missing_bucket_is_created_then_verified(region) -> None:
    s3 = OwnerCheckingS3(region=region)
    name = plugin_bucket_name(_ME, region)
    s3.new_bucket_misses = 1  # a brand-new bucket is not found for a moment
    sleeps: list[float] = []
    OwnedBucket(s3, name, _ME).ensure(region, sleep=sleeps.append)
    assert s3.ops() == ["HeadBucket", "CreateBucket", "HeadBucket", "HeadBucket"]
    create = s3.calls[1][1]
    if region == "us-east-1":
        assert "CreateBucketConfiguration" not in create
    else:
        assert create["CreateBucketConfiguration"] == {"LocationConstraint": region}
    assert s3.buckets[name].owner == _ME
    assert sleeps == [1.0]
    assert s3.violations == []


def test_a_missing_bucket_that_stays_missing_is_an_error() -> None:
    s3 = OwnerCheckingS3()
    s3.new_bucket_misses = 5
    with pytest.raises(ClientError):
        OwnedBucket(s3, _BUCKET, _ME).ensure(_REGION, sleep=lambda _s: None)
    assert s3.ops().count("HeadBucket") == 4  # the probe, then three checks


def test_an_unreadable_account_stops_provisioning_before_any_s3_call(
    tmp_path: Path, monkeypatch
) -> None:
    _artifacts(tmp_path, monkeypatch)
    s3 = OwnerCheckingS3()
    with pytest.raises(S3ProvisionError) as exc:
        ensure_and_upload_plugins(s3, _NoSts(), _REGION)
    assert "sts:GetCallerIdentity" in str(exc.value)
    assert s3.calls == []


@pytest.mark.parametrize("client", [OwnerCheckingS3, ManagedOwnerCheckingS3])
def test_every_provisioning_request_asserts_the_owner(client, tmp_path: Path, monkeypatch) -> None:
    files = _artifacts(tmp_path, monkeypatch)
    s3 = client()
    result = ensure_and_upload_plugins(s3, _Sts(), _REGION)
    assert result.bucket_name == _BUCKET
    assert {op for op in s3.ops()} == {"HeadBucket", "CreateBucket", "HeadObject", "PutObject"}
    assert s3.violations == []
    assert all(p.get("ExpectedBucketOwner") == _ME for op, p in s3.calls if op != "CreateBucket")
    stored = s3.buckets[_BUCKET].objects
    assert {stored[k][0] for k in (result.debezium_key, result.dsql_sink_key)} == {
        files[s3_provision.DEBEZIUM_PLUGIN_KEY].read_bytes(),
        files[s3_provision.DSQL_SINK_PLUGIN_KEY].read_bytes(),
    }
    # A second deploy proves the objects in its own bucket and uploads nothing.
    s3.calls.clear()
    assert ensure_and_upload_plugins(s3, _Sts(), _REGION) == result
    assert set(s3.ops()) == {"HeadBucket", "HeadObject"}


def test_an_upload_that_is_refused_says_so(tmp_path: Path) -> None:
    artifact = tmp_path / "a.zip"
    artifact.write_bytes(b"x")
    s3 = OwnerCheckingS3()
    _foreign(s3)
    with pytest.raises(S3ProvisionError) as exc:
        upload_plugin(s3, _BUCKET, "cdc-plugins/0/a.zip", artifact, expected_owner=_ME)
    _assert_refusal(str(exc.value))
    assert s3.buckets[_BUCKET].objects == {}


class _Http:
    def __init__(self, status: int) -> None:
        self.status_code = status
        self.headers: dict = {}


def test_a_real_boto3_client_sends_the_expected_owner_on_every_request(
    tmp_path: Path, monkeypatch
) -> None:
    """Through the INSTALLED boto3 / s3transfer: HeadBucket, HeadObject and the managed
    upload's PutObject each carry x-amz-expected-bucket-owner (s3transfer accepts it among its
    ExtraArgs). Nothing is sent: a before-call handler answers each request."""
    import boto3
    import botocore.session

    connects: list = []
    monkeypatch.setattr(
        socket.socket, "connect", lambda _s, address: connects.append(address) or 1 / 0
    )
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_DEFAULTS_MODE"):
        monkeypatch.delenv(name, raising=False)
    sent: list[tuple[str, dict]] = []

    def answer(model, params, **_kw):
        sent.append((model.name, dict(params["headers"])))
        if model.name == "HeadObject":
            return _Http(404), {
                "Error": {"Code": "404", "Message": "Not Found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }
        return _Http(200), {"ResponseMetadata": {"HTTPStatusCode": 200}, "ETag": '"e"'}

    core = botocore.session.Session()
    core.set_config_variable("config_file", str(tmp_path / "no-config"))
    core.set_config_variable("credentials_file", str(tmp_path / "no-credentials"))
    client = boto3.session.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing",
        region_name=_REGION, botocore_session=core,
    ).client("s3")
    client.meta.events.register("before-call.s3", answer)
    _artifacts(tmp_path, monkeypatch)
    ensure_and_upload_plugins(client, _Sts(), _REGION, source_engine=SourceType.POSTGRES)
    assert [name for name, _ in sent] == ["HeadBucket"] + ["HeadObject", "PutObject"] * 2
    for name, headers in sent:
        assert headers.get("x-amz-expected-bucket-owner") == _ME, name
    assert connects == []


def test_the_infra_deploy_creates_no_stack_in_another_accounts_bucket(
    tmp_path: Path, monkeypatch
) -> None:
    from dsql_migrator.core.cdc_deployer import run_cdc_infra_deploy
    from tests.test_g25_s3_deploy import _Handle, _mysql_infra_params

    _artifacts(tmp_path, monkeypatch)
    s3 = OwnerCheckingS3()
    _foreign(s3)
    created: list = []
    deployer = SimpleNamespace(
        region=_REGION, template_s3_bucket=None,
        describe_stack_or_none=lambda _n: None,
        create_stack=lambda *a, **k: created.append(a),
    )
    log: list[str] = []
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_infra_deploy(
            _Handle(), stack_name="dsql-cdc-owner", template_body="T",
            params=_mysql_infra_params(), deployer=deployer,
            on_log=lambda _ts, msg: log.append(msg), region=_REGION,
            s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None,
        )
    _assert_refusal(str(exc.value))
    assert created == [] and deployer.template_s3_bucket is None
    assert any("another AWS account" in line for line in log)  # the deploy log says why


# ---------------------------------------------------------------------------
# The cdc-stack template (Deploy CDC infrastructure, Start CDC)
# ---------------------------------------------------------------------------


class _Cfn:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def describe_stacks(self, **_kw):
        self.calls.append("describe_stacks")
        return {"Stacks": [{"StackStatus": "UPDATE_COMPLETE", "Parameters": []}]}

    def create_stack(self, **kw):
        self.calls.append("create_stack")
        self.created = kw
        return {}

    def update_stack(self, **kw):
        self.calls.append("update_stack")
        self.updated = kw
        return {}


def _deployer(s3: OwnerCheckingS3, sts=None, region: str = _REGION):
    cfn = _Cfn()
    clients = {"s3": s3, "sts": sts or _Sts(), "cloudformation": cfn}
    session = SimpleNamespace(client=lambda name, region_name=None: clients[name])
    deployer = CdcStackDeployer(region, session=session)
    deployer.template_s3_bucket = plugin_bucket_name(_ME, region)
    return deployer, cfn


_BIG = "x" * 60000  # over CloudFormation's 51,200-byte inline limit, as the real template is


def test_the_template_upload_asserts_the_deployers_own_account() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    sts = _Sts()
    deployer, cfn = _deployer(s3, sts)
    deployer.create_stack("dsql-cdc-owner", _BIG, [("VpcId", "vpc-1")])
    deployer.submit_update("dsql-cdc-owner", [("DeploySink", "true")], template_body=_BIG)
    puts = [p for op, p in s3.calls if op == "PutObject"]
    assert [p["ExpectedBucketOwner"] for p in puts] == [_ME, _ME]
    assert sts.calls == 1  # read once per deployer
    assert cfn.created["TemplateURL"].startswith(f"https://{_BUCKET}.s3.{_REGION}.")
    assert cfn.updated["TemplateURL"] == cfn.created["TemplateURL"]
    assert s3.violations == []


@pytest.mark.parametrize("action", ["create", "update"])
def test_a_template_bucket_another_account_owns_stops_the_deploy(action) -> None:
    # FAILS WITHOUT THE FIX: the template went into the squatter's bucket, and CloudFormation
    # ran whatever that bucket then held with CAPABILITY_NAMED_IAM, as CdcDeployRole.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    deployer, cfn = _deployer(s3)
    with pytest.raises(CdcDeployError) as exc:
        if action == "create":
            deployer.create_stack("dsql-cdc-owner", _BIG, [("VpcId", "vpc-1")])
        else:
            deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    _assert_refusal(str(exc.value))
    assert str(exc.value).startswith("The cdc-stack template was not staged")
    assert "create_stack" not in cfn.calls and "update_stack" not in cfn.calls
    assert theirs.objects == {}
    assert s3.violations == []


def test_start_cdc_updates_no_stack_from_another_accounts_template_bucket() -> None:
    # Start CDC sends the bundled template (over the inline limit) through the REAL deployer:
    # the refusal reaches the deploy log, and UpdateStack is never called.
    from dsql_migrator.core.cdc import (
        DebeziumSourceConfig,
        SinkConnectorConfig,
        build_cdc_stack_params,
    )
    from dsql_migrator.core.cdc_deployer import run_cdc_start
    from tests import test_g25_s3_deploy as g25

    cfn = g25._FakeCfn(g25._deployed("mysql", "content"))
    s3 = OwnerCheckingS3(caller=g25._ACCOUNT)
    theirs = s3.bucket(g25._BUCKET, _THEM, lets_me_in=True)
    session = g25._FakeSession(cfn)
    session._clients.update({"s3": s3, "sts": _Sts(g25._ACCOUNT)})
    deployer = CdcStackDeployer(g25._REGION, session=session)
    deployer.template_s3_bucket = g25._BUCKET
    params = build_cdc_stack_params(
        DebeziumSourceConfig(name="mysql-source", table_include_list=["shop.orders"]),
        SinkConnectorConfig(name="mysql-sink", topics=["shop.orders"], dlq_topic="dsql-sink-dlq"),
        target_endpoint="c.dsql.us-east-1.on.aws",
        stack_name=g25._STACK,
    )
    log: list[str] = []
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(
            g25._Handle(), stack_name=g25._STACK, params=params, deployer=deployer,
            on_log=lambda _ts, msg: log.append(msg), template_body=_BIG,
            connector_timeout_seconds=5.0, poll_interval_seconds=0.0, sleep=lambda _s: None,
        )
    _assert_refusal(str(exc.value), g25._BUCKET)
    assert cfn.updates == []
    assert theirs.objects == {}
    assert any("another AWS account" in line for line in log)
    assert s3.violations == []


def test_a_template_upload_without_the_account_is_refused() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    deployer, cfn = _deployer(s3, _NoSts())
    with pytest.raises(CdcDeployError) as exc:
        deployer.create_stack("dsql-cdc-owner", _BIG, [("VpcId", "vpc-1")])
    assert "sts:GetCallerIdentity" in str(exc.value)
    assert s3.calls == [] and cfn.calls == []


def test_a_missing_template_bucket_says_what_to_do() -> None:
    s3 = OwnerCheckingS3()
    deployer, cfn = _deployer(s3)
    with pytest.raises(CdcDeployError) as exc:
        deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    assert "does not exist" in str(exc.value) and "Deploy CDC" in str(exc.value)
    assert "update_stack" not in cfn.calls


def test_a_small_template_needs_no_bucket_and_no_account() -> None:
    s3 = OwnerCheckingS3()
    deployer, cfn = _deployer(s3, _NoSts())
    deployer.create_stack("dsql-cdc-owner", "small", [("VpcId", "vpc-1")])
    assert cfn.created["TemplateBody"] == "small"
    assert s3.calls == []


def test_the_template_preflight_stages_it_once_and_the_update_stages_it_again() -> None:
    # Start CDC stages the template before it changes anything (preflight_template); the
    # update stages it again, owner-checked, right before UpdateStack.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    deployer, cfn = _deployer(s3)
    deployer.preflight_template(None)  # UsePreviousTemplate: nothing to stage
    deployer.preflight_template("small")  # sent inline: no bucket needed
    assert s3.calls == []
    deployer.preflight_template(_BIG)
    deployer.preflight_template(_BIG)  # this deployer staged one there: not again
    assert s3.ops() == ["PutObject"]
    deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    assert s3.ops() == ["PutObject", "PutObject"] and "update_stack" in cfn.calls
    assert s3.violations == []


def test_a_template_preflight_in_another_accounts_bucket_is_the_refusal() -> None:
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    deployer, cfn = _deployer(s3)
    for _ in range(2):  # a refusal confirms nothing: the next preflight tries again
        with pytest.raises(CdcDeployError) as exc:
            deployer.preflight_template(_BIG)
        _assert_refusal(str(exc.value))
    assert s3.ops() == ["PutObject", "PutObject"]
    assert theirs.objects == {} and cfn.calls == [] and s3.violations == []


@pytest.mark.parametrize("seed_mode", ["lambda", "external"])
def test_start_cdc_refuses_another_accounts_template_bucket_before_the_kafka_prep(
    seed_mode: str,
) -> None:
    # FAILS WITHOUT THE FIX: the template was first staged in submit_update, after the
    # in-process Kafka prep (SeedMode=External: EC2 hosts, every PostgreSQL source) had
    # created the topics and seeded connect-offsets. run_cdc_start now stages it while it
    # validates, before anything changes.
    from dsql_migrator.core.cdc import (
        DebeziumSourceConfig,
        SinkConnectorConfig,
        build_cdc_stack_params,
    )
    from dsql_migrator.core.cdc_deployer import run_cdc_start
    from tests import test_g25_s3_deploy as g25

    cfn = g25._FakeCfn(g25._deployed("mysql", "content"))
    s3 = OwnerCheckingS3(caller=g25._ACCOUNT)
    theirs = s3.bucket(g25._BUCKET, _THEM, lets_me_in=True)
    session = g25._FakeSession(cfn)
    session._clients.update({"s3": s3, "sts": _Sts(g25._ACCOUNT)})
    deployer = CdcStackDeployer(g25._REGION, session=session)
    deployer.template_s3_bucket = g25._BUCKET
    params = build_cdc_stack_params(
        DebeziumSourceConfig(name="mysql-source", table_include_list=["shop.orders"]),
        SinkConnectorConfig(name="mysql-sink", topics=["shop.orders"], dlq_topic="dsql-sink-dlq"),
        target_endpoint="c.dsql.us-east-1.on.aws",
        stack_name=g25._STACK,
    )
    seeded: list = []
    handle = g25._Handle()
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(
            handle, stack_name=g25._STACK, params=params, deployer=deployer,
            on_log=lambda _ts, _msg: None, template_body=_BIG, seed_mode=seed_mode,
            seed_fn=lambda *a, **k: seeded.append(k) or "seeded",
            connector_timeout_seconds=5.0, poll_interval_seconds=0.0, sleep=lambda _s: None,
        )
    _assert_refusal(str(exc.value), g25._BUCKET)
    assert seeded == [] and cfn.updates == [] and theirs.objects == {}
    assert s3.ops() == ["PutObject"] and s3.violations == []
    # Stopped while validating: the bootstrap brokers were never even read.
    stages = {c.chunk_id: str(c.status) for c in handle.job.chunks}
    assert stages["validate_params"] == "IN_PROGRESS" and stages["fetch_bootstrap"] == "PENDING"


# ---------------------------------------------------------------------------
# What a refusal says, and a 403 on a bucket this account is shown to own
# ---------------------------------------------------------------------------


def test_a_refusal_names_the_refused_request_its_permission_and_s3s_answer(
    tmp_path: Path,
) -> None:
    # FAILS WITHOUT THE FIX: every refusal pointed at "HeadBucket needs s3:ListBucket" and
    # dropped S3's own error -- Start CDC's included, whose CdcDeployRole has no
    # s3:ListBucket (its owner check is the template's PutObject).
    s3 = OwnerCheckingS3()
    _foreign(s3)
    deployer, _cfn = _deployer(s3)
    with pytest.raises(CdcDeployError) as staged:
        deployer.preflight_template(_BIG)
    with pytest.raises(S3ProvisionError) as ensured:
        ensure_plugin_bucket(s3, _ME, _REGION)
    managed = ManagedOwnerCheckingS3()
    _foreign(managed)
    artifact = tmp_path / "a.zip"
    artifact.write_bytes(b"x")
    with pytest.raises(S3ProvisionError) as uploaded:
        upload_plugin(managed, _BUCKET, "cdc-plugins/0/a.zip", artifact, expected_owner=_ME)
    for text, operation, permission in (
        (str(staged.value), "PutObject", "s3:PutObject"),
        (str(ensured.value), "HeadBucket", "s3:ListBucket"),
        (str(uploaded.value), "PutObject", "s3:PutObject"),  # s3transfer's wrapped error
    ):
        _assert_refusal(text)
        assert f"a {operation} request" in text, text
        assert f"allows {permission} on it" in text, text
        assert f"when calling the {operation} operation" in text, text  # S3's answer
        assert "Versions of the tool before this check" in text, text
    assert "s3:ListBucket" not in str(staged.value)


@pytest.mark.parametrize("operation", ["PutObject", "GetObject", "DeleteObject"])
def test_a_403_on_a_bucket_this_account_is_shown_to_own_is_not_another_accounts(
    operation: str, caplog
) -> None:
    # FAILS WITHOUT THE FIX: once the owner-checked HeadBucket had shown the bucket is this
    # account's, a 403 (IAM, a KMS key policy, an SCP) was still logged at ERROR as the
    # ownership refusal, kept in bucket_refusal and told the operator to delete the bucket.
    s3 = OwnerCheckingS3()
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)  # creates the bucket and shows it is this account's
    sessions.save(SessionSnapshot(session_id="s1"))
    s3.iam_denied = {operation}
    with caplog.at_level(logging.WARNING):
        jobs.save(_job("j1", "DONE"), None)
        jobs.delete("j9")
        if operation == "GetObject":
            assert sessions.load("s1") is None  # unreadable: nothing restored
        sessions.save(SessionSnapshot(session_id="s2"))
        sessions.delete("s2")
    assert jobs.bucket_refusal is None and sessions.bucket_refusal is None
    records = [r for r in caplog.records if r.name.startswith("dsql_migrator.core")]
    assert records and all(r.levelno == logging.WARNING for r in records)
    assert not [r for r in records if "another AWS account" in r.getMessage()]
    assert any("AccessDenied" in (r.exc_text or "") for r in records)  # S3's own answer
    assert s3.violations == []


def test_a_confirmed_buckets_403_is_reported_as_it_is_by_the_ledger_and_deployer() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    ledger = S3OverlayLedger(_BUCKET, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None)
    ledger.put_tables("k", {"shop.docs": _ledger_record()})  # the PUT shows it is ours
    deployer, cfn = _deployer(s3)
    deployer.preflight_template(_BIG)
    s3.iam_denied = {"GetObject", "PutObject"}
    read = ledger.read("k")
    assert read.status == "unreadable"
    assert "AccessDenied" in (read.error or "") and "another AWS account" not in read.error
    with pytest.raises(CdcDeployError) as exc:
        deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    assert "could not upload it to S3 bucket" in str(exc.value)
    assert "AccessDenied" in str(exc.value) and "another AWS account" not in str(exc.value)
    assert "update_stack" not in cfn.calls


def test_a_deployer_that_may_not_list_the_bucket_cannot_confirm_it_after_a_403() -> None:
    # Start CDC's CdcDeployRole has no s3:ListBucket, so the HeadBucket that checks a 403 on
    # its confirmed bucket again is refused too: the deployer cannot tell its IAM policy from
    # a bucket another account now owns, and says both -- naming the PutObject it sent and
    # s3:PutObject, never s3:ListBucket.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    s3.iam_denied = {"HeadBucket"}
    deployer, cfn = _deployer(s3)
    deployer.preflight_template(_BIG)  # the PutObject shows the bucket is this account's
    s3.iam_denied = {"HeadBucket", "PutObject"}
    with pytest.raises(CdcDeployError) as exc:
        deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    text = str(exc.value)
    _assert_refusal(text)
    assert "a PutObject request" in text and "allows s3:PutObject on it" in text
    assert "s3:ListBucket" not in text
    assert s3.ops() == ["PutObject", "PutObject", "HeadBucket"]
    assert "update_stack" not in cfn.calls


def _take_over(s3: OwnerCheckingS3, name: str = _BUCKET) -> _Bucket:
    """This account's bucket is deleted (which frees its name), and another account creates
    one by that name with a bucket policy that lets this identity in."""
    del s3.buckets[name]
    return _foreign(s3, name)


def _record_activity(monkeypatch) -> list[tuple[str, dict]]:
    """What the state stores write to the activity log: (action, keywords)."""
    from dsql_migrator.core import job_store, session_state_store

    recorded: list[tuple[str, dict]] = []

    def record(category, action, **kwargs):
        recorded.append((action, {"category": category, **kwargs}))

    monkeypatch.setattr(job_store, "log_activity", record)
    monkeypatch.setattr(session_state_store, "log_activity", record)
    return recorded


@pytest.mark.parametrize("background", [False, True])
def test_a_state_bucket_another_account_takes_over_while_the_app_runs_is_refused(
    background, caplog, monkeypatch
) -> None:
    # FAILS WITHOUT THE FIX: once the startup check had confirmed the bucket, a 403 was taken
    # for IAM -- a plain WARNING, no bucket_refusal, nothing in the activity log -- and the
    # stores never checked the bucket again until a restart (S3 refused every request all the
    # same, so nothing reached the other account's bucket either way).
    recorded = _record_activity(monkeypatch)
    s3 = OwnerCheckingS3()
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions = S3SessionStateStore(
        _BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts(), background=background
    )
    jobs.save(_job("j1"), None)  # creates the bucket and shows it is this account's
    sessions.save(SessionSnapshot(session_id="s1"))
    assert sessions.flush(5.0)
    theirs = _take_over(s3)
    with caplog.at_level(logging.WARNING):
        jobs.save(_job("j1", "DONE"), None)
        sessions.save(SessionSnapshot(session_id="s2"))
        assert sessions.flush(5.0)
        assert sessions.load("s1") is None
    assert theirs.objects == {} and s3.reads == [] and s3.violations == []
    _assert_refusal(jobs.bucket_refusal or "")
    _assert_refusal(sessions.bucket_refusal or "")
    stores = ("dsql_migrator.core.job_store", "dsql_migrator.core.session_state_store")
    logged = [r for r in caplog.records if r.name in stores]
    assert logged and all(r.levelno == logging.ERROR for r in logged)
    assert [action for action, _kw in recorded] == [
        "Job state is not saved: S3 bucket refused",
        "Session state is not saved: S3 bucket refused",
    ]
    # Each store checks the bucket again before its next save, and records nothing new...
    s3.calls.clear()
    jobs.save(_job("j2"), None)
    assert s3.ops() == ["HeadBucket"] and len(recorded) == 2
    # ...so once the other account's bucket is gone, it creates this account's and saves.
    del s3.buckets[_BUCKET]
    jobs.save(_job("j3"), None)
    sessions.save(SessionSnapshot(session_id="s3"))
    assert sessions.flush(5.0)
    assert s3.buckets[_BUCKET].owner == _ME
    assert {"jobs/j3.json", "sessions/s3.json"} <= set(s3.buckets[_BUCKET].objects)
    assert jobs.bucket_refusal is None and sessions.bucket_refusal is None
    assert s3.violations == []
    # A takeover after the bucket was usable again is a new refusal, recorded again.
    _take_over(s3)
    jobs.save(_job("j4"), None)
    sessions.save(SessionSnapshot(session_id="s4"))
    assert sessions.flush(5.0)
    assert [action for action, _kw in recorded][2:] == [
        "Job state is not saved: S3 bucket refused",
        "Session state is not saved: S3 bucket refused",
    ]


def test_the_ledger_and_the_deployer_refuse_a_bucket_taken_over_after_they_reached_it() -> None:
    # As for the state stores: the ledger read "unreadable" with a bare AccessDenied, and the
    # deployer's update said only that the upload failed.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    ledger = S3OverlayLedger(_BUCKET, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None)
    ledger.put_tables("k", {"shop.docs": _ledger_record()})  # the PUT shows it is ours
    deployer, cfn = _deployer(s3)
    deployer.preflight_template(_BIG)
    theirs = _take_over(s3)
    s3.reads.clear()  # the ledger's own read-back of its write
    read = ledger.read("k")
    assert read.status == "unreadable"
    _assert_refusal(read.error or "")
    with pytest.raises(OverlayLedgerError) as written:
        ledger.put_tables("k", {"shop.docs": _ledger_record()})
    _assert_refusal(str(written.value))
    with pytest.raises(CdcDeployError) as staged:
        deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    _assert_refusal(str(staged.value))
    assert "update_stack" not in cfn.calls
    assert theirs.objects == {} and s3.reads == [] and s3.violations == []


def test_the_plugin_uploads_after_the_bucket_is_confirmed_report_a_403_as_it_is(
    tmp_path: Path, monkeypatch
) -> None:
    _artifacts(tmp_path, monkeypatch)
    s3 = OwnerCheckingS3()
    s3.iam_denied = {"PutObject"}
    with pytest.raises(S3ProvisionError) as exc:
        ensure_and_upload_plugins(s3, _Sts(), _REGION)
    assert str(exc.value).startswith("Failed to upload cdc-plugins/"), exc.value
    assert "AccessDenied" in str(exc.value) and "another AWS account" not in str(exc.value)


# ---------------------------------------------------------------------------
# Job state (Fargate: DSQL_MIGRATOR_JOB_STATE_BUCKET is the managed bucket's name)
# ---------------------------------------------------------------------------


def _job(job_id: str, status: str = "RUNNING") -> MigrationJob:
    chunk = ChunkState(chunk_id="orders", status="DONE", rows_loaded=1)  # type: ignore[arg-type]
    return MigrationJob(job_id=job_id, status=status, chunks=[chunk])  # type: ignore[arg-type]


def _forged_job(bucket: _Bucket) -> None:
    body = json.dumps({"job": _job("forged", "DONE").model_dump_json(), "error": None})
    bucket.objects["jobs/forged.json"] = (body.encode(), {"status": "DONE"})


def test_the_job_store_uses_and_restores_nothing_from_another_accounts_bucket(caplog) -> None:
    # FAILS WITHOUT THE FIX: the store adopted the bucket, restored the forged job (its DONE
    # chunks are skipped on resume), and saved every job snapshot into it.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    _forged_job(theirs)
    planted = dict(theirs.objects)
    store = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    with caplog.at_level(logging.ERROR, logger="dsql_migrator.core.job_store"):
        assert store.load_all() == []
        store.save(_job("j1"), None)
        assert store.prune_terminal(0) == []
        store.delete("forged")
    assert theirs.objects == planted
    assert s3.reads == []
    assert s3.violations == []
    _assert_refusal(store.bucket_refusal or "")
    errors = [
        r for r in caplog.records
        if r.levelno == logging.ERROR and r.name == "dsql_migrator.core.job_store"
    ]
    assert errors and all("cannot resume" in r.getMessage() for r in errors)


def test_the_job_store_without_the_account_makes_no_s3_call() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    store = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_NoSts())
    store.save(_job("j1"), None)
    assert store.load_all() == []
    store.delete("j1")
    assert store.prune_terminal(0) == []
    assert s3.calls == []
    assert "sts:GetCallerIdentity" in (store.bucket_refusal or "")


def test_the_job_store_asserts_the_owner_on_every_request() -> None:
    s3 = OwnerCheckingS3(region="eu-west-1")
    sts = _Sts()
    name = plugin_bucket_name(_ME, "eu-west-1")
    store = S3JobStore(name, region="eu-west-1", s3_client=s3, sts_client=sts)
    store.save(_job("j1", "DONE"), None)  # creates the missing bucket, then checks it
    store.save(_job("j2"), "boom")
    assert [p.job.job_id for p in store.load_all()] == ["j1", "j2"]
    assert store.prune_terminal(0) == ["j1"]
    store.delete("j2")
    assert s3.buckets[name].objects == {}
    assert s3.ops()[:3] == ["HeadBucket", "CreateBucket", "HeadBucket"]
    assert s3.violations == [] and sts.calls == 1
    assert store.bucket_refusal is None


def test_the_job_store_refuses_a_name_another_account_creates_first() -> None:
    s3 = OwnerCheckingS3()
    s3.squatter_creates_first = _THEM
    store = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    store.save(_job("j1"), None)
    assert s3.buckets[_BUCKET].objects == {}
    assert "BucketAlreadyExists" in (store.bucket_refusal or "")


# ---------------------------------------------------------------------------
# Session state (Fargate: DSQL_MIGRATOR_SESSION_STATE_BUCKET is the managed bucket's name)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("background", [False, True])
def test_the_session_store_uses_and_restores_nothing_from_another_accounts_bucket(
    background, caplog
) -> None:
    # FAILS WITHOUT THE FIX: a reconnecting browser restored the squatter's snapshot (its
    # edited target DDL and CDC start point are acted on), and every save went to them.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    forged = SessionSnapshot(session_id="s1", generated_node_ids=["table:forged"])
    theirs.objects["sessions/s1.json"] = (forged.model_dump_json().encode(), {})
    planted = dict(theirs.objects)
    store = S3SessionStateStore(
        _BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts(), background=background
    )
    with caplog.at_level(logging.ERROR, logger="dsql_migrator.core.session_state_store"):
        assert store.load("s1") is None
        store.save(SessionSnapshot(session_id="s2"))
        assert store.flush(5.0)
        assert store.prune(0) == []
        store.delete("s1")
    assert theirs.objects == planted
    assert s3.reads == []
    assert s3.violations == []
    _assert_refusal(store.bucket_refusal or "")
    assert any(
        r.levelno == logging.ERROR and "starts over" in r.getMessage() for r in caplog.records
    )


def test_the_session_store_without_the_account_makes_no_s3_call() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    store = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_NoSts())
    store.save(SessionSnapshot(session_id="s1"))
    assert store.load("s1") is None
    store.delete("s1")
    assert store.prune(0) == []
    assert s3.calls == []
    assert "sts:GetCallerIdentity" in (store.bucket_refusal or "")


def test_the_session_store_asserts_the_owner_on_every_request() -> None:
    s3 = OwnerCheckingS3()
    store = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    store.save(SessionSnapshot(session_id="s1", generated_node_ids=["table:t"]))
    loaded = store.load("s1")
    assert loaded is not None and loaded.generated_node_ids == ["table:t"]
    assert store.load("missing") is None
    assert store.prune(0) == ["s1"]
    store.save(SessionSnapshot(session_id="s2"))
    store.delete("s2")
    assert s3.buckets[_BUCKET].objects == {}
    assert s3.violations == []


# ---------------------------------------------------------------------------
# The overlay ledger (in the session bucket)
# ---------------------------------------------------------------------------


def _ledger_record() -> OverlayLedgerTable:
    return OverlayLedgerTable(
        table="shop.docs", set_digest="d", set_columns=["body"],
        applied_at=_EPOCH,
        columns=[OverlayLedgerColumn(column="body", original_default="'x'::text")],
    )


def test_the_overlay_ledger_reads_another_accounts_bucket_as_unreadable() -> None:
    # FAILS WITHOUT THE FIX: the forged record was read as present, and cut over restored the
    # DEFAULTs it named.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    record = json.loads(_ledger_record().model_dump_json())
    forged = {"schema_version": 1, "tables": {"shop.docs": record}}
    theirs.objects["overlay-ledger/k.json"] = (json.dumps(forged).encode(), {})
    planted = dict(theirs.objects)
    ledger = S3OverlayLedger(_BUCKET, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None)
    read = ledger.read("k")
    assert read.status == "unreadable"
    _assert_refusal(read.error or "")
    with pytest.raises(OverlayLedgerError) as exc:
        ledger.put_tables("k", {"shop.docs": _ledger_record()})
    _assert_refusal(str(exc.value))
    assert theirs.objects == planted
    assert s3.reads == []
    assert s3.violations == []


def test_the_overlay_ledger_without_the_account_is_unreadable_and_makes_no_s3_call() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    ledger = S3OverlayLedger(_BUCKET, s3_client=s3, sts_client=_NoSts(), sleep=lambda _s: None)
    read = ledger.read("k")
    assert read.status == "unreadable" and "sts:GetCallerIdentity" in (read.error or "")
    with pytest.raises(OverlayLedgerError):
        ledger.put_tables("k", {"shop.docs": _ledger_record()})
    assert s3.calls == []


def test_the_overlay_ledger_asserts_the_owner_on_every_request() -> None:
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    ledger = S3OverlayLedger(_BUCKET, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None)
    assert ledger.read("k").status == "absent"
    ledger.put_tables("k", {"shop.docs": _ledger_record()})
    assert ledger.read("k").tables["shop.docs"] == _ledger_record()
    assert s3.violations == []
    assert {p["ExpectedBucketOwner"] for _op, p in s3.calls} == {_ME}


# ---------------------------------------------------------------------------
# A refused state bucket is recorded in the activity log
# ---------------------------------------------------------------------------


def test_a_refused_state_bucket_is_recorded_in_the_activity_log_once(monkeypatch) -> None:
    # Each store records its own refusal when it first meets it -- at startup, its restore or
    # prune -- so no caller has to remember to report it, and a refusal while the app runs is
    # recorded too (test_a_state_bucket_another_account_takes_over_while_the_app_runs_...).
    from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus

    recorded = _record_activity(monkeypatch)
    s3 = OwnerCheckingS3()
    _foreign(s3)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.load_all()
    jobs.save(_job("j1"), None)
    jobs.prune_terminal(0)
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions.prune(200)
    sessions.save(SessionSnapshot(session_id="s1"))
    assert sessions.load("s1") is None
    assert [action for action, _kw in recorded] == [
        "Job state is not saved: S3 bucket refused",
        "Session state is not saved: S3 bucket refused",
    ]
    for _action, kwargs in recorded:
        assert kwargs["category"] is ActivityCategory.SYSTEM
        assert kwargs["status"] is ActivityStatus.FAILURE
        _assert_refusal(kwargs["detail"])
    # A usable bucket records nothing; nor does a 403 that is this identity's IAM policy.
    recorded.clear()
    own = OwnerCheckingS3()
    usable = S3JobStore(_BUCKET, region=_REGION, s3_client=own, sts_client=_Sts())
    usable.load_all()
    own.iam_denied = {"PutObject"}
    usable.save(_job("j1"), None)
    assert recorded == [] and usable.bucket_refusal is None


class _StsFailsFirst(_Sts):
    """STS that fails its first call (throttled at startup, say), then answers."""

    def get_caller_identity(self):
        self.calls += 1
        if self.calls == 1:
            raise ClientError(
                {"Error": {"Code": "Throttling", "Message": "Rate exceeded"}},
                "GetCallerIdentity",
            )
        return super().get_caller_identity()


@pytest.mark.parametrize("store", ["jobs", "sessions"])
def test_a_foreign_bucket_after_an_unreadable_account_is_recorded_too(
    store: str, monkeypatch
) -> None:
    # FAILS WITHOUT THE FIX: the activity log took only the first refusal since the bucket was
    # last usable, so after an STS failure at startup the bucket another account owns -- the
    # refusal that matters -- reached the ERROR log only, under the STS entry titled "S3
    # bucket refused" although no S3 request had been sent. Each kind is recorded once now.
    recorded = _record_activity(monkeypatch)
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    sts = _StsFailsFirst()
    if store == "jobs":
        jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=sts)
        jobs.load_all()  # the account read fails: nothing is sent
        assert s3.calls == []
        jobs.save(_job("j1"), None)  # the account is read: the bucket is another account's
        jobs.save(_job("j2"), None)
        refusal, prefix = jobs.bucket_refusal, "Job"
    else:
        sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=sts)
        sessions.prune(200)
        assert s3.calls == []
        sessions.save(SessionSnapshot(session_id="s1"))
        assert sessions.load("s1") is None
        sessions.save(SessionSnapshot(session_id="s2"))
        refusal, prefix = sessions.bucket_refusal, "Session"
    assert [action for action, _kw in recorded] == [
        f"{prefix} state is not saved: AWS account ID unreadable",
        f"{prefix} state is not saved: S3 bucket refused",
    ]
    assert "sts:GetCallerIdentity" in recorded[0][1]["detail"]
    _assert_refusal(recorded[1][1]["detail"])
    assert recorded[1][1]["detail"] == refusal
    assert theirs.objects == {} and s3.reads == [] and s3.violations == []


# ---------------------------------------------------------------------------
# A refused state bucket is shown above every view
# ---------------------------------------------------------------------------


def test_the_state_bucket_notice_says_what_is_lost_and_why() -> None:
    from dsql_migrator.core.s3_ownership import not_owned_message
    from dsql_migrator.ui.workflow import state_bucket_notice

    refusal = not_owned_message(_BUCKET, _ME)
    other = "Could not read this identity's AWS account ID (sts:GetCallerIdentity: x)."
    assert state_bucket_notice(None, None) is None
    header, body = state_bucket_notice(refusal, None)
    assert header == "Job state is not saved"
    assert "an interrupted Full Load cannot resume" in body and "browser" not in body
    assert body.endswith(refusal)
    header, body = state_bucket_notice(None, refusal)
    assert header == "Session state is not saved"
    assert "a reconnecting browser starts over" in body and "Full Load" not in body
    assert body.endswith(refusal)
    # One bucket for both (the ECS Fargate default) is one refusal, said once...
    header, body = state_bucket_notice(refusal, refusal)
    assert header == "Job and session state are not saved"
    assert "cannot resume" in body and "browser starts over" in body
    assert body.count(refusal) == 1
    # ...and two different ones say which is which.
    _header, body = state_bucket_notice(refusal, other)
    assert f"Job state: {refusal} Session state: {other}" in body


class _NoPanel:
    """The AI panel, off (the shell builds it; the notice does not need it)."""

    def is_enabled(self) -> bool:
        return False

    def __getattr__(self, _name):
        return lambda *_a, **_k: None


def _shell(monkeypatch, refusals):
    """The real shell (build_workflow_sidebar) on its Connect view, with a NiceGUI double; the
    elements it built."""
    import types

    from dsql_migrator.ui import workflow as wf
    from dsql_migrator.ui.session import SessionStore
    from tests.test_ui_connect_ai_status import _io_bound, _Ui

    ui = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    fake.run = SimpleNamespace(  # type: ignore[attr-defined]
        io_bound=_io_bound, cpu_bound=_io_bound
    )
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(wf, "build_ai_panel", lambda *_a, **_k: _NoPanel())
    wf.build_workflow_sidebar(
        SessionStore(), "s3own", app_title="t", version="v",
        connect_builder=lambda *_a: ui.label("CONNECT FORM"),
        state_bucket_refusals_getter=lambda: refusals,
    )
    return ui.els


def test_the_shell_shows_a_refused_state_bucket_above_every_view(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the refusal reached only the ERROR log and the activity log
    # (downloadable from Settings), so the app ran Full Loads with their state unsaved and
    # said nothing on the page.
    import ast
    import inspect
    import textwrap

    from dsql_migrator.core.s3_ownership import not_owned_message
    from dsql_migrator.ui import workflow as wf
    from dsql_migrator.ui.design import notice_box_classes

    refusal = not_owned_message(_BUCKET, _ME)
    els = _shell(monkeypatch, (refusal, refusal))
    labels = [el.text for el in els if el.kind == "label"]
    assert labels.index("Job and session state are not saved") < labels.index("CONNECT FORM")
    (box,) = [
        el for el in els if el.kind == "row" and el.classes_str == notice_box_classes("error")
    ]
    texts = [el.text for el in box.children[1].children]
    assert texts[0] == "Job and session state are not saved" and texts[1].endswith(refusal)
    # Nothing while both stores use their bucket.
    els = _shell(monkeypatch, (None, None))
    assert not [el for el in els if el.kind == "label" and "not saved" in el.text]
    # Drawn before render_main branches on the view: on Connect, every step and the tools.
    tree = ast.parse(textwrap.dedent(inspect.getsource(wf.build_workflow_sidebar)))
    (render_main,) = [
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "render_main"
    ]
    body = [ast.unparse(statement) for statement in render_main.body]
    banner = next(i for i, s in enumerate(body) if s.startswith("_render_state_bucket_banner("))
    first_branch = next(i for i, s in enumerate(render_main.body) if isinstance(s, ast.If))
    assert banner < first_branch


def test_the_page_reads_each_state_stores_refusal_when_it_renders(monkeypatch) -> None:
    # build_page hands the shell a getter of both stores' bucket_refusal, read at each render:
    # a bucket refused at startup (the job restore, the session prune) is on the first page,
    # and one refused while the app runs, on the next render.
    import types

    from dsql_migrator.core.job_manager import JobManager
    from tests.test_ai_panel import _Ui as _PageUi

    _record_activity(monkeypatch)
    fake = types.ModuleType("nicegui")
    fake.ui = _PageUi()  # type: ignore[attr-defined]
    fake.run = SimpleNamespace(io_bound=None, cpu_bound=None)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    from dsql_migrator.ui import app as app_module

    hooks: dict = {}

    def _spy_sidebar(*_a, **kwargs):  # noqa: ANN002
        hooks.update(kwargs)
        return (None, lambda *_a, **_k: None)

    monkeypatch.setattr(app_module, "build_workflow_sidebar", _spy_sidebar)
    s3 = OwnerCheckingS3()
    _foreign(s3)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    monkeypatch.setattr(app_module, "JOB_MANAGER", JobManager(stall_timeout_seconds=None))
    monkeypatch.setattr(app_module, "SESSION_STATE_STORE", sessions)
    getter = app_module._state_bucket_refusals
    assert getter() == (None, None)  # SQLite-less, store-less: nothing to show
    app_module.JOB_MANAGER.attach_store(jobs)  # main(): the restore is refused
    sessions.prune(200)  # main(): so is the prune
    session_id = "s3own-page"
    try:
        app_module.build_page(app_module.AppConfig(), session_id)
    finally:
        for store in ("SESSION_STORE", "DATA_MIGRATION_STORE"):
            getattr(app_module, store).clear(session_id)
    assert hooks["state_bucket_refusals_getter"] is getter
    _assert_refusal(getter()[0] or "")
    _assert_refusal(getter()[1] or "")
    assert getter() == (jobs.bucket_refusal, sessions.bucket_refusal)
    assert s3.reads == [] and s3.violations == []


_STARTUP_HARNESS = Path(__file__).with_name("_startup_state_buckets_harness.py")


def test_the_apps_startup_records_each_refused_state_bucket_and_shows_it_on_the_page(
    tmp_path: Path,
) -> None:
    # The guides and the configuration help promise that the activity log records the
    # refusal at startup, and that the app shows it above every screen. The app's real main()
    # runs (in its own interpreter, as main() changes process-wide state) with both state
    # buckets set, as on ECS Fargate, and every AWS request refused before it is sent: neither
    # store can read this identity's account, so each refuses its bucket at its first request
    # -- the job restore and the session prune -- and no S3 request is sent.
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_CURRENT_TEST"}
    out = subprocess.run(
        [sys.executable, str(_STARTUP_HARNESS), str(tmp_path)],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=240, check=False,
    )
    assert out.returncode == 0, out.stderr[-4000:]
    result = json.loads(out.stdout.strip().splitlines()[-1])
    refused = [
        r for r in result["records"] if " state is not saved: " in str(r.get("action", ""))
    ]
    # FAILS WITHOUT THE FIX: both were titled "S3 bucket refused" although no S3 request was
    # sent -- the account read failed.
    assert [(r["action"], r["status"]) for r in refused] == [
        ("Job state is not saved: AWS account ID unreadable", "failure"),
        ("Session state is not saved: AWS account ID unreadable", "failure"),
    ]
    assert all("sts:GetCallerIdentity" in r["detail"] for r in refused)
    # Only STS was asked: no S3 request goes out without the account to assert.
    assert result["aws_refused"]
    assert all(urlsplit(url).hostname.startswith("sts.") for url in result["aws_refused"])
    # FAILS WITHOUT THE FIX: nothing on the page said so (the startup above is what main()
    # leaves for the first page render: build_page hands _state_bucket_refusals to the shell).
    header, body = result["notice"]
    assert header == "Job and session state are not saved"
    assert "cannot resume" in body and "sts:GetCallerIdentity" in body
    assert body.count("sts:GetCallerIdentity") == 1  # one refusal for both: said once


# ---------------------------------------------------------------------------
# The deployment guides: the template bucket and the plugin bucket's teardown
# ---------------------------------------------------------------------------

_GUIDES = ("DEPLOYMENT.md", "DEPLOYMENT.ko.md", "DEPLOYMENT.ja.md")
_TEMPLATE_OWNER_CHECK = (
    'aws s3api head-bucket --bucket "$TEMPLATE_BUCKET" --expected-bucket-owner "$ACCOUNT"'
)
_REPO = _SRC.parents[1]


def _guide(name: str) -> str:
    return (_REPO / "deploy" / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", _GUIDES)
def test_the_guides_template_bucket_is_unpredictable_and_owner_checked(name: str) -> None:
    # FAILS WITHOUT THE FIX: the guides named the bucket after the account and Region and
    # created it with `aws s3 mb ... 2>/dev/null || true`, so a bucket another account had
    # created first was used anyway -- and `aws cloudformation deploy` skips its upload when
    # the template's MD5-named key already exists, so a planted template was deployed with
    # CAPABILITY_NAMED_IAM under the operator's own credentials.
    text = _guide(name)
    assert "2>/dev/null || true" not in text
    assert "mysql-dsql-migrator-templates-$ACCOUNT-$AWS_REGION" not in text
    names = re.findall(r"^export TEMPLATE_BUCKET=(.+)$", text, re.M)
    assert len(names) == 2, names  # the Fargate and the EC2 deploy
    assert set(names) == {"mysql-dsql-migrator-templates-$ACCOUNT-$(openssl rand -hex 4)"}
    # At most 63 characters, S3's limit: 30 + a 12-digit account + 1 + 8 hex digits.
    assert len("mysql-dsql-migrator-templates-") + 12 + 1 + 8 <= 63
    # Every deploy from the bucket is chained to an owner check of its own (what that does
    # when the check fails: test_every_deploy_and_teardown_runs_only_after_its_owner_check).
    chained = f"{_TEMPLATE_OWNER_CHECK} \\\n  && aws cloudformation deploy"
    deploys = re.findall(r'--s3-bucket "\$TEMPLATE_BUCKET"', text)
    # The app stack, its image update, its AI-assist redeploy and the EC2 stack.
    assert len(deploys) == 4, name
    assert text.count(chained) == 4, name
    # The coding-agent prompts ask for the same.
    flat = " ".join(text.split())
    assert flat.count("aws s3api head-bucket --expected-bucket-owner") == 2, name
    # ...and the operator who still has the old guide's predictable bucket is told what to do.
    assert text.count("mysql-dsql-migrator-templates-<account>-<region>") == 2, name


# Each guide's "Ran an earlier version in this account?" note: its title, then what it must say
# (whitespace aside: the lines wrap differently in each language).
_EARLIER_VERSION_NOTE = {
    "DEPLOYMENT.md": (
        "**Ran an earlier version in this account?**",
        "the record of dropped DEFAULTs (the overlay ledger)",
        "rotate the source database credentials that the source secret holds",
        "check the target's schema, DEFAULTs and data against the Schema Conversion output",
    ),
    "DEPLOYMENT.ko.md": (
        "**이 계정에서 이전 버전을 실행했다면?**",
        "제거된 DEFAULT의 기록(오버레이 원장)",
        "소스 시크릿이 담은 소스 데이터베이스 자격 증명을 교체",
        "타깃의 스키마·DEFAULT·데이터를 Schema Conversion 출력과 대조",
    ),
    "DEPLOYMENT.ja.md": (
        "**このアカウントで以前のバージョンを実行した場合は?**",
        "削除された DEFAULT の記録（オーバーレイ台帳）",
        "ソースシークレットが保持するソースデータベースの認証情報をローテーション",
        "ターゲットのスキーマ・DEFAULT・データを Schema Conversion の出力と照合",
    ),
}


def test_the_earlier_version_steps_cover_the_source_credentials_and_the_target() -> None:
    # FAILS WITHOUT THE FIX: the untrusted list left out the overlay ledger, and the steps
    # (delete the CDC infrastructure, review, redeploy) rotated no credential and checked
    # nothing on the target -- though the connector plugins ran with a role that can read the
    # source secret and connect to the cluster as admin, a Full Load applies a restored
    # session's edited target DDL, and an earlier Cut over restored a ledger record's DEFAULT.
    from dsql_migrator.core.s3_ownership import not_owned_message

    def flat(text: str) -> str:
        return re.sub(r"\s+", "", text)

    for name, (title, *says) in _EARLIER_VERSION_NOTE.items():
        text = _guide(name)
        note = text[text.index(title):text.index("\n- **", text.index(title))]
        for phrase in says:
            assert flat(phrase) in flat(note), (name, phrase)
    # The refusal itself says the same.
    refusal = not_owned_message(_BUCKET, _ME)
    assert "the record of dropped DEFAULTs" in refusal
    assert "rotate the source database credentials" in refusal
    assert "check the target's schema, DEFAULTs and data" in refusal


@pytest.mark.parametrize("name", _GUIDES)
def test_the_owner_checked_commands_are_ones_the_cli_has(name: str) -> None:
    # `aws s3 mb`, `aws s3 rb`, `aws s3 cp` and `aws cloudformation deploy` take no expected
    # owner, so the check is an s3api command that does (head-bucket; get-object, the EC2
    # host's source download); no guide passes the option to a command without it.
    text = _guide(name)
    uses = [m.start() for m in re.finditer("--expected-bucket-owner", text)]
    assert len(uses) >= 10, (name, len(uses))
    for at in uses:
        command = text[text.rindex("aws ", 0, at):at]
        assert command.startswith(("aws s3api head-bucket", "aws s3api get-object")), (
            name, command
        )


# What a pasted guide command runs: bash, with no `set -e` (as an interactive paste), and a
# stub `aws` whose owner check fails or passes. Nothing else is on PATH, so nothing real runs.
_FENCED_BASH = re.compile(r"^```bash\n(.*?)^```$", re.M | re.S)
_INLINE_CODE = re.compile(r"`([^`\n]+)`")
_STUB_AWS = r"""#!/bin/sh
{ printf 'aws'; for a in "$@"; do printf '\t%s' "$a"; done; printf '\n'; } >> "$STUB_LOG"
case "$1 $2" in
  "sts get-caller-identity") echo "$STUB_ACCOUNT" ;;
  "s3api head-bucket")
    if [ "$STUB_OWNER_OK" != 1 ]; then
      echo "An error occurred (403) when calling the HeadBucket operation: Forbidden" >&2
      exit 254
    fi ;;
esac
exit 0
"""


def _stub_bin(tmp_path: Path, scripts: dict[str, str]) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name, body in scripts.items():
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    return bin_dir


def _logging_stub(name: str, output: str = "") -> str:
    """A stub command that records its arguments and prints ``output``."""
    return (
        "#!/bin/sh\n"
        f"{{ printf '{name}'; for a in \"$@\"; do printf '\\t%s' \"$a\"; done; printf '\\n'; }}"
        ' >> "$STUB_LOG"\n'
        + (f"echo {output}\n" if output else "")
    )


# How each shell runs a paste: bash without its start-up files, and zsh as an interactive
# shell without them (macOS's default shell: interactive, `#` starts no comment there, as
# its INTERACTIVE_COMMENTS option is off).
_SHELLS = {"bash": ("--noprofile", "--norc"), "zsh": ("-f", "-i")}
# The repository's scripts a guide block runs by a relative path: in the directory a paste
# runs in, each is a logging stub, so no pasted block can run the real one.
_GUIDE_SCRIPTS = {
    "build_and_push.sh": "",
    "build_in_codebuild.sh": "",
    "create_test_cert.sh": f"CertificateArn=arn:aws:acm:{_REGION}:{_ME}:certificate/test",
    "teardown.sh": "",
}


def _paste(
    tmp_path: Path, command: str, *, owner_ok: bool, shell: str = "bash",
    probe: tuple[str, ...] = (),
) -> list[list[str]]:
    """Run ``command`` as a guide block pasted into ``shell``, in ``tmp_path``; the stub
    commands it called, with their arguments. ``probe`` names variables whose values the
    paste then reports, as a last call (``envprobe``)."""
    executable = shutil.which(shell)
    assert executable is not None
    bin_dir = _stub_bin(tmp_path, {
        "aws": _STUB_AWS,
        "docker": _logging_stub("docker"),
        "openssl": _logging_stub("openssl", "1a2b3c4d"),
        "envprobe": _logging_stub("envprobe"),
    })
    scripts = tmp_path / "deploy"
    scripts.mkdir(exist_ok=True)
    for script, output in _GUIDE_SCRIPTS.items():
        (scripts / script).write_text(_logging_stub(f"deploy/{script}", output))
        (scripts / script).chmod(0o755)
    log = tmp_path / "calls.log"
    log.write_text("")
    env = {
        "PATH": str(bin_dir), "HOME": str(tmp_path), "STUB_LOG": str(log),
        "STUB_OWNER_OK": "1" if owner_ok else "0", "STUB_ACCOUNT": _ME,
        "ACCOUNT": _ME, "AWS_REGION": _REGION, "ECR_REPO": "mysql-dsql-migrator",
        "TEMPLATE_BUCKET": f"mysql-dsql-migrator-templates-{_ME}-1a2b3c4d",
    }
    if probe:
        command += "\nenvprobe " + " ".join(f'"${name}"' for name in probe) + "\n"
    subprocess.run(
        [executable, *_SHELLS[shell]], input=command.encode("utf-8"), env=env,
        cwd=tmp_path, capture_output=True, timeout=60, check=False,
    )
    return [line.split("\t") for line in log.read_text().splitlines()]


def _arg(call: list[str], flag: str) -> str:
    return call[call.index(flag) + 1]


def _gated_commands(text: str) -> tuple[list[str], list[str]]:
    """The guide's commands that deploy from a bucket (fenced blocks with
    ``aws cloudformation deploy --s3-bucket``) and that delete one (``aws s3 rb``, fenced
    or inline)."""
    blocks = _FENCED_BASH.findall(text)
    deploys = [b for b in blocks if "aws cloudformation deploy" in b and "--s3-bucket" in b]
    deletes = [b for b in blocks if "aws s3 rb" in b]
    deletes += [c for c in _INLINE_CODE.findall(text) if "aws s3 rb " in c and "&&" in c]
    return deploys, deletes


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("name", [*_GUIDES, "cdc-stack/README.md"])
def test_every_deploy_and_teardown_runs_only_after_its_owner_check(
    name: str, tmp_path: Path
) -> None:
    # FAILS WITHOUT THE FIX: the image update kept the check and the deploy in one block,
    # with only a comment between them, so a pasted block deployed after a 403; the AI-assist
    # redeploy and the cdc-stack README's manual deploy had no check at all. Each command now
    # runs as pasted: the owner check fails -> nothing is deployed or deleted; it passes ->
    # the deploy or delete runs, against the very bucket the check named.
    text = _guide(name)
    deploys, deletes = _gated_commands(text)
    assert (len(deploys), len(deletes)) == ((1, 0) if name.startswith("cdc-stack") else (4, 2))
    for command in deploys + deletes:
        acts = ("cloudformation", "deploy") if command in deploys else ("s3", "rb")
        refused = _paste(tmp_path, command, owner_ok=False)
        checks = [c for c in refused if c[1:3] == ["s3api", "head-bucket"]]
        assert checks and all(
            _arg(c, "--expected-bucket-owner") in (_ME, "<account>") for c in checks
        ), command
        assert not [c for c in refused if tuple(c[1:3]) == acts], command
        ran = [c for c in _paste(tmp_path, command, owner_ok=True) if tuple(c[1:3]) == acts]
        assert len(ran) == 1, command
        target = _arg(ran[0], "--s3-bucket") if acts[0] == "cloudformation" else ran[0][3]
        assert target.removeprefix("s3://") == _arg(checks[-1], "--bucket"), command


@pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("zsh") is None, reason="needs bash and zsh"
)
@pytest.mark.parametrize("name", [*_GUIDES, "cdc-stack/README.md"])
def test_the_guides_shell_blocks_run_alike_when_pasted_into_zsh(
    name: str, tmp_path: Path
) -> None:
    # FAILS WITHOUT THE FIX: pasted into macOS's interactive zsh, where `#` starts no
    # comment, the image update's comment line before its owner check opened a quote at
    # "CloudFormation's", so neither the check nor the deploy ran (the docker build and push
    # before it had); the EC2 setup block stopped the same way before its `aws s3 mb`, and the
    # cdc-stack README's deploy on the comment after its last argument. The AWS CLI deploy's
    # environment block, pasted just before the template bucket's commands, ran
    # deploy/create_test_cert.sh (an ACM certificate import) from its commented-out
    # CERTIFICATE_ARN line, and in English the apostrophe in "DB's" quoted away its VPC and
    # subnet exports; each image build ran with the tag "#" from its trailing comment, and
    # teardown.sh got its trailing comment as arguments. Every shell block of the guides and
    # the cdc-stack README now runs the same commands in zsh as in bash and leaves the same
    # values in the variables it exports, whether a block's owner check passes or fails.
    blocks = _FENCED_BASH.findall(_guide(name))
    buckets = [
        b for b in blocks
        if re.search(r"TEMPLATE_BUCKET|PLUGIN_BUCKET|mysql-dsql-migrator-plugins-", b)
    ]
    expected = (1, 1) if name.startswith("cdc-stack") else (22, 7)
    assert (len(blocks), len(buckets)) == expected, name
    for block in blocks:
        exported = tuple(dict.fromkeys(re.findall(r"^export ([A-Z_][A-Z0-9_]*)=", block, re.M)))
        for owner_ok in (True, False) if "head-bucket" in block else (True,):
            in_bash = _paste(tmp_path, block, owner_ok=owner_ok, probe=exported)
            # (A block with a <placeholder> to edit first runs nothing as written.)
            assert in_bash or block not in buckets, block
            in_zsh = _paste(tmp_path, block, owner_ok=owner_ok, shell="zsh", probe=exported)
            assert in_zsh == in_bash, block


# ---------------------------------------------------------------------------
# The single EC2 host's source download (SourceMode=s3)
# ---------------------------------------------------------------------------

_SOURCE_URI = "s3://my-src-bucket/builds/2026/dsql-src.tar.gz"


def _fn_sub(text: str, values: dict[str, str]) -> str:
    """CloudFormation's Fn::Sub: ``${Name}`` is the value, ``${!Literal}`` is ``${Literal}``."""
    return re.sub(r"\$\{(?!!)([^}]+)\}", lambda m: values[m.group(1)], text).replace("${!", "${")


def _source_fetch_script() -> str:
    """The user data's SourceMode branch, as the host runs it (rendered, under set -e)."""
    from tests.test_ec2_appstack import _user_data

    script = _user_data()
    start = script.index('SOURCE_MODE="${SourceMode}"')
    branch = script[start:script.index("\nfi\n", start) + len("\nfi\n")]
    return "set -euo pipefail\n" + _fn_sub(branch, {
        "SourceMode": "s3", "SourceS3Uri": _SOURCE_URI, "AWS::Region": _REGION,
        "AWS::AccountId": _ME, "SourceRepoRef": "main", "SourceRepoUrl": "https://example.invalid/r.git",
    })


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("owner_ok", [True, False])
def test_the_ec2_host_downloads_its_source_only_from_a_bucket_this_account_owns(
    owner_ok: bool, tmp_path: Path
) -> None:
    # FAILS WITHOUT THE FIX: the user data ran `aws s3 cp "${SourceS3Uri}"`, which cannot
    # assert an owner, and then ran the tarball as root -- from a bucket by that name in
    # another account too. It is now one s3api get-object that names this account as the
    # bucket's owner; when S3 refuses it (403), set -e stops the script before tar.
    get_object = (
        "#!/bin/sh\n"
        "{ printf 'aws'; for a in \"$@\"; do printf '\\t%s' \"$a\"; done; printf '\\n'; }"
        ' >> "$STUB_LOG"\n'
        f"[ {1 if owner_ok else 0} = 1 ] || exit 254\n"
    )
    bin_dir = _stub_bin(tmp_path, {
        "aws": get_object, "tar": _logging_stub("tar"), "chown": _logging_stub("chown"),
        "sudo": _logging_stub("sudo"),
    })
    log = tmp_path / "calls.log"
    log.write_text("")
    done = subprocess.run(
        [shutil.which("bash"), "--noprofile", "--norc"], input=_source_fetch_script(),
        env={"PATH": str(bin_dir), "HOME": str(tmp_path), "STUB_LOG": str(log)},
        capture_output=True, text=True, timeout=60, check=False,
    )
    calls = [line.split("\t") for line in log.read_text().splitlines()]
    assert calls[0] == [
        "aws", "s3api", "get-object", "--region", _REGION,
        "--bucket", "my-src-bucket", "--key", "builds/2026/dsql-src.tar.gz",
        "--expected-bucket-owner", _ME, "/tmp/dsql-src.tar.gz",
    ]
    if owner_ok:
        assert done.returncode == 0, done.stderr
        assert [c[0] for c in calls] == ["aws", "tar", "chown"]
    else:
        assert done.returncode != 0
        assert [c[0] for c in calls] == ["aws"]  # nothing extracted, nothing run


def test_source_mode_s3_grants_the_host_role_that_one_tarball() -> None:
    # The guide's SourceMode=s3 example (s3://$TEMPLATE_BUCKET/...) was denied: the host
    # role's only S3 read was the plugin bucket (which it keeps). With SourceMode=s3 a grant
    # adds the one object SourceS3Uri names (no grant with SourceMode=git); the download
    # asserts that this account owns its bucket.
    from tests.test_ec2_appstack import _load, _user_data

    doc = _load()
    # No high-level `aws s3` command (cp, sync, ...) is left: none can assert an owner.
    commands = [line for line in _user_data().splitlines() if not line.strip().startswith("#")]
    assert not [line for line in commands if re.search(r"\baws s3 ", line)]
    assert doc["Conditions"]["UseS3Source"] == {"Fn::Equals": [{"Ref": "SourceMode"}, "s3"]}
    (grant,) = [
        p["Fn::If"] for p in doc["Resources"]["HostRole"]["Properties"]["Policies"]
        if isinstance(p, dict) and "Fn::If" in p and p["Fn::If"][0] == "UseS3Source"
    ]
    assert grant[2] == {"Ref": "AWS::NoValue"}
    assert grant[1]["PolicyName"] == "read-source-tarball"
    (statement,) = grant[1]["PolicyDocument"]["Statement"]
    assert statement["Effect"] == "Allow" and statement["Action"] == "s3:GetObject"
    template, variables = statement["Resource"]["Fn::Sub"]
    assert template == "arn:${AWS::Partition}:s3:::${SourceObject}"
    index, (delimiter, joined) = (
        variables["SourceObject"]["Fn::Select"][0],
        variables["SourceObject"]["Fn::Select"][1]["Fn::Split"],
    )
    assert joined == {"Fn::Join": ["", [{"Ref": "SourceS3Uri"}, "s3://"]]}

    def resolve(uri: str) -> str:  # Fn::Select / Fn::Split / Fn::Join, as CloudFormation
        return (uri + "s3://").split(delimiter)[index]

    assert resolve(_SOURCE_URI) == "my-src-bucket/builds/2026/dsql-src.tar.gz"
    assert resolve("") == ""  # resolves for SourceMode=git's empty default too
    # A SourceMode=s3 deploy needs a URI, of that shape.
    rule = doc["Rules"]["SourceS3UriForS3Mode"]
    assert rule["RuleCondition"] == {"Fn::Equals": [{"Ref": "SourceMode"}, "s3"]}
    pattern = doc["Parameters"]["SourceS3Uri"]["AllowedPattern"]
    for ok in ("", _SOURCE_URI, "s3://a.b-c/k"):
        assert re.fullmatch(pattern, ok), ok
    for bad in ("s3://bucket", "s3://bucket/", "https://bucket/k", "s3://Bucket/k", "bucket/k"):
        assert not re.fullmatch(pattern, bad), bad
