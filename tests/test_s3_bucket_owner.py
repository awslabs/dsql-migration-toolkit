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
import re
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

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


def _error(operation: str, code: str, status: int) -> ClientError:
    """A botocore ClientError as S3 sends it (a HEAD has no body: its code is the status)."""
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )


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
        params = {"Bucket": Bucket, "Key": Key, **(ExtraArgs or {})}
        bucket = self._reach("PutObject", params)
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
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
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
# The app's startup reports a refused state bucket in the activity log
# ---------------------------------------------------------------------------


def test_the_startup_records_a_refused_state_bucket_in_the_activity_log(monkeypatch) -> None:
    from dsql_migrator.core import activity_log
    from dsql_migrator.ui import app

    logged: list[tuple] = []
    monkeypatch.setattr(
        activity_log, "log_activity", lambda *a, **k: logged.append((a, k))
    )
    s3 = OwnerCheckingS3()
    _foreign(s3)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.load_all()
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions.prune(200)
    app._report_refused_state_buckets(jobs, sessions)
    assert [a[1] for a, _k in logged] == [
        "Job state is not saved: S3 bucket refused",
        "Session state is not saved: S3 bucket refused",
    ]
    for _a, kwargs in logged:
        assert kwargs["status"] is activity_log.ActivityStatus.FAILURE
        _assert_refusal(kwargs["detail"])
    # A usable bucket, and a SQLite store, report nothing.
    logged.clear()
    own = OwnerCheckingS3()
    usable = S3JobStore(_BUCKET, region=_REGION, s3_client=own, sts_client=_Sts())
    usable.load_all()
    app._report_refused_state_buckets(usable, object())
    assert logged == []


# ---------------------------------------------------------------------------
# The deployment guides: the template bucket and the plugin bucket's teardown
# ---------------------------------------------------------------------------

_GUIDES = ("DEPLOYMENT.md", "DEPLOYMENT.ko.md", "DEPLOYMENT.ja.md")
_TEMPLATE_OWNER_CHECK = (
    'aws s3api head-bucket --bucket "$TEMPLATE_BUCKET" --expected-bucket-owner "$ACCOUNT"'
)


def _guide(name: str) -> str:
    return (_SRC.parents[1] / "deploy" / name).read_text(encoding="utf-8")


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
    # Every deploy from the bucket has an owner check of its own before it.
    deploys = [m.start() for m in re.finditer(r'--s3-bucket "\$TEMPLATE_BUCKET"', text)]
    checks = [m.start() for m in re.finditer(re.escape(_TEMPLATE_OWNER_CHECK), text)]
    assert len(deploys) == 3, name  # the app stack, its image update, the EC2 stack
    previous = 0
    for at in deploys:
        assert any(previous < check < at for check in checks), (name, at)
        previous = at
    # The coding-agent prompts ask for the same.
    flat = " ".join(text.split())
    assert flat.count("aws s3api head-bucket --expected-bucket-owner") == 2, name


@pytest.mark.parametrize("name", _GUIDES)
def test_the_owner_checked_commands_are_ones_the_cli_has(name: str) -> None:
    # `aws s3 mb`, `aws s3 rb` and `aws cloudformation deploy` take no expected owner, so the
    # check is s3api's head-bucket; no guide passes the option to a command without it.
    text = _guide(name)
    uses = [m.start() for m in re.finditer("--expected-bucket-owner", text)]
    assert len(uses) >= 7, (name, len(uses))
    for at in uses:
        command = text[text.rindex("aws ", 0, at):at]
        assert command.startswith("aws s3api head-bucket"), (name, command)
