# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""S3 bucket ownership for the bucket names the tool derives.

The tool names its managed bucket after the account and region,
``mysql-dsql-migrator-plugins-<account>-<region>``
(:func:`~dsql_migrator.core.s3_provision.plugin_bucket_name`; the ECS Fargate app template
derives the same name for its job and session state). Such a name can be predicted, and a bucket
name does not say which account owns the bucket: anyone who knows the account ID can create a
bucket with that name in their own account first, and a request that names it then reaches
THEIR bucket.

So every request the tool sends to a bucket whose name it derived carries ``ExpectedBucketOwner``
= the caller's own account ID, read with ``sts:GetCallerIdentity`` (which needs no IAM
permission). S3 answers such a request with 403 Access Denied unless that account owns the
bucket. :class:`OwnedBucket` binds a bucket to that account, and its methods are how the tool's
code reaches S3: tests/test_s3_bucket_owner.py fails on a direct S3 client call anywhere else in
the package, and on a call here that leaves the owner out.

CreateBucket is the one request that takes no expected owner. Its answer is the create-time
signal instead: ``BucketAlreadyExists`` means another account holds the name, which is a hard
error and never an adoption; ``BucketAlreadyOwnedByYou`` (in us-east-1, a 200, which is how that
region answers a re-create) is ours, and must still pass the owner-checked ``HeadBucket`` after
it (:meth:`OwnedBucket.ensure`). A bucket cannot change owner without being deleted, so an object
the tool uploaded with the check is the one a service reads next by name (CloudFormation's
TemplateURL, an MSK Connect plugin, a Lambda function's code).

Verified against the Amazon S3 documentation: a request whose expected owner is not the
bucket's owner fails with 403 Access Denied; HeadBucket answers a bucket that does not exist, or
one this identity may not access, with a bare 400, 403 or 404 (only a 404 is taken as "absent";
a 400 is neither, and fails closed); CreateBucket ignores an expected owner and answers 409
``BucketAlreadyExists`` (another account's) or ``BucketAlreadyOwnedByYou`` (ours; us-east-1
answers a re-create with 200).
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Iterator, Optional

# ExpectedBucketOwner takes an AWS account ID: 12 digits (S3 accepts no other form).
_ACCOUNT_ID = re.compile(r"\d{12}")

# A status code in an error's text, not inside a longer number (an account ID, say).
_STATUS_403 = re.compile(r"(?<!\d)403(?!\d)")
_STATUS_404 = re.compile(r"(?<!\d)404(?!\d)")
_DENIED_CODES = frozenset({"403", "AccessDenied", "Forbidden"})
_MISSING_CODES = frozenset({"404", "NoSuchBucket", "NotFound", "NoSuchKey"})

# A bucket just created can take a moment to be found: its verifying HeadBucket retries a 404
# this many times in all, waiting 1 s, then 2 s.
_VERIFY_ATTEMPTS = 3
_VERIFY_BACKOFF_SECONDS = 1.0

# What every refusal tells the operator to do. The tool has no setting that picks another
# bucket name (the name is derived), so the fix is on the bucket's side.
_REMEDY = (
    "If the bucket is yours in another AWS account, empty and delete it there (S3 cannot "
    "rename a bucket), then retry; otherwise, contact AWS Support."
)
_WHY = (
    "The tool derives the name from the account ID and Region, so another account can create a "
    "bucket with that name first."
)


class BucketOwnershipError(RuntimeError):
    """The tool does not use an S3 bucket whose owner it cannot confirm: another AWS account
    owns it, this identity cannot reach it, or the caller's own account could not be read."""


def _first_line(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    return lines[0] if lines else type(exc).__name__


def error_code(exc: BaseException) -> str:
    """The S3 error code of a botocore ``ClientError`` (``""`` when it carries none)."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


def _http_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def is_access_denied(exc: BaseException) -> bool:
    """S3 answered 403. With ``ExpectedBucketOwner``, that is a bucket another account owns,
    or one this identity may not reach: the tool cannot confirm it owns the bucket."""
    code, status = error_code(exc), _http_status(exc)
    if code or status is not None:
        return code in _DENIED_CODES or status == 403
    text = str(exc)
    return "AccessDenied" in text or "Forbidden" in text or bool(_STATUS_403.search(text))


def is_missing(exc: BaseException) -> bool:
    """S3 answered 404: the bucket (or the key) does not exist."""
    code, status = error_code(exc), _http_status(exc)
    if code or status is not None:
        return code in _MISSING_CODES or status == 404
    text = str(exc)
    return (
        "NoSuchBucket" in text
        or "NoSuchKey" in text
        or "Not Found" in text
        or bool(_STATUS_404.search(text))
    )


def _code_is(exc: BaseException, code: str) -> bool:
    return error_code(exc) == code or code in str(exc)


def not_owned_message(bucket: str, owner: str) -> str:
    """The refusal for a 403 to a request that asserted ``owner`` owns ``bucket``."""
    return (
        f"S3 bucket '{bucket}' is owned by another AWS account, or this identity cannot reach "
        f"it, so the tool did not use it: S3 refused a request that required this account "
        f"({owner}) to own it, and nothing was written to it. {_REMEDY} If the bucket is in "
        "this account, check that this identity's IAM policy allows the request (HeadBucket "
        f"needs s3:ListBucket on the bucket). {_WHY}"
    )


def already_exists_message(bucket: str, owner: str) -> str:
    """The refusal for a CreateBucket that answered ``BucketAlreadyExists``."""
    return (
        f"S3 bucket '{bucket}' already exists in another AWS account (CreateBucket: "
        "BucketAlreadyExists), so the tool did not use it, and nothing was written to it. "
        f"{_REMEDY} This account is {owner}. {_WHY}"
    )


def _sts_failure_message(exc: BaseException) -> str:
    return (
        "Could not read this identity's AWS account ID (sts:GetCallerIdentity: "
        f"{_first_line(exc)}). The tool does not use its S3 bucket without it: every request to "
        "the bucket asserts that this account owns it. Check this identity's AWS credentials "
        "and its network path to AWS STS, then retry."
    )


def caller_account_id(sts_client: Any) -> str:
    """The caller's own AWS account ID: ``sts:GetCallerIdentity``, which needs no IAM
    permission. :class:`BucketOwnershipError` when it cannot be read."""
    try:
        account = str(sts_client.get_caller_identity()["Account"])
    except Exception as exc:  # noqa: BLE001 - surfaced as the refusal it causes
        raise BucketOwnershipError(_sts_failure_message(exc)) from exc
    if not _ACCOUNT_ID.fullmatch(account):
        raise BucketOwnershipError(
            f"sts:GetCallerIdentity returned the account ID {account!r}, not 12 digits, so the "
            "tool cannot assert who owns its S3 bucket, and does not use it."
        )
    return account


class CallerAccount:
    """The caller's own account ID for one credential context, read on first use and kept.

    ``sts`` builds the STS client (called once, on first use). A failed read is not kept: the
    next use reads again, and no use proceeds without the account. Thread-safe.
    """

    def __init__(self, sts: Callable[[], Any]) -> None:
        self._sts = sts
        self._account: Optional[str] = None
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            if self._account is None:
                try:
                    client = self._sts()
                except Exception as exc:  # noqa: BLE001 - no client, no account
                    raise BucketOwnershipError(_sts_failure_message(exc)) from exc
                self._account = caller_account_id(client)
            return self._account


def session_caller_account(
    aws_profile: Optional[str], region: Optional[str], *, sts_client: Any = None
) -> CallerAccount:
    """A :class:`CallerAccount` for the app's shared session (``build_session(aws_profile)``),
    or for the injected ``sts_client`` (the test seam)."""

    def _build() -> Any:
        if sts_client is not None:
            return sts_client
        from dsql_migrator.core.aws_session import build_session

        return build_session(aws_profile).client("sts", region_name=region)

    return CallerAccount(_build)


class OwnedBucket:
    """A bucket whose name the tool derived, bound to the account that must own it.

    Every request this class sends carries ``ExpectedBucketOwner=owner`` (CreateBucket, which
    takes none, is followed by a HeadBucket that does). An ``owner`` that is not a 12-digit
    account ID is refused here, so no request goes out without a real one.
    """

    def __init__(self, client: Any, name: str, owner: str) -> None:
        if not _ACCOUNT_ID.fullmatch(str(owner or "")):
            raise BucketOwnershipError(
                f"No AWS account ID to assert as the owner of S3 bucket '{name}' (got "
                f"{owner!r}), so the tool does not use it."
            )
        self.client = client
        self.name = name
        self.owner = owner

    def head(self) -> Any:
        return self.client.head_bucket(Bucket=self.name, ExpectedBucketOwner=self.owner)

    def head_key(self, key: str) -> Any:
        return self.client.head_object(
            Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner
        )

    def get(self, key: str) -> Any:
        return self.client.get_object(
            Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner
        )

    def put(self, key: str, body: Any, **params: Any) -> Any:
        return self.client.put_object(
            Bucket=self.name, Key=key, Body=body, ExpectedBucketOwner=self.owner, **params
        )

    def delete(self, key: str) -> Any:
        return self.client.delete_object(
            Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner
        )

    def pages(self, prefix: str) -> Iterator[dict]:
        """The ListObjectsV2 pages under ``prefix``."""
        paginator = self.client.get_paginator("list_objects_v2")
        return iter(
            paginator.paginate(
                Bucket=self.name, Prefix=prefix, ExpectedBucketOwner=self.owner
            )
        )

    def upload(
        self, filename: str, key: str, *, extra_args: dict, config: Any = None
    ) -> None:
        """A managed upload (boto3 ``upload_file``); the owner is one of its ``ExtraArgs``."""
        self.client.upload_file(
            filename,
            self.name,
            key,
            ExtraArgs={**extra_args, "ExpectedBucketOwner": self.owner},
            Config=config,
        )

    def ensure(
        self, region: Optional[str], *, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        """Make sure the bucket exists and this account owns it, creating it when absent.

        An owner-checked HeadBucket first: a 200 is done; a 403 (another account owns the
        bucket, or this identity cannot reach it) raises :class:`BucketOwnershipError` and
        creates nothing; a 404 creates the bucket (with the Region's LocationConstraint; a
        bucket in us-east-1 takes none) and checks it again, which must then answer 200.
        CreateBucket ``BucketAlreadyExists`` raises :class:`BucketOwnershipError`, never an
        adoption; ``BucketAlreadyOwnedByYou`` goes on to that check like a new bucket. Any other
        error propagates.
        """
        try:
            self.head()
            return
        except Exception as exc:  # noqa: BLE001 - classified below
            if is_access_denied(exc):
                raise BucketOwnershipError(not_owned_message(self.name, self.owner)) from exc
            if not is_missing(exc):
                raise
        try:
            if region and region != "us-east-1":
                self.client.create_bucket(
                    Bucket=self.name,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            else:
                self.client.create_bucket(Bucket=self.name)
        except Exception as exc:  # noqa: BLE001 - classified below
            if _code_is(exc, "BucketAlreadyOwnedByYou"):
                pass  # an earlier create, or a race: checked below like a new bucket
            elif _code_is(exc, "BucketAlreadyExists"):
                raise BucketOwnershipError(
                    already_exists_message(self.name, self.owner)
                ) from exc
            else:
                raise
        for attempt in range(_VERIFY_ATTEMPTS):
            try:
                self.head()
                return
            except Exception as exc:  # noqa: BLE001 - classified below
                if is_access_denied(exc):
                    raise BucketOwnershipError(
                        not_owned_message(self.name, self.owner)
                    ) from exc
                if not is_missing(exc) or attempt + 1 == _VERIFY_ATTEMPTS:
                    raise
            sleep(_VERIFY_BACKOFF_SECONDS * (attempt + 1))


def refusal_text(exc: BaseException, bucket: OwnedBucket | None = None) -> Optional[str]:
    """The operator-facing refusal when ``exc`` is the tool not using a bucket it cannot
    confirm it owns (a :class:`BucketOwnershipError`, or a 403 to an owner-checked request on
    ``bucket``); ``None`` for any other error."""
    if isinstance(exc, BucketOwnershipError):
        return str(exc)
    if bucket is not None and is_access_denied(exc):
        return not_owned_message(bucket.name, bucket.owner)
    return None


__all__ = [
    "BucketOwnershipError",
    "CallerAccount",
    "OwnedBucket",
    "already_exists_message",
    "caller_account_id",
    "error_code",
    "is_access_denied",
    "is_missing",
    "not_owned_message",
    "refusal_text",
    "session_caller_account",
]
