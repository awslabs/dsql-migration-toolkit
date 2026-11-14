# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""S3 bucket ownership for the bucket names the tool derives.

The tool names its managed bucket after the account and region:
``mysql-dsql-migrator-plugins-<account>-<region>-an``, a name in this account's S3 account
regional namespace, which only this account can create, or the earlier
``mysql-dsql-migrator-plugins-<account>-<region>`` in the global namespace
(:func:`~dsql_migrator.core.s3_provision.managed_bucket_name` picks one; the ECS Fargate app
template sets the earlier name for its job and session state). A global-namespace name can be
predicted, and a bucket name does not say which account owns the bucket: anyone who knows the
account ID can create a bucket with that name in their own account first, and a request that
names it then reaches THEIR bucket. :meth:`OwnedBucket.ensure` creates a name of the form
``<prefix>-<this account>-<region>-an`` in this account's regional namespace
(``BucketNamespace=account-regional``).

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

A 403 to an owner-checked request cannot say why: another account owns the bucket, or this
identity may not make the request. So the refusal (:func:`not_owned_message`) names both, the
permission the refused request needs and S3's own answer. Once an owner-checked request has
reached the bucket (:attr:`OwnedBucket.confirmed`), a later 403 is checked again with an
owner-checked HeadBucket (:func:`refusal_text`): the bucket's owner may have deleted it since,
which frees the name for another account. A 200 shows the bucket is still this account's, so
that 403 is IAM, a KMS key policy or an SCP, reported as the error it is; anything else
unconfirms the bucket, and the 403 is the refusal.

What a refusal tells the operator to do depends on why the bucket has that name, which only the
caller knows: each :class:`OwnedBucket` carries its :class:`Remedy`. By default it is
:data:`DELETE_OR_SUPPORT`, for a derived name the tool has no other name for (a Region with no
S3 account regional namespace); :mod:`~dsql_migrator.core.s3_provision` binds the others (a
state bucket an operator named, the managed bucket's earlier name where the tool can move to
its ``-an`` name, a refusal another message wraps).

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
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

# ExpectedBucketOwner takes an AWS account ID: 12 digits (S3 accepts no other form).
_ACCOUNT_ID = re.compile(r"\d{12}")

# A bucket in an S3 account regional namespace is named <prefix>-<account>-<region>-an, and
# CreateBucket makes it there only when it sends this namespace (x-amz-bucket-namespace).
ACCOUNT_REGIONAL_NAMESPACE = "account-regional"
_ACCOUNT_REGIONAL_SUFFIX = re.compile(
    r"-(?P<account>\d{12})-(?P<region>[a-z]{2}(?:-[a-z]+)+-\d+)-an\Z"
)

# A status code in an error's text, not inside a longer number (an account ID, say).
_STATUS_403 = re.compile(r"(?<!\d)403(?!\d)")
_STATUS_404 = re.compile(r"(?<!\d)404(?!\d)")
_DENIED_CODES = frozenset({"403", "AccessDenied", "Forbidden"})
_MISSING_CODES = frozenset({"404", "NoSuchBucket", "NotFound", "NoSuchKey"})

# A bucket just created can take a moment to be found: its verifying HeadBucket retries a 404
# this many times in all, waiting 1 s, then 2 s.
_VERIFY_ATTEMPTS = 3
_VERIFY_BACKOFF_SECONDS = 1.0

# What a refusal tells the operator to do when the tool derived the bucket's name and has no
# other name for it: S3 cannot rename a bucket, so the fix is on the bucket's side. Since the
# managed bucket moved to the S3 account regional namespace (-an), that is only a Region with
# no such namespace (me-south-1, me-central-1); s3_provision binds the other cases' remedies.
DELETE_OR_SUPPORT = (
    "If the bucket is yours in another AWS account, empty and delete it there (S3 cannot "
    "rename a bucket), then retry; otherwise, contact AWS Support."
)


@dataclass(frozen=True)
class Remedy:
    """What a refusal of a bucket another AWS account may hold tells the operator.

    ``action``: what to do about it (``""`` when a message that embeds the refusal says it).
    ``derived``: the tool derived the bucket's name from the account ID and Region, so another
    account can create it first, and versions before the owner check kept CDC artifacts as
    well as state in it; ``False`` for a name an operator set (a state bucket setting), which
    held only state.
    """

    action: str = DELETE_OR_SUPPORT
    derived: bool = True


_DEFAULT_REMEDY = Remedy()
_WHY = (
    "The tool derives the name from the account ID and Region, so another account can create a "
    "bucket with that name first."
)
# The refusal is about this request; versions of the tool before the check used such a bucket.
_EARLIER = (
    "Versions of the tool before this check used such a bucket. If another account owns it, "
    "CDC infrastructure deployed in this account and Region may run connector plugins, an "
    "offset seeder and a template from it (delete it on the CDC step and review what its stack "
    "created), and on ECS Fargate the job and session state and the record of dropped "
    "DEFAULTs may have come from it; then rotate the source database credentials, check the "
    "target's schema and DEFAULTs against the Schema Conversion output, and its data with a "
    "CHECKSUM Validation."
)
# The same for a state bucket an operator named: the tool kept only state there, never CDC's.
_EARLIER_STATE = (
    "Versions of the tool before this check used such a bucket. If another account owns it, "
    "the state the app restored from it (and the record of dropped DEFAULTs kept beside the "
    "session state) may have come from that account; then rotate the source database "
    "credentials, check the target's schema and DEFAULTs against the Schema Conversion output, "
    "and its data with a CHECKSUM Validation."
)

# The IAM permission each request the tool sends to the bucket needs: what a 403 to it points
# at when the bucket is this account's (HeadObject is authorized by s3:GetObject).
_PERMISSIONS = {
    "HeadBucket": "s3:ListBucket",
    "ListObjectsV2": "s3:ListBucket",
    "HeadObject": "s3:GetObject",
    "GetObject": "s3:GetObject",
    "PutObject": "s3:PutObject",
    "DeleteObject": "s3:DeleteObject",
}
# botocore names the operation in an error's text (a managed upload's S3UploadFailedError
# carries only the text of the PutObject error it wraps).
_OPERATION_IN_TEXT = re.compile(r"when calling the (\w+) operation")


class BucketOwnershipError(RuntimeError):
    """The tool does not use an S3 bucket whose owner it cannot confirm: another AWS account
    owns it, this identity cannot reach it, or the caller's own account could not be read."""


class CallerAccountError(BucketOwnershipError):
    """The caller's own AWS account ID could not be read (or was not a 12-digit ID), so no
    request was sent to the bucket: there was no owner to assert. S3 refused nothing."""


class BucketCreateDenied(BucketOwnershipError):
    """CreateBucket answered 403: no bucket by that name was found for this account, and this
    identity may not create one (an SCP, a permissions boundary, no s3:CreateBucket)."""


def account_regional_region(name: str, owner: str) -> Optional[str]:
    """The Region in ``name`` when it is a bucket name in ``owner``'s S3 account regional
    namespace (``<prefix>-<owner>-<region>-an``); ``None`` for any other name."""
    match = _ACCOUNT_REGIONAL_SUFFIX.search(name or "")
    if match is None or match.group("account") != owner:
        return None
    return match.group("region")


def _first_line(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    return lines[0] if lines else type(exc).__name__


def error_code(exc: BaseException) -> str:
    """The S3 error code of a botocore ``ClientError`` (``""`` when it carries none)."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


def http_status(exc: BaseException) -> Optional[int]:
    """The HTTP status S3 answered a botocore ``ClientError`` with (``None`` when it carries
    none: a network failure, say)."""
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
    code, status = error_code(exc), http_status(exc)
    if code or status is not None:
        return code in _DENIED_CODES or status == 403
    text = str(exc)
    return "AccessDenied" in text or "Forbidden" in text or bool(_STATUS_403.search(text))


def is_missing(exc: BaseException) -> bool:
    """S3 answered 404: the bucket (or the key) does not exist."""
    code, status = error_code(exc), http_status(exc)
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


def _operation(exc: BaseException) -> Optional[str]:
    """The S3 operation ``exc`` is the answer to (``None`` when it does not say)."""
    name = getattr(exc, "operation_name", None)
    if isinstance(name, str) and name:
        return name
    match = _OPERATION_IN_TEXT.search(str(exc))
    return match.group(1) if match else None


def not_owned_message(
    bucket: str,
    owner: str,
    exc: Optional[BaseException] = None,
    *,
    remedy: Remedy = _DEFAULT_REMEDY,
) -> str:
    """The refusal for a 403 (``exc``) to a request that asserted ``owner`` owns ``bucket``,
    before any owner-checked request has reached it: another account owns the bucket, or this
    identity may not make that request, and S3 does not say which. Names the request, the
    IAM permission it needs and S3's own answer, then what to do (``remedy``; a name in this
    account's regional namespace needs none: no other account can hold it)."""
    operation = _operation(exc) if exc is not None else None
    permission = _PERMISSIONS.get(operation or "")
    request = f"a {operation} request" if operation else "a request"
    allows = f"allows {permission} on it" if permission else "allows the request"
    answer = f" S3 answered: {_first_line(exc).rstrip('.')}." if exc is not None else ""
    if account_regional_region(bucket, owner) is not None:
        # A name in this account's regional namespace: no other account can create it, so a
        # 403 is this identity's permissions, not another account's bucket.
        return (
            f"This identity cannot reach S3 bucket '{bucket}', so the tool did not use it: S3 "
            f"refused {request} that required this account ({owner}) to own it, so that "
            f"request read and wrote nothing.{answer} The bucket is in this account's S3 "
            "account regional namespace, where only this account can create a bucket, so "
            f"check that this identity's IAM policy {allows}, and check SCPs and permissions "
            "boundaries too; on ECS Fargate or EC2, update the app stack with this version's "
            "template, which grants it."
        )
    action = f" {remedy.action}" if remedy.action else ""
    background = f"{_EARLIER} {_WHY}" if remedy.derived else _EARLIER_STATE
    return (
        f"S3 bucket '{bucket}' is owned by another AWS account, or this identity cannot reach "
        f"it, so the tool did not use it: S3 refused {request} that required this account "
        f"({owner}) to own it, so that request read and wrote nothing.{answer}{action} If "
        f"the bucket is in this account, check that this identity's IAM policy {allows}. "
        f"{background}"
    )


def already_exists_message(
    bucket: str, owner: str, *, remedy: Remedy = _DEFAULT_REMEDY
) -> str:
    """The refusal for a CreateBucket that answered ``BucketAlreadyExists``, with what to do
    (``remedy``)."""
    action = f" {remedy.action}" if remedy.action else ""
    why = f" {_WHY}" if remedy.derived else ""
    return (
        f"S3 bucket '{bucket}' already exists in another AWS account (CreateBucket: "
        "BucketAlreadyExists), so the tool did not use it, and nothing was written to it."
        f"{action} This account is {owner}.{why}"
    )


def create_denied_message(bucket: str, owner: str, exc: BaseException) -> str:
    """The refusal for a CreateBucket that S3 denied (403): no bucket by that name was
    found for this account, and this identity may not create one -- an SCP, a permissions
    boundary or a missing s3:CreateBucket. Not another account's bucket: S3 answers that with
    ``BucketAlreadyExists``."""
    return (
        f"S3 bucket '{bucket}' does not exist in this account ({owner}), and this identity may "
        f"not create it (CreateBucket: {_first_line(exc).rstrip('.')}), so the tool did not use "
        "any bucket by that name and wrote nothing. Allow s3:CreateBucket on it for this "
        "identity (check SCPs and permissions boundaries too), or create the bucket in this "
        "account yourself, then retry."
        + (
            " The tool creates it in this account's S3 account regional namespace, so a policy "
            "condition on s3:x-amz-bucket-namespace applies to it as well."
            if account_regional_region(bucket, owner) is not None
            else ""
        )
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
    permission. :class:`CallerAccountError` when it cannot be read."""
    try:
        account = str(sts_client.get_caller_identity()["Account"])
    except Exception as exc:  # noqa: BLE001 - surfaced as the refusal it causes
        raise CallerAccountError(_sts_failure_message(exc)) from exc
    if not _ACCOUNT_ID.fullmatch(account):
        raise CallerAccountError(
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
                    raise CallerAccountError(_sts_failure_message(exc)) from exc
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

    :attr:`confirmed` turns true once one of those requests succeeds: S3 answered it, so this
    account owned the bucket then. A 403 after that is the refusal only if :meth:`still_owned`
    no longer reaches the bucket (:func:`refusal_text`).

    ``remedy`` is what a refusal of this bucket tells the operator to do (:attr:`remedy`).
    """

    def __init__(
        self, client: Any, name: str, owner: str, *, remedy: Optional[Remedy] = None
    ) -> None:
        if not _ACCOUNT_ID.fullmatch(str(owner or "")):
            raise CallerAccountError(
                f"No AWS account ID to assert as the owner of S3 bucket '{name}' (got "
                f"{owner!r}), so the tool does not use it."
            )
        self.client = client
        self.name = name
        self.owner = owner
        self._remedy = remedy or _DEFAULT_REMEDY
        #: An owner-checked request to the bucket has succeeded: this account owns it.
        self.confirmed = False

    @property
    def remedy(self) -> Remedy:
        """What a refusal of this bucket tells the operator to do (:data:`DELETE_OR_SUPPORT`
        unless the caller bound another)."""
        return self._remedy

    def _reached(self, response: Any) -> Any:
        self.confirmed = True
        return response

    def head(self) -> Any:
        return self._reached(
            self.client.head_bucket(Bucket=self.name, ExpectedBucketOwner=self.owner)
        )

    def still_owned(self) -> bool:
        """After a 403 on a :attr:`confirmed` bucket: whether an owner-checked HeadBucket still
        reaches it. A bucket cannot change owner, but its owner can delete it, and another
        account can then create one by that name. ``False``, and the bucket is no longer
        :attr:`confirmed`, when the HeadBucket fails for any reason (an identity without
        s3:ListBucket included: the owner cannot be confirmed then either)."""
        try:
            self.head()
        except Exception:  # noqa: BLE001 - any failure leaves the owner unconfirmed
            self.confirmed = False
            return False
        return True

    def head_key(self, key: str) -> Any:
        return self._reached(
            self.client.head_object(Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner)
        )

    def get(self, key: str) -> Any:
        return self._reached(
            self.client.get_object(Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner)
        )

    def put(self, key: str, body: Any, **params: Any) -> Any:
        return self._reached(
            self.client.put_object(
                Bucket=self.name, Key=key, Body=body, ExpectedBucketOwner=self.owner, **params
            )
        )

    def delete(self, key: str) -> Any:
        return self._reached(
            self.client.delete_object(Bucket=self.name, Key=key, ExpectedBucketOwner=self.owner)
        )

    def pages(self, prefix: str) -> Iterator[dict]:
        """The ListObjectsV2 pages under ``prefix``."""
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.name, Prefix=prefix, ExpectedBucketOwner=self.owner
        ):
            yield self._reached(page)

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
        self.confirmed = True

    def ensure(
        self, region: Optional[str], *, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        """Make sure the bucket exists and this account owns it, creating it when absent.

        An owner-checked HeadBucket first: a 200 is done; a 403 (another account owns the
        bucket, or this identity cannot reach it) raises :class:`BucketOwnershipError` and
        creates nothing; a 404 creates the bucket (with the Region's LocationConstraint; a
        bucket in us-east-1 takes none) and checks it again, which must then answer 200.
        A name in this account's regional namespace (:func:`account_regional_region`) is
        created there (``BucketNamespace=account-regional``), in the Region its name carries
        when ``region`` is ``None``. CreateBucket ``BucketAlreadyExists`` raises
        :class:`BucketOwnershipError`, never an adoption; a 403 raises
        :class:`BucketCreateDenied`; ``BucketAlreadyOwnedByYou`` goes on to that check like a
        new bucket. Any other error propagates.
        """
        try:
            self.head()
            return
        except Exception as exc:  # noqa: BLE001 - classified below
            if is_access_denied(exc):
                raise BucketOwnershipError(
                    not_owned_message(self.name, self.owner, exc, remedy=self.remedy)
                ) from exc
            if not is_missing(exc):
                raise
        namespace_region = account_regional_region(self.name, self.owner)
        region = region or namespace_region
        create: dict[str, Any] = {"Bucket": self.name}
        if region and region != "us-east-1":
            create["CreateBucketConfiguration"] = {"LocationConstraint": region}
        if namespace_region is not None:
            # Only this account can create a name in its regional namespace, and S3 creates
            # the bucket there only when the request says so.
            create["BucketNamespace"] = ACCOUNT_REGIONAL_NAMESPACE
        try:
            self.client.create_bucket(**create)
        except Exception as exc:  # noqa: BLE001 - classified below
            if _code_is(exc, "BucketAlreadyOwnedByYou"):
                pass  # an earlier create, or a race: checked below like a new bucket
            elif _code_is(exc, "BucketAlreadyExists"):
                raise BucketOwnershipError(
                    already_exists_message(self.name, self.owner, remedy=self.remedy)
                ) from exc
            elif is_access_denied(exc):
                # A denied CreateBucket is this identity's permissions (an SCP, a boundary,
                # no s3:CreateBucket), not another account's bucket: that answers
                # BucketAlreadyExists. Fail closed all the same, with the cause it is.
                raise BucketCreateDenied(
                    create_denied_message(self.name, self.owner, exc)
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
                        not_owned_message(self.name, self.owner, exc, remedy=self.remedy)
                    ) from exc
                if not is_missing(exc) or attempt + 1 == _VERIFY_ATTEMPTS:
                    raise
            sleep(_VERIFY_BACKOFF_SECONDS * (attempt + 1))


def refusal_text(exc: BaseException, bucket: OwnedBucket | None = None) -> Optional[str]:
    """The operator-facing refusal when ``exc`` is the tool not using a bucket it cannot
    confirm it owns: a :class:`BucketOwnershipError`, or a 403 to an owner-checked request on
    ``bucket``. ``None`` for any other error, and for a 403 on a bucket already
    :attr:`~OwnedBucket.confirmed` that an owner-checked HeadBucket still reaches
    (:meth:`~OwnedBucket.still_owned`): this account still owns it, so that 403 is this
    identity's IAM policy, a KMS key policy or an SCP, and the caller reports it as the error
    it is (S3 refused it all the same). A confirmed bucket the HeadBucket no longer reaches
    may have been deleted and created again by another account: the 403 is the refusal."""
    if isinstance(exc, BucketOwnershipError):
        return str(exc)
    if bucket is None or not is_access_denied(exc):
        return None
    if bucket.confirmed and bucket.still_owned():
        return None
    return not_owned_message(bucket.name, bucket.owner, exc, remedy=bucket.remedy)


__all__ = [
    "ACCOUNT_REGIONAL_NAMESPACE",
    "BucketCreateDenied",
    "BucketOwnershipError",
    "CallerAccount",
    "CallerAccountError",
    "DELETE_OR_SUPPORT",
    "OwnedBucket",
    "Remedy",
    "account_regional_region",
    "already_exists_message",
    "caller_account_id",
    "create_denied_message",
    "error_code",
    "http_status",
    "is_access_denied",
    "is_missing",
    "not_owned_message",
    "refusal_text",
    "session_caller_account",
]
