# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The tool's managed S3 bucket in an S3 account regional namespace.

``s3_provision.managed_bucket_name`` chooses, once per process for each account and Region,
between ``mysql-dsql-migrator-plugins-<account>-<region>-an`` (this account's regional
namespace: only this account can create a bucket by that name) and the earlier,
global-namespace ``mysql-dsql-migrator-plugins-<account>-<region>``, which an existing
deployment keeps while a bucket by it exists and is this account's, and which is created only
where the ``-an`` name cannot be. Every request stays owner-checked. Before, the tool derived
the earlier name only, so another account that created it first left Deploy and Start CDC (and
the ECS Fargate state) with no bucket at all in that account and Region.

Fakes only (:class:`tests.test_s3_bucket_owner.OwnerCheckingS3`): no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core import s3_provision
from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus
from dsql_migrator.core.job_store import S3JobStore
from dsql_migrator.core.overlay_ledger import S3OverlayLedger
from dsql_migrator.core.s3_ownership import (
    BucketOwnershipError,
    OwnedBucket,
    account_regional_region,
    not_owned_message,
)
from dsql_migrator.core.s3_provision import (
    NO_ACCOUNT_REGIONAL_NAMESPACE,
    S3ProvisionError,
    earlier_plugin_bucket_name,
    ensure_plugin_bucket,
    managed_bucket_name,
    regional_plugin_bucket_name,
    state_bucket,
)
from dsql_migrator.core.session_state_store import S3SessionStateStore, SessionSnapshot
from tests.test_s3_bucket_owner import (
    _BUCKET,
    _CUSTOM,
    _ME,
    _REGION,
    _REGIONAL,
    _THEM,
    OwnerCheckingS3,
    _error,
    _foreign,
    _job,
    _ledger_record,
    _Sts,
)


def _named(s3: OwnerCheckingS3) -> list[tuple[str, str]]:
    """Each request as (operation, bucket)."""
    return [(op, params["Bucket"]) for op, params in s3.calls]


def _record(monkeypatch) -> list[tuple]:
    recorded: list[tuple] = []
    monkeypatch.setattr(
        s3_provision, "log_activity", lambda *a, **k: recorded.append((a, k))
    )
    return recorded


# ---------------------------------------------------------------------------
# The names, and the namespace CreateBucket sends
# ---------------------------------------------------------------------------


def test_the_regional_name_is_the_earlier_one_in_this_accounts_namespace() -> None:
    assert _BUCKET == "mysql-dsql-migrator-plugins-111122223333-us-east-1"
    assert _REGIONAL == f"{_BUCKET}-an"
    assert NO_ACCOUNT_REGIONAL_NAMESPACE == frozenset({"me-south-1", "me-central-1"})
    assert account_regional_region(_REGIONAL, _ME) == _REGION
    assert account_regional_region(regional_plugin_bucket_name(_ME, "ap-northeast-2"), _ME) == (
        "ap-northeast-2"
    )
    # The earlier name, another account's -an name, and a bare "-an" are not this account's.
    for name in (_BUCKET, regional_plugin_bucket_name(_THEM, _REGION), "my-bucket-an"):
        assert account_regional_region(name, _ME) is None, name
    # The longest Region code still leaves the name inside S3's 63 characters.
    assert len(regional_plugin_bucket_name(_ME, "ap-southeast-14")) <= 63


@pytest.mark.parametrize("region", ["us-east-1", "ap-northeast-2"])
def test_create_bucket_for_a_regional_name_sends_the_account_regional_namespace(region) -> None:
    # FAILS WITHOUT THE FIX: OwnedBucket.ensure sent no BucketNamespace, and S3 creates a
    # bucket in this account's regional namespace only when the request says so.
    s3 = OwnerCheckingS3(region=region)
    name = regional_plugin_bucket_name(_ME, region)
    OwnedBucket(s3, name, _ME).ensure(region, sleep=lambda _s: None)
    create = dict(s3.calls)["CreateBucket"]
    assert create["BucketNamespace"] == "account-regional"
    if region == "us-east-1":
        assert "CreateBucketConfiguration" not in create
    else:
        assert create["CreateBucketConfiguration"] == {"LocationConstraint": region}
    # No Region given: the one the name carries.
    s3 = OwnerCheckingS3(region=region)
    OwnedBucket(s3, name, _ME).ensure(None, sleep=lambda _s: None)
    assert dict(s3.calls)["CreateBucket"] == create
    # Any other name is created as before, in the global namespace.
    for other in (_BUCKET, _CUSTOM, regional_plugin_bucket_name(_THEM, _REGION)):
        s3 = OwnerCheckingS3()
        OwnedBucket(s3, other, _ME).ensure(_REGION, sleep=lambda _s: None)
        assert dict(s3.calls)["CreateBucket"] == {"Bucket": other}, other


def test_a_403_on_this_accounts_regional_bucket_is_a_permission_problem() -> None:
    # Only this account can create the -an name, so a 403 is this identity's permissions: the
    # refusal does not send the operator to look for another account's bucket.
    text = not_owned_message(_REGIONAL, _ME, _error("HeadBucket", "403", 403))
    assert text.startswith(f"This identity cannot reach S3 bucket '{_REGIONAL}'")
    assert "allows s3:ListBucket on it" in text and "account regional namespace" in text
    assert "another AWS account" not in text and "AWS Support" not in text
    assert "rotate the source database credentials" not in text
    # The earlier name keeps its refusal.
    assert "owned by another AWS account" in not_owned_message(_BUCKET, _ME)


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def test_an_existing_regional_bucket_is_used(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the tool derived only the earlier name.
    recorded = _record(monkeypatch)
    s3 = OwnerCheckingS3()
    s3.bucket(_REGIONAL)
    s3.bucket(_BUCKET)  # an earlier bucket too: once the -an one exists, everything uses it
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL
    assert s3.calls == [("HeadBucket", {"Bucket": _REGIONAL, "ExpectedBucketOwner": _ME})]
    (args, kwargs), = recorded
    assert args == (ActivityCategory.SYSTEM, "S3 bucket in use")
    assert kwargs["status"] is ActivityStatus.INFO and kwargs["target"] == _REGIONAL
    assert "only this AWS account can create a bucket by this name" in kwargs["detail"]


def test_an_existing_earlier_bucket_of_this_account_is_kept(monkeypatch) -> None:
    # An existing deployment keeps its bucket, and the state in it: nothing is created,
    # copied or moved.
    recorded = _record(monkeypatch)
    region = "ap-northeast-2"
    earlier = earlier_plugin_bucket_name(_ME, region)
    regional = regional_plugin_bucket_name(_ME, region)
    s3 = OwnerCheckingS3(region=region)
    s3.bucket(earlier).objects["jobs/j1.json"] = (b"{}", {})
    assert ensure_plugin_bucket(s3, _ME, region) == earlier
    assert _named(s3) == [("HeadBucket", regional), ("HeadBucket", earlier)]
    assert set(s3.buckets) == {earlier}
    assert recorded[0][1]["target"] == earlier
    assert "keeps using the existing bucket" in recorded[0][1]["detail"]


def test_an_earlier_name_another_account_holds_gets_this_accounts_regional_bucket(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the earlier name was the only one, and another account's bucket by
    # it was refused: no bucket at all in this account and Region.
    recorded = _record(monkeypatch)
    region = "eu-west-1"
    earlier = earlier_plugin_bucket_name(_ME, region)
    regional = regional_plugin_bucket_name(_ME, region)
    s3 = OwnerCheckingS3(region=region)
    theirs = _foreign(s3, earlier)
    assert ensure_plugin_bucket(s3, _ME, region) == regional
    assert _named(s3) == [
        ("HeadBucket", regional), ("HeadBucket", earlier),
        ("HeadBucket", regional), ("CreateBucket", regional), ("HeadBucket", regional),
    ]
    assert s3.calls[3][1] == {
        "Bucket": regional,
        "CreateBucketConfiguration": {"LocationConstraint": region},
        "BucketNamespace": "account-regional",
    }
    assert s3.buckets[regional].owner == _ME and theirs.objects == {}
    assert "created in this account's S3 account regional namespace" in recorded[0][1]["detail"]
    assert s3.violations == []


def test_an_identity_granted_only_the_earlier_name_creates_the_earlier_bucket(
    monkeypatch,
) -> None:
    # An app stack template older than this version grants only the earlier name: the -an name
    # answers 403, and with no earlier bucket yet, the tool creates the earlier one, as before.
    recorded = _record(monkeypatch)
    s3 = OwnerCheckingS3()
    s3.no_grant = {_REGIONAL}
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _BUCKET
    assert _named(s3) == [
        ("HeadBucket", _REGIONAL), ("HeadBucket", _BUCKET),
        ("HeadBucket", _BUCKET), ("CreateBucket", _BUCKET), ("HeadBucket", _BUCKET),
    ]
    assert "BucketNamespace" not in s3.calls[3][1]  # no create of the -an name was tried
    assert recorded[0][1]["status"] is ActivityStatus.WARNING
    assert "older than this version" in recorded[0][1]["detail"]


def test_a_denied_regional_create_falls_back_to_the_earlier_name() -> None:
    # An SCP that forbids the account regional namespace (or a template without the -an
    # CreateBucket): with no earlier bucket either, the earlier name is created, as before.
    region = "eu-west-1"
    earlier = earlier_plugin_bucket_name(_ME, region)
    regional = regional_plugin_bucket_name(_ME, region)
    s3 = OwnerCheckingS3(region=region)
    s3.create_denied = {regional}
    assert ensure_plugin_bucket(s3, _ME, region) == earlier
    assert _named(s3) == [
        ("HeadBucket", regional), ("HeadBucket", earlier),
        ("HeadBucket", regional), ("CreateBucket", regional),
        ("HeadBucket", earlier), ("CreateBucket", earlier), ("HeadBucket", earlier),
    ]
    assert set(s3.buckets) == {earlier} and s3.buckets[earlier].owner == _ME


@pytest.mark.parametrize("unusable", ["create denied", "not granted"])
def test_no_bucket_this_account_can_use_is_refused_naming_both(unusable) -> None:
    # The earlier name is another account's, and the -an one cannot be created (or used): the
    # earlier one is never created over it, and the refusal names both buckets and the fix.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    if unusable == "create denied":
        s3.create_denied = {_REGIONAL}
    else:
        s3.no_grant = {_REGIONAL}
    with pytest.raises(S3ProvisionError) as exc:
        ensure_plugin_bucket(s3, _ME, _REGION)
    text = str(exc.value)
    assert f"'{_REGIONAL}'" in text and f"'{_BUCKET}'" in text
    assert "another AWS account may hold that name" in text
    assert "Allow s3:CreateBucket and s3:ListBucket" in text and text.endswith("Then retry.")
    assert ("CreateBucket", _BUCKET) not in _named(s3)
    assert theirs.objects == {} and _REGIONAL not in s3.buckets
    # Nothing was chosen: the next call chooses again.
    assert s3_provision._MANAGED_BUCKETS == {}


@pytest.mark.parametrize("region", sorted(NO_ACCOUNT_REGIONAL_NAMESPACE))
def test_a_region_without_the_namespace_keeps_the_earlier_name(region) -> None:
    earlier = earlier_plugin_bucket_name(_ME, region)
    s3 = OwnerCheckingS3(region=region)
    assert ensure_plugin_bucket(s3, _ME, region) == earlier
    assert _named(s3) == [
        ("HeadBucket", earlier), ("CreateBucket", earlier), ("HeadBucket", earlier)
    ]
    assert s3.calls[1][1] == {
        "Bucket": earlier, "CreateBucketConfiguration": {"LocationConstraint": region}
    }
    # ...owner-checked as before: another account's bucket by it is refused, not adopted.
    s3 = OwnerCheckingS3(region=region)
    theirs = _foreign(s3, earlier)
    with pytest.raises(S3ProvisionError) as exc:
        ensure_plugin_bucket(s3, _ME, region)
    assert "owned by another AWS account" in str(exc.value)
    assert _named(s3) == [("HeadBucket", earlier)] and theirs.objects == {}


def test_an_earlier_bucket_deleted_mid_run_is_chosen_again_not_created_again() -> None:
    # FAILS WITHOUT THE FIX: each ensure created a missing earlier bucket again -- a name
    # another account may have created in between.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _BUCKET
    del s3.buckets[_BUCKET]  # deleted while the app runs
    s3.calls.clear()
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL
    assert ("CreateBucket", _BUCKET) not in _named(s3) and _BUCKET not in s3.buckets
    assert _named(s3)[:3] == [
        ("HeadBucket", _BUCKET), ("HeadBucket", _REGIONAL), ("HeadBucket", _BUCKET)
    ]
    # Taken by another account after that: the -an bucket is unaffected.
    _foreign(s3)
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL


def test_a_regional_bucket_deleted_mid_run_is_created_again() -> None:
    # Only this account can create the -an name, so creating it again is safe.
    s3 = OwnerCheckingS3()
    s3.bucket(_REGIONAL)
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL
    del s3.buckets[_REGIONAL]
    s3.calls.clear()
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL
    assert _named(s3) == [
        ("HeadBucket", _REGIONAL), ("CreateBucket", _REGIONAL), ("HeadBucket", _REGIONAL)
    ]


def test_the_choice_is_kept_per_account_and_region_and_recorded_once(monkeypatch) -> None:
    recorded = _record(monkeypatch)
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _BUCKET
    # A later -an bucket (another app stack's, say) does not move this process off the
    # bucket it chose; another client of the same identity gets the same choice.
    s3.bucket(_REGIONAL)
    other = OwnerCheckingS3()
    other.buckets = s3.buckets
    assert managed_bucket_name(other, _ME, _REGION) == _BUCKET
    assert _named(other) == [("HeadBucket", _BUCKET)]  # checked, not chosen again
    assert len(recorded) == 1
    # Another Region (or account) is chosen on its own.
    elsewhere = OwnerCheckingS3(region="eu-west-1")
    assert managed_bucket_name(elsewhere, _ME, "eu-west-1") == (
        regional_plugin_bucket_name(_ME, "eu-west-1")
    )
    assert len(recorded) == 2
    assert set(s3_provision._MANAGED_BUCKETS) == {(_ME, _REGION), (_ME, "eu-west-1")}


def test_a_transient_error_propagates_and_chooses_nothing() -> None:
    s3 = OwnerCheckingS3()
    real_head = s3.head_bucket

    def slow_down(**params):
        if params["Bucket"] == _BUCKET:
            raise _error("HeadBucket", "503", 503)
        return real_head(**params)

    s3.head_bucket = slow_down  # type: ignore[method-assign]
    with pytest.raises(S3ProvisionError) as exc:
        ensure_plugin_bucket(s3, _ME, _REGION)
    assert "503" in str(exc.value)
    assert "CreateBucket" not in s3.ops() and s3_provision._MANAGED_BUCKETS == {}


# ---------------------------------------------------------------------------
# The ECS Fargate state stores: a setting equal to the earlier name is the managed bucket
# ---------------------------------------------------------------------------


def test_a_state_bucket_set_to_the_earlier_name_uses_the_managed_bucket() -> None:
    # FAILS WITHOUT THE FIX: the template's earlier name was used as given, so another
    # account's bucket by it left job and session state unsaved and the ledger unreadable.
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    ledger = S3OverlayLedger(
        _BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None
    )
    assert jobs.load_all() == []
    jobs.save(_job("j1"), None)
    sessions.prune(200)
    sessions.save(SessionSnapshot(session_id="s1"))
    assert ledger.read("k").status == "absent"
    ledger.put_tables("k", {"shop.docs": _ledger_record()})
    assert jobs.bucket_refusal is None and sessions.bucket_refusal is None
    # All three in this account's -an bucket: the ledger follows the session store.
    assert set(s3.buckets[_REGIONAL].objects) == {
        "jobs/j1.json", "sessions/s1.json", "overlay-ledger/k.json"
    }
    assert theirs.objects == {} and s3.reads[0][0] == _REGIONAL
    assert {bucket for bucket, _key in s3.reads} == {_REGIONAL}
    assert s3.violations == []


def test_a_state_bucket_keeps_an_existing_earlier_bucket_and_its_state() -> None:
    s3 = OwnerCheckingS3()
    earlier = s3.bucket(_BUCKET)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1", "DONE"), None)
    restored = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    assert [p.job.job_id for p in restored.load_all()] == ["j1"]
    assert set(s3.buckets) == {_BUCKET} and set(earlier.objects) == {"jobs/j1.json"}


def test_a_state_bucket_set_to_any_other_name_is_used_as_given() -> None:
    s3 = OwnerCheckingS3()
    for configured, region in (
        (_CUSTOM, _REGION),
        (earlier_plugin_bucket_name(_THEM, _REGION), _REGION),  # another account's name
        (earlier_plugin_bucket_name(_ME, "eu-west-1"), _REGION),  # another Region's
        (_BUCKET, None),  # no Region configured (a local run)
    ):
        bound = state_bucket(s3, configured, _ME, region)
        assert type(bound) is OwnedBucket and bound.name == configured, configured
    assert s3.calls == []  # bound, not yet used


def test_a_state_bucket_deleted_mid_run_moves_to_the_regional_bucket() -> None:
    # The earlier bucket the stores kept is deleted while the app runs: the next save chooses
    # again -- the -an bucket -- instead of creating the earlier name again.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    del s3.buckets[_BUCKET]
    jobs.save(_job("j2"), None)  # NoSuchBucket: not saved, and checked again next time
    jobs.save(_job("j3"), None)
    assert _BUCKET not in s3.buckets
    assert set(s3.buckets[_REGIONAL].objects) == {"jobs/j3.json"}
    assert jobs.bucket_refusal is None and s3.violations == []


def test_a_state_bucket_with_no_usable_bucket_is_the_stores_refusal() -> None:
    s3 = OwnerCheckingS3()
    theirs = _foreign(s3)
    s3.no_grant = {_REGIONAL}
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    assert f"'{_REGIONAL}'" in (jobs.bucket_refusal or "")
    assert f"'{_BUCKET}'" in (jobs.bucket_refusal or "")
    assert theirs.objects == {} and "PutObject" not in s3.ops()
    # A managed-bucket refusal is a BucketOwnershipError like any other bucket's.
    with pytest.raises(BucketOwnershipError):
        state_bucket(s3, _BUCKET, _ME, _REGION)


def test_the_ledger_follows_the_session_store_to_the_bucket_chosen_again() -> None:
    # The earlier bucket is deleted while the app runs: the ledger reads it as unreadable (fail
    # closed) until the session store chooses again, and then reads the -an bucket.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    sessions = S3SessionStateStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    ledger = S3OverlayLedger(
        _BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None
    )
    sessions.save(SessionSnapshot(session_id="s1"))
    assert ledger.read("k").status == "absent"
    del s3.buckets[_BUCKET]
    assert ledger.read("k").status == "unreadable"
    sessions.save(SessionSnapshot(session_id="s2"))  # NoSuchBucket: checked again next time
    sessions.save(SessionSnapshot(session_id="s3"))  # chosen again: the -an bucket
    ledger.put_tables("k", {"shop.docs": _ledger_record()})
    assert set(s3.buckets[_REGIONAL].objects) == {"sessions/s3.json", "overlay-ledger/k.json"}
    assert _BUCKET not in s3.buckets and s3.violations == []


def test_an_earlier_state_bucket_taken_over_mid_run_moves_to_the_regional_bucket() -> None:
    # This account's earlier bucket is deleted and another account creates the name: the first
    # save after that is refused (nothing reaches the other account's bucket), and the next one
    # chooses again -- this account's -an bucket.
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    del s3.buckets[_BUCKET]
    theirs = _foreign(s3)
    jobs.save(_job("j2"), None)
    assert "owned by another AWS account" in (jobs.bucket_refusal or "")
    jobs.save(_job("j3"), None)
    assert jobs.bucket_refusal is None
    assert set(s3.buckets[_REGIONAL].objects) == {"jobs/j3.json"}
    assert theirs.objects == {} and s3.reads == [] and s3.violations == []
