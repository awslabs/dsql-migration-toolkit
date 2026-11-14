# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""What a refused S3 bucket tells the operator to do, case by case.

v0.1.587 wrote one remedy for every refusal -- "if the bucket is yours in another AWS account,
empty and delete it there; otherwise, contact AWS Support" -- because then a bucket by the
managed name in another account blocked the tool everywhere, and the tool had no other name for
it. Since the managed bucket moved to this account's S3 account regional namespace (``-an``),
that holds only where the tool still has no other name, and every other refusal tells the
operator something that does not work (or is not needed):

(i)   me-south-1 / me-central-1 (no account regional namespace): the earlier name is the only
      one, so the fix stays on the bucket's side: delete it there if yours, else AWS Support.
(ii)  a job or session state bucket the operator named (DSQL_MIGRATOR_JOB_STATE_BUCKET /
      DSQL_MIGRATOR_SESSION_STATE_BUCKET set to a name other than the managed one): point the
      setting at a bucket this account owns, or at the managed bucket's name.
(iii) the -an bucket cannot be created (an app stack template older than this version, an SCP
      or a permissions boundary) while another account holds the earlier name: allow
      s3:CreateBucket on the -an name for s3:x-amz-bucket-namespace = account-regional, which
      this version's app stack template grants.
(iv)  the managed bucket's earlier name, which an existing deployment kept, refused while the
      app runs (its owner deleted it and another account took the name): retry, and the tool
      moves to its -an bucket by itself.

Each test pins its case's remedy and that no other case's is there. Fakes only: no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.cdc_stack_deployer import CdcDeployError
from dsql_migrator.core.job_store import S3JobStore
from dsql_migrator.core.overlay_ledger import S3OverlayLedger
from dsql_migrator.core.s3_ownership import DELETE_OR_SUPPORT
from dsql_migrator.core.s3_provision import (
    NO_ACCOUNT_REGIONAL_NAMESPACE,
    S3ProvisionError,
    earlier_plugin_bucket_name,
    ensure_plugin_bucket,
    regional_plugin_bucket_name,
)
from dsql_migrator.core.session_state_store import S3SessionStateStore, SessionSnapshot
from dsql_migrator.ui.workflow import state_bucket_notice
from tests.test_s3_bucket_owner import (
    _BIG,
    _BUCKET,
    _CUSTOM,
    _ME,
    _REGION,
    _REGIONAL,
    _THEM,
    OwnerCheckingS3,
    _deployer,
    _foreign,
    _job,
    _ledger_record,
    _Sts,
)

# One marker per case's remedy (the full text is pinned in each case's own test).
_NO_OTHER_NAME = "has no S3 account regional namespace, so the tool has no other name"
_SETTING = "which names this bucket, at a bucket this account owns"
_ALLOW_REGIONAL = "for a request whose s3:x-amz-bucket-namespace is account-regional"
_MOVES = "the tool then checks its managed bucket again and, while this bucket is refused"
_CASES = {
    "delete or support": (DELETE_OR_SUPPORT, "AWS Support"),
    "no other name": (_NO_OTHER_NAME,),
    "setting": (_SETTING,),
    "allow regional": (_ALLOW_REGIONAL,),
    "moves": (_MOVES,),
}


def _only(text: str, *cases: str) -> None:
    """``text`` gives each of ``cases``' remedies, and none of the others'."""
    for case, markers in _CASES.items():
        for marker in markers:
            assert (marker in text) is (case in cases), (case, marker, text)
    assert ".." not in text, text


# ---------------------------------------------------------------------------
# (i) A Region with no S3 account regional namespace
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("region", sorted(NO_ACCOUNT_REGIONAL_NAMESPACE))
@pytest.mark.parametrize("squat", ["holds the name", "creates it first"])
def test_where_the_tool_has_no_other_name_the_fix_is_on_the_buckets_side(region, squat) -> None:
    # FAILS WITHOUT THE FIX: the refusal did not say why the tool cannot just use another
    # bucket here, as it does in every other Region.
    earlier = earlier_plugin_bucket_name(_ME, region)
    remedy = (
        f"{region} has no S3 account regional namespace, so the tool has no other name for "
        f"this bucket there. {DELETE_OR_SUPPORT}"
    )

    def squatted() -> OwnerCheckingS3:
        s3 = OwnerCheckingS3(region=region)
        if squat == "holds the name":
            _foreign(s3, earlier)
        else:  # another account creates it between our HeadBucket and our CreateBucket
            s3.squatter_creates_first = _THEM
        return s3

    # Deploy, Start and Retry CDC (they choose the managed bucket first)...
    with pytest.raises(S3ProvisionError) as deploy:
        ensure_plugin_bucket(squatted(), _ME, region)
    # ...the job and session state set to the managed bucket's name, as the app template does
    # (the notice above every screen and the activity log show bucket_refusal)...
    s3 = squatted()
    jobs = S3JobStore(earlier, region=region, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    # ...and the cdc-stack template staged in that bucket.
    staging = OwnerCheckingS3(region=region)
    _foreign(staging, earlier)
    deployer, cfn = _deployer(staging, region=region)
    with pytest.raises(CdcDeployError) as staged:
        deployer.preflight_template(_BIG)
    texts = [str(deploy.value), jobs.bucket_refusal or ""]
    if squat == "holds the name":
        texts.append(str(staged.value))
    for text in texts:
        assert f"'{earlier}'" in text and remedy in text, text
        _only(text, "no other name", "delete or support")
    assert cfn.calls == [] and s3.buckets[earlier].objects == {}


# ---------------------------------------------------------------------------
# (ii) A state bucket the operator named
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("squat", ["holds the name", "creates it first"])
def test_a_state_bucket_the_operator_named_is_fixed_by_its_setting(squat) -> None:
    # FAILS WITHOUT THE FIX: the refusal said to delete the bucket in another account or call
    # AWS Support, that the tool derives the name (it is the setting's), and that CDC may have
    # run from it (CDC never uses a state bucket) -- not that the setting is the fix.
    s3 = OwnerCheckingS3()
    if squat == "holds the name":
        _foreign(s3, _CUSTOM)
    else:
        s3.squatter_creates_first = _THEM
    jobs = S3JobStore(_CUSTOM, region=_REGION, s3_client=s3, sts_client=_Sts())
    sessions = S3SessionStateStore(_CUSTOM, region=_REGION, s3_client=s3, sts_client=_Sts())
    ledger = S3OverlayLedger(
        _CUSTOM, region=_REGION, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None,
        max_attempts=1,
    )
    jobs.save(_job("j1"), None)
    sessions.save(SessionSnapshot(session_id="s1"))
    read = ledger.read("k")
    assert read.status == "unreadable"
    for text, setting in (
        (jobs.bucket_refusal or "", "DSQL_MIGRATOR_JOB_STATE_BUCKET"),
        (sessions.bucket_refusal or "", "DSQL_MIGRATOR_SESSION_STATE_BUCKET"),
        (read.error or "", "DSQL_MIGRATOR_SESSION_STATE_BUCKET"),  # the ledger follows it
    ):
        assert f"'{_CUSTOM}'" in text, text
        assert (
            f"Point {setting}, which names this bucket, at a bucket this account owns, or set "
            f"it to '{_BUCKET}', which the tool reads as its managed bucket (the value the ECS "
            "Fargate app template sets), then restart the app."
        ) in text, text
        _only(text, "setting")
        assert "The tool derives the name" not in text
        assert "CDC infrastructure" not in text and "connector plugins" not in text
        if squat == "holds the name":  # an earlier version may have restored state from it
            assert "rotate the source database credentials" in text
    # The notice above every screen says which setting to change for which state.
    _header, body = state_bucket_notice(jobs.bucket_refusal, sessions.bucket_refusal)
    assert "Point DSQL_MIGRATOR_JOB_STATE_BUCKET" in body
    assert "Point DSQL_MIGRATOR_SESSION_STATE_BUCKET" in body
    assert s3.buckets[_CUSTOM].objects == {}


def test_without_a_region_the_setting_remedy_offers_no_managed_name() -> None:
    # Without a Region the managed bucket's name is not read as the managed bucket
    # (state_bucket), so the refusal does not offer it.
    s3 = OwnerCheckingS3()
    _foreign(s3, _CUSTOM)
    jobs = S3JobStore(_CUSTOM, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    text = jobs.bucket_refusal or ""
    assert (
        "Point DSQL_MIGRATOR_JOB_STATE_BUCKET, which names this bucket, at a bucket this "
        "account owns, then restart the app."
    ) in text, text
    assert "managed bucket" not in text
    _only(text, "setting")


# ---------------------------------------------------------------------------
# (iii) The -an bucket cannot be created while another account holds the earlier name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "unusable", ["create denied", "not granted", "create denied, then a squatter creates first"]
)
def test_no_usable_bucket_is_fixed_by_allowing_the_regional_name(unusable) -> None:
    # FAILS WITHOUT THE FIX: the remedy did not tie the namespace condition to s3:CreateBucket,
    # and when the earlier name was taken during the fallback the refusal embedded the
    # earlier name's own "delete it there ... contact AWS Support" (with a double period).
    remedy = (
        f"Allow this identity s3:CreateBucket on '{_REGIONAL}' for a request whose "
        "s3:x-amz-bucket-namespace is account-regional (the tool creates it in this account's "
        "S3 account regional namespace), and s3:ListBucket on it; check SCPs and permissions "
        "boundaries too. On ECS Fargate or EC2, the app stack template of this version grants "
        "both: update the app stack with it. Then retry."
    )

    def blocked() -> OwnerCheckingS3:
        s3 = OwnerCheckingS3()
        if unusable == "not granted":  # an app stack template older than this version
            s3.no_grant = {_REGIONAL}
        else:  # an SCP or a permissions boundary
            s3.create_denied = {_REGIONAL}
        if unusable.endswith("creates first"):
            s3.squatter_creates_first = _THEM  # the earlier name is free until our create
        else:
            _foreign(s3)
        return s3

    with pytest.raises(S3ProvisionError) as deploy:
        ensure_plugin_bucket(blocked(), _ME, _REGION)
    s3 = blocked()
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    for text in (str(deploy.value), jobs.bucket_refusal or ""):
        assert f"'{_REGIONAL}'" in text and f"'{_BUCKET}'" in text, text
        assert text.endswith(remedy), text
        _only(text, "allow regional")
    assert _REGIONAL not in s3.buckets and s3.buckets[_BUCKET].objects == {}


# ---------------------------------------------------------------------------
# (iv) The managed bucket's earlier name, refused while the app runs
# ---------------------------------------------------------------------------


def test_a_kept_earlier_bucket_refused_mid_run_says_the_tool_moves_to_the_regional_name() -> None:
    # FAILS WITHOUT THE FIX: an existing deployment keeps its earlier-named bucket; once its
    # owner deleted it and another account took the name, the refusal said to delete that
    # bucket or call AWS Support -- while the next save (or Start CDC run again) moves to the
    # -an bucket, which only this account can create, on its own.
    remedy = (
        "Retry (the job and session state retry at their next save): the tool then checks "
        "its managed bucket again and, while this bucket is refused, moves to "
        f"'{_REGIONAL}' in this account's S3 account regional namespace, where only this "
        "account can create a bucket (nothing in this bucket is copied there)."
    )
    s3 = OwnerCheckingS3()
    s3.bucket(_BUCKET)
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    ledger = S3OverlayLedger(
        _BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts(), sleep=lambda _s: None,
        max_attempts=1,
    )
    jobs.save(_job("j1"), None)
    ledger.put_tables("k", {"shop.docs": _ledger_record()})
    deployer, cfn = _deployer(s3)  # Start CDC stages its template in the bucket it chose
    deployer.preflight_template(_BIG)
    del s3.buckets[_BUCKET]
    theirs = _foreign(s3)
    jobs.save(_job("j2"), None)
    read = ledger.read("k")
    with pytest.raises(CdcDeployError) as staged:
        deployer.submit_update("dsql-cdc-owner", [], template_body=_BIG)
    for text in (jobs.bucket_refusal or "", read.error or "", str(staged.value)):
        assert f"'{_BUCKET}'" in text and remedy in text, text
        _only(text, "moves")
    # ...and what it says is what happens: the next save, and Start CDC's choice of its
    # bucket, use the -an bucket.
    jobs.save(_job("j3"), None)
    assert jobs.bucket_refusal is None and "jobs/j3.json" in s3.buckets[_REGIONAL].objects
    assert ensure_plugin_bucket(s3, _ME, _REGION) == _REGIONAL
    assert theirs.objects == {} and "update_stack" not in cfn.calls


def test_a_name_in_this_accounts_regional_namespace_needs_no_remedy() -> None:
    # Only this account can create the -an name, so its refusal is this identity's
    # permissions (here: its grant on the bucket is gone mid-run): no case's remedy applies.
    assert regional_plugin_bucket_name(_ME, _REGION) == _REGIONAL
    s3 = OwnerCheckingS3()
    jobs = S3JobStore(_BUCKET, region=_REGION, s3_client=s3, sts_client=_Sts())
    jobs.save(_job("j1"), None)
    assert set(s3.buckets) == {_REGIONAL}
    s3.no_grant = {_REGIONAL}
    jobs.save(_job("j2"), None)
    text = jobs.bucket_refusal or ""
    assert text.startswith(f"This identity cannot reach S3 bucket '{_REGIONAL}'"), text
    _only(text)
