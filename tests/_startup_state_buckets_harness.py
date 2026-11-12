# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Runs the app's ``main()`` with S3 job and session state buckets, for
tests/test_s3_bucket_owner.py, and prints what the activity log recorded at startup.

Run as a script in its own interpreter (``main()`` changes process-wide state). The two state
buckets are set as the ECS Fargate app template sets them, and every AWS request is refused
before it is sent (the credentials are placeholders), so STS cannot name this identity's
account and each store refuses its bucket. All local state goes to the directory given as
the first argument. Prints one JSON object: the activity log's records and the URLs of the
refused AWS requests.
"""

from __future__ import annotations

import json
import os
import sys

WORK = sys.argv[1]
for key in [k for k in os.environ if k.startswith(("AWS_", "DSQL_MIGRATOR_"))]:
    del os.environ[key]
_BUCKET = "mysql-dsql-migrator-plugins-111122223333-us-east-1"
os.environ.update(
    {
        "NICEGUI_USER_SIMULATION": "true",
        "DSQL_MIGRATOR_AWS_REGION": "us-east-1",
        "DSQL_MIGRATOR_JOB_STATE_BUCKET": _BUCKET,
        "DSQL_MIGRATOR_SESSION_STATE_BUCKET": _BUCKET,
        "DSQL_MIGRATOR_JOB_STATE_PATH": f"{WORK}/job_state.sqlite",
        "DSQL_MIGRATOR_SESSION_STATE_PATH": f"{WORK}/session_state.sqlite",
        "DSQL_MIGRATOR_ACTIVITY_LOG_PATH": f"{WORK}/activity.log",
        "DSQL_MIGRATOR_STORAGE_SECRET": "startup-test-secret",
        "DB_PASSWORD": "",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_CONFIG_FILE": f"{WORK}/no-aws-config",
        "AWS_SHARED_CREDENTIALS_FILE": f"{WORK}/no-aws-credentials",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
)

import botocore.httpsession  # noqa: E402

REFUSED: list[str] = []


def _refuse(self, request, *_a, **_k):  # noqa: ANN001
    REFUSED.append(str(getattr(request, "url", "?")))
    raise RuntimeError("AWS request refused by the test harness")


botocore.httpsession.URLLib3Session.send = _refuse

import dsql_migrator.ui.app as app_module  # noqa: E402

if __name__ == "__main__":
    app_module.main()  # ui.run returns at once in user simulation
    with open(f"{WORK}/activity.log", encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    print(json.dumps({"records": records, "aws_refused": REFUSED}))
