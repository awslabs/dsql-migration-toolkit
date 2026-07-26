# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# AWS sample CloudFormation custom-resource response helper, vendored so the
# seeder Lambda needs no extra layer. Posts the resource outcome to the
# pre-signed S3 URL CloudFormation supplies, so the stack op unblocks.
from __future__ import annotations

import json
import time
import urllib.request

SUCCESS = "SUCCESS"
FAILED = "FAILED"

# The response PUT is retried: a single failed attempt means CloudFormation receives
# NO response and then waits its OWN ~1h custom-resource timeout before failing the
# stack op (the classic DELETE_FAILED hang). During teardown the S3-gateway egress
# path may be momentarily unreachable while ENIs/routes settle, so a few bounded
# retries land the response instead of dead-ending the whole teardown. Kept well
# under the Lambda's timeout: worst case ~4*10s PUT + (3+6+9)s backoff ≈ 58s.
_SEND_MAX_ATTEMPTS = 4
_SEND_TIMEOUT_SECONDS = 10


def send(
    event,
    context,
    response_status,
    response_data,
    physical_resource_id=None,
    no_echo=False,
    reason=None,
):
    response_url = event["ResponseURL"]

    response_body = {
        "Status": response_status,
        "Reason": reason
        or f"See the details in CloudWatch Log Stream: {context.log_stream_name}",
        "PhysicalResourceId": physical_resource_id or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": no_echo,
        "Data": response_data,
    }

    json_response_body = json.dumps(response_body).encode("utf-8")

    headers = {
        "content-type": "",
        "content-length": str(len(json_response_body)),
    }

    # Retry the PUT so a transient egress hiccup during teardown does not leave
    # CloudFormation with no response (which it would then wait ~1h on before
    # DELETE_FAILED). Each attempt uses a short bounded timeout to fail fast rather
    # than hang; between attempts we back off briefly to let the S3-gateway egress
    # path settle. Any successful PUT means CloudFormation has the outcome -> return.
    last_exc: Exception | None = None
    for attempt in range(1, _SEND_MAX_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(
                response_url,
                data=json_response_body,
                headers=headers,
                method="PUT",
            )
            with urllib.request.urlopen(  # noqa: S310
                req, timeout=_SEND_TIMEOUT_SECONDS
            ) as response:
                print(f"Status code: {response.status} (attempt {attempt})")
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"send(..) attempt {attempt} failed: {exc}")
            if attempt < _SEND_MAX_ATTEMPTS:
                time.sleep(3 * attempt)  # 3s, 6s, 9s backoff
    print(
        "send(..) exhausted retries executing urllib.request.urlopen(..); "
        f"last error: {last_exc}"
    )
