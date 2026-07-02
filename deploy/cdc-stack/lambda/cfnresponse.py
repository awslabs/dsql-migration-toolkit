# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# AWS sample CloudFormation custom-resource response helper, vendored so the
# seeder Lambda needs no extra layer. Posts the resource outcome to the
# pre-signed S3 URL CloudFormation supplies, so the stack op unblocks.
from __future__ import annotations

import json
import urllib.request

SUCCESS = "SUCCESS"
FAILED = "FAILED"


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

    try:
        req = urllib.request.Request(
            response_url,
            data=json_response_body,
            headers=headers,
            method="PUT",
        )
        # Short bounded timeout: the response URL is an S3 pre-signed PUT reachable
        # via the stack's S3 gateway VPC endpoint. If egress is ever unavailable
        # (e.g. a teardown race), fail fast rather than hanging the full 122s so the
        # invocation does not burn its whole budget.
        with urllib.request.urlopen(req, timeout=10) as response:  # noqa: S310
            print(f"Status code: {response.status}")
    except Exception as exc:  # noqa: BLE001
        print(f"send(..) failed executing urllib.request.urlopen(..): {exc}")
