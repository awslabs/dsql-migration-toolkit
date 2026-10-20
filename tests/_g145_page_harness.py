# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Drives the real ``/`` page through NiceGUI for tests/test_g145_health_check_sessions.py.

Run as a script in its own interpreter (``main()`` changes process-wide state): it registers
the app's real page in NiceGUI's in-process user simulation (no server, no socket), then

1. sends N cookie-less GETs of ``/`` -- each from a fresh HTTP client, like the load
   balancer's health check -- and reads the per-session store sizes;
2. prunes every client that never connected, as NiceGUI's prune timer does after 60 s;
3. opens ``/`` in one simulated browser (``nicegui.testing.User``: the GET and the
   websocket handshake) and reads whether the app is shown and the store sizes;
4. with ``--broken``, replaces ``build_page`` with one that raises and opens ``/`` in a
   simulated browser, which should see the error notice.

Every AWS request is refused, and all state goes to the directory given as the first
argument. Prints one JSON object.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

WORK, N, BROKEN = sys.argv[1], int(sys.argv[2]), "--broken" in sys.argv[3:]
os.environ.update(
    {
        "NICEGUI_USER_SIMULATION": "true",
        "DSQL_MIGRATOR_JOB_STATE_PATH": f"{WORK}/job_state.sqlite",
        "DSQL_MIGRATOR_SESSION_STATE_PATH": f"{WORK}/session_state.sqlite",
        "DSQL_MIGRATOR_ACTIVITY_LOG_PATH": f"{WORK}/activity.log",
        "DSQL_MIGRATOR_STORAGE_SECRET": "g145-test-secret",
        "DB_PASSWORD": "",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
)
for key in ("DSQL_MIGRATOR_JOB_STATE_BUCKET", "DSQL_MIGRATOR_SESSION_STATE_BUCKET"):
    os.environ.pop(key, None)

import botocore.httpsession  # noqa: E402

REFUSED: list[str] = []


def _refuse(self, request, *_a, **_k):  # noqa: ANN001
    REFUSED.append(str(getattr(request, "url", "?")))
    raise RuntimeError("AWS request refused by the test harness")


botocore.httpsession.URLLib3Session.send = _refuse

import httpx  # noqa: E402
from nicegui import Client, core  # noqa: E402
from nicegui.testing.user import User  # noqa: E402

import dsql_migrator.ui.app as app_module  # noqa: E402

STORES = (
    "SESSION_STORE",
    "EVALUATION_STORE",
    "SCHEMA_CONVERSION_STORE",
    "DATA_MIGRATION_STORE",
    "VALIDATION_STORE",
    "PLAYGROUND_STORE",
)


def _sessions() -> dict[str, int]:
    return {
        name: sum(len(v) for v in vars(getattr(app_module, name)).values() if isinstance(v, dict))
        for name in STORES
    }


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(core.app), base_url="http://test")


async def _sees(user: User, text: str) -> bool:
    try:
        await user.should_see(text)
        return True
    except AssertionError:
        return False


async def run() -> dict:
    result: dict = {}
    if BROKEN:
        def _raise(*_a, **_k) -> None:
            raise RuntimeError("simulated build failure")

        app_module.build_page = _raise
    app_module.main()  # registers "/"; ui.run returns at once in user simulation
    async with core.app.router.lifespan_context(core.app):
        statuses = []
        for _ in range(N):
            async with _client() as http:
                statuses.append((await http.get("/")).status_code)
        await asyncio.sleep(0.3)
        result["statuses"] = sorted(set(statuses))
        result["after_gets"] = _sessions()
        Client.prune_instances(client_age_threshold=0)
        await asyncio.sleep(0.3)
        result["after_prune"] = _sessions()
        result["clients_after_prune"] = len(Client.instances)
        async with _client() as http:
            user = User(http)
            await user.open("/")
            await asyncio.sleep(0.5)
            result["browser_sees_app"] = await _sees(user, "Aurora DSQL Migration Tool")
            result["browser_sees_failure"] = await _sees(user, "The page could not be built")
            result["after_browser"] = _sessions()
    result["aws_refused"] = REFUSED
    return result


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run())))
