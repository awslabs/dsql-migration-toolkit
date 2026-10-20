# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G145: a request that never opens the websocket no longer builds a session.

The load balancer's health check is a GET of ``/`` every 15 s from each node, with no
cookie, so each one came with a new browser id and built a new session: its state in
every per-session store, kept there for resume. Both deployed apps grew ~225 MB a day
with no user, and the 1 GiB task was OOM-killed every few days. The page is now built
only once the client's websocket is connected; a client NiceGUI prunes without ever
connecting ends the wait deleted, and nothing is built for it. A build that raises now
runs after the response, so it is shown in the page instead of NiceGUI's 500 page.

The behaviour tests drive the app's real ``/`` page through NiceGUI's user simulation in
a subprocess (tests/_g145_page_harness.py): ``main()`` changes process-wide state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from dsql_migrator.ui import app as app_module
from dsql_migrator.ui.app import build_when_connected

_HARNESS = Path(__file__).with_name("_g145_page_harness.py")


class _Client:
    """NiceGUI's Client as the page sees it: ``connected()`` returns once the websocket is
    up, or once NiceGUI deletes the client (``Client.delete`` sets the event to end such
    waits), and ``is_deleted`` tells the two apart."""

    def __init__(self, *, deleted: bool) -> None:
        self._deleted = deleted
        self.waited = False

    async def connected(self) -> None:
        self.waited = True

    @property
    def is_deleted(self) -> bool:
        return self._deleted


def test_a_connected_browser_gets_its_page() -> None:
    client, built = _Client(deleted=False), []
    assert asyncio.run(build_when_connected(client, lambda: built.append("page"))) is True
    assert client.waited and built == ["page"]


def test_a_client_pruned_without_a_connection_builds_nothing() -> None:
    client, built = _Client(deleted=True), []
    assert asyncio.run(build_when_connected(client, lambda: built.append("page"))) is False
    assert client.waited and built == []


def test_a_build_that_raises_is_logged_and_shown(monkeypatch, caplog) -> None:
    shown = []
    monkeypatch.setattr(app_module, "_show_build_failure", shown.append)

    def _raise() -> None:
        raise RuntimeError("boom")

    client = _Client(deleted=False)
    with caplog.at_level(logging.ERROR, logger="dsql_migrator.ui.app"):
        assert asyncio.run(build_when_connected(client, _raise)) is False
    assert shown == [client]
    assert any("The page could not be built" in r.getMessage() and r.exc_info for r in caplog.records)


def _drive(tmp_path: Path, *flags: str, n: int = 10) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTEST_CURRENT_TEST",)}
    out = subprocess.run(
        [sys.executable, str(_HARNESS), str(tmp_path), str(n), *flags],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=240,
    )
    assert out.returncode == 0, out.stderr[-4000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_health_checks_leave_no_session_and_a_browser_gets_its_page(tmp_path) -> None:
    result = _drive(tmp_path)
    assert result["statuses"] == [200]  # the health check still passes
    assert set(result["after_gets"].values()) == {0}, result["after_gets"]
    assert set(result["after_prune"].values()) == {0}, result["after_prune"]
    assert result["clients_after_prune"] == 0
    assert result["browser_sees_app"] is True and result["browser_sees_failure"] is False
    assert set(result["after_browser"].values()) == {1}, result["after_browser"]  # one session
    assert result["aws_refused"] == []


def test_a_page_that_fails_to_build_shows_an_error_instead_of_a_blank_page(tmp_path) -> None:
    result = _drive(tmp_path, "--broken", n=1)
    # The health check answers before the build, so it cannot see a broken build (the
    # upgrade notes say to open the app in a browser after an update).
    assert result["statuses"] == [200]
    assert result["browser_sees_failure"] is True and result["browser_sees_app"] is False


def test_the_image_leaves_out_macos_folder_metadata() -> None:
    ignore = (Path(__file__).resolve().parents[1] / ".dockerignore").read_text().splitlines()
    assert "**/.DS_Store" in ignore
