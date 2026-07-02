"""Pytest fixtures for the browser E2E suite (Playwright + a real app server).

These are true end-to-end tests: they launch the actual NiceGUI app as a
subprocess and drive it with a real Chromium browser, so they catch things the
in-process UI-double unit tests cannot — the page actually rendering, the
WebSocket hydrating, buttons wiring up, dialogs opening, code-block copy buttons
appearing, etc.

Two tiers:
- **smoke** (default): no live infrastructure. The app loads and every screen /
  nav item / form renders. Always runs (given a browser is installed).
- **connected** (``@pytest.mark.connected``): needs a reachable source MySQL +
  target Aurora DSQL (via ``.env``). Skipped unless ``RUN_E2E_CONNECTED=1``.

Requirements (dev only): ``playwright`` (in the dev dependency group) and its
Chromium build (``.venv/bin/python -m playwright install chromium`` once). The
whole ``tests/e2e`` directory is skipped cleanly if Playwright is not installed,
so the normal ``pytest`` run is unaffected on machines without it.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import pytest

# Skip the entire E2E suite (collection included) when Playwright is absent, so a
# plain `pytest` run on a machine without it neither errors nor is affected.
pytest.importorskip(
    "playwright.sync_api",
    reason="Playwright not installed; run `.venv/bin/python -m playwright install "
    "chromium` and install the dev deps to enable browser E2E.",
)
from playwright.sync_api import Browser, Page, sync_playwright  # noqa: E402


def pytest_collection_modifyitems(items) -> None:
    """Tag every test under tests/e2e with the ``e2e`` marker.

    The default ``pytest`` run deselects ``-m 'not e2e'`` (see pyproject), so the
    heavy browser suite runs only on an explicit ``-m e2e`` / ``pytest tests/e2e``.
    """
    for item in items:
        if "tests/e2e/" in str(item.fspath).replace("\\", "/"):
            item.add_marker(pytest.mark.e2e)


def _free_port() -> int:
    """Grab an OS-assigned free TCP port (so E2E never clashes with a dev server)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http_ready(url: str, timeout: float = 40.0) -> bool:
    """Poll ``url`` until it returns any HTTP response, or time out."""
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):  # noqa: S310 - localhost
                return True
        except urllib.error.HTTPError:
            return True  # any HTTP status means the server is up
        except OSError:
            time.sleep(0.5)
    return False


def _launch_app(extra_env: "dict[str, str] | None" = None) -> "tuple[subprocess.Popen, str, object]":
    """Start the app subprocess on a free port; return (proc, base_url, log_file)."""
    port = _free_port()
    env = dict(os.environ)
    # CRITICAL: NiceGUI's ui.run() switches into "screen test" mode when it thinks
    # it is under pytest — it checks for PYTEST_CURRENT_TEST and then demands
    # NICEGUI_SCREEN_TEST_PORT, crashing our plain app subprocess with a KeyError.
    # We launch the REAL production app as a child process, so strip the inherited
    # pytest markers to make it behave exactly like a normal `... ui` invocation.
    for key in ("PYTEST_CURRENT_TEST", "NICEGUI_SCREEN_TEST_PORT"):
        env.pop(key, None)
    env["DSQL_MIGRATOR_APP_HOST"] = "127.0.0.1"
    env["DSQL_MIGRATOR_APP_PORT"] = str(port)
    # A fixed storage secret so app.storage.browser works without a random key.
    env.setdefault("DSQL_MIGRATOR_STORAGE_SECRET", "e2e-test-storage-secret")
    # Keep test state off the repo: use temp paths for job/session/activity stores.
    tmp = os.environ.get("PYTEST_E2E_TMP", "/tmp/dsql_e2e")
    os.makedirs(tmp, exist_ok=True)
    env["DSQL_MIGRATOR_JOB_STATE_PATH"] = os.path.join(tmp, "job_state.sqlite")
    env["DSQL_MIGRATOR_SESSION_STATE_PATH"] = os.path.join(tmp, "session_state.sqlite")
    env["DSQL_MIGRATOR_ACTIVITY_LOG_PATH"] = os.path.join(tmp, "activity.log")
    if extra_env:
        env.update(extra_env)

    # Capture stdout/stderr so a startup failure is diagnosable (not a silent
    # "did not become ready"). Log name is per-port to avoid clashes.
    log_path = os.path.join(tmp, f"server-{port}.log")
    log_file = open(log_path, "w")  # noqa: SIM115 - closed by the fixture
    proc = subprocess.Popen(
        [sys.executable, "-m", "dsql_migrator.cli.main", "ui"],
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )
    base_url = f"http://127.0.0.1:{port}"
    if not _wait_http_ready(base_url + "/"):
        proc.terminate()
        log_file.flush()
        try:
            tail = "".join(open(log_path).readlines()[-25:])
        except OSError:
            tail = "(no server log)"
        log_file.close()
        pytest.fail(
            f"E2E app server did not become ready on {base_url}\n"
            f"exit={proc.poll()}\n--- server log tail ---\n{tail}"
        )
    return proc, base_url, log_file


def _stop_app(proc: subprocess.Popen, log_file: object) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    log_file.close()  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def app_server() -> Iterator[str]:
    """The real app on a dedicated port (production-like; no dev unlock)."""
    proc, base_url, log_file = _launch_app()
    try:
        yield base_url
    finally:
        _stop_app(proc, log_file)


@pytest.fixture(scope="session")
def app_server_unlocked() -> Iterator[str]:
    """The real app with DSQL_MIGRATOR_DEV_UNLOCK_STEPS=1.

    Lets an E2E open workflow steps and optional tools (e.g. Query validation)
    WITHOUT a live source/target connection — the dev escape hatch bypasses only
    the connection/prereq gating, never AWS/data safety. Used to exercise the
    Query Playground conversion flow (pure sqlglot, no DB) in the browser.
    """
    proc, base_url, log_file = _launch_app(
        {"DSQL_MIGRATOR_DEV_UNLOCK_STEPS": "1"}
    )
    try:
        yield base_url
    finally:
        _stop_app(proc, log_file)


@pytest.fixture(scope="session")
def browser() -> Iterator[Browser]:
    """A session-scoped headless Chromium (reused across tests for speed)."""
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        try:
            yield b
        finally:
            b.close()


def _open_page(browser: Browser, base_url: str) -> "tuple[object, Page]":
    context = browser.new_context()
    pg = context.new_page()
    pg.goto(base_url + "/", wait_until="networkidle", timeout=30000)
    # NiceGUI renders over the WebSocket after load; wait for a known element.
    pg.wait_for_selector("text=DSQL Migration Tool", timeout=15000)
    return context, pg


@pytest.fixture()
def page(browser: Browser, app_server: str) -> Iterator[Page]:
    """A fresh browser page (own context = clean storage) on the production app."""
    context, pg = _open_page(browser, app_server)
    try:
        yield pg
    finally:
        context.close()


@pytest.fixture()
def page_unlocked(browser: Browser, app_server_unlocked: str) -> Iterator[Page]:
    """A fresh page on the dev-unlocked app (steps/tools open without connecting)."""
    context, pg = _open_page(browser, app_server_unlocked)
    try:
        yield pg
    finally:
        context.close()


def _connected_enabled() -> bool:
    return os.environ.get("RUN_E2E_CONNECTED") == "1"


connected = pytest.mark.skipif(
    not _connected_enabled(),
    reason="live-connection E2E; set RUN_E2E_CONNECTED=1 (needs a reachable "
    "source MySQL + target Aurora DSQL via .env)",
)
