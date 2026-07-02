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


def _load_env_file() -> "dict[str, str]":
    """Parse the repo-root .env the same way the app / scripts do (best-effort)."""
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    values: dict[str, str] = {}
    try:
        with open(os.path.join(root, ".env"), encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                values[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return values


@pytest.fixture(scope="session")
def live_infra() -> None:
    """Skip the whole connected tier unless source MySQL + target DSQL are REACHABLE.

    The ``connected`` marker only checks the opt-in env var; this fixture actually
    opens both connections (read-only) so a connected run against a down DB skips
    cleanly instead of failing mid-browser. Connection values come from the same
    ``.env`` the app prefills the Connect form from — the single source of truth.
    """
    if not _connected_enabled():
        pytest.skip("set RUN_E2E_CONNECTED=1 to run the connected E2E tier")
    env = {**_load_env_file(), **os.environ}
    if not env.get("DB_HOST") or not env.get("TARGET_ENDPOINT"):
        pytest.skip("connected E2E needs DB_HOST + TARGET_ENDPOINT in .env")

    import sys as _sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    _sys.path.insert(0, os.path.join(root, "src"))
    # Source MySQL reachability (read-only SELECT 1).
    try:
        import pymysql

        conn = pymysql.connect(
            host=env["DB_HOST"], port=int(env.get("DB_PORT", "3306")),
            user=env.get("DB_USER", "admin"),
            password=env.get("DB_PASSWORD") or env.get("MYSQL_PWD") or "",
            connect_timeout=10, read_timeout=15,
        )
        conn.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"source MySQL unreachable: {type(exc).__name__}: {str(exc)[:100]}")
    # Target DSQL reachability (IAM token + TLS SELECT 1).
    try:
        from dsql_migrator.core.models import TargetConnectionConfig
        from dsql_migrator.core.target_connection import DsqlConnector

        ep = env["TARGET_ENDPOINT"]
        region = env.get("TARGET_REGION") or (
            ep.split(".dsql.")[1].split(".on.aws")[0] if ".dsql." in ep else "us-east-1"
        )
        cfg = TargetConnectionConfig(
            cluster_endpoint=ep, region=region,
            database=env.get("TARGET_DATABASE", "postgres"),
            username=env.get("TARGET_USERNAME", "admin"),
        )
        DsqlConnector(cfg, aws_profile=env.get("AWS_PROFILE")).connect().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"target DSQL unreachable: {type(exc).__name__}: {str(exc)[:100]}")


@pytest.fixture(scope="session")
def bedrock_reachable() -> None:
    """Skip the AI-tuning sub-tier unless Amazon Bedrock is actually reachable.

    DB reachability does NOT imply Bedrock (missing bedrock:InvokeModel, blank/
    wrong region, throttling), so the "Tune with AI DBA" tests gate on this in
    ADDITION to ``live_infra``. Runs the app's own "Verify AI access" preflight.
    """
    env = {**_load_env_file(), **os.environ}
    import sys as _sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    _sys.path.insert(0, os.path.join(root, "src"))
    try:
        from dsql_migrator.ui.ai_assist import build_ai_assist_config, run_verify_ai_access

        cfg = build_ai_assist_config(
            enabled=True,
            model_id=env.get("BEDROCK_MODEL_ID"),
            region=env.get("BEDROCK_REGION"),
        )
        result = run_verify_ai_access(cfg, env.get("AWS_PROFILE"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Bedrock preflight errored: {type(exc).__name__}: {str(exc)[:100]}")
    if not getattr(result, "ok", False):
        pytest.skip(
            f"Bedrock not reachable ({getattr(result, 'reason', '?')}); "
            "AI-tuning E2E skipped"
        )
