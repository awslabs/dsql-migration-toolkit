# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connected browser E2E — needs live source MySQL + target Aurora DSQL.

Opt-in and reachability-gated (see conftest ``live_infra``): runs only when
``RUN_E2E_CONNECTED=1`` AND both databases are actually reachable, otherwise it
skips cleanly. It drives the REAL connect → Query validation → Test-on-target
flow in a browser against live infrastructure, and (when Amazon Bedrock is also
reachable, ``bedrock_reachable``) the AI-DBA tuning + re-test + per-code-block
copy flow.

Design notes grounded in the app's behavior:
- Connection values are NOT typed into the browser — the app prefills the Connect
  form from ``.env`` (the same source of truth the reachability fixtures use), so
  the tests just click the Test buttons.
- AI assist is enabled BEFORE verifying: editing/toggling a field after a verify
  re-locks the workflow (connect.py invalidate_source/target).
- The tuning query is ``SELECT 1`` so "Test on target" plans on any DSQL target
  regardless of schema.
- AI replies are non-deterministic: if the model proposes no runnable rewrite
  (no ```sql block) or Bedrock degrades mid-turn, the re-test / copy assertions
  soft-skip rather than fail — that is model/infra behavior, not an app bug.
"""

from __future__ import annotations

import pytest
from playwright.sync_api import Page, expect

# Live network round-trips (IAM token, TLS, Bedrock streaming) — be generous.
_PROBE_TIMEOUT_MS = 60_000
_AI_TIMEOUT_MS = 90_000


def _verify_connections(page: Page, *, enable_ai: bool = False) -> None:
    """Verify source + target from the .env-prefilled Connect form; optionally AI.

    AI is toggled on BEFORE the connection tests because editing/toggling after a
    verify re-locks the workflow gate.
    """
    if enable_ai:
        page.get_by_text("Enable AI Assist", exact=False).first.click()
    page.get_by_role("button", name="Test source connection").click()
    expect(
        page.get_by_text("Verified", exact=True).first
    ).to_be_visible(timeout=_PROBE_TIMEOUT_MS)
    page.get_by_role("button", name="Test target connection").click()
    # Both cards now show "Verified"; the Next gate unlocks.
    expect(
        page.get_by_role("button", name="Next: Migration plan")
    ).to_be_enabled(timeout=_PROBE_TIMEOUT_MS)


def _open_query_validation(page: Page) -> None:
    page.get_by_text("Query validation", exact=False).first.click()
    expect(page.get_by_label("MySQL SQL", exact=False).first).to_be_visible(
        timeout=15_000
    )


def _convert_and_test(page: Page, sql: str, *, analyze: bool = True) -> None:
    """Convert ``sql`` and run Test on target (with EXPLAIN ANALYZE for a DPU)."""
    page.get_by_label("MySQL SQL", exact=False).first.fill(sql)
    page.get_by_role("button", name="Convert").click()
    expect(page.get_by_role("button", name="Test on target")).to_be_visible(
        timeout=10_000
    )
    if analyze:
        # Turn on EXPLAIN ANALYZE so DSQL emits the per-statement DPU estimate.
        page.get_by_text("Run EXPLAIN ANALYZE", exact=False).first.click()
    page.get_by_role("button", name="Test on target").click()


# ---------------------------------------------------------------------------
# DB tier — needs live source + target (live_infra), NOT Bedrock.
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("live_infra")
def test_connect_verifies_and_unlocks(page: Page) -> None:
    _verify_connections(page)
    expect(
        page.get_by_text("Source and target connections verified", exact=False)
    ).to_be_visible()


@pytest.mark.usefixtures("live_infra")
def test_query_convert_and_test_on_target_passes_with_dpu(page: Page) -> None:
    _verify_connections(page)
    _open_query_validation(page)
    _convert_and_test(page, "SELECT 1", analyze=True)
    # Live EXPLAIN ANALYZE against DSQL: the PASSED verdict + a DPU cost appear.
    expect(page.get_by_text("Runs on Aurora DSQL", exact=False)).to_be_visible(
        timeout=_PROBE_TIMEOUT_MS
    )
    expect(page.get_by_text("Estimated cost", exact=False)).to_be_visible(
        timeout=_PROBE_TIMEOUT_MS
    )
    assert "DPU" in page.inner_text("body")


# ---------------------------------------------------------------------------
# AI tier — additionally needs Bedrock (bedrock_reachable). Non-deterministic
# model output is soft-skipped, never hard-failed.
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("live_infra", "bedrock_reachable")
def test_tune_button_appears_only_after_passed_probe(page: Page) -> None:
    _verify_connections(page, enable_ai=True)
    _open_query_validation(page)
    # Before any Test on target, the tuner is not offered.
    expect(page.get_by_role("button", name="Tune with AI DBA")).to_have_count(0)
    _convert_and_test(page, "SELECT 1", analyze=True)
    expect(page.get_by_text("Runs on Aurora DSQL", exact=False)).to_be_visible(
        timeout=_PROBE_TIMEOUT_MS
    )
    # After a PASSED SELECT probe, the AI-DBA tuning button is offered.
    expect(page.get_by_role("button", name="Tune with AI DBA")).to_be_visible(
        timeout=15_000
    )


@pytest.mark.usefixtures("live_infra", "bedrock_reachable")
def test_tune_streams_rewrite_and_offers_retest_and_copy(page: Page) -> None:
    _verify_connections(page, enable_ai=True)
    _open_query_validation(page)
    _convert_and_test(page, "SELECT 1", analyze=True)
    expect(page.get_by_text("Runs on Aurora DSQL", exact=False)).to_be_visible(
        timeout=_PROBE_TIMEOUT_MS
    )
    page.get_by_role("button", name="Tune with AI DBA").click()
    # The AI-DBA chat drawer opens.
    expect(page.get_by_text("AI DBA — query tuning", exact=False)).to_be_visible(
        timeout=15_000
    )
    # Wait for the assistant reply to finish streaming. The "AI is writing…"
    # indicator is HIDDEN (display:none via set_visibility(False)) on completion —
    # not removed from the DOM — so wait for the "Generated by model" meta that the
    # done branch sets on an available reply (a positive completion signal that
    # also tolerates a degraded reply, handled just below).
    try:
        page.wait_for_selector(
            "text=Generated by model", timeout=_AI_TIMEOUT_MS
        )
    except Exception:  # noqa: BLE001 - degraded reply shows no model meta
        page.wait_for_selector("text=AI is writing", state="hidden", timeout=15_000)

    body = page.inner_text("body")
    if "AI reply unavailable" in body:
        pytest.skip("Bedrock degraded mid-turn (AI reply unavailable) — infra, not a bug")

    # If the model proposed a runnable rewrite, a per-code-block copy button and a
    # "Test rewrite on target" action appear; otherwise (e.g. 'already efficient')
    # soft-skip those assertions — model behavior, not an app defect.
    copy_buttons = page.locator(".nicegui-code-copy")
    retest = page.get_by_role("button", name="Test rewrite on target")
    if copy_buttons.count() == 0 and retest.count() == 0:
        pytest.skip("AI proposed no runnable rewrite (no code block) — nothing to re-test")

    if copy_buttons.count() > 0:
        expect(copy_buttons.first).to_be_visible(timeout=10_000)

    if retest.count() > 0:
        retest.first.click()
        # The re-test reprobes the target and the SAME assistant reports the
        # before/after DPU as a follow-up turn.
        expect(
            page.get_by_text("I re-tested your rewritten query", exact=False).first
        ).to_be_visible(timeout=_AI_TIMEOUT_MS)
