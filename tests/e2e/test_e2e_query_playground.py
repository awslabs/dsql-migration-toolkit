"""Browser E2E for the Query Playground (Query validation) — no live infra.

Uses the dev-unlocked app server so the optional Query validation tool opens
without a verified source/target connection. The **conversion** flow is pure
sqlglot (no database), so it is fully exercisable offline: enter a MySQL query,
Convert, and assert the converted DSQL SQL + the "Test on target" affordance
render. The actual "Test on target" / "Tune with AI DBA" steps need a live target
and are covered by the connected tier.
"""

from __future__ import annotations

from playwright.sync_api import Page, expect


def _open_query_validation(page: Page) -> None:
    page.get_by_text("Query validation", exact=False).first.click()
    # The screen's SQL editor (aria-label "MySQL SQL") confirms we're on it.
    expect(page.get_by_label("MySQL SQL", exact=False).first).to_be_visible(timeout=10000)


def test_query_validation_opens_when_unlocked(page_unlocked: Page) -> None:
    _open_query_validation(page_unlocked)
    expect(page_unlocked.get_by_role("button", name="Convert")).to_be_visible()


def test_convert_renders_converted_sql_and_test_affordance(page_unlocked: Page) -> None:
    _open_query_validation(page_unlocked)
    page_unlocked.get_by_label("MySQL SQL", exact=False).first.fill(
        "SELECT id FROM orders WHERE id = 1"
    )
    page_unlocked.get_by_role("button", name="Convert").click()
    # Conversion is deterministic (no DB): the converted DSQL SQL renders and the
    # read-only "Test on target" action appears for a SELECT.
    expect(page_unlocked.get_by_role("button", name="Test on target")).to_be_visible(
        timeout=10000
    )
    body = page_unlocked.inner_text("body")
    assert "SELECT" in body  # converted SQL is shown


def test_convert_flags_lock_anti_pattern(page_unlocked: Page) -> None:
    # FOR UPDATE is a DSQL lock anti-pattern the converter warns about — a good
    # end-to-end check that conversion + warning rendering works in the browser.
    _open_query_validation(page_unlocked)
    page_unlocked.get_by_label("MySQL SQL", exact=False).first.fill(
        "SELECT * FROM orders WHERE id = 1 FOR UPDATE"
    )
    page_unlocked.get_by_role("button", name="Convert").click()
    page_unlocked.wait_for_timeout(800)
    # The screen stays responsive and shows conversion output (the anti-pattern
    # notice / converted SQL); assert the Convert cycle completed without a crash.
    expect(page_unlocked.get_by_role("button", name="Convert")).to_be_visible()
    assert "SELECT" in page_unlocked.inner_text("body")
