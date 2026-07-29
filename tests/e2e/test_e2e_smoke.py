# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Browser E2E smoke tests — no live infrastructure required.

These drive a real Chromium against the real app and assert the UI actually
renders and its core affordances work, catching "the page is broken / won't
hydrate" regressions that the in-process UI-double unit tests cannot. They do NOT
connect to any source/target, so they run anywhere the app boots.
"""

from __future__ import annotations

from playwright.sync_api import Page, expect


def test_app_loads_with_title(page: Page) -> None:
    assert page.title() == "DSQL Migration Tool"
    # The Connect screen (the landing step) renders its connection actions.
    expect(page.get_by_role("button", name="Test source connection")).to_be_visible()
    expect(page.get_by_role("button", name="Test target connection")).to_be_visible()


def test_workflow_steps_render_in_nav(page: Page) -> None:
    # All five workflow steps + the optional Query validation tool appear in the
    # left nav (locked until connected, but present).
    for label in (
        "1. Evaluation",
        "2. Schema Conversion",
        "3. Data Migration",
        "4. Validation",
        "5. Cut over",
        "Query validation",
    ):
        expect(page.get_by_text(label, exact=False).first).to_be_visible()


def test_connect_form_fields_present(page: Page) -> None:
    # Source + target connection inputs are rendered (by aria-label).
    for aria in ("Host", "Port", "Username", "Password", "Cluster endpoint"):
        expect(page.get_by_label(aria, exact=False).first).to_be_visible()


def test_start_over_dialog_opens_and_cancels(page: Page) -> None:
    # The Start over control opens its type-to-confirm dialog and can be dismissed
    # without resetting anything.
    page.get_by_role("button", name="Start over").click()
    expect(page.get_by_text("Start over — reset this session")).to_be_visible()
    page.get_by_role("button", name="Cancel").click()
    expect(page.get_by_text("Start over — reset this session")).to_be_hidden()


def test_first_run_orientation_panel_expands(page: Page) -> None:
    # The "New here?" orientation expansion opens to show guidance.
    page.get_by_text("New here?", exact=False).first.click()
    # An expansion reveals more text; just assert the click did not crash the page
    # and the app is still responsive (title still there).
    expect(page.get_by_role("button", name="Test source connection")).to_be_visible()


def test_locked_step_is_gated_before_connect(page: Page) -> None:
    # A workflow step advertises it is locked until a connection is verified.
    expect(page.get_by_text("Locked until connected").first).to_be_visible()
