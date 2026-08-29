# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the shared AWS-style design system (ui.design).

These lock in the single-source-of-truth contract: the tone palette is complete
and Tailwind-only, and the renderers emit the expected box/label structure with a
NiceGUI double (no running server). Other UI modules delegate to these helpers, so
keeping them correct keeps the whole app's visual language consistent.
"""

from __future__ import annotations

from dsql_migrator.ui.design import (
    AI_ACCENT_BG,
    AI_ACCENT_BORDER,
    AI_ACCENT_BUBBLE_BG,
    AI_ACCENT_COLOR,
    AI_ACCENT_TEXT,
    BADGE_TONES,
    CHIP_GROUP_PALETTE,
    INLINE_HINT_TEXT,
    NOTICE_STYLE,
    SEGMENTED_TOGGLE_CLASSES,
    EXPANSION_PANEL_CLASSES,
    SEGMENTED_TOGGLE_PROPS,
    badge_classes,
    chip_group_index,
    chip_group_text_class,
    inline_hint,
    render_notice,
    schema_chip_classes,
    section_header,
    segmented_control,
)


class _El:
    """Chainable no-op element double; records classes/props/icon if asked."""

    def __init__(self, recorder, kind, icon=None):
        self._recorder = recorder
        self._kind = kind
        self._icon = icon

    def classes(self, value="", *_a, **_k):
        if value:
            self._recorder.classes.append(value)
        return self

    def props(self, value="", *_a, **_k):
        if value:
            self._recorder.props.append(value)
        return self

    def on(self, event, handler=None, *_a, **_k):
        # radio_tiles registers each tile's selection as a click on its card.
        if event == "click" and handler is not None:
            self._recorder.clicks.append(handler)
        return self

    def tooltip(self, text="", *_a, **_k):
        # Recorded separately from `texts`: tooltip content is NOT visible text, and the
        # design rule is that guidance must not live only here.
        if text:
            self._recorder.tooltips.append(str(text))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingUi:
    """Minimal NiceGUI stand-in capturing emitted text, icons, classes and props."""

    def __init__(self):
        self.texts: list[str] = []
        self.icons: list[str] = []
        self.classes: list[str] = []
        self.props: list[str] = []
        self.spinner_colors: list[str] = []
        self.clicks: list = []  # radio_tiles click handlers, in render order
        self.tooltips: list[str] = []  # hover-only text, kept apart from `texts`

    def spinner(self, *_a, color=None, **_k):
        self.spinner_colors.append(str(color))
        return _El(self, "spinner")

    def card(self, *_a, **_k):
        return _El(self, "card")

    def row(self, *_a, **_k):
        return _El(self, "row")

    def column(self, *_a, **_k):
        return _El(self, "column")

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _El(self, "label")

    def icon(self, name="", *_a, **_k):
        if name:
            self.icons.append(str(name))
        return _El(self, "icon", icon=name)

    def space(self, *_a, **_k):
        return _El(self, "space")

    def badge(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _El(self, "badge")

    def element(self, tag="div", *_a, **_k):
        return _El(self, tag)

    def toggle(self, options=None, *_a, value=None, on_change=None, **_k):
        # Record the option labels so a test can assert what the segmented
        # control offered; returns a chainable element double.
        if isinstance(options, dict):
            self.texts.extend(str(v) for v in options.values())
        elif isinstance(options, (list, tuple)):
            self.texts.extend(str(v) for v in options)
        return _El(self, "toggle")

    def select(self, options=None, *_a, value=None, label=None, on_change=None, **_k):
        # Record the option labels and the floating property label so a test can
        # assert what the filter dropdown offered; returns a chainable double.
        if label:
            self.texts.append(str(label))
        if isinstance(options, dict):
            self.texts.extend(str(v) for v in options.values())
        elif isinstance(options, (list, tuple)):
            self.texts.extend(str(v) for v in options)
        return _El(self, "select")


# ---------------------------------------------------------------------------
# Palette invariants
# ---------------------------------------------------------------------------


def test_notice_palette_has_all_four_tones() -> None:
    assert set(NOTICE_STYLE) == {"info", "success", "warning", "error"}
    for tone, (bg, border, icon_color, icon) in NOTICE_STYLE.items():
        # One palette: Tailwind *-50 backgrounds, *-200 borders, *-600 icons.
        assert bg.endswith("-50"), f"{tone} bg should be a -50 shade: {bg}"
        assert border.endswith("-200"), f"{tone} border should be -200: {border}"
        assert icon_color.endswith("-600"), f"{tone} icon should be -600: {icon_color}"
        assert icon, f"{tone} must have a default icon"


def test_notice_palette_uses_amber_not_orange_for_warning() -> None:
    bg, border, icon_color, _icon = NOTICE_STYLE["warning"]
    assert "amber" in bg and "amber" in border and "amber" in icon_color
    assert "orange" not in (bg + border + icon_color)


def test_ai_accent_tokens_are_a_single_indigo_source_of_truth() -> None:
    # The AI DBA brand accent must live in ONE place (design.py), not be re-typed
    # inline in the panel. One indigo family across the token set; caller composes
    # the border shorthand (border / border-b) with the border COLOR token.
    assert AI_ACCENT_COLOR == "indigo-6"
    assert AI_ACCENT_TEXT == "text-indigo-700"
    assert AI_ACCENT_BG == "bg-indigo-50"
    assert AI_ACCENT_BORDER == "indigo-100"  # color only (no "border-" prefix)
    assert AI_ACCENT_BUBBLE_BG == "bg-indigo-600"
    # All accent tokens are the same brand hue (indigo), so the surface reads coherent.
    for token in (AI_ACCENT_COLOR, AI_ACCENT_TEXT, AI_ACCENT_BG,
                  AI_ACCENT_BORDER, AI_ACCENT_BUBBLE_BG):
        assert "indigo" in token


def test_ai_panel_does_not_hardcode_indigo_inline() -> None:
    # Regression: the panel must reference the design-system AI-accent tokens, not
    # re-type "indigo-..." in class/props strings (the CLAUDE.md single-source rule).
    import inspect

    from dsql_migrator.ui import ai_panel

    src = inspect.getsource(ai_panel)
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # a descriptive comment may mention the color
        assert "indigo-" not in line, f"hardcoded indigo in ai_panel: {line!r}"


def test_badge_tones_present_and_fallback() -> None:
    assert {"ok", "bad", "active", "neutral", "reconnect"} <= set(BADGE_TONES)
    assert badge_classes("ok") == BADGE_TONES["ok"]
    # Unknown tone falls back to neutral, never raises.
    assert badge_classes("does-not-exist") == BADGE_TONES["neutral"]


# ---------------------------------------------------------------------------
# Status dot (StatusIndicator)
# ---------------------------------------------------------------------------


def test_status_dot_tones_cover_badge_tones() -> None:
    from dsql_migrator.ui.design import STATUS_DOT_TONES

    assert set(STATUS_DOT_TONES) >= {"ok", "bad", "active", "neutral", "reconnect"}


def test_render_status_dot_emits_dot_and_text() -> None:
    from dsql_migrator.ui.design import STATUS_DOT_TONES, render_status_dot

    ui = _RecordingUi()
    render_status_dot(ui, "Connected", tone="ok")
    assert "Connected" in ui.texts
    blob = " ".join(ui.classes)
    dot_bg, text_color = STATUS_DOT_TONES["ok"]
    assert dot_bg in blob
    assert text_color in blob


def test_render_status_dot_unknown_tone_falls_back() -> None:
    from dsql_migrator.ui.design import STATUS_DOT_TONES, render_status_dot

    ui = _RecordingUi()
    render_status_dot(ui, "Unknown", tone="does-not-exist")
    blob = " ".join(ui.classes)
    dot_bg, _text_color = STATUS_DOT_TONES["neutral"]
    assert dot_bg in blob


def test_status_pill_tones_cover_badge_tones() -> None:
    from dsql_migrator.ui.design import STATUS_PILL_TONES

    assert set(STATUS_PILL_TONES) >= {"ok", "bad", "active", "neutral", "reconnect"}


def test_render_status_pill_emits_dot_text_and_tinted_container() -> None:
    from dsql_migrator.ui.design import STATUS_PILL_TONES, render_status_pill

    ui = _RecordingUi()
    render_status_pill(ui, "Connected", tone="ok")
    assert "Connected" in ui.texts
    blob = " ".join(ui.classes)
    dot_bg, text_color, pill_bg = STATUS_PILL_TONES["ok"]
    assert dot_bg in blob  # the leading status dot
    assert text_color in blob  # the label color
    assert pill_bg in blob  # the tinted pill container
    assert "rounded-full" in blob  # rendered as a pill, not loose text


def test_render_status_pill_unknown_tone_falls_back() -> None:
    from dsql_migrator.ui.design import STATUS_PILL_TONES, render_status_pill

    ui = _RecordingUi()
    render_status_pill(ui, "Unknown", tone="does-not-exist")
    blob = " ".join(ui.classes)
    _dot_bg, _text_color, pill_bg = STATUS_PILL_TONES["neutral"]
    assert pill_bg in blob


# ---------------------------------------------------------------------------
# render_notice
# ---------------------------------------------------------------------------


def test_render_notice_emits_header_body_and_tone_icon() -> None:
    ui = _RecordingUi()
    render_notice(ui, tone="success", header="Done", body="All good.")
    assert "Done" in ui.texts
    assert "All good." in ui.texts
    # Default icon for the tone is used.
    assert NOTICE_STYLE["success"][3] in ui.icons
    # The box carries the tone's background + border classes.
    blob = " ".join(ui.classes)
    assert NOTICE_STYLE["success"][0] in blob  # bg
    assert NOTICE_STYLE["success"][1] in blob  # border


def test_render_notice_unknown_tone_falls_back_to_info() -> None:
    ui = _RecordingUi()
    render_notice(ui, tone="bogus", header="Heads up")
    assert NOTICE_STYLE["info"][3] in ui.icons


def test_render_activity_event_is_a_cohesive_tinted_chip() -> None:
    from dsql_migrator.ui.design import ACTIVITY_EVENT_STYLE, render_activity_event

    ui = _RecordingUi()
    render_activity_event(ui, "Source (MySQL) connection test failed", tone="error")
    assert "Source (MySQL) connection test failed" in ui.texts
    blob = " ".join(ui.classes)
    bg, border, icon_color, icon = ACTIVITY_EVENT_STYLE["error"]
    # Cohesive: tinted background + matching border + matching icon color (not a bare
    # white box with a loud edge), and the tone's default glyph.
    assert bg in blob and border in blob and icon_color in blob
    assert icon in ui.icons
    # Text stays dark and readable, not faint gray.
    assert "text-gray-700" in blob


def test_render_activity_event_unknown_tone_falls_back_to_info() -> None:
    from dsql_migrator.ui.design import ACTIVITY_EVENT_STYLE, render_activity_event

    ui = _RecordingUi()
    render_activity_event(ui, "Something happened", tone="bogus")
    assert ACTIVITY_EVENT_STYLE["info"][3] in ui.icons


def test_render_notice_icon_override() -> None:
    ui = _RecordingUi()
    render_notice(ui, tone="info", header="Cost", body="x", icon="payments")
    assert "payments" in ui.icons
    assert NOTICE_STYLE["info"][3] not in ui.icons


def test_render_notice_body_optional() -> None:
    ui = _RecordingUi()
    render_notice(ui, tone="info", header="Header only")
    assert ui.texts == ["Header only"]


def test_render_notice_busy_swaps_the_glyph_for_a_spinner_and_badge() -> None:
    # A static icon cannot distinguish "this is happening right now" from "here is a
    # fact", so a notice reporting a long-running background operation (a CDC teardown
    # runs ~15-45 min) marks itself busy: animated spinner + "In progress" badge.
    ui = _RecordingUi()
    render_notice(ui, tone="info", header="Teardown in progress", body="~15-45 min.")
    assert ui.spinner_colors == []          # not busy -> no spinner
    assert NOTICE_STYLE["info"][3] in ui.icons

    busy = _RecordingUi()
    render_notice(
        busy, tone="info", header="Teardown in progress", body="~15-45 min.", busy=True
    )
    assert busy.spinner_colors == ["primary"]  # tone -> Quasar color
    assert busy.icons == []                    # the spinner REPLACES the static glyph
    assert "In progress" in busy.texts
    assert "Teardown in progress" in busy.texts
    assert "~15-45 min." in busy.texts


def test_render_notice_busy_spinner_color_follows_the_tone() -> None:
    # ui.spinner takes a Quasar color name, while NOTICE_STYLE holds a Tailwind text
    # class -- so the mapping is explicit and must cover every tone.
    from dsql_migrator.ui.design import _QUASAR_SPINNER_COLOR

    assert set(_QUASAR_SPINNER_COLOR) == set(NOTICE_STYLE)
    for tone, expected in _QUASAR_SPINNER_COLOR.items():
        ui = _RecordingUi()
        render_notice(ui, tone=tone, header="x", busy=True)
        assert ui.spinner_colors == [expected], tone

    # An unknown tone still renders (falls back like the palette does).
    unknown = _RecordingUi()
    render_notice(unknown, tone="bogus", header="x", busy=True)
    assert unknown.spinner_colors == ["primary"]


# ---------------------------------------------------------------------------
# section_header
# ---------------------------------------------------------------------------


def test_section_header_with_badge_returns_badge_element() -> None:
    ui = _RecordingUi()
    badge_el = section_header(
        ui, icon="storage", title="Source", badge=("Connected", "positive")
    )
    assert "Source" in ui.texts
    assert "Connected" in ui.texts
    assert "storage" in ui.icons
    assert badge_el is not None


def test_section_header_without_badge_returns_none() -> None:
    ui = _RecordingUi()
    result = section_header(ui, icon="cloud", title="Target")
    assert result is None
    assert "Target" in ui.texts


# ---------------------------------------------------------------------------
# inline_hint
# ---------------------------------------------------------------------------


def test_inline_hint_palette_complete_and_warning_is_amber() -> None:
    assert set(INLINE_HINT_TEXT) == {"info", "success", "warning", "error", "neutral"}
    # One palette: warnings are amber (never orange), all are Tailwind text colors.
    assert INLINE_HINT_TEXT["warning"] == "text-amber-700"
    assert "orange" not in " ".join(INLINE_HINT_TEXT.values())
    for tone, cls in INLINE_HINT_TEXT.items():
        assert cls.startswith("text-"), tone


def test_inline_hint_applies_tone_color_and_emits_text() -> None:
    ui = _RecordingUi()
    inline_hint(ui, "Select at least one table", tone="warning")
    assert "Select at least one table" in ui.texts
    blob = " ".join(ui.classes)
    assert INLINE_HINT_TEXT["warning"] in blob
    assert "text-xs" in blob


def test_inline_hint_defaults_to_neutral_and_unknown_tone_falls_back() -> None:
    ui = _RecordingUi()
    inline_hint(ui, "just guidance")  # default neutral
    inline_hint(ui, "bogus tone", tone="does-not-exist")
    blob = " ".join(ui.classes)
    # Both resolve to the neutral gray (default + fallback).
    assert blob.count(INLINE_HINT_TEXT["neutral"]) == 2


# ---------------------------------------------------------------------------
# segmented_control
# ---------------------------------------------------------------------------


def test_expansion_panel_border_token_matches_the_card_palette() -> None:
    # Collapsible panels must read as bordered panels like the cards around them,
    # using the one palette (Tailwind border-*-200, rounded).
    assert "border" in EXPANSION_PANEL_CLASSES
    assert "rounded" in EXPANSION_PANEL_CLASSES
    assert "border-gray-200" in EXPANSION_PANEL_CLASSES


def test_segmented_control_tokens_are_aws_style() -> None:
    # One palette: selected segment filled primary on a white group; the group
    # is bordered and clips its segments (Cloudscape "SegmentedControl" look).
    assert "toggle-color=primary" in SEGMENTED_TOGGLE_PROPS
    assert "no-caps" in SEGMENTED_TOGGLE_PROPS
    assert "border" in SEGMENTED_TOGGLE_CLASSES
    assert "rounded" in SEGMENTED_TOGGLE_CLASSES


def test_segmented_control_emits_options_props_and_border() -> None:
    ui = _RecordingUi()
    captured: list[object] = []
    toggle = segmented_control(
        ui,
        {"ALL": "All", "ATTENTION": "Needs attention"},
        value="ALL",
        on_change=lambda e: captured.append(e),
    )
    assert toggle is not None
    # The option labels were offered.
    assert "All" in ui.texts and "Needs attention" in ui.texts
    # The shared AWS-style props and border classes were applied.
    props_blob = " ".join(ui.props)
    assert "toggle-color=primary" in props_blob and "no-caps" in props_blob
    classes_blob = " ".join(ui.classes)
    assert "border" in classes_blob and "rounded" in classes_blob


def test_segmented_control_appends_caller_props_and_classes() -> None:
    ui = _RecordingUi()
    segmented_control(
        ui,
        {"ALL": "All"},
        value="ALL",
        on_change=lambda e: None,
        extra_props="spread",
        classes="w-full",
    )
    assert "spread" in " ".join(ui.props)
    assert "w-full" in " ".join(ui.classes)


# ---------------------------------------------------------------------------
# filter_select / filter_bar
# ---------------------------------------------------------------------------


def test_filter_select_tokens_are_aws_style() -> None:
    from dsql_migrator.ui.design import FILTER_SELECT_CLASSES, FILTER_SELECT_PROPS

    # Cloudscape filtering "Select": compact, outlined, white background.
    assert "outlined" in FILTER_SELECT_PROPS
    assert "dense" in FILTER_SELECT_PROPS
    assert "bg-white" in FILTER_SELECT_CLASSES


def test_filter_select_emits_label_options_and_props() -> None:
    from dsql_migrator.ui.design import filter_select

    ui = _RecordingUi()
    captured: list[object] = []
    select = filter_select(
        ui,
        label="Classification",
        options={"ALL": "All classifications", "AUTO": "Automatic"},
        value="ALL",
        on_change=lambda e: captured.append(e),
    )
    assert select is not None
    # The floating property label and the option labels were offered.
    assert "Classification" in ui.texts
    assert "All classifications" in ui.texts and "Automatic" in ui.texts
    # The shared AWS-style outlined/dense props + white bg were applied.
    props_blob = " ".join(ui.props)
    assert "outlined" in props_blob and "dense" in props_blob
    assert "bg-white" in " ".join(ui.classes)


def test_filter_bar_is_a_wrapping_row() -> None:
    from dsql_migrator.ui.design import filter_bar

    ui = _RecordingUi()
    bar = filter_bar(ui)
    assert bar is not None
    classes_blob = " ".join(ui.classes)
    assert "flex-wrap" in classes_blob and "items-center" in classes_blob


# ---------------------------------------------------------------------------
# definition_row
# ---------------------------------------------------------------------------


def test_definition_row_emits_bold_term_and_description() -> None:
    from dsql_migrator.ui.design import definition_row

    ui = _RecordingUi()
    desc = definition_row(ui, "Stream lag", "how far behind in time")
    assert desc is not None
    assert "Stream lag" in ui.texts
    assert "how far behind in time" in ui.texts
    # The term is rendered bold (semibold) so it stands out as the key.
    assert "font-semibold" in " ".join(ui.classes)


def test_definition_row_returns_container_for_rich_description() -> None:
    # With no description text, the caller fills the returned container (e.g. with
    # status badges) — it must be a usable context manager and stay empty of text.
    from dsql_migrator.ui.design import definition_row

    ui = _RecordingUi()
    desc = definition_row(ui, "Consistency")
    assert "Consistency" in ui.texts
    with desc:
        ui.badge("consistent")
    assert "consistent" in ui.texts


# ---------------------------------------------------------------------------
# Selectable object chips, colored by group (schema)
# ---------------------------------------------------------------------------


def test_chip_group_index_is_stable_and_in_range() -> None:
    # Deterministic across calls (not Python's salted hash) and within the palette.
    a = chip_group_index("customers_sample_new")
    assert a == chip_group_index("customers_sample_new")
    assert 0 <= a < len(CHIP_GROUP_PALETTE)


def test_chip_group_colors_differ_by_schema() -> None:
    # Different schema names should generally land on different palette entries.
    assert chip_group_text_class("schema_a") != chip_group_text_class("schema_b")


def test_schema_chip_classes_selected_vs_unselected() -> None:
    sel = schema_chip_classes("s1", selected=True)
    off = schema_chip_classes("s1", selected=False)
    # Always clickable.
    assert "cursor-pointer" in sel and "cursor-pointer" in off
    # Selected carries the group fill + a solid colored border; unselected is a
    # quiet gray outline.
    assert "bg-" in sel and "font-medium" in sel
    assert "border-gray-300" in off and "bg-white" in off


def test_chip_group_quasar_color_is_stable_and_aligned() -> None:
    from dsql_migrator.ui.design import (
        CHIP_GROUP_PALETTE,
        CHIP_GROUP_QUASAR_COLOR,
        chip_group_quasar_color,
        chip_group_index,
    )

    # One Quasar color per palette entry, same order (so a clickable q-btn chip
    # and a static label chip of the same schema read as the same color family).
    assert len(CHIP_GROUP_QUASAR_COLOR) == len(CHIP_GROUP_PALETTE)
    # Deterministic and index-aligned.
    assert chip_group_quasar_color("orders") == (
        CHIP_GROUP_QUASAR_COLOR[chip_group_index("orders")]
    )
    assert chip_group_quasar_color("x") == chip_group_quasar_color("x")


# ---------------------------------------------------------------------------
# radio_tiles (Cloudscape "Tiles")
# ---------------------------------------------------------------------------


def _tile_options():
    return (
        ("KEEP", "vpn_key", "Keep source PK", "Nothing in the application changes."),
        ("COMPOSITE", "shuffle", "Composite key", "Higher insert throughput."),
    )


def test_radio_tiles_render_label_description_and_radio_glyph() -> None:
    from dsql_migrator.ui.design import radio_tiles

    ui = _RecordingUi()
    radio_tiles(
        ui, _tile_options(), selected="KEEP", on_select=lambda _v: None
    )
    # Both options' labels + descriptions are emitted.
    assert "Keep source PK" in ui.texts
    assert "Composite key" in ui.texts
    assert "Nothing in the application changes." in ui.texts
    # The selected tile gets the filled radio, the other the empty one.
    assert "radio_button_checked" in ui.icons
    assert "radio_button_unchecked" in ui.icons
    # Each option's leading icon is rendered too.
    assert "vpn_key" in ui.icons and "shuffle" in ui.icons
    # One palette: selected tile is primary-bordered + tinted (design system).
    blob = " ".join(ui.classes)
    assert "border-blue-500" in blob and "bg-blue-50" in blob


def test_radio_tiles_report_the_clicked_value_including_the_current_one() -> None:
    # Re-selecting must still fire: a caller may treat clicking the already-selected
    # tile as CONFIRMING a default (the migration-type picker does exactly that).
    from dsql_migrator.ui.design import radio_tiles

    ui = _RecordingUi()
    picked: list[str] = []
    radio_tiles(
        ui, _tile_options(), selected="KEEP", on_select=picked.append
    )
    assert len(ui.clicks) == 2  # one handler per tile, in render order
    ui.clicks[0]()
    ui.clicks[1]()
    assert picked == ["KEEP", "COMPOSITE"]


def test_radio_tiles_locked_group_wires_no_handlers() -> None:
    # A locked group must be inert AND look inert -- not silently clickable.
    from dsql_migrator.ui.design import radio_tiles

    ui = _RecordingUi()
    radio_tiles(
        ui, _tile_options(), selected="KEEP", on_select=lambda _v: None, locked=True
    )
    assert ui.clicks == []
    blob = " ".join(ui.classes)
    assert "cursor-not-allowed" in blob and "opacity-60" in blob
    assert "cursor-pointer" not in blob


def test_radio_tiles_tolerate_missing_icon_and_description() -> None:
    from dsql_migrator.ui.design import radio_tiles

    ui = _RecordingUi()
    radio_tiles(
        ui, (("A", "", "Only a label", ""),), selected="A", on_select=lambda _v: None
    )
    assert "Only a label" in ui.texts
    # No leading icon beyond the radio glyph, and no description label.
    assert ui.icons == ["radio_button_checked"]
    assert ui.texts == ["Only a label"]


# ---------------------------------------------------------------------------
# Code surface + diff tokens (Cloudscape "CodeEditor")
# ---------------------------------------------------------------------------


def test_code_surface_is_neutral_not_tinted() -> None:
    # AWS renders code on a NEUTRAL surface and keeps semantic color to narrow
    # accents. A washed panel made a heterogeneous conversion (which rewrites nearly
    # every line) look like an error report.
    from dsql_migrator.ui.design import (
        CODE_HEADER_CLASSES,
        CODE_SURFACE_CLASSES,
        CODE_TEXT_CLASSES,
    )

    assert "bg-white" in CODE_SURFACE_CLASSES
    assert "border" in CODE_SURFACE_CLASSES and "rounded" in CODE_SURFACE_CLASSES
    # No semantic (red/green) fill anywhere on the reading surface.
    for token in (CODE_SURFACE_CLASSES, CODE_HEADER_CLASSES, CODE_TEXT_CLASSES):
        for hue in ("rose", "emerald", "red", "green"):
            assert hue not in token, f"{hue} must not tint the code surface: {token}"
    assert "font-mono" in CODE_TEXT_CLASSES


# ---------------------------------------------------------------------------
# Sidebar footer: one "Settings" entry whose modal groups the utilities in tabs.
# ---------------------------------------------------------------------------


def test_footer_is_one_settings_entry_not_three_inline_panels() -> None:
    """The three runtime utilities live behind ONE sidebar row.

    They used to be two inline ``ui.expansion`` panels plus a button: that put a
    nine-field form into the ~16rem sidebar column, and opening one panel shoved the
    others around. They are also all the same kind of thing -- app-wide runtime settings,
    none part of the migration flow -- so they belong behind a single entry point.
    """
    import inspect

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    # A single gear row, with a caption short enough not to wrap in the sidebar.
    assert 'ui.icon("settings"' in src
    assert 'ui.item_label("Settings")' in src
    # No inline expansions for these utilities any more.
    for module_fn in (
        app._render_tuning_group_controls,
        app._render_activity_log_controls,
    ):
        body = inspect.getsource(module_fn)
        assert 'ui.expansion(' not in body, module_fn.__name__


def test_settings_modal_gives_each_tuning_group_its_own_tab() -> None:
    """One tab per tuning GROUP, not one "Performance" tab holding all of them.

    "Performance" is not a category the operator thinks in: they arrive wanting to change
    the Full Load or the CDC sink. A combined panel made them read past the other groups,
    and each group's timing caption ("the next run" vs "the next Start CDC") sat mid-list
    where it read as a note on whichever field came next. The tabs are DERIVED from the
    registry, so a knob added in a new group grows the tab strip on its own.
    """
    import inspect

    from dsql_migrator.config import tunable_groups
    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    assert "ui.tabs(" in src and "ui.tab_panels(" in src
    # Group tabs come from the registry rather than a hard-coded list.
    assert "tunable_groups()" in src
    # No catch-all Performance TAB (the prose may still mention why it went away, so
    # match the ui.tab(...) call rather than the bare string).
    assert 'ui.tab("Performance"' not in src, "the catch-all Performance tab is gone"
    # ONE merged "Activity log" tab (the former separate "Diagnostics" tab is gone --
    # level + mirror + download are the same subject, so they share one tab + panel).
    assert 'ui.tab("Activity log"' in src
    assert 'ui.tab("Diagnostics"' not in src, "Diagnostics merged into Activity log"
    assert "_render_tuning_group_controls(name)" in src
    assert "_render_activity_log_controls(activity_log_path)" in src

    # The tab list must be DERIVED, not a literal that happens to match today's
    # registry: hard-coding it would silently drop a group's tab (its knobs would be
    # unreachable from the UI while still existing in config) -- so assert on the
    # assignment itself, and that no group name is spelled out as a literal.
    import ast

    tree = ast.parse(src.lstrip())
    assigns = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "groups" for t in node.targets
        )
    ]
    assert len(assigns) == 1, "expected exactly one `groups = ...` assignment"
    rhs = ast.unparse(assigns[0].value)
    assert "tunable_groups()" in rhs, f"tab list must come from the registry: {rhs}"
    literals = {
        node.value
        for node in ast.walk(assigns[0].value)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    registry_groups = {name for name, _knobs in tunable_groups()}
    assert not (literals & registry_groups), (
        f"group names must not be hard-coded in the tab list: {literals & registry_groups}"
    )


def test_settings_modal_panel_height_is_fixed_so_tabs_do_not_move() -> None:
    """Switching tabs must not resize the dialog.

    With a min/max range the panel took each tab's natural height (Full Load has three
    knobs, Validation one), so the card grew and shrank on every switch -- and because a
    centred dialog is positioned from its middle, the tab strip itself moved under the
    pointer, making the whole panel jump as you clicked through. A single fixed height
    anchors the strip so only the content changes.
    """
    import inspect
    import re

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    style = re.search(r'"(height:[^"]*)"', src)
    assert style is not None, "the panel container must set an explicit height"
    declared = style.group(1)
    # A FIXED height, not a floor that lets the panel grow with its content.
    assert re.search(r"(^|;\s*)height:\s*\d", declared), declared
    assert "min-height" not in declared, (
        f"a min-height lets the tallest tab stretch the dialog: {declared}"
    )
    # Still capped against a small viewport, and a panel that exceeds it scrolls
    # internally rather than resizing the card.
    assert "max-height: 68vh" in declared
    assert "overflow-y: auto" in declared
    # Persistent + an explicit close, so an outside click cannot lose a half-typed value.
    assert '.props("persistent")' in src
    assert 'icon="close"' in src


def test_tuning_panel_leads_with_its_own_apply_timing() -> None:
    """Each tab must state ITS timing, taken from the registry.

    The panel holds two kinds of knob: Full Load / Validation values are re-read by
    ``load_config()`` on the next run, while the CDC value is a cdc-stack CloudFormation
    parameter read at the next Start CDC. One blanket "applies to the next run" would be a
    false promise for CDC -- nothing re-reads it, and a sink already RUNNING keeps its
    capacity until the connector is updated. So the renderer must call ``group_applies``
    rather than hard-code a phrase, and the toast must repeat the knob's OWN timing.
    """
    import inspect

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_tuning_group_controls)
    assert "group_applies(group)" in src
    # The confirmation names the knob's own timing, not a hard-coded "next run".
    assert "{k.applies}" in src
    # Cloudscape FormField rows: label + description + constraint are all VISIBLE.
    assert "form_field(" in src
    assert "description=knob.description" in src
    # The description must not be hidden behind a hover-only glyph again.
    assert "tooltip(knob.description)" not in src
    # An enum-valued knob renders as a select (only legal values), not a spinner.
    assert "if knob.allowed" in src
    assert "ui.select(" in src and "ui.number(" in src


class _FormUi:
    """NiceGUI double for the Settings form: records text, selects, numbers, switches."""

    def __init__(self):
        self.texts: list[str] = []
        self.selects: list[tuple] = []
        self.numbers: list[dict] = []
        self.switches: list[dict] = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def style(self, *_a, **_k):
            return self

        def tooltip(self, text="", *_a, **_k):
            self._ui.texts.append(f"[tooltip]{text}")
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def select(self, options=None, value=None, label=None, **_k):
        self.selects.append((tuple(options or ()), value))
        # Quasar renders a select's `label` as a floating caption INSIDE the control, so
        # it is user-visible text and must be recorded -- otherwise a control that
        # duplicates its form_field row label looks identical to one that does not.
        if label:
            self.texts.append(str(label))
        return self._El(self)

    def number(self, **kw):
        self.numbers.append(kw)
        return self._El(self)

    def switch(self, *_a, **kw):
        self.switches.append(kw)
        return self._El(self)

    def icon(self, name="", *_a, **_k):
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def spinner(self, *_a, **_k):
        return self._El(self)

    def separator(self, *_a, **_k):
        return self._El(self)

    def badge(self, *_a, **_k):
        return self._El(self)

    def button(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def notify(self, *_a, **_k):
        return None


def _render_tuning_tab(group: str, ui):
    """Render one tuning tab through the REAL renderer with a NiceGUI double."""
    import sys
    import types

    from dsql_migrator.ui import app

    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    saved = sys.modules.get("nicegui")
    sys.modules["nicegui"] = fake
    try:
        app._render_tuning_group_controls(group)
    finally:
        if saved is None:
            sys.modules.pop("nicegui", None)
        else:
            sys.modules["nicegui"] = saved


def test_cdc_tab_shows_the_mcu_dropdown_and_its_own_timing() -> None:
    """End-to-end through the real renderer.

    Asserting on the registry alone would not catch a renderer that ignores a group --
    exactly how a "state exists but is never rendered" gap hides.
    """
    import os

    from dsql_migrator.config import ENV_PREFIX

    # Pin the value rather than inheriting the ambient environment: the form shows the
    # CURRENTLY effective value, so a stray env var would decide what this test sees.
    key = f"{ENV_PREFIX}CDC_SINK_MCU_COUNT"
    saved = os.environ.get(key)
    os.environ[key] = "4"
    try:
        ui = _FormUi()
        _render_tuning_tab("CDC", ui)
    finally:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved

    blob = " ".join(ui.texts)
    # Leads with the CDC timing -- never the Full Load one.
    assert "Changes apply to the next Start CDC." in blob
    assert "the next run" not in blob
    # The MCU knob is a select over exactly the CloudFormation AllowedValues.
    assert ((1, 2, 4, 8), 4) in ui.selects
    assert not ui.numbers, "an enum knob must not render as a free number field"
    # Label, description and the accepted values are all visible (not hover-only).
    assert "Sink compute (MCU)" in blob
    assert "CPU-bound" in blob
    assert "1 / 2 / 4 / 8" in blob
    # An info tooltip is allowed -- and here expected -- but ONLY as extra depth on top of
    # a visible description. The regression to guard is the old anti-pattern: guidance
    # available *only* on hover (unreachable on touch). So require that the visible text
    # already explains the field, and that the tooltip adds detail rather than repeating
    # the description as its sole home.
    tooltips = [t[len("[tooltip]") :] for t in ui.texts if t.startswith("[tooltip]")]
    assert len(tooltips) == 1, "the MCU field should carry exactly one info tooltip"
    help_text = tooltips[0]
    assert "When to raise it" in help_text
    # The three facts that do not fit a one-line description: the symptom, the cost, and
    # the fact a running pipeline needs Start CDC re-run.
    assert "lag" in help_text
    assert "bill" in help_text
    assert "Start CDC" in help_text
    # The tooltip is NOT just a copy of the visible description.
    assert help_text.strip() != next(
        t for t in ui.texts if "CPU-bound" in t and not t.startswith("[tooltip]")
    ).strip()
    # The connection-product notice belongs to Full Load only.
    assert "Connections" not in blob


def test_full_load_tab_shows_ranges_and_the_connection_product_warning() -> None:
    ui = _FormUi()
    _render_tuning_tab("Full Load", ui)
    blob = " ".join(ui.texts)

    assert "Changes apply to the next run." in blob
    # The one way to misconfigure this panel into a failing run is called out here.
    assert "Connections ≈ tables in parallel × batches per table" in blob
    # Range-valued knobs stay numeric inputs, with their bounds shown as constraints.
    # Four Full Load knobs: tables-in-parallel, batches-per-table, rows-per-batch, and
    # the opt-in source-load throttle (all range-valued -> numeric inputs, no selects).
    assert len(ui.numbers) == 4
    assert not ui.selects
    assert "1–3000" in blob  # rows per batch (DSQL's per-transaction cap)
    assert "Rows per batch" in blob
    # The source-load throttle is settable here (the only path on Fargate), with 0=off
    # and its 0–10000 range shown as the constraint.
    assert "Source-load throttle (max Threads_running)" in blob
    assert "0–10000" in blob


def test_validation_tab_is_rendered_without_the_full_load_notice() -> None:
    ui = _FormUi()
    _render_tuning_tab("Validation", ui)
    blob = " ".join(ui.texts)

    assert "Changes apply to the next run." in blob
    assert "Tables in parallel" in blob
    assert len(ui.numbers) == 1
    # Scoped notice: meaningless beside a single field, so it must not appear.
    assert "Connections" not in blob


def test_activity_log_tab_uses_the_same_form_field_rows() -> None:
    """The merged Activity-log tab's two runtime knobs share the tuning tabs' shape.

    Level + mirror route through ``form_field`` so they read as one form (same
    label/description structure as every tuning tab). The tab also carries the
    download action (the former separate "Diagnostics"/"Activity log" split is gone),
    and the mirror control is named by its DESTINATION ("CloudWatch Logs"), not the
    opaque "stdout" mechanism.
    """
    import sys
    import types

    from dsql_migrator.ui import app

    ui = _FormUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    saved = sys.modules.get("nicegui")
    sys.modules["nicegui"] = fake
    try:
        app._render_activity_log_controls("/tmp/does-not-matter.ndjson")
    finally:
        if saved is None:
            sys.modules.pop("nicegui", None)
        else:
            sys.modules["nicegui"] = saved

    blob = " ".join(ui.texts)
    assert "Level and mirroring changes apply immediately." in blob
    # Renamed control: destination, not "stdout".
    assert "Log level" in blob and "Mirror to CloudWatch Logs" in blob
    assert "Mirror to stdout" not in blob, "the opaque 'stdout' label must be gone"
    assert ui.selects and ui.selects[0][0] == ("DEBUG", "INFO", "WARNING", "ERROR")
    assert ui.switches
    assert "CloudWatch" in blob
    # Merged in: the download action lives on this same tab now.
    assert "Download activity log" in ui.texts
    # The form_field row supplies the label, so the control must NOT carry a floating
    # one too -- Quasar would render "Log level" twice, stacked, in the same row.
    assert ui.texts.count("Log level") == 1, (
        f"the label is duplicated by the control: {ui.texts}"
    )




# ---------------------------------------------------------------------------
# form_field (Cloudscape "FormField")
# ---------------------------------------------------------------------------


def test_form_field_shows_label_description_and_constraint_visibly() -> None:
    """Cloudscape pairs every control with a visible label, description and constraint.

    The app previously hid the description behind a hover-only info glyph to keep each
    knob on one line, which made the form unreadable at a glance -- you had to hover each
    field in turn to learn what it did, and hover text is unavailable on touch.
    """
    from dsql_migrator.ui.design import form_field

    ui = _RecordingUi()
    slot = form_field(
        ui,
        label="Rows per batch",
        description="Rows per INSERT batch.",
        constraint="1–3000",
    )
    assert ui.texts == ["Rows per batch", "Rows per INSERT batch.", "1–3000"]
    # The returned value is a container the caller puts the control into.
    assert hasattr(slot, "__enter__")
    blob = " ".join(ui.classes)
    # The label is the prominent one; description/constraint are quieter.
    assert "text-sm font-medium text-gray-900" in blob
    assert "text-xs text-gray-500" in blob
    # Constraint text is monospace so accepted values read as data.
    assert "font-mono" in blob


def test_form_field_omits_the_second_line_when_there_is_nothing_to_say() -> None:
    from dsql_migrator.ui.design import form_field

    ui = _RecordingUi()
    form_field(ui, label="Just a label")
    assert ui.texts == ["Just a label"]


def test_form_field_control_width_is_overridable_and_right_aligned() -> None:
    """Controls right-align so a column of inputs lines up down the panel."""
    from dsql_migrator.ui.design import form_field

    ui = _RecordingUi()
    form_field(ui, label="Wide", control_width="w-40")
    blob = " ".join(ui.classes)
    assert "w-40" in blob
    assert "justify-end" in blob


def test_settings_tab_order_follows_the_migration_journey() -> None:
    """Tabs follow Full Load -> CDC -> Validation, the order the work happens in.

    The tab strip is derived from TUNABLE_KNOBS' group order, so this is a property of the
    registry, not of the render loop. CDC pairs with Full Load (both are data-movement
    throughput); Validation is the after-the-fact check and comes last. Previously CDC sat
    after Validation purely because that knob was added later.
    """
    from dsql_migrator.config import tunable_groups

    assert [name for name, _knobs in tunable_groups()] == [
        "Full Load",
        "CDC",
        "Validation",
    ]


def test_form_field_info_tooltip_supplements_a_visible_description() -> None:
    """The info glyph is EXTRA depth, never the only home for the guidance.

    The regression it must not reintroduce: guidance available only on hover, which is
    unreadable at a glance and unreachable on touch. So the glyph is additive -- the
    label, description and constraint all still render as visible text.
    """
    from dsql_migrator.ui.design import form_field

    ui = _RecordingUi()
    form_field(
        ui,
        label="Sink compute (MCU)",
        description="Visible one-liner.",
        constraint="1 / 2 / 4 / 8",
        help_text="The long version.",
    )
    # Everything visible is still emitted as text -- the tooltip is NOT among it.
    assert ui.texts == ["Sink compute (MCU)", "Visible one-liner.", "1 / 2 / 4 / 8"]
    # Plus an info glyph carrying the deeper guidance on hover.
    assert "info_outline" in ui.icons
    assert ui.tooltips == ["The long version."]
    blob = " ".join(ui.classes)
    assert "cursor-help" in blob

    # Without help_text there is no glyph at all (no empty tooltip target).
    plain = _RecordingUi()
    form_field(plain, label="Rows per batch", description="Visible.")
    assert "info_outline" not in plain.icons
    assert plain.tooltips == []


def test_activity_log_tab_presents_its_action_at_full_size() -> None:
    """This tab is an ACTION, so the button is NOT wedged into a form-field slot.

    Routing it through ``form_field`` (as an earlier pass did) put the button in the
    right-hand control slot, which is sized for a number input and right-aligned so a
    COLUMN of inputs lines up -- meaningless for a single button. The result was a small
    button stranded far from the text it belongs to, with the description wrapping beneath
    it. It now reads as a described section with the action below it at full size.
    """
    import sys
    import types

    from dsql_migrator.ui import app

    ui = _FormUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    saved = sys.modules.get("nicegui")
    sys.modules["nicegui"] = fake
    try:
        app._render_activity_log_controls("/tmp/does-not-matter.ndjson")
    finally:
        if saved is None:
            sys.modules.pop("nicegui", None)
        else:
            sys.modules["nicegui"] = saved

    blob = " ".join(ui.texts)
    # The download's own timing/what note (now folded into the merged tab).
    assert "Downloads the log as it stands right now." in blob
    assert "One UTC line per event" in blob
    # The action NAMES what it downloads, matching the Full Load error-log button
    # ("Download" alone left the verb to be paired with a heading by eye).
    assert "Download activity log" in ui.texts
    # The ephemeral-storage caveat is available without leaving the dialog.
    tooltips = [t for t in ui.texts if t.startswith("[tooltip]")]
    assert any("CloudWatch" in t for t in tooltips)

    import inspect
    import re

    src = inspect.getsource(app._render_activity_log_controls)
    # The DOWNLOAD button must not be wedged into a form_field control slot (that would
    # shrink and right-align it). The merged tab DOES use form_field for its two runtime
    # knobs (level + mirror), so assert on the button call itself, not the whole function:
    # the button is created directly under the column, never inside a `with form_field(...)`.
    button_call = re.search(
        r'ui\.button\(\s*"Download activity log".*?\.props\(\s*"([^"]*)"', src, re.S
    )
    assert button_call is not None, "expected the download button in the merged tab"
    # Full-size primary action -- not shrunk by dense/size=sm, since nothing competes.
    button_props = button_call
    assert button_props is not None, "expected a props() call on the download button"
    props = button_props.group(1)
    assert "color=primary" in props
    assert "dense" not in props and "size=sm" not in props, (
        f"the tab's only action should not be styled down: {props}"
    )


def test_settings_footer_warns_that_values_do_not_persist() -> None:
    """The non-persistence caveat must be a notice, and sit at the modal FOOTER.

    It prevents a real mistake: an operator who tunes here and walks away assumes the
    value sticks, but any restart -- including a Fargate task replacement they did not
    initiate -- silently reverts it to the deploy-time default, so a carefully tuned run
    behaves differently next time with no sign why. It sits below the tabs as a closing
    note on the whole panel (a caption under the title read as boilerplate; a banner above
    the tabs competed with the title). It also must NOT claim "changes apply to the next
    run", which is true only of the Full Load / Validation groups (each panel states its
    own timing).
    """
    import inspect

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    # Rendered through the shared notice component (box + border + icon carry the weight).
    assert "render_notice(" in src
    assert '"These settings are not permanent"' in src
    # Leads with the consequence and names the durable alternative.
    assert "revert to the" in src and "restarts" in src
    assert "DSQL_MIGRATOR_*" in src
    # A FOOTER below the tabs, not a header banner: the notice is rendered after the
    # tab panels are built, so it reads as a closing note on the whole panel.
    assert src.index("ui.tab_panels(") < src.index('"These settings are not permanent"')
    # The dialog-wide line must not assert a per-group timing. Check the STRING LITERALS
    # only -- the prose above explains why that wording was dropped, and matching the
    # whole source would flag the explanation as the thing it warns against.
    import ast

    literals = " ".join(
        node.value
        for node in ast.walk(ast.parse(src.lstrip()))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ).lower()
    assert "changes apply to the next run" not in literals
