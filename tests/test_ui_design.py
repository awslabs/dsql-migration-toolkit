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
    BADGE_TONES,
    CHIP_GROUP_PALETTE,
    INLINE_HINT_TEXT,
    NOTICE_STYLE,
    SEGMENTED_TOGGLE_CLASSES,
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


def test_diff_side_style_marks_change_without_relying_on_color() -> None:
    # Color must never be the ONLY signal: each changed side carries a +/- glyph, so
    # the diff survives a monochrome screenshot and is legible to a colorblind reader.
    from dsql_migrator.ui.design import DIFF_SIDE_STYLE

    assert set(DIFF_SIDE_STYLE) == {"unchanged", "removed", "added"}
    removed_mark, removed_class, removed_tint = DIFF_SIDE_STYLE["removed"]
    added_mark, added_class, added_tint = DIFF_SIDE_STYLE["added"]
    assert removed_mark and added_mark and removed_mark != added_mark
    assert "rose" in removed_class and "emerald" in added_class
    # An unchanged row is completely unmarked, so the eye lands only on differences.
    mark, _cls, tint = DIFF_SIDE_STYLE["unchanged"]
    assert mark == "" and tint == ""


def test_diff_row_tints_are_barely_there() -> None:
    # The wash groups a changed row without competing with the text: a *-50 shade at
    # reduced alpha, never a solid *-100 fill (which is what looked unprofessional).
    from dsql_migrator.ui.design import DIFF_SIDE_STYLE

    for role in ("removed", "added"):
        _mark, _cls, tint = DIFF_SIDE_STYLE[role]
        assert "-50/" in tint, f"{role} tint should be a -50 shade with alpha: {tint}"
        assert "-100" not in tint and "-200" not in tint


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
        app._render_performance_tuning_controls,
        app._render_diagnostics_controls,
    ):
        body = inspect.getsource(module_fn)
        assert 'ui.expansion(' not in body, module_fn.__name__


def test_settings_modal_groups_the_categories_in_tabs() -> None:
    # Tabs, not stacked sections: you come here to change ONE category, so stacking made
    # the reader scroll past two groups to reach the third.
    import inspect

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    assert "ui.tabs(" in src and "ui.tab_panels(" in src
    for label in ('"Performance"', '"Diagnostics"', '"Activity log"'):
        assert f"ui.tab({label}" in src, label
    # Each utility renders into its own panel.
    assert "_render_performance_tuning_controls()" in src
    assert "_render_diagnostics_controls()" in src
    assert "_render_activity_log_download(activity_log_path)" in src


def test_settings_modal_panels_are_bounded_but_not_padded() -> None:
    """A short panel must not be padded out to a tall one's height.

    A 22rem floor gave Diagnostics (two controls) a screen of empty space; the cap is
    what keeps a long panel scrolling instead of pushing the dialog off-viewport.
    """
    import inspect

    from dsql_migrator.ui import app

    src = inspect.getsource(app._render_footer_tools)
    assert "min-height: 9rem" in src
    assert "max-height: 68vh" in src
    assert "overflow-y: auto" in src
    # Persistent + an explicit close, so an outside click cannot lose a half-typed value.
    assert '.props("persistent")' in src
    assert 'icon="close"' in src
