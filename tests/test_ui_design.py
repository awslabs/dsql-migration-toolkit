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

    def toggle(self, options=None, *_a, value=None, on_change=None, **_k):
        # Record the option labels so a test can assert what the segmented
        # control offered; returns a chainable element double.
        if isinstance(options, dict):
            self.texts.extend(str(v) for v in options.values())
        elif isinstance(options, (list, tuple)):
            self.texts.extend(str(v) for v in options)
        return _El(self, "toggle")


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
