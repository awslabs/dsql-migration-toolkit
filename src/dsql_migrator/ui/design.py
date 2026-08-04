# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AWS Console (Cloudscape)-inspired design system for the DSQL migrator UI.

Single source of truth for the app's visual language so every page reads like one
AWS-service experience instead of diverging per screen. The real AWS Console uses
the open-source `Cloudscape Design System <https://cloudscape.design/>`_; NiceGUI is
Quasar-based so Cloudscape cannot be used directly, but this module borrows its
*semantics* (Alert, StatusIndicator, Container header) and *tokens* (a fixed,
Tailwind-only severity palette) and maps them onto NiceGUI elements.

This module is a **leaf**: it imports nothing from the rest of the app, so any UI
module can import it without risking a circular import. The ``ui`` object (the
NiceGUI module or a test double) is always passed in explicitly rather than
imported, keeping these helpers unit-testable without a running server.

Design rules:

- **Severity = tone, never ad-hoc color.** Use :func:`render_notice` with a tone
  from :data:`NOTICE_STYLE` (``info`` / ``success`` / ``warning`` / ``error``)
  rather than loose colored text. The tinted box + border + leading status icon +
  bold header carry the severity; body text stays dark and readable so a notice
  reads as awareness, not alarm.
- **One palette.** Backgrounds use the Tailwind ``*-50`` shades, borders ``*-200``,
  icons ``*-600``. Do not mix Quasar numeric shades (``bg-blue-1``) with Tailwind
  shades, and prefer ``amber`` over ``orange`` for the warning tone.
- **Tone meanings** mirror Cloudscape: ``info`` = neutral FYI (blue/sky),
  ``success`` = completed OK (green), ``warning`` = be aware / non-blocking issue
  (amber), ``error`` = action required / blocking (red).
- **Busy buttons: never the Quasar ``loading`` prop.** Its spinner overlays the
  button border and reads as a "spinning border" artifact (worst on
  ``outline``/``flat`` buttons). Indicate an in-progress action by *disabling* the
  button (optionally with a "…"/"Stopping…" label, or a separate ``ui.spinner``
  beside it) -- not with ``button.props("loading")``.
"""

from __future__ import annotations

from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Alert / notice tones (Cloudscape "Alert")
# ---------------------------------------------------------------------------

# Tone -> (background, border, icon-color, default-icon). The box, border and icon
# carry the severity; body text stays dark for readability. Keep this the ONLY
# place severity colors are defined so the whole app shares one palette.
NOTICE_STYLE: dict[str, Tuple[str, str, str, str]] = {
    "info": ("bg-sky-50", "border-sky-200", "text-sky-600", "info"),
    "success": ("bg-green-50", "border-green-200", "text-green-600", "check_circle"),
    "warning": ("bg-amber-50", "border-amber-200", "text-amber-600", "warning"),
    "error": ("bg-red-50", "border-red-200", "text-red-600", "error"),
}

# Quasar color name per tone, for the animated spinner a ``busy`` notice shows in
# place of its static glyph. ``ui.spinner`` takes a Quasar color, not the Tailwind
# text class in NOTICE_STYLE, so the two cannot share one value.
_QUASAR_SPINNER_COLOR: dict[str, str] = {
    "info": "primary",
    "success": "positive",
    "warning": "warning",
    "error": "negative",
}


def render_notice(
    ui,
    *,
    tone: str,
    header: str,
    body: str = "",
    icon: str = "",
    busy: bool = False,
):
    """Render an AWS Console (Cloudscape "Alert")-style notice box.

    A tinted, rounded, bordered box with a leading status icon, a bold header
    label (the "box label"), and readable body text -- the calm awareness
    treatment AWS uses instead of loose red text. ``tone`` selects the palette
    from :data:`NOTICE_STYLE` (unknown tones fall back to ``info``); ``icon``
    overrides the tone's default Material icon. ``body`` is optional so the box
    can be a single bold line.

    ``busy`` marks the notice as reporting a **live, still-running** operation: the
    leading glyph becomes an animated spinner and an "In progress" badge is pinned to
    the right of the header. A static icon cannot distinguish "this is happening right
    now" from "here is a fact", so a long background operation (a CDC teardown runs
    ~15-45 min) read as an inert message the user could not tell was still moving.

    Returns ``(header_label, body_label)`` -- the body is ``None`` when ``body`` was
    empty -- so a POLLED region can update the wording in place via ``set_text``
    instead of re-rendering. That matters because re-rendering destroys any element the
    pointer is over, which closes a hovered tooltip; at a sub-second poll interval the
    tooltip flickers and cannot be read. Callers that just draw a static notice can
    keep ignoring the return value.
    """
    bg, border, icon_color, default_icon = NOTICE_STYLE.get(tone, NOTICE_STYLE["info"])
    with ui.row().classes(
        f"items-start gap-2 no-wrap w-full rounded-md border {border} {bg} p-3"
    ):
        if busy:
            # Quasar's spinner takes a Quasar color name, while the tone palette is a
            # Tailwind text class ("text-sky-600"); map it to the closest Quasar color
            # so the spinner still reads as part of the tone.
            ui.spinner(size="sm", color=_QUASAR_SPINNER_COLOR.get(tone, "primary"))
        else:
            ui.icon(icon or default_icon).classes(f"{icon_color} text-lg")
        with ui.column().classes("gap-0 flex-1 min-w-0"):
            if busy:
                with ui.row().classes("items-center gap-2 no-wrap w-full"):
                    header_label = ui.label(header).classes(
                        "text-sm font-semibold text-gray-900"
                    )
                    ui.badge("In progress", color="primary").props("outline")
            else:
                header_label = ui.label(header).classes(
                    "text-sm font-semibold text-gray-900"
                )
            body_label = (
                ui.label(body).classes("text-xs text-gray-700") if body else None
            )
    return header_label, body_label


# ---------------------------------------------------------------------------
# Inline hints (one-line status text, too light for a full notice box)
# ---------------------------------------------------------------------------

# Tone -> Tailwind text color for a one-line inline hint (field validation, a
# disabled-button reason, a small caption). A full notice box would be too heavy
# for these, but the COLOR must still come from one place so a warning is always
# amber (never orange), an error always the same red, etc. "neutral" is the calm
# gray for non-status guidance.
INLINE_HINT_TEXT: dict[str, str] = {
    "info": "text-sky-700",
    "success": "text-green-700",
    "warning": "text-amber-700",
    "error": "text-red-700",
    "neutral": "text-gray-500",
}


def inline_hint(ui, text: str, *, tone: str = "neutral", classes: str = "text-xs"):
    """Render a one-line status hint with the palette color for ``tone``.

    The lightweight counterpart to :func:`render_notice`: for short messages that
    sit under an input or beside a button, where a bordered box would be visually
    too heavy. Routing them through here keeps the severity color consistent with
    the notices (amber warnings, one red, one sky info) instead of ad-hoc shades.
    ``classes`` tunes size/spacing (default ``text-xs``); the color is appended.
    Returns the created label so the caller can chain (e.g. ``.tooltip(...)``).
    """
    color = INLINE_HINT_TEXT.get(tone, INLINE_HINT_TEXT["neutral"])
    return ui.label(text).classes(f"{classes} {color}")


# ---------------------------------------------------------------------------
# Status chips / badges (Cloudscape "StatusIndicator")
# ---------------------------------------------------------------------------

# Small inline status chip styles, keyed by semantic tone, for the migration
# diagram nodes and similar at-a-glance indicators. Border + text color only
# (no fill) except ``reconnect``, which is tinted to draw the eye to a resumable
# session. Used by the workflow diagram.
BADGE_TONES: dict[str, str] = {
    "ok": "border-green-500 text-green-700",
    "bad": "border-red-400 text-red-600",
    "active": "border-blue-500 text-blue-700",
    "neutral": "border-gray-300 text-gray-500",
    "reconnect": "border-amber-400 text-amber-700 bg-amber-50",
}


def badge_classes(tone: str) -> str:
    """Return the Tailwind classes for a status chip of ``tone`` (info fallback)."""
    return BADGE_TONES.get(tone, BADGE_TONES["neutral"])


# ---------------------------------------------------------------------------
# Status indicator dot (Cloudscape "StatusIndicator" — dot + text)
# ---------------------------------------------------------------------------

# A lightweight status treatment for tight spaces: a small filled circle + colored
# text, no border or background fill. Reads faster than a bordered chip and uses
# less visual weight. Keyed by the same semantic tones as BADGE_TONES so the two
# can coexist when migrating progressively.
STATUS_DOT_TONES: dict[str, Tuple[str, str]] = {
    "ok": ("bg-green-500", "text-green-700"),
    "bad": ("bg-red-500", "text-red-600"),
    "active": ("bg-blue-500", "text-blue-700"),
    "neutral": ("bg-gray-400", "text-gray-500"),
    "reconnect": ("bg-amber-500", "text-amber-700"),
}


def render_status_dot(ui, text: str, *, tone: str = "neutral") -> None:
    """Render a Cloudscape-style StatusIndicator: colored dot + colored text.

    Lighter than the bordered badge chips — uses only a small filled circle
    (``h-2 w-2 rounded-full``) and colored text. For tight inline contexts
    where a bordered pill would be too heavy (e.g. the migration overview
    diagram segments). ``tone`` selects from :data:`STATUS_DOT_TONES`;
    unknown tones fall back to ``neutral``.
    """
    dot_bg, text_color = STATUS_DOT_TONES.get(tone, STATUS_DOT_TONES["neutral"])
    with ui.row().classes("items-center gap-1.5 no-wrap"):
        ui.element("div").classes(f"h-2 w-2 rounded-full shrink-0 {dot_bg}")
        ui.label(text).classes(f"text-[11px] leading-tight {text_color}")


# ---------------------------------------------------------------------------
# Selectable object chips, colored by group (e.g. schema)
# ---------------------------------------------------------------------------

# A small categorical palette for grouping selectable object chips by a key
# (e.g. database schema). Each entry is the (border, text, light-fill) Tailwind
# trio for the SELECTED state plus an accent the unselected state borrows for its
# text, so chips from the same group read as one color family whether on or off.
# AWS/Cloudscape stays in one palette family (no Quasar numeric shades); colors
# are picked to be distinguishable and accessible on white. Assigned to a group
# by stable hashing in :func:`schema_chip_classes` so the same schema always maps
# to the same color across renders.
CHIP_GROUP_PALETTE: tuple[tuple[str, str, str], ...] = (
    ("border-sky-500", "text-sky-700", "bg-sky-50"),
    ("border-violet-500", "text-violet-700", "bg-violet-50"),
    ("border-teal-500", "text-teal-700", "bg-teal-50"),
    ("border-amber-500", "text-amber-700", "bg-amber-50"),
    ("border-rose-500", "text-rose-700", "bg-rose-50"),
    ("border-indigo-500", "text-indigo-700", "bg-indigo-50"),
    ("border-emerald-500", "text-emerald-700", "bg-emerald-50"),
    ("border-fuchsia-500", "text-fuchsia-700", "bg-fuchsia-50"),
)


def chip_group_index(group: str) -> int:
    """Return a stable palette index for ``group`` (deterministic across renders).

    Uses a simple stable string hash (Python's ``hash`` is salted per-process, so
    it is NOT used) so a given schema name always maps to the same color in a
    session and across restarts.
    """
    total = 0
    for ch in group:
        total = (total * 31 + ord(ch)) & 0xFFFFFFFF
    return total % len(CHIP_GROUP_PALETTE)


def chip_group_text_class(group: str) -> str:
    """Return just the Tailwind TEXT-color class for ``group`` (for a heading)."""
    _border, text, _fill = CHIP_GROUP_PALETTE[chip_group_index(group)]
    return text


# Quasar (q-btn) ``color`` names aligned 1:1 with CHIP_GROUP_PALETTE, in the same
# order. A Quasar button colors itself via its ``color`` prop (filled or
# ``outline``); Tailwind bg/text utility classes do NOT reliably override q-btn's
# own styling, so a CLICKABLE chip rendered as a button must take its color from
# here, not from :func:`schema_chip_classes` (which is for static label chips).
CHIP_GROUP_QUASAR_COLOR: tuple[str, ...] = (
    "light-blue-7",  # sky
    "deep-purple-6",  # violet
    "teal-7",  # teal
    "amber-8",  # amber
    "pink-6",  # rose
    "indigo-6",  # indigo
    "green-7",  # emerald
    "purple-6",  # fuchsia
)


def chip_group_quasar_color(group: str) -> str:
    """Return the Quasar ``color`` name for ``group`` (for a clickable q-btn chip)."""
    return CHIP_GROUP_QUASAR_COLOR[chip_group_index(group)]


def schema_chip_classes(group: str, *, selected: bool) -> str:
    """Tailwind classes for a selectable object chip colored by its ``group``.

    Selected chips are filled in their group's light tint with a solid border and
    colored text (a clear "on" state); unselected chips are a quiet gray outline
    that only borrows the group's text accent, so the grouping is legible without
    shouting. The chip is always clickable (the caller wires the toggle).
    """
    border, text, fill = CHIP_GROUP_PALETTE[chip_group_index(group)]
    base = "cursor-pointer select-none transition-colors"
    if selected:
        return f"{base} {border} {text} {fill} font-medium"
    return f"{base} border-gray-300 {text} bg-white hover:bg-gray-50"


# ---------------------------------------------------------------------------
# Segmented control (Cloudscape "SegmentedControl")
# ---------------------------------------------------------------------------

# A Cloudscape "segmented control" is a single bordered group of segments where
# the selected segment is filled with the primary color and the rest stay quiet
# (white background, dark text). NiceGUI is Quasar-based, so this maps onto a
# ``q-btn-toggle`` (``ui.toggle``): keeping the props/classes here is the single
# source of truth so every in-app filter/segmented picker reads the same instead
# of each screen styling a toggle ad hoc. Props give the selected/unselected
# coloring; the classes draw the surrounding border and clip the segments.
SEGMENTED_TOGGLE_PROPS = (
    "no-caps unelevated dense toggle-color=primary color=white text-color=grey-8"
)
SEGMENTED_TOGGLE_CLASSES = "rounded-md border border-gray-300 overflow-hidden"


# Border/shape for a collapsible ``ui.expansion`` so it reads as its own panel like
# the bordered ``ui.card`` sections around it. A bare expansion draws no border, so
# collapsible items (connector config, deploy log, Delete CDC infrastructure, ...) sat
# as borderless headers that looked unstyled next to every carded section. Apply this
# (usually with ``w-full``) to keep every collapsible panel matching the cards. Kept
# here as the single source of truth so the shade cannot drift per screen.
EXPANSION_PANEL_CLASSES = "rounded-md border border-gray-200"


def segmented_control(
    ui,
    options,
    *,
    value,
    on_change,
    extra_props: str = "",
    classes: str = "",
):
    """Render an AWS Console (Cloudscape "SegmentedControl")-style picker.

    Wraps NiceGUI's ``ui.toggle`` with the shared :data:`SEGMENTED_TOGGLE_PROPS`
    and :data:`SEGMENTED_TOGGLE_CLASSES` so every segmented filter in the app
    looks the same: a bordered group of segments with the selected one filled in
    the primary color. ``options`` is the usual ``ui.toggle`` mapping (value ->
    label) or list; ``value``/``on_change`` are forwarded unchanged.
    ``extra_props``/``classes`` append caller tweaks. Returns the created toggle
    so the caller can further chain (e.g. ``.tooltip(...)``).
    """
    toggle = ui.toggle(options, value=value, on_change=on_change)
    toggle.props(f"{SEGMENTED_TOGGLE_PROPS} {extra_props}".strip())
    toggle.classes(f"{SEGMENTED_TOGGLE_CLASSES} {classes}".strip())
    return toggle


# ---------------------------------------------------------------------------
# Code surface + diff (Cloudscape "CodeEditor"-style)
# ---------------------------------------------------------------------------

# AWS Console renders code on a NEUTRAL surface and keeps semantic color to narrow
# accents -- a status gutter, a border, a badge -- never a wash across the whole
# reading area. A heterogeneous MySQL->DSQL conversion rewrites nearly every line, so
# tinting each changed row filled the entire panel red/green: it read as an error
# report, made the monospace text harder to read, and looked amateurish next to real
# console surfaces. These tokens are the single source of truth for that treatment.
CODE_SURFACE_CLASSES = "bg-white border border-slate-200 rounded-lg overflow-hidden"
CODE_HEADER_CLASSES = "bg-slate-50 border-b border-slate-200"
CODE_HEADER_LABEL_CLASSES = (
    "text-xs font-semibold tracking-wide text-slate-600 uppercase"
)
CODE_TEXT_CLASSES = "font-mono text-xs leading-relaxed text-slate-800"



# ---------------------------------------------------------------------------
# Radio tiles (Cloudscape "Tiles")
# ---------------------------------------------------------------------------


def radio_tiles(
    ui,
    options,
    *,
    selected,
    on_select,
    locked: bool = False,
    compact: bool = False,
) -> None:
    """Render a Cloudscape "Tiles" group: bordered radio cards, one per choice.

    AWS uses tiles (not a segmented control) when the choice is a *decision with
    consequences* and each option needs a sentence of explanation -- the segmented
    control is for switching views. This is the single source of truth for that look:
    a bordered card per option, primary border + tint on the selected one, a
    radio glyph, an optional leading icon, a bold label, and an optional description.

    ``options`` is a sequence of ``(value, icon, label, description)`` tuples;
    ``icon`` and ``description`` may be empty. ``selected`` is the currently chosen
    value (compared by equality). ``on_select`` is called with the clicked value --
    including when it is already selected, so a caller can treat re-selecting as
    confirming a default. ``locked`` mutes the group and drops the click handlers.
    ``compact`` drops the min-height and tightens padding for a small inline group.
    """
    with ui.row().classes("w-full gap-3 items-stretch no-wrap"):
        for value, icon, label, description in options:
            is_selected = value == selected
            border = "border-blue-500" if is_selected else "border-gray-300"
            bg = "bg-blue-50" if is_selected else "bg-white"
            interactivity = (
                "opacity-60 cursor-not-allowed"
                if locked
                else "cursor-pointer hover:border-blue-400"
            )
            padding = "p-2" if compact else "p-3"
            tile = ui.card().classes(
                f"flex-1 {padding} rounded-lg border {border} {bg} {interactivity} "
                "transition-colors gap-1"
            )
            if not locked:
                tile.on("click", lambda _e=None, _v=value: on_select(_v))
            with tile:
                with ui.row().classes("items-center gap-2 no-wrap"):
                    ui.icon(
                        "radio_button_checked"
                        if is_selected
                        else "radio_button_unchecked",
                        color="primary" if is_selected else "grey-6",
                    ).classes("text-lg")
                    if icon:
                        ui.icon(
                            icon, color="primary" if is_selected else "grey-7"
                        ).classes("text-lg")
                    ui.label(label).classes("text-sm font-semibold")
                if description:
                    ui.label(description).classes("text-xs text-gray-600")


# ---------------------------------------------------------------------------
# Filter dropdown (Cloudscape collection "filtering" Select)
# ---------------------------------------------------------------------------

# AWS Console list views filter a collection with compact, outlined "Select"
# dropdowns (one per property, e.g. an EC2 instance-state filter) sitting in a
# filter bar above the list -- not a segmented control (which suits 2-4 mutually
# exclusive views, not a property with several values that combine). This is the
# single source of truth for that look: a dense, outlined, white-background
# dropdown with a floating property label. Use ``filter_bar`` to wrap several.
FILTER_SELECT_PROPS = "outlined dense options-dense"
FILTER_SELECT_CLASSES = "bg-white text-sm min-w-[13rem]"


def filter_bar(ui, *, classes: str = ""):
    """Return a row container for a group of :func:`filter_select` dropdowns.

    A left-aligned, wrapping row with consistent gaps -- the Cloudscape "filtering"
    bar that sits above a filtered collection. Use as a context manager::

        with filter_bar(ui):
            filter_select(ui, label="Classification", ...)
            filter_select(ui, label="Estimated manual effort", ...)
    """
    return ui.row().classes(
        f"items-center gap-3 flex-wrap {classes}".strip()
    )


def filter_select(ui, *, label, options, value, on_change, classes: str = ""):
    """Render an AWS Console (Cloudscape filtering "Select")-style filter dropdown.

    ``options`` is a ``{value: display_label}`` mapping (as ``ui.select`` takes);
    ``label`` is the property name shown as the floating caption; ``value`` /
    ``on_change`` are forwarded unchanged. Reusing this keeps every list filter in
    the app visually identical instead of styling a ``ui.select`` ad hoc. Returns
    the created select so the caller can chain (e.g. ``.tooltip(...)``).
    """
    select = ui.select(options, value=value, label=label, on_change=on_change)
    select.props(FILTER_SELECT_PROPS)
    select.classes(f"{FILTER_SELECT_CLASSES} {classes}".strip())
    return select


# ---------------------------------------------------------------------------
# Container header (Cloudscape "Container"/"Header")
# ---------------------------------------------------------------------------


def section_header(ui, *, icon: str, title: str, badge: Optional[Tuple[str, str]] = None):
    """Render an AWS-console-style card section header: icon + title + badge.

    A leading service glyph (in the primary color) and a bold title on the left,
    with an optional status badge pushed to the right -- the consistent header
    band Cloudscape puts atop a "Container". ``badge`` is an optional
    ``(label, quasar_color)`` tuple; when given, the created ``ui.badge`` is
    returned so the caller can update it later. Returns ``None`` when no badge.
    """
    with ui.row().classes("items-center gap-2 w-full no-wrap"):
        ui.icon(icon, color="primary").classes("text-2xl")
        ui.label(title).classes("text-lg font-semibold")
        ui.space()
        if badge is not None:
            label, color = badge
            return ui.badge(label).props(f"color={color} outline")
    return None


# ---------------------------------------------------------------------------
# Definition row (Cloudscape "key-value pairs" / definition list)
# ---------------------------------------------------------------------------


def definition_row(ui, term, description: str = "", *, term_width: str = "min-w-[10rem]"):
    """Render one AWS-style definition row: a bold term beside its description.

    A scannable alternative to a bullet list for a legend/glossary: the bold
    ``term`` (e.g. a table column name) sits on the left at a fixed min-width so
    terms align into a column, and the description flows to its right. Returns the
    **description container** so the caller can append rich content — e.g. status
    badges that match a table's cells — via ``with``::

        desc = definition_row(ui, "Consistency")
        with desc:
            ui.badge("consistent").props("color=positive outline")

    When ``description`` text is given it is rendered as a plain label inside that
    container. Reusing this keeps every in-app legend visually identical.
    """
    with ui.row().classes("items-start gap-2 w-full no-wrap"):
        ui.label(term).classes(
            f"text-xs font-semibold text-gray-900 {term_width} shrink-0"
        )
        desc = ui.row().classes("items-center gap-1.5 flex-1 flex-wrap")
        if description:
            with desc:
                ui.label(description).classes("text-xs text-gray-600")
    return desc


# ---------------------------------------------------------------------------
# Form field (Cloudscape "FormField")
# ---------------------------------------------------------------------------


def form_field(
    ui,
    *,
    label: str,
    description: str = "",
    constraint: str = "",
    help_text: str = "",
    control_width: str = "w-24",
):
    """Render one AWS Console (Cloudscape "FormField") settings row.

    Cloudscape pairs every control with a VISIBLE label, a description explaining what
    it does, and constraint text stating the accepted values. This app previously hid
    the description behind a hover-only info glyph to keep each knob on one line, which
    made the form unreadable at a glance: you had to hover five fields in turn to learn
    what any of them did, and hover text is unavailable on touch.

    Layout: label and control share the top line (control right-aligned, so a column of
    inputs aligns down the panel), with the description and constraint on a quieter
    second line. That keeps a dense settings list scannable while still showing the
    guidance inline.

    ``help_text`` is Cloudscape's "info" affordance: a small glyph after the label whose
    tooltip carries the deeper explanation -- when to change the value, what it costs,
    when it takes effect. This is NOT the old anti-pattern: ``description`` stays visible,
    so the field is still fully readable without hovering, and the tooltip only holds the
    detail that would bloat the row. Use it for a knob whose guidance is genuinely long.

    Returns the **control container** so the caller places the input via ``with``::

        with form_field(ui, label="Rows per batch", description="…", constraint="1–3000"):
            ui.number(...)
    """
    with ui.column().classes("gap-0.5 w-full"):
        with ui.row().classes("items-center gap-3 no-wrap w-full"):
            if help_text:
                # Label + glyph must sit together, so wrap them; without this the glyph
                # is pushed to the far right by the label's flex-1.
                with ui.row().classes("items-center gap-1 no-wrap flex-1 min-w-0"):
                    ui.label(label).classes(
                        "text-sm font-medium text-gray-900 truncate"
                    )
                    ui.icon("info_outline").classes(
                        "text-gray-400 text-sm cursor-help shrink-0"
                    ).tooltip(help_text)
            else:
                ui.label(label).classes(
                    "text-sm font-medium text-gray-900 flex-1 min-w-0 truncate"
                )
            slot = ui.row().classes(
                f"items-center justify-end no-wrap shrink-0 {control_width}"
            )
        if description or constraint:
            with ui.row().classes("items-baseline gap-2 no-wrap w-full"):
                if description:
                    ui.label(description).classes(
                        "text-xs text-gray-500 leading-snug flex-1 min-w-0"
                    )
                if constraint:
                    # Monospace so the accepted values read as data, and right-aligned
                    # under the control they constrain.
                    ui.label(constraint).classes(
                        "text-xs text-gray-400 font-mono shrink-0"
                    )
    return slot


__all__ = [
    "NOTICE_STYLE",
    "render_notice",
    "INLINE_HINT_TEXT",
    "inline_hint",
    "BADGE_TONES",
    "badge_classes",
    "STATUS_DOT_TONES",
    "render_status_dot",
    "CHIP_GROUP_PALETTE",
    "CHIP_GROUP_QUASAR_COLOR",
    "chip_group_index",
    "chip_group_text_class",
    "chip_group_quasar_color",
    "schema_chip_classes",
    "SEGMENTED_TOGGLE_PROPS",
    "SEGMENTED_TOGGLE_CLASSES",
    "EXPANSION_PANEL_CLASSES",
    "segmented_control",
    "FILTER_SELECT_PROPS",
    "FILTER_SELECT_CLASSES",
    "filter_bar",
    "filter_select",
    "definition_row",
    "form_field",
    "section_header",
]
