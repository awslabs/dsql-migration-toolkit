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

Design rules (see also the repo CLAUDE.md "UI / AWS-style design system" section):

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


def render_notice(
    ui,
    *,
    tone: str,
    header: str,
    body: str = "",
    icon: str = "",
) -> None:
    """Render an AWS Console (Cloudscape "Alert")-style notice box.

    A tinted, rounded, bordered box with a leading status icon, a bold header
    label (the "box label"), and readable body text -- the calm awareness
    treatment AWS uses instead of loose red text. ``tone`` selects the palette
    from :data:`NOTICE_STYLE` (unknown tones fall back to ``info``); ``icon``
    overrides the tone's default Material icon. ``body`` is optional so the box
    can be a single bold line.
    """
    bg, border, icon_color, default_icon = NOTICE_STYLE.get(tone, NOTICE_STYLE["info"])
    with ui.row().classes(
        f"items-start gap-2 no-wrap w-full rounded-md border {border} {bg} p-3"
    ):
        ui.icon(icon or default_icon).classes(f"{icon_color} text-lg")
        with ui.column().classes("gap-0 flex-1 min-w-0"):
            ui.label(header).classes("text-sm font-semibold text-gray-900")
            if body:
                ui.label(body).classes("text-xs text-gray-700")


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


__all__ = [
    "NOTICE_STYLE",
    "render_notice",
    "INLINE_HINT_TEXT",
    "inline_hint",
    "BADGE_TONES",
    "badge_classes",
    "CHIP_GROUP_PALETTE",
    "CHIP_GROUP_QUASAR_COLOR",
    "chip_group_index",
    "chip_group_text_class",
    "chip_group_quasar_color",
    "schema_chip_classes",
    "SEGMENTED_TOGGLE_PROPS",
    "SEGMENTED_TOGGLE_CLASSES",
    "segmented_control",
    "section_header",
]
