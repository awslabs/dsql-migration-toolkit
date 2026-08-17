# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Five-step migration workflow shell (stepper) and its pure logic.

The migration is organized into five top-level steps shown to the user as a
stepper (Connect is the preliminary step handled in ``connect.py``):

    Evaluation -> Schema Conversion -> Data Migration -> Validation -> Cut over

Each step's status (NOT_STARTED / IN_PROGRESS / DONE / FAILED) is tracked in a
per-session :class:`~dsql_migrator.core.models.WorkflowState` and displayed on
the stepper (Requirement 8.7). Steps can be run and re-run independently
(Requirement 8.6); prerequisites are surfaced as *advisory* guidance, never a
hard block, so a user may run a later step before an earlier one is complete.

The helpers below (step ordering/titles, status transitions, gating rules,
navigation) are independent of NiceGUI so they can be unit tested directly; the
page builder at the bottom wires them to NiceGUI widgets. The per-step content
screens are implemented in later tasks and plug in through the ``step_content``
and ``runners`` hooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
import re
from typing import TYPE_CHECKING, Callable, Optional, Sequence

from dsql_migrator.core.models import MigrationContext, StepStatus, WorkflowState
from dsql_migrator.ui.ai_panel import AiPanelHandle, build_ai_panel
from dsql_migrator.ui.design import (
    STATUS_DOT_TONES,
    inline_hint,
    render_notice,
    render_status_dot,
)

# The Start-over confirmation dialog + CDC-teardown banner UI were extracted to
# start_over.py for maintainability. Re-exported so build_workflow_sidebar's calls and
# the existing test imports (`from dsql_migrator.ui.workflow import _start_over_cdc_warning`)
# resolve unchanged.
from dsql_migrator.ui.start_over import (  # noqa: F401
    _cdc_teardown_banner_copy,
    _open_start_over_dialog,
    _render_cdc_teardown_banner,
    _start_over_cdc_warning,
)


def _dev_unlock_steps() -> bool:
    """Dev-only escape hatch: unlock all steps regardless of connection/prereqs.

    When ``DSQL_MIGRATOR_DEV_UNLOCK_STEPS`` is truthy, the connection latch and
    per-step prerequisite gating are bypassed so a developer can open any step
    (e.g. to review the Cut over screen) without running the whole workflow. This
    is strictly for local UI review -- it never relaxes any AWS/data safety, only
    the in-UI navigation gates. Off by default; leave unset in any real use.
    """
    return os.environ.get("DSQL_MIGRATOR_DEV_UNLOCK_STEPS", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

if TYPE_CHECKING:
    from dsql_migrator.ui.session import SessionStore


class WorkflowStep(str, Enum):
    """A top-level workflow step.

    Each value matches the corresponding field name on
    :class:`~dsql_migrator.core.models.WorkflowState`, so the step's status can
    be read/written by attribute name.
    """

    # RETIRED as a nav step. It asked "Include CDC?" before Evaluation had produced
    # any evidence to decide on, and duplicated the migration-type selector that
    # Data Migration already owns; the CDC-infra deploy it offered now lives on Data
    # Migration's Prerequisites sub-step, where the table set is confirmed and the
    # ~15-20 min MSK create can overlap the Full Load. The member (and its
    # WorkflowState field) is KEPT so older persisted snapshots -- which name it --
    # still load, mirroring the deprecated ``DATA_MIGRATION`` alias below.
    MIGRATION_PLAN = "migration_plan"
    EVALUATION = "evaluation"
    SCHEMA_CONVERSION = "schema_conversion"
    # ``DATA_MIGRATION`` is deprecated as a nav step: it is split into the
    # independent ``FULL_LOAD`` and ``CDC`` sub-steps. The member (and its
    # WorkflowState field) is kept so older persisted snapshots and back-compat
    # references still resolve.
    DATA_MIGRATION = "data_migration"
    FULL_LOAD = "full_load"
    CDC = "cdc"
    VALIDATION = "validation"
    # The final step: guidance for switching the application from MySQL to DSQL.
    # The tool cannot run or verify a cut-over (it is an operational act the
    # operator performs), so this step has no Run action and is marked Done by
    # the user acknowledging they've cut over.
    CUT_OVER = "cut_over"


@dataclass(frozen=True)
class StepDefinition:
    """Static description of a workflow step: its title and prerequisite."""

    step: WorkflowStep
    title: str
    prerequisite: Optional[WorkflowStep]


@dataclass(frozen=True)
class OptionalTool:
    """A standalone, optional tool shown under the nav's "Optional tools" section.

    Optional tools are not part of the linear four-step migration and have no
    workflow status/gating: they are reachable once the connections are unlocked
    (the same latch the steps use), in any order. ``content`` renders the tool's
    screen and is given the shell's ``refresh`` callback (like a step builder).
    ``view_key`` is the stable string used as the selected-view sentinel and the
    persisted ``active_view`` value.
    """

    view_key: str
    label: str
    caption: str
    icon: str
    content: Callable[[Callable[[], None]], None]


# Ordered nav definitions. "Data Migration" is a single step (backed by the
# WorkflowStep.FULL_LOAD identifier) whose inner type selector picks Full load
# only / CDC only / Full load + CDC; it depends only on Schema Conversion.
# Validation depends on that step having run. Connect is the precondition for
# Evaluation and is handled separately by the page.
_STEP_DEFINITIONS: tuple[StepDefinition, ...] = (
    StepDefinition(WorkflowStep.EVALUATION, "Evaluation", None),
    StepDefinition(
        WorkflowStep.SCHEMA_CONVERSION, "Schema Conversion", WorkflowStep.EVALUATION
    ),
    StepDefinition(
        WorkflowStep.FULL_LOAD, "Data Migration", WorkflowStep.SCHEMA_CONVERSION
    ),
    StepDefinition(WorkflowStep.VALIDATION, "Validation", WorkflowStep.FULL_LOAD),
    StepDefinition(WorkflowStep.CUT_OVER, "Cut over", WorkflowStep.VALIDATION),
)

_DEFINITION_BY_STEP: dict[WorkflowStep, StepDefinition] = {
    definition.step: definition for definition in _STEP_DEFINITIONS
}
# Back-compat: keep a title/prerequisite for steps that are no longer listed as
# nav entries (the deprecated DATA_MIGRATION alias and CDC, which was folded into
# the unified Data Migration step) so ``step_title``/``prerequisite`` still
# resolve for any legacy reference (e.g. a restored session whose stored view or
# status names them).
_DEFINITION_BY_STEP[WorkflowStep.DATA_MIGRATION] = StepDefinition(
    WorkflowStep.DATA_MIGRATION, "Data Migration", WorkflowStep.SCHEMA_CONVERSION
)
_DEFINITION_BY_STEP[WorkflowStep.CDC] = StepDefinition(
    WorkflowStep.CDC, "Data Migration", WorkflowStep.SCHEMA_CONVERSION
)
# The retired Migration plan step: a restored session can still name it (a stored
# ``active_view``, or a persisted status), so ``step_title``/``prerequisite`` must
# keep resolving instead of raising KeyError. It is deliberately absent from
# ``_STEP_DEFINITIONS``, so it never appears in the nav / stepper and is never
# reachable via previous_step/next_step.
_DEFINITION_BY_STEP[WorkflowStep.MIGRATION_PLAN] = StepDefinition(
    WorkflowStep.MIGRATION_PLAN, "Migration plan", None
)

# Steps whose prerequisite is satisfied when ANY of the listed steps is DONE
# (non-linear gating). The unified Data Migration step (WorkflowStep.FULL_LOAD)
# is now Validation's single prerequisite regardless of migration type, so no
# any-of gating is needed; kept as an (empty) extension point.
_ANY_OF_PREREQS: dict[WorkflowStep, tuple[WorkflowStep, ...]] = {}

_STATUS_LABELS: dict[StepStatus, str] = {
    StepStatus.NOT_STARTED: "Not started",
    StepStatus.IN_PROGRESS: "In progress",
    StepStatus.DONE: "Success",
    StepStatus.FAILED: "Failed",
}

# Quasar color names used for the status badge / stepper accent.
_STATUS_COLORS: dict[StepStatus, str] = {
    StepStatus.NOT_STARTED: "grey",
    StepStatus.IN_PROGRESS: "primary",
    StepStatus.DONE: "positive",
    StepStatus.FAILED: "negative",
}

# Material icon names used for each step's stepper icon.
_STATUS_ICONS: dict[StepStatus, str] = {
    StepStatus.NOT_STARTED: "radio_button_unchecked",
    StepStatus.IN_PROGRESS: "autorenew",
    StepStatus.DONE: "check_circle",
    StepStatus.FAILED: "error",
}


def step_definitions() -> tuple[StepDefinition, ...]:
    """Return the ordered workflow step definitions."""
    return _STEP_DEFINITIONS


def ordered_steps() -> tuple[WorkflowStep, ...]:
    """Return the workflow steps in execution order."""
    return tuple(definition.step for definition in _STEP_DEFINITIONS)


def step_title(step: WorkflowStep) -> str:
    """Return the human-readable English title for ``step``."""
    return _DEFINITION_BY_STEP[step].title


# The action verb for each step's primary action button, as (first-run, re-run)
# labels. A plain "Run"/"Re-run" is ambiguous on a step-by-step journey, so each
# step names its OWN action -- "Start validation" / "Re-run validation" -- far more
# intuitive about WHAT the button kicks off, and the two labels stay parallel.
# Steps not listed fall back to "Run <title>" / "Re-run <title>".
_STEP_RUN_VERB: dict[WorkflowStep, tuple[str, str]] = {
    WorkflowStep.EVALUATION: ("Run evaluation", "Re-run evaluation"),
    WorkflowStep.SCHEMA_CONVERSION: (
        "Apply schema conversion",
        "Re-apply schema conversion",
    ),
    WorkflowStep.FULL_LOAD: ("Start migration", "Re-run migration"),
    WorkflowStep.DATA_MIGRATION: ("Start migration", "Re-run migration"),
    WorkflowStep.CDC: ("Start migration", "Re-run migration"),
    WorkflowStep.VALIDATION: ("Start validation", "Re-run validation"),
}


def step_run_label(step: WorkflowStep, status: "StepStatus") -> str:
    """Return the primary action-button label for ``step`` in ``status``.

    First run (``NOT_STARTED``): a step-specific "start" verb (e.g. "Start
    validation"); afterwards a parallel "re-run" verb (e.g. "Re-run validation")
    so the button always names its own action instead of a bare "Run"/"Re-run".
    """
    first, rerun = _STEP_RUN_VERB.get(
        step, (f"Run {step_title(step)}", f"Re-run {step_title(step)}")
    )
    return first if status is StepStatus.NOT_STARTED else rerun


# Steps that are sub-steps of a named group: shown under that group's subheader
# in the nav and prefixed (group breadcrumb) in the step header and the diagram
# "Current stage" chip. Data Migration is now a single top-level step (its
# Full load / CDC choice is an inner type selector), so there are no nav groups;
# kept as an (empty) extension point.
_STEP_GROUP: dict[WorkflowStep, str] = {}


def step_group(step: WorkflowStep) -> Optional[str]:
    """Return the group a step belongs to (e.g. 'Data Migration'), or None."""
    return _STEP_GROUP.get(step)


def step_breadcrumb(step: WorkflowStep) -> str:
    """Return ``'Group / Title'`` for a grouped step, else just the title."""
    group = step_group(step)
    title = step_title(step)
    return f"{group} / {title}" if group else title


def prerequisite(step: WorkflowStep) -> Optional[WorkflowStep]:
    """Return the step that should precede ``step``, or ``None`` for the first."""
    return _DEFINITION_BY_STEP[step].prerequisite


def previous_step(step: WorkflowStep) -> Optional[WorkflowStep]:
    """Return the step immediately before ``step`` in order, or ``None``."""
    steps = ordered_steps()
    index = steps.index(step)
    return steps[index - 1] if index > 0 else None


def next_step(step: WorkflowStep) -> Optional[WorkflowStep]:
    """Return the step immediately after ``step`` in order, or ``None``."""
    steps = ordered_steps()
    index = steps.index(step)
    return steps[index + 1] if index < len(steps) - 1 else None


def get_status(state: WorkflowState, step: WorkflowStep) -> StepStatus:
    """Return the current status of ``step`` from ``state``."""
    return getattr(state, step.value)


def with_status(
    state: WorkflowState, step: WorkflowStep, status: StepStatus
) -> WorkflowState:
    """Return a copy of ``state`` with ``step`` set to ``status``.

    The input ``state`` is not mutated, so callers can treat workflow state as
    immutable and store the returned value back on the session.
    """
    return state.model_copy(update={step.value: status})


def status_label(status: StepStatus) -> str:
    """Return the user-facing English label for ``status``."""
    return _STATUS_LABELS[status]


def status_color(status: StepStatus) -> str:
    """Return the Quasar color name used to display ``status``."""
    return _STATUS_COLORS[status]


def status_icon(status: StepStatus) -> str:
    """Return the Material icon name used to display ``status``."""
    return _STATUS_ICONS[status]


def is_prerequisite_met(state: WorkflowState, step: WorkflowStep) -> bool:
    """Return whether ``step``'s prerequisite is complete.

    A step with no prerequisite is always considered met. A step with an
    any-of prerequisite (e.g. Validation) is met when ANY of its listed steps is
    DONE. Otherwise the single prerequisite must be :attr:`StepStatus.DONE`.
    """
    any_of = _ANY_OF_PREREQS.get(step)
    if any_of is not None:
        return any(get_status(state, s) is StepStatus.DONE for s in any_of)
    prereq = prerequisite(step)
    if prereq is None:
        return True
    return get_status(state, prereq) is StepStatus.DONE


def gating_message(state: WorkflowState, step: WorkflowStep) -> Optional[str]:
    """Return advisory guidance when ``step``'s prerequisite is incomplete.

    Returns ``None`` when the prerequisite is met (or there is none). The message
    is guidance only: the user may still run the step independently
    (Requirement 8.6).
    """
    if is_prerequisite_met(state, step):
        return None
    any_of = _ANY_OF_PREREQS.get(step)
    if any_of is not None:
        names = " or ".join(step_title(s) for s in any_of)
        return (
            f"{names} is not complete yet. You can still run {step_title(step)} "
            f"independently, but its results may be incomplete until one of them "
            f"finishes."
        )
    prereq = prerequisite(step)
    assert prereq is not None  # implied by is_prerequisite_met being False
    return (
        f"{step_title(prereq)} is not complete yet. You can still run "
        f"{step_title(step)} independently, but its results may be incomplete "
        f"until {step_title(prereq)} finishes."
    )


@dataclass(frozen=True)
class DiagramNode:
    """One node of the source -> tool -> target migration overview diagram.

    ``connected`` drives the visual cue (green when the connection is verified,
    grey otherwise); the tool node is always "connected" (it is this app).
    ``reconnect`` flags a restored-but-unverified endpoint: progress was resumed
    from a snapshot but the live connection has not been re-tested this process,
    so the node shows an amber "Reconnect to resume" cue rather than a flat grey
    "Not connected" (which reads as "never set up").
    """

    title: str
    subtitle: str
    icon: str
    connected: bool
    reconnect: bool = False
    # Each detail line is an (icon, text) pair so the node can render a small
    # leading icon next to a labeled value (e.g. "Instance type: db.r7i.large").
    details: tuple[tuple[str, str], ...] = ()
    region: Optional[str] = None
    # Small bordered status chips (label-card feel): (text, tone) where tone is
    # one of "ok" | "bad" | "active" | "neutral". Used for connectivity on the
    # source/target and the stage / AI-assist status on the tool node.
    badges: tuple[tuple[str, str], ...] = ()
    # Optional inline SVG service glyph rendered instead of the Material ``icon``.
    # Authored, offline-safe glyphs (not the trademarked AWS Architecture Icons);
    # swap in an official SVG here when bundled.
    svg: Optional[str] = None


# Marker embedded in an Aurora MySQL ``VERSION()`` string, e.g.
# ``8.0.mysql_aurora.3.10.4`` -> MySQL-compatible base ``8.0`` + Aurora ``3.10.4``.
_AURORA_VERSION_MARKER = ".mysql_aurora."
# A clean community version like ``8.0.42`` (used to prefer the full MySQL patch).
_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+")


def format_source_engine(
    version: Optional[str],
    mysql_version: Optional[str] = None,
    aurora_version: Optional[str] = None,
) -> Optional[str]:
    """Format the source engine label from the captured version strings.

    Prefers the explicit Aurora engine version (``@@aurora_version``, e.g.
    ``3.07.1``) when present -- newer Aurora MySQL reports only the
    MySQL-compatible patch in ``VERSION()`` so the Aurora tag may be absent
    there. Otherwise falls back to the Aurora tag embedded in ``VERSION()``
    (``8.0.mysql_aurora.3.10.4``). Plain MySQL is shown as ``MySQL 8.0.35``.
    Returns ``None`` when no version was captured.
    """
    if aurora_version:
        base = version.split(_AURORA_VERSION_MARKER)[0] if version else None
        mysql = mysql_version or base
        suffix = f" (MySQL {mysql})" if mysql else ""
        return f"Aurora MySQL {aurora_version}{suffix}"
    if not version:
        return f"MySQL {mysql_version}" if mysql_version else None
    if _AURORA_VERSION_MARKER in version:
        base, _, aurora = version.partition(_AURORA_VERSION_MARKER)
        mysql = (
            mysql_version
            if mysql_version and _SEMVER_RE.match(mysql_version)
            else base
        )
        return f"Aurora MySQL {aurora} (MySQL {mysql})"
    # Plain MySQL: VERSION() already carries the full community version.
    return f"MySQL {version}"


def _source_engine_title(
    version: Optional[str],
    host: Optional[str],
    aurora_version: Optional[str] = None,
) -> str:
    """Classify the source engine for the diagram title.

    Aurora MySQL is detected by ``@@aurora_version``, the ``.mysql_aurora.``
    version marker, or a ``.cluster-`` Aurora cluster endpoint host. A non-Aurora
    ``.rds.amazonaws.com`` host is RDS MySQL; a plain community version is native
    ``MySQL``. Falls back to the generic ``Source MySQL`` when undetermined.
    """
    if (
        aurora_version
        or (version and _AURORA_VERSION_MARKER in version)
        or (host and ".cluster-" in host)
    ):
        return "Aurora MySQL"
    if host and host.endswith(".rds.amazonaws.com"):
        return "RDS MySQL"
    if version:
        return "MySQL"
    return "Source MySQL"


def _aws_region_from_host(host: Optional[str]) -> Optional[str]:
    """Extract an AWS region token (e.g. ``us-east-1``) from a hostname.

    Works for RDS/Aurora MySQL hosts (``...us-east-1.rds.amazonaws.com``) and
    DSQL-style hosts (``...us-east-1.on.aws``). Returns ``None`` when the host
    carries no recognizable region (e.g. a bare ``db.example.com``).
    """
    if not host:
        return None
    match = re.search(r"\b([a-z]{2}-[a-z]+-\d+)\b", host)
    return match.group(1) if match else None


def _dsql_cluster_name(endpoint: Optional[str]) -> Optional[str]:
    """Derive the DSQL cluster name (id) from its endpoint.

    A DSQL endpoint looks like ``<cluster-id>.dsql.<region>.on.aws``; the cluster
    id is the label before ``.dsql.`` (or the first label otherwise). Returns
    ``None`` for an empty endpoint.
    """
    if not endpoint:
        return None
    head, marker, _rest = endpoint.partition(".dsql.")
    return head if marker else endpoint.split(".", 1)[0]


# Official AWS Architecture Icons (64/Databases), inlined for offline-safe
# rendering (no external URL / static route). Both are the trademarked AWS icons
# from the AWS Architecture Icons asset package: the Databases-category squid
# glyph on the #C925D1 tile. A 12px corner radius is added to the background tile
# so it reads as a rounded chip in the diagram node; the size is controlled by
# the node renderer's Tailwind classes (h-10 w-10), so no width/height here.
# Source: Amazon RDS (covers RDS MySQL and Aurora MySQL sources).
_SOURCE_DB_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 80 80" '
    'role="img" aria-label="Amazon RDS source database">'
    '<rect x="0" y="0" width="80" height="80" rx="12" fill="#C925D1"/>'
    '<path fill="#FFFFFF" d="M15.414,14 L24.707,23.293 L23.293,24.707 L14,15.414 '
    'L14,23 L12,23 L12,13 C12,12.448 12.447,12 13,12 L23,12 L23,14 L15.414,14 Z '
    'M68,13 L68,23 L66,23 L66,15.414 L56.707,24.707 L55.293,23.293 L64.586,14 '
    'L57,14 L57,12 L67,12 C67.553,12 68,12.448 68,13 L68,13 Z M66,57 L68,57 '
    'L68,67 C68,67.552 67.553,68 67,68 L57,68 L57,66 L64.586,66 L55.293,56.707 '
    'L56.707,55.293 L66,64.586 L66,57 Z M65.5,39.213 C65.5,35.894 61.668,32.615 '
    '55.25,30.442 L55.891,28.548 C63.268,31.045 67.5,34.932 67.5,39.213 '
    'C67.5,43.495 63.268,47.383 55.89,49.879 L55.249,47.984 C61.668,45.812 '
    '65.5,42.534 65.5,39.213 L65.5,39.213 Z M14.556,39.213 C14.556,42.393 '
    '18.143,45.585 24.152,47.753 L23.473,49.634 C16.535,47.131 12.556,43.333 '
    '12.556,39.213 C12.556,35.094 16.535,31.296 23.473,28.792 L24.152,30.673 '
    'C18.143,32.842 14.556,36.034 14.556,39.213 L14.556,39.213 Z M24.707,56.707 '
    'L15.414,66 L23,66 L23,68 L13,68 C12.447,68 12,67.552 12,67 L12,57 L14,57 '
    'L14,64.586 L23.293,55.293 L24.707,56.707 Z M40,31.286 C32.854,31.286 '
    '29,29.44 29,28.686 C29,27.931 32.854,26.086 40,26.086 C47.145,26.086 '
    '51,27.931 51,28.686 C51,29.44 47.145,31.286 40,31.286 L40,31.286 Z '
    'M40.029,39.031 C33.187,39.031 29,37.162 29,36.145 L29,31.284 '
    'C31.463,32.643 35.832,33.286 40,33.286 C44.168,33.286 48.537,32.643 '
    '51,31.284 L51,36.145 C51,37.163 46.835,39.031 40.029,39.031 L40.029,39.031 '
    'Z M40.029,46.667 C33.187,46.667 29,44.798 29,43.781 L29,38.862 '
    'C31.431,40.291 35.742,41.031 40.029,41.031 C44.292,41.031 48.578,40.292 '
    '51,38.867 L51,43.781 C51,44.799 46.835,46.667 40.029,46.667 L40.029,46.667 '
    'Z M40,53.518 C32.883,53.518 29,51.605 29,50.622 L29,46.498 C31.431,47.927 '
    '35.742,48.667 40.029,48.667 C44.292,48.667 48.578,47.929 51,46.503 '
    'L51,50.622 C51,51.605 47.117,53.518 40,53.518 L40,53.518 Z M40,24.086 '
    'C33.739,24.086 27,25.525 27,28.686 L27,50.622 C27,53.836 33.54,55.518 '
    '40,55.518 C46.46,55.518 53,53.836 53,50.622 L53,28.686 C53,25.525 '
    '46.261,24.086 40,24.086 L40,24.086 Z"/></svg>'
)
# Target: Amazon Aurora (Aurora DSQL is part of the Aurora family; no standalone
# DSQL architecture icon exists yet, so the official Aurora icon is used).
_DSQL_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 80 80" '
    'role="img" aria-label="Amazon Aurora DSQL">'
    '<rect x="0" y="0" width="80" height="80" rx="12" fill="#C925D1"/>'
    '<path fill="#FFFFFF" d="M45.0911055,18.0451258 L42.0830668,18.0451258 '
    'L42.0830668,16.0270755 L45.0911055,16.0270755 L45.0911055,13 '
    'L47.0964647,13 L47.0964647,16.0270755 L50.1045034,16.0270755 '
    'L50.1045034,18.0451258 L47.0964647,18.0451258 L47.0964647,21.0722014 '
    'L45.0911055,21.0722014 L45.0911055,18.0451258 Z M57.1232604,29.1444027 '
    'L54.1152217,29.1444027 L54.1152217,27.1263524 L57.1232604,27.1263524 '
    'L57.1232604,24.0992769 L59.1286196,24.0992769 L59.1286196,27.1263524 '
    'L62.1366583,27.1263524 L62.1366583,29.1444027 L59.1286196,29.1444027 '
    'L59.1286196,32.1714782 L57.1232604,32.1714782 L57.1232604,29.1444027 Z '
    'M51.9013052,61.0336342 C49.9861872,56.1681148 45.5663757,51.7203319 '
    '40.7294494,49.7920848 C45.5663757,47.8648467 49.9861872,43.4170637 '
    '51.9013052,38.5505353 C53.8164232,43.4170637 58.2362348,47.8648467 '
    '63.0721584,49.7920848 C58.2362348,51.7203319 53.8164232,56.1681148 '
    '51.9013052,61.0336342 L51.9013052,61.0336342 Z M67.9973204,48.7830596 '
    'C60.9444723,48.7830596 52.9039848,40.6916868 52.9039848,33.5942037 '
    'C52.9039848,33.0372218 52.455787,32.5851785 51.9013052,32.5851785 '
    'C51.3468234,32.5851785 50.8986256,33.0372218 50.8986256,33.5942037 '
    'C50.8986256,40.6916868 42.8571354,48.7830596 35.8042873,48.7830596 '
    'C35.2508082,48.7830596 34.8016077,49.2351029 34.8016077,49.7920848 '
    'C34.8016077,50.3500757 35.2508082,50.8011099 35.8042873,50.8011099 '
    'C42.8571354,50.8011099 50.8986256,58.8924828 50.8986256,65.9909748 '
    'C50.8986256,66.5479567 51.3468234,67 51.9013052,67 C52.455787,67 '
    '52.9039848,66.5479567 52.9039848,65.9909748 C52.9039848,58.8924828 '
    '60.9444723,50.8011099 67.9973204,50.8011099 C68.5507996,50.8011099 '
    '69,50.3500757 69,49.7920848 C69,49.2351029 68.5507996,48.7830596 '
    '67.9973204,48.7830596 L67.9973204,48.7830596 Z M13.0053591,28.9032457 '
    'C15.9251621,31.040361 21.5983231,32.1714782 27.0428732,32.1714782 '
    'C32.4874233,32.1714782 38.1605843,31.040361 41.0803872,28.9032457 '
    'L41.0803872,38.5676888 C39.635526,40.500981 34.3223269,42.4060205 '
    '27.2434091,42.4060205 C19.0946322,42.4060205 13.0053591,39.8430966 '
    '13.0053591,37.5516004 L13.0053591,28.9032457 Z M27.0428732,21.0722014 '
    'C35.7401158,21.0722014 41.0803872,23.7168563 41.0803872,25.6128146 '
    'C41.0803872,27.5087729 35.7401158,30.1534279 27.0428732,30.1534279 '
    'C18.3456306,30.1534279 13.0053591,27.5087729 13.0053591,25.6128146 '
    'C13.0053591,23.7168563 18.3456306,21.0722014 27.0428732,21.0722014 '
    'L27.0428732,21.0722014 Z M41.0803872,58.941925 C41.0803872,61.267728 '
    '35.0753393,63.8700039 27.0378598,63.8700039 C19.0063964,63.8700039 '
    '13.0053591,61.267728 13.0053591,58.941925 L13.0053591,52.4942542 '
    'C15.9612585,54.7504344 21.7477224,55.9451202 27.3005618,55.9451202 '
    'C31.1618809,55.9451202 34.8958596,55.3962105 37.8106491,54.3992937 '
    'L37.1669289,52.4882 C34.4536779,53.4154941 30.9493128,53.9270699 '
    '27.3005618,53.9270699 C19.1186965,53.9270699 13.0053591,51.364146 '
    '13.0053591,49.0726498 L13.0053591,40.9792589 C15.9532371,43.2314031 '
    '21.7146339,44.4240709 27.2434091,44.4240709 C33.16724,44.4240709 '
    '38.2478174,43.1960872 41.0803872,41.2516957 L41.0803872,44.2797803 '
    'L43.0857464,44.2797803 L43.0857464,25.6128146 C43.0857464,21.3527104 '
    '34.8206587,19.054151 27.0428732,19.054151 C19.5819345,19.054151 '
    '11.6918489,21.1751219 11.0621661,25.108302 L11,25.108302 L11,58.941925 '
    'C11,63.4532765 19.2630824,65.8880543 27.0378598,65.8880543 '
    'C34.8176506,65.8880543 43.0857464,63.4532765 43.0857464,58.941925 '
    'L43.0857464,55.3790571 L41.0803872,55.3790571 L41.0803872,58.941925 Z"/>'
    '</svg>'
)


def _connection_badge(verified: bool, reconnect: bool) -> tuple[str, str]:
    """Return the (text, tone) connectivity chip for a source/target node.

    Three states: verified -> green "Connected"; restored-but-unverified ->
    amber "Reconnect to resume" (resumable work, just re-test the connection);
    otherwise grey "Not connected" (never set up this session).
    """
    if verified:
        return ("Connected", "ok")
    if reconnect:
        return ("Reconnect to resume", "reconnect")
    return ("Not connected", "neutral")


def build_migration_diagram(
    state: "object", current_step: "Optional[WorkflowStep]" = None
) -> tuple[DiagramNode, DiagramNode, DiagramNode]:
    """Build the (source, tool, target) nodes for the migration overview diagram.

    Labels are derived from the session's configured connections so the diagram
    reflects the actual source/target; before a connection is configured a
    generic placeholder is shown. ``current_step`` (when given) makes the middle
    "Migration Tool" node show the active stage (Evaluation / Schema Conversion /
    Data Migration / Validation). NiceGUI-agnostic so the label derivation is
    unit-testable.
    """
    source_config = getattr(state, "source_config", None)
    target_config = getattr(state, "target_config", None)
    source_verified = bool(getattr(state, "source_verified", False))
    target_verified = bool(getattr(state, "target_verified", False))
    # "Reconnect to resume": there is restored progress to come back to, but the
    # live connections were never re-verified this process (credentials/verified
    # flags are never persisted -- Property 7). The diagram then distinguishes a
    # resumable session ("Reconnect to resume", amber) from a brand-new one
    # ("Not connected", grey). Computed once for both endpoint nodes.
    resumable = bool(reconnect_notice(state))

    # Only reflect real connection details once the connection test has passed;
    # an unverified/typed-in (or restored-but-unverified) connection shows a
    # generic placeholder until the user re-tests successfully.
    if source_config is not None and source_verified:
        host = getattr(source_config, "host", "")
        database = getattr(source_config, "database", None)
        # Mirror the target: the cluster/instance name is the primary line and
        # the full endpoint (host) is shown small as a labeled detail.
        source_subtitle = (host.split(".", 1)[0] if host else "") or "Source MySQL"
        server_version = getattr(state, "source_server_version", None)
        aurora_version = getattr(state, "source_aurora_version", None)
        instance_class = getattr(state, "source_instance_class", None)
        source_region = _aws_region_from_host(host)
        engine = format_source_engine(
            server_version,
            getattr(state, "source_mysql_version", None),
            aurora_version,
        )
        # (icon, labeled text) pairs; each on its own line so nothing is cut off.
        source_details = tuple(
            pair
            for pair in (
                ("developer_board", engine) if engine else None,
                ("memory", f"Instance type: {instance_class}")
                if instance_class
                else None,
                ("dns", f"Endpoint: {host}") if host else None,
                ("storage", f"Database: {database}") if database else None,
            )
            if pair is not None
        )
        source_title = _source_engine_title(server_version, host, aurora_version)
    else:
        source_subtitle = "RDS / Aurora MySQL"
        source_details = ()
        source_title = "Source MySQL"
        source_region = None
    # Source credentials/config are never persisted (Property 7), so on a resume
    # the source node has no remembered details -- only the "Reconnect to resume"
    # cue telling the user the live connection must be re-tested.
    source_reconnect = resumable and not source_verified
    source = DiagramNode(
        title=source_title,
        subtitle=source_subtitle,
        icon="storage",
        connected=source_verified,
        reconnect=source_reconnect,
        details=source_details,
        region=source_region,
        svg=_SOURCE_DB_SVG,
        badges=(_connection_badge(source_verified, source_reconnect),),
    )

    # Middle node: the tool's role as subtitle, with the current stage and the
    # AI-assist on/off shown as small bordered status chips.
    ai_enabled = bool(getattr(getattr(state, "ai_assist", None), "enabled", False))
    tool_badges: tuple[tuple[str, str], ...] = ()
    if current_step is not None:
        tool_badges += (
            (f"Current stage: {step_breadcrumb(current_step)}", "active"),
        )
    tool_badges += (
        ("AI assist: On", "ok") if ai_enabled else ("AI assist: Off", "neutral"),
    )
    tool = DiagramNode(
        title="Migration Tool",
        subtitle="Convert · Load · Validate",
        icon="sync_alt",
        connected=True,
        badges=tool_badges,
    )

    # Target: show the cluster name as the primary line, with the full endpoint
    # and region as separate (untruncated) detail lines. Shown when the target
    # test has passed, OR when the (non-secret) target config was restored from a
    # snapshot on a resume -- DSQL is IAM-auth, so endpoint + region are remembered
    # and worth previewing (dimmed) next to the "Reconnect to resume" cue.
    target_reconnect = resumable and not target_verified and target_config is not None
    if target_config is not None and (target_verified or target_reconnect):
        endpoint = getattr(target_config, "cluster_endpoint", "") or ""
        target_region = getattr(target_config, "region", None)
        # Prefer the cluster's "Name" tag (looked up on the target test) over the
        # cluster id parsed from the endpoint.
        cluster_name = (
            getattr(state, "target_cluster_name", None)
            or _dsql_cluster_name(endpoint)
        )
        target_subtitle = cluster_name or "Aurora DSQL"
        cluster_id = _dsql_cluster_name(endpoint)
        target_details = tuple(
            pair
            for pair in (
                ("badge", f"Cluster id: {cluster_id}")
                if cluster_id and cluster_id != cluster_name
                else None,
                ("dns", f"Endpoint: {endpoint}") if endpoint else None,
            )
            if pair is not None
        )
    else:
        target_subtitle = "PostgreSQL-compatible"
        target_details = ()
        target_region = None
    target = DiagramNode(
        title="Aurora DSQL",
        subtitle=target_subtitle,
        icon="cloud",
        connected=target_verified,
        reconnect=target_reconnect,
        details=target_details,
        region=target_region,
        svg=_DSQL_SVG,
        badges=(_connection_badge(target_verified, target_reconnect),),
    )
    return source, tool, target


def _render_diagram_segment(
    ui: object,
    node: DiagramNode,
    *,
    role_pill: "Optional[tuple[str, str]]" = None,
) -> None:
    """Render one segment of the unified migration overview panel.

    ``role_pill`` is an optional ``(label, tailwind_classes)`` tuple that renders
    a small pill badge at the top of the segment — used only on the tool node to
    convey its "CONVERT · LOAD" function. Source/target are self-explanatory from
    their titles and need no role pill.
    """
    segment_bg = " bg-amber-50/60" if node.reconnect else ""
    with ui.column().classes(  # type: ignore[attr-defined]
        "items-center gap-1 py-5 px-5 min-w-0 flex-1" + segment_bg
    ):
        # Role pill — only on the tool node.
        if role_pill is not None:
            pill_text, pill_classes = role_pill
            ui.label(pill_text).classes(  # type: ignore[attr-defined]
                "text-[9px] font-bold tracking-widest uppercase "
                f"px-2 py-0.5 rounded-full border {pill_classes} mb-1"
            )
        # Service glyph (SVG) or Material icon — scaled up for visual weight.
        if node.svg:
            svg_opacity = (
                "" if node.connected else (" opacity-60" if node.reconnect else " opacity-25")
            )
            ui.html(node.svg).classes("h-10 w-10" + svg_opacity)  # type: ignore[attr-defined]
        else:
            color = "primary" if node.connected else ("amber-7" if node.reconnect else "grey-5")
            ui.icon(node.icon, color=color).classes("text-3xl")  # type: ignore[attr-defined]
        # Title — text-sm for clear hierarchy.
        ui.label(node.title).classes(  # type: ignore[attr-defined]
            "text-sm font-semibold text-gray-900 text-center mt-2"
        )
        # Subtitle.
        ui.label(node.subtitle).classes(  # type: ignore[attr-defined]
            "text-xs text-gray-400 text-center break-all leading-tight"
        )
        # Region.
        if node.region:
            with ui.row().classes("items-center gap-1 no-wrap mt-0.5"):  # type: ignore[attr-defined]
                ui.icon("public", color="grey-4").classes("text-[10px]")  # type: ignore[attr-defined]
                ui.label(node.region).classes(  # type: ignore[attr-defined]
                    "text-[10px] text-gray-400"
                )
        # Status indicators — larger dot and heavier text for legibility.
        if node.badges:
            with ui.column().classes("items-center gap-1 mt-2"):  # type: ignore[attr-defined]
                for text, tone in node.badges:
                    dot_bg, text_color = STATUS_DOT_TONES.get(
                        tone, STATUS_DOT_TONES["neutral"]
                    )
                    with ui.row().classes("items-center gap-1.5 no-wrap"):  # type: ignore[attr-defined]
                        ui.element("div").classes(  # type: ignore[attr-defined]
                            f"h-2.5 w-2.5 rounded-full shrink-0 {dot_bg}"
                        )
                        ui.label(text).classes(  # type: ignore[attr-defined]
                            f"text-xs font-medium {text_color}"
                        )
        # Detail key-value pairs.
        if node.details:
            with ui.column().classes(  # type: ignore[attr-defined]
                "gap-1 mt-3 w-full pt-2 border-t border-gray-100"
            ):
                for _icon_name, line in node.details:
                    if ": " in line:
                        label, _, value = line.partition(": ")
                        with ui.column().classes("gap-0 w-full"):  # type: ignore[attr-defined]
                            ui.label(label).classes(  # type: ignore[attr-defined]
                                "text-[9px] text-gray-400 uppercase tracking-wide"
                            )
                            ui.label(value).classes(  # type: ignore[attr-defined]
                                "text-[11px] text-gray-700 break-all leading-snug"
                            )
                    else:
                        ui.label(line).classes(  # type: ignore[attr-defined]
                            "text-[11px] text-gray-700 break-all leading-snug"
                        )


def _render_migration_diagram(
    ui: object, state: "object", current_step: "Optional[WorkflowStep]" = None
) -> None:
    """Render the source -> tool -> target overview as a single unified panel.

    Three segments share one surface. Flow direction is communicated by bold
    ``arrow_forward`` icons between segments — no faint divider lines, no
    redundant role labels. The tool segment gets a single "CONVERT · LOAD" pill
    badge; source/target are self-explanatory from their titles.
    """
    source, tool, target = build_migration_diagram(state, current_step)
    with ui.card().classes(  # type: ignore[attr-defined]
        "w-full !shadow-none border border-gray-200 rounded-xl p-0 overflow-hidden"
    ):
        with ui.row().classes(  # type: ignore[attr-defined]
            "items-stretch w-full no-wrap gap-0"
        ):
            _render_diagram_segment(ui, source)
            # Flow arrow — bold, unmistakable direction.
            with ui.column().classes(  # type: ignore[attr-defined]
                "items-center justify-center w-8 shrink-0 self-center"
            ):
                ui.icon("arrow_forward", color="grey-6").classes("text-xl")  # type: ignore[attr-defined]
            _render_diagram_segment(
                ui, tool, role_pill=("CONVERT · LOAD", "bg-blue-50 border-blue-200 text-blue-600")
            )
            # Flow arrow.
            with ui.column().classes(  # type: ignore[attr-defined]
                "items-center justify-center w-8 shrink-0 self-center"
            ):
                ui.icon("arrow_forward", color="grey-6").classes("text-xl")  # type: ignore[attr-defined]
            _render_diagram_segment(ui, target)


def _migration_type_chosen(state: "object") -> bool:
    """Whether the session has an EXPLICITLY chosen migration type.

    ``state.migration_type`` always returns a value (full-load-only is the default),
    so it cannot distinguish "the user picked Full load only" from "the user has not
    decided yet". The session latches a separate flag on
    :meth:`~dsql_migrator.ui.session.SessionConnectionState.set_migration_type`.

    Duck-typed and fail-closed: a state object without the flag (an older snapshot, a
    test double) reports ``False``, so the banner is omitted rather than asserting a
    choice that was never made.
    """
    getter = getattr(state, "migration_type_chosen", None)
    if not callable(getter):
        return False
    try:
        return bool(getter())
    except Exception:  # noqa: BLE001 - decorative header; never break the page
        return False


def _migration_type_meta(state: "object"):
    """Return (label, icon, blurb) for the session's chosen migration type.

    Read-only: the type is chosen on Data Migration; every step shows it for
    continuity. Imported lazily to avoid a circular import (data_migration imports
    workflow). Falls back to a neutral label if the metadata can't be resolved.
    """
    try:
        from dsql_migrator.ui.data_migration import _MIGRATION_TYPE_META

        mt = getattr(state, "migration_type", None)
        meta = _MIGRATION_TYPE_META.get(mt)
        if meta is not None:
            return meta.label, meta.icon, meta.blurb
    except Exception:  # noqa: BLE001 - header is decorative; never break the page
        pass
    return "Migration", "tune", ""


def _render_journey_header(
    ui: object,
    state: "object",
    current_step: "WorkflowStep",
    select: "Callable[[object], None]",
) -> None:
    """Render the shared 'one journey' header above every workflow step.

    Two unified bands, identical on all steps so the flow reads as a single guided
    journey rather than separate screens:

    1. A horizontal progress stepper (Evaluation -> Schema -> Migration ->
       Validation -> Cut over) with each step's real status icon/color and the
       current step highlighted; clicking a step navigates to it (the sidebar's lock
       rules still apply via ``select``).
    2. A compact migration-type banner showing the type chosen on Data Migration.
       Shown ONLY on the Data Migration step (``WorkflowStep.CDC``), next to the
       selector that owns the choice. It used to render on every step for "one
       journey" continuity, but the single ``session.migration_type`` cannot describe
       a session that ran Full Load and THEN switched to "CDC only" (the guided
       post-Full-Load path): later steps -- Validation especially -- then showed
       "CDC only" with a blurb saying "no Full Load in this session", contradicting
       what the user had just done. Rather than assert a stale, sometimes-false
       summary on screens that cannot correct it, the banner stays where the choice
       is actually made and current. The journey stepper (band 1) still carries the
       cross-step continuity on every step.
    """
    steps = ordered_steps()
    # Band 1: the journey stepper.
    with ui.row().classes(  # type: ignore[attr-defined]
        "items-center gap-1 w-full no-wrap overflow-x-auto py-1"
    ):
        for index, st in enumerate(steps):
            st_status = get_status(state.workflow, st)
            is_current = st is current_step
            done = st_status is StepStatus.DONE
            # Current step: filled primary chip. Done: green check. Else: grey.
            if is_current:
                icon_name, icon_color = "adjust", "primary"
            else:
                icon_name = status_icon(st_status)
                icon_color = status_color(st_status)
            chip = (
                "items-center gap-1 no-wrap rounded-full px-2 py-1 cursor-pointer "
                + ("bg-blue-50 border border-blue-300" if is_current else "hover:bg-gray-100")
            )
            with ui.row().classes(chip).on(  # type: ignore[attr-defined]
                "click", lambda _e=None, s=st: select(s)
            ):
                ui.icon(icon_name, color=icon_color).classes("text-base")  # type: ignore[attr-defined]
                ui.label(f"{index + 1}. {step_title(st)}").classes(  # type: ignore[attr-defined]
                    "text-xs whitespace-nowrap "
                    + (
                        "font-semibold text-blue-700"
                        if is_current
                        else "text-positive" if done else "text-gray-500"
                    )
                )
            if index < len(steps) - 1:
                ui.icon("chevron_right", color="grey-5").classes("text-sm")  # type: ignore[attr-defined]

    # Band 2: the migration-type banner. Two gates:
    #  - Only on the Data Migration step (WorkflowStep.CDC), where the selector lives
    #    and the type is current -- see the docstring for why it no longer rides along
    #    to Validation/Cut over, where a Full-Load-then-CDC-only session read wrong.
    #  - Only once the user has ACTUALLY chosen (``migration_type`` defaults to
    #    full-load-only, so an unconditional render would present that default as a
    #    settled decision before the user picked anything).
    if current_step is not WorkflowStep.CDC:
        return
    if not _migration_type_chosen(state):
        return
    # Layout: icon + "Migration type:" + the type name stay together on one line
    # (no-wrap), and the description wraps fully onto following lines (never
    # truncated/cut off). items-start so the icon aligns to the first line when
    # the description wraps to several rows.
    label, icon, blurb = _migration_type_meta(state)
    with ui.row().classes(  # type: ignore[attr-defined]
        "items-start gap-2 no-wrap w-full rounded-md border border-blue-200 "
        "bg-blue-50 px-3 py-2"
    ):
        ui.icon(icon, color="primary").classes("text-lg shrink-0")  # type: ignore[attr-defined]
        # min-w-0 lets the text column shrink so its children can wrap inside the row.
        with ui.column().classes("gap-0 min-w-0 flex-1"):  # type: ignore[attr-defined]
            with ui.row().classes("items-baseline gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.label("Migration type:").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 shrink-0"
                )
                ui.label(label).classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-blue-800 whitespace-nowrap"
                )
            if blurb:
                # Full description, wrapped (not truncated) so nothing is cut off.
                ui.label(blurb).classes("text-xs text-gray-600")  # type: ignore[attr-defined]


def connection_nav_state(state: "object") -> str:
    """Classify the Connect nav item's connection state for its status icon.

    Three situations the nav used to render identically (the icon reflected only whether
    Connect was the SELECTED view, so a session needing re-verification looked exactly
    like a healthy one):

    * ``"connected"``  -- both source and target verified in THIS process.
    * ``"reconnect"``  -- restored progress but the connections are not verified:
      credentials are never persisted (Property 7), so an app restart lands here.
    * ``"unset"``      -- a fresh session that simply has not connected yet. Not a
      problem, so it must not be flagged like one.

    ``reconnect`` is amber, not red: the data is intact and re-entering credentials fixes
    it, so per the design system's severity calibration it is a recoverable warning, not
    a blocking error -- and it matches the amber reconnect banner describing the same
    state. Pure/duck-typed so a test double works.
    """
    connected = bool(getattr(state, "source_verified", False)) and bool(
        getattr(state, "target_verified", False)
    )
    if connected:
        return "connected"
    # Same "is there anything to resume" signal as the reconnect banner, so the icon and
    # the banner can never disagree.
    return "reconnect" if reconnect_notice(state) else "unset"


def reconnect_notice(state: "object") -> Optional[str]:
    """Return a resume hint when restored progress needs the connections re-verified.

    After an app restart the session's progress is restored from durable state
    (workflow status, evaluation result, the Full Load job linkage, the chosen
    migration plan, the last-viewed step, and a deployed cdc-stack's identity),
    but the source/target connections are not -- credentials are never persisted
    (Property 7). So when a session has anything to resume yet both connections
    are not currently verified, the user must re-verify on the Connect step
    before they can run/resume. Returns ``None`` when there is nothing to resume
    or the connections are already verified.
    """
    workflow = getattr(state, "workflow", None)
    if workflow is None:
        return None
    # "Progressed" is broader than just a started workflow step: choosing a CDC
    # migration type, deploying CDC infrastructure (cdc-stack), or being parked on a
    # non-Connect view all mean there is real restored work to resume -- even when
    # every workflow step is still NOT_STARTED (e.g. CDC infra was deployed from the
    # Data Migration step's Prerequisites sub-step before any step completed).
    step_progressed = any(
        getattr(workflow, step.value) != "NOT_STARTED" for step in WorkflowStep
    )
    active_view = getattr(state, "active_view", None)
    parked_past_connect = bool(active_view) and active_view != "connect"
    # A chosen migration type that includes CDC is restorable work. Normalize the
    # value (enum or string) and only count the non-default CDC modes -- the
    # full-load-only default is not, by itself, "progress" to resume.
    mt = getattr(state, "migration_type", None)
    mt_value = getattr(mt, "value", mt)
    chose_cdc_plan = mt_value in ("cdc_only", "full_load_and_cdc")
    # Non-empty session-level CDC infra inputs mean infrastructure was set up.
    infra_getter = getattr(state, "cdc_infra_inputs", None)
    has_cdc_infra = bool(infra_getter()) if callable(infra_getter) else False
    progressed = (
        step_progressed or parked_past_connect or chose_cdc_plan or has_cdc_infra
    )
    connected = bool(getattr(state, "source_verified", False)) and bool(
        getattr(state, "target_verified", False)
    )
    if progressed and not connected:
        return (
            "Reconnected — your previous progress was restored (your migration type, "
            "any deployed CDC infrastructure, and the step you were on). "
            "Re-verify the source and target connections on the Connect step to "
            "resume exactly where you left off; nothing already done is lost."
        )
    return None


def _render_reconnect_banner(ui: object, state: "object") -> None:
    """Render the resume hint banner when a reconnected session needs re-verifying."""
    notice = reconnect_notice(state)
    if not notice:
        return
    render_notice(
        ui,
        tone="warning",
        header="Reconnected — re-verify your connections",
        body=notice,
    )


# How often the persistent CDC-teardown banner re-checks the background job. A
# teardown runs for minutes (delete ~15–45 min), so a slow poll is plenty; it only
# needs to notice completion to remove itself.


def build_workflow_sidebar(
    store: "SessionStore",
    session_id: str,
    *,
    app_title: str,
    version: str,
    connect_builder: Callable[[Callable[[], None], Callable[[], None]], None],
    step_content: Optional[
        dict[WorkflowStep, Callable[[Callable[[], None]], None]]
    ] = None,
    runners: Optional[dict[WorkflowStep, Callable[[], None]]] = None,
    run_guards: Optional[dict[WorkflowStep, Callable[[], Optional[str]]]] = None,
    on_state_change: Optional[Callable[[], None]] = None,
    nav_export: Optional[Callable[[Callable[[object], None]], None]] = None,
    footer_extra: Optional[Callable[[], None]] = None,
    on_reset: Optional[Callable[[], None]] = None,
    on_reset_cdc: Optional[Callable[[str], None]] = None,
    cdc_deployed_getter: Optional[Callable[[], bool]] = None,
    cdc_stack_name_getter: Optional[Callable[[], Optional[str]]] = None,
    cdc_stack_names_getter: Optional[Callable[[], Sequence[str]]] = None,
    cdc_teardown_in_flight_getter: Optional[Callable[[], bool]] = None,
    cdc_teardown_banner_getter: Optional[Callable[[], Optional[dict]]] = None,
    cdc_teardown_retry: Optional[Callable[[], None]] = None,
    cdc_teardown_dismiss: Optional[Callable[[], None]] = None,
    # A FINISHED teardown, reported until the operator closes it (the toast that used to
    # be the only completion signal does not survive a refresh).
    cdc_teardown_done_getter: Optional[Callable[[], Optional[dict]]] = None,
    cdc_teardown_done_dismiss: Optional[Callable[[], None]] = None,
    cdc_op_in_flight_getter: Optional[Callable[[], Optional[str]]] = None,
    cdc_probe: Optional[Callable[[], None]] = None,
    optional_tools: Optional[dict[str, "OptionalTool"]] = None,
    on_ai_panel_ready: Optional[Callable[["AiPanelHandle"], None]] = None,
    ai_context_getter: Optional[Callable[[], "MigrationContext"]] = None,
    ai_general_streamer_factory: Optional[Callable[[], object]] = None,
    ai_progress_provider: Optional[Callable[[], Optional[dict]]] = None,
) -> None:
    """Render the app as a sidebar layout: header + left-drawer nav + content.

    The left drawer lists the preliminary Connect screen and the four workflow
    steps, each with its status icon and label. Selecting an item renders only
    that screen in the main content area, so the user navigates the migration
    setup like a multi-screen tool instead of one long scroll.

    ``connect_builder`` renders the Connect screen; it is given two callbacks:
    ``go_to_first_step`` (advance into the first workflow step, e.g. from a
    "Next" button) and ``on_connection_change`` (refresh the nav so the step
    lock state updates live when a connection is verified or invalidated).
    ``step_content`` maps a step
    to a builder that renders that step's screen; each builder is given a
    ``refresh`` callback that re-renders both the nav (status badges) and the
    content, so a screen can reflect status changes that happen asynchronously
    (e.g. a background job finishing). Steps without a builder show a
    placeholder. ``runners`` maps a step to its Run/Re-run callback; a step
    without a runner falls back to marking itself ``DONE`` so the status display
    and gating remain demonstrable before the real screens exist.
    """
    from nicegui import ui

    state = store.get_or_create(session_id)
    content = step_content or {}
    step_runners = runners or {}
    step_run_guards = run_guards or {}
    tools = optional_tools or {}

    # The currently selected view: the sentinel _CONNECT_VIEW or a WorkflowStep.
    _CONNECT_VIEW = "connect"

    def _restore_view() -> object:
        """Restore the view the user was last on, so a browser refresh stays put.

        Falls back to the Connect view when nothing is stored, when the workflow
        is still locked (connections not yet verified this run), or when the
        stored step is no longer reachable (its prerequisite is not Done). An
        optional-tool view is restored once the workflow is unlocked.
        """
        stored = getattr(state, "active_view", None)
        if not stored or stored == _CONNECT_VIEW:
            return _CONNECT_VIEW
        if stored in tools:
            return stored if state.workflow_unlocked() else _CONNECT_VIEW
        for step in ordered_steps():
            if step.value == stored:
                if state.workflow_unlocked() and is_prerequisite_met(
                    state.workflow, step
                ):
                    return step
                return _CONNECT_VIEW
        return _CONNECT_VIEW

    selected: dict[str, object] = {"view": _restore_view()}

    def refresh_all() -> None:
        # Re-render both the nav (status badges) and the main content.
        render_nav.refresh()
        render_main.refresh()
        # Persist the session snapshot (dirty-checked by the caller) so progress
        # survives an app restart (resumability, Property 4).
        if on_state_change is not None:
            on_state_change()

    def _announce_nav(view: object, previous: object) -> None:
        # Mirror a genuine STEP transition into the AI panel's activity feed so the
        # conversation follows along ("Moved to the Evaluation step"). Only for a real
        # WorkflowStep that actually changed (not re-selecting the same step, not
        # opening an optional tool); post_event self-gates when AI is off.
        if view == previous or not isinstance(view, WorkflowStep):
            return
        try:
            ai_panel.post_event(
                text=f"Moved to the {step_title(view)} step", status="info"
            )
            # Update the panel's baseline step chip so the "where you are" label follows
            # the navigation (unless pinned to a specific object scope).
            ai_panel.refresh_context()
        except Exception:  # noqa: BLE001 - the feed is best-effort, never break nav
            pass

    def select(view: object) -> None:
        previous = selected.get("view")
        # An optional tool (string key, not a WorkflowStep) is reachable once the
        # connections are unlocked, in any order -- it has no workflow gating. The
        # dev escape hatch (DSQL_MIGRATOR_DEV_UNLOCK_STEPS) opens ANY step for local
        # UI review, so it must open optional tools too (they were previously the
        # one exception, which also blocked opening e.g. Query validation offline).
        if isinstance(view, str) and view in tools:
            if not state.workflow_unlocked() and not _dev_unlock_steps():
                ui.notify(
                    "Connect and verify the source and target connections first.",
                    type="warning",
                )
                return
            selected["view"] = view
            state.set_active_view(view)
            refresh_all()
            return
        # Lock the four workflow steps until both connections have been verified
        # at least once; the Connect view is always reachable so the user can fix
        # the connection. Uses the sticky workflow-unlock latch (not the live
        # verified flags) so navigating between steps is never re-blocked by a
        # transient connection invalidation on the Connect screen.
        if (
            isinstance(view, WorkflowStep)
            and not state.workflow_unlocked()
            and not _dev_unlock_steps()
        ):
            ui.notify(
                "Connect and verify the source and target connections first.",
                type="warning",
            )
            return
        # Enforce the step order: a step cannot be opened until its prerequisite
        # is Done. Data Migration therefore stays unreachable until Schema
        # Conversion is Done -- either by applying the conversion or by verifying
        # the target schema is already prepared (the "skip conversion" check).
        if (
            isinstance(view, WorkflowStep)
            and not is_prerequisite_met(state.workflow, view)
            and not _dev_unlock_steps()
        ):
            prereq = prerequisite(view)
            assert prereq is not None  # implied by is_prerequisite_met being False
            ui.notify(
                f"Complete {step_title(prereq)} first before opening "
                f"{step_title(view)}.",
                type="warning",
            )
            return
        selected["view"] = view
        # Remember where the user is so a browser refresh restores this view
        # instead of resetting to Connect.
        state.set_active_view(
            view.value if isinstance(view, WorkflowStep) else _CONNECT_VIEW
        )
        _announce_nav(view, previous)
        refresh_all()

    def go_to_first_step() -> None:
        # Advance from Connect into the first workflow step (Evaluation).
        select(ordered_steps()[0])

    # Hand the navigation function back to the caller (e.g. so a step screen can
    # jump straight to another step, like "skip conversion -> Data Migration").
    if nav_export is not None:
        nav_export(select)

    def _make_runner(step: WorkflowStep) -> Callable[[], None]:
        def _run() -> None:
            runner = step_runners.get(step)
            if runner is not None:
                runner()
            else:
                # Placeholder until the step's screen (later task) drives status.
                state.set_workflow(with_status(state.workflow, step, StepStatus.DONE))
            refresh_all()

        return _run

    # --- Persistent AI assistant panel (right drawer) -------------------------
    # Built once at the shell so it survives content refreshes (it is a sibling of
    # the header/left-drawer, outside the refreshable main content). Renders from the
    # session (transcript + open/closed state survive close/reopen, navigation, and a
    # browser refresh). Screens deep-link into it via the handle's open_scope; the
    # header toggle opens/closes it. Handed back via on_ai_panel_ready so app.py can
    # route the screens' AI buttons + Connect's auto-open through the same handle.
    ai_panel = build_ai_panel(
        ui,
        state=state,
        get_context=ai_context_getter,
        general_streamer_factory=ai_general_streamer_factory,
        # Persist the session (dirty-checked) after a chat turn / activity event /
        # open-close, so the transcript survives an unexpected app restart even with
        # no intervening navigation. Only ever invoked from the UI event loop.
        on_change=on_state_change,
        # Drives the live progress monitor card (e.g. Full Load) from a persistent
        # panel-owned timer, so it keeps updating across navigation.
        get_progress=ai_progress_provider,
    )
    if on_ai_panel_ready is not None:
        on_ai_panel_ready(ai_panel)

    def _toggle_ai_panel() -> None:
        if ai_panel.is_enabled():
            ai_panel.toggle()
        else:
            ui.notify(
                "Enable AI Assist on the Connect screen to use AI DBA.",
                type="info",
            )

    def _open_readiness_briefing() -> None:
        # Proactive "what should I do next & what are my top risks" briefing. Uses the
        # SAME general streamer as the header chat (tool_chat over this session's real
        # state), seeded so AI DBA reads the actual assessment / conversion / load /
        # validation state via tools and names the specific objects blocking progress
        # -- turning the passive panel into a next-step guide. Re-opening the scope
        # re-focuses without re-asking (open_scope dedupes by scope_id).
        if not ai_panel.is_enabled():
            ui.notify(
                "Enable AI Assist on the Connect screen to use AI DBA.",
                type="info",
            )
            return
        streamer = (
            ai_general_streamer_factory()
            if ai_general_streamer_factory is not None
            else None
        )
        if streamer is None:
            ai_panel.toggle()  # AI enabled but no streamer available; just open blank
            return
        ai_panel.open_scope(
            scope_id="readiness",
            title="AI DBA",
            subtitle="What's next & top risks",
            chip="Migration readiness",
            streamer=streamer,
            seed_question=(
                "Given where I am in this MySQL → Aurora DSQL migration, what should "
                "I do next, and what are my top risks right now? Use your tools to "
                "check my real assessment, schema conversion, data-load, CDC health "
                "(DLQ poison records, schema drift, a stalled sink) and validation "
                "state, and call out the specific objects/tables that are blocking "
                "progress or are standing gaps CDC won't backfill before cut over."
            ),
        )

    # --- Header ---------------------------------------------------------------
    with ui.header().classes("items-center justify-between"):
        with ui.row().classes("items-center gap-2"):
            ui.button(icon="menu", on_click=lambda: drawer.toggle()).props(
                "flat round dense color=white"
            )
            ui.label(app_title).classes("text-lg font-bold")
        with ui.row().classes("items-center gap-3"):
            # Proactive briefing: asks AI DBA, grounded on the real session state,
            # what to do next and the top risks -- so the panel isn't only reactive.
            ui.button(
                "What's next?",
                icon="tips_and_updates",
                on_click=_open_readiness_briefing,
            ).props("flat dense color=white").tooltip(
                "Ask AI DBA what to do next and your top risks, from your real state"
            )
            # AI DBA toggle: a LABELED button (not a bare icon) so it is
            # obvious it opens/closes the side panel -- matches the "Start over"
            # button's treatment. Always present so the panel is reachable anytime;
            # a no-op-with-hint until AI Assist is enabled on Connect.
            ui.button(
                "AI DBA", icon="auto_awesome", on_click=_toggle_ai_panel
            ).props("flat dense color=white").tooltip(
                "Open or close the AI DBA panel"
            )
            if on_reset is not None:

                async def _open_start_over() -> None:
                    # Confirm the LIVE CDC deployment state before deciding whether to
                    # show the stop/delete tiles. cdc_deployed_getter only reads cached
                    # discovery, which is populated when the CDC step renders -- so from
                    # any OTHER step (or a session that never opened it) a genuinely
                    # deployed CDC would otherwise fall back to the passive warning with
                    # no teardown action. Run the read-only AWS probe off the event loop
                    # first (it is blocking network I/O), then open the dialog with the
                    # freshly-refreshed cached state. Best-effort: if the probe fails we
                    # still open with whatever was cached.
                    #
                    # The probe is ~1-2s of network I/O, so show a busy cue on the
                    # button while it runs: disable it (prevents a double-open) and
                    # swap its label to "Checking…" + a sync/hourglass icon, restoring
                    # both when the dialog opens. We swap the LABEL/ICON rather than use
                    # Quasar's `loading` prop, matching the app-wide busy idiom in
                    # connect.py (the loading prop spins the border on flat buttons and
                    # reads as an artifact); the label swap makes the short wait legible.
                    if cdc_probe is not None:
                        from nicegui import run as _sd_run

                        start_over_btn.disable()
                        start_over_btn.set_text("Checking…")
                        start_over_btn.props("icon=hourglass_top")
                        try:
                            await _sd_run.io_bound(cdc_probe)
                        except Exception:  # noqa: BLE001 - open with cached state
                            pass
                        finally:
                            if not getattr(start_over_btn, "is_deleted", False):
                                start_over_btn.set_text("Start over")
                                start_over_btn.props("icon=restart_alt")
                                start_over_btn.enable()
                    _open_start_over_dialog(
                        ui, state, on_reset, select, refresh_all, _CONNECT_VIEW,
                        cdc_deployed=(
                            bool(cdc_deployed_getter()) if cdc_deployed_getter else False
                        ),
                        on_reset_cdc=on_reset_cdc,
                        cdc_stack_name=(
                            cdc_stack_name_getter() if cdc_stack_name_getter else None
                        ),
                        cdc_stack_names=(
                            cdc_stack_names_getter() if cdc_stack_names_getter else None
                        ),
                        cdc_teardown_in_flight=(
                            bool(cdc_teardown_in_flight_getter())
                            if cdc_teardown_in_flight_getter
                            else False
                        ),
                        cdc_op_in_flight=(
                            cdc_op_in_flight_getter()
                            if cdc_op_in_flight_getter
                            else None
                        ),
                    )

                start_over_btn = ui.button(
                    "Start over",
                    icon="restart_alt",
                    on_click=_open_start_over,
                ).props("flat dense color=white")
                start_over_btn.tooltip("Clear this session and start a new migration")
            ui.label(f"v{version}").classes("text-sm opacity-80")

    # --- Left drawer (navigation) --------------------------------------------
    drawer = ui.left_drawer(value=True, bordered=True).classes("bg-grey-1")
    with drawer:

        @ui.refreshable
        def render_nav() -> None:
            workflow = state.workflow
            with ui.list().props("padding").classes("w-full"):
                ui.item_label("Setup").props("header").classes(
                    "text-xs uppercase text-gray-500"
                )
                connect_active = selected["view"] == _CONNECT_VIEW
                with ui.item(on_click=lambda: select(_CONNECT_VIEW)).props(
                    "clickable"
                ).classes(
                    "rounded-borders " + ("bg-blue-1" if connect_active else "")
                ):
                    # The icon carries the CONNECTION state, not just whether Connect
                    # is the selected view: green = both verified, amber broken-link =
                    # restored progress needing re-verification (a restart drops the
                    # credentials -- Property 7), grey = not connected yet. Without this
                    # all three looked identical, so after a restart nothing hinted that
                    # Connect had to be revisited before anything could run.
                    _conn = connection_nav_state(state)
                    _icon, _color, _caption, _tip = {
                        "connected": (
                            "link", "positive", "Connected",
                            "Source and target are verified.",
                        ),
                        "reconnect": (
                            "link_off", "warning", "Reconnect to resume",
                            "Credentials are not kept across a restart — re-verify "
                            "the source and target to resume.",
                        ),
                    }.get(
                        _conn,
                        (
                            "link",
                            "primary" if connect_active else "grey",
                            "Source / target",
                            "Enter the source and target connections.",
                        ),
                    )
                    with ui.item_section().props("avatar"):
                        ui.icon(_icon, color=_color).tooltip(_tip)
                    with ui.item_section():
                        ui.item_label("Connect")
                        ui.item_label(_caption).props("caption")

                ui.separator()
                ui.item_label("Migration workflow").props("header").classes(
                    "text-xs uppercase text-gray-500"
                )
                # The four steps are locked until both connections have been
                # verified once on the Connect screen (gate enforced in
                # select()); the sticky latch keeps them unlocked thereafter.
                locked = not state.workflow_unlocked() and not _dev_unlock_steps()

                def _render_step_item(
                    definition: StepDefinition,
                    *,
                    ordinal: Optional[str],
                    indent: bool,
                ) -> None:
                    step = definition.step
                    status = get_status(workflow, step)
                    active = selected["view"] == step
                    item_classes = "rounded-borders " + (
                        "bg-blue-1" if active else ""
                    )
                    if indent:
                        item_classes += " pl-6"
                    if locked:
                        item_classes += " opacity-50"
                    with ui.item(on_click=lambda s=step: select(s)).props(
                        "clickable"
                    ).classes(item_classes):
                        with ui.item_section().props("avatar"):
                            if locked:
                                ui.icon("lock", color="grey")
                            else:
                                ui.icon(
                                    status_icon(status), color=status_color(status)
                                )
                        with ui.item_section():
                            prefix = f"{ordinal}. " if ordinal else ""
                            ui.item_label(f"{prefix}{definition.title}")
                            skipped = (
                                step is WorkflowStep.SCHEMA_CONVERSION
                                and bool(
                                    getattr(
                                        state, "schema_conversion_skipped", False
                                    )
                                )
                            )
                            caption = (
                                "Locked until connected"
                                if locked
                                else "Skipped"
                                if skipped
                                else status_label(status)
                            )
                            ui.item_label(caption).props("caption")

                # Sub-steps with a group (e.g. Full Load / CDC under "Data
                # Migration") render under that group's subheader, indented;
                # everything else is a numbered top-level step. The group label
                # comes from ``step_group`` so the nav and the step/diagram
                # headers stay in sync.
                # Outline numbering: top-level steps are numbered (1. Evaluation,
                # 2. Schema Conversion, 4. Validation). The "Data Migration" group
                # occupies a number slot (3) but shows no number on its header;
                # its sub-steps carry the letter form (3a. Full Load, 3b. CDC), so
                # the grouping is clear while Validation stays a distinct step.
                number = 0
                current_group: Optional[str] = None
                group_number = 0
                letter = 0
                for definition in step_definitions():
                    group = step_group(definition.step)
                    if group is not None:
                        if group != current_group:
                            number += 1  # the group occupies this number slot
                            group_number = number
                            letter = 0
                            ui.item_label(group).props("header").classes(
                                "text-xs uppercase text-gray-500 q-mt-sm"
                            )
                            current_group = group
                        ordinal = f"{group_number}{chr(ord('a') + letter)}"
                        letter += 1
                        _render_step_item(definition, ordinal=ordinal, indent=True)
                    else:
                        current_group = None
                        number += 1
                        _render_step_item(
                            definition, ordinal=str(number), indent=False
                        )

                # --- Optional tools (not part of the linear migration) ---------
                # Standalone capabilities kept separate from the numbered migration
                # steps (e.g. the Query Playground: convert + test app queries on
                # DSQL). They do not move data, so they sit in their own section
                # below Validation, divided by a separator. Like the steps, they are
                # locked until the connections are verified, but they carry no
                # workflow status/gating and open in any order.
                if tools:
                    ui.separator()
                    ui.item_label("Optional tools").props("header").classes(
                        "text-xs uppercase text-gray-500 q-mt-sm"
                    )
                    for tool in tools.values():
                        tool_active = selected["view"] == tool.view_key
                        item_classes = "rounded-borders " + (
                            "bg-blue-1" if tool_active else ""
                        )
                        if locked:
                            item_classes += " opacity-50"
                        with ui.item(
                            on_click=lambda k=tool.view_key: select(k)
                        ).props("clickable").classes(item_classes):
                            with ui.item_section().props("avatar"):
                                if locked:
                                    ui.icon("lock", color="grey")
                                else:
                                    ui.icon(
                                        tool.icon,
                                        color="primary" if tool_active else "grey",
                                    )
                            with ui.item_section():
                                ui.item_label(tool.label)
                                ui.item_label(
                                    "Locked until connected"
                                    if locked
                                    else tool.caption
                                ).props("caption")

        render_nav()
        ui.space()
        ui.separator()
        if footer_extra is not None:
            footer_extra()

    # --- Main content area ----------------------------------------------------
    with ui.column().classes("w-full max-w-6xl gap-4 p-4"):

        @ui.refreshable
        def render_main() -> None:
            view = selected["view"]
            # Persistent CDC-teardown banner, pinned above ALL views (Connect, the
            # workflow steps, and the optional tools) so a background stop/delete --
            # e.g. one fired by Start over, which lands the user on Connect -- stays
            # visible until it completes. Self-polls and removes itself on settle.
            _render_cdc_teardown_banner(
                ui,
                cdc_teardown_banner_getter,
                on_retry=cdc_teardown_retry,
                on_dismiss=cdc_teardown_dismiss,
                done_getter=cdc_teardown_done_getter,
                on_done_dismiss=cdc_teardown_done_dismiss,
            )
            if view == _CONNECT_VIEW:
                # Refresh only the nav (not main) so verifying a connection
                # unlocks the steps without rebuilding the Connect form.
                connect_builder(go_to_first_step, render_nav.refresh)
                return

            # An optional tool (string key): no migration diagram / journey header /
            # step actions -- it is a standalone screen that renders itself.
            if isinstance(view, str) and view in tools:
                tools[view].content(refresh_all)
                return

            assert isinstance(view, WorkflowStep)
            step = view
            workflow = state.workflow
            status = get_status(workflow, step)

            # Resume hint when restored progress needs the connections re-verified.
            # Pinned to the very top (above the orientation diagram) so the "re-verify
            # to resume" call-to-action is the first thing seen after a reconnect.
            _render_reconnect_banner(ui, state)
            # Orientation banner: source -> migration tool -> Aurora DSQL, shown
            # on every workflow step (not on Connect) to aid understanding. The
            # current step drives the middle node's active-stage label.
            _render_migration_diagram(ui, state, step)
            # Shared "one journey" header: the progress stepper + the chosen
            # migration type, identical on every step so the flow reads as a single
            # guided journey instead of separate screens.
            _render_journey_header(ui, state, step, select)

            # Title + status on the left; the step actions (Run/Re-run and Next)
            # on the right, so they stay at the top instead of below long
            # content (e.g. a large assessment report).
            run_label = step_run_label(step, status)
            following = next_step(step)

            def make_next_button() -> None:
                """Render a "Next: <step>" button (shared by the top and bottom).

                Advances to the following step once this step is ``DONE``; until
                then it stays disabled with a hint so the guided path is clear.
                The sidebar still allows free navigation for power users.
                """
                if following is None:
                    return
                next_button = ui.button(
                    f"Next: {step_title(following)}",
                    on_click=lambda s=following: select(s),
                ).props("color=primary outline")
                if status is not StepStatus.DONE:
                    next_button.disable()
                    next_button.tooltip(
                        f"Run {step_title(step)} first to continue to "
                        f"{step_title(following)}."
                    )

            with ui.row().classes("items-center justify-between w-full no-wrap"):
                with ui.row().classes("items-center gap-3"):
                    group = step_group(step)
                    with ui.column().classes("gap-0"):
                        if group:
                            ui.label(group).classes(
                                "text-xs uppercase text-gray-500 font-semibold "
                                "leading-tight"
                            )
                        ui.label(step_title(step)).classes("text-2xl font-bold")
                    ui.badge(status_label(status)).props(
                        f"color={status_color(status)}"
                    )
                with ui.row().classes("items-center gap-2"):
                    # A step may declare a run guard: a callable returning a
                    # disable-reason string (or None when runnable). Used e.g.
                    # by Schema Conversion to require an object-browser
                    # selection before the bulk Run is allowed.
                    guard = step_run_guards.get(step)
                    disable_reason = guard() if guard is not None else None
                    # While a step is running, disable Run/Re-run so it cannot be
                    # re-submitted until the current run finishes.
                    if status is StepStatus.IN_PROGRESS:
                        disable_reason = f"{step_title(step)} is running..."
                    # Evaluation drives its first run from an in-content
                    # "Run evaluation" call-to-action, so the top action stays
                    # hidden until a report exists and then appears as "Re-run".
                    # Every other step keeps the top Run/Re-run from the start.
                    # Cut over has no job to run (it's an operational step the
                    # operator performs); it is acknowledged from in-content, so it
                    # never shows a top Run/Re-run button.
                    hide_top_run = (
                        step is WorkflowStep.EVALUATION
                        and status is StepStatus.NOT_STARTED
                    ) or step is WorkflowStep.CUT_OVER
                    if not hide_top_run:
                        run_button = ui.button(
                            run_label, on_click=_make_runner(step)
                        )
                        if disable_reason:
                            run_button.disable()
                            run_button.tooltip(disable_reason)
                    # "Next" lives only at the bottom (after the step content), so
                    # the top keeps just the status and the Run/Re-run action; the
                    # natural "review then proceed" flow ends with Next below.

            guidance = gating_message(workflow, step)
            if guidance:
                inline_hint(ui, guidance, tone="warning", classes="text-sm")

            builder = content.get(step)
            if builder is not None:
                try:
                    builder(refresh_all)
                except Exception as exc:  # noqa: BLE001 - keep the app shell usable
                    import logging

                    logging.getLogger(__name__).exception(
                        "Step content render failed for %s", step
                    )
                    render_notice(
                        ui,
                        tone="error",
                        header=f"{step_title(step)} could not be displayed",
                        body=(
                            "An unexpected error occurred while rendering this step "
                            f"({type(exc).__name__}). Your connections and migrated "
                            "data are unaffected — use the sidebar to navigate to "
                            "another step or refresh the page. If it persists, check "
                            "the activity log."
                        ),
                    )
            else:
                ui.label(
                    "This step's screen is implemented in a later task."
                ).classes("text-sm text-gray-400")

            # Convenience "Next" at the bottom so the user can advance without
            # scrolling back up past a long step screen (mirrors the top button).
            if following is not None:
                ui.separator()
                with ui.row().classes("items-center justify-end w-full"):
                    make_next_button()

        render_main()


__all__ = [
    "WorkflowStep",
    "StepDefinition",
    "OptionalTool",
    "step_definitions",
    "ordered_steps",
    "step_title",
    "step_group",
    "step_breadcrumb",
    "prerequisite",
    "previous_step",
    "next_step",
    "get_status",
    "with_status",
    "status_label",
    "status_color",
    "status_icon",
    "is_prerequisite_met",
    "gating_message",
    "build_migration_diagram",
    "DiagramNode",
    "reconnect_notice",
    "build_workflow_sidebar",
]
