# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Step 1 (Evaluation) screen of the four-step migration workflow.

The Evaluation screen produces an AWS SCT-style assessment report by analyzing
both the source and the target (design.md "Workflow Steps"). It ties together
the read-only :class:`~dsql_migrator.core.introspector.SourceIntrospector`, the
:class:`~dsql_migrator.core.assessor.CompatibilityAssessor`, and the read-only
:class:`~dsql_migrator.core.target_introspector.TargetIntrospector`. From the
configured source and target connections it:

1. introspects the source into an inventory (Requirement 8.2),
2. assesses the inventory and produces a compatibility assessment report that
   classifies every object (AUTO / MANUAL / UNSUPPORTED) and estimates the
   manual conversion effort (Simple / Medium / Significant) for non-automatic
   objects (Requirement 2 / 8.2),
3. introspects the target DSQL catalog and flags source objects that already
   exist on the target (Requirement 8.2 / 10.3), and
4. lets the user export the assessment report as JSON or text (Requirement 8.4).

Because introspection can be slow, the run executes on a background job via
:class:`~dsql_migrator.core.job_manager.JobManager` so the NiceGUI event loop is
never blocked (Requirement 9.3); the screen polls the job with a ``ui.timer``
and updates the Evaluation step status in the per-session
:class:`~dsql_migrator.core.models.WorkflowState` (NOT_STARTED -> IN_PROGRESS ->
DONE/FAILED) through the workflow helpers.

The orchestration, status mapping, and report-export serialization below are
independent of NiceGUI so they can be unit tested directly; only
:func:`build_evaluation_screen` touches NiceGUI widgets.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol

from dsql_migrator.config import SecretValue
from dsql_migrator.core.assessment_strategist import (
    AssessmentStrategist,
    ObjectGuidanceOutcome,
)
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.assessor import KIND_LABELS, CompatibilityAssessor
from dsql_migrator.core.assessor import export_report as export_assessment_report
from dsql_migrator.core.assessor import render_html_report
from dsql_migrator.core.introspector import SourceIntrospector
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.core.models import (
    AiAssistConfig,
    AssessmentItem,
    AssessmentReport,
    Classification,
    SourceConnectionConfig,
    SourceInventory,
    StepStatus,
    TargetConnectionConfig,
    TargetInventory,
)
from dsql_migrator.core.target_introspector import TargetIntrospector
from dsql_migrator.ui.connect import make_source_engine_factory
from dsql_migrator.ui.ai_chat_drawer import chat_turns_remaining
from dsql_migrator.ui.design import (
    filter_bar,
    filter_select,
    render_notice,
    section_header,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status


# ---------------------------------------------------------------------------
# Engine contracts (Protocols keep run orchestration testable with fakes)
# ---------------------------------------------------------------------------


class _Introspector(Protocol):
    """Minimal contract used by :func:`run_evaluation` (read-only source)."""

    def introspect(self, conn: SourceConnectionConfig) -> SourceInventory: ...


class _TargetBrowser(Protocol):
    """Minimal contract used by :func:`run_evaluation` (read-only target)."""

    def browse(self, conn: TargetConnectionConfig) -> TargetInventory: ...


class _Strategist(Protocol):
    """Minimal contract for the AI strategist used by the Evaluation drawer.

    The Evaluation screen runs a multi-turn chat about one object: it streams
    each assistant turn token-by-token into a chat-style drawer, grounded on the
    object's deterministic facts. This contract is that streaming chat call -- it
    takes the running transcript, emits incremental text via ``on_delta``, and
    returns the final graceful :class:`ObjectGuidanceOutcome` for the turn.
    """

    def stream_object_chat(
        self,
        item: AssessmentItem,
        messages: "list[dict[str, str]]",
        on_delta: Callable[[str], None],
    ) -> ObjectGuidanceOutcome: ...


# Builds an introspector that injects the in-memory source password.
IntrospectorFactory = Callable[[Optional[SecretValue]], _Introspector]
# Builds a target catalog browser for the optional global AWS profile.
TargetBrowserFactory = Callable[[Optional[str]], _TargetBrowser]
# Builds the AI assessment strategist for a config + optional global AWS profile.
StrategistFactory = Callable[[AiAssistConfig, Optional[str]], _Strategist]


def _default_introspector_factory(
    password: Optional[SecretValue],
) -> _Introspector:
    """Build a read-only-guarded source introspector for ``password``.

    Reuses the Connect screen's engine factory so the source is always accessed
    through the read-only guard (Property 1) and the plaintext password is read
    only at connect time (Property 7).
    """
    return SourceIntrospector(engine_factory=make_source_engine_factory(password))


def _default_target_browser_factory(aws_profile: Optional[str]) -> _TargetBrowser:
    """Build a read-only target catalog browser honoring the global AWS profile.

    The DSQL connector is built with the optional single global ``aws_profile``
    so target introspection shares the same credential context as every other
    AWS client (Requirements 9.5, 9.7). ``boto3``/``psycopg`` stay lazily
    imported inside the connector.
    """
    from dsql_migrator.core.target_connection import DsqlConnector

    return TargetIntrospector(
        connector_factory=lambda conn: DsqlConnector(conn, aws_profile=aws_profile)
    )


def _default_strategist_factory(
    config: AiAssistConfig, aws_profile: Optional[str]
) -> _Strategist:
    """Build the default Bedrock-backed AI strategist for on-demand guidance.

    Used by the Evaluation screen to generate per-object remediation guidance on
    demand (the AI drawer), not a batch report. The strategist shares the same
    single global AWS profile / credential context as every other AWS client
    (Requirements 9.5, 9.7); the Bedrock client is built lazily inside it, so
    this performs no network call.
    """
    return AssessmentStrategist(config, aws_profile=aws_profile)


# ---------------------------------------------------------------------------
# Run orchestration (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EvaluationInputs:
    """Everything needed to run an evaluation for one session.

    Both the source and target connections are required: the assessment analyzes
    the source schema against the target, and the target is introspected to
    detect objects that already exist there.
    """

    source_config: SourceConnectionConfig
    source_password: Optional[SecretValue]
    target_config: TargetConnectionConfig
    aws_profile: Optional[str] = None


@dataclass(frozen=True)
class EvaluationResult:
    """The artifacts produced by an evaluation run.

    ``target_conflicts`` lists the names of source objects (tables/views) that
    already exist on the target, so the user is warned before applying converted
    DDL (Requirement 10.3). The deterministic compatibility report is always
    authoritative and stands on its own; AI assistance, when enabled, is offered
    on demand per object via the Evaluation screen's AI drawer rather than
    produced up front, so there is no batch AI artifact on this result
    (Requirement 11.10).
    """

    inventory: SourceInventory
    assessment: AssessmentReport
    target_inventory: TargetInventory
    target_conflicts: list[str]


def _find_target_conflicts(
    source: SourceInventory, target: TargetInventory
) -> list[str]:
    """Return source table/view names that already exist on the target.

    Mirrors :meth:`TargetIntrospector.object_exists`: a source name is matched
    against the target's QUALIFIED names (``schema.table``) when it is itself
    qualified (the tool qualifies source objects as ``database.table``), and
    against unqualified names otherwise. Matching is case-insensitive
    (PostgreSQL identifier folding). This is what surfaces a conflict when the
    target already has the same schema *and* table name as the source. Source
    order is preserved.
    """
    qualified_names = {
        relation.qualified_name.lower()
        for schema in target.schemas
        for relation in (*schema.tables, *schema.views)
    }
    unqualified_names = {
        relation.name.lower()
        for schema in target.schemas
        for relation in (*schema.tables, *schema.views)
    }

    def _exists(name: str) -> bool:
        normalized = name.strip().lower()
        if "." in normalized:
            return normalized in qualified_names
        return normalized in unqualified_names

    conflicts: list[str] = []
    for table in source.tables:
        if _exists(table.name):
            conflicts.append(table.name)
    for view in source.views:
        if _exists(view.name):
            conflicts.append(view.name)
    return conflicts


@dataclass(frozen=True)
class AssessmentChartData:
    """Per-kind object counts split by CLASSIFICATION, for a stacked bar chart.

    Each list is parallel to ``kinds``: per kind, how many objects are AUTO, MANUAL and
    UNSUPPORTED.

    Classification rather than effort, because the chart sits beside the classification
    summary and a list whose badges read Auto / Review needed / Unsupported -- splitting
    the bars by effort instead made the reader translate between two vocabularies to
    reconcile the two. Effort is still reported, in its own summary and per object; it is
    just not what this bar answers.
    """

    kinds: list[str]
    auto: list[int]
    manual: list[int]
    unsupported: list[int]


def build_assessment_chart_data(report: AssessmentReport) -> AssessmentChartData:
    """Aggregate an assessment report into per-kind classification counts.

    Kinds are ordered by total object count descending, reusing the shared core
    aggregation so the UI chart and the HTML export agree.
    """
    from dsql_migrator.core.assessor import classification_stats_by_kind

    stats = classification_stats_by_kind(report)
    return AssessmentChartData(
        kinds=[kind for kind, _by_class, _total in stats],
        auto=[by_class[Classification.AUTO] for _k, by_class, _t in stats],
        manual=[by_class[Classification.MANUAL] for _k, by_class, _t in stats],
        unsupported=[by_class[Classification.UNSUPPORTED] for _k, by_class, _t in stats],
    )


# ---------------------------------------------------------------------------
# Migration readiness score
# ---------------------------------------------------------------------------

# Per-object "cost to migrate" weight, derived from the deterministic assessment.
# An AUTO object is free (the tool converts it). A non-automatic object costs by
# how much manual work it needs (its effort estimate); an UNSUPPORTED object that
# carries no effort still costs at least the SIGNIFICANT weight because DSQL
# cannot express it at all. The score is 100 minus the share of the worst-case
# cost actually incurred, so "every object AUTO" => 100 and "every object the
# hardest possible" => 0. Weights are intentionally simple and documented; they
# are a readiness heuristic for prioritization, not a time estimate.
_EFFORT_PENALTY: dict[str, float] = {
    "SIMPLE": 1.0,
    "MEDIUM": 3.0,
    "SIGNIFICANT": 7.0,
}
# Worst case per object (used to normalize): the heaviest single-object cost.
_MAX_OBJECT_PENALTY = _EFFORT_PENALTY["SIGNIFICANT"]
# A non-automatic object missing an effort estimate still costs this much: an
# UNSUPPORTED item with no effort is treated as the hardest; a MANUAL item with
# no effort as MEDIUM (a conservative middle).
_NO_EFFORT_PENALTY: dict[str, float] = {
    "UNSUPPORTED": _EFFORT_PENALTY["SIGNIFICANT"],
    "MANUAL": _EFFORT_PENALTY["MEDIUM"],
}

# Score band -> (label, notice tone, one-line meaning). Bands read on one axis of
# "how ready is this migration", reusing the shared notice tones so the score
# card matches the rest of the AWS-style UI.
_SCORE_BANDS: tuple[tuple[int, str, str, str], ...] = (
    (90, "Ready", "success",
     "Almost everything converts automatically — a straightforward migration."),
    (70, "Low effort", "success",
     "Mostly automatic with a little manual work on a few objects."),
    (45, "Moderate effort", "warning",
     "A meaningful share of objects need manual conversion — plan for it."),
    (1, "Significant effort", "warning",
     "Many objects need manual work or are unsupported — budget time and review."),
    (0, "High effort", "error",
     "Most objects need manual work or cannot move as-is — expect substantial rework."),
)

# Score notice tone -> Quasar color for the circular gauge, so the gauge color
# agrees with the tone-colored notice beneath it (one severity language).
_SCORE_GAUGE_COLOR: dict[str, str] = {
    "success": "positive",
    "warning": "amber",
    "error": "negative",
}


@dataclass(frozen=True)
class MigrationScore:
    """A 0-100 migration-readiness score derived from the assessment.

    ``score`` is 100 when every assessed object converts automatically and falls
    toward 0 as more objects need manual work (weighted by effort) or are
    unsupported. ``band``/``tone``/``summary`` describe the score for display
    (the tone is a shared notice tone). ``auto``/``manual``/``unsupported`` and
    ``total`` echo the underlying counts so the card can show the basis. An empty
    report yields a ``None`` score (no objects to rate) -- callers guard for it.
    """

    score: int
    band: str
    tone: str
    summary: str
    total: int
    auto: int
    manual: int
    unsupported: int


def compute_migration_score(report: AssessmentReport) -> Optional[MigrationScore]:
    """Compute a 0-100 migration-readiness score from the assessment report.

    Each object contributes a penalty: 0 for AUTO; its effort weight
    (SIMPLE/MEDIUM/SIGNIFICANT) for a non-automatic object; or a
    classification-based fallback when a non-automatic object carries no effort
    (UNSUPPORTED -> hardest, MANUAL -> medium). The score is
    ``round(100 * (1 - total_penalty / worst_case_penalty))`` where the
    worst-case is every object at the heaviest single-object cost, so it is
    bounded to 0-100 and is 100 exactly when nothing needs manual work. Returns
    ``None`` for an empty report (nothing to score). Pure and deterministic so it
    is unit-testable and matches the on-screen and exported values.
    """
    items = report.items
    if not items:
        return None

    total_penalty = 0.0
    auto = manual = unsupported = 0
    for item in items:
        classification = item.classification.value
        if classification == "AUTO":
            auto += 1
            continue
        if classification == "UNSUPPORTED":
            unsupported += 1
        else:
            manual += 1
        effort = item.effort.value if item.effort is not None else None
        if effort is not None:
            total_penalty += _EFFORT_PENALTY.get(effort, _EFFORT_PENALTY["MEDIUM"])
        else:
            total_penalty += _NO_EFFORT_PENALTY.get(classification, _MAX_OBJECT_PENALTY)

    worst_case = _MAX_OBJECT_PENALTY * len(items)
    ratio = total_penalty / worst_case if worst_case else 0.0
    score = max(0, min(100, round(100 * (1 - ratio))))

    label, tone, summary = _SCORE_BANDS[-1][1:]
    for threshold, band_label, band_tone, band_summary in _SCORE_BANDS:
        if score >= threshold:
            label, tone, summary = band_label, band_tone, band_summary
            break

    return MigrationScore(
        score=score,
        band=label,
        tone=tone,
        summary=summary,
        total=len(items),
        auto=auto,
        manual=manual,
        unsupported=unsupported,
    )


# Quasar badge colors for each classification (importance cue).
_CLASS_BADGE_COLOR = {
    "AUTO": "positive",
    "MANUAL": "warning",
    "UNSUPPORTED": "negative",
}

# User-facing labels for each classification. The internal enum values
# (AUTO/MANUAL/UNSUPPORTED) are kept for logic, filtering, and persistence; only
# the displayed text is friendlier so the three values read on one axis -- how
# much human work a conversion needs: "Automatic" -> "Review needed" ->
# "Unsupported". ("Manual" was ambiguous next to "Unsupported": it named the
# actor, not the feasibility.)
_CLASS_DISPLAY_LABEL = {
    "AUTO": "Automatic",
    "MANUAL": "Review needed",
    "UNSUPPORTED": "Unsupported",
}


def classification_label(value: str) -> str:
    """Return the user-facing label for a classification value.

    Maps the internal enum value (``AUTO``/``MANUAL``/``UNSUPPORTED``) to its
    display text; unknown values pass through unchanged so nothing is hidden.
    """
    return _CLASS_DISPLAY_LABEL.get(value, value)

# Quasar badge color for EVERY effort badge -- one neutral outline, no per-level ramp.
#
# Effort deliberately does not use the green/amber/red ramp. In this screen that ramp means
# COMPATIBILITY: the chart, the classification badges and the Risk/Recommendation panels all
# use it for "does this move, and how badly is it blocked". Effort is a different axis -- an
# ordered scale of hours, not a severity -- so painting it with the same three colors both
# dilutes what the ramp means and collides on the object rows, where an amber "Review
# needed" badge sat beside an amber "effort: MEDIUM" one and a red "Unsupported" beside a
# red "effort: SIGNIFICANT". The per-level map that used to live here was applied to the
# summary badges only, so the same value was colored in one place and gray in the two others
# (object rows, finding cards); one constant keeps all three consistent.
_EFFORT_BADGE_COLOR = "blue-grey-6"



def sort_assessment_items(items: list) -> list:
    """Return assessment items ordered by importance (most critical first).

    Primary key is classification severity (UNSUPPORTED > MANUAL > AUTO),
    secondary key is estimated effort (SIGNIFICANT > MEDIUM > SIMPLE > none), and
    ties break by object name for a stable, readable order.
    """
    from dsql_migrator.core.models import Classification, EffortLevel

    severity = {
        Classification.UNSUPPORTED: 2,
        Classification.MANUAL: 1,
        Classification.AUTO: 0,
    }
    effort_rank = {
        EffortLevel.SIGNIFICANT: 3,
        EffortLevel.MEDIUM: 2,
        EffortLevel.SIMPLE: 1,
    }
    return sorted(
        items,
        key=lambda item: (
            -severity.get(item.classification, 0),
            -effort_rank.get(item.effort, 0),
            item.object_name,
        ),
    )


def filter_assessment_items(
    items: list, *, classification: str = "ALL", effort: str = "ALL"
) -> list:
    """Return the assessment items kept under the two "Objects by importance" filters.

    Filters along the two color-coded categories independently and combines them
    (AND): ``classification`` is ``ALL`` or a classification enum value
    (``AUTO`` / ``MANUAL`` / ``UNSUPPORTED``); ``effort`` is ``ALL`` or an
    :class:`EffortLevel` value (``SIMPLE`` / ``MEDIUM`` / ``SIGNIFICANT``). An
    unknown value for either axis is treated as ``ALL`` (nothing hidden). Because
    ``AUTO`` objects carry no effort, selecting a specific effort naturally
    excludes them. Order is preserved, so callers can sort first and filter after.
    """
    from dsql_migrator.core.models import Classification, EffortLevel

    class_target = {
        "UNSUPPORTED": Classification.UNSUPPORTED,
        "MANUAL": Classification.MANUAL,
        "AUTO": Classification.AUTO,
    }.get(classification)
    effort_target = {
        "SIMPLE": EffortLevel.SIMPLE,
        "MEDIUM": EffortLevel.MEDIUM,
        "SIGNIFICANT": EffortLevel.SIGNIFICANT,
    }.get(effort)

    def _keep(item) -> bool:
        if class_target is not None and item.classification != class_target:
            return False
        if effort_target is not None and item.effort != effort_target:
            return False
        return True

    return [item for item in items if _keep(item)]


# Display order for grouping assessed objects by kind. Tables come first (the
# migration's primary objects), then Views, Triggers, Routines; any other kind
# follows in first-seen order.
_KIND_DISPLAY_ORDER = (
    "DATABASE", "TABLE", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION", "ROUTINE", "EVENT"
)

# Friendly, pluralized section labels for the known object kinds.
# Imported from core.assessor so the list headings, the UI chart axis and the HTML export
# all read from one map; kept under the old name for the existing call sites.
_KIND_LABELS = KIND_LABELS


def source_inventory_tallies(inventory) -> list[tuple[str, int]]:
    """Per-kind source object counts for the tally row, in the assessed list's vocabulary.

    Routines are split into stored procedures and functions rather than counted as one
    "Routines" tile. MySQL itself groups both under ``information_schema.ROUTINES``, so the
    inventory field is named correctly -- but the assessment splits them (DSQL treats them
    differently: a ``LANGUAGE SQL`` function can survive where plpgsql cannot), and the list
    and chart below therefore say "Stored procedures" and "Functions". A tile reading
    "3 Routines" left the reader hunting for a heading that does not exist.

    Labels come from :data:`_KIND_LABELS`, the same source the group headings use, so the
    two cannot drift. A kind with no objects is dropped -- an all-zero tile is noise --
    except that Tables always shows, since a source with no tables is itself worth seeing.
    """
    routines = list(getattr(inventory, "routines", None) or [])
    subtype_counts: dict[str, int] = {}
    for routine in routines:
        object_type = getattr(routine, "object_type", None)
        value = getattr(object_type, "value", None) or "ROUTINE"
        # Anything without a recognised subtype falls back to the generic ROUTINE kind,
        # matching how the assessor categorises it.
        key = value if value in ("PROCEDURE", "FUNCTION") else "ROUTINE"
        subtype_counts[key] = subtype_counts.get(key, 0) + 1

    tallies = [
        ("TABLE", len(inventory.tables)),
        ("VIEW", len(inventory.views)),
        ("TRIGGER", len(inventory.triggers)),
        ("PROCEDURE", subtype_counts.get("PROCEDURE", 0)),
        ("FUNCTION", subtype_counts.get("FUNCTION", 0)),
        ("ROUTINE", subtype_counts.get("ROUTINE", 0)),
        ("EVENT", len(inventory.events)),
    ]
    return [
        (kind_section_label(kind), count)
        for kind, count in tallies
        if count or kind == "TABLE"
    ]


def group_assessment_items_by_kind(items: list) -> list[tuple[str, list]]:
    """Group assessment items by object kind, preserving each kind's order.

    Returns ``(kind, items)`` pairs for non-empty kinds in a stable display
    order (Tables, Views, Triggers, Routines, then any other kind in first-seen
    order). The within-kind order is the input order, so a caller that sorts by
    importance and filters first keeps that ordering inside each kind group.
    """
    buckets: dict[str, list] = {}
    seen: list[str] = []
    for item in items:
        if item.kind not in buckets:
            buckets[item.kind] = []
            seen.append(item.kind)
        buckets[item.kind].append(item)
    ordered = [kind for kind in _KIND_DISPLAY_ORDER if kind in buckets]
    ordered += [kind for kind in seen if kind not in _KIND_DISPLAY_ORDER]
    return [(kind, buckets[kind]) for kind in ordered]


# Display labels for the per-object findings breakdown. Advisory findings read
# "Recommended" -- the word their own badge uses inside the expanded card -- rather than the
# MANUAL classification they technically carry.
_CONCERN_SUMMARY_LABEL = {
    "RECOMMENDED": "Recommended",
}

# Badge color per findings category. The three classifications reuse the classification
# palette so a row, the chart and the expanded findings all agree; advisory findings take
# the calm ``info`` tone their own badge uses inside the card, deliberately outside the
# green/amber/red severity ramp because they report no defect.
_CONCERN_BADGE_COLOR = {
    **_CLASS_BADGE_COLOR,
    "RECOMMENDED": "info",
}


def assessment_kind_summary(items: list) -> str:
    """Return a one-line per-classification count for a kind group.

    Counts are ordered by severity (UNSUPPORTED, MANUAL, AUTO) so the most
    critical share reads first, using the user-facing labels, e.g.
    ``2 Unsupported · 10 Automatic``.
    """
    counts: dict[str, int] = {}
    for item in items:
        value = item.classification.value
        counts[value] = counts.get(value, 0) + 1
    order = ["UNSUPPORTED", "MANUAL", "AUTO"]
    parts = [
        f"{counts[value]} {classification_label(value)}"
        for value in order
        if counts.get(value)
    ]
    parts += [
        f"{count} {classification_label(value)}"
        for value, count in counts.items()
        if value not in order
    ]
    return " · ".join(parts)


def assessment_concern_counts(item) -> list[tuple[str, int, str]]:
    """Break an object's findings into ``(label, count, badge_color)``, most severe first.

    This is what the collapsed object row renders instead of a single governing badge. That
    badge stated only the most severe classification and was silent about the rest:
    measured on a real source, 16 of 18 tables carried a mix -- typically a real gap plus
    the AUTO_INCREMENT recommendation -- behind one badge, and a row reading "Unsupported"
    could hide six findings of which four were merely review-needed and one was optional
    advice, so the object looked wholly blocked when most of it was not.

    Every entry carries its own label, so severity is never conveyed by color alone: a bare
    count badge would need the chart legend, or a hover, to decode.  Advisory findings are
    labeled ``Recommended`` -- the word their own badge uses inside the expanded card --
    rather than the ``MANUAL`` they technically carry.

    Ordered UNSUPPORTED, MANUAL, RECOMMENDED, AUTO, so the leading badge is the one the
    governing classification used to show. Returns ``[]`` for an object with no findings.
    """
    concerns = list(getattr(item, "concerns", None) or [])
    if not concerns:
        return []
    counts: dict[str, int] = {}
    for concern in concerns:
        key = (
            "RECOMMENDED"
            if getattr(concern, "is_advisory", False)
            else concern.classification.value
        )
        counts[key] = counts.get(key, 0) + 1
    order = ["UNSUPPORTED", "MANUAL", "RECOMMENDED", "AUTO"]
    entries = [
        (
            _CONCERN_SUMMARY_LABEL.get(key, classification_label(key)),
            counts[key],
            _CONCERN_BADGE_COLOR.get(key, "grey"),
        )
        for key in order
        if counts.get(key)
    ]
    # Anything unrecognised still shows rather than being silently dropped.
    entries += [
        (classification_label(key), count, _CONCERN_BADGE_COLOR.get(key, "grey"))
        for key, count in counts.items()
        if key not in order
    ]
    return entries


def kind_section_label(kind: str) -> str:
    """Return the friendly section heading for an object ``kind``."""
    return _KIND_LABELS.get(kind, kind.title())


def run_evaluation(
    inputs: EvaluationInputs,
    *,
    introspector_factory: IntrospectorFactory = _default_introspector_factory,
    assessor: Optional[CompatibilityAssessor] = None,
    target_browser_factory: TargetBrowserFactory = _default_target_browser_factory,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> EvaluationResult:
    """Introspect source and target, then assess compatibility (SCT-style).

    Source introspection and target catalog browsing are both read-only
    (Property 1). The deterministic compatibility report classifies every
    inventory object and estimates manual effort (Property 8 / Requirement 2),
    and the target inventory is compared against the source to flag pre-existing
    objects (Requirement 10.3). No AI is invoked here: AI assistance is offered
    on demand, per object, from the Evaluation screen's AI drawer (Requirement
    11.1, 11.2), so the run stays deterministic and incurs no Bedrock cost. The
    collaborators are injectable so this orchestration can be unit tested without
    a real database or AWS. ``progress_cb`` is an optional ``(percent, message)``
    reporter invoked at each phase so the UI can show coarse progress; it
    defaults to a no-op.
    """
    report = progress_cb or (lambda _pct, _msg: None)

    report(5, "Introspecting source schema...")
    introspector = introspector_factory(inputs.source_password)
    inventory = introspector.introspect(inputs.source_config)

    report(35, "Assessing source/target compatibility...")
    # The default rule set is source-engine-specific (MySQL vs PostgreSQL); pass the
    # source type so a PostgreSQL source is assessed with its own rules.
    assessment = (
        assessor
        or CompatibilityAssessor(source_type=inputs.source_config.source_type)
    ).assess(inventory)

    report(60, "Introspecting target Aurora DSQL catalog...")
    browser = target_browser_factory(inputs.aws_profile)
    target_inventory = browser.browse(inputs.target_config)

    report(85, "Detecting objects that already exist on the target...")
    target_conflicts = _find_target_conflicts(inventory, target_inventory)

    report(100, "Finalizing the assessment report...")
    return EvaluationResult(
        inventory=inventory,
        assessment=assessment,
        target_inventory=target_inventory,
        target_conflicts=target_conflicts,
    )


def job_status_to_step_status(job_status: str) -> Optional[StepStatus]:
    """Map a :class:`JobManager` job status to the Evaluation step status.

    Returns ``DONE``/``FAILED`` for terminal job states and ``None`` while the
    job is still ``PENDING``/``RUNNING`` (the step stays ``IN_PROGRESS``).
    """
    if job_status == "DONE":
        return StepStatus.DONE
    if job_status == "FAILED":
        return StepStatus.FAILED
    return None


def reconcile_evaluation_step(
    status: StepStatus, *, job_alive: bool, has_result: bool
) -> Optional[StepStatus]:
    """Reconcile a possibly-interrupted Evaluation step against the live job.

    An app restart restores the workflow status (so the step can read ``IN_PROGRESS``)
    but the background job that was running it does NOT survive -- a fresh process has
    no live job for it (``eval_state.job_id`` is not persisted). The poll timer bails
    when there is no job, so the "Starting evaluation... 0%" panel would spin forever.

    Returns ``NOT_STARTED`` when the step is ``IN_PROGRESS`` but there is neither a
    live job nor a result (the interrupted case) so the spinner stops and the "Run
    evaluation" affordance returns; otherwise ``None`` (leave the status unchanged --
    a genuinely running job keeps spinning, a finished one keeps its DONE/FAILED).
    Pure + deterministic so the reconciliation is unit-testable.
    """
    if status is StepStatus.IN_PROGRESS and not job_alive and not has_result:
        return StepStatus.NOT_STARTED
    return None


def _eval_job_alive(job_manager: JobManager, job_id: Optional[str]) -> bool:
    """True when the evaluation job still exists in this process (any state).

    ``False`` when there is no job id (a fresh process after a restart never restores
    it) or the job is unknown to the current :class:`JobManager` -- either way there is
    nothing to poll, so the step was interrupted."""
    if job_id is None:
        return False
    try:
        job_manager.get_status(job_id)
        return True
    except JobNotFoundError:
        return False


# ---------------------------------------------------------------------------
# Report export serialization (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReportDownload:
    """A serialized report ready to be downloaded: name, content, media type."""

    filename: str
    content: str
    media_type: str


_MEDIA_TYPES: dict[str, str] = {
    "json": "application/json",
    "text": "text/plain",
    "html": "text/html",
}
_EXTENSIONS: dict[str, str] = {"json": "json", "text": "txt", "html": "html"}


def _download_parts(stem: str, fmt: str) -> tuple[str, str]:
    """Return the ``(filename, media_type)`` for ``stem`` in ``fmt``."""
    normalized = fmt.lower()
    if normalized not in _MEDIA_TYPES:
        raise ValueError(f"unsupported report format: {fmt!r} (use 'json' or 'text')")
    return f"{stem}.{_EXTENSIONS[normalized]}", _MEDIA_TYPES[normalized]


def assessment_download(result: EvaluationResult, fmt: str = "json") -> ReportDownload:
    """Serialize the compatibility assessment report for download (Req 8.4).

    For HTML, the Target analysis (Aurora DSQL catalog + name conflicts) is
    appended so the exported report mirrors what the screen shows; JSON/text
    export the deterministic assessment only. No AI section is included in any
    format: AI assistance is on-demand and per-object (the Evaluation screen's AI
    drawer), so it is not part of the exported, shareable report.
    """
    if fmt.lower() == "html":
        content = render_html_report(
            result.assessment,
            target=result.target_inventory,
            conflicts=result.target_conflicts,
        )
    else:
        content = export_assessment_report(result.assessment, fmt)
    filename, media_type = _download_parts("compatibility_assessment", fmt)
    return ReportDownload(filename=filename, content=content, media_type=media_type)


# ---------------------------------------------------------------------------
# Per-session evaluation state
# ---------------------------------------------------------------------------


class EvaluationState:
    """Per-session evaluation outputs and the running job id.

    ``result``/``error`` are produced by a background worker and read by the UI
    poller, so they are guarded by a lock to make the cross-thread handoff safe.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.job_id: Optional[str] = None
        # Filters for the "Objects by importance" list (UI-thread only), split
        # along the two color-coded categories the summary already shows so the
        # controls read as "narrow by Classification and/or Estimated manual
        # effort" rather than one list mixing a derived "needs attention" bucket
        # with per-classification values. Each is "ALL" or an enum value; they
        # combine (AND). Lets the user focus on actionable objects at scale.
        self.classification_filter: str = "ALL"
        self.effort_filter: str = "ALL"
        self._result: Optional[EvaluationResult] = None
        self._error: Optional[str] = None
        # Coarse progress for the in-progress UI, updated by the background
        # worker and read by the poll timer (guarded by the same lock).
        self._progress_pct: int = 0
        self._progress_message: str = ""

    def set_progress(self, percent: int, message: str) -> None:
        """Record coarse run progress (percent 0-100 and a phase message)."""
        with self._lock:
            self._progress_pct = max(0, min(100, int(percent)))
            self._progress_message = message

    @property
    def progress_pct(self) -> int:
        """Return the last reported progress percent (0-100)."""
        with self._lock:
            return self._progress_pct

    @property
    def progress_message(self) -> str:
        """Return the last reported progress phase message."""
        with self._lock:
            return self._progress_message

    def set_result(self, result: EvaluationResult) -> None:
        """Record a successful run's result (clears any prior error)."""
        with self._lock:
            self._result = result
            self._error = None

    def set_error(self, message: str) -> None:
        """Record a failure message for display."""
        with self._lock:
            self._error = message

    @property
    def result(self) -> Optional[EvaluationResult]:
        """Return the last successful result, if any."""
        with self._lock:
            return self._result

    @property
    def error(self) -> Optional[str]:
        """Return the last failure message, if any."""
        with self._lock:
            return self._error

    def clear_outputs(self) -> None:
        """Discard the previous result/error before a (re-)run."""
        with self._lock:
            self._result = None
            self._error = None
            self._progress_pct = 0
            self._progress_message = ""


@dataclass
class EvaluationStore:
    """Process-memory map of session id to :class:`EvaluationState`.

    Mirrors :class:`~dsql_migrator.ui.session.SessionStore` so each UI session
    sees only its own evaluation state; nothing is persisted to disk.
    """

    _states: dict[str, EvaluationState] = field(default_factory=dict)

    def get_or_create(self, session_id: str) -> EvaluationState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = EvaluationState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[EvaluationState]:
        """Return the state for ``session_id``, or ``None`` if absent."""
        return self._states.get(session_id)

    def clear(self, session_id: Optional[str]) -> None:
        """Remove the state for ``session_id`` (no-op if absent)."""
        if session_id is None:
            return
        self._states.pop(session_id, None)

    def reset_in_place(self, session_id: Optional[str]) -> None:
        """Reset the state WITHOUT replacing the object (no-op if absent).

        The workflow screen captures this state object in its builder closures at
        build time, so popping + recreating would orphan the captured reference
        (the screen keeps rendering the old, now-detached state). Re-initialising
        the SAME instance keeps every closure pointing at the live, wiped object.
        """
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            state.__init__()  # type: ignore[misc]  # re-run init on the same object


# ---------------------------------------------------------------------------
# NiceGUI screen
# ---------------------------------------------------------------------------

# How often the screen polls the background job (seconds).
_POLL_INTERVAL_SECONDS = 0.5


def build_evaluation_screen(
    store: SessionStore,
    session_id: str,
    *,
    job_manager: JobManager,
    eval_store: EvaluationStore,
    introspector_factory: IntrospectorFactory = _default_introspector_factory,
    assessor: Optional[CompatibilityAssessor] = None,
    target_browser_factory: TargetBrowserFactory = _default_target_browser_factory,
    strategist_factory: StrategistFactory = _default_strategist_factory,
    open_ai_scope: Optional[Callable[..., object]] = None,
    ai_post_event: Optional[Callable[..., object]] = None,
    ai_tool_execute: Optional[Callable[[str, dict], str]] = None,
    ai_tools: Optional[list] = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Evaluation screen, returning ``(content_builder, runner)``.

    ``content_builder`` renders the screen and is given the workflow shell's
    refresh callback so it can reflect background-job completion. ``runner`` is
    invoked by the step's Run/Re-run button: it requires both source and target
    connections, marks the step ``IN_PROGRESS``, and submits the evaluation to
    ``job_manager`` (returning immediately so the UI never blocks). Both are
    wired into :func:`~dsql_migrator.ui.workflow.build_workflow_sidebar` via its
    ``step_content``/``runners`` hooks.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    eval_state = eval_store.get_or_create(session_id)

    def runner() -> None:
        if not session.has_source():
            eval_state.set_error(
                "Configure and test the source connection first, then run the "
                "evaluation."
            )
            return
        if not session.has_target():
            eval_state.set_error(
                "Configure and test the target connection first, then run the "
                "evaluation."
            )
            return

        source_config = session.source_config
        target_config = session.target_config
        assert source_config is not None  # guaranteed by has_source()
        assert target_config is not None  # guaranteed by has_target()
        inputs = EvaluationInputs(
            source_config=source_config,
            source_password=session.source_password,
            target_config=target_config,
            aws_profile=session.aws_profile,
        )
        eval_state.clear_outputs()
        session.set_workflow(
            with_status(session.workflow, WorkflowStep.EVALUATION, StepStatus.IN_PROGRESS)
        )

        def work(_handle: object) -> None:
            log_activity(
                ActivityCategory.ASSESSMENT,
                "run assessment",
                status=ActivityStatus.STARTED,
                detail="assessing the source schema against Aurora DSQL",
            )
            if ai_post_event is not None:
                ai_post_event(
                    text="Started compatibility assessment of the source against Aurora DSQL",
                    status="started",
                )
            try:
                result = run_evaluation(
                    inputs,
                    introspector_factory=introspector_factory,
                    assessor=assessor,
                    target_browser_factory=target_browser_factory,
                    progress_cb=eval_state.set_progress,
                )
            except Exception as exc:  # noqa: BLE001 - re-raised; job marks FAILED
                log_activity(
                    ActivityCategory.ASSESSMENT,
                    "run assessment",
                    status=ActivityStatus.FAILURE,
                    detail=f"{type(exc).__name__}: {exc}",
                    exc=exc,
                )
                if ai_post_event is not None:
                    # Keep the event lean: the type is the useful signal; the full
                    # exception message can be long/verbose (and is already in the
                    # activity log), so it is not sent into the AI feed/grounding.
                    ai_post_event(
                        text=f"Compatibility assessment failed ({type(exc).__name__})",
                        status="error",
                    )
                raise
            eval_state.set_result(result)
            report = result.assessment
            counts = ", ".join(f"{k.value}={v}" for k, v in report.summary.items())
            conflicts = len(result.target_conflicts or [])
            log_activity(
                ActivityCategory.ASSESSMENT,
                "run assessment",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{len(report.items)} object(s) assessed ({counts}); "
                    f"{conflicts} target conflict(s)"
                ),
            )
            if ai_post_event is not None:
                # Friendly, short text (also the AI's grounding line) + structured data
                # so the panel renders a VISUAL breakdown bar instead of a wall of text.
                _sm = {k.value: v for k, v in report.summary.items()}
                _auto = _sm.get("AUTO", 0)
                _man = _sm.get("MANUAL", 0)
                _uns = _sm.get("UNSUPPORTED", 0)
                ai_post_event(
                    text=(
                        f"Assessment complete: {len(report.items)} objects — "
                        f"{_auto} automatic, {_man} need review, {_uns} unsupported"
                        + (f"; {conflicts} already on target" if conflicts else "")
                    ),
                    status="success",
                    kind="assessment",
                    data={
                        "total": len(report.items),
                        "auto": _auto,
                        "manual": _man,
                        "unsupported": _uns,
                        "conflicts": conflicts,
                    },
                )

        eval_state.job_id = job_manager.submit(work)

    def content(refresh: Callable[[], None]) -> None:
        status = get_status(session.workflow, WorkflowStep.EVALUATION)
        # Reconcile a stale IN_PROGRESS step whose job did not survive an app restart
        # (the poll would otherwise spin at 0% with no terminal state). Flip it back to
        # NOT_STARTED so the spinner stops and the "Run evaluation" button returns, and
        # note the interruption so the reset is not confusing.
        interrupted = False
        reconciled = reconcile_evaluation_step(
            status,
            job_alive=_eval_job_alive(job_manager, eval_state.job_id),
            has_result=eval_state.result is not None,
        )
        if reconciled is not None:
            session.set_workflow(
                with_status(session.workflow, WorkflowStep.EVALUATION, reconciled)
            )
            status = reconciled
            interrupted = True

        def run_now() -> None:
            # In-screen Run so the action is discoverable on the empty screen
            # (not only via the workflow shell's Run button); shares the runner
            # and refreshes so the progress panel/poll-timer appear inline.
            runner()
            refresh()

        with ui.column().classes("w-full gap-3"):
            # (No standalone intro banner here: the journey header names the step, the
            # "Ready to evaluate" empty-state card below explains it before the first
            # run, and after a run the assessment report itself is on screen -- a
            # persistent paragraph restating all that was redundant.)
            if interrupted:
                render_notice(
                    ui,
                    tone="info",
                    header="Evaluation was interrupted by a restart",
                    body=(
                        "The app restarted while the evaluation was running, so it "
                        "stopped — nothing was changed (this step only reads the "
                        "databases). Run it again to continue."
                    ),
                )

            if not session.has_source() or not session.has_target():
                render_notice(
                    ui,
                    tone="warning",
                    header="Source and target required",
                    body=(
                        "Both a source and a target connection are required. Set "
                        "them up in the Connect section above."
                    ),
                )

            # Empty-state call-to-action: before the first run, give an on-screen
            # Run button (the workflow shell's Run is easy to miss) and reassure
            # that this step is read-only.
            if status is StepStatus.NOT_STARTED and eval_state.result is None:
                with ui.card().classes("w-full bg-gray-50"):
                    ui.label("Ready to evaluate").classes("text-md font-semibold")
                    ui.label(
                        "This step only reads the source and target (no changes "
                        "are made), classifies every object (Automatic / Review "
                        "needed / Unsupported), and estimates the manual effort."
                    ).classes("text-sm text-gray-600")
                    run_button = ui.button(
                        "Run evaluation", on_click=run_now
                    ).props("color=primary")
                    if not (session.has_source() and session.has_target()):
                        run_button.disable()
                        run_button.tooltip(
                            "Configure and test the source and target "
                            "connections first."
                        )

            error = eval_state.error
            if error and status is not StepStatus.IN_PROGRESS:
                render_notice(
                    ui,
                    tone="error",
                    header="Evaluation failed",
                    body=error,
                )

            if status is StepStatus.IN_PROGRESS:
                # Distinct, colored progress panel with a phase message and a
                # coarse percentage so the user sees what stage the run is in.
                with ui.card().classes(
                    "w-full bg-blue-50 border border-blue-200"
                ):
                    with ui.row().classes("items-center gap-2 w-full no-wrap"):
                        ui.spinner(size="sm", color="primary")
                        progress_label = ui.label(
                            eval_state.progress_message or "Starting evaluation..."
                        ).classes("text-sm text-blue-800 font-medium")
                        ui.space()
                        percent_label = ui.label(
                            f"{eval_state.progress_pct}%"
                        ).classes("text-sm text-blue-800 font-semibold")
                    progress_bar = (
                        ui.linear_progress(
                            value=eval_state.progress_pct / 100, show_value=False
                        )
                        .props("color=primary")
                        .classes("w-full")
                    )
                _install_poll_timer(
                    ui,
                    job_manager,
                    session,
                    eval_state,
                    refresh,
                    progress_label=progress_label,
                    percent_label=percent_label,
                    progress_bar=progress_bar,
                )

            result = eval_state.result
            if result is not None:
                # On-demand per-object AI guidance is offered only when AI assist
                # is enabled for this session (opt-in). The strategist is built
                # here (no network until invoked) and its graceful per-object
                # guidance call is handed to the result renderer; when AI is off
                # the provider is None and the renderer shows a disabled, clearly
                # labeled affordance instead (discoverable, never silently gone).
                guidance_provider = None
                if session.ai_assist.enabled:
                    strategist = strategist_factory(
                        session.ai_assist, session.aws_profile
                    )
                    if ai_tool_execute is not None:
                        # Give the per-object/finding chat the SAME read-only tools the
                        # general chat has, so a chat started on ONE object can still
                        # answer WIDER-migration questions (e.g. "list all unsupported
                        # objects") by looking up the real data instead of refusing.
                        def guidance_provider(  # noqa: ANN001
                            item, messages, on_delta, _s=strategist
                        ):
                            return _s.stream_object_chat(
                                item, messages, on_delta,
                                tools=ai_tools or [], execute=ai_tool_execute,
                            )
                    else:
                        guidance_provider = strategist.stream_object_chat
                _render_result(
                    ui,
                    result,
                    eval_state=eval_state,
                    refresh=refresh,
                    guidance_provider=guidance_provider,
                    open_ai_scope=open_ai_scope,
                )

    return content, runner


def _install_poll_timer(
    ui: object,
    job_manager: JobManager,
    session: object,
    eval_state: EvaluationState,
    refresh: Callable[[], None],
    *,
    progress_label: object = None,
    percent_label: object = None,
    progress_bar: object = None,
) -> None:
    """Poll the running job and finalize the step status when it completes.

    The timer lives only while the step is ``IN_PROGRESS``: each tick it updates
    the optional progress widgets (phase message, percent, bar) from the
    background worker's reported progress, and once the job reaches a terminal
    state the status is updated (so the next render shows ``DONE``/``FAILED`` and
    creates no new timer) and the shell is refreshed.
    """
    job_id = eval_state.job_id
    if job_id is None:
        return

    def _update_progress() -> None:
        pct = eval_state.progress_pct
        message = eval_state.progress_message or "Working..."
        if (
            progress_label is not None
            and not progress_label.is_deleted  # type: ignore[attr-defined]
        ):
            progress_label.set_text(message)  # type: ignore[attr-defined]
        if (
            percent_label is not None
            and not percent_label.is_deleted  # type: ignore[attr-defined]
        ):
            percent_label.set_text(f"{pct}%")  # type: ignore[attr-defined]
        if (
            progress_bar is not None
            and not progress_bar.is_deleted  # type: ignore[attr-defined]
        ):
            progress_bar.set_value(pct / 100)  # type: ignore[attr-defined]

    def poll() -> None:
        _update_progress()
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(job.status)
        if mapped is None:
            return
        if mapped is StepStatus.FAILED:
            eval_state.set_error(
                job_manager.get_error(job_id) or "Evaluation failed."
            )
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(session.workflow, WorkflowStep.EVALUATION, mapped)  # type: ignore[attr-defined]
        )
        refresh()

    ui.timer(_POLL_INTERVAL_SECONDS, poll)  # type: ignore[attr-defined]


def _render_result(
    ui: object,
    result: EvaluationResult,
    *,
    eval_state: Optional["EvaluationState"] = None,
    refresh: Optional[Callable[[], None]] = None,
    guidance_provider: Optional[
        Callable[
            ["AssessmentItem", "list[dict[str, str]]", Callable[[str], None]],
            ObjectGuidanceOutcome,
        ]
    ] = None,
    open_ai_scope: Optional[Callable[..., object]] = None,
) -> None:
    """Render the result as distinct, readable sections (one card per group).

    A migration-readiness score card leads, followed by the compatibility
    assessment card (source counts, conversion stats, and the importance-ordered
    object list, with the target name-conflict analysis as a subsection because
    it is part of the assessment report) and the export actions. There is no
    batch AI section: when AI assist is enabled, ``guidance_provider`` is a
    graceful per-object guidance call wired to a right slide-in AI drawer opened
    on demand from each actionable object; when it is ``None`` the affordance
    renders disabled with a hint instead. ``eval_state``/``refresh`` drive the
    assessment list filter (and are omitted only by non-UI callers).
    """
    inventory = result.inventory
    ai_enabled = guidance_provider is not None
    # Build the right slide-in AI guidance drawer once for this render; ``on_ai``
    # opens it for a given object. None when AI assist is disabled (the item
    # renderer then shows a disabled, clearly labeled affordance instead).
    on_ai = (
        _build_guidance_drawer(guidance_provider, open_ai_scope=open_ai_scope)
        if guidance_provider is not None
        else None
    )

    # Migration-readiness score (its own leading card; skipped for an empty
    # report, which has nothing to score).
    _render_score_card(ui, result.assessment)

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        # Title on the left, export buttons on the top-right so the user can
        # export without scrolling past a possibly long objects list.
        with ui.row().classes("items-center justify-between w-full no-wrap"):  # type: ignore[attr-defined]
            ui.label("Compatibility assessment report").classes(  # type: ignore[attr-defined]
                "text-lg font-semibold"
            )
            _render_downloads(ui, result)
        # Source object counts (moved here from a separate "Source inventory"
        # section): a compact per-kind tally above the assessed object list.
        with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
            for label, count in source_inventory_tallies(inventory):
                with ui.row().classes(  # type: ignore[attr-defined]
                    "items-baseline gap-1 rounded bg-gray-100 px-3 py-1"
                ):
                    ui.label(str(count)).classes("text-base font-semibold")  # type: ignore[attr-defined]
                    ui.label(label).classes("text-sm text-gray-600")  # type: ignore[attr-defined]
        _render_assessment(
            ui,
            result.assessment,
            render_title=False,
            eval_state=eval_state,
            refresh=refresh,
            on_ai=on_ai,
            ai_enabled=ai_enabled,
        )
        # Target name-conflict detection is part of the assessment (Req: the
        # report includes target name-conflict detection), so the target analysis
        # is a subsection of this card rather than a separate top-level card.
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
        _render_target(ui, result)


def _render_score_card(ui: object, report: AssessmentReport) -> None:
    """Render the migration-readiness score as a leading gauge card.

    Shows a 0-100 circular gauge (colored by the score's tone), the band label,
    the evidence counts the score is built from (total / automatic / review
    needed / unsupported), and a tone-colored one-line notice explaining the
    band. Nothing is rendered for an empty report (no objects to score).
    """
    score = compute_migration_score(report)
    if score is None:
        return
    color = _SCORE_GAUGE_COLOR.get(score.tone, "primary")
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        section_header(ui, icon="speed", title="Migration readiness")
        with ui.row().classes("items-center gap-4 w-full no-wrap"):  # type: ignore[attr-defined]
            ui.circular_progress(  # type: ignore[attr-defined]
                value=score.score, min=0, max=100, size="86px", color=color,
                show_value=True,
            ).props("thickness=0.22 track-color=grey-3")
            with ui.column().classes("gap-1 flex-1 min-w-0"):  # type: ignore[attr-defined]
                with ui.row().classes("items-center gap-2"):  # type: ignore[attr-defined]
                    ui.badge(score.band).props(f"color={color}")  # type: ignore[attr-defined]
                    ui.label(f"{score.score} / 100 readiness").classes(  # type: ignore[attr-defined]
                        "text-sm text-gray-500"
                    )
                with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
                    for lbl, cnt in (
                        ("Total", score.total),
                        ("Automatic", score.auto),
                        ("Review needed", score.manual),
                        ("Unsupported", score.unsupported),
                    ):
                        with ui.row().classes(  # type: ignore[attr-defined]
                            "items-baseline gap-1 rounded bg-gray-100 px-2 py-0.5"
                        ):
                            ui.label(str(cnt)).classes(  # type: ignore[attr-defined]
                                "text-sm font-semibold"
                            )
                            ui.label(lbl).classes("text-xs text-gray-600")  # type: ignore[attr-defined]
        render_notice(ui, tone=score.tone, header=score.band, body=score.summary)


def _guidance_question(item: "AssessmentItem") -> str:
    """Phrase the on-demand guidance request as a short, natural chat question."""
    effort = (
        f" (estimated effort: {item.effort.value})" if item.effort is not None else ""
    )
    return (
        f"How should I handle {item.object_name} ({item.kind}) when migrating to "
        f"Amazon Aurora DSQL? It's flagged "
        f"{classification_label(item.classification.value)} by rule "
        f"{item.rule_id}{effort}."
    )


def _concern_question(item: "AssessmentItem", concern: object) -> str:
    """Phrase a guidance request about ONE specific finding on an object."""
    rule = getattr(concern, "rule_id", "") or item.rule_id
    risk = (getattr(concern, "risk", "") or "").strip()
    detail = f' The concern: "{risk}".' if risk else ""
    return (
        f"On {item.object_name} ({item.kind}), migrating to Amazon Aurora DSQL, help "
        f"me with the {rule} finding.{detail} What exactly should I do about it?"
    )


def _build_guidance_drawer(
    guidance_provider: Callable[
        ["AssessmentItem", "list[dict[str, str]]", Callable[[str], None]],
        ObjectGuidanceOutcome,
    ],
    *,
    open_ai_scope: Optional[Callable[..., object]] = None,
) -> Optional[Callable[..., object]]:
    """Return an opener that deep-links one object's AI guidance into the panel.

    Guidance opens in the persistent app-wide AI panel
    (:mod:`dsql_migrator.ui.ai_panel`) so the conversation lives alongside the whole
    journey; the streamer is bound to ``guidance_provider`` (the strategist's grounded
    streaming chat for the object). Returns ``None`` when the panel is not wired
    (``open_ai_scope is None`` -- e.g. a unit test that does not build the shell), so
    the caller shows the disabled affordance.
    """
    if open_ai_scope is None:
        return None

    def open_guidance(item: "AssessmentItem", concern: object = None) -> None:
        # A per-FINDING icon passes its concern -> a distinct scope + a question about
        # that specific finding; the object-level fallback (concern None) keeps the
        # whole-object question. Either way the streamer grounds on the whole item.
        rule = getattr(concern, "rule_id", None)
        if concern is not None and rule:
            scope_id = f"evaluation:{item.object_name}:{rule}"
            subtitle = f"{item.object_name} \u00b7 {rule}"
            seed = _concern_question(item, concern)
        else:
            scope_id = f"evaluation:{item.object_name}"
            subtitle = f"{item.object_name} \u00b7 {item.kind}"
            seed = _guidance_question(item)
        open_ai_scope(
            scope_id=scope_id,
            title="AI DBA",
            subtitle=subtitle,
            chip=f"Evaluation \u00b7 {item.object_name}",
            seed_question=seed,
            streamer=lambda messages, on_delta: guidance_provider(
                item, messages, on_delta
            ),
        )

    return open_guidance


def _render_assessment_chart(ui: object, report: AssessmentReport) -> None:
    """Render a stacked horizontal bar chart of per-kind compatibility.

    Per object kind, the bar stacks the three classifications -- Auto-converted /
    Review needed / Unsupported -- in that order, so it reads left-to-right from
    "moves by itself" to "cannot move" and uses the same three words as the
    classification badges above it and the table below. Nothing is rendered for an
    empty report.
    """
    data = build_assessment_chart_data(report)
    if not data.kinds:
        return

    # Reuse the canonical classification order/labels/colors so the UI chart, the HTML
    # export and the classification badges all agree (source of truth in core.assessor).
    from dsql_migrator.core.assessor import (
        _CHART_CLASS_COLORS,
        _CHART_ORDER,
        _CLASS_CHART_LABELS,
    )

    counts_by_class = {
        Classification.AUTO: data.auto,
        Classification.MANUAL: data.manual,
        Classification.UNSUPPORTED: data.unsupported,
    }
    labels = [_CLASS_CHART_LABELS[cls] for cls in _CHART_ORDER]
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        # Legend at the top of the chart; the grid drops below it so the bars
        # never overlap the legend.
        "legend": {"top": 0, "data": labels},
        "grid": {"left": 110, "right": 30, "top": 40, "bottom": 30},
        "xAxis": {"type": "value", "minInterval": 1},
        # Friendly labels, matching the list headings below and the HTML export's chart --
        # the axis used to show the raw enum ("PROCEDURE") beside a heading reading
        # "Stored procedures".
        "yAxis": {
            "type": "category",
            "data": [kind_section_label(kind) for kind in data.kinds],
            "inverse": True,
        },
        "series": [
            {
                "name": _CLASS_CHART_LABELS[cls],
                "type": "bar",
                "stack": "total",
                "itemStyle": {"color": _CHART_CLASS_COLORS[cls]},
                "data": counts_by_class[cls],
            }
            for cls in _CHART_ORDER
        ],
    }
    ui.label("Compatibility by object kind").classes("text-md font-semibold")  # type: ignore[attr-defined]
    ui.echart(option).classes("w-full").style("height: 320px")  # type: ignore[attr-defined]


def _render_assessment(
    ui: object,
    report: AssessmentReport,
    *,
    render_title: bool = True,
    eval_state: Optional["EvaluationState"] = None,
    refresh: Optional[Callable[[], None]] = None,
    on_ai: Optional[Callable[["AssessmentItem"], object]] = None,
    ai_enabled: bool = False,
) -> None:
    """Render the assessment chart, summary, and an importance-ordered list.

    Per-object detail is shown as an expandable accordion (not a table) so long
    Risk/Recommendation text is never truncated, ordered most-critical-first and
    showing each object's kind and classification. A filter (All / Needs
    attention / per classification) lets the user focus on actionable objects in
    a large schema; it is driven by ``eval_state``/``refresh`` and omitted when
    those are not provided. ``render_title`` is False when the caller already
    shows the section title in a header (e.g. alongside the export buttons).
    ``on_ai``/``ai_enabled`` wire each actionable object's on-demand AI guidance
    (opening the fixed right drawer); when AI is off the affordance renders
    disabled with a hint.
    """
    if render_title:
        ui.label("Compatibility assessment report").classes(  # type: ignore[attr-defined]
            "text-lg font-semibold"
        )

    # Classification counts as impactful, color-coded badges so the at-a-glance numbers
    # stand out instead of reading as plain gray text. These sit here, above the chart,
    # because the chart splits by exactly these three categories -- header and picture
    # answer the same question in the same words.
    #
    # The effort summary deliberately does NOT sit here. It used to, and that put a row
    # the chart says nothing about directly beside one the chart is built from; worse, the
    # two rows looked identical but did not add up to the same total (effort counts only
    # objects that need work, so an advice-only or AUTO object appears in neither bucket),
    # which read as missing objects rather than as a different question. Effort is a tool
    # for working the object list, so it now sits with that list and its filters below.
    with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
        ui.label("Classification").classes(  # type: ignore[attr-defined]
            "text-sm font-semibold text-gray-700"
        )
        for classification, count in report.summary.items():
            color = _CLASS_BADGE_COLOR.get(classification.value, "grey")
            ui.badge(  # type: ignore[attr-defined]
                f"{classification_label(classification.value)}: {count}"
            ).props(f"color={color}").classes("text-sm q-px-sm q-py-xs")

    # Wrap the chart in solid separators so "Compatibility by object kind" reads as
    # a distinct section between the summary badges above and the
    # per-object list below. Only when there is a chart to show (non-empty
    # report), so an empty report does not render stray rules.
    if report.items:
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
        _render_assessment_chart(ui, report)
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
    else:
        _render_assessment_chart(ui, report)

    # Per-object detail as an expandable list (accordion), ordered by importance
    # so the most critical objects are at the top. Two AWS-style dropdown filters
    # (Classification / Estimated manual effort — the same color-coded categories
    # the summary badges show) let the user focus on actionable objects in a large
    # schema instead of scrolling past every AUTO object.
    if not report.items:
        ui.label("Objects by importance (most critical first)").classes(  # type: ignore[attr-defined]
            "text-md font-semibold"
        )
        ui.label("No objects were assessed.").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
        return

    class_mode = eval_state.classification_filter if eval_state is not None else "ALL"
    effort_mode = eval_state.effort_filter if eval_state is not None else "ALL"
    ordered = sort_assessment_items(report.items)
    visible = filter_assessment_items(
        ordered, classification=class_mode, effort=effort_mode
    )

    ui.label(  # type: ignore[attr-defined]
        f"Objects by importance — showing {len(visible)} of {len(report.items)}"
    ).classes("text-md font-semibold")
    # Effort distribution, beside the list and the effort filter it describes. The count
    # is spelled out as "objects needing work" because it is NOT the object total: an
    # object with no required work (all-AUTO, or only a recommendation) carries no effort
    # estimate and so appears in none of these buckets.
    if any(item.effort is not None for item in report.items):
        with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
            ui.label("Estimated manual effort").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-700"
            )
            for level, count in report.effort_summary.items():
                ui.badge(f"{level.value}: {count}").props(  # type: ignore[attr-defined]
                    f"color={_EFFORT_BADGE_COLOR} outline"
                ).classes("text-sm q-px-sm q-py-xs")
            needing = sum(report.effort_summary.values())
            ui.label(  # type: ignore[attr-defined]
                f"({needing} of {len(report.items)} objects need work)"
            ).classes("text-xs text-gray-500")
    if eval_state is not None and refresh is not None:

        def on_class(event: object) -> None:
            eval_state.classification_filter = getattr(event, "value", "ALL") or "ALL"
            refresh()

        def on_effort(event: object) -> None:
            eval_state.effort_filter = getattr(event, "value", "ALL") or "ALL"
            refresh()

        def on_clear() -> None:
            eval_state.classification_filter = "ALL"
            eval_state.effort_filter = "ALL"
            refresh()

        with filter_bar(ui):  # type: ignore[attr-defined]
            filter_select(  # type: ignore[attr-defined]
                ui,
                label="Classification",
                options={
                    "ALL": "All classifications",
                    "AUTO": classification_label("AUTO"),
                    "MANUAL": classification_label("MANUAL"),
                    "UNSUPPORTED": classification_label("UNSUPPORTED"),
                },
                value=class_mode,
                on_change=on_class,
            )
            filter_select(  # type: ignore[attr-defined]
                ui,
                label="Estimated manual effort",
                options={
                    "ALL": "All efforts",
                    "SIMPLE": "Simple (< 2h)",
                    "MEDIUM": "Medium (2–6h)",
                    "SIGNIFICANT": "Significant (> 6h)",
                },
                value=effort_mode,
                on_change=on_effort,
            )
            if class_mode != "ALL" or effort_mode != "ALL":
                ui.button("Clear filters", on_click=on_clear).props(  # type: ignore[attr-defined]
                    "flat dense no-caps color=primary"
                )

    if not visible:
        ui.label(  # type: ignore[attr-defined]
            "No objects match these filters."
        ).classes("text-sm text-gray-500")
        return

    # Render the full list inline (the page scrolls) -- no inner scroll area, so
    # there is no nested-scroll trap and the browser's find works across every
    # item. Objects are grouped by kind; a group is expanded by default only when
    # it has actionable (UNSUPPORTED/MANUAL) items, so a large all-AUTO group
    # (common at scale) stays collapsed with its counts visible in the header.
    with ui.column().classes("w-full gap-1"):  # type: ignore[attr-defined]
        for kind, kind_items in group_assessment_items_by_kind(visible):
            actionable = any(
                item.classification.value != "AUTO" for item in kind_items
            )
            with ui.expansion(value=actionable).classes("w-full").props(  # type: ignore[attr-defined]
                "expand-separator"
            ) as kind_exp:
                with kind_exp.add_slot("header"):
                    with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                        ui.label(kind_section_label(kind)).classes(  # type: ignore[attr-defined]
                            "text-sm font-semibold"
                        )
                        ui.badge(str(len(kind_items))).props("color=primary")  # type: ignore[attr-defined]
                        ui.label(assessment_kind_summary(kind_items)).classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-500"
                        )
                with ui.column().classes("gap-1 w-full pl-2"):  # type: ignore[attr-defined]
                    for item in kind_items:
                        _render_assessment_item(
                            ui, item, on_ai=on_ai, ai_enabled=ai_enabled
                        )


def _render_concern_line(
    ui: object,
    *,
    icon: str,
    label: str,
    text: str,
    icon_class: str,
    label_class: str,
    panel_class: str = "",
) -> None:
    """Render one labeled line of a concern card -- its risk or its recommendation.

    A leading glyph + a small uppercase caption name the line ("RISK" / "RECOMMENDATION")
    so the reader never has to infer which is which, and the caption sits ABOVE the text
    rather than inline so a long sentence wraps flush instead of indenting under the
    label. ``panel_class`` optionally tints the block, which is what separates the
    recommendation from the risk at a glance.
    """
    container = ui.column().classes(  # type: ignore[attr-defined]
        f"gap-0.5 w-full {panel_class}".strip()
    )
    with container:
        with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
            ui.icon(icon).classes(f"text-sm {icon_class}")  # type: ignore[attr-defined]
            ui.label(label).classes(  # type: ignore[attr-defined]
                f"text-xs font-semibold uppercase tracking-wide {label_class}"
            )
        ui.label(text).classes("text-sm text-gray-900")  # type: ignore[attr-defined]


def _render_assessment_item(
    ui: object,
    item: "AssessmentItem",
    *,
    on_ai: Optional[Callable[["AssessmentItem"], object]] = None,
    ai_enabled: bool = False,
) -> None:
    """Render one assessed object as an expandable row (header + detail body).

    For an object that needs attention (not AUTO), the header row carries a compact
    on-demand AI-guidance ICON that opens the side panel for this object (``on_ai``),
    so guidance is one click away without expanding the row; ``click.stop`` keeps that
    click from toggling the expansion. When AI assist is off the icon is shown disabled
    with a hint, so the affordance is discoverable rather than silently missing.
    """
    is_auto = item.classification.value == "AUTO"
    color = _CLASS_BADGE_COLOR.get(item.classification.value, "grey")
    with ui.expansion().classes("w-full").props("expand-separator") as exp:  # type: ignore[attr-defined]
        with exp.add_slot("header"):
            with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label(item.object_name).classes("font-medium")  # type: ignore[attr-defined]
                ui.badge(item.kind).props("color=grey-6 outline")  # type: ignore[attr-defined]
                # ONE BADGE PER FINDINGS CATEGORY, not a single governing badge. That badge
                # named only the most severe classification and was silent about the rest:
                # measured on a real source, 16 of 18 tables carried a mix -- typically a
                # real gap plus the AUTO_INCREMENT recommendation -- behind one badge, and a
                # row reading "Unsupported" could hide six findings of which four were
                # merely review-needed and one was optional advice. The object looked wholly
                # blocked when most of it was not.
                #
                # Each badge keeps its label rather than showing a bare count, so severity
                # never rests on color alone (a colorblind reader, or a monochrome
                # screenshot, would otherwise need the chart legend to decode it). The
                # leading badge is the classification the old single badge showed, so the
                # row still reads worst-first at a glance.
                #
                # The per-object effort badge is deliberately NOT here. It described the
                # object as a whole while this row now summarises its findings, and each
                # finding carries its own effort inside -- which is the number to act on,
                # since one SIMPLE fix and one SIGNIFICANT one do not average. The effort
                # distribution for the whole schema stays in the summary above the list.
                counts = assessment_concern_counts(item)
                if counts:
                    for label, count, badge_color in counts:
                        ui.badge(f"{count} {label}").props(  # type: ignore[attr-defined]
                            f"color={badge_color}"
                        )
                else:
                    # No findings at all (a clean AUTO object): still say so, rather than
                    # leaving the row with nothing but a name.
                    ui.badge(  # type: ignore[attr-defined]
                        classification_label(item.classification.value)
                    ).props(f"color={color}")
        with ui.column().classes("gap-3 p-3 w-full"):  # type: ignore[attr-defined]
            # One block per matched rule, each pairing a risk with ITS OWN
            # recommendation. Previously every rule's text was semicolon-joined into a
            # single Risk paragraph and a single Recommendation paragraph, so a table
            # matching five rules (FK + AUTO_INCREMENT + CI collation + ENUM + ON UPDATE)
            # produced two run-on sentences and left the reader to guess which fix went
            # with which problem. `concerns` is empty only for a report persisted before
            # it existed, which falls back to the joined text below.
            concerns = list(getattr(item, "concerns", None) or [])
            if concerns:
                # Each concern is a bordered card indented behind ONE vertical spine, so
                # the expanded body reads as children of the row above it. Flat
                # separators (the previous treatment) gave no such cue: at two expanded
                # tables the findings ran together and it was not obvious which object a
                # given block belonged to. The spine + card is the same containment
                # idiom as the object tree on the Schema Conversion screen.
                with ui.column().classes(  # type: ignore[attr-defined]
                    "gap-2 w-full pl-4 ml-1 border-l-2 border-gray-200"
                ):
                    for concern in concerns:
                        # An ADVISORY finding is not a defect: the conversion is complete
                        # and correct and ignoring it costs throughput, not correctness.
                        # It therefore gets the calm info-blue treatment instead of the
                        # amber warning one, so a reader can tell at a glance which of an
                        # object's five findings they MUST act on. Same distinction
                        # Schema Conversion has drawn since v0.1.151.
                        advisory = getattr(concern, "is_advisory", False)
                        with ui.column().classes(  # type: ignore[attr-defined]
                            "gap-2 w-full rounded-md border p-3 "
                            + (
                                "border-sky-200 bg-sky-50"
                                if advisory
                                else "border-gray-200 bg-gray-50"
                            )
                        ):
                            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                                # Per-concern class: the item's badge shows only the
                                # governing (most severe) one, which says nothing about
                                # the others. An advisory finding says "RECOMMENDED"
                                # rather than repeating the object's severity, because
                                # its classification is about how much work the CHANGE
                                # would be, not about anything being wrong.
                                if advisory:
                                    ui.badge("RECOMMENDED").props(  # type: ignore[attr-defined]
                                        "color=info outline"
                                    )
                                else:
                                    ui.badge(  # type: ignore[attr-defined]
                                        classification_label(
                                            concern.classification.value
                                        )
                                    ).props(
                                        "color="
                                        + _CLASS_BADGE_COLOR.get(
                                            concern.classification.value, "grey"
                                        )
                                        + " outline"
                                    )
                                ui.label(concern.rule_id).classes(  # type: ignore[attr-defined]
                                    "text-xs text-gray-500 font-mono"
                                )
                                # Push the effort badge + per-finding AI icon to the
                                # right of the concern's header line.
                                ui.space()  # type: ignore[attr-defined]
                                if concern.effort is not None:
                                    # "if you take it" makes clear the estimate is the
                                    # cost of OPTING IN, not required migration work --
                                    # it is excluded from the object's effort for exactly
                                    # that reason.
                                    ui.badge(  # type: ignore[attr-defined]
                                        (
                                            f"effort if you take it: "
                                            f"{concern.effort.value}"
                                        )
                                        if advisory
                                        else f"effort: {concern.effort.value}"
                                    ).props(f"color={_EFFORT_BADGE_COLOR} outline")
                                # Per-FINDING AI guidance icon: ask AI about THIS
                                # specific finding on this object (one click).
                                _render_item_ai_action(
                                    ui, item, on_ai=on_ai, ai_enabled=ai_enabled,
                                    concern=concern,
                                )
                            # Problem and fix are LABELED and visually separated. A bare
                            # sentence followed by a fainter arrowed sentence read as one
                            # wrapped paragraph -- the arrow was the only cue that the
                            # second line was the remedy, and it was easy to skim past.
                            # Each now carries its own leading chip (amber "Risk" /
                            # green "Recommendation"), matching the amber=be-aware,
                            # green=resolution tones the notice palette uses, with the
                            # fix on a tinted panel so problem and solution are two
                            # distinct blocks rather than two lines of prose.
                            if concern.risk:
                                _render_concern_line(
                                    ui,
                                    # Advisory: the text describes the situation, not a
                                    # hazard, so neither the word "Risk" nor the amber
                                    # warning palette applies to it.
                                    icon="info" if advisory else "warning",
                                    label="Note" if advisory else "Risk",
                                    text=concern.risk,
                                    icon_class=(
                                        "text-sky-600" if advisory else "text-amber-600"
                                    ),
                                    label_class=(
                                        "text-sky-700" if advisory else "text-amber-700"
                                    ),
                                    panel_class=(
                                        "rounded border px-2 py-1.5 "
                                        + (
                                            "border-sky-200 bg-white"
                                            if advisory
                                            else "border-amber-200 bg-amber-50"
                                        )
                                    ),
                                )
                            if concern.recommendation:
                                _render_concern_line(
                                    ui,
                                    icon="lightbulb",
                                    label="Recommendation",
                                    text=concern.recommendation,
                                    icon_class="text-green-600",
                                    label_class="text-green-700",
                                    panel_class=(
                                        "rounded border border-green-200 bg-green-50 "
                                        "px-2 py-1.5"
                                    ),
                                )
            else:
                ui.label(f"Rule: {item.rule_id}").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500"
                )
                if item.risk:
                    ui.label("Risk").classes("text-sm font-semibold")  # type: ignore[attr-defined]
                    ui.label(item.risk).classes("text-sm")  # type: ignore[attr-defined]
                if item.recommendation:
                    ui.label("Recommendation").classes(  # type: ignore[attr-defined]
                        "text-sm font-semibold"
                    )
                    ui.label(item.recommendation).classes("text-sm")  # type: ignore[attr-defined]


def _render_item_ai_action(
    ui: object,
    item: "AssessmentItem",
    *,
    on_ai: Optional[Callable[..., object]],
    ai_enabled: bool,
    concern: object = None,
) -> None:
    """Render an AI-guidance affordance as a compact ICON button next to a finding.

    Sits on the right of a concern's header line in the item body, so guidance for
    THAT specific finding is one click away. When AI assist is on it opens the side
    panel and asks about the finding (``on_ai(item, concern)``). When AI is off the
    icon is shown disabled with a hint, so the affordance stays discoverable rather
    than silently missing. ``concern`` is the finding this icon is attached to (its
    text scopes the AI question); ``None`` falls back to object-level guidance.
    """
    if ai_enabled and on_ai is not None:
        btn = ui.button(icon="auto_awesome")  # type: ignore[attr-defined]
        btn.props("flat round dense size=sm color=indigo-6")
        # `.stop` so the click never toggles an enclosing expansion.
        btn.on(  # type: ignore[attr-defined]
            "click.stop", lambda _e=None, it=item, cc=concern: on_ai(it, cc)
        )
        btn.tooltip("Ask AI about this finding")  # type: ignore[attr-defined]
    else:
        disabled = ui.button(icon="auto_awesome")  # type: ignore[attr-defined]
        disabled.props("flat round dense size=sm color=grey-5")
        disabled.disable()  # type: ignore[attr-defined]
        disabled.tooltip(  # type: ignore[attr-defined]
            "Enable AI Assist on the Connect screen (toggle it on, set the Bedrock "
            "model, and re-test the connection) to get on-demand guidance for this "
            "finding."
        )


def _render_target(ui: object, result: EvaluationResult) -> None:
    """Render the target catalog summary and any pre-existing-object conflicts."""
    target = result.target_inventory
    table_count = sum(len(schema.tables) for schema in target.schemas)
    view_count = sum(len(schema.views) for schema in target.schemas)
    ui.label("Target analysis (Aurora DSQL)").classes("text-lg font-semibold")  # type: ignore[attr-defined]
    ui.label(  # type: ignore[attr-defined]
        f"Target catalog: {len(target.schemas)} schemas, {table_count} tables, "
        f"{view_count} views."
    ).classes("text-sm text-gray-600")

    if result.target_conflicts:
        render_notice(
            ui,
            tone="warning",
            header="Objects already exist on target",
            body=(
                f"{len(result.target_conflicts)} source object(s) already exist on "
                "the target and may conflict when applying converted DDL:"
            ),
        )
        with ui.row().classes("items-center gap-1 flex-wrap"):  # type: ignore[attr-defined]
            for name in result.target_conflicts:
                ui.badge(name).props("color=amber-7 outline")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Resolve these in Schema Conversion: choose SKIP (keep the existing "
            "object) or REPLACE (recreate it) when you apply."
        ).classes("text-xs text-gray-500")
    else:
        render_notice(
            ui,
            tone="success",
            header="No conflicts on the target",
            body="No source objects conflict with existing target objects.",
        )


def _render_downloads(ui: object, result: EvaluationResult) -> None:
    """Render compact export (download) buttons for the report (Req 8.4).

    No heading: this is meant to sit on the right of the assessment report
    header, so it shows only the format buttons.
    """

    def _download(download: ReportDownload) -> None:
        ui.download.content(  # type: ignore[attr-defined]
            download.content, download.filename, download.media_type
        )

    with ui.row().classes("gap-2 flex-wrap items-center"):  # type: ignore[attr-defined]
        ui.label("Export:").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
        ui.button(  # type: ignore[attr-defined]
            "HTML",
            on_click=lambda: _download(assessment_download(result, "html")),
        ).props("dense outline").tooltip(
            "Full report: assessment + target analysis (deterministic; no AI)."
        )
        ui.button(  # type: ignore[attr-defined]
            "JSON",
            on_click=lambda: _download(assessment_download(result, "json")),
        ).props("dense outline").tooltip(
            "Deterministic assessment data (no AI section)."
        )
        ui.button(  # type: ignore[attr-defined]
            "Text",
            on_click=lambda: _download(assessment_download(result, "text")),
        ).props("dense outline").tooltip(
            "Deterministic assessment, human-readable (no AI section)."
        )


__all__ = [
    "EvaluationInputs",
    "EvaluationResult",
    "run_evaluation",
    "compute_migration_score",
    "MigrationScore",
    "chat_turns_remaining",
    "filter_assessment_items",
    "sort_assessment_items",
    "source_inventory_tallies",
    "group_assessment_items_by_kind",
    "assessment_kind_summary",
    "assessment_concern_counts",
    "classification_label",
    "kind_section_label",
    "job_status_to_step_status",
    "ReportDownload",
    "assessment_download",
    "EvaluationState",
    "EvaluationStore",
    "build_evaluation_screen",
]
