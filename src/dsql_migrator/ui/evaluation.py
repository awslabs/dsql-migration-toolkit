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

import itertools
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from dsql_migrator.config import SecretValue
from dsql_migrator.core.assessment_strategist import (
    AssessmentStrategist,
    ObjectGuidanceOutcome,
    source_engine_word,
)
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.assessor import (
    DSQL_DATABASE_LIMITS,
    KIND_DATABASE,
    KIND_LABELS,
    PG_CATALOG_CHECKED_RULE_ID,
    PG_CATALOG_UNREAD_RULE_ID,
    CompatibilityAssessor,
    carry_probe_results,
    source_visibility_items,
)
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
    SourceType,
    StepStatus,
    TargetConnectionConfig,
    TargetInventory,
)
from dsql_migrator.core.target_introspector import TargetIntrospector
from dsql_migrator.ui.ai_assist import ai_is_usable
from dsql_migrator.ui.connect import make_source_engine_factory
from dsql_migrator.ui.ai_chat_drawer import chat_turns_remaining
from dsql_migrator.ui.design import (
    filter_bar,
    filter_input,
    filter_select,
    inline_hint,
    render_notice,
    section_header,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    format_source_engine,
    get_status,
    with_status,
)


# ---------------------------------------------------------------------------
# Engine contracts (Protocols keep run orchestration testable with fakes)
# ---------------------------------------------------------------------------


class _Introspector(Protocol):
    """Minimal contract used by :func:`run_evaluation` (read-only source).

    ``progress(tables_read, tables_total)`` is called while the source is read."""

    def introspect(
        self,
        conn: SourceConnectionConfig,
        *,
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> SourceInventory: ...


class EvaluationStopped(Exception):
    """Raised from a progress report when the operator cancelled the run: it unwinds the
    read (closing its connection) at the next table, and the worker returns normally so
    the job ends CANCELLED rather than FAILED."""


# What the stall watchdog says when it reaps an evaluation ({seconds} is filled in): the
# Full Load wording ("the load appears to have stalled ... retry the failed tables") is
# wrong for a read-only step.
EVALUATION_STALL_MESSAGE = (
    "No progress for {seconds}s while reading the source or target catalog, so the "
    "evaluation was stopped (an unresponsive connection, or a lock held on the source). "
    "Nothing was changed. Run it again."
)


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
    # The engine the last source test read ("Aurora MySQL 3.07.1 (MySQL 8.0.39)"), for the
    # exported report's header.
    source_engine: Optional[str] = None


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
    # The migration's SOURCE engine, so the exported report is titled correctly
    # ("PostgreSQL to Aurora DSQL..." vs "MySQL to..."). Defaults to MySQL for older
    # callers/tests that build a result without it.
    source_type: SourceType = SourceType.MYSQL
    # The DSQL endpoint ``target_inventory`` / ``target_conflicts`` were read FROM.
    #
    # Without it the target half of this result is indistinguishable from a target half
    # read somewhere else, and the operator can change the target cluster at any time
    # from the Connect screen: the browsed catalog then describes cluster A while every
    # consumer acts on cluster B. That is not only a wrong tree -- Data Migration derives
    # the set of migratable tables (and from it the CDC capture list and the Validation
    # scope) from this inventory, so a stale value produces a GO for tables that do not
    # exist on the connected cluster.
    #
    # The stamp is compared at the point of USE (:func:`target_inventory_is_stale`), not
    # cleared on a target change: ``set_target`` runs BEFORE the connection test, the
    # per-keystroke invalidation must not destroy a good catalog, and a restored session
    # snapshot re-creates the mismatched pair with no Connect event at all -- so an
    # invariant checked where the value is read is the only form that covers every path.
    # ``None`` means unknown provenance (a pre-0.1.524 snapshot, or a test fixture) and is
    # trusted, so older sessions keep working exactly as before.
    target_endpoint: Optional[str] = None
    # What was assessed and when, for the exported report's header: the source's engine
    # and address (never a credential) and the UTC time the run finished. Not persisted: a
    # restored report keeps the address from the snapshot and has no time, and its export
    # then says only when it was exported.
    source_label: Optional[str] = None
    assessed_at: Optional[str] = None


class _EndpointOnly:
    """Minimal stand-in carrying just ``cluster_endpoint``.

    Lets the render reuse the one staleness predicate instead of re-implementing the
    comparison (which is where a second, subtly different rule would drift in).
    """

    __slots__ = ("cluster_endpoint",)

    def __init__(self, cluster_endpoint: "Optional[str]") -> None:
        self.cluster_endpoint = cluster_endpoint


def target_inventory_is_stale(
    result: "Optional[EvaluationResult]",
    target_config: "Optional[TargetConnectionConfig]",
) -> bool:
    """Whether ``result``'s target half was read from a DIFFERENT cluster. Pure.

    ``False`` whenever the question cannot be answered -- no result, no stamp (unknown
    provenance), or no configured target -- so this never invalidates a catalog it merely
    failed to identify. Endpoint comparison is case-insensitive and whitespace-trimmed,
    matching how the Connect screen normalises what the operator types.
    """
    if result is None or target_config is None:
        return False
    stamped = (getattr(result, "target_endpoint", None) or "").strip().lower()
    if not stamped:
        return False
    live = (getattr(target_config, "cluster_endpoint", None) or "").strip().lower()
    if not live:
        return False
    return stamped != live


def with_refreshed_catalogs(
    result: EvaluationResult,
    *,
    inventory: SourceInventory,
    target_inventory: TargetInventory,
    target_endpoint: "Optional[str]",
    source_type: "Optional[SourceType]" = None,
    vanished: "Optional[Sequence[str]]" = None,
    source_label: Optional[str] = None,
) -> EvaluationResult:
    """``result`` with a re-read source inventory and/or target catalog, and what they feed.

    ``vanished`` is what a fresh source read listed but found gone (its introspector's
    ``last_vanished``): it replaces the result's own CATALOG_CHANGED_DURING_READ item. With
    no fresh read (the result's own inventory: a target refresh, Generate, a restore) the
    item is carried, so the tables it names are not silently forgotten.

    ``source_label`` is the source a fresh read came from (:func:`session_source_label`):
    it re-stamps the report's Source and Assessed lines, which otherwise kept naming the
    database the first run read -- in every export, the file name, the empty-database
    notice and the "describes another source" check -- above another database's objects.

    The assessment is recomputed together with the catalogs: Aurora DSQL's database-level
    limits (tables, schemas, views per database) are counted against the TARGET as well as
    the source, so a refresh that swapped the catalog and re-stamped the endpoint but kept
    the old assessment showed the other cluster's verdict as current -- a green 'Ready' for
    a cluster with no room left, or a stale 'Blocked' after the room was made. The name
    conflicts are recomputed with it. ``source_type`` is the engine the inventory was just
    read with (default: the result's own); a re-read after the operator switched engines on
    Connect is graded by that engine's rules, and the result says so. Pure but not free --
    it scales with the schema -- so UI callers run it off the event loop
    (``run.io_bound``); :func:`dataclasses.replace` keeps every other field.
    """
    from dataclasses import replace

    engine = source_type if source_type is not None else result.source_type
    assessment = CompatibilityAssessor(source_type=engine).assess(
        inventory, target_inventory=target_inventory
    )
    # What the run's own probes of the source found -- the privilege gap, and for
    # PostgreSQL row-level security and standalone sequences -- is not in the inventory, so
    # a re-assessment carries it over (re-run Step 1 to probe again). Not across an engine
    # switch, nor onto a fresh read of ANOTHER source: those findings describe the source
    # that was probed, and carried under the new source's name (now re-stamped) they read
    # 'no row-level security' for a database nobody checked.
    same_source = source_label is None or same_source_address(
        source_address(result.source_label), source_address(source_label)
    )
    if engine is result.source_type and same_source:
        assessment = carry_probe_results(result.assessment, assessment)
    from dsql_migrator.core.assessor import CATALOG_CHANGED_RULE_ID, catalog_changed_items

    if vanished is not None:
        changed = catalog_changed_items(list(vanished))
    elif inventory is result.inventory:
        changed = [i for i in result.assessment.items if i.rule_id == CATALOG_CHANGED_RULE_ID]
    else:
        changed = []
    if changed:
        assessment = AssessmentReport.from_items([*assessment.items, *changed])
    if engine is SourceType.POSTGRES and not any(
        item.rule_id in (PG_CATALOG_CHECKED_RULE_ID, PG_CATALOG_UNREAD_RULE_ID)
        for item in assessment.items
    ):
        # A report made before the PostgreSQL catalog probe existed (or re-read across an
        # engine switch) holds no trace of it, and a clean probe holds none either -- say
        # so rather than let it read "Ready" over row-level security never checked.
        from dsql_migrator.core.assessor_postgres import catalog_unread_items

        assessment = AssessmentReport.from_items(
            [*assessment.items, *catalog_unread_items(None, not_checked=True)]
        )
    return replace(
        result,
        inventory=inventory,
        assessment=assessment,
        source_type=engine,
        target_inventory=target_inventory,
        target_conflicts=_find_target_conflicts(inventory, target_inventory),
        target_endpoint=target_endpoint,
        **(
            {"source_label": source_label, "assessed_at": _utc_stamp()}
            if source_label is not None
            else {}
        ),
    )


def _find_target_conflicts(
    source: SourceInventory, target: TargetInventory
) -> list[str]:
    """Return source table/view names that already exist on the target.

    Decides as Schema Apply does: a qualified source name (the tool qualifies source
    objects as ``database.table``) is matched exactly against the target's stored names
    (``shop.Orders`` is not ``shop.orders``), an unqualified one case-insensitively. This
    is what surfaces a conflict when the target already has the same schema *and* table
    name as the source. Source order is preserved (tables, then views). The matching rule
    itself is
    :func:`~dsql_migrator.core.target_introspector.relations_existing_on_target`,
    shared with the per-database limit counts so the two cannot disagree.
    """
    from dsql_migrator.core.converter import relation_clash_names
    from dsql_migrator.core.target_introspector import relations_existing_on_target

    view_names = [view.name for view in source.views]
    # Names that share a stored form are never created (NAME_COLLISION): their copy on the
    # target is no SKIP/REPLACE choice.
    clash = relation_clash_names(source)
    return [
        name
        for name in relations_existing_on_target(
            [table.name for table in source.tables] + view_names,
            target,
            views=view_names,
        )
        if name not in clash
    ]


def _find_target_blockers(
    source: SourceInventory, target: TargetInventory, *, is_postgres: bool = False
) -> "list[tuple[str, str]]":
    """``(source table/view, what holds its name)`` where ANOTHER kind of object (a view
    where a table goes, an index) holds the name on the target: Schema Apply refuses those
    in both modes. Computed at render from the browsed catalog (nothing new persisted).

    Only for what Schema Apply creates: a name-collision placeholder table, a view whose
    definition was not read, and a PostgreSQL materialized view or foreign table never
    are, so freeing their name achieves nothing -- or drops the very table the operator
    built to replace a materialized view."""
    from dsql_migrator.core.converter import index_renames, relation_clash_names
    from dsql_migrator.core.schema_applier import converted_names
    from dsql_migrator.core.target_introspector import TargetNames

    clashing = relation_clash_names(source)
    created = [table.name for table in source.tables if table.name not in clashing] + [
        view.name
        for view in source.views
        if view.name not in clashing and view.definition and not view.unsupported_kind
    ]
    names = TargetNames(
        target,
        views=[view.name for view in source.views],
        legacy_indexes=converted_names(
            (), index_renames(source, is_postgres=is_postgres)
        ).legacy_indexes,
    )
    blocked: list[tuple[str, str]] = []
    for name in created:
        holder = names.blocked_by(name)
        if holder is not None:
            blocked.append((name, holder))
    return blocked



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
#
# The ratio alone does not pick the band when something BLOCKS the migration, because an
# average dilutes a blocker into nothing: one cluster-level quota item among 1,001
# otherwise-clean tables is 1/1002 of the worst case, so the card read a green "Ready --
# a straightforward migration" for a source whose 1,001st CREATE TABLE fails at apply, and
# two tables without a primary key among ten read "Ready" too. So blockers decide first
# (see :func:`compute_migration_score`): a database-level limit the source exceeds forces
# :data:`_BLOCKED_BAND`, and any UNSUPPORTED object caps a success band at
# :data:`_UNSUPPORTED_BAND`. The ratio still places everything else.
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

# A database-level Aurora DSQL limit the source exceeds (tables / schemas / views per
# database): nothing about the per-object work matters until it is resolved, so it is an
# error regardless of the ratio. The summary is built per report (it names the limit).
_BLOCKED_BAND = ("Blocked", "error")
# At least one UNSUPPORTED object, in a report whose ratio alone would read as a success:
# the rest may be easy, but these objects cannot move as-is, so the card must not read as
# an all-clear. Bands that are already warning/error keep their own label.
_UNSUPPORTED_BAND = (
    "Unsupported objects",
    "warning",
    "{count} object{plural} cannot move to Aurora DSQL as-is and {verb} a redesign (see "
    "the Unsupported items below).",
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
    bounded to 0-100; it is capped at 99 while any object needs work, so it is 100
    exactly when nothing does (rounding alone reached 100 with one SIGNIFICANT object
    among 200).

    The band is decided by blockers BEFORE the ratio: a database-level limit the migration
    exceeds (an UNSUPPORTED limit item of kind ``DATABASE`` -- too many tables, schemas or
    views for one Aurora DSQL database) forces "Blocked" (error), naming the limit;
    otherwise any UNSUPPORTED object turns a success band into "Unsupported objects"
    (warning). The ratio places everything else. A limit item is not per-object work, so it
    sets the band without entering the ratio (the score then reads as the per-object
    readiness it is captioned as). Returns ``None`` for an empty report (nothing to score).
    Pure and deterministic so it is unit-testable and matches the on-screen and exported
    values.
    """
    items = report.assessed_items
    if not items:
        return None

    # Keyed on the limit checks themselves (not merely kind DATABASE), so the band's
    # "exceeds an Aurora DSQL database limit" can only ever name a real limit.
    blockers = [
        item
        for item in items
        if item.classification is Classification.UNSUPPORTED
        and item.kind == KIND_DATABASE.upper()
        and item.rule_id in DSQL_DATABASE_LIMITS
    ]
    # The note that the catalog probe ran is not an object to migrate: it must not dilute
    # the penalties of the objects that are.
    scored = [
        item
        for item in items
        if not any(item is b for b in blockers) and item.rule_id != PG_CATALOG_CHECKED_RULE_ID
    ]

    total_penalty = 0.0
    auto = manual = unsupported = 0
    for item in items:
        classification = item.classification.value
        if classification == "AUTO":
            auto += 1
        elif classification == "UNSUPPORTED":
            unsupported += 1
        else:
            manual += 1
    for item in scored:
        classification = item.classification.value
        if classification == "AUTO":
            continue
        effort = item.effort.value if item.effort is not None else None
        if effort is not None:
            total_penalty += _EFFORT_PENALTY.get(effort, _EFFORT_PENALTY["MEDIUM"])
        else:
            total_penalty += _NO_EFFORT_PENALTY.get(classification, _MAX_OBJECT_PENALTY)

    worst_case = _MAX_OBJECT_PENALTY * len(scored)
    ratio = total_penalty / worst_case if worst_case else 0.0
    score = max(0, min(100, round(100 * (1 - ratio))))
    if total_penalty > 0:
        score = min(score, 99)

    label, tone, summary = _SCORE_BANDS[-1][1:]
    for threshold, band_label, band_tone, band_summary in _SCORE_BANDS:
        if score >= threshold:
            label, tone, summary = band_label, band_tone, band_summary
            break

    if blockers:
        label, tone = _BLOCKED_BAND
        summary = _blocked_summary(blockers)
    elif unsupported and tone == "success":
        label, tone, template = _UNSUPPORTED_BAND
        summary = template.format(
            count=unsupported,
            plural="" if unsupported == 1 else "s",
            verb="needs" if unsupported == 1 else "need",
        )

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


def _blocked_summary(blockers: list) -> str:
    """The "Blocked" band's sentence: which database-level limit(s) the migration exceeds."""
    parts = []
    for item in blockers:
        noun, limit = DSQL_DATABASE_LIMITS[item.rule_id]
        parts.append(f"{item.object_name} (Aurora DSQL allows {limit:,} {noun} per database)")
    several = len(parts) > 1
    subject = "Aurora DSQL database limits" if several else "an Aurora DSQL database limit"
    return (
        f"This migration exceeds {subject}: "
        + "; ".join(parts)
        + f". Resolve the database / cluster-level item{'s' if several else ''} below "
        "before converting; the score rates the per-object work only."
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
    ties break by object name for a stable, readable order -- the exports' order too
    (:func:`~dsql_migrator.core.assessor.importance_order`).
    """
    from dsql_migrator.core.assessor import importance_order

    return importance_order(items)


def filter_assessment_items(
    items: list, *, classification: str = "ALL", effort: str = "ALL", name: str = ""
) -> list:
    """Return the assessment items kept under the "Objects by importance" filters.

    Filters along the two color-coded categories independently and combines them
    (AND): ``classification`` is ``ALL`` or a classification enum value
    (``AUTO`` / ``MANUAL`` / ``UNSUPPORTED``); ``effort`` is ``ALL`` or an
    :class:`EffortLevel` value (``SIMPLE`` / ``MEDIUM`` / ``SIGNIFICANT``). An
    unknown value for either axis is treated as ``ALL`` (nothing hidden). Because
    ``AUTO`` objects carry no effort, selecting a specific effort naturally
    excludes them. ``name`` keeps the objects whose name contains it, ignoring case and
    surrounding spaces -- the rule Schema Conversion's "Filter objects by name" box
    applies to its tree -- or that carry a finding whose rule id contains it (BYTEA_KEY,
    collation): one fix often applies across many tables. Order is preserved, so callers
    can sort first and filter after.
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
    needle = (name or "").strip().lower()

    def _keep(item) -> bool:
        if class_target is not None and item.classification != class_target:
            return False
        if effort_target is not None and item.effort != effort_target:
            return False
        if needle and needle not in item.object_name.lower() and not any(
            needle in (concern.rule_id or "").lower()
            for concern in (item.concerns or ())
        ) and needle not in (item.rule_id or "").lower():
            return False
        return True

    return [item for item in items if _keep(item)]


# Rows per object-kind group in the "Objects by importance" list. Each group shows one page
# of its objects, most critical first, so the screen builds a bounded number of elements
# however large the source is (the exports still list every object).
OBJECTS_PER_PAGE = 50

# Target name-conflict badges drawn before "and N more".
TARGET_CONFLICT_BADGES = 20


def assessment_page(
    items: list, page: int, *, size: int = OBJECTS_PER_PAGE
) -> tuple[list, int, int]:
    """Return ``(the items on page, the page clamped to 1..pages, pages)``. Pure.

    ``page`` is 1-based; one out of range (a group that shrank under a filter, a stale
    page from an earlier render) is clamped rather than showing an empty page. An empty
    list is one empty page.
    """
    pages = max(1, -(-len(items) // size))
    current = min(max(1, int(page or 1)), pages)
    start = (current - 1) * size
    return items[start : start + size], current, pages


# Display order for grouping assessed objects by kind. Tables come first (the
# migration's primary objects), then Views, Triggers, Routines; any other kind
# follows in first-seen order.
_KIND_DISPLAY_ORDER = (
    "DATABASE", "TABLE", "VIEW", "SEQUENCE", "TRIGGER", "PROCEDURE", "FUNCTION", "ROUTINE",
    "EVENT",
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
        f"{counts[value]:,} {classification_label(value)}"
        for value in order
        if counts.get(value)
    ]
    parts += [
        f"{count:,} {classification_label(value)}"
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

    def _source_read(read: int, total: int) -> None:
        # 5% -> 35% across the source read: one report per table is a heartbeat (a large
        # schema used to be one silent span the 900 s watchdog reaped) and a stop point.
        pct = 5 + (30 * read // total if total else 0)
        report(pct, f"Reading the source catalog: {read:,} of {total:,} tables")

    inventory = introspector.introspect(inputs.source_config, progress=_source_read)
    # What the source user's privileges keep out of that inventory (a least-privilege MySQL
    # user sees no triggers, events or routines at all), so the report says so instead of
    # reading "Ready" over objects it never saw. Optional seam: a test double may not probe.
    _probe = getattr(introspector, "listing_gaps", None)
    listing_gaps = (
        _probe(inputs.source_config, _listing_databases(inventory, inputs.source_config))
        if callable(_probe)
        else []
    )
    # PostgreSQL catalog facts the inventory does not carry: row-level security (Aurora
    # DSQL has none, and a migration user the policies apply to would read a subset) and
    # sequences no column owns (Schema Conversion creates none). Same optional seam; a read
    # that fails is reported as not checked, never as nothing found.
    _catalog_probe = getattr(introspector, "source_catalog", None)
    catalog_probed = (
        inputs.source_config.source_type is SourceType.POSTGRES and callable(_catalog_probe)
    )
    catalog = _catalog_probe(inputs.source_config) if catalog_probed else None

    # The target is read BEFORE the assessment because Aurora DSQL's database-level limits
    # (tables, schemas, views per database) are about what the target will hold after the
    # migration: 700 source tables are a blocker on a cluster that already has 400 others.
    report(35, "Introspecting target Aurora DSQL catalog...")
    browser = target_browser_factory(inputs.aws_profile)
    target_inventory = browser.browse(inputs.target_config)

    report(60, "Assessing source/target compatibility...")
    # The default rule set is source-engine-specific (MySQL vs PostgreSQL); pass the
    # source type so a PostgreSQL source is assessed with its own rules.
    assessment = (
        assessor
        or CompatibilityAssessor(source_type=inputs.source_config.source_type)
    ).assess(inventory, target_inventory=target_inventory)
    # What the read listed but found gone (twice): said, never silently left out.
    vanished = list(getattr(introspector, "last_vanished", None) or [])
    if vanished:
        from dsql_migrator.core.assessor import catalog_changed_items

        assessment = AssessmentReport.from_items(
            [*assessment.items, *catalog_changed_items(vanished)]
        )
    if listing_gaps:
        assessment = AssessmentReport.from_items(
            [*assessment.items, *source_visibility_items(listing_gaps)]
        )
    if catalog_probed:
        from dsql_migrator.core.assessor_postgres import with_pg_catalog_facts

        assessment = with_pg_catalog_facts(assessment, catalog, inventory)

    report(85, "Detecting objects that already exist on the target...")
    target_conflicts = _find_target_conflicts(inventory, target_inventory)

    report(100, "Finalizing the assessment report...")
    return EvaluationResult(
        inventory=inventory,
        assessment=assessment,
        target_inventory=target_inventory,
        target_conflicts=target_conflicts,
        source_type=inputs.source_config.source_type,
        target_endpoint=inputs.target_config.cluster_endpoint,
        source_label=describe_source(inputs.source_config, inputs.source_engine),
        assessed_at=_utc_stamp(),
    )


def set_result_if_unchanged(
    eval_state: "EvaluationState", expected: "Optional[EvaluationResult]", result: EvaluationResult
) -> bool:
    """Store ``result`` only while ``expected`` is still the current result -- a refresh
    computed from an older result must not overwrite an Evaluation that finished while it
    ran. Returns whether it was stored."""
    if eval_state.result is not expected:
        return False
    eval_state.set_result(result)
    return True


# The address of a describe_source() label -- its host, and its database when there is one:
# the leftmost " at " (or the start) followed by a whole host[:port][/database].
_SOURCE_ADDRESS_RE = re.compile(
    r"(?:^| at )(?P<address>(?P<host>[^\s:/]+)(?::\d*)?(?:/(?P<database>.+))?)$"
)


def refresh_stepped_aside_text(eval_state: "EvaluationState") -> Optional[str]:
    """What to tell the operator when a refresh was not applied (``set_result_if_unchanged``
    stepped aside), or ``None`` to say nothing: after Start over there is no result to
    speak of, and 'a newer evaluation finished' was wrong whenever another refresh, or
    another tab, had replaced the result instead."""
    if eval_state.result is None:
        return None
    return (
        "The Evaluation result changed while this refresh read the catalogs (a re-run, "
        "another refresh, or another tab), so it was not applied over it. Refresh again "
        "to read the current catalogs."
    )


def describe_source(
    config: "Optional[SourceConnectionConfig]", engine: Optional[str] = None
) -> Optional[str]:
    """``"<engine> at host:port/database"`` for a report header -- never a credential.
    ``None`` without a host."""
    host = (getattr(config, "host", None) or "").strip()
    if not host:
        return None
    database = (getattr(config, "database", None) or "").strip()
    port = getattr(config, "port", None)
    address = host + (f":{port}" if port else "") + (f"/{database}" if database else "")
    return f"{engine} at {address}" if engine else address


def source_address(label: Optional[str]) -> Optional[str]:
    """The ``host:port[/database]`` of a :func:`describe_source` label (``None`` without).

    The address follows the FIRST `` at `` that starts one (an engine label holds no
    `` at ``; a database name may: ``sales at hq``, which the last one cut to ``hq``).
    """
    match = _SOURCE_ADDRESS_RE.search(label or "")
    return match.group("address") if match else None


def same_source_address(a: Optional[str], b: Optional[str]) -> bool:
    """Whether two :func:`source_address` values name one source: the host compares
    case-insensitively (DNS does), the port and database exactly."""
    if a is None or b is None:
        return a == b
    first, second = _SOURCE_ADDRESS_RE.search(a), _SOURCE_ADDRESS_RE.search(b)
    if first is None or second is None:
        return a == b

    def key(match: "re.Match[str]") -> tuple[str, str]:
        host = match.group("host")
        return host.lower(), match.group("address")[len(host):]

    return key(first) == key(second)


def session_source_label(session: Any) -> Optional[str]:
    """:func:`describe_source` of the session's connected source, with its engine, as a
    run stamps it: what a fresh source read re-stamps a refreshed report with."""
    config = getattr(session, "source_config", None)
    if config is None:
        return None
    return describe_source(
        config,
        format_source_engine(
            getattr(session, "source_server_version", None),
            getattr(session, "source_engine_version", None),
            getattr(session, "source_aurora_version", None),
            source_type=config.source_type,
        ),
    )


def _utc_stamp() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _listing_databases(
    inventory: SourceInventory, config: SourceConnectionConfig
) -> list[str]:
    """The source databases whose listing the privilege probe checks: those the inventory
    holds objects of, plus the configured one (it may hold only what the user cannot see)."""
    names: list[str] = []
    for name in (
        *(t.name for t in inventory.tables),
        *(v.name for v in inventory.views),
        *(o.name for o in (*inventory.routines, *inventory.events, *inventory.triggers)),
    ):
        database = name.partition(".")[0] if "." in name else ""
        if database and database not in names:
            names.append(database)
    if config.database and config.database not in names:
        names.append(config.database)
    return names


def job_status_to_step_status(job_status: str) -> Optional[StepStatus]:
    """Map a :class:`JobManager` job status to the Evaluation step status.

    Returns ``DONE``/``FAILED`` for terminal job states and ``None`` while the
    job is still ``PENDING``/``RUNNING`` (the step stays ``IN_PROGRESS``).
    """
    if job_status == "DONE":
        return StepStatus.DONE
    if job_status == "FAILED":
        return StepStatus.FAILED
    if job_status == "CANCELLED":
        return StepStatus.NOT_STARTED
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
    evaluation" affordance returns, and ``DONE`` when the result IS there (the run
    finished while the screen was not open to record it -- only its poll did -- and
    the snapshot kept the result); otherwise ``None`` (leave the status unchanged --
    a genuinely running job keeps spinning, a finished one keeps its DONE/FAILED).
    Pure + deterministic so the reconciliation is unit-testable.
    """
    if status is StepStatus.IN_PROGRESS and not job_alive:
        return StepStatus.DONE if has_result else StepStatus.NOT_STARTED
    return None


def _settle_unfinished_run(
    eval_state: "EvaluationState", *, cancelled: bool, error: str
) -> StepStatus:
    """The step status after a run that did not finish, and what to tell the operator.

    The previous report was kept through the run (see :meth:`EvaluationState.begin_run`):
    when there is one, it stands -- the step stays DONE, so the later steps are not
    blocked by a re-run that failed or was cancelled -- and a notice says so. Without
    one, a failure is FAILED with its error and a cancel returns to NOT_STARTED."""
    has_previous = eval_state.result is not None
    if cancelled:
        eval_state.run_notice = (
            "info",
            "Evaluation cancelled",
            (
                "The run was cancelled; nothing was changed (this step only reads). "
                + (
                    "The previous assessment below is kept."
                    if has_previous
                    else "Run it again when you are ready."
                )
            ),
        )
        return StepStatus.DONE if has_previous else StepStatus.NOT_STARTED
    if has_previous:
        eval_state.run_notice = (
            "warning",
            "The re-run failed; the previous assessment is kept",
            error,
        )
        return StepStatus.DONE
    eval_state.set_error(error)
    return StepStatus.FAILED


def _eval_job_running(job_manager: JobManager, job_id: Optional[str]) -> bool:
    """True while the evaluation job is PENDING or RUNNING in this process."""
    if job_id is None:
        return False
    try:
        return job_manager.get_status(job_id).status in ("PENDING", "RUNNING")
    except JobNotFoundError:
        return False


def _notify_quietly(ui: object, message: str) -> None:
    """``ui.notify`` an info message when a client context exists (a no-op otherwise)."""
    try:
        ui.notify(message, type="info")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - no client context (a test, a background call)
        pass


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


# The SCT-style effort buckets (EffortLevel), spelled out in every export that counts them.
EFFORT_SCALE = (
    "SIMPLE under 2 hours, MEDIUM 2 to 6 hours, SIGNIFICANT over 6 hours, per object that "
    "does not convert automatically"
)


def report_metadata(result: EvaluationResult, *, exported_at: str) -> "list[tuple[str, str]]":
    """The ``(label, value)`` header of every export: what was assessed, against what,
    when, by which version, and the verdict with the counts behind it -- so a shared copy
    stands on its own. Never a credential."""
    from dsql_migrator import __version__

    lines: list[tuple[str, str]] = []
    if result.source_label:
        lines.append(("Source", result.source_label))
    if result.target_endpoint:
        lines.append(("Target", result.target_endpoint))
    if result.assessed_at:
        lines.append(("Assessed", result.assessed_at))
    lines.append(("Exported", exported_at))
    lines.append(("Tool version", __version__))
    score = compute_migration_score(result.assessment)
    if score is None:
        lines.append(("Readiness", "No objects to rate"))
    else:
        caption = "per-object readiness" if score.band == _BLOCKED_BAND[0] else "readiness"
        lines.append(("Readiness", f"{score.band} ({score.score} / 100 {caption})"))
        lines.append((
            "Objects",
            f"{score.total:,} assessed: {score.auto:,} automatic, {score.manual:,} review "
            f"needed, {score.unsupported:,} unsupported",
        ))
    lines.append(("Effort scale", EFFORT_SCALE))
    return lines




def report_filename_stem(result: EvaluationResult, exported_at: str) -> str:
    """``compatibility_assessment_<database or host>_<YYYYMMDD-HHMMSS>``: two exports of two
    sources, or of one source twice, no longer overwrite each other."""
    parts = ["compatibility_assessment"]
    match = _SOURCE_ADDRESS_RE.search(result.source_label or "")
    if match:
        name = match.group("database") or match.group("host").split(".")[0]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-.")[:40]
        if slug:
            parts.append(slug)
    digits = re.sub(r"\D", "", exported_at)
    if len(digits) >= 14:
        parts.append(f"{digits[:8]}-{digits[8:14]}")
    return "_".join(parts)


def assessment_download(
    result: EvaluationResult, fmt: str = "json", *, exported_at: Optional[str] = None
) -> ReportDownload:
    """Serialize the compatibility assessment report for download (Req 8.4).

    Every format opens with :func:`report_metadata` (source, target, times, tool
    version, readiness, counts and the effort scale) and carries the target's existing
    and held names: the HTML in its Target analysis section (with the catalog), JSON
    under ``"target"``, text after the items. No AI section is included in any format:
    AI assistance is on-demand and per object (the Evaluation screen's AI drawer), so it
    is not part of the exported, shareable report.
    """
    exported_at = exported_at or _utc_stamp()
    metadata = report_metadata(result, exported_at=exported_at)
    # The screen's "Names held on the target by another object" list: without it the
    # export printed the all-clear over objects neither apply mode can create.
    blocked = _find_target_blockers(
        result.inventory,
        result.target_inventory,
        is_postgres=result.source_type is SourceType.POSTGRES,
    )
    if fmt.lower() == "html":
        content = render_html_report(
            result.assessment,
            target=result.target_inventory,
            conflicts=result.target_conflicts,
            source_type=result.source_type,
            blocked=blocked,
            target_endpoint=result.target_endpoint,
            metadata=metadata,
        )
    else:
        content = export_assessment_report(
            result.assessment,
            fmt,
            source_type=result.source_type,
            metadata=metadata,
            target_endpoint=result.target_endpoint,
            conflicts=result.target_conflicts,
            blocked=blocked,
        )
    filename, media_type = _download_parts(report_filename_stem(result, exported_at), fmt)
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
        # The run token of the job in ``job_id``: a poll or a cancel retires THAT run,
        # never whichever run happens to be current.
        self.job_token: Optional[int] = None
        # The current run's token (0: none). A worker writes only while its token is
        # current: Start over re-inits this object, and a stopped or reaped run is
        # retired, so a superseded worker can no longer overwrite the result.
        self._run_token = 0
        # Set by the Cancel button (UI thread only).
        self.cancel_requested = False
        # (tone, header, body) about how the last run ended when a previous report was
        # kept (a failed or cancelled re-run), shown above it; or a cancelled first run.
        self.run_notice: Optional[tuple[str, str, str]] = None
        # Filters for the "Objects by importance" list (UI-thread only), split
        # along the two color-coded categories the summary already shows so the
        # controls read as "narrow by Classification and/or Estimated manual
        # effort" rather than one list mixing a derived "needs attention" bucket
        # with per-classification values. Each is "ALL" or an enum value; they
        # combine (AND). Lets the user focus on actionable objects at scale.
        self.classification_filter: str = "ALL"
        self.effort_filter: str = "ALL"
        # The list's name search and, per object kind, the page each group shows (both
        # UI-thread only and never persisted, like the filters above).
        self.name_filter: str = ""
        self.list_pages: dict[str, int] = {}
        self._result: Optional[EvaluationResult] = None
        self._error: Optional[str] = None
        # Coarse progress for the in-progress UI, updated by the background
        # worker and read by the poll timer (guarded by the same lock).
        self._progress_pct: int = 0
        self._progress_message: str = ""

    def begin_run(self) -> int:
        """Start a run: a new token, cleared progress/error/notice. The previous result is
        KEPT until the new run succeeds -- later steps read it, and a failed or cancelled
        re-run must not leave them with nothing."""
        with self._lock:
            self._run_token = next(_RUN_TOKENS)
            self._error = None
            self._progress_pct = 0
            self._progress_message = ""
            self.run_notice = None
            self.cancel_requested = False
            return self._run_token

    def is_current(self, token: Optional[int]) -> bool:
        """Whether ``token`` is the current run's (``None``: an untokened caller)."""
        with self._lock:
            return token is None or token == self._run_token

    def retire_run(self, token: Optional[int] = None) -> None:
        """Make the current run's (or ``token``'s) late writes no-ops: it was cancelled or
        reaped, and a worker that comes back later must not land a result under it."""
        with self._lock:
            if token is None or token == self._run_token:
                self._run_token = -next(_RUN_TOKENS)

    def set_progress(self, percent: int, message: str, *, token: Optional[int] = None) -> None:
        """Record coarse run progress (percent 0-100 and a phase message)."""
        with self._lock:
            if token is not None and token != self._run_token:
                return
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

    def set_result(self, result: EvaluationResult, *, token: Optional[int] = None) -> bool:
        """Record a successful run's result (clears any prior error). ``False`` (nothing
        recorded) for a superseded run's ``token``."""
        with self._lock:
            if token is not None and token != self._run_token:
                return False
            self._result = result
            self._error = None
            # A different set of objects: every group of the list starts at its first page.
            self.list_pages.clear()
            return True

    def set_error(self, message: str, *, token: Optional[int] = None) -> None:
        """Record a failure message for display."""
        with self._lock:
            if token is not None and token != self._run_token:
                return
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


def untested_connections(session: Any) -> list[str]:
    """The configured connections NOT tested live this session (``source``/``target``).

    Configured is not enough to run: after a restart the source is restored without its
    password and neither side is tested, so a run failed minutes in with a raw driver
    error. A missing flag (a UI double) reads as tested.
    """
    return [
        name
        for name, configured, verified in (
            ("source", session.has_source(), getattr(session, "source_verified", True)),
            ("target", session.has_target(), getattr(session, "target_verified", True)),
        )
        if configured and verified is False
    ]


def evaluation_run_guard_reason(session: Any) -> Optional[str]:
    """Why the shell's Run / Re-run evaluation is disabled, or ``None``: a connection not
    tested this session (the on-screen Run button is gated the same way)."""
    untested = untested_connections(session)
    return untested_connections_text(untested) if untested else None


def untested_connections_text(untested: list[str]) -> str:
    """What to do about :func:`untested_connections`, for a notice or a refusal."""
    one = len(untested) == 1
    reenter = (
        " (after a restart, re-enter the source password)" if "source" in untested else ""
    )
    return (
        f"The {' and '.join(untested)} {'connection has' if one else 'connections have'} "
        f"not been tested in this session. Test {'it' if one else 'them'} on the Connect "
        f"screen{reenter}, then run the evaluation."
    )


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

# Run tokens, unique in the process (never 0, the "no run" token), so a re-initialised
# EvaluationState can never match a superseded worker's token.
_RUN_TOKENS = itertools.count(1)


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
        if _eval_job_running(job_manager, eval_state.job_id):
            # A second Run (another tab, a double click) would start a second read of the
            # production source and race the first for the result.
            _notify_quietly(
                ui,
                "An evaluation is already running. Wait for it to finish, or cancel it.",
            )
            return
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
        untested = untested_connections(session)
        if untested:
            eval_state.set_error(untested_connections_text(untested))
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
            source_engine=format_source_engine(
                getattr(session, "source_server_version", None),
                getattr(session, "source_engine_version", None),
                getattr(session, "source_aurora_version", None),
                source_type=source_config.source_type,
            ),
        )
        token = eval_state.begin_run()
        session.set_workflow(
            with_status(session.workflow, WorkflowStep.EVALUATION, StepStatus.IN_PROGRESS)
        )

        def work(_handle: object) -> None:
            def _settle_from_worker(*, cancelled: bool, error: str) -> None:
                # A failed or stopped run is settled here too, not only by the screen's
                # poll: with the operator on another step the step stayed IN_PROGRESS and
                # Schema Conversion stayed locked. Only while this is still the session's
                # job (Start over or a newer run has moved on); the poll's settle repeats it.
                if eval_state.job_id != getattr(_handle, "job_id", None):
                    return
                eval_state.retire_run(token)
                session.set_workflow(
                    with_status(
                        session.workflow,
                        WorkflowStep.EVALUATION,
                        _settle_unfinished_run(eval_state, cancelled=cancelled, error=error),
                    )
                )

            def _progress(pct: int, message: str) -> None:
                # Source introspection + assessment + target browse were ONE silent span,
                # so a very large schema's evaluation was reaped by the job watchdog and
                # rendered a red "Evaluation failed" carrying a Full-Load-worded stall
                # message -- for a read-only step -- which then blocks the journey's
                # step-order gate. A reported phase (and each source table) is a real
                # unit boundary, and where a cancel takes effect.
                if getattr(_handle, "cancelled", False) or not eval_state.is_current(token):
                    raise EvaluationStopped()
                snapshot = getattr(_handle, "snapshot", None)
                if callable(snapshot) and snapshot().status != "RUNNING":
                    # Reaped by the stall watchdog (FAILED): it cannot be cancelled any
                    # more, and its result would be thrown away -- stop reading.
                    raise EvaluationStopped()
                beat = getattr(_handle, "heartbeat", None)
                if callable(beat):
                    beat()
                eval_state.set_progress(pct, message, token=token)

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
                    progress_cb=_progress,
                )
            except EvaluationStopped:
                log_activity(
                    ActivityCategory.ASSESSMENT,
                    "run assessment",
                    status=ActivityStatus.INFO,
                    detail="stopped (cancelled, superseded or reaped); read only, so nothing was changed",
                )
                # Reaped by the watchdog (FAILED, with its stall message) is a failure,
                # not a cancel.
                _snapshot = getattr(_handle, "snapshot", None)
                _reaped = callable(_snapshot) and _snapshot().status == "FAILED"
                _settle_from_worker(
                    cancelled=not _reaped,
                    error=(
                        (job_manager.get_error(_handle.job_id) or "Evaluation failed.")  # type: ignore[attr-defined]
                        if _reaped
                        else ""
                    ),
                )
                return  # the job ends CANCELLED (its cancel was requested)
            except Exception as exc:  # noqa: BLE001 - re-raised; job marks FAILED
                log_activity(
                    ActivityCategory.ASSESSMENT,
                    "run assessment",
                    status=ActivityStatus.FAILURE,
                    detail=f"{type(exc).__name__}: {exc}",
                    exc=exc,
                )
                if ai_post_event is not None and eval_state.is_current(token):
                    # Keep the event lean: the type is the useful signal; the full
                    # exception message can be long/verbose (and is already in the
                    # activity log), so it is not sent into the AI feed/grounding.
                    ai_post_event(
                        text=f"Compatibility assessment failed ({type(exc).__name__})",
                        status="error",
                    )
                _first = str(exc).strip().splitlines()
                _settle_from_worker(
                    cancelled=False,
                    error=(
                        f"{type(exc).__name__}: {_first[0].strip()[:300]}"
                        if _first
                        else type(exc).__name__
                    ),
                )
                raise
            if not eval_state.set_result(result, token=token):
                # Superseded (Start over, or a cancelled/reaped run that came back late):
                # its result must not replace the session's.
                return
            # Recorded here, not only by the screen's poll: a run that finished while the
            # operator was on another step stayed "In progress" until they came back.
            session.set_workflow(
                with_status(session.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
            )
            report = result.assessment
            counts = ", ".join(f"{k.value}={v}" for k, v in report.summary.items())
            conflicts = len(result.target_conflicts or [])
            log_activity(
                ActivityCategory.ASSESSMENT,
                "run assessment",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{len(report.assessed_items)} object(s) assessed ({counts}); "
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
                        f"Assessment complete: {len(report.assessed_items)} objects — "
                        f"{_auto} automatic, {_man} need review, {_uns} unsupported"
                        + (f"; {conflicts} already on target" if conflicts else "")
                    ),
                    status="success",
                    kind="assessment",
                    data={
                        "total": len(report.assessed_items),
                        "auto": _auto,
                        "manual": _man,
                        "unsupported": _uns,
                        "conflicts": conflicts,
                    },
                )

        eval_state.job_token = token
        # Not durable: nothing resumes a read-only Evaluation after a restart, and stored
        # runs counted toward the retention cap that could evict a Full Load job.
        eval_state.job_id = job_manager.submit(
            work, stall_message=EVALUATION_STALL_MESSAGE, durable=False
        )

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

        def _cancel_evaluation() -> None:
            job_id = eval_state.job_id
            if job_id is None or eval_state.cancel_requested:
                return
            log_activity(
                ActivityCategory.ASSESSMENT,
                "cancel requested",
                status=ActivityStatus.INFO,
                detail="operator cancelled the evaluation; it stops after the table being read",
            )
            job_manager.request_cancel(job_id)
            # Retired at once: a result the run produces after its last stop point is not
            # recorded, so "cancelled; the previous assessment is kept" stays true.
            eval_state.retire_run(eval_state.job_token)
            eval_state.cancel_requested = True
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

            untested = untested_connections(session)
            if not session.has_source() or not session.has_target():
                render_notice(
                    ui,
                    tone="warning",
                    header="Source and target required",
                    body=(
                        "Both a source and a target connection are required. Set "
                        "them up on the Connect screen."
                    ),
                )
            elif untested and status is not StepStatus.IN_PROGRESS:
                render_notice(
                    ui,
                    tone="warning",
                    header="Test the connections first",
                    body=untested_connections_text(untested),
                )

            # Empty-state call-to-action: before the first run, give an on-screen
            # Run button (the workflow shell's Run is easy to miss) and reassure
            # that this step is read-only.
            if status is StepStatus.NOT_STARTED:
                with ui.card().classes("w-full bg-gray-50"):
                    ui.label("Ready to evaluate").classes("text-base font-semibold")
                    ui.label(
                        "This step only reads the source and target (no changes "
                        "are made), classifies every object (Automatic / Review "
                        "needed / Unsupported), and estimates the manual effort."
                    ).classes("text-sm text-gray-600")
                    run_button = ui.button(
                        "Run evaluation", on_click=run_now
                    ).props("color=primary")
                    if not (session.has_source() and session.has_target()) or untested:
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
                        # Cancelling a read-only run is a normal (secondary) action, as
                        # Validation's "Cancel validation": nothing is half-changed.
                        cancel_button = ui.button(
                            "Cancel evaluation",
                            icon="stop_circle",
                            on_click=lambda: _cancel_evaluation(),
                        ).props("outline color=grey-8 no-caps")
                        if eval_state.cancel_requested:
                            cancel_button.disable()
                            cancel_button.tooltip(
                                "Cancel requested -- it takes effect after the table "
                                "being read. Read-only, so nothing is left half-changed."
                            )
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
                if eval_state.result is not None:
                    render_notice(
                        ui,
                        tone="info",
                        header="Showing the previous assessment",
                        body=(
                            "It stays in place, and the later steps keep using it, until "
                            "this run finishes."
                        ),
                    )
            elif eval_state.run_notice is not None:
                _tone, _header, _body = eval_state.run_notice
                render_notice(ui, tone=_tone, header=_header, body=_body)

            result = eval_state.result
            if result is not None:
                # On-demand per-object AI guidance is offered only when AI assist
                # is enabled for this session (opt-in). The strategist is built per
                # send (no network until invoked) and its graceful per-object
                # guidance call is handed to the result renderer; when AI is off
                # the provider is None and the renderer shows a disabled, clearly
                # labeled affordance instead (discoverable, never silently gone).
                guidance_provider = None
                if ai_is_usable(session):

                    def strategist_now():  # noqa: ANN202
                        # Resolved per SEND, not at result render: the app-wide panel
                        # keeps a guidance streamer while its scope stays open, so a
                        # strategist built here would keep answering -- and latching
                        # denials -- with the Bedrock model / region / AWS profile of THIS
                        # render after the operator switched to a verified one.
                        strategist = strategist_factory(
                            session.ai_assist, session.aws_profile
                        )
                        # Ground the AI DBA on THIS migration's source engine (MySQL vs
                        # PostgreSQL); the factory builds the strategist before the
                        # source type is in scope, so pin it here (best-effort — a bare
                        # fake in tests may not expose the setter).
                        try:
                            strategist.source_engine = source_engine_word(
                                getattr(session.source_config, "source_type", None)
                            )
                        except AttributeError:
                            pass
                        return strategist

                    if ai_tool_execute is not None:
                        # Give the per-object/finding chat the SAME read-only tools the
                        # general chat has, so a chat started on ONE object can still
                        # answer WIDER-migration questions (e.g. "list all unsupported
                        # objects") by looking up the real data instead of refusing.
                        def guidance_provider(  # noqa: ANN001
                            item, messages, on_delta
                        ):
                            return strategist_now().stream_object_chat(
                                item, messages, on_delta,
                                tools=ai_tools or [], execute=ai_tool_execute,
                            )
                    else:

                        def guidance_provider(  # noqa: ANN001
                            item, messages, on_delta
                        ):
                            return strategist_now().stream_object_chat(
                                item, messages, on_delta
                            )
                _render_result(
                    ui,
                    result,
                    eval_state=eval_state,
                    guidance_provider=guidance_provider,
                    open_ai_scope=open_ai_scope,
                    # So the target subsection can tell the operator when the assessed
                    # cluster is no longer the connected one.
                    live_target_endpoint=getattr(
                        getattr(session, "target_config", None),
                        "cluster_endpoint",
                        None,
                    ),
                    live_source=describe_source(getattr(session, "source_config", None)),
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
    job_token = eval_state.job_token

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
        if eval_state.job_id != job_id:
            # A leftover poll (another tab, a reconnecting client) for a job the session
            # has moved past: it must not settle -- or retire -- the current run.
            refresh()
            return
        _update_progress()
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(job.status)
        if mapped is None:
            return
        if job.status in ("FAILED", "CANCELLED"):
            # The worker has failed, been cancelled or been reaped by the watchdog (it may
            # still return later): retire ITS token so nothing it writes lands now.
            eval_state.retire_run(job_token)
            mapped = _settle_unfinished_run(
                eval_state,
                cancelled=job.status == "CANCELLED",
                error=job_manager.get_error(job_id) or "Evaluation failed.",
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
    guidance_provider: Optional[
        Callable[
            ["AssessmentItem", "list[dict[str, str]]", Callable[[str], None]],
            ObjectGuidanceOutcome,
        ]
    ] = None,
    open_ai_scope: Optional[Callable[..., object]] = None,
    live_target_endpoint: Optional[str] = None,
    live_source: Optional[str] = None,
) -> None:
    """Render the result as distinct, readable sections (one card per group).

    A migration-readiness score card leads, followed by the compatibility
    assessment card (source counts, conversion stats, and the importance-ordered
    object list, with the target name-conflict analysis as a subsection because
    it is part of the assessment report) and the export actions. There is no
    batch AI section: when AI assist is enabled, ``guidance_provider`` is a
    graceful per-object guidance call wired to a right slide-in AI drawer opened
    on demand from each actionable object; when it is ``None`` the affordance
    renders disabled with a hint instead. ``eval_state`` holds the object list's
    filters, search and pages (it is omitted only by non-UI callers, which get the
    list without its filter bar).
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
    # The band counts the TARGET too (tables / schemas / views per database), so when the
    # connected cluster is no longer the assessed one the card describes the other cluster.
    # Said here, at the card, not only in the target subsection far below it.
    if compute_migration_score(result.assessment) is not None and target_inventory_is_stale(
        result, _EndpointOnly(live_target_endpoint) if live_target_endpoint else None
    ):
        render_notice(
            ui,
            tone="warning",
            header="Readiness was assessed against another cluster",
            body=(
                "The database-level limits in this score were counted on "
                f"{result.target_endpoint}; the connected target is now "
                f"{live_target_endpoint}. Re-run Evaluation, or refresh the target catalog "
                "in Schema Conversion, to count them for the current cluster."
            ),
        )
    # The SOURCE can change too (the Connect step accepts another host or database at any
    # time), and then every finding describes a database the migration will not read.
    assessed = source_address(result.source_label)
    if assessed and live_source and not same_source_address(assessed, live_source):
        render_notice(
            ui,
            tone="warning",
            header="This report describes another source",
            body=(
                f"Evaluation read {assessed}; the connected source is now {live_source}. "
                "Re-run Evaluation before converting, so the report, the conversion and the "
                "migration describe the same database."
            ),
        )
    # Directly under the score, because it is the score that needs qualifying: an
    # evaluation over ZERO tables still produces a confident band.
    _render_empty_inventory_notice(ui, result)

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
                    ui.label(f"{count:,}").classes("text-base font-semibold")  # type: ignore[attr-defined]
                    ui.label(label).classes("text-sm text-gray-600")  # type: ignore[attr-defined]
        _render_assessment(
            ui,
            result.assessment,
            render_title=False,
            eval_state=eval_state,
            on_ai=on_ai,
            ai_enabled=ai_enabled,
        )
        # Target name-conflict detection is part of the assessment (Req: the
        # report includes target name-conflict detection), so the target analysis
        # is a subsection of this card rather than a separate top-level card.
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
        _render_target(ui, result, live_endpoint=live_target_endpoint)


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
                    # Blocked is decided by a database-level limit, not by the gauge, so the
                    # caption says what the number does measure; a bare "99 / 100 readiness"
                    # beside "Blocked" read as a contradiction.
                    caption = (
                        "per-object readiness"
                        if score.band == _BLOCKED_BAND[0]
                        else "readiness"
                    )
                    ui.label(f"{score.score} / 100 {caption}").classes(  # type: ignore[attr-defined]
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


def empty_inventory_message(result: EvaluationResult) -> "Optional[str]":
    """The "you may be on the wrong database" body, or ``None`` when there are tables.

    Zero migratable tables is almost never a real finding -- it is the wrong database. The
    old behaviour was the dangerous kind of quiet: Evaluation ran to completion over nothing
    and reported a confident readiness score, so the operator's next signal was an empty
    migration. PostgreSQL is where this bites, because a connection is scoped to ONE
    database and Aurora always provides an empty ``postgres`` alongside the real one; the
    trap is worst when the database and the schema share a name, since the existing hint
    ("PostgreSQL connects to ONE database -- enter its name") cannot tell the operator which
    of the two they typed. MySQL assesses the whole cluster when Database is blank, so it
    has no equivalent, but the message is engine-neutral and would be just as true there.

    Names the siblings when the probe could read them (``sibling_databases``), because
    that turns a dead end into one obvious next step.
    """
    inventory = result.inventory
    if inventory.tables:
        return None
    others = [name for name in (inventory.sibling_databases or []) if name]
    match = _SOURCE_ADDRESS_RE.search(result.source_label or "")
    database = (match.group("database") if match else None) or "the connected database"
    scored = compute_migration_score(result.assessment) is not None
    body = (
        f"No tables were found in {database}, so there is nothing to migrate"
        + (" and the score above is a report on an empty schema." if scored else ".")
        + " The usual cause is the wrong Database on the Connect step -- it is the "
        "DATABASE name, not the schema name."
    )
    if others:
        listed = ", ".join(others[:8])
        more = f" (and {len(others) - 8} more)" if len(others) > 8 else ""
        body += f" Other databases on this server: {listed}{more}."
    return body


def _render_empty_inventory_notice(ui: object, result: EvaluationResult) -> None:
    """Warn when an evaluation completed over ZERO tables (see :func:`empty_inventory_message`)."""
    body = empty_inventory_message(result)
    if not body:
        return
    render_notice(
        ui,
        tone="warning",
        header="No tables found — check the Database on the Connect step",
        body=body,
    )


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
        # The KIND is part of the id: a trigger, an event or a routine may share its name
        # with a table, and a shared id reopened the other object's conversation instead
        # of asking about this one.
        if concern is not None and rule:
            scope_id = f"evaluation:{item.kind}:{item.object_name}:{rule}"
            subtitle = f"{item.object_name} \u00b7 {rule}"
            seed = _concern_question(item, concern)
        else:
            scope_id = f"evaluation:{item.kind}:{item.object_name}"
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
    ui.label("Compatibility by object kind").classes("text-base font-semibold")  # type: ignore[attr-defined]
    ui.echart(option).classes("w-full").style("height: 320px")  # type: ignore[attr-defined]


def _render_assessment(
    ui: object,
    report: AssessmentReport,
    *,
    render_title: bool = True,
    eval_state: Optional["EvaluationState"] = None,
    on_ai: Optional[Callable[["AssessmentItem"], object]] = None,
    ai_enabled: bool = False,
) -> None:
    """Render the assessment chart, summary, and an importance-ordered list.

    Per-object detail is shown as an expandable accordion (not a table) so long
    Risk/Recommendation text is never truncated, ordered most-critical-first and
    showing each object's kind and classification. Two filters (Classification /
    Estimated manual effort) and a name search let the user focus on actionable
    objects in a large schema; they are driven by ``eval_state`` and omitted
    without one. The list is bounded and lazily built (see
    :func:`_render_object_list`). ``render_title`` is False when the caller already
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
                f"{classification_label(classification.value)}: {count:,}"
            ).props(f"color={color}").classes("text-sm q-px-sm q-py-xs")

    # Wrap the chart in solid separators so "Compatibility by object kind" reads as
    # a distinct section between the summary badges above and the
    # per-object list below. Only when there is a chart to show (non-empty
    # report), so an empty report does not render stray rules.
    if report.assessed_items:
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
        _render_assessment_chart(ui, report)
        ui.separator().classes("my-2")  # type: ignore[attr-defined]
    else:
        _render_assessment_chart(ui, report)

    # Per-object detail as an expandable list (accordion), ordered by importance
    # so the most critical objects are at the top. Two AWS-style dropdown filters
    # (Classification / Estimated manual effort — the same color-coded categories
    # the summary badges show) and a name search let the user focus on actionable
    # objects in a large schema instead of scrolling past every AUTO object.
    if not report.assessed_items:
        ui.label("Objects by importance (most critical first)").classes(  # type: ignore[attr-defined]
            "text-base font-semibold"
        )
        ui.label("No objects were assessed.").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
        return

    ui.label("Objects by importance (most critical first)").classes(  # type: ignore[attr-defined]
        "text-base font-semibold"
    )
    # Effort distribution, beside the list and the effort filter it describes. The count
    # is spelled out as "objects needing work" because it is NOT the object total: an
    # object with no required work (all-AUTO, or only a recommendation) carries no effort
    # estimate and so appears in none of these buckets.
    if any(item.effort is not None for item in report.assessed_items):
        with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
            ui.label("Estimated manual effort").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-700"
            )
            for level, count in report.effort_summary.items():
                ui.badge(f"{level.value}: {count:,}").props(  # type: ignore[attr-defined]
                    f"color={_EFFORT_BADGE_COLOR} outline"
                ).classes("text-sm q-px-sm q-py-xs")
            needing = sum(report.effort_summary.values())
            ui.label(  # type: ignore[attr-defined]
                f"({needing:,} of {len(report.assessed_items):,} objects need work)"
            ).classes("text-xs text-gray-500")

    # The list's filters, search and pages. A non-UI caller (no eval_state) gets the list
    # with its defaults and no filter bar.
    view = eval_state if eval_state is not None else EvaluationState()
    # Sorted once per render; a filter, the search or Clear filters only re-filters it.
    ordered = sort_assessment_items(report.assessed_items)
    controls: dict[str, object] = {}
    # Set while Clear filters puts the controls back: their change handlers then fire
    # with values already applied, and the one redraw is Clear's own.
    clearing = False

    def _sync_controls() -> None:
        """Show what the list is filtered by. The state is the session's, shared by its
        browser tabs, so another tab may have changed a filter whose old value this tab's
        control still shows -- and picking that value again would fire no change."""
        nonlocal clearing
        clearing = True
        try:
            for key, value in (
                ("classification", view.classification_filter),
                ("effort", view.effort_filter),
            ):
                control = controls.get(key)
                if control is not None and getattr(control, "value", value) != value:
                    control.set_value(value)  # type: ignore[attr-defined]
            box = controls.get("name")
            if box is not None and (getattr(box, "value", "") or "").strip() != view.name_filter:
                box.set_value(view.name_filter)  # type: ignore[attr-defined]
        finally:
            clearing = False

    def _list_changed() -> None:
        if clearing:
            return
        _sync_controls()
        # A different set of objects, so every group starts again at its first page.
        view.list_pages.clear()
        object_list.refresh()  # type: ignore[attr-defined]

    def on_clear() -> None:
        nonlocal clearing
        view.classification_filter = "ALL"
        view.effort_filter = "ALL"
        view.name_filter = ""
        clearing = True
        try:
            for key, value in (("classification", "ALL"), ("effort", "ALL"), ("name", "")):
                control = controls.get(key)
                if control is not None:
                    control.set_value(value)  # type: ignore[attr-defined]
        finally:
            clearing = False
        _list_changed()

    if eval_state is not None:
        # A handler fires only when THIS control's value changed, so it always redraws:
        # the state is the session's, shared by its browser tabs, and may already hold the
        # value another tab chose while this tab's list still shows the old one.

        def on_class(event: object) -> None:
            view.classification_filter = getattr(event, "value", None) or "ALL"
            _list_changed()

        def on_effort(event: object) -> None:
            view.effort_filter = getattr(event, "value", None) or "ALL"
            _list_changed()

        def on_name(event: object) -> None:
            view.name_filter = (getattr(event, "value", None) or "").strip()
            _list_changed()

        # The bar sits OUTSIDE the list's refreshable, so a redraw never re-creates the
        # search box under the operator's cursor.
        with filter_bar(ui):  # type: ignore[attr-defined]
            controls["classification"] = filter_select(  # type: ignore[attr-defined]
                ui,
                label="Classification",
                options={
                    "ALL": "All classifications",
                    "AUTO": classification_label("AUTO"),
                    "MANUAL": classification_label("MANUAL"),
                    "UNSUPPORTED": classification_label("UNSUPPORTED"),
                },
                value=view.classification_filter,
                on_change=on_class,
            )
            controls["effort"] = filter_select(  # type: ignore[attr-defined]
                ui,
                label="Estimated manual effort",
                options={
                    "ALL": "All efforts",
                    "SIMPLE": "Simple (< 2h)",
                    "MEDIUM": "Medium (2–6h)",
                    "SIGNIFICANT": "Significant (> 6h)",
                },
                value=view.effort_filter,
                on_change=on_effort,
            )
            controls["name"] = filter_input(
                ui,
                placeholder="Filter by object name or rule (e.g. BYTEA_KEY)",
                value=view.name_filter,
                on_change=on_name,
            )

    # Its own refreshable: a filter, the search or Clear filters redraws this list and
    # nothing else. They used to call the workflow shell's refresh, which rebuilt the whole
    # page -- sidebar, diagram, score, chart and every row -- on each click.
    @ui.refreshable  # type: ignore[attr-defined]
    def object_list() -> None:
        _render_object_list(
            ui,
            ordered,
            view=view,
            total=len(report.assessed_items),
            on_clear=on_clear,
            on_ai=on_ai,
            ai_enabled=ai_enabled,
        )

    object_list()


def _render_object_list(
    ui: object,
    ordered: list,
    *,
    view: "EvaluationState",
    total: int,
    on_clear: Callable[[], None],
    on_ai: Optional[Callable[..., object]],
    ai_enabled: bool,
) -> None:
    """The filtered list: a count line, then one section per object kind.

    What it builds is bounded whatever the source's size. A section's rows are built only
    while it is open and one page (:data:`OBJECTS_PER_PAGE`) at a time, and an object's
    finding cards only when its row is first expanded. Building every row and card up front
    -- collapsed ones included, which Quasar keeps in the DOM -- cost about 100 elements
    per object: some 98,000 for 1,000 tables, five seconds of the event loop that every
    session shares on each render. The browser's find does not see a collapsed row's
    findings either way, so the name search is the way to look an object up; the exports
    list every object.
    """
    visible = filter_assessment_items(
        ordered,
        classification=view.classification_filter,
        effort=view.effort_filter,
        name=view.name_filter,
    )
    narrowed = (
        view.classification_filter != "ALL"
        or view.effort_filter != "ALL"
        or bool(view.name_filter)
    )
    if not visible:
        with ui.row().classes("items-center gap-2"):  # type: ignore[attr-defined]
            ui.label("No objects match these filters.").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-500"
            )
            ui.button("Clear filters", on_click=on_clear).props(  # type: ignore[attr-defined]
                "flat dense no-caps color=primary"
            )
        return

    with ui.row().classes("items-center gap-2"):  # type: ignore[attr-defined]
        inline_hint(
            ui,
            (
                f"{len(visible):,} of {total:,} objects match"
                if narrowed
                else f"{total:,} object{'' if total == 1 else 's'}"
            ),
            classes="text-sm",
        )
        if narrowed:
            ui.button("Clear filters", on_click=on_clear).props(  # type: ignore[attr-defined]
                "flat dense no-caps color=primary"
            )
    with ui.column().classes("w-full gap-1"):  # type: ignore[attr-defined]
        for kind, kind_items in group_assessment_items_by_kind(visible):
            _render_kind_group(
                ui,
                kind,
                kind_items,
                view=view,
                # Narrowed by a filter or the search: the operator asked to see these.
                open_=narrowed
                or any(item.classification.value != "AUTO" for item in kind_items),
                on_ai=on_ai,
                ai_enabled=ai_enabled,
            )


def _lazy_expansion(
    ui: object,
    *,
    header: Callable[[], None],
    body: Callable[[], None],
    value: bool = False,
) -> object:
    """A ``ui.expansion`` whose content is built the first time it opens.

    ``header`` fills the header slot. ``body`` runs inside the expansion once: right away
    when it starts open, otherwise on its first open (its value-change event). Collapsing
    keeps what was built, so a re-open costs nothing. Returns the expansion.
    """
    built = False

    def build() -> None:
        nonlocal built
        if built:
            return
        built = True
        with expansion:  # type: ignore[attr-defined]
            body()

    def on_change(event: object) -> None:
        if getattr(event, "value", False):
            build()

    expansion = (
        ui.expansion(value=value, on_value_change=on_change)  # type: ignore[attr-defined]
        .classes("w-full")
        .props("expand-separator")
    )
    with expansion.add_slot("header"):
        header()
    if value:
        build()
    return expansion


def _render_kind_group(
    ui: object,
    kind: str,
    kind_items: list,
    *,
    view: "EvaluationState",
    open_: bool,
    on_ai: Optional[Callable[..., object]],
    ai_enabled: bool,
) -> None:
    """One object kind's section: a header with its counts, and its rows built lazily.

    It starts open when ``open_`` (the kind has work to do, or the list is narrowed), and
    otherwise closed and empty, so a large all-AUTO group stays one header line until the
    operator opens it.
    """

    def header() -> None:
        with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
            ui.label(kind_section_label(kind)).classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
            ui.badge(f"{len(kind_items):,}").props("color=primary")  # type: ignore[attr-defined]
            ui.label(assessment_kind_summary(kind_items)).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-500"
            )

    _lazy_expansion(
        ui,
        header=header,
        body=lambda: _render_kind_rows(
            ui, kind, kind_items, view=view, on_ai=on_ai, ai_enabled=ai_enabled
        ),
        value=open_,
    )


def _render_kind_rows(
    ui: object,
    kind: str,
    kind_items: list,
    *,
    view: "EvaluationState",
    on_ai: Optional[Callable[..., object]],
    ai_enabled: bool,
) -> None:
    """One page of a section's rows, most critical first, under a page control when the
    section has more than one page.

    The page is kept per kind in ``view.list_pages``, so a redraw of the list keeps it, and
    turning it redraws this section's rows only. The control sits above the rows, where it
    stays in view after the page turns (Cloudscape puts a collection's pagination there).
    """
    rows_box = ui.column().classes("gap-1 w-full pl-2")  # type: ignore[attr-defined]
    # The page THIS section shows: the stored one is the session's, which another browser
    # tab may already have turned to the page this one is asked for.
    shown: dict[str, int] = {}

    def turn(event: object) -> None:
        page = int(getattr(event, "value", None) or 1)
        if page == shown.get("page"):
            return
        view.list_pages[kind] = page
        rows_box.clear()  # type: ignore[attr-defined]
        with rows_box:  # type: ignore[attr-defined]
            draw()

    def draw() -> None:
        rows, page, pages = assessment_page(kind_items, view.list_pages.get(kind, 1))
        shown["page"] = view.list_pages[kind] = page
        if pages > 1:
            first = (page - 1) * OBJECTS_PER_PAGE + 1
            with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                inline_hint(
                    ui,
                    f"Showing {first:,} to {first + len(rows) - 1:,} of "
                    f"{len(kind_items):,}. The exports list every object.",
                )
                ui.space()  # type: ignore[attr-defined]
                ui.pagination(  # type: ignore[attr-defined]
                    1, pages, direction_links=True, value=page, on_change=turn
                ).props("max-pages=7 boundary-numbers")
        for item in rows:
            _render_assessment_item(ui, item, on_ai=on_ai, ai_enabled=ai_enabled)

    with rows_box:  # type: ignore[attr-defined]
        draw()


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
    """Render one assessed object as an expandable row: its findings summary in the
    header, and its finding cards (:func:`_render_assessment_item_body`) built when the
    row is first expanded.

    A collapsed row is about 6 elements, and its cards add about 18 per finding once
    built. They used to be built for every row, collapsed or not, so a 1,000-table source
    with five findings per table paid for 5,000 cards nobody had opened.
    """
    color = _CLASS_BADGE_COLOR.get(item.classification.value, "grey")

    def header() -> None:
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

    _lazy_expansion(
        ui,
        header=header,
        body=lambda: _render_assessment_item_body(
            ui, item, on_ai=on_ai, ai_enabled=ai_enabled
        ),
    )


def _render_assessment_item_body(
    ui: object,
    item: "AssessmentItem",
    *,
    on_ai: Optional[Callable[["AssessmentItem"], object]] = None,
    ai_enabled: bool = False,
) -> None:
    """One object's finding cards: a risk and its own recommendation per matched rule.

    Each card carries a compact on-demand AI-guidance icon for that finding
    (``on_ai``); when AI assist is off the icon is shown disabled with a hint, so the
    affordance is discoverable rather than silently missing.
    """
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
            "AI DBA is not available: turn AI Assist on in the Connect screen (set the "
            "Bedrock model and re-test), or, if Amazon Bedrock denied access, fix the "
            "permission or model access and re-verify AI access there."
        )


def _render_target(
    ui: object,
    result: EvaluationResult,
    *,
    live_endpoint: "Optional[str]" = None,
) -> None:
    """Render the target catalog summary and any pre-existing-object conflicts.

    ``live_endpoint`` is the CURRENTLY connected DSQL endpoint. When the assessed target
    was a different cluster, the whole subsection is replaced by one notice: the counts
    would describe the wrong cluster, and -- worse -- the "No conflicts on the target"
    success notice below is a FALSE ALL-CLEAR, telling the operator the target is clean
    when the connected cluster may already hold the very tables about to be created.
    """
    if target_inventory_is_stale(
        result, _EndpointOnly(live_endpoint) if live_endpoint else None
    ):
        ui.label("Target analysis (Aurora DSQL)").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        render_notice(
            ui,
            tone="warning",
            header="Target analysis is out of date",
            body=(
                f"The target was assessed on {result.target_endpoint}; the connected "
                f"target is now {live_endpoint}. Re-run Evaluation to assess the "
                "current target — the catalog counts and conflict list below would "
                "describe the other cluster."
            ),
        )
        return
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
        # Bounded like the object list: a target that already holds 1,000 of the tables
        # drew 1,000 badges. The exports list every one.
        shown = result.target_conflicts[:TARGET_CONFLICT_BADGES]
        with ui.row().classes("items-center gap-1 flex-wrap"):  # type: ignore[attr-defined]
            for name in shown:
                ui.badge(name).props("color=amber-7 outline")  # type: ignore[attr-defined]
            if len(result.target_conflicts) > len(shown):
                ui.label(  # type: ignore[attr-defined]
                    f"and {len(result.target_conflicts) - len(shown):,} more (every one is "
                    "in the exported report)"
                ).classes("text-xs text-gray-500")
        ui.label(  # type: ignore[attr-defined]
            "Resolve these in Schema Conversion: choose SKIP (keep the existing "
            "object) or REPLACE (recreate it) when you apply."
        ).classes("text-xs text-gray-500")
    blocked = _find_target_blockers(
        result.inventory, target, is_postgres=result.source_type is SourceType.POSTGRES
    )
    if blocked:
        from dsql_migrator.core.target_introspector import BLOCKED_NAMES_ADVICE

        render_notice(
            ui,
            tone="error",
            header="Names held on the target by another object",
            body=(
                f"{len(blocked)} source object(s) cannot be created in either apply mode "
                "while another object holds the name on the target (Aurora DSQL keeps a "
                "schema's tables, views and indexes in one namespace; an earlier apply may "
                f"have left it). {BLOCKED_NAMES_ADVICE}"
            ),
        )
        with ui.row().classes("items-center gap-1 flex-wrap"):  # type: ignore[attr-defined]
            for name, holder in blocked:
                ui.badge(f"{name}: {holder}").props("color=red-7 outline")  # type: ignore[attr-defined]
    if not result.target_conflicts and not blocked:
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
            "Full report: what was assessed, the assessment and the target analysis (deterministic; no AI)."
        )
        ui.button(  # type: ignore[attr-defined]
            "JSON",
            on_click=lambda: _download(assessment_download(result, "json")),
        ).props("dense outline").tooltip(
            "Assessment data with its header and the target's existing names (deterministic; no AI)."
        )
        ui.button(  # type: ignore[attr-defined]
            "Text",
            on_click=lambda: _download(assessment_download(result, "text")),
        ).props("dense outline").tooltip(
            "The same, human-readable (deterministic; no AI)."
        )


__all__ = [
    "EvaluationInputs",
    "EvaluationResult",
    "run_evaluation",
    "compute_migration_score",
    "MigrationScore",
    "with_refreshed_catalogs",
    "chat_turns_remaining",
    "filter_assessment_items",
    "assessment_page",
    "OBJECTS_PER_PAGE",
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
