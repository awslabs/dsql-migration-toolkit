# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The column-exclusion read-models of the Data Migration screen (extracted from
``_models.py``, dmshared-07).

Pure, NiceGUI-free: the oversized-LOB candidates and their preselection, the exclusion
context and the effective exclusions (:func:`effective_lob_exclusions`), the restored and
forced-replace views, the stored-entry overlay and its issues, the rules popup's texts and
resolution, the exclusion drift, the quarantine rule suggestion, the exclusion row's
summary, the exclusion refusals and block reasons, and what CDC captures
(:func:`cdc_capture_exclusions`, :func:`capture_exclusions_for`, the deployed capture, the
mismatch and the ambiguity warning).

Its top-level imports are ``core`` and the standard library only. It reads two names of
``_models`` -- ``session_source_type`` and ``MigrationType`` -- inside the functions that
use them, because ``_models`` imports this module at its top: ``_models`` re-exports every
name defined here, private ones included, so ``_models.<name>`` (the screen, the tests and
the E2E harness bind them there) resolves unchanged.

A re-export does not carry a monkeypatch: a patch on ``_models`` reaches a reader that
imports the name from ``_models`` at call time (``cdc_catalog_drift_gate``, the lazy
importers in ``_cdc_ui``, ``validation``, ...), and the two ``_models`` names this module
imports inside its functions; it does not reach the calls this module's own functions make
to each other, which a test patches here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.assessor import (
    _OVERSIZED_LOB_BASES,
    _base_type,
    is_pg_oversized_lob_type,
    pg_oversized_lob_column_names,
)
from dsql_migrator.core.models import SourceInventory, SourceType


# Aurora DSQL rejects a single text/bytea/json value over 1 MiB; a row over the Kafka
# client limit kills the source task. The CDC screen reuses the
# evaluation OVERSIZED_LOB type set (_OVERSIZED_LOB_BASES) and base-type parser
# (_base_type) imported above, so the exclusion offer stays in lock-step with the
# evaluation rule instead of duplicating the type list.


@dataclass(frozen=True)
class LobExclusionCandidate:
    """One table's columns that CDC can optionally exclude at capture.

    ``columns`` are the column names flagged by the evaluation ``OVERSIZED_LOB``
    rule (MySQL ``mediumtext/longtext/mediumblob/longblob/json``) whose values can
    exceed the Aurora DSQL 1 MiB per-value limit. Excluding them (the column then
    migrates as NULL, and Debezium's ``column.exclude.list`` never captures it) is the
    safe handling for values that can also exceed the CDC message size limit --
    runtime isolation cannot catch those.
    """

    table: str
    columns: tuple[str, ...]


def lob_exclusion_candidates(
    inventory: Optional[SourceInventory],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> list[LobExclusionCandidate]:
    """Return the oversized-LOB columns per table, sorted by table name.

    Pure: derived directly from the source inventory's column types, so the CDC
    screen can offer exclusion without re-running evaluation. The classification is
    chosen by ``source_type`` -- MySQL ``mediumtext/longtext/mediumblob/longblob/json`` vs
    PostgreSQL's ``is_pg_oversized_lob_type`` -- because ``column.mysql_type`` holds the
    SOURCE engine's type name (the MySQL set matched against PG types finds at most the
    shared ``json`` spelling).
    Both come from :mod:`core.assessor`, so this offer, the Evaluation
    ``OVERSIZED_LOB`` rule and the Schema Conversion note can never disagree about which
    columns are at risk. Primary-key columns are never offered (a PK can't be dropped);
    returns ``[]`` when nothing qualifies.
    """
    if inventory is None:
        return []
    candidates: list[LobExclusionCandidate] = []
    for table in inventory.tables:
        pk = set(table.primary_key)
        if source_type is SourceType.POSTGRES:
            # Table-aware: a CHECK limiting the column to a finite literal set means it
            # cannot hold an oversized value, so it must not be OFFERED for exclusion
            # either -- excluding it would drop a column for nothing.
            at_risk = set(pg_oversized_lob_column_names(table))
        else:
            at_risk = {
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) in _OVERSIZED_LOB_BASES
            }
        columns = tuple(
            column.name
            for column in table.columns
            if column.name in at_risk and column.name not in pk
        )
        if columns:
            candidates.append(
                LobExclusionCandidate(table=table.name, columns=columns)
            )
    candidates.sort(key=lambda c: c.table)
    return candidates


# Source LOB base types split by the DSQL type they convert to, so a quarantine reason
# that names the DSQL type can be narrowed back to the source columns that produced it.
# (MySQL converter: mediumblob/longblob -> bytea, mediumtext/longtext -> text, json -> json.)
# Every member of _OVERSIZED_LOB_BASES must sit in exactly one bucket (pinned by a test), or
# a quarantine naming its type pre-ticks nothing. A ``jsonb`` reason (a json column re-typed
# jsonb in Schema Conversion) contains ``json`` and so pre-ticks the source json column.
_LOB_BASES_BY_TARGET_TYPE: "dict[str, frozenset[str]]" = {
    "bytea": frozenset({"mediumblob", "longblob"}),
    "text": frozenset({"mediumtext", "longtext"}),
    "json": frozenset({"json"}),
}

# The PostgreSQL map. PG -> DSQL is the IDENTITY for these types, so the source spelling
# equals the target type named in the reason -- which is precisely why the MySQL map found
# nothing for a PG source and the dialog opened with nothing pre-ticked. Kept as a separate
# dict rather than merged into the one above: a MySQL source has its own ``text`` type, and
# although it can never reach this helper today (``_OVERSIZED_LOB_BASES`` deliberately
# omits plain ``text``/``blob`` as too small to exceed 1 MiB), merging would make that
# exclusion load-bearing for CORRECTNESS -- widen the MySQL set later and a MySQL ``text``
# column would start being pre-ticked from a ``text`` reason. ``json``/``jsonb`` map to
# themselves and appear in a reason under their own name.
_PG_LOB_BASES_BY_TARGET_TYPE: "dict[str, frozenset[str]]" = {
    "bytea": frozenset({"bytea"}),
    "text": frozenset(
        {"text", "character varying", "varchar", "character", "char", "bpchar"}
    ),
    "jsonb": frozenset({"jsonb"}),
    "json": frozenset({"json"}),
}


def preselect_lob_columns_for_reason(
    columns_with_types: "Sequence[tuple[str, str]]",
    reason_text: str,
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> tuple[str, ...]:
    """Return the LOB columns a quarantine reason points at, for pre-ticking a picker.

    The offending COLUMN is not recoverable from the quarantine record: it carries only
    ``table`` / ``pk`` / ``error_code`` / ``message``, and the message keeps just the first
    line of the driver error -- which names the DSQL TYPE, not the column
    (``datatype limit greater than 1048576 bytes not supported for bytea``).

    That type token is still a real signal, because the source->DSQL mapping is
    deterministic: for MySQL ``mediumblob``/``longblob`` become ``bytea``,
    ``mediumtext``/``longtext`` become ``text``, and ``json`` stays ``json``; for PostgreSQL
    the mapping is the identity.
    So a ``bytea`` reason narrows a table with both a blob and a text LOB column
    (``product_media`` has ``content`` and ``full_description``) down to the blob one.

    ``source_type`` picks the mapping. Without it a PostgreSQL source matched PG spellings
    against MySQL base names and pre-ticked NOTHING (today: only a json column, the one
    spelling the engines share) -- not a blocker (the dialog's documented else-branch
    explains the empty selection and the action refuses an empty pick), but one
    avoidable click on the recovery path.

    This only PRE-SELECTS; the caller must still show the choice, because excluding the
    wrong column silently NULLs it for the whole table. Returns ``()`` when the reason
    names no known type or nothing matches, which means "make the user choose".
    """
    lowered = (reason_text or "").lower()
    by_target = (
        _PG_LOB_BASES_BY_TARGET_TYPE
        if source_type is SourceType.POSTGRES
        else _LOB_BASES_BY_TARGET_TYPE
    )
    matched: list[str] = []
    # Longest target name first so a "jsonb" reason is not also matched by the "json"
    # entry, which would pre-tick a json column for a jsonb failure.
    for target_type in sorted(by_target, key=len, reverse=True):
        if target_type not in lowered:
            continue
        for column, mysql_type in columns_with_types:
            # The single source of truth decides whether this column can hold an oversized
            # value at all, so the pre-tick can never disagree with the candidate offer. The
            # bucket match alone strips the length modifier, so a bounded
            # `character varying(50)` fell into the `text` bucket -- pre-ticking a column
            # that CANNOT exceed 1 MiB, in a dialog whose confirm NULLs it for every row.
            # Unreachable today only because the candidate list already excludes bounded
            # types; keyed here so the two filters cannot drift apart, and so widening the
            # core set later does not silently reopen the empty-pre-tick symptom.
            if source_type is SourceType.POSTGRES and not is_pg_oversized_lob_type(
                mysql_type or ""
            ):
                continue
            base = str(mysql_type or "").split("(")[0].strip().lower()
            if base in by_target[target_type] and column not in matched:
                matched.append(column)
        break
    return tuple(matched)


def make_lob_candidates_for(inventory, session, migration_state=None):
    """Build the post-quarantine LOB picker's candidate lookup for one session.

    Module-level, not a closure inside the page builder, so it can be TESTED. It used to be
    nested, which is what let the engine-unaware call inside it ship: every test of the
    button injects a lambda for this callable, so no test ever ran the real body, and the
    only guard left was an AST assertion that the ``source_type`` keyword appears -- which a
    hard-coded ``SourceType.MYSQL`` would also satisfy. Closing over just ``inventory`` and
    ``session`` made hoisting it a two-argument factory.

    The picker needs the TYPES, not just the names: the quarantine reason names the DSQL
    type (``bytea``/``text``/``json``) rather than the column, and the source type is what
    maps back to it. :class:`LobExclusionCandidate` carries names only, so the types come
    from the inventory here.

    With ``migration_state`` the lookup also drops every column the exclusion rule refuses
    (a composite target key, a column a preserved foreign key references, a column Aurora
    DSQL computes), so neither recovery route offers an "Exclude & reload" that the rule
    would then refuse -- or, before the rule existed, accept and break the reload.
    """

    def lob_candidates_for(table_name: str):
        if inventory is None:
            return ()
        table = next((t for t in inventory.tables if t.name == table_name), None)
        if table is None:
            return ()
        from dsql_migrator.ui.data_migration._models import session_source_type

        allowed = {
            c.table: set(c.columns)
            # The engine MUST be passed: the parameter defaults to MySQL, whose type names
            # (mediumtext/longblob/...) match a PostgreSQL inventory's format_type spellings
            # only on the shared ``json`` -- never text/bytea/jsonb. Before MySQL json joined
            # that set the result was empty, and the caller's "no candidates -> do not
            # offer a dead button" early return hid the ONLY post-quarantine recovery
            # action for every PostgreSQL migration; without the engine it would still hide
            # it on a table with no json column, and elsewhere offer only the json column.
            for c in (
                lob_exclusion_candidates(
                    inventory, source_type=session_source_type(session)
                )
                or ()
            )
        }.get(table_name, set())
        if allowed and migration_state is not None:
            from dsql_migrator.core.column_exclusion import column_exclusion_verdict

            ctx = exclusion_context_for(
                migration_state,
                inventory,
                selected_tables=exclusion_scope(migration_state),
                focus_tables=[table_name],
            )
            allowed = {
                c for c in allowed
                if not column_exclusion_verdict(table_name, c, ctx).blocked
            }
        return tuple(
            (col.name, col.mysql_type) for col in table.columns if col.name in allowed
        )

    return lob_candidates_for


def scope_lob_candidates(
    candidates: Sequence[LobExclusionCandidate],
    *,
    selected_tables: Optional[Sequence[str]],
    stored_selection: Sequence[str],
) -> list[LobExclusionCandidate]:
    """Scope oversized-LOB candidates to the tables that will actually be migrated.

    ``selected_tables`` is the caller's RESOLVED effective selection (e.g. the Full
    Load screen's ``effective_migration_selection``): when provided, filter strictly
    to it -- so selecting a single schema lists only that schema's LOB columns even
    though ``stored_selection`` (``migration_state.selection.selected_tables``) is
    still the empty "= all" default until the picker is touched (the Schema-Conversion
    pick only pre-ticks the picker). When ``None``, fall back to ``stored_selection``
    (empty => all), the legacy behavior for callers that can't resolve the effective
    set. Pure, so the scoping is unit-testable without a live screen. Both name forms
    are qualified ``db.table`` (matches ``TableDef.name`` / ``TableSelection``).
    """
    if selected_tables is not None:
        scoped = set(selected_tables)
        return [c for c in candidates if c.table in scoped]
    stored = set(stored_selection)
    if not stored:
        return list(candidates)
    return [c for c in candidates if c.table in stored]


@dataclass(frozen=True)
class ExclusionInputs:
    """What every exclusion gate judges, resolved LIVE by the Data Migration screen.

    ``selected`` -- the tables the migration moves (the picker, else the Schema Conversion
    default); ``applied`` -- the applied conversion (edits honoured); ``live_columns`` --
    each existing target table's columns from the target catalog (``None`` when there is
    no current catalog).
    """

    selected: "list[str]"
    applied: "Mapping[str, object]"
    preserve_foreign_keys: bool
    live_columns: "Optional[Mapping[str, frozenset[str]]]" = None
    # The tree's universe (tables with a target to load into), in its order; the applied
    # view DDLs (W6, view findings); the conversion signature the applied set is keyed by.
    migratable: "list[str]" = field(default_factory=list)
    view_ddls: "Mapping[str, str]" = field(default_factory=dict)
    signature: tuple = ()


def live_target_columns_of(target_inventory) -> "Optional[dict[str, frozenset[str]]]":
    """``{schema.table: column names}`` of the target catalog's tables, or ``None``."""
    if target_inventory is None:
        return None
    return {
        f"{rel.schema_name}.{rel.name}": frozenset(c.name for c in rel.columns)
        for schema in getattr(target_inventory, "schemas", None) or ()
        for rel in getattr(schema, "tables", None) or ()
    }


def exclusion_context_for(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
    selected_tables: "Optional[Sequence[str]]" = None,
    focus_tables: "Optional[Iterable[str]]" = None,
    view_ddls: "Optional[Mapping[str, str]]" = None,
    warnings: bool = False,
):
    """The :class:`~dsql_migrator.core.column_exclusion.ExclusionContext` for this session.

    ``applied_conversions`` / ``preserve_foreign_keys`` default to what the last Data
    Migration render recorded (``DataMigrationState.set_applied_conversions``), so the CDC
    builders and Validation -- which do not hold the Schema Conversion state -- judge a
    column against the same applied target DDL as Full Load. Before any render (a restored
    session) that record is empty and the rule falls back to the source schema plus the
    stored ``message.key.columns`` map, which is itself derived from the applied DDL.
    """
    from dsql_migrator.core.column_exclusion import build_exclusion_context

    getter = getattr(migration_state, "applied_conversions", None)
    cached, cached_preserve = getter() if callable(getter) else ({}, True)
    mkc_getter = getattr(migration_state, "cdc_message_key_columns", None)
    return build_exclusion_context(
        inventory,
        cached if applied_conversions is None else applied_conversions,
        message_key_columns=mkc_getter() if callable(mkc_getter) else None,
        preserve_foreign_keys=(
            cached_preserve if preserve_foreign_keys is None else preserve_foreign_keys
        ),
        selected_tables=selected_tables,
        focus_tables=focus_tables,
        view_ddls=view_ddls,
        warnings=warnings,
    )


def effective_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """The exclusion set EVERY path uses: the stored set minus key columns (B1/B2).

    One answer for Full Load, the CDC ``column.exclude.list`` and Validation, so the paths
    can no longer disagree. Before this, the stored set went to each path raw: Full Load
    silently skipped a source-PK entry (``apply_lob_exclusions``) while the CDC path sent
    it to Debezium, and a Schema Conversion composite key's leading column was not
    protected on either path (the importer reads every target-key column from each row).
    A valid set passes through unchanged, so every normal session behaves exactly as
    before. With no inventory there is nothing to judge against, and the stored set is
    returned as-is (the paths that need an inventory refuse to run without one).
    """
    stored = migration_state.lob_exclusions()
    if inventory is None or not getattr(inventory, "tables", None):
        return {table: frozenset(cols) for table, cols in stored.items() if cols}
    from dsql_migrator.core.column_exclusion import effective_exclusions

    ctx = exclusion_context_for(
        migration_state,
        inventory,
        applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
        focus_tables=stored.keys(),
    )
    return dict(effective_exclusions(stored, ctx).columns)


@dataclass(frozen=True)
class RestoredExclusionFacts:
    """What a pre-overlay snapshot said about its exclusions, kept until they are
    classified (legacy = data already moved under the old semantics)."""

    entries: "frozenset[tuple[str, str]]"
    job_id: Optional[str]
    connector_names: "tuple[str, ...]"
    cdc_started: bool
    stack_possible: bool
    # True once stage 1 (the job evidence) found nothing and only the CDC probe can
    # decide; stage 2 never runs before it.
    stage1_passed: bool = False


def _chunk_moved(chunk) -> bool:
    """Evidence a Full Load chunk wrote (or tried to write) rows: it left PENDING, was
    attempted, or counted loaded / skipped / quarantined rows."""
    return (
        getattr(chunk, "status", "PENDING") != "PENDING"
        or (getattr(chunk, "attempts", 0) or 0) > 0
        or (getattr(chunk, "rows_loaded", 0) or 0) > 0
        or (getattr(chunk, "rows_skipped", 0) or 0) > 0
        or (getattr(chunk, "rows_quarantined", 0) or 0) > 0
    )


def classify_restored_exclusions(migration_state, job_manager=None) -> "Optional[bool]":
    """Classify a restored pre-overlay session's exclusions ONLY on evidence of writes.

    ``True`` legacy (omit-only), ``False`` upgraded (overlay entries), ``None`` still
    pending (behaves as legacy). Idempotent; reads jobs only.

    Stage 1 (needs ``job_manager``): legacy when the Full Load job is missing (a pruned
    record is not proof of nothing), holds a chunk that left PENDING or loaded rows, or
    the snapshot names connectors or a CDC start. With none of that and no possible CDC
    stack, upgrade. Stage 2 (state only, once the CDC probe has landed): legacy when CDC
    started, connectors are known, a different Full Load job ran meanwhile, an offset is
    committed or the stack is running; upgrade when the stack is absent or infra-only
    with no committed offset; otherwise still pending. Infra inputs and an infra deploy
    are never evidence.
    """
    pending = getattr(migration_state, "exclusion_classification_pending", None)
    if not callable(pending) or not pending():
        return None
    facts = migration_state.restored_exclusion_facts()
    if facts is None:
        return None

    def _settle(legacy: bool) -> bool:
        migration_state.finalize_exclusion_classification(legacy=legacy)
        return legacy

    if job_manager is not None:
        if facts.job_id is not None:
            from dsql_migrator.core.job_manager import JobNotFoundError

            try:
                job = job_manager.get_status(facts.job_id)
            except JobNotFoundError:
                job = None
            if job is None:
                return _settle(True)
            if any(_chunk_moved(chunk) for chunk in getattr(job, "chunks", ()) or ()):
                return _settle(True)
            # The newest job can be all PENDING (a retry re-seeds its chunks, a run that
            # ended before its first table leaves them untouched) while an earlier run
            # of the lineage moved rows: look at every job the store knows for these
            # tables before calling it clean.
            tables = {table for table, _column in facts.entries}
            lister = getattr(job_manager, "list_jobs", None)
            for other in (lister() if callable(lister) else ()) or ():
                if any(
                    chunk.chunk_id in tables and _chunk_moved(chunk)
                    for chunk in getattr(other, "chunks", ()) or ()
                ):
                    return _settle(True)
        if facts.connector_names or facts.cdc_started:
            return _settle(True)
        if not facts.stack_possible:
            return _settle(False)
        if not facts.stage1_passed:
            from dataclasses import replace as _replace

            facts = _replace(facts, stage1_passed=True)
            migration_state.set_restored_exclusion_facts(facts)
    if not facts.stage1_passed:
        return None  # the job evidence has not been judged yet
    # Stage 2: what this session's own probe and runs have seen since the restore.
    if (
        getattr(migration_state, "cdc_ops_window_start", None) is not None
        or getattr(migration_state, "cdc_connector_names", None)
        or getattr(migration_state, "job_id", None) != facts.job_id
        or getattr(migration_state, "cdc_has_committed_offset", False)
    ):
        return _settle(True)
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        return None
    phase = getattr(migration_state, "cdc_stack_phase", None)
    if phase == "running":
        return _settle(True)
    if phase in ("absent", "infra"):
        return _settle(False)
    return None


@dataclass(frozen=True)
class ForcedReplaceView:
    """What the Start dialog learned about the column-exclusion overlay.

    ``forced``: table -> why it is dropped and reloaded (the NULL contract needs it);
    ``appended_warnings``: table -> why a table that would be forced is appended anyway
    (CDC is streaming into it: the engine's exemption, which the UI does not reach since
    G129); ``undecided``: tables whose target could not be read (never
    forced -- the engine then refuses them if needed); ``null_columns``: table -> the
    columns that load as NULL.
    """

    forced: "dict[str, str]"
    appended_warnings: "dict[str, str]"
    undecided: "tuple[str, ...]"
    null_columns: "dict[str, tuple[str, ...]]"


def exclusion_forced_replace(
    action_tables: "Sequence[str]",
    *,
    overlay_by_table: "Mapping[str, Iterable[str]]",
    excluded_by_table: "Mapping[str, Iterable[str]]",
    source_columns_for: "Callable[[str], Sequence[str]]",
    facts: "Mapping[str, object]",
    ledger_tables: "Mapping[str, object]",
    cdc_coexisting: bool,
    recreated_unfinished: "Iterable[str]",
    index_ddls_for: "Callable[[str], Sequence[str]]",
) -> ForcedReplaceView:
    """Which action tables the Start dialog must drop and reload, and why.

    Per table, in order: an overlay or recorded table whose target facts are unknown is
    UNDECIDED (never forced). Otherwise the one shared verdict
    (``core.overlay_ledger.append_verdict``, the engine's own check) decides whether an
    append would break the NULL contract; a table the tool recreated and did not finish
    (``recreated_unfinished``) is reloaded so its indexes are built. While CDC streams
    nothing is forced: each such table would be appended with a warning, as the engine
    exempts a load beside a streaming sink -- a load the UI no longer runs (G129: the
    dialog cannot be confirmed then, and it shows no such warning, G132). Live computed
    columns are omit-only and never force.
    """
    from dsql_migrator.core.overlay_ledger import append_verdict

    unfinished = set(recreated_unfinished or ())
    forced: dict[str, str] = {}
    warnings: dict[str, str] = {}
    undecided: list[str] = []
    null_columns: dict[str, tuple[str, ...]] = {}
    for table in action_tables:
        overlay = frozenset(overlay_by_table.get(table, ()) or ())
        record = ledger_tables.get(table)
        if not overlay and record is None and table not in unfinished:
            continue
        table_facts = facts.get(table)
        refusal = None
        if table in unfinished and not cdc_coexisting:
            # Reloading it needs no facts: it was recreated and not finished, so its rows
            # are the tool's own partial load and its indexes were never built.
            forced[table] = (
                "recreated by the last run and not finished; it is dropped and reloaded "
                "so its indexes are built"
            )
            if overlay:
                null_columns[table] = tuple(sorted(overlay))
            continue
        if overlay or record is not None:
            if table_facts is None:
                if cdc_coexisting and table in unfinished:
                    ddls = list(index_ddls_for(table) or ())
                    warnings[table] = (
                        f"{table} was recreated by the last run and not finished; while CDC "
                        "streams it is appended, so its indexes are not built: "
                        + ("; ".join(ddls) if ddls else "none recorded")
                    )
                undecided.append(table)
                if overlay:
                    null_columns[table] = tuple(sorted(overlay))
                continue
            live = sorted(
                c for c in overlay
                if (f := table_facts.columns.get(c)) is not None and not f.computed
            )
            computed = sorted(
                c for c in overlay
                if (f := table_facts.columns.get(c)) is not None and f.computed
            )
            if live:
                null_columns[table] = tuple(live)
            if getattr(table_facts, "exists", True):
                refusal, _reincluded = append_verdict(
                    record,
                    table_facts=table_facts,
                    overlay_live=live,
                    computed=computed,
                    excluded=tuple(excluded_by_table.get(table, ()) or ()),
                    source_columns=tuple(source_columns_for(table) or ()),
                )
        if cdc_coexisting:
            if refusal is not None:
                warnings[table] = (
                    f"{table} cannot be dropped while CDC streams: existing rows keep "
                    f"their values in {', '.join(refusal.columns)}; Validation will count "
                    "them."
                )
            elif table in unfinished:
                ddls = list(index_ddls_for(table) or ())
                warnings[table] = (
                    f"{table} was recreated by the last run and not finished; while CDC "
                    "streams it is appended, so its indexes are not built: "
                    + ("; ".join(ddls) if ddls else "none recorded")
                )
            continue
        if refusal is not None and refusal.kind == "reincluded":
            forced[table] = (
                f"{', '.join(refusal.columns)} hold NULL from an earlier exclusion set; it "
                "is dropped and reloaded so they get their source values"
            )
        elif refusal is not None:
            forced[table] = (
                "already holds rows with a different exclusion set (or none recorded); it "
                f"is dropped and reloaded so every row is NULL in {', '.join(refusal.columns)}"
            )
    return ForcedReplaceView(forced, warnings, tuple(undecided), null_columns)


def legacy_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """The omit-only entries (legacy, and a still-pending classification) within the
    effective set, so a key column (B1/B2) is never among them."""
    effective = effective_lob_exclusions(
        migration_state, inventory, applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
    )
    getter = getattr(migration_state, "legacy_exclusions", None)
    legacy = getter() if callable(getter) else {}
    out = {t: frozenset(effective.get(t, frozenset()) & cols) for t, cols in legacy.items()}
    return {t: cols for t, cols in out.items() if cols}


def overlay_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """THE overlay set: the effective set minus legacy / pending entries minus the columns
    Aurora DSQL computes (B4 by the applied DDL). The CANDIDATE set: the run further skips
    columns absent live and live-computed ones.

    Contract: omitted = ``effective_lob_exclusions`` (unchanged); overlay = this; legacy =
    omitted - overlay - computed. A legacy-only table has no overlay column, so it is
    never forced to reload and appends with a warning.
    """
    effective = effective_lob_exclusions(
        migration_state, inventory, applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
    )
    if not effective:
        return {}
    getter = getattr(migration_state, "legacy_exclusions", None)
    legacy = getter() if callable(getter) else {}
    generated: "Mapping[str, frozenset[str]]" = {}
    if inventory is not None and getattr(inventory, "tables", None):
        generated = exclusion_context_for(
            migration_state, inventory, applied_conversions=applied_conversions,
            preserve_foreign_keys=preserve_foreign_keys, focus_tables=effective.keys(),
        ).generated
    out: dict[str, frozenset[str]] = {}
    for table, columns in effective.items():
        kept = frozenset(columns) - frozenset(legacy.get(table, ())) - frozenset(
            generated.get(table, ())
        )
        if kept:
            out[table] = kept
    return out


def stored_exclusion_issues(
    migration_state,
    inventory,
    *,
    committed: bool,
    selected_tables: "Optional[Sequence[str]]" = None,
):
    """Judge the STORED exclusions (see ``column_exclusion.check_stored_exclusions``).

    ``committed`` must be the exclusion card's own ``locked`` flag. Before it locks, a
    blocked or vanished entry is an ERROR that holds Start Full Load, Deploy CDC
    infrastructure and Start CDC -- and the card, being unlocked, offers the Remove that
    the gate names. Once it locks (data moved, or the set was handed to the CDC pipeline)
    the same entry is only a warning: blocking there would point at a Remove the frozen
    card cannot perform.

    ``selected_tables`` (the tables this migration moves) scopes the check: an entry on a
    source table that is NOT selected affects nothing and is ignored, while an entry on a
    table that is no longer in the source at all is still reported -- its table may have
    been renamed, and the exclusion the user asked for would silently lapse. ``None``
    falls back to the stored selection (pinned by the prerequisite checks, which every
    gated button requires first), and to every table when that is still empty -- so the
    card and the gate it serves judge the same scope and the gate can never name an
    entry the card does not list. Empty with no stored entries or no inventory.
    """
    stored = migration_state.lob_exclusions()
    if not stored or inventory is None or not getattr(inventory, "tables", None):
        return ()
    from dsql_migrator.core.column_exclusion import check_stored_exclusions

    if selected_tables is None:
        selected_tables = exclusion_scope(migration_state)
    if selected_tables is not None:
        known = {t.name for t in inventory.tables}
        selected = set(selected_tables)
        stored = {t: c for t, c in stored.items() if t in selected or t not in known}
        if not stored:
            return ()
    ctx = exclusion_context_for(
        migration_state,
        inventory,
        selected_tables=selected_tables,
        focus_tables=stored.keys(),
    )
    return check_stored_exclusions(stored, ctx, committed=committed)


def exclusion_scope(migration_state) -> "Optional[list[str]]":
    """The tables the exclusion rule treats as migrated when the caller has no resolved set.

    The selection the last Data Migration render RESOLVED (the same ``selected_names`` the
    Full Load card is given -- an explicit "unselect all" stays ``[]``); before any render,
    the stored selection, or ``None`` -- every table -- while that is still the empty
    "= all" default. One definition, so the card, every gate (Continue, Start Full Load,
    the header Run, Deploy, Attach) and the recovery actions judge the same tables.
    """
    getter = getattr(migration_state, "resolved_selection", None)
    resolved = getter() if callable(getter) else None
    if resolved is not None:
        return resolved
    selection = getattr(migration_state, "selection", None)
    return list(getattr(selection, "selected_tables", None) or ()) or None


# --- the exclusion popup's pure composition (core.exclusion_rules over live inputs) ----


def exclusion_rules_text_for(migration_state, inventory) -> str:
    """The saved rule text, else lines synthesized from the stored set (a session that
    never saved rules: legacy, card ticks)."""
    from dsql_migrator.core.exclusion_rules import synthesize_rule_text

    saved = migration_state.exclusion_rules_text()
    if saved is not None:
        return saved
    return synthesize_rule_text(migration_state.lob_exclusions(), migration_state.legacy_exclusions())


def popup_open_text(migration_state, inventory) -> "tuple[str, list[str]]":
    """What a fresh open shows -- the saved (or synthesized) rules plus the pending lines --
    and those pending lines (a Save removes exactly them)."""
    text = exclusion_rules_text_for(migration_state, inventory)
    pending = migration_state.pending_rules()
    if pending:
        text = "\n".join([text.rstrip("\n")] + pending) if text.strip() else "\n".join(pending)
    return text, pending


def popup_resolution(
    migration_state,
    inventory,
    text: str,
    *,
    inputs: "ExclusionInputs",
    source_type: SourceType,
    selection_lock_reason: "Optional[str]" = None,
    capture_tables: "Optional[Callable[[Sequence[str]], Sequence[object]]]" = None,
    memo_slot: str = "editor",
    convert_legacy: "Iterable[str]" = (),
    add_lock=None,
):
    """:func:`~dsql_migrator.core.exclusion_rules.resolve_exclusion_rules` over this
    session's live inputs: the ONE context (selected' + focus, warnings on), the resolved
    selection, the provenance and re-ticks, the legacy (omit-only) entries, the stored set
    for the diff, the applied DDL and live columns for 42703 and the target changes, and --
    when ``capture_tables(selected')`` is given (CDC types) -- the capture preview.

    ``add_lock`` (Full load + CDC after its Full Load, before CDC starts, G16: the table
    picker's per-table add-lock, ``_table_set_lock.TableSetAddLock``) turns a table line
    that would select a table the lock refuses -- or unselect one the Full Load read in parts
    (G137) -- into a per-table E3; ``None`` changes nothing.

    Memoized on the state (transient) by the text's digest and every input, the inventory,
    applied set and live columns held by IDENTITY (a reused ``id`` never serves a stale
    result), so the row and the popup re-resolve only when something changed. One entry
    per ``memo_slot`` (the row's saved rules, the editor's text), so typing in the popup
    does not evict the row's comparison. ``convert_legacy``: tables whose omit-only entries
    Change & reload converts (its ticked checkboxes) -- resolved as overlay entries, so
    their warnings and ALTERs are the ones the reload really runs.
    """
    import hashlib

    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules

    stored = migration_state.lob_exclusions()
    converted = set(convert_legacy or ())
    legacy = {t: c for t, c in migration_state.legacy_exclusions().items()
              if t not in converted}
    prev = migration_state.rule_excluded_tables()
    reticks = migration_state.rule_reticked_tables()
    mkc_getter = getattr(migration_state, "cdc_message_key_columns", None)
    mkc = mkc_getter() if callable(mkc_getter) else {}
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    key = (
        hashlib.sha256((text or "").encode("utf-8")).hexdigest(),
        tuple(inputs.signature), tuple(inputs.selected), tuple(inputs.migratable),
        frozenset(prev), frozenset(reticks),
        tuple(sorted((t, tuple(c)) for t, c in (mkc or {}).items())),
        getattr(getattr(migration_state, "migration_type", None), "value", None),
        selection_lock_reason,
        frozenset((t, c) for t, cs in legacy.items() for c in cs),
        frozenset((t, c) for t, cs in stored.items() for c in cs),
        deployed if capture_tables is not None else None,
        capture_tables is not None,
        source_type,
        add_lock,
    )
    held = (inventory, inputs.applied, inputs.live_columns, inputs.view_ddls)
    memos = getattr(migration_state, "_exclusion_resolution_memo", None)
    if not isinstance(memos, dict):
        memos = {}
        migration_state._exclusion_resolution_memo = memos
    memo = memos.get(memo_slot)
    if memo and memo[0] == key and all(a is b for a, b in zip(memo[1], held)):
        return memo[2]
    tables = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    migratable = [tables[n] for n in inputs.migratable if n in tables]

    def factory(selected, focus):
        return exclusion_context_for(
            migration_state, inventory, applied_conversions=inputs.applied,
            preserve_foreign_keys=inputs.preserve_foreign_keys, selected_tables=selected,
            focus_tables=focus, view_ddls=inputs.view_ddls, warnings=True,
        )

    capture = None
    if capture_tables is not None:
        def capture(columns, selected):
            return capture_exclusions_for(
                columns, columns, inventory, list(capture_tables(selected)), deployed,
                applied=inputs.applied)

    reselect_lock = None
    unselect_lock = None
    if add_lock is not None:
        def reselect_lock(names):
            blocked = add_lock.blocked(names, tree=False)
            return (blocked, add_lock.refusal(blocked, tree=False)) if blocked else None

        # G137: a table the Full Load read in parts stays selected (CDC must replay it).
        def unselect_lock(names):
            blocked = add_lock.removal_blocked(names)
            return (
                (blocked, add_lock.removal_refusal(blocked, tree=False)) if blocked else None
            )

    resolution = resolve_exclusion_rules(
        text,
        migratable=migratable,
        source_type=source_type,
        effective_selection=inputs.selected,
        rule_excluded_prev=prev,
        selection_lock_reason=selection_lock_reason,
        retick_overrides=reticks,
        context_factory=factory,
        applied_conversions=inputs.applied,
        live_columns=inputs.live_columns,
        legacy=legacy,
        stored_columns=stored,
        capture=capture,
        reselect_lock=reselect_lock,
        unselect_lock=unselect_lock,
    )
    memos[memo_slot] = (key, held, resolution)
    return resolution


@dataclass(frozen=True)
class ExclusionDrift:
    """What re-resolving the SAVED rules would change now (after Refresh objects, a tree
    tick): shown as a chip and in the Start dialog, never applied."""

    added: "tuple[tuple[str, str], ...]"
    removed: "tuple[tuple[str, str], ...]"
    unselects: "tuple[str, ...]"
    reselects: "tuple[str, ...]"
    # Saved lines that no longer resolve (e.g. a dropped table), and a global error.
    error_lines: "tuple[int, ...]" = ()
    global_error: bool = False
    # Columns the rules exclude on a table re-ticked in the tree since the last Save.
    retick_columns: "tuple[tuple[str, str], ...]" = ()


def exclusion_drift(
    migration_state, resolution, *, migratable: "Optional[Iterable[str]]" = None
) -> "Optional[ExclusionDrift]":
    """The difference between ``resolution`` (of the saved rules) and the stored state, or
    ``None``. A rule-excluded table re-ticked in the tree is an override, not drift -- but
    the column rules that would apply to it are reported. Lines that no longer resolve are
    reported too, and never hide the column changes the rest of the rules make; a table
    change under the selection lock (E3) is not drift. A stored entry on a table unticked
    in the tree (migratable, out of scope) is not drift either: the rules still match it,
    the table is simply not migrated. One on a table gone from the inventory still is."""
    if resolution is None:
        return None
    stored = migration_state.lob_exclusions()
    scope = set(resolution.in_scope)
    universe = set(migratable) if migratable is not None else None
    now = {(t, c) for t, cs in resolution.columns.items() for c in cs}
    before = {(t, c) for t, cs in stored.items() for c in cs
              if t in scope or (universe is not None and t not in universe)}
    reticks = migration_state.rule_reticked_tables()
    locked = any(e.code == "E3" for es in resolution.errors.values() for e in es) or any(
        e.code == "E3" for e in resolution.global_errors)
    unselects = () if locked else tuple(
        t for t in resolution.tables_to_unselect if t not in reticks)
    reselects = () if locked else tuple(resolution.tables_to_reselect)
    error_lines = tuple(sorted(n for n, es in resolution.errors.items()
                               if any(e.code != "E3" for e in es)))
    drift = ExclusionDrift(
        tuple(sorted(now - before)), tuple(sorted(before - now)), unselects, reselects,
        error_lines, any(e.code != "E3" for e in resolution.global_errors),
        tuple(resolution.retick_columns),
    )
    if not (drift.added or drift.removed or drift.unselects or drift.reselects
            or drift.error_lines or drift.global_error or drift.retick_columns):
        return None
    return drift


def open_lob_candidates(candidates, excluded: "Mapping[str, Iterable[str]]"):
    """Oversized-LOB candidates minus the columns ``excluded`` already excludes. Works on
    :class:`LobExclusionCandidate` lists and on one table's ``(column, type)`` pairs
    (``excluded`` then maps that table); empty tables drop out."""
    if candidates and isinstance(candidates[0], LobExclusionCandidate):
        out = []
        for c in candidates:
            gone = set(excluded.get(c.table, ()) or ())
            kept = tuple(col for col in c.columns if col not in gone)
            if kept:
                out.append(LobExclusionCandidate(table=c.table, columns=kept))
        return out
    gone = set(excluded or ())
    return tuple(pair for pair in candidates or () if pair[0] not in gone)


def overlay_change_summary(actions: "Iterable[str]") -> str:
    """"N x DROP NOT NULL (one-way), M x DROP DEFAULT (restored at Cut over)" for the
    overlay statements' actions -- ONE wording for the popup and both Start dialogs."""
    actions = list(actions)
    n_not_null = sum(1 for a in actions if a == "DROP NOT NULL")
    n_default = sum(1 for a in actions if a == "DROP DEFAULT")
    parts = []
    if n_not_null:
        parts.append(f"{n_not_null} x DROP NOT NULL (one-way)")
    if n_default:
        parts.append(f"{n_default} x DROP DEFAULT (restored at Cut over)")
    return ", ".join(parts)


def start_dialog_exclusion_notes(migration_state) -> "list[tuple[str, str]]":
    """The Start Full Load dialog's exclusion lines, ``(tone, text)``: the tables the RULES
    unselected (in the tree) are not loaded, and the row's drift -- never applied, never a
    gate."""
    notes: list[tuple[str, str]] = []
    getter = getattr(migration_state, "exclusion_inputs", None)
    inputs = getter() if callable(getter) else None
    provenance = migration_state.rule_excluded_tables()
    if inputs is not None:
        # Only the ones still unselected: never "not loaded" for a selected table.
        provenance = (provenance & set(inputs.migratable)) - set(inputs.selected)
    if provenance:
        n = len(provenance)
        notes.append(("info", f"{n} table{'s' if n != 1 else ''} unselected by your rules "
                      f"{'is' if n == 1 else 'are'} not loaded (left as is on Aurora DSQL)."))
    drift = migration_state.exclusion_drift_text()
    if drift:
        notes.append(("warning", f"{drift} in Exclude tables & columns."))
    return notes


def deploy_exclusion_disclosure(migration_state, inventory) -> "Optional[tuple[str, str]]":
    """The Deploy CDC confirm's one info line, ``(header, details)``, or ``None`` when
    nothing is excluded: what the stack is created with, and that changing it later needs
    Delete CDC infrastructure. Never a gate (the gates stay in the deploy block notice)."""
    stored = migration_state.lob_exclusions()
    columns = sum(len(c) for c in stored.values())
    getter = getattr(migration_state, "exclusion_inputs", None)
    inputs = getter() if callable(getter) else None
    provenance = migration_state.rule_excluded_tables()
    if inputs is not None:
        # Only the ones still unselected: never "not loaded" for a selected table.
        provenance = (provenance & set(inputs.migratable)) - set(inputs.selected)
    if not columns and not provenance:
        return None
    parts = []
    if provenance:
        parts.append(f"{len(provenance)} table{'s' if len(provenance) != 1 else ''}")
    if columns:
        parts.append(f"{columns} column{'s' if columns != 1 else ''}")
    header = (f"Excluded: {', '.join(parts)}. They are fixed once deployed: changing them "
              "later needs Delete CDC infrastructure.")
    names = ", ".join(f"{t}.{c}" for t in sorted(stored) for c in sorted(stored[t]))
    sent = ""
    if inventory is not None and getattr(inventory, "tables", None):
        wanted = set(inputs.selected) if inputs is not None else None
        captured = [t for t in inventory.tables if wanted is None or t.name in wanted]
        sent = cdc_capture_exclusions(migration_state, inventory, captured).sent
    plain = ",".join(f"{t}.{c}" for t in sorted(stored) for c in sorted(stored[t]))
    if names and sent == plain:
        # The capture list is the same names: say so once instead of listing them twice.
        return header, f"Columns (sent as ColumnExcludeList unchanged): {names}."
    details = (f"Columns: {names or '(none)'}. Sent as ColumnExcludeList: "
               f"{sent or '(nothing excluded at capture)'}")
    return header, details


QUARANTINE_RULE_COMMENT = "# from the Full Load quarantine"


@dataclass(frozen=True)
class QuarantineSuggestion:
    """What the "Exclude column & reload" route does with one quarantined table.

    ``outcome``: ``A`` (``lines`` to insert, one per column the reason's type names that
    the open rules leave open; ``kept`` maps a column an include keeps to its line),
    ``NOT_SAVED`` (only an unsaved or pending line -- ``not_saved_lines`` -- excludes it:
    Save), ``B`` (the reason names no open column: pick one; ``filter_table`` when section
    4 has rows for it) or ``C`` (the saved rules exclude every flagged column: the value
    is in another column)."""

    outcome: str
    lines: "tuple[str, ...]" = ()
    kept: "Mapping[str, tuple[int, ...]]" = field(default_factory=dict)
    not_saved_lines: "tuple[int, ...]" = ()
    filter_table: bool = False
    # Preselected columns whose name holds a line break: no rule can name them.
    unwritable: "tuple[str, ...]" = ()


def quarantine_rule_suggestion(
    table: str,
    reason_text: str,
    candidates: "Sequence[tuple[str, str]]",
    *,
    open_excluded: "Iterable[str]",
    saved_excluded: "Iterable[str]",
    include_keepers: "Mapping[str, tuple[int, ...]]" = (),
    open_excluded_by: "Mapping[str, tuple[int, ...]]" = (),
    source_type: SourceType = SourceType.MYSQL,
) -> QuarantineSuggestion:
    """Decide the quarantine route's outcome for ``table`` (first match wins), from its
    ``(column, type)`` LOB candidates. OPEN = minus what the OPEN rule text excludes (the
    editor, else the saved rules plus pending lines) -- it decides which lines to insert,
    so none duplicates one already there; SAVED = minus the effective (saved) set -- it
    decides the copy when nothing is inserted, so a column only an unsaved line excludes is
    never treated as fixed. ``include_keepers``/``open_excluded_by`` map a column to the
    include lines keeping it / the open lines excluding it."""
    from dsql_migrator.core.exclusion_rules import rule_name_writable, rule_part_token

    keepers = dict(include_keepers or {})
    by_line = dict(open_excluded_by or {})
    open_c = open_lob_candidates(candidates, open_excluded)
    saved_c = open_lob_candidates(candidates, saved_excluded)
    pre_open = preselect_lob_columns_for_reason(open_c, reason_text, source_type=source_type)
    if pre_open:
        schema, _sep, rest = table.partition(".")
        lines, kept, unwritable = [], {}, []
        for column in pre_open:
            if column in keepers:
                kept[column] = tuple(keepers[column])
                continue
            if not all(rule_name_writable(x) for x in (schema, rest, column)):
                unwritable.append(column)
                continue
            lines.append("exclude " + ".".join(rule_part_token(x) for x in (schema, rest, column))
                         + f"  {QUARANTINE_RULE_COMMENT}")
        return QuarantineSuggestion("A", tuple(lines), kept, unwritable=tuple(unwritable))
    pre_saved = preselect_lob_columns_for_reason(saved_c, reason_text, source_type=source_type)
    if pre_saved:
        lines = tuple(sorted({n for c in pre_saved for n in by_line.get(c, ())}))
        return QuarantineSuggestion("NOT_SAVED", not_saved_lines=lines)
    if saved_c:
        if open_c:
            return QuarantineSuggestion("B", filter_table=True)
        lines = tuple(sorted({n for c, _t in saved_c for n in by_line.get(c, ())}))
        return QuarantineSuggestion("B", not_saved_lines=lines)
    return QuarantineSuggestion("C")


@dataclass(frozen=True)
class ExclusionRowSummary:
    """The button row's summary under the tree: ``variant`` is ``nothing``, ``counts``,
    ``locked``, ``drift`` or ``blocking`` (a notice box, not a hint); the LOB line counts
    the type-based candidates the open rules leave open in the selected tables."""

    variant: str
    text: str
    lob_count: int = 0
    lob_text: "Optional[str]" = None
    lob_tone: str = "info"


def _count_text(tables: int, columns: int) -> str:
    parts = []
    if tables:
        parts.append(f"{tables} table{'s' if tables != 1 else ''}")
    if columns:
        parts.append(f"{columns} column{'s' if columns != 1 else ''}")
    return ", ".join(parts)


def exclusion_row_summary(
    migration_state,
    *,
    locked: bool,
    drift: "Optional[ExclusionDrift]",
    block_reason: "Optional[str]",
    blocking_count: int = 0,
    lob_candidates: "Sequence[LobExclusionCandidate]" = (),
    measured_over: "Iterable[str]" = (),
    migratable: "Optional[Iterable[str]]" = None,
) -> ExclusionRowSummary:
    """The row's variant and copy. "N tables" counts only the RULE-excluded tables (the
    tree counter stays the one selection number); ``lob_candidates`` are already scoped
    and minus the open exclusions; the LOB line turns warning only when a measured
    over-limit value (``measured_over``, (table, column) pairs) is still not excluded."""
    stored = migration_state.lob_exclusions()
    columns = sum(len(c) for c in stored.values())
    provenance = migration_state.rule_excluded_tables()
    # Provenance outlives a table leaving the tree (so it stays excluded if it returns);
    # the count is of the tables the tree shows.
    tables = len(provenance & set(migratable)) if migratable is not None else len(provenance)
    counts = _count_text(tables, columns)
    lob_count = sum(len(c.columns) for c in lob_candidates)
    lob_text = None
    lob_tone = "info"
    if lob_count:
        lob_text = (
            f"{lob_count} column{'s' if lob_count != 1 else ''} in the selected tables can "
            "exceed Aurora DSQL's value limit (type-based). Review them in Exclude tables & "
            "columns."
        )
        open_names = {(c.table, col) for c in lob_candidates for col in c.columns}
        found = len(open_names & set(measured_over))
        if found:
            # A measured offender, not only a type: say so, or the warning tone is the
            # only difference from the type-based line.
            lob_tone = "warning"
            from dsql_migrator.ui.data_migration._models import MigrationType

            mtype = getattr(migration_state, "migration_type", None)
            consequence = (
                "each such row is dead-lettered by CDC (kept out of Aurora DSQL)"
                if mtype is MigrationType.CDC_ONLY
                else "each such row is quarantined at load"
                + (" (or dead-lettered during CDC)"
                   if mtype is MigrationType.FULL_LOAD_AND_CDC else "")
            )
            # "(measured)", not "(Check sizes)": the size cache is filled by the popup's
            # Check sizes AND by the prerequisite run's SOURCE_VALUE_SIZE answers
            # (DataMigrationState.set_prereq_report), so a user who only ran the checks
            # was credited with a click they never made.
            lob_text = (
                f"{found} column{'s' if found != 1 else ''} in the selected tables "
                f"{'hold' if found != 1 else 'holds'} a value over Aurora DSQL's limit "
                f"(measured): {consequence}. Review them in Exclude tables & columns."
            )
    if block_reason:
        n = blocking_count or 1
        text = (f"{n} saved exclusion{'s' if n != 1 else ''} no longer appl"
                f"{'y' if n != 1 else 'ies'}. Starting is blocked until you review them.")
        return ExclusionRowSummary("blocking", text, lob_count, lob_text, lob_tone)
    if locked:
        if drift is not None and drift.retick_columns:
            # Full load only after a load (G12): a rule-unselected table ticked again in the
            # tree has none of the column rules it lost; a reload would load those columns.
            n = len(drift.retick_columns)
            names = ", ".join(f"{t}.{c}" for t, c in drift.retick_columns[:3])
            more = f" and {n - 3} more" if n > 3 else ""
            text = (f"Your rules exclude {names}{more} on tables ticked again after your last "
                    f"Save (not excluded now), so a reload would load "
                    f"{'it' if n == 1 else 'them'} - use Change & reload")
            return ExclusionRowSummary("drift", text, lob_count, lob_text, lob_tone)
        text = f"{counts} excluded - locked" if counts else "Nothing excluded - locked"
        return ExclusionRowSummary("locked", text, lob_count, lob_text, lob_tone)
    if drift is not None:
        bits = []
        if drift.error_lines or drift.global_error:
            where = ", ".join(f"L{n}" for n in drift.error_lines)
            bits.append(f"have lines that no longer resolve ({where})" if where
                        else "have an error")
        if drift.added:
            names = ", ".join(f"{t}.{c}" for t, c in drift.added[:3])
            more = f" and {len(drift.added) - 3} more" if len(drift.added) > 3 else ""
            n = len(drift.added)
            bits.append(f"match {n} more column{'s' if n != 1 else ''} ({names}{more})")
        if drift.removed:
            n = len(drift.removed)
            bits.append(f"no longer match {n} saved column{'s' if n != 1 else ''}")
        if drift.unselects or drift.reselects:
            bits.append("would change the table selection")
        if drift.retick_columns:
            names = ", ".join(f"{t}.{c}" for t, c in drift.retick_columns[:3])
            more = f" and {len(drift.retick_columns) - 3} more" if len(drift.retick_columns) > 3 else ""
            bits.append(f"exclude {names}{more} on tables ticked again after your last Save "
                        "(not excluded now)")
        text = "Your rules now " + "; ".join(bits) + " - review"
        return ExclusionRowSummary("drift", text, lob_count, lob_text, lob_tone)
    if counts:
        return ExclusionRowSummary("counts", f"Rules exclude {counts}", lob_count, lob_text,
                                   lob_tone)
    return ExclusionRowSummary("nothing", "Nothing excluded", lob_count, lob_text, lob_tone)


def exclusion_refusal(migration_state, inventory, table_name: str, columns) -> "Optional[str]":
    """Why the rule refuses to exclude ``columns`` of ``table_name``, or ``None``.

    The click-time check for the two recovery actions (Full Load quarantine "Exclude &
    reload", CDC dead-letter "Exclude at capture"), which write the exclusion directly and
    never pass a prerequisite check: a refused column (or one the source no longer has) is
    reported and NOTHING is written. With no inventory there is nothing to judge against.
    """
    if not columns or inventory is None or not getattr(inventory, "tables", None):
        return None
    from dsql_migrator.core.column_exclusion import check_stored_exclusions

    ctx = exclusion_context_for(
        migration_state,
        inventory,
        selected_tables=exclusion_scope(migration_state),
        focus_tables=[table_name],
    )
    issues = check_stored_exclusions({table_name: set(columns)}, ctx, committed=False)
    if not issues:
        return None
    return " ".join(i.message for i in issues) + " Nothing was changed."


def missing_column_block_reason(
    migration_state,
    inventory,
    *,
    selected_tables: "Optional[Sequence[str]]" = None,
    job_manager=None,
) -> "Optional[str]":
    """The gate text for source columns the load would fail on (42703), or ``None``.

    Judged over the tables this migration moves (``selected_tables``, else
    :func:`exclusion_scope`, else every table) against the applied conversion, minus the
    EFFECTIVE exclusions -- an excluded column is not written, so it is not a problem.
    Only a CERTAIN failure is reported: a column the applied DDL lacks is let through when
    the live target table (from the target catalog) still has it, because an append into
    that table succeeds -- as it did in 0.1.561 -- whatever an unapplied edit says. Not
    tied to the exclusion card's lock (the remedy is Schema Conversion, which a stack does
    not block); skipped while CDC streams (Schema Conversion Apply and Start Full Load are
    held then anyway). Unknown (no conversion yet) means no claim.
    """
    if inventory is None or not getattr(inventory, "tables", None):
        return None
    if job_manager is not None:
        from dsql_migrator.ui.data_migration._cdc_state import cdc_streaming_started

        if cdc_streaming_started(migration_state, job_manager):
            return None
    getter = getattr(migration_state, "applied_conversions", None)
    applied, _preserve = getter() if callable(getter) else ({}, True)
    if not applied:
        return None
    from dsql_migrator.core.column_exclusion import missing_target_columns

    scope = selected_tables if selected_tables is not None else exclusion_scope(migration_state)
    wanted = None if scope is None else set(scope)
    tables = [t for t in inventory.tables if wanted is None or t.name in wanted]
    missing = missing_target_columns(
        tables, applied, effective_lob_exclusions(migration_state, inventory)
    )
    live_getter = getattr(migration_state, "live_target_columns", None)
    live = live_getter() if callable(live_getter) else None
    if live:
        missing = {
            table: kept
            for table, cols in missing.items()
            if (kept := tuple(c for c in cols if c not in live.get(table, frozenset())))
        }
    if not missing:
        return None
    pairs = [f"{table}.{column}" for table, cols in missing.items() for column in cols]
    shown = ", ".join(pairs[:3]) + (f" and {len(pairs) - 3} more" if len(pairs) > 3 else "")
    many = len(pairs) > 1
    them = "them" if many else "it"
    mtype = getattr(getattr(migration_state, "migration_type", None), "value", "")
    consequence = (
        f"CDC would dead-letter every change to {them}"
        if mtype == "cdc_only"
        else f"the load would fail on {them} and CDC would dead-letter every change to {them}"
        if mtype == "full_load_and_cdc"
        else f"the load would fail on {them}"
    )
    return (
        f"{shown} {'are' if many else 'is'} not in your Aurora DSQL table"
        f"{'s' if len(missing) > 1 else ''}; {consequence} (42703, undefined column). "
        f"Add {them} back to the target DDL in Schema Conversion. "
        "Columns are never dropped from a migration automatically."
    )


def stored_exclusion_block_reason(
    issues, *, where: str = "in Exclude tables & columns (under the table list)"
) -> "Optional[str]":
    """The gate text for stored-entry ERRORs, or ``None`` when nothing blocks.

    "Above" holds on every gated button: the card renders under the table picker, above
    all sub-steps, for a type with a Full Load, and above Deploy inside the CDC sub-step
    for CDC only.
    """
    from dsql_migrator.core.column_exclusion import IssueSeverity

    errors = [i for i in issues if i.severity is IssueSeverity.ERROR]
    if not errors:
        return None
    shown = " ".join(i.message for i in errors[:3])  # each message ends with a period
    more = f" And {len(errors) - 3} more." if len(errors) > 3 else ""
    many = len(errors) > 1
    return (
        f"{len(errors)} saved column exclusion{'s' if many else ''} no longer "
        f"appl{'y' if many else 'ies'}: {shown}{more} Remove {'them' if many else 'it'} "
        f"{where} first."
    )


def cdc_excluded_columns(
    lob_exclusions: "Mapping[str, Sequence[str]]",
    inventory: object,
) -> "dict[str, list[str]]":
    """The columns CDC must not stream: the operator's LOB exclusions PLUS the columns
    Aurora DSQL computes itself.

    A preserved ``GENERATED ALWAYS AS (expr) STORED`` column is maintained by the TARGET, and
    DSQL REJECTS a write that supplies a value for it (SQLSTATE 428C9, a permanent error the
    sink dead-letters). So it must be in Debezium's ``column.exclude.list`` for the same
    reason Full Load omits it from the INSERT.

    In practice PostgreSQL 17 and earlier do not publish generated columns at all, so this is
    belt-and-braces there; it becomes load-bearing on PostgreSQL 18, whose publications can
    opt into publishing them (``publish_generated_columns``).

    Derived from the SOURCE inventory with the converter's own predicate, so it agrees with
    what Schema Conversion emits. Caveat worth knowing: if the operator EDITS the target DDL
    to delete the GENERATED clause, the target column becomes ordinary and would then want
    the stream -- re-run Schema Conversion's apply so the two agree.
    """
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    merged: dict[str, set[str]] = {
        table: set(columns) for table, columns in (lob_exclusions or {}).items()
    }
    for table in getattr(inventory, "tables", None) or ():
        preserved = [column.name for column in pg_preserved_generated_columns(table)]
        if preserved:
            merged.setdefault(table.name, set()).update(preserved)
    return {table: sorted(columns) for table, columns in merged.items() if columns}


def format_column_exclude_list(
    selected: dict[str, Sequence[str]],
) -> str:
    """Format selected exclusions as a Debezium ``column.exclude.list`` value.

    Maps ``{table: [col, ...]}`` to the comma-separated ``db.table.column`` form
    the connector template's ``ColumnExcludeList`` parameter expects, sorted for
    a stable, reviewable value. ``table`` keys are the fully-qualified
    ``db.table`` names; empty input yields ``""`` (exclude nothing).
    """
    entries: list[str] = []
    for table in sorted(selected):
        for column in sorted(selected[table]):
            entries.append(f"{table}.{column}")
    return ",".join(entries)


@dataclass(frozen=True)
class DeployedCaptureExclusions:
    """The operator exclusions an attached stack's ColumnExcludeList carries, PROVEN by a
    byte-for-byte round trip. ``form``: ``"exact"`` (only the 0.1.562+ formula reproduces
    it), ``"legacy"`` (only the 0.1.561 formula does) or ``"plain"`` (both do). The form
    says which VERSION deployed the stack -- never whether its entries were overlaid: a
    0.1.562+ session can hold omit-only (legacy) entries too."""

    columns: "dict[str, list[str]]"
    form: str


def deployed_capture_exclusions(
    migration_state, inventory
) -> "Optional[DeployedCaptureExclusions]":
    """Read the attached stack's ColumnExcludeList back into operator columns, or ``None``.

    * 0.1.562+ (exact) reading: each entry matched the way Debezium matches it against the
      stack's OWN captured tables (its TableIncludeList decided any ``(?-i)`` marker and
      the compact grouping), minus the generated columns the value itself carries, then
      re-formatted with exactly those generated columns.
    * 0.1.561 (legacy) reading, tried whenever that fails: the value was the raw stored set
      (any inventory table, captured or not) plus EVERY inventory table's generated columns,
      unescaped -- so each entry is a literal ``db.table.column`` of the inventory.

    Accepted only when the reading's own formula reproduces the deployed value exactly.
    ``None``: no value recorded, a stack table missing from the inventory, or a value
    neither formula reproduces.
    """
    from dsql_migrator.core.cdc import (
        format_column_exclude_value,
        infer_column_exclusions,
        split_column_exclude_value,
    )
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    if deployed is None:
        return None
    if deployed == "":
        return DeployedCaptureExclusions({}, "plain")
    tables = list(getattr(inventory, "tables", None) or ())
    by_name = {t.name: t for t in tables}
    generated_all = generated_capture_columns(migration_state, tables)

    def _operator(found):
        return {
            t: sorted(set(cols) - generated_all.get(t, set()))
            for t, cols in found.items()
            if set(cols) - generated_all.get(t, set())
        }

    legacy_of = lambda operator: format_column_exclude_list(  # noqa: E731
        cdc_excluded_columns(operator, inventory)
    )
    names = list(getattr(migration_state, "cdc_reconciled_table_names", None) or ())
    if names and all(name in by_name for name in names):
        captured = [by_name[name] for name in names]
        found = infer_column_exclusions(deployed, captured)
        if found is not None:
            operator = _operator(found)
            # Re-formatted from what the VALUE matched (operator + the generated columns it
            # carries), so a generated column that appeared or went away since deploy does
            # not fail the reading.
            exact = format_column_exclude_value(found, captured)
            if deployed == exact:
                return DeployedCaptureExclusions(
                    operator, "plain" if deployed == legacy_of(operator) else "exact"
                )
    literal = {f"{t.name}.{c.name}": (t.name, c.name) for t in tables for c in t.columns}
    found: dict[str, set[str]] = {}
    for entry in split_column_exclude_value(deployed):
        if entry not in literal:
            return None
        table, column = literal[entry]
        found.setdefault(table, set()).add(column)
    # Re-formatted from the literal entries themselves (generated columns as the value
    # carries them), so a generated column that appeared since deploy does not fail it.
    if deployed == format_column_exclude_list({t: sorted(c) for t, c in found.items()}):
        return DeployedCaptureExclusions(_operator(found), "legacy")
    return None


def capture_exclusion_mismatch(
    migration_state, inventory, capture: "CdcCaptureExclusions", captured_tables
) -> "Optional[str]":
    """Why Start CDC must not send ``capture`` to the attached stack, or ``None``.

    Start re-sends ColumnExcludeList on every pass, so a session whose set differs from a
    stack that already streamed would silently change what CDC captures for rows it has
    been streaming under the other set (live: a fresh session sent ``""`` over a stack
    excluding 5 columns, so changes would have carried values the loaded rows hold as
    NULL). Compared STRUCTURALLY on the tables captured both now and by the stack, so a
    legitimate string change (0.1.561 -> exact form, a changed table set, a generated
    column that appeared) is not refused. Nothing to refuse when the bytes already match,
    the stack never streamed, or no stack value is known yet.
    """
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    if deployed is None or capture.sent == deployed:
        return None
    phase = getattr(migration_state, "cdc_stack_phase", None)
    # "Streamed" = the stack was ever started: a committed offset, connectors detected, or a
    # phase only a started stack reaches (an update that failed rolling back is "unstable").
    if not (
        getattr(migration_state, "cdc_has_committed_offset", False)
        or getattr(migration_state, "cdc_connector_names", None)
        or phase in ("running", "partial", "unstable")
    ):
        return None
    now_names = {getattr(t, "name", None) for t in captured_tables or ()}
    stack_names = set(getattr(migration_state, "cdc_reconciled_table_names", None) or ())
    shared = (now_names & stack_names) if stack_names else now_names
    generated = generated_capture_columns(migration_state, captured_tables)
    session = {
        t: sorted(set(cols) - generated.get(t, set()))
        for t, cols in capture.structured.items()
        if t in shared and set(cols) - generated.get(t, set())
    }
    stack = deployed_capture_exclusions(migration_state, inventory)
    name = getattr(migration_state, "cdc_stack_name", None) or "the CDC infrastructure"

    def _named(columns) -> str:
        flat = [f"{t}.{c}" for t, cols in sorted(columns.items()) for c in cols]
        if not flat:
            return "no columns"
        return ", ".join(flat[:6]) + (f" and {len(flat) - 6} more" if len(flat) > 6 else "")

    if stack is None:
        how = (
            "Delete the CDC infrastructure, change the rules in Exclude tables & columns, "
            "then deploy it again."
            if getattr(migration_state, "job_id", None) is None
            else "Delete the CDC infrastructure and deploy it again: it then captures with "
            "this session's set, the one its Full Load used (changing that set needs Start "
            "over)."
        )
        return (
            f"'{name}' has streamed with an excluded-column list that cannot be read "
            "against this session's inventory (a table it captures is missing here, or the "
            "list does not match these columns exactly), so Start CDC cannot tell whether "
            f"sending this session's set would change what CDC captures. {how}"
        )
    stack_cols = {t: cols for t, cols in stack.columns.items() if t in shared}
    if stack_cols == session:
        return None
    remedy = (
        "Delete the CDC infrastructure and deploy it again with this session's exclusions"
        + (
            ", or Start over and attach it again so the session takes the stack's exclusions."
            if getattr(migration_state, "job_id", None) is None
            else " (this session's Full Load already wrote its rows under this session's set)."
        )
    )
    return (
        f"'{name}' has streamed while excluding {_named(stack_cols)}, but this session "
        f"would exclude {_named(session)}. Start CDC sends the session's set, so changes "
        f"CDC streams from here on would not match the rows already streamed. {remedy}"
    )


@dataclass(frozen=True)
class CdcCaptureExclusions:
    """Everything the CDC surfaces need about ``column.exclude.list``, from ONE computation.

    ``structured`` is ``{db.table: [columns]}``: the EFFECTIVE exclusion set plus the
    STORED generated columns of the CAPTURED tables. ``value`` is its exact-match form
    (``core.cdc.format_column_exclude_value``) and ``legacy`` the exact 0.1.561 value for
    the same session (raw stored set, every inventory table's generated columns, the
    unescaped formatter). ``sent`` is what Start CDC sends against the stack recorded by
    the last discovery -- the preview shows it, so preview == sent. ``error`` (the value is
    over CloudFormation's 4,096-byte parameter; never for a value the stack already
    carries) blocks Deploy, Start CDC and -- for Full load + CDC -- Start Full Load, each
    with the remedy true in its state; ``inexact`` lists the names a stack KEPT on the
    legacy form does not match exactly.
    """

    structured: "dict[str, list[str]]"
    value: str
    legacy: str
    sent: str
    error: "Optional[str]" = None
    inexact: "tuple[str, ...]" = ()
    # Bytes the automatically added generated columns ALONE need: over the limit, fewer
    # operator exclusions cannot fix a size refusal.
    generated_bytes: int = 0
    # Excluded names Debezium cannot tell apart from another captured column.
    ambiguous: "Mapping[str, list[str]]" = field(default_factory=dict)


def cdc_capture_exclusions(
    migration_state, inventory, captured_tables: "Sequence[object]"
) -> CdcCaptureExclusions:
    """Build :class:`CdcCaptureExclusions` for the tables CDC captures (see there), from
    the session's stored set and the stack recorded by the last discovery."""
    return capture_exclusions_for(
        effective_lob_exclusions(migration_state, inventory),
        migration_state.lob_exclusions(),
        inventory,
        captured_tables,
        getattr(migration_state, "cdc_deployed_column_exclude_list", None),
        applied=applied_conversions_of(migration_state),
    )


def applied_conversions_of(migration_state) -> "Optional[Mapping[str, object]]":
    """The applied Schema Conversion recorded on ``migration_state`` (``None`` when there is
    none to read, e.g. a test double)."""
    applied_of = getattr(migration_state, "applied_conversions", None)
    if not callable(applied_of):
        return None
    try:
        return applied_of()[0]
    except Exception:  # noqa: BLE001 - no applied conversion: the source decides
        return None


def generated_capture_columns(migration_state, tables) -> "dict[str, set[str]]":
    """``{table: columns}`` CDC leaves out because Aurora DSQL computes them (see
    :func:`target_generated_capture_columns`) -- the set every reading of the capture
    exclusions subtracts or adds, so they all agree with what Start CDC sends."""
    applied = applied_conversions_of(migration_state)
    return {
        t.name: set(target_generated_capture_columns(t, applied))
        for t in tables or ()
        if getattr(t, "name", None)
    }


def target_generated_capture_columns(table, applied: "Optional[Mapping[str, object]]") -> "list[str]":
    """The columns of ``table`` Aurora DSQL computes, by their SOURCE names: CDC must not
    send them (a value for a generated column is refused, 428C9, so every change of the
    table would be dead-lettered). The APPLIED DDL decides when there is one -- it is what
    the target has, a column the operator made GENERATED in Schema Conversion included, and
    one edited back to ordinary excluded -- else the source's preserved PostgreSQL STORED
    columns. A key column is never listed: the sink needs it to write and delete."""
    from dsql_migrator.core.converter import (
        parse_target_column_names,
        parse_target_generated_columns,
        pg_preserved_generated_columns,
    )

    ddl = getattr((applied or {}).get(table.name), "target_ddl", None)
    if not ddl:
        names = [c.name for c in pg_preserved_generated_columns(table)]
    else:
        named = parse_target_generated_columns(ddl, folded=True)
        exact = {c.name for c in table.columns}
        lowered: dict[str, list[str]] = {}
        for column in table.columns:
            lowered.setdefault(column.name.lower(), []).append(column.name)
        # A fold names a source column only when the DDL does not define that exact name
        # too: a target-only "total" beside the source's own "Total" is another column.
        defined = set(parse_target_column_names(ddl) or ())
        names = []
        for name in sorted(named):
            if name in exact:
                names.append(name)
            elif len(lowered.get(name.lower(), ())) == 1:
                source = lowered[name.lower()][0]
                if source not in defined:
                    names.append(source)
    keys = set(getattr(table, "primary_key", None) or ())
    return sorted(name for name in names if name not in keys)


def capture_exclusions_for(
    effective: "Mapping[str, Iterable[str]]",
    raw_stored: "Mapping[str, Iterable[str]]",
    inventory,
    captured_tables: "Sequence[object]",
    deployed_value: "Optional[str]",
    *,
    applied: "Optional[Mapping[str, object]]" = None,
) -> CdcCaptureExclusions:
    """:func:`cdc_capture_exclusions` for ANY set -- pure, so the exclusion popup previews
    exactly what Start CDC would send after its Save: ``effective`` is the exclusion set
    every path reads, ``raw_stored`` the stored set the 0.1.561 legacy form is built from,
    ``deployed_value`` the stack's recorded ColumnExcludeList, ``applied`` the applied
    Schema Conversion (which columns the target generates)."""
    from dsql_migrator.core.cdc import (
        CDC_PARAMETER_BYTE_LIMIT,
        choose_column_exclude_value,
        column_exclude_ambiguous_entries,
        column_exclude_inexact_entries,
        format_column_exclude_value,
    )

    captured = list(captured_tables or ())
    captured_names = {t.name for t in captured}
    # Only the CAPTURED tables' exclusions: an entry on a table outside table.include.list
    # is inert, the card does not show it, and counting it made a size refusal whose
    # "exclude fewer" remedy could not be followed. (The legacy value below is untouched.)
    merged: dict[str, set[str]] = {
        table: set(cols)
        for table, cols in (effective or {}).items()
        if table in captured_names
    }
    generated: dict[str, list[str]] = {}
    for table in captured:
        preserved = target_generated_capture_columns(table, applied)
        if preserved:
            merged.setdefault(table.name, set()).update(preserved)
            generated[table.name] = sorted(preserved)
    structured = {t: sorted(c) for t, c in sorted(merged.items()) if c}
    value = format_column_exclude_value(structured, captured)
    legacy_columns = cdc_excluded_columns(
        {t: set(c) for t, c in (raw_stored or {}).items()}, inventory)
    legacy = format_column_exclude_list(legacy_columns)
    deployed = deployed_value
    # The 0.1.561 form is kept only for a stack that carries it AND only while the capture
    # adds nothing it cannot express: it is built from the stored exclusions alone, so a
    # stack deployed with nothing excluded carries '' -- the legacy formula's value too --
    # and a column made GENERATED afterwards was never left out: every change of its table
    # was refused (428C9) with the stack reading "as deployed".
    legacy_sets = {t: set(c) for t, c in legacy_columns.items()}
    if any(set(cols) - legacy_sets.get(t, set()) for t, cols in generated.items()):
        legacy = None
    sent = choose_column_exclude_value(value, legacy, deployed)
    error: "Optional[str]" = None
    inexact: "tuple[str, ...]" = ()
    if deployed is not None and sent == deployed:
        # Exactly what the stack already carries: nothing new to refuse (0.1.561 accepted
        # it), but a legacy value may not match its names exactly -- say so.
        if sent == legacy:
            inexact = tuple(column_exclude_inexact_entries(legacy_columns, captured))
    else:
        # A comma in a name is carried escaped (\\,), so only the size can refuse. The
        # text states the FACT; each caller appends the remedy that is true in its state
        # (fewer exclusions while the card is editable, the lock's own remedy otherwise).
        size = len(sent.encode("utf-8"))
        if size > CDC_PARAMETER_BYTE_LIMIT:
            error = (
                f"The column exclusions need {size:,} bytes in the CDC capture "
                f"configuration, over CloudFormation's {CDC_PARAMETER_BYTE_LIMIT:,}-byte "
                "limit for one parameter."
            )
    generated_bytes = (
        len(format_column_exclude_value(generated, captured).encode("utf-8"))
        if error and generated
        else 0
    )
    ambiguous = column_exclude_ambiguous_entries(structured, captured)
    return CdcCaptureExclusions(
        structured, value, legacy, sent, error, inexact, generated_bytes, ambiguous
    )


def capture_ambiguity_warning(capture: "CdcCaptureExclusions") -> "Optional[str]":
    """The warning for excluded names Debezium cannot separate from another column."""
    if not capture.ambiguous:
        return None
    pairs = [f"{name} (also drops {', '.join(others)})" for name, others in capture.ambiguous.items()]
    return (
        "Debezium names these the same as another captured column, so excluding one "
        "also drops the other at capture: " + "; ".join(pairs[:3])
        + (f" and {len(pairs) - 3} more" if len(pairs) > 3 else "")
        + ". No escaping can tell them apart; rename one, or keep both excluded."
    )
