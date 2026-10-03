# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator-approved ADD / RENAME COLUMN recovery for CDC source-schema drift.

CDC does not propagate DDL: when the source runs ``ALTER TABLE ... ADD COLUMN`` (or
``RENAME COLUMN``) the target is unchanged, so the first row written under the new source
schema is rejected by Aurora DSQL (SQLSTATE 42703) and dead-lettered. Phase 1 detects and
classifies that (``core.cdc.classify_schema_drift`` -> the DLQ panel's "source schema change
detected" banner; ``core.cdc_catalog_drift`` -> the source-schema re-check) but deliberately
stops there. This module is the opt-in recovery: :func:`plan_target_fix` decides what the
target is missing, renders the exact statements, and :func:`apply_add_columns` applies them
**only** after a human approves them. Both entry points (the dead-letter banner's and the
re-check's "Fix target schema…") use the same plan, and the re-check's ``fixable`` flag is
that plan's :attr:`AddColumnPlan.offers_fix`.

Design constraints this module enforces (they are not incidental):

* **ADD or RENAME COLUMN only.** A source DROP COLUMN or an incompatible type change is
  never auto-repaired -- those stay alert-only, because reconciling them can destroy or
  rewrite target data. A genuinely new column is added; a column the source RENAMED is
  renamed (``ALTER TABLE ... RENAME COLUMN``, which keeps its values) when the evidence says
  it is a rename: the Step 1 inventory has the old name at the same position with the same
  type, the source-schema re-check saw the rename, or the two names differ only in letter
  case. Weak or ambiguous evidence never guesses: the column is offered as added, with a
  warning naming the target column the source no longer has.
* **Never for a record-key column.** CDC writes (``ON CONFLICT``) and deletes
  (``WHERE key = ?``) by the record key: the source primary key, the live target primary
  key, and a re-keyed table's ``message.key.columns``. When one of them was renamed, removed
  or changed on the source, adding or renaming a column cannot repair the table -- inserts
  keep failing and every delete silently matches no row -- and Aurora DSQL cannot change a
  primary key in place or drop a primary-key column. The whole table is refused
  (:class:`KeyRefusal`) and the caller explains how to recreate it.
* **Never for a column the exclusions keep out.** A new column a saved exclusion rule
  matches, an excluded column the source renamed, and any new column while an excluded
  column is gone from the source (a rename the tool cannot tell from a drop) are held
  (:class:`HeldColumn`), never added or renamed: doing so would copy the excluded values.
  Rules that cannot be evaluated hold every add and rename (fail closed).
* **Generated columns are withheld.** The source computes them and CDC may not send their
  values, so a plain column added for one could stay empty. On PostgreSQL CDC never sends
  one, so a generated column is withheld as generated even when a saved rule matches it.
* **Exact names.** The converter quotes and preserves identifiers and the sink writes
  column names exactly, so names are compared case-sensitively everywhere here.
* **No silent schema mutation (Property 6).** Nothing here runs on the CDC hot path or on
  a timer. The planners are pure and produce DDL *text* for review;
  :func:`apply_add_columns` runs only when the caller (the UI, after an explicit
  confirmation) hands it that plan.
* **Nullable, no default.** A new column is added as NULLable with no default.
  Adding ``NOT NULL`` without a default to a table that already has rows is
  rejected by the engine, and inventing a default would silently fabricate data
  for the rows CDC already applied. Existing rows therefore read NULL until the
  operator backfills them -- which takes a Step 1 (Evaluation) refresh and then a
  'Drop & reload', NOT a plain per-table Reload: that reads the stale Step-1 column
  list (so it never even SELECTs the new column) and appends with SKIP_EXISTING (so it
  never rewrites a primary key the target already has) -- while new change events carry
  the real value.
* **Never guess a type.** A source type that the converter cannot map is skipped
  and reported (:class:`SkippedColumn`), not approximated.
* **One DDL per transaction.** Aurora DSQL rejects two DDL statements in one
  transaction ("multiple ddl statements not supported in a transaction" --
  verified live), so each ``ALTER`` is executed on its own autocommit connection,
  mirroring :mod:`dsql_migrator.core.schema_applier`.

The planners are deliberately IO-free so the mapping/ordering/refusal rules are unit
tested without a MySQL or DSQL connection; the two read helpers and the applier
hold the only IO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional, Sequence, Union

from sqlglot import exp

from dsql_migrator.core.cdc_catalog_drift import (
    LiveTable,
    read_live_catalog,
    same_recorded_type,
    same_type,
)
from dsql_migrator.core.converter import map_mysql_type
from dsql_migrator.core.converter_postgres import (
    clamp_pg_numeric,
    unsupported_dsql_reason,
)
from dsql_migrator.core.models import SourceType

# Mirrors converter._quote_pg_identifier. Kept local like exporter/validator keep
# their own MySQL quoter: the rule is one line and a local copy cannot drift out
# from under this module's rendered DDL.
_POSTGRES = "postgres"


def _quote_ident(name: str) -> str:
    """Return ``name`` as a safely double-quoted PostgreSQL/DSQL identifier."""
    return exp.to_identifier(name, quoted=True).sql(dialect=_POSTGRES)


def _quote_qualified(name: str) -> str:
    """Double-quote a possibly schema-qualified name as ``"schema"."table"``."""
    if "." in name:
        schema, _, obj = name.partition(".")
        return f"{_quote_ident(schema)}.{_quote_ident(obj)}"
    return _quote_ident(name)


@dataclass(frozen=True)
class AddColumnStep:
    """One reviewed ``ALTER TABLE ... ADD COLUMN`` to run on the target."""

    column: str
    source_type: str
    target_type: str
    ddl: str
    warning: Optional[str] = None


@dataclass(frozen=True)
class SkippedColumn:
    """A source column that is NOT offered, with why (never silently dropped)."""

    column: str
    source_type: str
    reason: str


# Evidence that a source column was renamed (RenameColumnStep.evidence).
EVIDENCE_INVENTORY = "inventory"
EVIDENCE_RECHECK = "re-check"
EVIDENCE_CASE = "case"


@dataclass(frozen=True)
class RenameColumnStep:
    """One reviewed ``ALTER TABLE ... RENAME COLUMN old TO column`` to run on the target.

    ``column`` is the NEW (source) name, ``old`` the target column that holds its values,
    ``source_type`` the live source catalog type and ``evidence`` why this is a rename
    (:data:`EVIDENCE_INVENTORY` / :data:`EVIDENCE_RECHECK` / :data:`EVIDENCE_CASE`).
    """

    column: str
    old: str
    source_type: str
    ddl: str
    evidence: str
    warning: Optional[str] = None


# Why a column the target lacks is NOT offered (HeldColumn.reason).
HELD_RULE = "rule"  # a saved exclusion rule matches the (new) name
HELD_RENAMED_EXCLUDED = "renamed-excluded"  # an excluded column, renamed on the source
HELD_EXCLUDED_GONE = "excluded-gone"  # new columns while an excluded column is gone
HELD_NAME_TAKEN = "name-taken"  # a rename whose new name the target already has
HELD_GENERATED = "generated"  # the source computes it
HELD_RULES_UNKNOWN = "rules-unknown"  # the saved rules could not be evaluated


@dataclass(frozen=True)
class HeldColumn:
    """A column the target lacks that the plan deliberately does not add or rename.

    ``old`` is the column it replaces (a rename), ``rule`` the saved rule line that matches
    it (:data:`HELD_RULE`, when known), ``ddl`` the ADD statement withheld
    (:data:`HELD_EXCLUDED_GONE`) and ``others`` the excluded columns gone from the source.
    """

    column: str
    reason: str
    old: Optional[str] = None
    rule: Optional[str] = None
    ddl: Optional[str] = None
    others: tuple[str, ...] = ()


@dataclass(frozen=True)
class KeyRefusal:
    """Why the whole table is refused: a record-key column changed on the source.

    ``renamed`` ``(old, new)`` pairs that touch a key column; ``gone`` key columns the source
    no longer has; ``missing`` live primary-key columns the target does not have; ``before``
    / ``after`` the source primary key (only when it changed); ``rekeyed`` the re-key
    (``message.key.columns``) columns among the gone or renamed ones.
    """

    renamed: tuple[tuple[str, str], ...] = ()
    gone: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    before: tuple[str, ...] = ()
    after: tuple[str, ...] = ()
    rekeyed: tuple[str, ...] = ()


PlanStep = Union[AddColumnStep, RenameColumnStep]


@dataclass(frozen=True)
class AddColumnPlan:
    """The reviewable result of diffing one table's source and target columns.

    ``steps`` holds :class:`AddColumnStep` and (from :func:`plan_target_fix`)
    :class:`RenameColumnStep` entries in source position order; ``held`` the columns
    deliberately not offered; ``refusal`` is set when the whole table is refused (then
    there are no steps); ``orphans`` the target columns the source does not have, reported
    beside an ADD because one of them may be the old name of a rename the tool could not
    confirm.
    """

    table: str
    steps: tuple[PlanStep, ...] = ()
    skipped: tuple[SkippedColumn, ...] = ()
    held: tuple[HeldColumn, ...] = ()
    refusal: Optional[KeyRefusal] = None
    orphans: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        """True when there is nothing to apply (target already has every column)."""
        return not self.steps

    @property
    def ddl_text(self) -> str:
        """The plan's statements as one reviewable block, one per line."""
        return "\n".join(step.ddl for step in self.steps)

    @property
    def offers_fix(self) -> bool:
        """Fix target schema… has something to apply, or to explain beyond what the re-check
        row already says: no key refusal, and a statement, a skipped column or a held one
        other than a generated column (the row already says a generated one is not added)."""
        return self.refusal is None and bool(
            self.steps
            or self.skipped
            or any(h.reason != HELD_GENERATED for h in self.held)
        )


@dataclass(frozen=True)
class AddColumnOutcome:
    """Per-statement result of applying a plan (ordered like the plan)."""

    column: str
    ddl: str
    applied: bool
    error: Optional[str] = None


def _resolve_target_type(
    source_type_str: str, source_engine: SourceType
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Map one source column type to its Aurora DSQL type.

    Returns ``(target_type, warning_message, skip_reason)`` -- exactly one of
    ``target_type`` / ``skip_reason`` is set. MySQL types go through the shared
    ``map_mysql_type`` remap; a PostgreSQL type is near-identity (it IS a PG type
    already), so it is emitted verbatim except that a DSQL-unsupported PG type is
    skipped (never approximated) and an over-precise numeric is clamped with a note --
    mirroring the converter's PG table path.
    """
    if source_engine is SourceType.POSTGRES:
        reason = unsupported_dsql_reason(source_type_str)
        if reason is not None:
            return None, None, reason
        clamped, clamp_note = clamp_pg_numeric(source_type_str)
        return clamped, clamp_note, None
    try:
        target_type, warning = map_mysql_type(source_type_str)
    except ValueError as exc:
        return None, None, f"no Aurora DSQL mapping for this type ({exc})"
    return target_type, (warning.message if warning is not None else None), None


def plan_add_columns(
    table: str,
    source_columns: Sequence[tuple[str, str]],
    target_columns: Iterable[str],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> AddColumnPlan:
    """Diff a table's columns and render the additive DDL the target is missing.

    ``source_columns`` is ``[(column_name, source_column_type), ...]`` in the
    source's ordinal order, and ``target_columns`` is the column names the target
    already has. Comparison is exact, case included: the converter quotes and preserves
    identifiers (``"Full_Name"`` stays ``"Full_Name"``) and the sink writes column names
    exactly, so a target ``full_name`` does NOT hold a source ``Full_Name`` (a case-only
    rename is :func:`plan_target_fix`'s job).

    ``source_type`` selects how each column type is mapped: a MySQL type is remapped
    to its DSQL equivalent; a PostgreSQL type is emitted near-verbatim (DSQL-unsupported
    PG types are skipped, over-precise numerics clamped) -- matching the schema
    converter, so an ADD COLUMN recovery on a PostgreSQL source produces PG DDL instead
    of erroring through the MySQL remap.

    Pure: no connection, no clock, no IO. Ordering follows the source so a
    multi-column ALTER sequence reads like the source's own change history.
    """
    have = set(target_columns)
    steps: list[AddColumnStep] = []
    skipped: list[SkippedColumn] = []
    for column, source_column_type in source_columns:
        if column in have:
            continue
        target_type, warning, skip_reason = _resolve_target_type(
            source_column_type, source_type
        )
        if target_type is None:
            # Unsupported/unmappable source type: report it, never approximate.
            skipped.append(
                SkippedColumn(
                    column=column,
                    source_type=source_column_type,
                    reason=skip_reason or "no Aurora DSQL mapping for this type",
                )
            )
            continue
        steps.append(
            AddColumnStep(
                column=column,
                source_type=source_column_type,
                target_type=target_type,
                # Nullable, no default -- see the module docstring.
                ddl=(
                    f"ALTER TABLE {_quote_qualified(table)} "
                    f"ADD COLUMN {_quote_ident(column)} {target_type}"
                ),
                warning=warning,
            )
        )
    return AddColumnPlan(table=table, steps=tuple(steps), skipped=tuple(skipped))


def _pair_renames(
    live_cols: Sequence,
    target: Sequence[str],
    tset: "set[str]",
    *,
    baseline: Optional[object],
    seen_renames: Sequence[tuple[str, str, str]],
    source_type: SourceType,
) -> "dict[str, tuple[str, str]]":
    """``{new name: (old name, evidence)}`` for the source columns renamed. Pure.

    Each old and each new name pairs at most once, from the strongest evidence first: the
    Step 1 inventory (the old name at the same rank among ALL columns -- excluded ones
    included, so a renamed excluded column is seen -- with the same type), then a rename the
    source-schema re-check recorded (needed once Step 1 was re-run after the rename; its
    recorded type is bounded text, so a type longer than 60 characters is matched on its
    stored prefix), then a target column that differs from a source one only in letter
    case (exactly one).
    """
    live_by = {c.name: c for c in live_cols}
    pairs: dict[str, tuple[str, str]] = {}
    used: set[str] = set()
    if baseline is not None:
        base = list(getattr(baseline, "columns", None) or ())
        base_names = {b.name for b in base}
        for rank, b in enumerate(base):
            if b.name in live_by or b.name in used or rank >= len(live_cols):
                continue
            c = live_cols[rank]
            if (
                c.name not in base_names
                and c.name not in pairs
                and same_type(str(b.mysql_type), c.type, source_type=source_type)
            ):
                pairs[c.name] = (b.name, EVIDENCE_INVENTORY)
                used.add(b.name)
    for old, new, before in seen_renames or ():
        if not old or not new or new in pairs or old in used:
            continue
        if old in live_by or new not in live_by:
            continue
        if not same_recorded_type(before, live_by[new].type, source_type=source_type):
            continue
        pairs[new] = (old, EVIDENCE_RECHECK)
        used.add(old)
    for c in live_cols:
        if c.name in tset or c.name in pairs:
            continue
        candidates = [
            t for t in target
            if t not in live_by and t not in used and t != c.name
            and t.lower() == c.name.lower()
        ]
        if len(candidates) == 1:
            pairs[c.name] = (candidates[0], EVIDENCE_CASE)
            used.add(candidates[0])
    return pairs


def plan_target_fix(
    table: str,
    live: LiveTable,
    target_columns: Iterable[str],
    *,
    baseline: Optional[object] = None,
    seen_renames: Sequence[tuple[str, str, str]] = (),
    record_key: Iterable[str] = (),
    rekey_columns: Iterable[str] = (),
    excluded: Iterable[str] = (),
    rule_matches: Optional[Mapping[str, Optional[str]]] = None,
    source_type: SourceType = SourceType.MYSQL,
) -> AddColumnPlan:
    """What Fix target schema… offers for ``table``: ADD / RENAME steps, held columns, or a
    whole-table key refusal. Pure; the ONE plan both entry points (and the re-check's
    ``fixable``) use.

    ``live`` is the source's live definition (``read_source_table``); ``target_columns``
    the names the target has (in order); ``baseline`` the Step 1 inventory ``TableDef``
    (or ``None``); ``seen_renames`` ``((old, new, old catalog type), ...)`` the re-check
    recorded; ``record_key`` the live target primary key; ``rekey_columns`` the current and
    deployed ``message.key.columns``; ``excluded`` the effective stored exclusions;
    ``rule_matches`` ``{column: rule text or None}`` the SAVED rules match on the live
    table -- ``None`` means they could not be evaluated, which holds every add and rename.

    Order: pair renames; refuse the whole table when a key column was renamed, is gone, is
    missing on the target, or the source key changed; hold everything when the rules are
    unknown; per rename: name already taken, excluded, rule-matched, RENAME, or (neither
    name on the target) an add; per other new column: excluded (not captured, nothing to
    do), rule-matched (on PostgreSQL a generated column is withheld as generated before the
    rule check: CDC never sends it), or an add -- generated adds are withheld; the adds go
    through :func:`plan_add_columns` unchanged; while an excluded column is gone from the
    source (it may be one of them, renamed) every add is held with its statement. Names
    are compared exactly.
    """
    cols = sorted(live.columns, key=lambda c: c.position)
    live_by = {c.name: c for c in cols}
    rank = {c.name: i for i, c in enumerate(cols)}
    target = [str(t) for t in target_columns]
    tset = set(target)
    excl = frozenset(str(e) for e in (excluded or ()))
    pairs = _pair_renames(
        cols, target, tset, baseline=baseline, seen_renames=seen_renames,
        source_type=source_type,
    )
    ordered = sorted(pairs.items(), key=lambda kv: rank[kv[0]])
    pair_olds = {old for old, _ev in pairs.values()}

    # 2. A record-key column changed: refuse the whole table (no ADD, no RENAME).
    base_pk = [str(k) for k in (getattr(baseline, "primary_key", None) or ())] if (
        baseline is not None
    ) else []
    live_pk = frozenset(live.primary_key or ())
    keys = set(base_pk) | set(live_pk) | {str(k) for k in record_key or ()} | {
        str(k) for k in rekey_columns or ()
    }
    renamed = tuple(
        (old, new) for new, (old, _ev) in ordered if old in keys or new in keys
    )
    renamed_olds = {old for old, _new in renamed}
    renamed_news = {new for _old, new in renamed}
    gone = tuple(sorted(k for k in keys if k not in live_by and k not in renamed_olds))
    missing = tuple(
        c.name for c in cols
        if c.name in live_pk and c.name not in tset and c.name not in renamed_news
    )
    changed = baseline is not None and set(base_pk) != set(live_pk)
    if renamed or gone or missing or changed:
        rekey = {str(k) for k in rekey_columns or ()}
        return AddColumnPlan(
            table=table,
            refusal=KeyRefusal(
                renamed=renamed,
                gone=gone,
                missing=missing,
                before=tuple(base_pk) if changed else (),
                after=tuple(
                    sorted(live_pk, key=lambda n: (rank.get(n, len(rank)), n))
                ) if changed else (),
                rekeyed=tuple(sorted(rekey & (set(gone) | renamed_olds))),
            ),
        )

    def _by_position(item: HeldColumn) -> int:
        return rank.get(item.column, len(rank))

    # 3. The saved rules could not be evaluated: add or rename nothing (fail closed).
    if rule_matches is None:
        unknown = [
            HeldColumn(new, HELD_RULES_UNKNOWN, old=old)
            for new, (old, _ev) in ordered if new not in tset
        ] + [
            HeldColumn(c.name, HELD_RULES_UNKNOWN)
            for c in cols if c.name not in pairs and c.name not in tset and c.name not in excl
        ]
        return AddColumnPlan(table=table, held=tuple(sorted(unknown, key=_by_position)))

    held: list[HeldColumn] = []
    renames: list[RenameColumnStep] = []
    candidates: list[str] = []
    # 4. Each rename.
    for new, (old, evidence) in ordered:
        if new in tset:
            # Already renamed (only the new name), or both names: an earlier Fix may have
            # added the new one empty. Never two columns for one source column.
            if old in tset and old not in excl and new not in excl:
                held.append(HeldColumn(new, HELD_NAME_TAKEN, old=old))
            continue
        if old in excl or new in excl:
            held.append(HeldColumn(new, HELD_RENAMED_EXCLUDED, old=old))
        elif new in rule_matches:
            held.append(HeldColumn(new, HELD_RULE, old=old, rule=rule_matches[new]))
        elif old in tset:
            renames.append(
                RenameColumnStep(
                    column=new,
                    old=old,
                    source_type=live_by[new].type,
                    ddl=(
                        f"ALTER TABLE {_quote_qualified(table)} "
                        f"RENAME COLUMN {_quote_ident(old)} TO {_quote_ident(new)}"
                    ),
                    evidence=evidence,
                )
            )
        else:
            candidates.append(new)
    # 5. Each other column the target lacks.
    for c in cols:
        if c.name in pairs or c.name in tset or c.name in excl:
            # An excluded column is not captured, so it cannot dead-letter: nothing to do.
            continue
        if c.name in rule_matches and not (
            # PostgreSQL never streams a generated column (pgoutput does not publish it), so
            # a rule that matches one has nothing to keep out: it is held as generated.
            source_type is SourceType.POSTGRES and c.generated
        ):
            held.append(HeldColumn(c.name, HELD_RULE, rule=rule_matches[c.name]))
        else:
            candidates.append(c.name)
    plain: list[str] = []
    for name in sorted(candidates, key=lambda n: rank[n]):
        if live_by[name].generated:
            held.append(HeldColumn(name, HELD_GENERATED))
        else:
            plain.append(name)
    # 6. The adds, exactly as plan_add_columns renders them.
    sub = plan_add_columns(
        table, [(name, live_by[name].type) for name in plain], target,
        source_type=source_type,
    )
    adds = list(sub.steps)
    # 7. An excluded column gone from the source may be one of these, renamed.
    gone_excluded = tuple(sorted(e for e in excl if e not in live_by and e not in pair_olds))
    if gone_excluded and adds:
        held.extend(
            HeldColumn(step.column, HELD_EXCLUDED_GONE, ddl=step.ddl, others=gone_excluded)
            for step in adds
        )
        adds = []
    steps: list[PlanStep] = sorted(
        [*renames, *adds], key=lambda step: rank.get(step.column, len(rank))
    )
    orphans: tuple[str, ...] = ()
    if adds:
        orphans = tuple(
            t for t in target
            if t not in live_by and t not in pair_olds and t not in excl and t not in keys
        )
    return AddColumnPlan(
        table=table,
        steps=tuple(steps),
        skipped=sub.skipped,
        held=tuple(sorted(held, key=_by_position)),
        orphans=orphans,
    )


def read_source_table(
    connection, table: str, *, source_type: SourceType = SourceType.MYSQL
) -> Optional[LiveTable]:
    """The live source definition of ONE qualified ``table``, or ``None`` when it has no
    column visible to the source user (or the name is unqualified: never read).

    ONE bounded, read-only catalog statement (:func:`cdc_catalog_drift.read_live_catalog`:
    MySQL ``information_schema.COLUMNS`` + ``STATISTICS`` with ``COLUMN_TYPE``; PostgreSQL
    ``pg_catalog`` with ``format_type``) giving names, types, positions, NULL rules,
    generated flags and the primary key -- never a data scan. ``connection`` is a
    SQLAlchemy connection from the read-only guarded source engine. Raises on failure.
    """
    return read_live_catalog(connection, [table], source_type=source_type).get(table)


def read_target_columns(connection, table: str) -> list[str]:
    """Read the column names ``schema.table`` currently has on the target."""
    schema, _, name = table.partition(".")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, name),
        )
        return [row[0] for row in cursor.fetchall()]


def apply_add_columns(
    plan: AddColumnPlan,
    connection_factory: Callable[[], object],
) -> tuple[AddColumnOutcome, ...]:
    """Apply a reviewed plan (ADD or RENAME steps), one DDL per transaction, stopping at
    the first error.

    ``connection_factory`` returns a fresh autocommit DSQL connection per call --
    the same shape :mod:`schema_applier` uses, because Aurora DSQL rejects two DDL
    statements in one transaction. Execution stops at the first failure so a
    partially-evolved table is reported honestly instead of the caller having to
    guess which statements ran; the statements already applied are durable (each ALTER
    committed on its own), so re-running the plan is safe -- a column that now exists
    under its source name simply drops out of the next plan.
    """
    outcomes: list[AddColumnOutcome] = []
    for step in plan.steps:
        try:
            connection = connection_factory()
        except Exception as exc:  # noqa: BLE001 - surfaced to the operator verbatim
            outcomes.append(
                AddColumnOutcome(
                    column=step.column, ddl=step.ddl, applied=False, error=str(exc)
                )
            )
            break
        try:
            with connection.cursor() as cursor:  # type: ignore[attr-defined]
                cursor.execute(step.ddl)
            outcomes.append(
                AddColumnOutcome(column=step.column, ddl=step.ddl, applied=True)
            )
        except Exception as exc:  # noqa: BLE001 - one bad type must not hide the rest
            outcomes.append(
                AddColumnOutcome(
                    column=step.column, ddl=step.ddl, applied=False, error=str(exc)
                )
            )
            break
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:  # noqa: BLE001 - best effort
                    pass
    return tuple(outcomes)


__all__ = [
    "AddColumnOutcome",
    "AddColumnPlan",
    "AddColumnStep",
    "EVIDENCE_CASE",
    "EVIDENCE_INVENTORY",
    "EVIDENCE_RECHECK",
    "HELD_EXCLUDED_GONE",
    "HELD_GENERATED",
    "HELD_NAME_TAKEN",
    "HELD_RENAMED_EXCLUDED",
    "HELD_RULE",
    "HELD_RULES_UNKNOWN",
    "HeldColumn",
    "KeyRefusal",
    "PlanStep",
    "RenameColumnStep",
    "SkippedColumn",
    "apply_add_columns",
    "plan_add_columns",
    "plan_target_fix",
    "read_source_table",
    "read_target_columns",
]
