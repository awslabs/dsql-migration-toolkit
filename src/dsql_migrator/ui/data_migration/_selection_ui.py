# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Table-selection render helpers (extracted from ``data_migration/__init__``).

The "Tables to migrate" picker every migration type shows, and the table-name helpers it
is built from: the migratable universe (``generated_table_names``,
``target_existing_table_names``, ``migratable_table_names``, ``_split_migration_schema``),
the pre-ticked default and the effective selection (``default_migration_selection``,
``effective_migration_selection``), the schema -> Tables -> table tree with its primary-key
and lock icons (``build_migration_table_tree``, ``_header_table_slot_with_locks``), and the
renderer (``_render_table_selection``). The migration-type selector and the existing CDC
infrastructure banner stay in the package ``__init__``: the selector falls back to
``migration_type_lock_reason``, which the package defines, and the banner renders
``_cdc_ui``'s attach row (and reads its ``_cdc_stack_has_seeder_lambda``), so either would
make this module import the package or ``_cdc_ui``, which it never does (below).

The package ``__init__`` re-exports every name defined here, private ones included, at its
bottom: for its own code -- the screen builder, ``resolve_migration_selection`` and the
exclusion-inputs resolver read the picker, the default and effective selection and the
migratable / target-existing names as module globals -- and for the readers that reach them
as ``dm.<name>`` / ``from ...data_migration import <name>``. A re-export does not carry a
monkeypatch: a patch on ``dm.<name>`` reaches the package's calls only, and the calls made
here read this module's names, so a test patches them here. One-directional: this module
imports, at its top, from ``_models`` / ``_table_set_lock`` and the shared ``core`` + ``ui``
modules -- never from the package ``__init__``, or from ``_cdc_ui``, which reads two of the
package's helpers (``exclusion_card_locked``, ``table_set_lock_reason``) each inside a
function, at call time; ``tests/test_rr_dmshared06_render_split.py`` pins that this module
imports neither.
"""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Sequence

from dsql_migrator.core.converter import relation_clash_names
from dsql_migrator.core.models import (
    SourceInventory,
    TableSelection,
    TargetInventory,
)
from dsql_migrator.ui.design import inline_hint, render_notice
from dsql_migrator.ui.schema_conversion import TABLE_PREFIX, selected_object_names
from dsql_migrator.ui.data_migration._models import keep_unlisted_covered
from dsql_migrator.ui.data_migration._table_set_lock import LOCK_TIP_RULE, LOCK_TIP_SPLIT


def _split_migration_schema(name: str) -> tuple[str, str]:
    """Split a possibly-qualified ``schema.object`` name into (schema, object).

    Mirrors the Step 2 Object browser grouping: a qualified ``database.object``
    name splits on the dot; a plain name falls under the ``"source"`` schema.
    """
    if "." in name:
        schema, _, obj = name.partition(".")
        return schema, obj
    return "source", name


def generated_table_names(
    inventory: SourceInventory, generated_node_ids: Optional[Sequence[str]]
) -> list[str]:
    """Return inventory table names whose schema DDL was generated in Step 2.

    The Data Migration table picker is scoped to the tables the user generated
    schema DDL for in Step 2 (Schema Conversion): only those have a target table
    to load into. ``generated_node_ids`` is the Schema Conversion
    ``generated_node_ids`` snapshot (object-leaf node ids); this maps it back to
    the table names present in ``inventory`` (inventory order). ``None`` /empty
    (nothing generated yet) yields an empty list.
    """
    if not generated_node_ids:
        return []
    names = selected_object_names(generated_node_ids)
    return [table.name for table in inventory.tables if table.name in names]


def target_existing_table_names(
    inventory: SourceInventory, target: Optional[TargetInventory]
) -> list[str]:
    """Return source table names that already exist on the target DSQL.

    A table whose schema DDL was applied to DSQL in an earlier Schema Conversion
    run (a prior session or out of band) already has a target table to load into,
    so it is migratable even without a current-session DDL generation.

    Matching is **qualified**: the converter creates a PostgreSQL schema named
    after each source MySQL database and qualifies tables into it, so a source
    ``db.table`` exists on the target only when a target table of that name lives
    in the matching schema -- NOT merely because some other schema happens to
    have a same-named table (which would otherwise wrongly pre-select a table
    whose schema is absent from the target). An unqualified source table
    (single-database introspection, schema ``"source"``) falls back to matching
    by unqualified name. Comparison is case-insensitive (PostgreSQL identifier
    folding); target views are ignored. Returns inventory order; empty when no
    target inventory is available (Step 1 not run / no target catalog).
    """
    if target is None:
        return []
    target_qualified = {
        (schema.name.lower(), relation.name.lower())
        for schema in target.schemas
        for relation in schema.tables
    }
    target_unqualified = {name for _schema, name in target_qualified}
    existing: list[str] = []
    for table in inventory.tables:
        schema, obj = _split_migration_schema(table.name)
        obj_lower = obj.lower()
        if schema == "source":
            if obj_lower in target_unqualified:
                existing.append(table.name)
        elif (schema.lower(), obj_lower) in target_qualified:
            existing.append(table.name)
    return existing


def migratable_table_names(
    inventory: SourceInventory,
    generated_node_ids: Optional[Sequence[str]],
    target: Optional[TargetInventory],
) -> list[str]:
    """Return the tables that can be migrated, in inventory order (Req 5.9).

    A table is migratable when it has a target table to load into, which is true
    when EITHER its schema DDL was generated in this session's Step 2
    (``generated_node_ids``) OR it already exists on the target DSQL (``target``)
    -- e.g. Schema Conversion was applied in a prior session/out of band. Taking
    the union lets Data Migration proceed without re-running Schema Conversion in
    the current session when the target schema is already in place.

    A table whose stored name another table or view of its schema shares
    (:func:`~dsql_migrator.core.converter.relation_clash_names`) is never migratable:
    Schema Conversion creates neither, and on a target an earlier version built, the one
    relation that exists would receive both tables' rows.
    """
    migratable = set(generated_table_names(inventory, generated_node_ids))
    migratable |= set(target_existing_table_names(inventory, target))
    migratable -= relation_clash_names(inventory)
    return [table.name for table in inventory.tables if table.name in migratable]


def default_migration_selection(
    inventory: SourceInventory,
    generated_node_ids: Optional[Sequence[str]],
    target: Optional[TargetInventory],
    ticked_node_ids: Optional[Sequence[str]] = None,
) -> list[str]:
    """Return the tables to pre-tick in the picker before the user touches it.

    **The Schema Conversion selection wins whenever one is known.** Picking three tables
    in Step 2 and finding all eleven ticked in Step 3 is wrong: the default used to be
    "every table that exists on the target", so a target carrying tables from earlier
    runs (or a full E2E reset) silently re-selected them all and discarded the deliberate
    Step 2 selection. Worse, it defaults to migrating MORE than asked -- the wrong
    direction for a long-running load.

    Resolves the Step 2 scope exactly the way Schema Conversion's own apply does
    (``_selected_apply_names``): the committed ``generated_node_ids`` when DDL was
    generated, **else the live ``ticked_node_ids``**. Both are persisted, so this holds
    across a restart. Using only the generated ids (the first cut of this fix) still
    over-ticked every real flow that applies without pressing "Generate DDL for
    selected", or that pressed Clear afterwards -- ``generated_node_ids`` is empty there
    while the user's tick set is right there in state.

    Falls back to the target-existing set only when NEITHER is known -- a reconnect into
    a session that never ticked anything, or a schema applied out of band -- where the
    Step 2 choice is genuinely unknown and an empty default would leave the user staring
    at zero ticked tables with no explanation. Intersected with the migratable universe
    by the caller.
    """
    generated = generated_table_names(inventory, generated_node_ids)
    if generated:
        return generated
    ticked = generated_table_names(inventory, ticked_node_ids)
    if ticked:
        return ticked
    return target_existing_table_names(inventory, target)


def effective_migration_selection(
    migratable: Sequence[str],
    selection: TableSelection,
    *,
    touched: bool,
    default: Optional[Sequence[str]] = None,
) -> list[str]:
    """Resolve the effective migration selection within the migratable set.

    The migratable universe is ``migratable`` (tables that have a target table to
    load into -- DDL generated this session or already present on the target).
    Until the user changes the picker (``touched`` is ``False``) the pre-selected
    default is ``default`` intersected with ``migratable`` (in ``migratable``
    order); when ``default`` is ``None`` every migratable table is pre-selected.
    The default is the tables whose DDL exists on the target DSQL, so only tables
    that actually have a destination table are checked out of the box. Once
    changed, the effective selection is the user's chosen names intersected with
    the still-migratable set; an explicit empty selection stays empty so the run
    guard can require at least one table.
    """
    migratable_order = list(migratable)
    if not touched:
        if default is None:
            return migratable_order
        default_set = set(default)
        return [name for name in migratable_order if name in default_set]
    chosen = set(selection.selected_tables)
    return [name for name in migratable_order if name in chosen]


def build_migration_table_tree(
    inventory: SourceInventory,
    migratable: Sequence[str],
    *,
    add_locked: Optional[Mapping[str, str]] = None,
) -> list[dict]:
    """Assemble the hierarchical table picker for ``ui.tree`` (Schema/Tables).

    Mirrors the Step 2 Object browser layout (schema -> Tables -> table leaves)
    but is tables-only: data migration loads table rows, so views/triggers/
    routines are not offered. Only migratable tables (in ``migratable`` -- DDL
    generated this session or already present on the target) are listed and
    tickable; tables with no target table to load into are omitted entirely, so
    a source-only schema absent from the target does not appear (and never shows
    a misleading indeterminate parent checkbox). Leaf node ids reuse
    ``TABLE_PREFIX`` so ticks map back to table
    names via :func:`~dsql_migrator.ui.schema_conversion.selected_object_names`.

    Each table leaf also carries ``has_pk`` (whether the table has a primary key)
    and ``header: "table"`` so the renderer's ``header-table`` Quasar slot can
    show a small PK indicator beside the leaf label -- Aurora DSQL requires a
    primary key, so a missing one is worth flagging in the picker.

    ``add_locked`` (``{table: code}``, Full load + CDC after its Full Load, G16 --
    :meth:`TableSetAddLock.node_flags`) marks the leaves whose checkbox cannot change now
    (an unticked one that cannot be ticked; G137: a ticked one read in parts, ``"split"``,
    that cannot be unticked): ``tickable: False`` (its checkbox disabled; NOT ``disabled``,
    which would also kill the lock icon's tooltip) and ``add_locked`` (the code the slot
    shows a lock icon for). ``None`` builds exactly today's nodes.
    """
    migratable_set = set(migratable)
    locked_codes = add_locked or {}
    order: list[str] = []
    by_schema: dict[str, list[dict]] = {}

    for table in inventory.tables:
        # Scoped to migratable tables (Req 5.9): a table with no target table to
        # load into is omitted entirely (not shown disabled), so the picker
        # lists only what can actually be migrated. This also means a schema
        # whose tables are all non-migratable never appears -- avoiding a
        # misleading indeterminate parent checkbox for a schema nothing can be
        # selected under (e.g. a source-only schema absent from the target).
        if table.name not in migratable_set:
            continue
        schema, obj = _split_migration_schema(table.name)
        if schema not in by_schema:
            by_schema[schema] = []
            order.append(schema)
        leaf = {
            "id": f"{TABLE_PREFIX}{table.name}",
            "label": obj,
            "has_pk": bool(table.primary_key),
            "header": "table",  # -> the "header-table" leaf slot (PK badge)
        }
        code = locked_codes.get(table.name)
        if code is not None:
            leaf["tickable"] = False
            leaf["add_locked"] = code
        by_schema[schema].append(leaf)

    schema_nodes: list[dict] = []
    for schema in order:
        tables = by_schema[schema]
        category = {
            "id": f"category:tables:{schema}",
            "label": f"Tables ({len(tables)})",
            "children": tables,
        }
        schema_nodes.append(
            {
                "id": f"schema:{schema}",
                "label": f"Schema: {schema}",
                "children": [category],
            }
        )
    return schema_nodes


def _header_table_slot_with_locks() -> str:
    """The table leaf slot with the G16 lock icon (Full load + CDC after its Full Load): a
    grey lock on a leaf an exclude table rule holds (the only per-table reason since G17),
    or (G137) on a ticked leaf the Full Load read in parts, its reason on hover. Static text,
    so the whole slot stays a client-side template (no per-node Python work)."""
    return (
        r"""
            <div class="row items-center no-wrap">
              <span class="text-body2">{{ props.node.label }}</span>
              <q-icon v-if="props.node.has_pk" name="check_circle"
                      color="green-6" size="16px" class="q-ml-xs">
                <q-tooltip>Has a primary key</q-tooltip>
              </q-icon>
              <q-icon v-else name="warning" color="amber-7" size="16px" class="q-ml-xs">
                <q-tooltip>No primary key — required to migrate to Aurora DSQL</q-tooltip>
              </q-icon>
              <q-icon v-if="props.node.add_locked === 'rule'" name="lock"
                      color="grey-6" size="16px" class="q-ml-xs">
                <q-tooltip>"""
        + LOCK_TIP_RULE
        + r"""</q-tooltip>
              </q-icon>
              <q-icon v-if="props.node.add_locked === 'split'" name="lock"
                      color="grey-6" size="16px" class="q-ml-xs">
                <q-tooltip>"""
        + LOCK_TIP_SPLIT
        + r"""</q-tooltip>
              </q-icon>
            </div>
            """
    )


def _render_table_selection(
    ui,
    inventory: SourceInventory,
    migration_state,
    migratable: Sequence[str],
    *,
    target_existing: Sequence[str] = (),
    default_selection: Optional[Sequence[str]] = None,
    on_refresh: Optional[Callable[[], object]] = None,
    locked: bool = False,
    lock_reason: Optional[str] = None,
    locked_selection: Optional[Sequence[str]] = None,
    on_selection_change: Optional[Callable[[list[str]], None]] = None,
    lock_check: Optional[Callable[[], Optional[str]]] = None,
    on_locked: Optional[Callable[[], None]] = None,
    hint: Optional[str] = None,
    filter_state: Optional[dict] = None,
    add_lock_fn: Optional[Callable[[], object]] = None,
) -> None:
    """Render the hierarchical table picker scoped to migratable tables (Req 5.9).

    Mirrors the Step 2 Object browser (schema -> Tables -> table leaves):
    only migratable tables -- those with a target table to load into, whether the
    DDL was generated this session or already exists on the target (Schema
    Conversion run earlier) -- are listed and tickable; tables with no target
    table are omitted entirely (and a schema with none does not appear).
    ``default_selection`` (from :func:`default_migration_selection`) is the set to
    pre-tick before the user touches the picker: this session's Schema Conversion
    choice when there is one, else the tables already on the target. When it is
    ``None`` the renderer falls back to ``target_existing`` (the pre-existing
    behaviour, kept so a caller that only knows the target set still works).
    ``target_existing`` remains the "has a destination table" universe. Pre-selected
    tables can be unticked. The ticked set is
    persisted to the session selection so Full Load / CDC / prerequisite checks
    run over exactly the chosen tables (Property 16). ``on_refresh``, when given,
    re-introspects this session's source/target so the browser reflects the
    latest schema (e.g. tables just created on the target in Step 2).

    ``locked`` freezes the picker; ``lock_reason`` (from
    :func:`selection_lock_reason`) is the cause + remedy shown on the lock icon's
    tooltip. The causes differ materially -- a finished Full Load, live CDC, and
    deployed CDC infrastructure each need a different remedy -- so the caller passes
    the reason rather than this renderer guessing one.

    ``lock_check`` re-asks the lock at CLICK time (a tick, Select all, Unselect all): a
    reason there (another tab started a load) applies nothing, warns, and calls
    ``on_locked`` (a page refresh that re-ticks the stored set). ``hint`` is a neutral
    line under the title (after a load: what a tick or an untick does).
    ``filter_state`` is a caller-held dict keeping the filter text across a refresh.

    ``add_lock_fn`` (Full load + CDC after its Full Load, before CDC starts, G16) returns
    the per-table add-lock (:func:`table_set_add_lock`) or ``None``. With a lock, each
    untickable leaf (since G17: one an exclude table rule holds) shows a lock icon with its
    reason (client-side), a tick that would ADD a locked table is refused whole at the
    click (warned, nothing stored, ``on_locked``), Select all selects only the tables that
    may be selected, and every write keeps the stored covered tables the tree cannot list
    (:func:`keep_unlisted_covered`). The lock is read at render (the icons) and again at
    every click (the current type decides there). ``None``, or a function returning
    ``None``, renders exactly today's picker.
    """
    with ui.row().classes("items-center gap-1"):
        ui.label("Tables to migrate").classes("text-sm font-semibold")
        # "Why only tables?" is useful context but reads as clutter when always on --
        # move it to a hover ⓘ next to the title (matches the app's other header
        # tooltips) instead of a standing paragraph.
        ui.icon("info").classes("text-gray-400 text-sm cursor-help").tooltip(
            "Only tables are listed — they hold the row data. Views, triggers and "
            "routines have no data of their own; they are created in Schema "
            "Conversion, not migrated in this step."
        )
        # The refresh button is hidden once locked: re-introspecting could change the
        # migratable set out from under a committed migration (a running/finished load,
        # or the connector + partition plan CDC was deployed with).
        if on_refresh is not None and not locked:
            ui.button(on_click=on_refresh).props(
                "flat dense round size=sm icon=refresh"
            ).tooltip("Refresh source/target objects")
        if locked:
            # The lock icon's tooltip carries the reason + how to change it, so no
            # separate standing "Locked — …" paragraph is needed below. The reason is
            # per-cause (see selection_lock_reason); the fallback only covers a caller
            # that passes locked=True without one.
            ui.icon("lock", color="grey").classes("text-sm").tooltip(
                lock_reason
                or "Locked — this table set is already committed to a migration."
            )
    if hint and not locked:
        inline_hint(ui, hint, tone="neutral")
    if not migratable:
        render_notice(
            ui,
            tone="warning",
            header="No tables ready to migrate",
            body="No tables are ready to migrate yet. In Step 2 (Schema Conversion), "
            'tick tables and click "Generate DDL for selected" -- or connect to a '
            "target where the tables already exist. Only tables with a target "
            "table can be migrated.",
        )
        return

    # Describe the ACTUAL pre-tick set, and say where it came from. This used to count
    # ``target_existing`` and always claim "already on the target", which stopped being
    # true once the default became this session's Schema Conversion selection -- and was
    # misleading even before, since it described the default rather than what was ticked.
    default_set = set(
        default_selection if default_selection is not None else target_existing
    )
    pre_selected = [n for n in migratable if n in default_set]
    # Name the real origin. ``default_migration_selection`` FALLS BACK to the
    # target-existing set when this session generated nothing (a reconnect / schema
    # applied out of band), so "came from Schema Conversion" cannot be inferred from
    # the parameter merely being present -- it has to differ from the target set.
    # Claiming otherwise told a reconnected user their 11 ticked tables were a Step 2
    # choice they never made in this session.
    origin = (
        "selected in Schema Conversion"
        if default_selection is not None
        and set(default_selection) != set(target_existing)
        else "already on the target"
    )
    ui.label(
        f"Pre-selected: {len(pre_selected)} of {len(migratable)} table(s) "
        f"{origin} — tick or untick to change."
    ).classes("text-xs text-gray-400")

    migratable_order = list(migratable)
    if locked and locked_selection is not None:
        # Locked because CDC is live: the connectors stream a FIXED table set, so the
        # browser must reflect THAT set -- not the generic "everything on the target"
        # default. Without this, a reconnect (which resets selection_touched paths)
        # shows every migratable table ticked and frozen, misrepresenting what CDC is
        # actually replicating. Intersect with the migratable universe so only real,
        # tickable leaves are marked. (Only the caller's live-CDC branch passes this;
        # the other lock causes keep the normal selection view.)
        locked_set = set(locked_selection)
        effective = [name for name in migratable_order if name in locked_set]
    else:
        effective = effective_migration_selection(
            migratable_order,
            migration_state.selection,
            touched=migration_state.selection_touched,
            default=list(default_set),
        )
    # G16: which unticked leaves cannot be ticked now (Full load + CDC after its load).
    render_lock = add_lock_fn() if add_lock_fn is not None and not locked else None
    # Object-browser tree (schema -> Tables -> leaf, leaf checkboxes), styled to
    # match the app's AWS/Cloudscape look: a live name filter + selection counter
    # above a white, bordered scroll panel. Each leaf shows a small PK indicator
    # via the "header-table" Quasar slot (Aurora DSQL requires a primary key, so a
    # missing one is worth flagging). The slot is a client-side Vue template, so
    # it adds no per-node Python work.
    nodes = build_migration_table_tree(
        inventory,
        migratable_order,
        add_locked=(
            render_lock.node_flags(migratable_order, effective)
            if render_lock is not None
            else None
        ),
    )
    effective_set = set(effective)
    total_leaves = sum(len(n["children"][0]["children"]) for n in nodes)

    def _current_effective() -> set:
        """The selection the tree shows NOW (the same resolution as ``effective``)."""
        return set(effective_migration_selection(
            migratable_order,
            migration_state.selection,
            touched=migration_state.selection_touched,
            default=list(default_set),
        ))

    def _kept(lock) -> list:
        """The stored covered names a write keeps although the tree cannot list them."""
        if lock is None:
            return []
        return keep_unlisted_covered(
            migration_state.selection.selected_tables, migratable_order, lock.covered
        )

    def _refused() -> bool:
        """The lock re-asked at the click: a reason applies nothing (see the docstring)."""
        reason = lock_check() if lock_check is not None else None
        if not reason:
            return False
        ui.notify(f"{reason} This change was not applied.", type="warning", multi_line=True)
        if on_locked is not None:
            on_locked()
        return True

    def on_tick(event: object) -> None:
        if _refused():
            return
        value = getattr(event, "value", None) or []
        names = selected_object_names(value)
        chosen = [name for name in migratable_order if name in names]
        # G16: the add-lock is read at the CLICK; a tick that would add a table it refuses
        # applies nothing (all or nothing, like the lock above).
        lock = add_lock_fn() if add_lock_fn is not None else None
        if lock is not None:
            current = _current_effective()
            blocked = lock.blocked([n for n in chosen if n not in current], tree=True)
            if blocked:
                ui.notify(
                    f"{lock.refusal(blocked, tree=True)} This change was not applied.",
                    type="warning",
                    multi_line=True,
                )
                if on_locked is not None:
                    on_locked()
                return
            # G137: an untick of a table read in parts is refused whole too (an error: it
            # would leave its copy torn).
            chosen_set = set(chosen)
            kept_split = lock.removal_refusal(
                [n for n in migratable_order if n in current and n not in chosen_set],
                tree=True,
            )
            if kept_split:
                ui.notify(
                    f"{kept_split} This change was not applied.",
                    type="negative",
                    multi_line=True,
                )
                if on_locked is not None:
                    on_locked()
                return
        migration_state.set_selection(
            TableSelection(selected_tables=chosen + _kept(lock))
        )
        count_label.text = f"{len(chosen)} of {total_leaves} selected"
        if on_selection_change is not None:
            on_selection_change(chosen)

    # Filter + bulk actions + selection counter (Cloudscape table header band).
    filter_input = None
    count_label = None
    if not locked:
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            filter_input = (
                ui.input(
                    placeholder="Filter tables by name",
                    value=(filter_state or {}).get("text") or "",
                    on_change=(
                        (lambda e: filter_state.__setitem__(
                            "text", getattr(e, "value", "") or ""))
                        if filter_state is not None
                        else None
                    ),
                )
                .props("dense clearable outlined")
                .classes("flex-1 min-w-0 text-sm")
            )

            def _dm_select_all() -> None:
                if _refused():
                    return
                lock = add_lock_fn() if add_lock_fn is not None else None
                if lock is None:
                    tree.tick()
                    migration_state.set_selection(
                        TableSelection(selected_tables=list(migratable_order))
                    )
                    count_label.text = f"{len(migratable_order)} of {total_leaves} selected"
                    if on_selection_change is not None:
                        on_selection_change(list(migratable_order))
                    return
                # G16: only the tables that may be selected now. Explicit ids: tick() with
                # none ticks EVERY node, locked leaves included.
                current = _current_effective()
                ids = [
                    n for n in migratable_order
                    if n in current or lock.code(n, tree=True) is None
                ]
                tree.tick([f"{TABLE_PREFIX}{n}" for n in ids])
                migration_state.set_selection(
                    TableSelection(selected_tables=ids + _kept(lock))
                )
                count_label.text = f"{len(ids)} of {total_leaves} selected"
                skipped = len(migratable_order) - len(ids)
                if skipped:
                    one = skipped == 1
                    ui.notify(
                        f"{skipped} table{'' if one else 's'} cannot be selected now and "
                        f"{'stays' if one else 'stay'} unselected (a lock marks "
                        f"{'it' if one else 'them'} in the list).",
                        type="info",
                    )
                if on_selection_change is not None:
                    on_selection_change(list(ids))

            def _dm_unselect_all() -> None:
                if _refused():
                    return
                lock = add_lock_fn() if add_lock_fn is not None else None
                # G137: a table read in parts stays selected (CDC must replay it); the rest
                # are unselected, as Select all selects only what may be selected.
                current = _current_effective() if lock is not None else set()
                held = (
                    lock.removal_blocked([n for n in migratable_order if n in current])
                    if lock is not None
                    else []
                )
                if not held:
                    tree.untick()
                    migration_state.set_selection(
                        TableSelection(selected_tables=_kept(lock))
                    )
                    count_label.text = f"0 of {total_leaves} selected"
                    if on_selection_change is not None:
                        on_selection_change([])
                    return
                held_set = set(held)
                tree.untick(
                    [f"{TABLE_PREFIX}{n}" for n in migratable_order if n not in held_set]
                )
                migration_state.set_selection(
                    TableSelection(selected_tables=list(held) + _kept(lock))
                )
                count_label.text = f"{len(held)} of {total_leaves} selected"
                one = len(held) == 1
                ui.notify(
                    f"{lock.removal_refusal(held, tree=True)} "
                    f"{'It stays' if one else 'They stay'} selected; every other table was "
                    "unselected.",
                    type="negative",
                    multi_line=True,
                )
                if on_selection_change is not None:
                    on_selection_change(list(held))

            ui.button("Select all", on_click=_dm_select_all).props(
                "flat dense no-caps size=sm color=primary icon=done_all"
            )
            ui.button("Unselect all", on_click=_dm_unselect_all).props(
                "flat dense no-caps size=sm color=grey-7 icon=remove_done"
            )
            count_label = ui.label(
                f"{len(effective_set)} of {total_leaves} selected"
            ).classes("text-xs text-gray-500 whitespace-nowrap")

    # Legend for the per-table primary-key indicator, so the check / warning icons
    # beside each leaf are self-explanatory (they also carry a hover tooltip).
    with ui.row().classes("items-center gap-3 w-full text-xs text-gray-500"):
        with ui.row().classes("items-center gap-1 no-wrap"):
            ui.icon("check_circle", color="green-6").classes("text-sm")
            ui.label("Has a primary key")
        with ui.row().classes("items-center gap-1 no-wrap"):
            ui.icon("warning", color="amber-7").classes("text-sm")
            ui.label("No primary key (required to migrate to Aurora DSQL)")

    with ui.scroll_area().classes(
        "w-full bg-white rounded-md border border-gray-200"
    ).style("height: 280px"):
        # When locked, omit on_tick so ticks can't change the selection; grey the
        # checkboxes (vs primary) so it reads as non-interactive, not just inert.
        tree = ui.tree(
            nodes,
            label_key="label",
            node_key="id",
            tick_strategy="leaf",
            on_tick=None if locked else on_tick,
        )
        tree.props(f"tick-color={'grey' if locked else 'primary'} no-connectors")
        # Wire the name filter to the tree (Quasar QTree ``filter`` prop): typing
        # narrows the visible nodes to matching table leaves. Without this bind
        # the input renders but does nothing.
        if filter_input is not None:
            filter_input.bind_value_to(tree, "filter")
        # A small PK indicator beside each table leaf (client-side Vue template):
        # a green check when the table has a primary key, an amber warning when it
        # does not. Non-leaf nodes (schema / "Tables (N)") have no "header" key, so
        # this slot only renders on table leaves.
        tree.add_slot(
            "header-table",
            r"""
            <div class="row items-center no-wrap">
              <span class="text-body2">{{ props.node.label }}</span>
              <q-icon v-if="props.node.has_pk" name="check_circle"
                      color="green-6" size="16px" class="q-ml-xs">
                <q-tooltip>Has a primary key</q-tooltip>
              </q-icon>
              <q-icon v-else name="warning" color="amber-7" size="16px" class="q-ml-xs">
                <q-tooltip>No primary key — required to migrate to Aurora DSQL</q-tooltip>
              </q-icon>
            </div>
            """
            if render_lock is None
            else _header_table_slot_with_locks(),
        )
        # Load fully expanded so every schema's tables are visible without manual
        # drilling (the migratable-only tree keeps this bounded).
        tree.expand()
        ticked_ids = [f"{TABLE_PREFIX}{name}" for name in effective]
        if ticked_ids:
            tree.tick(ticked_ids)
        if locked:
            # pointer-events-none blocks clicks; the greyed checkboxes + dimmed
            # tree communicate "selection is locked" at a glance (not just dead).
            tree.props("no-selection-unset").classes(
                "pointer-events-none opacity-70"
            )
