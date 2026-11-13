# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Why PostgreSQL CDC must not start against the objects on the source, and the way on.

The blocks -- a slot that is not at the Full Load's start point, a missing slot, a name that
is not usable, a missing publication, one that does not cover every selected table or does
not publish every change type, a table with inheritance child tables, a read that failed --
with their headers, the body each one is given, and the ROUTE rule that says how to get a
stream going again from where the stack now is (:func:`pg_cdc_route`: the fix a block names
itself, new topics and a Full Load re-run on a stack that has streamed, a re-snapshot on one
that has not).

All of it is PURE: every function here grades a reading somebody else took and returns text.
:mod:`dsql_migrator.core.cdc_pg_slot` -- which takes those readings over a real connection
and makes the source writes -- re-exports every name below, so the UI, the prerequisites and
the tests keep importing them from there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect import dialect_for

if TYPE_CHECKING:  # the reading the blocker grades; cdc_pg_slot is what reads it
    from dsql_migrator.core.cdc_pg_slot import PgReplicationObjects


# The slot-position block (G25). It judges only a stack whose connectors were never started --
# none stopped, in place or detected, or created by a Start whose update CloudFormation rolled
# back, whatever the rollback status (``_cdc_ui._pg_stack_started``); its way out is the route
# rule's (:func:`pg_cdc_route`), like every block's.
SLOT_POSITION_HEADER = "CDC's replication slot is not at this Full Load's start point"
# The other blocks whose stream cannot continue as is (G25), each routed by pg_cdc_route.
SLOT_MISSING_HEADER = "CDC's replication slot does not exist on the source"
SLOT_UNUSABLE_HEADER = "A replication slot of that name is not usable for CDC"
PUBLICATION_MISSING_HEADER = "CDC's publication does not exist on the source"
PUBLICATION_COVERAGE_HEADER = "CDC's publication does not cover every selected table"
# Blocks that name their own fix (G25), so the Start CDC dialog does not judge a re-snapshot
# for them (``_cdc_ui._pg_objects_gate_block``): a re-snapshot fixes none of them -- Debezium
# reuses an existing publication as it is, and its snapshot is what reads a parent's child
# tables. A read again works on any stack; the inheritance block's own fix is a Full Load run,
# which pg_cdc_route puts after new topics on a stack started before. The publish list's fix
# comes first but does not bring back the changes committed while the list was narrowed
# (pgoutput publishes from the publication as it stood at each change's commit), so
# pg_cdc_route names the route after it, as for every block whose stream cannot continue.
PUBLISH_LIST_HEADER = "CDC's publication does not publish every change type"
INHERITANCE_SNAPSHOT_HEADER = "CDC cannot snapshot a table that has inheritance child tables"
# The read behind that block failed: fail closed, and its fix is to read again (G25).
INHERITANCE_UNREADABLE_HEADER = (
    "Whether a captured table has inheritance child tables could not be read"
)
BLOCKS_WITH_THEIR_OWN_FIX = frozenset(
    {PUBLISH_LIST_HEADER, INHERITANCE_SNAPSHOT_HEADER, INHERITANCE_UNREADABLE_HEADER}
)

# The re-snapshot remedy's standing on a stack never started (``pg_cdc_route``'s
# ``resnapshot``): the Start CDC dialog judges the Start it would unlock before it offers it.
RESNAPSHOT_OFFERED = "offered"  # that Start's own checks passed: the dialog offers the remedy
RESNAPSHOT_REFUSED = "refused"  # they refused it, for the reason ``block`` names

_RERUN_FULL_LOAD = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
_SLOT_AGAIN = "it creates the slot again at its own snapshot point"
_NEW_TOPICS = (
    "use Delete CDC infrastructure and deploy it again (the new stack starts with empty topics)"
)
# Why a stack started before needs new topics before a Full Load re-run (G25): Stop CDC deletes
# the source and the sink in parallel, with no drain, and a rolled-back Start deletes them
# mid-stream, so the topics may hold changes no sink applied; this stack's sink group resumes
# from its last commit and applies them over the reloaded tables -- silently.
_UNAPPLIED_IN_TOPICS = (
    "its CDC topics may still hold changes that no sink applied (Stop CDC and a rolled-back "
    "Start remove the connectors without draining them), which this stack's sink would apply "
    "over a reload"
)
_SNAPSHOT_FIRST = "it snapshots every selected table"
# The blocks a stack started before gets the new-topics route for (G25): every one but a
# failed inheritance read, whose own fix (a read again) works on any stack.
_STARTED_ROUTED_BLOCKS = frozenset(
    {
        SLOT_POSITION_HEADER,
        SLOT_MISSING_HEADER,
        SLOT_UNUSABLE_HEADER,
        PUBLICATION_MISSING_HEADER,
        PUBLICATION_COVERAGE_HEADER,
        PUBLISH_LIST_HEADER,
        INHERITANCE_SNAPSHOT_HEADER,
    }
)
# The blocks whose own fix comes first, before the route (G25): a slot of that name it cannot
# use (drop it, or another CDC stack name -- a Full Load re-run refuses such a slot before any
# write), and a narrowed publish list (set it back -- which does not bring back the changes
# committed while it was narrowed).
_FIX_FIRST_BLOCKS = frozenset({SLOT_UNUSABLE_HEADER, PUBLISH_LIST_HEADER})


def _sentence(clause: str) -> str:
    """``clause`` as a sentence: its first letter capitalised, a period added. Pure."""
    return f"{clause[:1].upper()}{clause[1:]}."


def _new_topics_route(cdc_only: Optional[bool]) -> str:
    """The route of a stack started before, as a clause: new topics first (G25). Pure."""
    full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN})"
    if cdc_only is None:
        return (
            f"{_NEW_TOPICS}; then, for Full load + CDC, {full_load} and Start CDC, and for CDC "
            f"only, Start CDC on the new stack ({_SNAPSHOT_FIRST})"
        )
    if cdc_only:
        return (
            f"{_NEW_TOPICS}, then Start CDC on the new stack, which snapshots every selected "
            "table (a row deleted on the source in between stays on the target; Validation "
            "reports it as an EXTRA row)"
        )
    return f"{_NEW_TOPICS}, then {full_load}, then Start CDC"


def _no_snapshot_route(cdc_only: Optional[bool], *, note: str = "") -> str:
    """The route of a stack never started once a re-snapshot is ruled out, as a clause: a Full
    Load run, which creates the slot again, so the Start CDC after it takes no snapshot -- after
    a switch to Full load + CDC with CDC only, which has no Full Load step (G25). Pure."""
    full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN}{note}), then Start CDC"
    if cdc_only is None:
        return f"{full_load} (with CDC only, switch the migration type to Full load + CDC first)"
    if cdc_only:
        return f"switch the migration type to Full load + CDC, then {full_load}"
    return full_load


def pg_cdc_route(
    block: Optional[str],
    *,
    started: bool,
    cdc_only: Optional[bool],
    resnapshot: Optional[str] = None,
) -> str:
    """The way out of a PostgreSQL CDC block whose stream cannot continue as is -- THE route
    rule (G25). Every surface that names a route takes it from here: the Start CDC dialog's
    block, its withheld re-snapshot notice, the Start CDC job's refusal, the "Publication
    autocreation is disabled" diagnosis and the Start CDC / start-point cards. Pure.

    ``block``: the block's header; ``None`` for a card naming the route of a stack started
    before mid-sentence (the bare clause, no capital, no period). ``started``: the stack's
    connectors were started before (``_cdc_ui._pg_stack_started``: stopped since, in place, or
    created by a Start CloudFormation rolled back). ``cdc_only``: the migration type is CDC
    only (``None``: not known here, so both types are named).

    A stack started before: its CDC topics may hold changes no sink applied, and its sink would
    resume from its last commit, so its ONLY route is new topics first -- Delete CDC
    infrastructure and deploy it again, then (Full load + CDC) Re-run Full Load with Drop &
    reload, then Start CDC; (CDC only) Start CDC on the new stack, which snapshots. A Full Load
    re-run on its topics would have those changes applied over the reloaded tables, and a
    re-snapshot cannot be relied on there: Debezium continues from a stored offset whose
    snapshot completed.

    A stack never started: the re-snapshot remedy when its own checks pass (``resnapshot`` is
    :data:`RESNAPSHOT_OFFERED`: nothing to add, the dialog offers it), else a Start that takes
    no snapshot after a Full Load re-run -- a step after the block's own fix when the block
    names one -- or, with CDC only, the Start once its checks pass, Delete CDC infrastructure
    for a publication that misses a table, and a switch to Full load + CDC where only a Start
    with no snapshot can run (never a Full Load step it does not have). ``resnapshot`` is
    :data:`RESNAPSHOT_REFUSED` when ``block`` is the refusal of that remedy (or of the Start for
    the same reason), ``None`` when it was not judged (the Start CDC job): both are named.

    A narrowed publish list's own fix comes first, and the route follows it: setting the list
    back does not bring back the changes committed while it was narrowed (pgoutput publishes
    from the publication as it stood when each change committed), so a Start that continues
    from the slot would skip them -- a Full Load re-run after the fix (with CDC only, behind a
    switch to Full load + CDC), or new topics first on a stack started before. A failed
    inheritance read names its own fix, which works on any stack: no route. So does the
    inheritance block of a never-started stack's own Start."""
    if not started:
        return _never_started_route(block, cdc_only=cdc_only, resnapshot=resnapshot)
    if block is None:
        return _new_topics_route(cdc_only)
    if block not in _STARTED_ROUTED_BLOCKS:
        return ""
    if block == INHERITANCE_SNAPSHOT_HEADER:
        # Its own fix is a Full load + CDC run: new topics first (either type).
        return (
            f"This stack's connectors were started before, and {_UNAPPLIED_IN_TOPICS}. So "
            f"before that Full Load, {_NEW_TOPICS}."
        )
    # A block that names its own fix (a slot of that name it cannot use, a narrowed publish
    # list): the route comes after it.
    head = "The gapless route"
    if block in _FIX_FIRST_BLOCKS:
        head = "After that fix, the gapless route"
    return (
        "This stack's connectors were started before: if its source connector streamed, "
        "Debezium skips the snapshot on every later Start, so a re-snapshot cannot be relied on "
        f"here, and {_UNAPPLIED_IN_TOPICS}. {head}: {_new_topics_route(cdc_only)}."
    )


def _never_started_route(
    block: Optional[str], *, cdc_only: Optional[bool], resnapshot: Optional[str]
) -> str:
    """:func:`pg_cdc_route` for a stack whose connectors were never started. Pure."""
    if block == PUBLICATION_COVERAGE_HEADER:
        # A re-snapshot grades coverage too, so it never helps: a Full Load re-run adds each
        # missing table to the reused publication (G17); Delete CDC infrastructure drops it.
        full_load = (
            f"{_RERUN_FULL_LOAD}, which adds each missing selected table to the publication and "
            "creates the slot again at its own snapshot point, then Start CDC"
        )
        delete = (
            "use Delete CDC infrastructure and deploy it again: it drops the publication, so the "
            "new stack's Start CDC can create one over the selected tables"
        )
        if cdc_only is None:
            return f"{full_load} (with CDC only, {delete})."
        return _sentence(delete if cdc_only else full_load)
    if block == SLOT_UNUSABLE_HEADER:
        # After its own fix (drop that slot, or another CDC stack name): a Full Load re-run
        # would refuse such a slot before any write. CDC only: the Start, its checks then pass.
        full_load = f"{_RERUN_FULL_LOAD} ({_SLOT_AGAIN}), then Start CDC"
        if cdc_only is None:
            return f"Then {full_load} (with CDC only, Start CDC again)."
        return "Then Start CDC again." if cdc_only else f"Then {full_load}."
    if block == PUBLISH_LIST_HEADER:
        # After its own fix (the list set back): a Start that continues from the slot would
        # still skip the changes committed while it was narrowed, and a re-snapshot is not
        # offered (it reuses the publication as it is), so a Full Load re-run takes a new start
        # point -- with CDC only, after a switch to Full load + CDC.
        return f"Then {_no_snapshot_route(cdc_only)}."
    if block in (INHERITANCE_SNAPSHOT_HEADER, INHERITANCE_UNREADABLE_HEADER):
        # The refusal of the re-snapshot remedy: a Start with no snapshot works. As the block of
        # the Start itself, its own fix stands alone.
        if resnapshot != RESNAPSHOT_REFUSED:
            return ""
        no_snapshot = f"{_SLOT_AGAIN}, and the Start CDC after it takes no snapshot"
        if cdc_only:
            return (
                f"Or switch the migration type to Full load + CDC, then {_RERUN_FULL_LOAD}: "
                f"{no_snapshot}."
            )
        if cdc_only is None:
            return (
                f"{_RERUN_FULL_LOAD} instead (with CDC only, switch the migration type to Full "
                f"load + CDC first): {no_snapshot}."
            )
        return f"{_RERUN_FULL_LOAD} instead: {no_snapshot}."
    if block in (SLOT_POSITION_HEADER, SLOT_MISSING_HEADER, PUBLICATION_MISSING_HEADER):
        # A Start that resumes from the Full Load's slot: a re-snapshot fixes it when its own
        # checks pass, else a Full Load run creates the slot again.
        if resnapshot == RESNAPSHOT_OFFERED:
            return ""
        note = (
            "; give this migration its own CDC stack name if another migration shares this one"
            if block == SLOT_POSITION_HEADER
            else ""
        )
        route = _no_snapshot_route(cdc_only, note=note)
        if resnapshot == RESNAPSHOT_REFUSED:
            return _sentence(route)
        return (
            "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
            f"re-snapshots passes its own checks), or {route}."
        )
    return ""


def publish_list_fix(publication_name: str) -> str:
    """The fix for a publication restricted to a subset of INSERT/UPDATE/DELETE: its publish
    list set in place, by its owner. A re-snapshot and a Full Load re-run both reuse an
    existing publication as it is; only this -- or Delete CDC infrastructure, which drops
    it -- changes the list (G25). Pure."""
    quoted = dialect_for(SourceType.POSTGRES).quote_identifier(str(publication_name))
    return (
        f"As its owner, run ALTER PUBLICATION {quoted} SET (publish = 'insert, update, "
        "delete') on the source"
    )


def _inheritance_snapshot_block(parents: Sequence[tuple], *, untick: bool = True) -> tuple:
    """The block for a Start that snapshots a plain table with inheritance child tables
    (``parents``: :func:`read_inheritance_children` rows, at least one). Pure (G25).

    ``untick``: the table set can still change, so unticking the parent is a fix the UI can
    carry out. Once a Start was submitted (or with CDC only, while its infrastructure exists)
    the table picker is locked, and the block names only the Start from a Full Load's snapshot
    point."""
    table, count, sample = parents[0]
    shown = ", ".join(sample) + (" …" if count > len(sample) else "")
    others = len(parents) - 1
    more = (
        f" (and {others} more captured table{'s' if others != 1 else ''} like it)"
        if others
        else ""
    )
    alternative = (
        f" -- or untick {table}: each child table migrates as a table of its own."
        if untick
        else "."
    )
    return (
        INHERITANCE_SNAPSHOT_HEADER,
        f"{table} has {count} inheritance (INHERITS) child table{'s' if count != 1 else ''}: "
        f"{shown}{more}. This Start sends snapshot.mode=initial, and Debezium's snapshot "
        f"reads {table} with its child tables' rows: they would be copied into {table} on "
        f"Aurora DSQL, where CDC never updates or deletes them (it captures only {table}'s "
        "own changes). Start CDC from a Full Load's snapshot point instead -- a \"Full load "
        f"+ CDC\" run whose Full Load has finished reads only {table}'s own rows, and its "
        f"CDC starts without a snapshot{alternative}",
    )


# Fail closed (G25): a snapshot of a parent copies its child tables' rows into it, and
# Debezium's own publication then names it without ONLY, adding child tables whose REPLICA
# IDENTITY nothing grades -- so a Start that snapshots never goes ahead without that read.
_INHERITANCE_UNREADABLE_BLOCK = (
    INHERITANCE_UNREADABLE_HEADER,
    "This Start sends snapshot.mode=initial. Debezium's snapshot reads a table with "
    "inheritance (INHERITS) child tables together with their rows, which CDC never updates "
    "or deletes on Aurora DSQL, so Start CDC first reads which captured tables have any, and "
    "that read failed. Check that the source is reachable from the tool, then Start CDC "
    "again.",
)


def _lsn_value(lsn: Optional[str]) -> Optional[int]:
    """A PostgreSQL ``X/Y`` LSN as one integer, or ``None`` when it is not one. Pure."""
    high, separator, low = str(lsn or "").strip().partition("/")
    try:
        return (int(high, 16) << 32) | int(low, 16) if separator else None
    except ValueError:
        return None


def pg_replication_objects_blocker(
    objects: PgReplicationObjects,
    selected_tables: Sequence[str],
    *,
    resumes_from_slot: bool,
    connector_autocreates_publication: bool = False,
    start_lsn: Optional[str] = None,
    stack_started: bool = False,
    snapshots: bool = False,
    untick: bool = True,
) -> Optional[tuple]:
    """Why CDC must not start against these objects, as ``(header, body)``, or None. Pure.

    The two keywords are the connector's INTENT and must mirror
    ``build_pg_source_config`` exactly, or this refuses a configuration that would have
    worked:

    * ``resumes_from_slot`` <-> ``snapshot.mode=never``. True only when the watermark
      carries the slot the Full Load created. With ``initial`` instead, Debezium creates
      the slot itself and snapshots every captured table, so a missing slot is not a gap
      and must not block.
    * ``connector_autocreates_publication`` <-> ``publication.autocreate.mode=filtered``.
      True when the connector's own DB user will create the publication over exactly the
      captured tables. Without this, an absent publication blocked UNCONDITIONALLY -- so
      the Start dialog's "Re-snapshot every table instead" button un-disabled Start and
      the worker backstop, which calls this function right before creating anything, then
      raised "Start CDC blocked". The escape hatch shipped dead in v0.1.507.

    Coverage is graded, not just existence: a publication that omits a selected table lets
    Debezium reach RUNNING while replicating NOTHING for it -- strictly worse than a task
    that dies loudly. Deliberately NOT graded here: ``puballtables`` and whether the slot is
    currently ``active`` (neither is a failure of THIS migration).

    The slot's POSITION too (G25), on a stack's FIRST Start only: ``start_lsn`` is the Full
    Load watermark's LSN -- the consistent point of the slot it created. While the stack
    records no Start of its connectors (``stack_started`` False), none of them has streamed
    from the slot, so it must still be exactly there; anywhere else, another run using the
    same CDC stack name re-created it (or read it), and resuming would silently skip the
    changes in between. Its way out, like every block's, is the route rule's
    (:func:`pg_cdc_route`), which knows the migration type and the re-snapshot remedy's
    standing: the block itself names none. A stack whose connectors were started before is
    never judged on it: its connector moved the slot legitimately, and Debezium
    continues from the later of its stored offset and the slot's confirmed point (a Full Load
    that takes a new start point waits for new topics there: the Full Load step refuses it on
    a stack that has streamed, ``_cdc_status.cdc_stack_has_streamed``). A Start whose
    CloudFormation update rolled back counts as one, whatever the rollback status (complete,
    in progress or failed), and so do connectors of the stack still in place
    (``_cdc_ui._pg_stack_started``): its source connector may have streamed before the
    rollback deleted it. Stateless -- what the
    stack records decides, nothing the UI keeps -- so a refused Start, one that failed before
    it updated the stack, an app restart and Start over all leave the next Start judged the
    same way. Residual, like a re-created slot after this stack streamed (each migration
    needs its own CDC stack name): another migration re-creates the slot after this stack's
    first Start rolled back.

    ``snapshots`` <-> ``snapshot.mode=initial`` (G25): a captured plain table with inheritance
    child tables (``objects.inheritance_children``) blocks, because Debezium's snapshot reads
    it without ``ONLY``, so its children's rows would land in its target, and CDC never
    changes them there -- and so does a failed read of them (``None``): fail closed, as this
    block is all that keeps Debezium's own publication from adding child tables nothing
    grades. ``untick`` (:func:`_inheritance_snapshot_block`): the table set can still change,
    so that block also names unticking the parent. A slot of that name it cannot use is
    reported before it: every route needs that slot's own fix first. A narrowed publish list
    names its own fix (:func:`publish_list_fix`), which no re-snapshot or Full Load re-run
    makes, and which does not bring back the changes committed while the list was narrowed:
    the route rule's route follows it (:func:`pg_cdc_route`).
    """
    pub = objects.publication_name
    if not objects.publication_present and not connector_autocreates_publication:
        return (
            PUBLICATION_MISSING_HEADER,
            f"The connector is configured with publication.autocreate.mode=disabled and "
            f"expects a publication named '{pub}', which is not on the source. Only a "
            "'Full load + CDC' run creates it; a 'Full load only' run does not. Starting "
            "now would deploy the connectors and the Debezium task would die immediately "
            "with \"Publication autocreation is disabled\".",
        )
    # Coverage and publish-list grading READ the publication, so they are meaningless when
    # it does not exist yet and the connector is about to create it: an empty
    # publication_tables would otherwise report every selected table as "missing" and
    # block on the very next branch.
    missing = (
        sorted(set(selected_tables) - set(objects.publication_tables))
        if objects.publication_present
        else []
    )
    if missing:
        shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        return (
            PUBLICATION_COVERAGE_HEADER,
            f"Publication '{pub}' exists but does not include {len(missing)} of the "
            f"selected tables: {shown}. Those tables would replicate NOTHING while the "
            "connector reported RUNNING, which is worse than a visible failure.",
        )
    if objects.publication_present and objects.publication_publishes_all_dml is False:
        # Its own fix first, then the route rule's (decision C, G25): pgoutput decides what to
        # publish from the publication as it stood when each change committed, so setting the
        # list back does not bring back what was left out while it was narrowed.
        return (
            PUBLISH_LIST_HEADER,
            f"Publication '{pub}' is restricted to a subset of INSERT/UPDATE/DELETE, so "
            "the change types it omits would never reach the target and the connector "
            "would still report RUNNING. A re-snapshot and a Full Load re-run both reuse "
            f"it as it is. {publish_list_fix(pub)} first. Setting the list back does not "
            "bring back the changes committed while it was narrowed: pgoutput decides what to "
            "publish from the publication as it stood when each change committed, so a Start "
            "that continues from the slot would still skip them.",
        )
    if resumes_from_slot and not objects.slot_usable:
        # A same-named slot it cannot use (G25): its own fix comes before any route -- a Full
        # Load re-run drops only an inactive slot of this database, and refuses another
        # database's (or another type's) before any write.
        elsewhere = (
            " A slot of that name does exist in this cluster (invalidated, in another database, "
            "or not a logical pgoutput slot): if it is another database's or of another type, "
            "drop it or give this migration its own CDC stack name first -- a Full Load re-run "
            "refuses such a slot before any write."
            if objects.slot_present_any_database
            else ""
        )
        return (
            SLOT_MISSING_HEADER,
            f"This start is configured to resume from replication slot "
            f"'{objects.slot_name}' (snapshot.mode=never), and the source has no usable "
            "logical pgoutput slot of that name in this database. A slot cannot be created "
            "at a past position, so one created now would start from the CURRENT WAL and "
            f"silently skip every change committed since the Full Load.{elsewhere}",
        )
    # Before the inheritance check (G25): every route needs this fix first -- a Start that
    # snapshots cannot create its own slot under that name, and a Full Load re-run refuses
    # such a slot before any write -- so the refusal of a re-snapshot names it, not a Full
    # Load re-run that would be refused too.
    if objects.slot_present_any_database and not objects.slot_usable:
        return (
            SLOT_UNUSABLE_HEADER,
            f"A slot named '{objects.slot_name}' exists in this cluster but is not a "
            "logical pgoutput slot in this database, so the connector cannot use it and "
            "cannot create its own under that name. Drop it, or use a different CDC stack "
            "name so the derived slot name differs.",
        )
    if snapshots:
        if objects.inheritance_children is None:
            return _INHERITANCE_UNREADABLE_BLOCK
        captured = set(selected_tables)
        parents = [p for p in objects.inheritance_children if p[0] in captured]
        if parents:
            return _inheritance_snapshot_block(parents, untick=untick)
    at = _lsn_value(objects.slot_confirmed_flush_lsn)
    wanted = _lsn_value(start_lsn)
    if (
        resumes_from_slot
        and not stack_started
        and at is not None
        and wanted is not None
        and at != wanted
    ):
        return (
            SLOT_POSITION_HEADER,
            f"Slot '{objects.slot_name}' is at {objects.slot_confirmed_flush_lsn}, not at "
            f"{start_lsn} where this Full Load's CDC starts, and this CDC stack records no "
            "Start of its connectors (not even one that rolled back), so none of them has "
            "streamed from it: another run with the same CDC stack name re-created or read "
            "it, so starting now would silently skip the changes in between.",
        )
    return None
