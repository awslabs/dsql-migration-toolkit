# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The MySQL CDC start-point check: refuse a ``snapshot.mode=recovery`` Start whose captured
tables changed since the start point. Pure (verdict + copy); the read lives in
``data_migration._cdc_status.read_cdc_start_point_once``.

A MySQL Start that seeds a NEW start offset (the first Start from the Full Load watermark or
a Manual binlog point, a Start after Re-run Full Load or a newer Manual point, the first
Start after Delete CDC infrastructure) runs Debezium in ``recovery`` mode: it rebuilds its
schema history from the tables AS THEY ARE NOW and then reads the binlog from that EARLIER
position, matching row values to columns by position. A column added or dropped since then
stops the connector on every start, a rename stops it, and a same-count change (a type
change other than a CHAR / VARCHAR / VARBINARY length or a DECIMAL precision, a reorder, a
replacement, a character-set conversion) writes shifted or NULL values with no error at
all. Starting again, Retry CDC and Debezium's own "recovery mode" advice all replay the same
rows, so the only way forward is a start point after the change.

The start point records a short per-table fingerprint (``core.cdc_catalog_drift``): the
Full Load watermark does, just before its binlog position, and so does Fetch current
position for a Manual point. Start CDC compares the captured tables with it -- in the Start
dialog and again on the job's worker thread (Retry CDC skips the dialog) -- with ONE bounded
catalog statement. A resume of a stopped stack (``schema_only``: the recorded history is
replayed in binlog order) is not checked, and neither is a stack whose connectors exist --
except a resume whose schema history is missing: it rebuilds the history (``recovery``) at
the committed position, so it is checked against the start point, which that position is
past, and its copy says so (``resumes``).

Policy: a changed table is an error and blocks; a recorded fingerprint that cannot be
compared (no source connection, a failed read) blocks too, because the class it guards is
silent; a start point with no fingerprint (an earlier version of the tool, a typed position,
a failed capture) only warns, as before this check existed. Value-free (Property 7): table
names, fixed reason texts and an exception class name only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core.activity_log import ActivityStatus

KIND_OK = "ok"
KIND_CHANGED = "changed"
KIND_UNREADABLE = "unreadable"
KIND_UNVERIFIED = "unverified"

PROBLEM_NO_BASELINE = "no-baseline"
PROBLEM_NO_SOURCE = "no-source-connection"
PROBLEM_READ_FAILED = "read-failed"

# How many tables a notice names before counting the rest.
TABLE_LIMIT = 6

ACTION_CHANGED = "start blocked: captured tables changed since the CDC start point"
ACTION_UNREADABLE = "start blocked: CDC start point not checkable"
ACTION_UNVERIFIED = "CDC start point not checked"

E1_HEADER = "Captured tables changed since the CDC start point"
E2_HEADER = "Cannot check the captured tables against the CDC start point"
W1_HEADER = "The captured tables cannot be checked against this start point"

E1_TOOLTIP = "A captured table changed since the CDC start point — see the notice above."
E2_TOOLTIP = (
    "The captured tables could not be checked against the CDC start point — see the "
    "notice above."
)

NO_SEED_LABEL = "this stack's recorded stream position"
# How :func:`point_label` names a Manual point (the no-baseline causes differ by origin).
_MANUAL_PREFIX = "your Manual start point"


@dataclass(frozen=True)
class StartPointVerdict:
    """The outcome of one start-point check (value-free).

    ``kind``: ``ok`` / ``changed`` (``changed`` holds ``(table, reason)`` pairs) /
    ``unreadable`` (``problem`` is ``no-source-connection`` or ``read-failed``, with the
    exception class) / ``unverified`` (``unverified`` lists the tables with no usable
    fingerprint; ``problem`` is ``no-baseline`` when the start point has none at all).
    """

    kind: str
    changed: tuple = ()
    unverified: tuple = ()
    problem: Optional[str] = None
    error_class: Optional[str] = None

    @property
    def blocks(self) -> bool:
        """Whether Start CDC must refuse."""
        return self.kind in (KIND_CHANGED, KIND_UNREADABLE)


def verdict_from_check(check: object) -> StartPointVerdict:
    """A verdict from ``core.cdc_catalog_drift.check_start_point``'s result. Pure."""
    changed = tuple(getattr(check, "changed", ()) or ())
    unverified = tuple(getattr(check, "unverified", ()) or ())
    if changed:
        return StartPointVerdict(kind=KIND_CHANGED, changed=changed, unverified=unverified)
    if unverified:
        return StartPointVerdict(kind=KIND_UNVERIFIED, unverified=unverified)
    return StartPointVerdict(kind=KIND_OK)


def _coord(seed: object) -> str:
    f = getattr(seed, "binlog_file", None)
    p = getattr(seed, "binlog_position", None)
    return f"binlog {f}:{p}" if f and p is not None else ""


def point_coord(seed: object) -> str:
    """``binlog F:P`` of the start point, or the no-seed label. Pure."""
    return _coord(seed) or NO_SEED_LABEL


def point_label(seed: object, origin: str) -> str:
    """How the copy names the start point. Pure."""
    coord = _coord(seed)
    if not coord:
        return NO_SEED_LABEL
    if origin == "manual":
        return f"{_MANUAL_PREFIX} ({coord})"
    ts = getattr(seed, "snapshot_timestamp", None)
    when = ts.strftime("%Y-%m-%d %H:%M UTC") if hasattr(ts, "strftime") else ""
    return f"the Full Load watermark ({coord}, captured {when})" if when else (
        f"the Full Load watermark ({coord})"
    )


def _capitalized(text: str) -> str:
    return text[:1].upper() + text[1:]


def _capped(items: "tuple | list") -> str:
    shown = list(items)[:TABLE_LIMIT]
    rest = len(items) - len(shown)
    return "; ".join(shown) + (f" (+{rest} more)" if rest > 0 else "")


def _names(items: "tuple | list") -> str:
    shown = list(items)[:TABLE_LIMIT]
    rest = len(items) - len(shown)
    return ", ".join(shown) + (f" (+{rest} more)" if rest > 0 else "")


def _cdc_only(migration_type: object) -> bool:
    value = getattr(migration_type, "value", migration_type)
    return str(value or "") == "cdc_only"


def changed_rows(verdict: StartPointVerdict) -> str:
    """``db.t (reason); ...`` capped at :data:`TABLE_LIMIT`. Pure."""
    return _capped([f"{name} ({reason})" for name, reason in verdict.changed])


# A reload resets a Full load + CDC Manual point to Automatic (reset_stale_manual_start_point),
# which offers this start point again: the Manual route must say to re-choose it, or it loops.
RELOAD_RESETS_MANUAL = (
    "A reload switches the CDC start point back to Automatic, so choose Manual again and "
    "Use this start point before Start CDC (the fetched point is kept)."
)

# G25 (decisions A/B): a new start point on a stack that has streamed -- stopped since, in
# place, or a Start CloudFormation rolled back -- needs new CDC topics first: its topics may
# hold changes no sink applied, and its sink resumes from its last commit. The Full Load step
# refuses a new start point there, and Start CDC refuses a newer one (A2).
NEW_TOPICS = (
    "Delete CDC infrastructure and deploy it again (the new stack starts with empty topics)"
)


def new_topics_route(migration_type: object) -> str:
    """The route to a new start point on a stack that has streamed, as a clause. Pure."""
    if _cdc_only(migration_type):
        return f"{NEW_TOPICS}, then Start CDC on the new stack"
    return (
        f"{NEW_TOPICS}, then Re-run Full Load on the Full Load step choosing 'Drop & reload', "
        "then Start CDC"
    )


# G26: with CDC only, the refusals' route starts the new stack from the Full Load's watermark,
# and CDC from a watermark carries only the changes after it. A Full Load that appended to rows
# already on the target (Append is the Re-run dialog's default) kept them as they were, so an
# update made before that point stays stale and a delete stays an extra row, silently (ROW_COUNT
# can still match). The job record keeps no load mode: only the run's activity line names it, and
# a chunk's rows "already there" also count a source-drop re-read's or a Retry's own rows. So the
# route states the condition (rule E). RELOAD_FIRST follows its "then Start CDC on the new
# stack."; _RELOAD_FIRST_ROUTE is the one-line form of the route. G147: the re-run goes through
# Full load + CDC, whose Full Load makes the start point exact (a Full load only run's binlog
# position can cover a transaction still committing, mysql_start_point_fence).
RELOAD_FIRST = (
    "That Start begins at the Full Load's watermark, so if that Full Load appended to rows already "
    "on the target, run it again choosing 'Drop & reload' before that Start (CDC only has no Full "
    "Load step: switch the migration type to Full load + CDC for that run, whose Full Load makes "
    "the CDC start point exact). An append keeps those rows as they were, and CDC from a "
    "watermark carries only the changes after it."
)
_RELOAD_FIRST_ROUTE = (
    "Delete CDC infrastructure and deploy it again, then Start CDC (if the Full Load appended to "
    "rows already on the target, re-run it choosing 'Drop & reload' first)"
)


def reloads_first(migration_type: object, origin: str = "full_load") -> bool:
    """Does a CDC only refusal's route start the new stack from the Full Load's watermark, so
    that it names :data:`RELOAD_FIRST` (G26)? Not for a Manual start point (``origin``
    ``"manual"``): the new stack starts from that point. Pure."""
    return _cdc_only(migration_type) and origin == "full_load"


# G27: the same condition before a Start that is not refused and begins at the Full Load's
# watermark, when the session shows that Full Load was not this migration's first copy
# (``_cdc_status.drop_reload_condition_due``: Start CDC was submitted from an earlier start point
# before it ran and it found rows already on the target, or -- G27 repair, with or without a
# Start -- it found them on a table's first attempt). The start-point card, the Start CDC
# dialog and the Start's activity line say it; a first copy (a first Full Load, or a Drop &
# reload, which recreates each table it loads) keeps its text.
RELOAD_CONDITION_HEADER = "Gapless only if this Full Load did not append"
# The Start's activity line, in place of "(gapless from the Full Load watermark)".
RELOAD_CONDITION_LINE = (
    "(from the Full Load watermark; gapless only if that Full Load did not append to rows already "
    "on the target)"
)
# The start-point card's hover text for Automatic.
RELOAD_CONDITION_TOOLTIP = (
    "Where change streaming begins. Automatic starts where the Full Load snapshot ended, which "
    "is gapless only if that Full Load did not append to rows already on the target."
)


# G27 repair (rule E): the MySQL GTID-only start-point notice's re-run -- the Full Load again, for
# a watermark with a binlog position, is a NEW copy -- named here, with the other routes.
RERUN_FULL_LOAD_DROP_AND_RELOAD = "re-run the Full Load choosing 'Drop & reload'"


def drop_and_reload_rerun(
    migration_type: object, *, postgres: bool = False, split: bool = False
) -> "tuple[str, str]":
    """``(rerun, note)``: the re-run that takes a NEW copy of the Full Load, which names 'Drop &
    reload' (rule E), and what CDC only needs for it -- said after the clause ``rerun`` leads
    (empty otherwise). With CDC only the re-run needs a type switch to Full load + CDC: on
    PostgreSQL (``postgres``) its Full Load creates the replication slot again; on MySQL it makes
    the CDC start point exact (G147; as in :data:`RELOAD_FIRST`) -- a Full load only run records
    a binlog position no commit fence made exact, and refuses the switch anyway while a table the
    Full Load read in parts waits for CDC's replay (``split``, G137), so ``split`` no longer
    changes MySQL's route. Pure."""
    if _cdc_only(migration_type) and postgres:
        return (
            "switch the migration type to Full load + CDC and Re-run Full Load there choosing "
            "'Drop & reload'",
            " (it creates the slot again at its own snapshot point)",
        )
    if _cdc_only(migration_type):
        return (
            "run it again choosing 'Drop & reload'",
            " (CDC only has no Full Load step: switch the migration type to Full load + CDC for "
            "that run, whose Full Load makes the CDC start point exact)",
        )
    return "Re-run Full Load on the Full Load step choosing 'Drop & reload'", ""


def reload_condition(
    migration_type: object, *, postgres: bool = False, started: bool = False, split: bool = False
) -> str:
    """The body of the :data:`RELOAD_CONDITION_HEADER` notice: why it is shown, then
    :data:`RELOAD_FIRST`'s condition said before Start CDC, with the re-run
    :func:`drop_and_reload_rerun` names (``split``: through Full load + CDC from CDC only, G137).
    ``started`` (``_cdc_status.cdc_started_before_load``):
    Start CDC was submitted in this migration from an earlier start point before this Full Load
    ran, which the body then says first; without it (G27 repair: an Append re-run before any
    Start, or a Start from this watermark that did not stream) it says only what the Full Load
    found, which holds whichever sign showed the condition. Pure."""
    rerun, note = drop_and_reload_rerun(migration_type, postgres=postgres, split=split)
    why = (
        "This migration started CDC from an earlier start point before this Full Load ran, and "
        "the Full Load found rows already on the target."
        if started
        else "This Full Load found rows already on the target."
    )
    return (
        f"{why} Start CDC begins at its watermark, so if the Full Load appended to them, "
        f"{rerun} before Start CDC{note}. An append keeps those rows as they were, and CDC from "
        "a watermark carries only the changes after it."
    )


def _route_line(migration_type: object, origin: str) -> str:
    """The refusals' route in their one-line form. Pure."""
    if reloads_first(migration_type, origin):
        return _RELOAD_FIRST_ROUTE
    if _cdc_only(migration_type):
        return "Delete CDC infrastructure and deploy it again, then Start CDC on the new stack"
    return (
        "Delete CDC infrastructure and deploy it again, then Re-run Full Load choosing "
        "'Drop & reload', then Start CDC"
    )


# A2: Start CDC refuses a start point newer than the one a streamed stack last started from.
NEWER_POINT_HEADER = "This CDC stack cannot start from a newer start point"
# ...and every start point on a stack whose first Start CloudFormation rolled back, while the
# start offset that Start offered is unknown (``_cdc_ui._offer_unknown_refusal``): the refused
# point may be that same one, so this header does not call it newer (G26).
OFFER_UNKNOWN_HEADER = "This CDC stack's last start point is not known"


def newer_point_line(migration_type: object, origin: str = "full_load") -> str:
    """A2's one-line form: the disabled Start button's tooltip, a click-time refusal, and the
    Start CDC job's error after "Start CDC blocked: " -- under 300 characters with it, since a
    job error keeps only its first line, so it carries the reason and the route. ``origin`` is
    the refused start point's (:func:`reloads_first`); the route's Drop & reload condition
    takes the place of the parenthesised why. Pure."""
    why = (
        "" if reloads_first(migration_type, origin)
        else " (its sink would apply older changes over newer rows)"
    )
    return (
        "This CDC stack has streamed, so Start CDC does not move it forward to a newer start "
        f"point{why}: {_route_line(migration_type, origin)}. Nothing was changed."
    )


def offer_unknown_line(migration_type: object, origin: str = "full_load") -> str:
    """The one-line form of the refusal on a stack whose first Start CloudFormation rolled
    back, when the start offset that Start offered is no longer known -- neither this session
    nor the stack's events, as read, give it (``_cdc_ui._offer_unknown_refusal``) -- like
    :func:`newer_point_line`. Pure."""
    why = (
        "" if reloads_first(migration_type, origin)
        else ", so no start point can be checked against it"
    )
    return (
        "This CDC stack's last Start rolled back and the start offset it offered is no longer "
        f"known{why}: {_route_line(migration_type, origin)}. Nothing was changed."
    )


def newer_point_notice(
    *,
    point: str,
    last: Optional[str],
    rolled_back: bool,
    migration_type: object,
    automatic_resumes: bool = False,
    origin: str = "full_load",
) -> tuple[str, str, str, str]:
    """``(tone, icon, header, body)`` refusing a MySQL Start from ``point`` (a
    :func:`point_label`), later than ``last`` (``binlog F:P``, or ``None``: not recorded) --
    the start offset this streamed stack's last Start offered, or (``rolled_back``) the one
    the Start CloudFormation rolled back offered (G25, A2). ``automatic_resumes``: the start
    point is a Manual one, and Automatic would continue this stream instead. ``origin``: the
    start point's, for :func:`reloads_first`. Pure."""
    if rolled_back:
        lead = (
            f"CloudFormation rolled back this stack's last Start, which offered {last}; that "
            "Start's source connector may have streamed past it before the rollback removed "
            f"it, and {point} is later than {last}."
        )
    elif last:
        lead = (
            f"This CDC stack has streamed: the position it committed is at or past {last}, "
            f"the start offset its last Start offered, and {point} is later than that."
        )
    else:
        lead = (
            "This CDC stack has streamed, and its last Start offered no start offset, so "
            f"{point} cannot be shown to be at or before the position it committed."
        )
    body = (
        f"{lead} Start CDC does not move a streamed stack's position forward: its CDC topics "
        "may hold changes no sink applied (Stop CDC and a rolled-back Start remove the "
        "connectors without draining them), and its sink resumes from its last commit, so it "
        "would write those older changes over anything loaded or corrected since, while the "
        "source would skip every change between where it stopped and the new point — "
        "silently. To start from a new "
        f"start point, use {new_topics_route(migration_type)}."
    )
    if reloads_first(migration_type, origin):
        body += f" {RELOAD_FIRST}"
    if automatic_resumes:
        body += " To continue this stream instead, set the CDC start point back to Automatic."
    return "error", "fast_forward", NEWER_POINT_HEADER, body + " No connectors were changed."


def _ways_after_change(coord: str, migration_type: object, *, streamed: bool = False) -> str:
    if streamed:
        # G25: the stack has streamed (a resume, or a retry after a rolled-back Start), so a
        # start point after the change needs new CDC topics first (decisions A/B, A2).
        if _cdc_only(migration_type):
            return (
                "To continue, give CDC a start point after the change. This stack's "
                "connectors were started before, so that needs new CDC topics: "
                f"{NEW_TOPICS}, update these tables "
                "on Aurora DSQL to their new definitions (re-run Step 1 and Schema "
                "Conversion), then choose Manual on the new stack (Fetch current position, "
                "Use this start point) and Start CDC. Changes made between where CDC stopped "
                "and the new point are not replicated: correct them on Aurora DSQL and "
                "validate with CHECKSUM."
            )
        return (
            "To continue, give CDC a start point after the change. This stack's connectors "
            f"were started before, so that needs new CDC topics: {NEW_TOPICS}, re-run Step 1 "
            "(Evaluation) and "
            "update these tables in Schema Conversion, then Re-run Full Load for all tables "
            "with \"Drop & reload\", then Start CDC."
        )
    if _cdc_only(migration_type):
        return (
            "To continue, give CDC a start point after the change: update these tables on "
            "Aurora DSQL to their new definitions (re-run Step 1 and Schema Conversion), "
            "then choose Manual, then Fetch current position and Use this start point. "
            f"Changes made between {coord} and the new point are not replicated: correct "
            "them on Aurora DSQL and validate with CHECKSUM."
        )
    return (
        "To continue, give CDC a start point after the change: re-run Step 1 (Evaluation) "
        "and update these tables in Schema Conversion, then Re-run Full Load for all tables "
        "with \"Drop & reload\" — its new watermark is past the change (Reload specific "
        "tables and Retry keep this one). Or choose Manual, then Fetch current position "
        "and Use this start point: changes to every captured table between "
        f"{coord} and that point are then not replicated, so reload them with \"Drop & "
        f"reload\" and validate with CHECKSUM. {RELOAD_RESETS_MANUAL}"
    )


def _unreadable_reason(verdict: StartPointVerdict) -> tuple[str, str]:
    if verdict.problem == PROBLEM_NO_SOURCE:
        return (
            "this session has no source connection (the source password is kept in memory "
            "only, so an app restart clears it).",
            "Test the source connection on the Connect step, then Start CDC again.",
        )
    return (
        f"reading their definitions failed ({verdict.error_class or 'error'}).",
        "Check that the source database is reachable from the tool, then Start CDC again.",
    )


def _unverified_reason(verdict: StartPointVerdict, point: str) -> str:
    if verdict.problem == PROBLEM_NO_BASELINE:
        if point == NO_SEED_LABEL:
            return f"{_capitalized(point)} has no recorded definitions of the captured tables."
        # The causes a start point of THIS origin can have: a watermark is never typed.
        causes = (
            "typed by hand, fetched by an earlier version of the tool, or the definitions "
            "could not be read at the time"
            if point.startswith(_MANUAL_PREFIX)
            else "captured by an earlier version of the tool, or the definitions could not "
            "be read at the time"
        )
        return (
            f"{_capitalized(point)} has no recorded definitions of the captured tables "
            f"({causes})."
        )
    return (
        f"These captured tables were not recorded with {point}: "
        f"{_names(verdict.unverified)}."
    )


# A checked RESUME is only ever the history-missing one (an ordinary resume runs schema_only
# and is not checked): it rebuilds the history at the committed position, not at the point.
_RESUME_LEAD = (
    "This start continues from this stack's recorded stream position and rebuilds "
    "Debezium's schema history, which is missing, from the tables' definitions as they "
    "are now"
)
# What W1 calls an alteration that matters (a length or DECIMAL precision change does not).
_ALTERED = (
    "a column added, dropped, renamed, reordered, or changed to another type — a new "
    "length or DECIMAL precision aside"
)


def start_point_notice(
    verdict: Optional[StartPointVerdict],
    *,
    point_label: str,
    coord: str,
    migration_type: object,
    resumes: bool = False,
    streamed: bool = False,
    split_qualifier: str = "",
) -> Optional[tuple[str, str, str, str]]:
    """``(tone, icon, header, body)`` for the Start dialog / the job's refusal, or None. Pure.

    ``resumes``: the start resumes the stack's committed offset (a history-missing resume,
    the only checked one), so the copy says it reads from the recorded stream position --
    not from ``point_label``, which is only what the tables can be compared with.
    ``streamed``: the stack has streamed (a resume, or a retry after a Start CloudFormation
    rolled back), so a start point after the change needs new CDC topics first (G25): the
    Full Load step refuses a new start point there, and Start CDC refuses a newer one.
    ``split_qualifier`` (G137, ``_models.split_later_point_qualifier``): said after a remedy
    that names Fetch current position while CDC captures a table the Full Load read in parts
    -- Start CDC refuses that later point until the table is reloaded or matches in a
    CHECKSUM Validation.
    """
    notice = _start_point_notice(
        verdict, point_label=point_label, coord=coord, migration_type=migration_type,
        resumes=resumes, streamed=streamed,
    )
    return with_split_qualifier(notice, split_qualifier)


def with_split_qualifier(
    notice: Optional[tuple[str, str, str, str]], qualifier: str
) -> Optional[tuple[str, str, str, str]]:
    """``notice`` with ``qualifier`` after its body when the body names Fetch current position
    (G137: a fetched point is later than the Full Load's), else as it is. Pure."""
    if notice is None or not qualifier or "Fetch current position" not in notice[3]:
        return notice
    tone, icon, header, body = notice
    return tone, icon, header, f"{body} {qualifier}"


def _start_point_notice(
    verdict: Optional[StartPointVerdict],
    *,
    point_label: str,
    coord: str,
    migration_type: object,
    resumes: bool = False,
    streamed: bool = False,
) -> Optional[tuple[str, str, str, str]]:
    if verdict is None or verdict.kind == KIND_OK:
        return None
    point = point_label
    streamed = streamed or resumes
    if verdict.kind == KIND_CHANGED:
        if resumes:
            lead = (
                f"{_RESUME_LEAD}. The tool can compare them only with {point}, which that "
                "position is past. If a table changed while CDC was stopped, row changes "
                "written before the change would be read with the wrong columns (the "
                "connector stops, or can write shifted, wrapped or NULL values without any "
                "error); a change made before Stop CDC would be harmless, but the tool "
                "cannot tell the two apart."
            )
        else:
            lead = (
                f"Start CDC would read the binary log from {point} using the tables' "
                "definitions as they are now, so row changes written before the change "
                "would be read with the wrong columns: the connector stops on every start "
                "when a column was added, dropped or renamed, and otherwise can write "
                "shifted, wrapped or NULL values without any error. Starting again, Retry "
                "CDC and Debezium's \"recovery mode\" all replay the same way."
            )
        body = (
            f"{lead} Changed since {coord}: {changed_rows(verdict)}. "
            f"{_ways_after_change(coord, migration_type, streamed=streamed)} No connectors "
            "were changed."
        )
        return "error", "schema", E1_HEADER, body
    if verdict.kind == KIND_UNREADABLE:
        reason, exit_ = _unreadable_reason(verdict)
        lead = (
            f"{_RESUME_LEAD}, so Start CDC first checks the captured tables against {point}"
            if resumes
            else f"This start reads the binary log from {point} using the tables' definitions "
            "as they are now, so Start CDC first checks that no captured table changed since "
            "then"
        )
        body = f"{lead}, and {reason} {exit_} No connectors were changed."
        return "error", "link_off", E2_HEADER, body
    if streamed:
        # G25: a new start point on a stack that has streamed needs new CDC topics first.
        way = (
            f"use {NEW_TOPICS}, then choose Manual on the new stack, Fetch current position "
            "and Use this start point for a start point the tool can check (changes made "
            "before it are then not replicated), then Start CDC."
            if _cdc_only(migration_type)
            else f"use {NEW_TOPICS}, then Re-run Full Load with \"Drop & reload\" for a "
            "start point the tool can check, then Start CDC."
        )
    else:
        way = (
            "choose Manual, then Fetch current position and Use this start point for a start "
            "point the tool can check (changes made before it are then not replicated)."
            if _cdc_only(migration_type)
            else 're-run the Full Load with "Drop & reload" for a start point the tool can '
            "check."
        )
    reads = (
        f"{_RESUME_LEAD}, so if one of them was altered while CDC was stopped ({_ALTERED})"
        if resumes
        else f"CDC reads the binary log from {point} using the tables' definitions as they "
        f"are now, so if one of them was altered since then ({_ALTERED})"
    )
    body = (
        f"{_unverified_reason(verdict, point)} {reads}, the connector stops or writes wrong "
        f"values. Start only if none was; otherwise {way}"
    )
    return "warning", "help_outline", W1_HEADER, body


def start_point_tooltip(verdict: Optional[StartPointVerdict]) -> Optional[str]:
    """The disabled Start button's tooltip for a blocking verdict, else None. Pure."""
    if verdict is None:
        return None
    if verdict.kind == KIND_CHANGED:
        return E1_TOOLTIP
    if verdict.kind == KIND_UNREADABLE:
        return E2_TOOLTIP
    return None


def start_point_activity(
    verdict: Optional[StartPointVerdict], *, point_label: str
) -> Optional[tuple[str, ActivityStatus, str]]:
    """``(action, status, detail)`` for the activity log, or None. Pure; names and reasons
    only, never values."""
    if verdict is None or verdict.kind == KIND_OK:
        return None
    if verdict.kind == KIND_CHANGED:
        return (
            ACTION_CHANGED, ActivityStatus.FAILURE,
            f"{E1_HEADER}: {changed_rows(verdict)}",
        )
    if verdict.kind == KIND_UNREADABLE:
        return ACTION_UNREADABLE, ActivityStatus.FAILURE, _unreadable_reason(verdict)[0]
    return ACTION_UNVERIFIED, ActivityStatus.INFO, _unverified_reason(verdict, point_label)


__all__ = [
    "ACTION_CHANGED",
    "ACTION_UNREADABLE",
    "ACTION_UNVERIFIED",
    "E1_HEADER",
    "E2_HEADER",
    "NEWER_POINT_HEADER",
    "NEW_TOPICS",
    "OFFER_UNKNOWN_HEADER",
    "W1_HEADER",
    "KIND_CHANGED",
    "KIND_OK",
    "KIND_UNREADABLE",
    "KIND_UNVERIFIED",
    "PROBLEM_NO_BASELINE",
    "PROBLEM_NO_SOURCE",
    "PROBLEM_READ_FAILED",
    "RELOAD_CONDITION_HEADER",
    "RELOAD_CONDITION_LINE",
    "RELOAD_CONDITION_TOOLTIP",
    "RELOAD_FIRST",
    "RELOAD_RESETS_MANUAL",
    "RERUN_FULL_LOAD_DROP_AND_RELOAD",
    "StartPointVerdict",
    "changed_rows",
    "drop_and_reload_rerun",
    "new_topics_route",
    "newer_point_line",
    "newer_point_notice",
    "offer_unknown_line",
    "point_coord",
    "point_label",
    "reload_condition",
    "reloads_first",
    "start_point_activity",
    "start_point_notice",
    "start_point_tooltip",
    "verdict_from_check",
    "with_split_qualifier",
]
