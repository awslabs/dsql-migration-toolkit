# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The MySQL CDC GTID start check (G19): on a ``gtid_mode=ON`` source, refuse a Start that
cannot begin exactly at its start point. Pure (verdict + copy); the read lives in
``data_migration._cdc_status.read_source_gtid_state_once``.

Debezium 2.7.4 positions by GTID only when the source's ``gtid_mode`` is exactly ``ON``
(``MySqlConnection.isGtidModeEnabled``). It then asks MySQL for every transaction NOT in the
GTID set of its offset and ignores the offset's binlog file:position, and an offset with no
GTID set makes it ask for everything the source still retains. So on an ON source the GTID
set seeded with the start point IS the start point:

- a start point with no GTID set would re-read the whole retained binary log (G1);
- one recorded while ``gtid_mode`` was not ON cannot be streamed by GTID: MySQL stops on the
  changes written without a GTID, or skips them with no error (G2);
- a set this source never executed, or one the source purged binary logs after, cannot be
  streamed either (G4).

A resume continues the stack's committed offset, so it is judged by the stack's OWN
records, not by whatever start point the session holds now. First by the ``gtid_mode`` its
last Start positioned under (its ``StartGtidMode`` parameter, G20: every Start this check
lets through writes the mode it read -- a resume with no start point included -- and Stop
carries it forward; it is kept apart from ``Watermark*``, so the Start with no start point
that blanks those does not erase it; a Start that could not read the mode writes nothing).
Debezium advances the GTID set in its offset only while ``gtid_mode`` is exactly ON, so a
stack last started under another mode holds a set that stopped advancing at that start: on
an ON source every start of it is refused -- a resume, and a seeded start too, because the
offset seeder keeps the committed offset whenever the new point is not later than it and
Start CDC cannot see where that offset is. A stack last started under ON continues by GTID.
A stack with no such record (started by version 0.1.566 or earlier, or whose starts could
not read the mode) is judged by the start offset its last Start offered (its ``Watermark*``
parameters, which Stop carries forward): a stack that was offered a binlog position with no
GTID set streamed by file:position (or was started by an earlier version) and holds no GTID
set to continue from (G1, resume variant).

With ``gtid_mode`` OFF, OFF_PERMISSIVE or ON_PERMISSIVE Debezium positions by binlog
file:position, exactly as before this check existed, so nothing is refused there (that mode
becomes the stack's record). When the mode cannot be read (no source connection, a failed
read), a start point WITHOUT a GTID set, or one recorded while the mode was not ON, is
refused (G3), and so is every start of a stack that holds a ``gtid_mode`` record and a
stack's first start: the record is only true if every start reads the mode. Otherwise (a
stack with no record that streamed before) Debezium uses whichever coordinate the mode
requires, and a resume continues from what Debezium committed. On every mode, a Manual GTID
set that is not a GTID set at all is refused before anything is read: Debezium would write it
into every offset it commits and fail on it at the next task start.

Start CDC runs the check in the Start dialog and again on the job's worker thread before
anything changes (Retry CDC skips the dialog), never for a stack whose connectors exist. A
Start the worker's check lets through records the mode it read on the stack
(:func:`gtid_mode_to_record`); one that could not read it records nothing.
Value-free (Property 7): positions, GTID sets, modes and exception class names only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.cdc_coords import (
    gtid_mode_is_on,
    gtid_set_summary,
    normalize_gtid_set,
)
from dsql_migrator.ui.cdc_start_point_gate import (
    NEW_TOPICS,
    NO_SEED_LABEL,
    PROBLEM_NO_SOURCE,
    PROBLEM_READ_FAILED,
    RELOAD_RESETS_MANUAL,
    point_coord,
    point_label,
    with_split_qualifier,
)

PROBLEM_CHECK_FAILED = "check-failed"

KIND_PROCEED = "proceed"
KIND_NO_GTID = "no-gtid"
KIND_MODE_CHANGED = "mode-changed"
KIND_UNREADABLE = "unreadable"
KIND_NOT_USABLE = "not-usable"
KIND_MALFORMED = "malformed"

# Why a GTID set is not usable (KIND_NOT_USABLE).
REASON_NOT_EXECUTED = "not-executed"
REASON_PURGED = "purged"
REASON_CHECK_FAILED = "check-failed"

# How a start that proceeds positions (the deploy log + activity line).
POSITION_GTID = "gtid"
POSITION_GTID_RESUME = "gtid-resume"
POSITION_BINLOG = "binlog"
POSITION_BINLOG_RESUME = "binlog-resume"
POSITION_UNKNOWN = "unknown"
POSITION_UNKNOWN_RESUME = "unknown-resume"

G1_HEADER = "The CDC start point has no GTID set"
G2_HEADER = "The source's gtid_mode changed since the CDC start point"
# G2 judged by the stack's own gtid_mode record (G20): it last started while not ON.
G2_STACK_HEADER = "The source's gtid_mode changed since this CDC stack last started"
G3_HEADER = "Cannot read the source's gtid_mode"
G4_HEADER = "The source cannot stream from the start point's GTID set"
# G4 when the check itself failed: it could not be checked, not "cannot be used".
G4_CHECK_HEADER = "Cannot check the start point's GTID set against the source"
MALFORMED_HEADER = "The CDC start point's GTID set is not a valid GTID set"

BLOCK_TOOLTIP = (
    "The CDC start point cannot be used on this gtid_mode=ON source — see the notice above."
)
G3_TOOLTIP = "The source's gtid_mode could not be read — see the notice above."
G4_CHECK_TOOLTIP = (
    "The start point's GTID set could not be checked against the source — see the notice "
    "above."
)
MALFORMED_TOOLTIP = "The CDC start point's GTID set is not valid — see the notice above."

ACTION_BLOCKED = "start blocked: CDC start point's GTID position"
ACTION_POSITION = "CDC start position"

# How a resume names where it continues from (no start point is seeded).
_STOPPED_AT = "where this stack stopped"
# How the copy names a resume judged by the stack's own record of its last start offset
# (its Watermark* parameters) rather than by the session's start point.
STACK_POINT_LABEL = "the start offset this stack was last started from"

_HEADERS = {
    KIND_NO_GTID: (G1_HEADER, "fingerprint"),
    KIND_MODE_CHANGED: (G2_HEADER, "sync_problem"),
    KIND_UNREADABLE: (G3_HEADER, "link_off"),
    KIND_NOT_USABLE: (G4_HEADER, "history_toggle_off"),
    KIND_MALFORMED: (MALFORMED_HEADER, "rule"),
}

_LEAD = (
    "This source runs gtid_mode=ON, so Debezium starts streaming after the GTID set "
    "recorded with the start point, not at its binlog file:position."
)
_NO_CHANGE = " No connectors were changed."


@dataclass(frozen=True)
class GtidSourceState:
    """What one read of the source said (value-free).

    ``mode``: ``@@GLOBAL.gtid_mode`` as read, ``None`` when unread. ``within_executed`` /
    ``covers_purged``: ``GTID_SUBSET(set, @@GLOBAL.gtid_executed)`` and
    ``GTID_SUBSET(@@GLOBAL.gtid_purged, set)``, ``None`` when not checked. ``problem``:
    ``no-source-connection`` / ``read-failed`` (the mode is unknown) or ``check-failed``
    (the mode was read, the GTID set check failed), with the exception's class name.
    """

    mode: Optional[str] = None
    within_executed: Optional[bool] = None
    covers_purged: Optional[bool] = None
    problem: Optional[str] = None
    error_class: Optional[str] = None


@dataclass(frozen=True)
class GtidStartVerdict:
    """The outcome of one GTID start check (value-free).

    ``kind``: ``proceed`` (``positioning`` says how Debezium positions) or a refusal --
    ``no-gtid`` (G1), ``mode-changed`` (G2), ``unreadable`` (G3, ``reason`` is the read
    problem), ``not-usable`` (G4, ``reason`` is ``not-executed`` / ``purged`` /
    ``check-failed``) or ``malformed`` (a Manual GTID set that is not one). ``mode`` /
    ``captured_mode``: the source's mode now and the one recorded with the start point
    (upper-cased; ``None`` when unknown). ``resumes``: the start continues the stack's
    committed offset. ``stack_point``: ``binlog F:P`` of the start offset the stack's last
    Start offered, set when a resume was refused on that record rather than on the
    session's start point (the copy names it, see :func:`gtid_notice_point`).
    ``stack_mode``: the stack's ``gtid_mode`` record (G20, upper-cased), set only on a
    refusal the record decided -- ``mode-changed`` (the stack last started while it was
    not ON) or ``unreadable`` with ``mode_required`` (the mode must be read to keep the
    record true: the stack holds one, or this is its first start; ``stack_mode`` is then
    ``None`` for a first start). ``seed_fits_new_stack``: set only on a seeded start that
    record refused (``mode-changed`` with ``stack_mode``, not a resume) when the same rules
    would let a NEW stack -- no records, no committed offset -- start exactly at the seed
    (the copy then offers that way out instead of always asking for a new start point).
    """

    kind: str
    positioning: Optional[str] = None
    reason: Optional[str] = None
    mode: Optional[str] = None
    captured_mode: Optional[str] = None
    resumes: bool = False
    error_class: Optional[str] = None
    stack_point: Optional[str] = None
    stack_mode: Optional[str] = None
    mode_required: bool = False
    seed_fits_new_stack: bool = False

    @property
    def blocks(self) -> bool:
        """Whether Start CDC must refuse."""
        return self.kind != KIND_PROCEED


def _seedable(seed: object) -> bool:
    return bool(getattr(seed, "binlog_file", None)) and getattr(
        seed, "binlog_position", None
    ) is not None


def gtid_start_check_due(seed: object, facts: object) -> bool:
    """Must Start CDC run the GTID start check for ``seed``? Pure.

    Not for a stack whose connectors exist (``facts.live_mode``: Retry CDC, a live
    re-Start -- they keep what they run). Otherwise for a resume of the committed offset
    and for a seedable start point (binlog file:position); a GTID set alone seeds nothing.
    """
    if facts is not None and getattr(facts, "live_mode", None):
        return False
    if facts is not None and getattr(facts, "resumes", False):
        return True
    return seed is not None and _seedable(seed)


def _upper(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip().upper()
    return text or None


def mysql_gtid_start_verdict(
    state: Optional[GtidSourceState],
    *,
    seed_gtids: Optional[str],
    seed_gtid_mode: Optional[str],
    seedable: bool,
    resumes: bool,
    stack_gtids: Optional[str] = None,
    stack_point: Optional[str] = None,
    seed_is_stack_point: bool = False,
    seed_set_malformed: bool = False,
    stack_gtid_mode: Optional[str] = None,
    first_start: bool = False,
) -> GtidStartVerdict:
    """The GTID start verdict for one MySQL Start (rules R0-R6, in order). Pure.

    ``seed_gtids``: the start point's GTID set (normalized; empty when it has none).
    ``seed_gtid_mode``: the source's ``gtid_mode`` recorded with it (a Full Load watermark
    of this version; ``None`` for a Manual point or an older watermark). ``seedable``: the
    start point has a binlog file:position. ``resumes``: the start continues the stack's
    committed offset. ``stack_gtids``: the stack's own record of the start offset its last
    Start offered -- that offer's GTID set (its ``WatermarkGtids``, normalized; ``""`` when
    it offered a binlog position with none), or ``None`` when the stack records no start
    offset; ``stack_point`` names that offer (``binlog F:P``) and ``seed_is_stack_point``
    says the session's start point is that very position. ``seed_set_malformed``: the
    start point's GTID set is a Manual entry that is not a GTID set. ``stack_gtid_mode``:
    the stack's ``gtid_mode`` record (its ``StartGtidMode``, G20: the mode its last
    positioning Start ran under; ``None`` or blank when it has none -- any value other than
    ``ON`` counts as not ON). ``first_start``: the stack has never started (no committed
    offset, not a resume). Both default to the judgement of a stack without a record.

    0. A seeded start whose Manual GTID set is not a GTID set: refused on every mode.
    1. Mode unread: a start point without a GTID set is refused (G3), and so is one
       recorded while the mode was not ON (if the source is ON now, its set does not
       describe the point: a silent skip is possible). Any other start that positions (a
       resume or a seedable point) is refused too when the stack holds a ``gtid_mode``
       record or this is its first start (G3, ``mode_required``): the record is only true
       if every start reads the mode. Otherwise a resume proceeds, and so does a seed with
       a GTID set (Debezium uses whichever the mode requires).
    2. Mode not ON (OFF, OFF_PERMISSIVE, ON_PERMISSIVE): proceed by binlog file:position
       (that mode becomes the stack's record).
    2s. ON, and the stack holds a ``gtid_mode`` record (G20): one other than ``ON`` refuses
       every start that positions (G2, ``stack_mode``) -- the GTID set in the committed
       offset stopped advancing at that start, and a seeded start cannot replace it with
       certainty (the offset seeder keeps the committed offset whenever the new point is
       not later than it); a seeded start refused here also says whether a new stack would
       start exactly at the seed (``seed_fits_new_stack``: rules 3, 5 and 6 without the
       stack's records). Record ``ON``: a resume continues after the GTID set Debezium
       committed (whatever the Watermark* record says); a seeded start falls through to
       rules 3, 5 and 6, which judge the seed.
    3. ON, and the start point recorded while the mode was not ON: refused (G2) -- for a
       resume only when that point is what the stack was last started from (or the stack
       records nothing); otherwise rule 4 judges the stack's own record.
    4. ON and a resume, judged by the stack's own record when it has one: an offer with a
       GTID set continues after the GTID set Debezium committed, and so does an empty set
       recorded while ON at that very point; an offer with a binlog position and no GTID
       set is refused (G1, resume variant) -- that stack streamed by file:position or was
       started by an earlier version, so it holds no GTID set to continue from. With no
       record, the session's start point is judged the same way. (Reached only by a stack
       without a ``gtid_mode`` record: rule 2s decides every other resume.)
    5. ON and a seed with no GTID set (and no ON recorded with it): refused (G1).
    6. ON and a seed with a GTID set (or an empty one recorded while ON): it must be within
       the source's ``gtid_executed`` and cover its ``gtid_purged`` (G4 otherwise, also
       when the check failed or could not run).
    """
    mode = _upper(getattr(state, "mode", None)) if state is not None else None
    cap = _upper(seed_gtid_mode)
    problem = getattr(state, "problem", None) if state is not None else PROBLEM_NO_SOURCE
    error_class = getattr(state, "error_class", None) if state is not None else None
    has_set = bool(normalize_gtid_set(seed_gtids))
    # The stack's own record (None: it records no start offset).
    record = normalize_gtid_set(stack_gtids) if stack_gtids is not None else None
    # The stack's gtid_mode record (None: none). Anything but exactly ON is "not ON".
    stack_mode = _upper(stack_gtid_mode)
    # Does this start position the source connector at all (a resume or a seeded point)?
    positions = resumes or seedable

    def _verdict(kind: str, **extra) -> GtidStartVerdict:
        return GtidStartVerdict(
            kind=kind, mode=mode, captured_mode=cap, resumes=resumes, **extra
        )

    # R0: a Manual GTID set that is not one would be seeded into the offset whatever the
    # mode (Debezium cannot read it back at the next task start), so nothing is read first.
    if seed_set_malformed and seedable and not resumes:
        return _verdict(KIND_MALFORMED)
    # R1: the mode is unknown.
    if mode is None or problem in (PROBLEM_NO_SOURCE, PROBLEM_READ_FAILED):
        unread = problem or PROBLEM_READ_FAILED
        if not resumes and seedable and not (has_set and cap in (None, "ON")):
            # A seed without a GTID set, or one recorded while not ON (as before).
            return _verdict(KIND_UNREADABLE, reason=unread, error_class=error_class)
        if positions and (stack_mode is not None or first_start):
            # G20: the stack's gtid_mode record is only true if every start reads the mode
            # -- the record it holds may say "not ON", and a first start must write one.
            return _verdict(
                KIND_UNREADABLE, reason=unread, error_class=error_class,
                stack_mode=stack_mode, mode_required=True,
            )
        if resumes:
            return _verdict(
                KIND_PROCEED, positioning=POSITION_UNKNOWN_RESUME,
                reason=unread, error_class=error_class,
            )
        return _verdict(
            KIND_PROCEED, positioning=POSITION_UNKNOWN if seedable else None,
            reason=unread, error_class=error_class,
        )
    # R2: not ON -> binlog file:position, as before.
    if not gtid_mode_is_on(mode):
        return _verdict(
            KIND_PROCEED,
            positioning=POSITION_BINLOG_RESUME if resumes else POSITION_BINLOG,
        )
    # R2s (G20): ON now, and the stack records the gtid_mode its last Start positioned
    # under. Debezium advances the GTID set in its offset only while gtid_mode is exactly
    # ON, so a stack last started under another mode holds a set that stopped advancing
    # then -- refused for a resume AND a seeded start (the seeder keeps the committed
    # offset whenever the new point is not later than it, unseen from here). A stack last
    # started under ON continues by GTID; its seeded start is judged on the seed below.
    if stack_mode is not None and positions:
        if stack_mode != "ON":
            # For the copy only: a NEW stack has no committed offset for the seeder to keep,
            # so would it start exactly at this seed? The same rules, without the records.
            fits = not resumes and mysql_gtid_start_verdict(
                state, seed_gtids=seed_gtids, seed_gtid_mode=seed_gtid_mode,
                seedable=seedable, resumes=False, first_start=True,
            ).kind == KIND_PROCEED
            return _verdict(
                KIND_MODE_CHANGED, stack_mode=stack_mode, seed_fits_new_stack=fits
            )
        if resumes:
            return _verdict(KIND_PROCEED, positioning=POSITION_GTID_RESUME)
    # R3: ON now, recorded while not ON. A resume continues the stack's committed offset,
    # so the recorded mode speaks for it only when the stack was last started from this
    # very point (or records no start offset at all).
    if cap is not None and cap != "ON":
        if not resumes or record is None or seed_is_stack_point:
            return _verdict(KIND_MODE_CHANGED)
    # R4: ON, a resume -- judged by what the stack itself was last offered, when it records
    # it (Stop carries Watermark* forward), not by the session's point: a session may hold
    # no point (an attached stack, a GTID set alone) or one edited since.
    if resumes:
        if record is not None:
            if record or (seed_is_stack_point and cap == "ON"):
                return _verdict(KIND_PROCEED, positioning=POSITION_GTID_RESUME)
            return _verdict(
                KIND_NO_GTID, stack_point=None if seed_is_stack_point else stack_point
            )
        if seedable and not has_set and cap is None:
            return _verdict(KIND_NO_GTID)
        return _verdict(KIND_PROCEED, positioning=POSITION_GTID_RESUME)
    if not seedable:
        return _verdict(KIND_PROCEED)
    # R5: ON, a seed with no GTID set and no ON recorded with it.
    if not has_set and cap is None:
        return _verdict(KIND_NO_GTID)
    # R6: ON, a seed with a GTID set (or an empty one recorded while ON).
    if problem == PROBLEM_CHECK_FAILED:
        return _verdict(KIND_NOT_USABLE, reason=REASON_CHECK_FAILED, error_class=error_class)
    within = getattr(state, "within_executed", None)
    covers = getattr(state, "covers_purged", None)
    if within is False:
        return _verdict(KIND_NOT_USABLE, reason=REASON_NOT_EXECUTED)
    if covers is False:
        return _verdict(KIND_NOT_USABLE, reason=REASON_PURGED)
    if within is None or covers is None:
        return _verdict(KIND_NOT_USABLE, reason=REASON_CHECK_FAILED, error_class=error_class)
    return _verdict(KIND_PROCEED, positioning=POSITION_GTID)


def _capitalized(text: str) -> str:
    return text[:1].upper() + text[1:]


def _cdc_only(migration_type: object) -> bool:
    value = getattr(migration_type, "value", migration_type)
    return str(value or "") == "cdc_only"


def _keep(start: str) -> str:
    return (
        "To keep this start point, set the source's gtid_mode back to ON_PERMISSIVE until "
        "cut over (a parameter-group change on RDS and Aurora): CDC then streams from "
        f"{start} by binlog file:position."
    )


def _new_point(
    since: str, migration_type: object, *, redeploy: bool = False, keep_held: bool = False
) -> str:
    # ``redeploy``: the stack's committed offset would outrank any new start point (the
    # stack's gtid_mode record refused it, G20), so the new point needs a new stack.
    # ``keep_held``: a new stack could also start from the point the session holds
    # (``seed_fits_new_stack``) -- exactly, only if gtid_mode stayed ON since that point
    # (no change after it lacks a GTID), which Start CDC cannot see: the copy names it.
    if keep_held:
        lead = (
            "Or Delete CDC infrastructure on the CDC step and deploy it again (a new stack "
            "has no committed offset, so CDC then starts exactly at its start point), then "
            "Start CDC again — from this start point if the source's gtid_mode was already "
            "ON when it was recorded and has stayed ON since, or else from a new one"
        )
    elif redeploy:
        lead = (
            "Or Delete CDC infrastructure on the CDC step, deploy it again (a new stack has "
            "no committed offset, so CDC then starts exactly at its start point) and give "
            "CDC a new start point"
        )
    else:
        lead = "Or give CDC a new start point"
    if _cdc_only(migration_type):
        return (
            f"{lead}: choose Manual, then Fetch current position and "
            f"Use this start point. Changes made between {since} and the new point are then "
            "not replicated: correct them on Aurora DSQL and validate with CHECKSUM."
        )
    return (
        f"{lead}: Re-run Full Load for all tables with \"Drop & "
        "reload\" — its new watermark records the GTID set with the position (Reload "
        "specific tables and Retry keep this one) — or choose Manual, then Fetch current "
        "position and Use this start point: changes to the captured tables between "
        f"{since} and that point are then not replicated, so reload them with \"Drop & "
        f"reload\" and validate with CHECKSUM. {RELOAD_RESETS_MANUAL}"
    )


def _since_clause(coord: str, migration_type: object) -> str:
    if _cdc_only(migration_type):
        return (
            f"changes made between {coord} and the new point are then not replicated: "
            "correct them on Aurora DSQL and validate with CHECKSUM"
        )
    return (
        f"changes to the captured tables between {coord} and the new point are then not "
        "replicated, so reload them with \"Drop & reload\" and validate with CHECKSUM"
    )


def _reload_suffix(migration_type: object) -> str:
    return "" if _cdc_only(migration_type) else f" {RELOAD_RESETS_MANUAL}"


# G25: a Start CloudFormation rolled back may have streamed into this stack's CDC topics, so a
# NEW start point there needs new topics first (the Full Load step refuses a new one, and Start
# CDC a newer one): said before a remedy that would offer one.
ROLLED_BACK_NEW_POINT = (
    "This stack's last Start rolled back, so a new start point needs new CDC topics: use "
    f"{NEW_TOPICS} first."
)


def _no_gtid_body(
    verdict: GtidStartVerdict,
    point: str,
    coord: str,
    origin: str,
    migration_type: object,
    rolled_back: bool = False,
) -> str:
    if verdict.resumes:
        # A resume: the stack streamed, so a new start point needs a new stack (G25).
        return (
            f"{_capitalized(point)} has no GTID set, and the source's gtid_mode is ON now. A "
            "stack started from such a point streamed by binlog file:position — while "
            "gtid_mode was not ON — or was started by an earlier version of the tool, so "
            "the position it recorded may hold no GTID set to continue from, and continuing "
            "by GTID could re-read changes or skip some with no error. "
            f"{_keep(_STOPPED_AT)} {_new_point(_STOPPED_AT, migration_type, redeploy=True)}"
        )
    if origin == "manual":
        fetch = (
            f"{ROLLED_BACK_NEW_POINT} Then choose Fetch current position on the new stack and "
            "Use this start point"
            if rolled_back
            else "Choose Fetch current position and Use this start point"
        )
        return (
            f"{_LEAD} {_capitalized(point)} has no GTID set — it was entered without one, "
            "fetched while the source's gtid_mode was not ON, or restored from a session "
            "an earlier version of the tool saved (which did not keep a GTID set with its "
            "position) — so Debezium would read "
            "the source's whole retained binary log again from its beginning and re-apply "
            f"changes made before it. {fetch}: it records the binlog file:position and the "
            f"GTID set together ({_since_clause(coord, migration_type)}). If the source's "
            f"gtid_mode was ON at {coord}, you can instead enter the GTID set it had executed "
            "at exactly that position (for example the one your dump recorded) and Use this "
            f"start point. {_keep(coord)}{_reload_suffix(migration_type)}"
        )
    return (
        f"{_LEAD} {_capitalized(point)} has no GTID set: the source reported none when it "
        "was captured, because its gtid_mode was not ON then (or no transaction had a GTID "
        "yet). Debezium would therefore read the source's whole retained binary log again "
        "from its beginning and re-apply changes the Full Load already copied. "
        f"{_keep(coord)} {_new_point(coord, migration_type, redeploy=rolled_back)}"
    )


def _mode_changed_body(
    verdict: GtidStartVerdict,
    point: str,
    coord: str,
    migration_type: object,
    rolled_back: bool = False,
) -> str:
    captured = verdict.captured_mode or "not ON"
    lead = (
        f"{_capitalized(point)} was recorded while the source's gtid_mode was {captured}, "
        "and it is ON now."
    )
    if verdict.resumes:
        # A resume: the stack streamed, so a new start point needs a new stack (G25).
        return (
            f"{lead} This stack streamed from that point by binlog file:position, so the "
            "position it recorded holds no GTID set to continue from, and continuing by "
            "GTID would re-read changes or skip some with no error. "
            f"{_keep(_STOPPED_AT)} {_new_point(_STOPPED_AT, migration_type, redeploy=True)}"
        )
    return (
        f"{lead} With gtid_mode=ON Debezium starts from a GTID set, and the changes the "
        "source wrote without a GTID after that point cannot be streamed that way: MySQL "
        "stops the connector on them, or skips them with no error. "
        f"{_keep(coord)} {_new_point(coord, migration_type, redeploy=rolled_back)}"
    )


def _stack_mode_body(
    verdict: GtidStartVerdict, coord: str, migration_type: object
) -> str:
    """G2 decided by the stack's own gtid_mode record (G20): it last started while not ON."""
    lead = (
        f"This CDC stack last started while the source's gtid_mode was {verdict.stack_mode}, "
        "so its source connector streamed by binlog file:position and the GTID set in its "
        "committed offset stopped advancing at that start. The source's gtid_mode is ON "
        "now, so Debezium would continue after that GTID set: it would re-send changes "
        "already applied, stop on the changes written without a GTID, or skip them with no "
        "error."
    )
    if verdict.resumes:
        return (
            f"{lead} {_keep(_STOPPED_AT)} "
            f"{_new_point(_STOPPED_AT, migration_type, redeploy=True)}"
        )
    # A seeded start on this stack (G25, A2): Start CDC does not take a start point newer
    # than where a streamed stack last started (its sink would apply older changes over
    # newer rows), and one at or before it is a resume -- so only a new stack starts there.
    route = _new_point(
        _STOPPED_AT, migration_type, redeploy=True, keep_held=verdict.seed_fits_new_stack
    )
    return (
        f"{lead} A new start point cannot replace that offset on this stack: Start CDC does "
        "not take a start point newer than where this stack last started, and the offset "
        "seeder keeps the committed offset whenever the new point is not later than it. "
        # The redeploy is the only way here, not an alternative to keeping the point.
        + _capitalized(route[len("Or "):] if route.startswith("Or ") else route)
    )


def _unread_reason_exit(verdict: GtidStartVerdict) -> tuple[str, str]:
    """``(reason, exit)`` sentences for a gtid_mode that could not be read."""
    if verdict.reason == PROBLEM_NO_SOURCE:
        return (
            "this session has no source connection (the source password is kept in memory "
            "only, so an app restart clears it).",
            "Test the source connection on the Connect step, then Start CDC again.",
        )
    return (
        f"reading it failed ({verdict.error_class or 'no value returned'}).",
        "Check that the source database is reachable from the tool, then Start CDC again.",
    )


def _unreadable_body(verdict: GtidStartVerdict, point: str) -> str:
    reason, exit_ = _unread_reason_exit(verdict)
    if verdict.mode_required:
        # G20: the stack's gtid_mode record must stay true, so every start reads the mode.
        if verdict.stack_mode is not None and verdict.stack_mode != "ON":
            return (
                "This CDC stack last started while the source's gtid_mode was "
                f"{verdict.stack_mode}, so the GTID set in its committed offset stopped "
                "advancing then: Start CDC first checks that the source's gtid_mode is "
                "still not ON — with gtid_mode=ON, Debezium would continue after that GTID "
                "set and re-send changes, or skip some with no error — and "
                f"{reason} {exit_}"
            )
        return (
            "Start CDC reads the source's gtid_mode before every start of this CDC stack "
            "and records it on the stack: Debezium positions by GTID only when it is ON, "
            "and a later start can continue by GTID only after a start under ON. It cannot "
            f"be read now: {reason} {exit_}"
        )
    if verdict.captured_mode is not None and verdict.captured_mode != "ON":
        return (
            f"{_capitalized(point)} was recorded while the source's gtid_mode was "
            f"{verdict.captured_mode}, so Start CDC first checks that the source's "
            "gtid_mode is still not ON — with gtid_mode=ON, Debezium could not start "
            "exactly at it: it would re-read changes, or skip some with no error — and "
            f"{reason} {exit_}"
        )
    return (
        f"{_capitalized(point)} has no GTID set, so Start CDC first checks that the "
        "source's gtid_mode is not ON — with gtid_mode=ON, Debezium would read the "
        "source's whole retained binary log again from its beginning — and "
        f"{reason} {exit_}"
    )


def _check_failed_body(verdict: GtidStartVerdict, origin: str) -> str:
    # The check could not run (a dropped connection, a read timeout): nothing says the start
    # point is unusable, so the way forward is to check again, never a new start point.
    return (
        f"{_LEAD} Checking that GTID set against the source failed "
        f"({verdict.error_class or 'no result'}), so Start CDC cannot confirm that the source "
        "can stream from it."
        + (
            " If you entered the GTID set by hand, check that it is a valid MySQL GTID set."
            if origin == "manual"
            else ""
        )
        + " Check that the source database is reachable from the tool, then Start CDC again."
    )


def _malformed_body(point: str, coord: str, migration_type: object) -> str:
    return (
        f"The GTID set entered with {point} is not a GTID set Debezium can read — "
        "<server UUID>:<interval>, comma-separated, for example "
        "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100 — usually because quotes, a ';' or a "
        "SET statement were copied with it. Debezium writes the start point's GTID set into "
        "every offset it commits and cannot read this one back, so on any gtid_mode CDC "
        "would stop at the connector's next restart. Correct the GTID set, clear it if the "
        "source's gtid_mode is not ON, or choose Fetch current position and Use this start "
        "point: it records the binlog file:position and the GTID set together "
        f"({_since_clause(coord, migration_type)}).{_reload_suffix(migration_type)}"
    )


def _not_usable_body(
    verdict: GtidStartVerdict,
    point: str,
    coord: str,
    origin: str,
    migration_type: object,
    rolled_back: bool = False,
) -> str:
    if verdict.reason == REASON_CHECK_FAILED:
        return _check_failed_body(verdict, origin)
    if verdict.reason == REASON_NOT_EXECUTED:
        cause = (
            f"The GTID set recorded with {point} names transactions this source has not "
            "executed — it was read from another server (a source restored, cloned or "
            "switched over since)"
            + (", or entered incorrectly" if origin == "manual" else "")
            + " — so streaming from it would fail, or skip those transactions if they ever "
            "ran."
        )
    else:
        cause = (
            "The source has already purged binary logs that hold changes made after that "
            "GTID set, so those changes can no longer be streamed."
        )
    purged = verdict.reason == REASON_PURGED
    # G25: after a Start CloudFormation rolled back, a remedy that takes a new start point runs
    # on a new stack (a corrected GTID set for the same position does not): said first.
    first = f" {ROLLED_BACK_NEW_POINT} Then" if rolled_back else ""
    on_new = " on the new stack" if rolled_back else ""
    if origin == "manual":
        if rolled_back:
            remedy = (
                ("" if purged else " Correct the GTID set you entered, or give CDC a new start "
                 "point.")
                + f"{first} choose Fetch current position{on_new} and Use this start point: it "
                "records the GTID set together with the position "
                f"({_since_clause(coord, migration_type)}).{_reload_suffix(migration_type)}"
            )
        else:
            remedy = (
                " " + ("Choose" if purged else "Correct the GTID set you entered, or choose")
                + " Fetch current position and Use this start point: it records the GTID set "
                f"together with the position ({_since_clause(coord, migration_type)})."
                + _reload_suffix(migration_type)
            )
    elif _cdc_only(migration_type):
        remedy = (
            f"{first or ' Give CDC a new start point:'} choose Manual{on_new}, then Fetch "
            f"current position and Use this start point. Changes made between {coord} and "
            "the new point are then not replicated: correct them on Aurora DSQL and validate "
            "with CHECKSUM."
        )
    else:
        remedy = (
            f"{first} Re-run Full Load for all tables with \"Drop & reload\" for a new start "
            "point"
            + (
                " (raise the source's binlog retention first, for example CALL "
                "mysql.rds_set_configuration('binlog retention hours', 168))"
                if purged
                else ""
            )
            + f", or choose Manual{on_new}, then Fetch current position and Use this start "
            f"point: changes to the captured tables between {coord} and that point are then "
            "not replicated, so reload them with \"Drop & reload\" and validate with "
            "CHECKSUM. " + RELOAD_RESETS_MANUAL
        )
    return f"{_LEAD} {cause}{remedy}"


def gtid_start_notice(
    verdict: Optional[GtidStartVerdict],
    *,
    point_label: str,
    coord: str,
    origin: str,
    migration_type: object,
    rolled_back: bool = False,
    split_qualifier: str = "",
) -> Optional[tuple[str, str, str, str]]:
    """``(tone, icon, header, body)`` for a refusal (the Start dialog, the job), or None.

    ``point_label`` / ``coord`` name the start point as the start-point check does
    (:func:`~dsql_migrator.ui.cdc_start_point_gate.point_label` /
    :func:`~dsql_migrator.ui.cdc_start_point_gate.point_coord`); ``origin`` is the seed's
    (``"full_load"`` / ``"manual"``). ``rolled_back``: CloudFormation rolled back this stack's
    last Start (G25), so a remedy that takes a new start point runs on a new stack -- as on
    every resume, the stack's connectors were started before. ``split_qualifier`` (G137): said
    after a remedy that names Fetch current position while CDC captures a table the Full Load
    read in parts (:func:`~dsql_migrator.ui.cdc_start_point_gate.with_split_qualifier`). Pure.
    """
    return with_split_qualifier(
        _gtid_start_notice(
            verdict, point_label=point_label, coord=coord, origin=origin,
            migration_type=migration_type, rolled_back=rolled_back,
        ),
        split_qualifier,
    )


def _gtid_start_notice(
    verdict: Optional[GtidStartVerdict],
    *,
    point_label: str,
    coord: str,
    origin: str,
    migration_type: object,
    rolled_back: bool = False,
) -> Optional[tuple[str, str, str, str]]:
    if verdict is None or not verdict.blocks:
        return None
    header, icon = _header_icon(verdict)
    if verdict.kind == KIND_NO_GTID:
        body = _no_gtid_body(verdict, point_label, coord, origin, migration_type, rolled_back)
    elif verdict.kind == KIND_MODE_CHANGED and verdict.stack_mode is not None:
        body = _stack_mode_body(verdict, coord, migration_type)
    elif verdict.kind == KIND_MODE_CHANGED:
        body = _mode_changed_body(verdict, point_label, coord, migration_type, rolled_back)
    elif verdict.kind == KIND_UNREADABLE:
        body = _unreadable_body(verdict, point_label)
    elif verdict.kind == KIND_MALFORMED:
        body = _malformed_body(point_label, coord, migration_type)
    else:
        body = _not_usable_body(
            verdict, point_label, coord, origin, migration_type, rolled_back
        )
    return "error", icon, header, body + _NO_CHANGE


def _header_icon(verdict: GtidStartVerdict) -> tuple[str, str]:
    if verdict.kind == KIND_NOT_USABLE and verdict.reason == REASON_CHECK_FAILED:
        return G4_CHECK_HEADER, "link_off"
    if verdict.kind == KIND_MODE_CHANGED and verdict.stack_mode is not None:
        return G2_STACK_HEADER, "sync_problem"
    return _HEADERS[verdict.kind]


def gtid_notice_point(
    verdict: Optional[GtidStartVerdict], *, seed: object, origin: str
) -> tuple[str, str]:
    """``(point_label, coord)`` the refusal copy and its activity line name. Pure.

    The session's start point (:func:`point_label` / :func:`point_coord`), except for a
    resume refused on the stack's own record of its last start offset
    (``verdict.stack_point``): that offer is named, since the session may hold no point;
    and for a resume refused on the stack's ``gtid_mode`` record (G20), which would continue
    from where the stack stopped: named by the no-seed label (its Watermark* may be blank).
    """
    stack_point = getattr(verdict, "stack_point", None) if verdict is not None else None
    if stack_point:
        return f"{STACK_POINT_LABEL} ({stack_point})", stack_point
    if verdict is not None and verdict.stack_mode is not None and verdict.resumes:
        return NO_SEED_LABEL, NO_SEED_LABEL
    return point_label(seed, origin), point_coord(seed)


def gtid_start_tooltip(verdict: Optional[GtidStartVerdict]) -> Optional[str]:
    """The disabled Start button's tooltip for a refusal, else None. Pure."""
    if verdict is None or not verdict.blocks:
        return None
    if verdict.kind == KIND_UNREADABLE:
        return G3_TOOLTIP
    if verdict.kind == KIND_MALFORMED:
        return MALFORMED_TOOLTIP
    if verdict.kind == KIND_NOT_USABLE and verdict.reason == REASON_CHECK_FAILED:
        return G4_CHECK_TOOLTIP
    return BLOCK_TOOLTIP


def _why_unread(verdict: GtidStartVerdict) -> str:
    if verdict.reason == PROBLEM_NO_SOURCE:
        return "this session has no source connection"
    return f"the read failed: {verdict.error_class or 'no value returned'}"


def gtid_positioning_line(
    verdict: Optional[GtidStartVerdict], *, coord: str, seed_gtids: Optional[str]
) -> Optional[str]:
    """How a start that proceeds positions, for the deploy log and the activity log. Pure.

    ``None`` for a refusal or when nothing is positioned (no seed, no resume).
    """
    if verdict is None or verdict.blocks or verdict.positioning is None:
        return None
    summary = gtid_set_summary(seed_gtids)
    mode = verdict.mode or ""
    if verdict.positioning == POSITION_GTID:
        return (
            "Source gtid_mode=ON: Debezium starts after the start point's GTID set "
            f"{summary or '(empty)'} ({coord})."
        )
    if verdict.positioning == POSITION_GTID_RESUME:
        return "Source gtid_mode=ON: Debezium continues after the GTID set it last committed."
    if verdict.positioning == POSITION_BINLOG:
        return f"Source gtid_mode={mode}: Debezium starts at {coord} by binlog file:position."
    if verdict.positioning == POSITION_BINLOG_RESUME:
        return (
            f"Source gtid_mode={mode}: Debezium continues from the binlog file:position it "
            "last committed."
        )
    if verdict.positioning == POSITION_UNKNOWN:
        return (
            f"The source's gtid_mode could not be read ({_why_unread(verdict)}): the start "
            f"offset carries {coord} and the GTID set {summary or '(empty)'}; Debezium uses "
            "whichever the source's gtid_mode requires."
        )
    return (
        f"The source's gtid_mode could not be read ({_why_unread(verdict)}): Debezium "
        "continues from the position it last committed."
    )


def gtid_start_activity(
    verdict: Optional[GtidStartVerdict], *, coord: str, seed_gtids: Optional[str] = None
) -> Optional[tuple[str, ActivityStatus, str]]:
    """``(action, status, detail)`` for the activity log, or None. Pure; positions, sets and
    modes only."""
    if verdict is None:
        return None
    if verdict.blocks:
        header, _icon = _header_icon(verdict)
        suffix = (
            f" (last started under gtid_mode={verdict.stack_mode})"
            if verdict.stack_mode is not None and verdict.stack_mode != "ON"
            else ""
        )
        return ACTION_BLOCKED, ActivityStatus.FAILURE, f"{header}: {coord}{suffix}"
    line = gtid_positioning_line(verdict, coord=coord, seed_gtids=seed_gtids)
    if line is None:
        return None
    return ACTION_POSITION, ActivityStatus.INFO, line


def gtid_mode_to_record(verdict: Optional[GtidStartVerdict]) -> Optional[str]:
    """The ``gtid_mode`` a Start that passed the check records on the stack (G20), or None.

    The mode the check READ (upper-cased) for a start that proceeds; ``None`` for no check,
    a refusal, or a mode that could not be read -- nothing is then recorded, and the stack
    keeps the record it has. Pure.
    """
    if verdict is None or verdict.blocks:
        return None
    if verdict.positioning in (POSITION_UNKNOWN, POSITION_UNKNOWN_RESUME):
        return None
    if verdict.reason in (PROBLEM_NO_SOURCE, PROBLEM_READ_FAILED):
        return None
    return verdict.mode or None


__all__ = [
    "ACTION_BLOCKED",
    "ACTION_POSITION",
    "BLOCK_TOOLTIP",
    "G1_HEADER",
    "G2_HEADER",
    "G2_STACK_HEADER",
    "G3_HEADER",
    "G3_TOOLTIP",
    "G4_CHECK_HEADER",
    "G4_CHECK_TOOLTIP",
    "G4_HEADER",
    "GtidSourceState",
    "GtidStartVerdict",
    "KIND_MALFORMED",
    "KIND_MODE_CHANGED",
    "KIND_NOT_USABLE",
    "KIND_NO_GTID",
    "KIND_PROCEED",
    "KIND_UNREADABLE",
    "MALFORMED_HEADER",
    "MALFORMED_TOOLTIP",
    "POSITION_BINLOG",
    "POSITION_BINLOG_RESUME",
    "POSITION_GTID",
    "POSITION_GTID_RESUME",
    "POSITION_UNKNOWN",
    "POSITION_UNKNOWN_RESUME",
    "PROBLEM_CHECK_FAILED",
    "PROBLEM_NO_SOURCE",
    "PROBLEM_READ_FAILED",
    "REASON_CHECK_FAILED",
    "ROLLED_BACK_NEW_POINT",
    "REASON_NOT_EXECUTED",
    "REASON_PURGED",
    "STACK_POINT_LABEL",
    "gtid_mode_to_record",
    "gtid_notice_point",
    "gtid_positioning_line",
    "gtid_set_summary",
    "gtid_start_activity",
    "gtid_start_check_due",
    "gtid_start_notice",
    "gtid_start_tooltip",
    "mysql_gtid_start_verdict",
    "point_coord",
    "point_label",
]
